// SPDX-License-Identifier: Apache-2.0

//! `magnetar` — command-line client for Apache Pulsar.
//!
//! The binary speaks two kinds of subcommands:
//!
//! - `produce` / `consume`: data-plane operations. They are stubs in M9 (they print `not yet wired`
//!   and exit 0). They get wired to the runtime once M2's
//!   [`Connection`](magnetar::proto::Connection) state machine and M3's tokio engine are integrated
//!   into the [`magnetar`] façade.
//! - `admin ...`: control-plane operations. Fully wired against [`magnetar_admin::AdminClient`].
//!   Output is JSON to stdout; errors go to stderr with a non-zero exit code.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

mod version;

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use magnetar::proto::TokenAuth;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::runtime_tokio::ClientError;
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_admin::{AdminClient, AdminClientBuilder, AdminError, TenantInfo};

/// magnetar — produce, consume, inspect, and admin against an Apache Pulsar broker.
#[derive(Debug, Parser)]
#[command(
    name = "magnetar",
    version = version::short(),
    long_version = version::long(),
    about,
    long_about = None,
)]
pub(crate) struct Cli {
    /// Increase logging verbosity (-v, -vv, -vvv). Accepted at any level
    /// (`magnetar admin -vv tenant-list` is the same as
    /// `magnetar -vv admin tenant-list`).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub(crate) verbose: u8,

    /// Pulsar service URL for data-plane (`pulsar://` / `pulsar+ssl://`).
    #[arg(
        long,
        env = "MAGNETAR_SERVICE_URL",
        default_value = "pulsar://localhost:6650",
        global = true
    )]
    pub(crate) service_url: String,

    /// Pulsar admin REST URL (`http://` / `https://`).
    #[arg(
        long,
        env = "MAGNETAR_ADMIN_URL",
        default_value = "http://localhost:8080",
        global = true
    )]
    pub(crate) admin_url: String,

    /// Bearer token for admin auth. Reads from `MAGNETAR_TOKEN` if unset.
    #[arg(long, env = "MAGNETAR_TOKEN", global = true)]
    pub(crate) token: Option<String>,

    /// Admin request timeout in seconds.
    #[arg(
        long,
        env = "MAGNETAR_ADMIN_TIMEOUT_SECS",
        default_value_t = 60,
        global = true
    )]
    pub(crate) admin_timeout_secs: u64,

    #[command(subcommand)]
    pub(crate) cmd: Cmd,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum Cmd {
    /// Produce a message to a topic.
    Produce {
        /// Topic (e.g. `persistent://public/default/orders`).
        topic: String,
        /// Inline message payload. Reads from stdin if absent.
        #[arg(long)]
        message: Option<String>,
        /// Optional routing key (sets `partition_key`).
        #[arg(long)]
        key: Option<String>,
        /// Optional property in `key=value` form. Repeatable.
        #[arg(long = "property", value_parser = parse_property)]
        properties: Vec<(String, String)>,
        /// Send N copies of the same payload (useful for smoke tests).
        #[arg(long, default_value_t = 1)]
        count: usize,
    },
    /// Consume from a topic.
    Consume {
        /// Topic (e.g. `persistent://public/default/orders`).
        topic: String,
        /// Subscription name.
        #[arg(long)]
        subscription: String,
        /// Subscription type: `exclusive`, `shared`, `failover`, `key-shared`.
        #[arg(long, default_value = "exclusive", value_parser = parse_sub_type)]
        sub_type: SubType,
        /// Number of messages to receive before exiting.
        #[arg(long, default_value_t = 1)]
        count: usize,
        /// Acknowledge each received message before printing the next.
        #[arg(long, default_value_t = true)]
        ack: bool,
        /// PIP-33: mark this subscription as replicated. The broker
        /// synchronises the cursor position across geo-replicated peer
        /// clusters at ~1s granularity, so a failover consumer resumes
        /// near its previous position. **Requires broker-side geo-
        /// replication + `namespace replicated_subscription_status=true`**;
        /// against a single-cluster broker the flag is silently ignored.
        /// See `docs/replicated-subscriptions.md`.
        #[arg(long, default_value_t = false)]
        replicate_subscription_state: bool,
    },
    /// Admin commands (`/admin/v2/...`).
    Admin {
        #[command(subcommand)]
        sub: AdminCmd,
    },
    /// Shadow-topic commands (PIP-180 / ADR-0033).
    ///
    /// Create, delete, or list shadow topics on the broker. A shadow topic
    /// shares its ledger storage with a source topic and exposes a
    /// read-only view of every entry to consumers — a lightweight fan-out
    /// alternative to geo-replication. See `docs/shadow-topic.md`.
    Shadow {
        #[command(subcommand)]
        sub: ShadowCmd,
    },
    /// **Experimental** (PIP-460 / ADR-0031). Print a scalable topic's current
    /// segment DAG. Resolves a `topic://...` URL against the controller broker
    /// and prints each segment's id, key range, state, and broker URL.
    /// Requires a Pulsar 5.0+ broker with PIP-460 enabled (no broker ships it
    /// today — see `docs/scalable-topics.md`).
    #[cfg(feature = "scalable-topics")]
    TopicInfo {
        /// Scalable topic URL (`topic://tenant/namespace/topic`).
        topic: String,
    },
}

/// `shadow` subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum ShadowCmd {
    /// Create a shadow topic on top of a source topic.
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`.
    Create {
        /// Source topic (`[persistent://]tenant/namespace/topic`).
        source: String,
        /// Shadow topic (`persistent://tenant/namespace/topic`).
        shadow: String,
    },
    /// Delete a shadow topic.
    /// `DELETE /admin/v2/persistent/{tenant}/{namespace}/{shadow}`.
    Delete {
        /// Shadow topic (`[persistent://]tenant/namespace/topic`).
        shadow: String,
        /// Force-delete (kicks off connected subscribers).
        #[arg(long)]
        force: bool,
    },
    /// List the shadow topics created on a source topic.
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`.
    List {
        /// Source topic (`[persistent://]tenant/namespace/topic`).
        source: String,
    },
    /// Resolve the source topic of a shadow topic.
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{shadow}/shadowSource`.
    Source {
        /// Shadow topic (`[persistent://]tenant/namespace/topic`).
        shadow: String,
    },
}

/// `admin` subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum AdminCmd {
    /// List clusters.
    ClusterList,
    /// List tenants.
    TenantList,
    /// Create a tenant.
    TenantCreate {
        /// Tenant name.
        name: String,
        /// Admin roles. Repeat the flag for multiple values.
        #[arg(long = "admin-role")]
        admin_role: Vec<String>,
        /// Allowed clusters. Repeat the flag for multiple values.
        #[arg(long = "cluster")]
        cluster: Vec<String>,
    },
    /// Delete a tenant.
    TenantDelete {
        /// Tenant name.
        name: String,
    },
    /// List namespaces under a tenant.
    NamespaceList {
        /// Tenant name.
        tenant: String,
    },
    /// Create a namespace.
    NamespaceCreate {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Delete a namespace.
    NamespaceDelete {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// List persistent topics in a namespace.
    TopicList {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Create a partitioned topic.
    TopicCreate {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
        /// Number of partitions.
        #[arg(long)]
        partitions: u32,
    },
    /// Delete a partitioned topic.
    TopicDelete {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
        /// Force-delete (drops connected producers/consumers).
        #[arg(long)]
        force: bool,
    },
    /// Get topic stats. Auto-detects partitioned topics: a single
    /// `GET .../partitions` probe routes the request to `partitioned-stats`
    /// when the topic has `partitions > 0`, otherwise to plain `stats`. The
    /// aggregated counters surface either way; for per-partition detail call
    /// `topic-stats` against each `<topic>-partition-N`.
    TopicStats {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("magnetar: failed to start tokio runtime: {err}");
            return ExitCode::from(1);
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("magnetar: {err}");
            ExitCode::from(1)
        }
    }
}

fn init_tracing(verbose: u8) {
    let default = match verbose {
        0 => "magnetar=info",
        1 => "magnetar=debug",
        2 => "magnetar=trace",
        _ => "magnetar=trace,reqwest=debug",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

async fn run(cli: Cli) -> Result<(), CliError> {
    let service_url = cli.service_url.clone();
    let token_for_data = cli.token.clone();
    match cli.cmd {
        Cmd::Produce {
            topic,
            message,
            key,
            properties,
            count,
        } => {
            run_produce(
                &service_url,
                token_for_data,
                &topic,
                message,
                key,
                properties,
                count,
            )
            .await
        }
        Cmd::Consume {
            topic,
            subscription,
            sub_type,
            count,
            ack,
            replicate_subscription_state,
        } => {
            run_consume(
                &service_url,
                token_for_data,
                &topic,
                &subscription,
                sub_type,
                count,
                ack,
                replicate_subscription_state,
            )
            .await
        }
        Cmd::Admin { sub } => {
            run_admin(&cli.admin_url, cli.token, cli.admin_timeout_secs, sub).await
        }
        Cmd::Shadow { sub } => {
            run_shadow(&cli.admin_url, cli.token, cli.admin_timeout_secs, sub).await
        }
        #[cfg(feature = "scalable-topics")]
        Cmd::TopicInfo { topic } => run_topic_info(&service_url, token_for_data, &topic).await,
    }
}

/// **Experimental** (PIP-460 / ADR-0031). Resolve a scalable topic's segment
/// DAG and print it as a table. Wraps
/// [`magnetar::PulsarClient::lookup_scalable_topic`].
// Width-formatted string-literal column headers are the idiomatic CLI table
// shape; `print_literal` would have us synthesise owned `String`s for no gain.
#[allow(clippy::print_literal)]
#[cfg(feature = "scalable-topics")]
async fn run_topic_info(
    service_url: &str,
    token: Option<String>,
    topic: &str,
) -> Result<(), CliError> {
    if !magnetar::runtime_tokio::is_scalable_topic_url(topic) {
        return Err(CliError::BadArg(format!(
            "topic-info expects a scalable `topic://...` URL, got `{topic}`"
        )));
    }
    let client = build_data_client(service_url, token.as_deref()).await?;
    let lookup = client
        .lookup_scalable_topic(topic)
        .await
        .map_err(|e| CliError::BadArg(format!("scalable lookup failed: {e}")))?;
    println!("topic: {topic}");
    println!("controller-broker: {}", lookup.controller_broker_url);
    println!("lookup-token: {}", lookup.lookup_token);
    println!(
        "{:<10} {:<18} {:<10} BROKER",
        "SEGMENT", "KEY-RANGE", "STATE"
    );
    for seg in &lookup.segments {
        let state = format!("{:?}", seg.state);
        println!(
            "{:<10} [{:>5},{:>5}) {state:<10} {}",
            seg.segment_id.0, seg.key_range.start, seg.key_range.end, seg.broker_url,
        );
    }
    println!("({} segment(s))", lookup.segments.len());
    Ok(())
}

/// PIP-180 / ADR-0033: dispatch shadow-topic subcommands over the admin
/// REST client. Wraps `magnetar_admin::AdminClient::{create,delete,
/// get_shadow_topics, get_shadow_source}`.
async fn run_shadow(
    admin_url: &str,
    token: Option<String>,
    timeout_secs: u64,
    cmd: ShadowCmd,
) -> Result<(), CliError> {
    let admin = build_admin(admin_url, token, timeout_secs)?;
    match cmd {
        ShadowCmd::Create { source, shadow } => {
            admin.create_shadow_topic(&source, &shadow).await?;
            Ok(())
        }
        ShadowCmd::Delete { shadow, force } => {
            admin.delete_shadow_topic(&shadow, force).await?;
            Ok(())
        }
        ShadowCmd::List { source } => print_json(&admin.get_shadow_topics(&source).await?),
        ShadowCmd::Source { shadow } => print_json(&admin.get_shadow_source(&shadow).await?),
    }
}

async fn run_admin(
    admin_url: &str,
    token: Option<String>,
    timeout_secs: u64,
    cmd: AdminCmd,
) -> Result<(), CliError> {
    let admin = build_admin(admin_url, token, timeout_secs)?;
    match cmd {
        AdminCmd::ClusterList => print_json(&admin.cluster_list().await?),
        AdminCmd::TenantList => print_json(&admin.tenants_list().await?),
        AdminCmd::TenantCreate {
            name,
            admin_role,
            cluster,
        } => {
            admin
                .tenant_create(
                    &name,
                    TenantInfo {
                        admin_roles: admin_role,
                        allowed_clusters: cluster,
                    },
                )
                .await?;
            Ok(())
        }
        AdminCmd::TenantDelete { name } => {
            admin.tenant_delete(&name).await?;
            Ok(())
        }
        AdminCmd::NamespaceList { tenant } => print_json(&admin.namespaces_list(&tenant).await?),
        AdminCmd::NamespaceCreate { namespace } => {
            admin.namespace_create(&namespace).await?;
            Ok(())
        }
        AdminCmd::NamespaceDelete { namespace } => {
            admin.namespace_delete(&namespace).await?;
            Ok(())
        }
        AdminCmd::TopicList { namespace } => print_json(&admin.topics_list(&namespace).await?),
        AdminCmd::TopicCreate { topic, partitions } => {
            admin.topic_create_partitioned(&topic, partitions).await?;
            Ok(())
        }
        AdminCmd::TopicDelete { topic, force } => {
            admin.topic_delete(&topic, force).await?;
            Ok(())
        }
        AdminCmd::TopicStats { topic } => {
            // The broker has two endpoints — `stats` for non-partitioned topics
            // and `partitioned-stats` for the partitioned parent name. Probe
            // the partition count first and dispatch; a non-partitioned topic
            // returns `partitions: 0` here.
            let partitions = admin.topic_partitions_count(&topic).await?;
            let stats = if partitions > 0 {
                admin.topic_partitioned_stats(&topic).await?
            } else {
                admin.topic_stats(&topic).await?
            };
            // `TopicStats` derives `Deserialize` but not `Serialize` (it is
            // permissive); re-emit it via a manual JSON object so the CLI
            // output is human-friendly.
            let json = serde_json::json!({
                "partitions": partitions,
                "msgInCounter": stats.msg_in_counter,
                "bytesInCounter": stats.bytes_in_counter,
                "publishers": stats.publishers,
                "subscriptions": stats.subscriptions,
            });
            print_json(&json)
        }
    }
}

fn build_admin(
    admin_url: &str,
    token: Option<String>,
    timeout_secs: u64,
) -> Result<AdminClient, CliError> {
    let url = admin_url
        .parse()
        .map_err(|err: url::ParseError| CliError::BadArg(format!("--admin-url: {err}")))?;
    let mut builder: AdminClientBuilder = AdminClient::builder()
        .service_url(url)
        .timeout(Duration::from_secs(timeout_secs));
    if let Some(tok) = token {
        builder = builder.token(tok);
    }
    Ok(builder.build()?)
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), CliError> {
    let s = serde_json::to_string_pretty(value)?;
    println!("{s}");
    Ok(())
}

/// Errors surfaced from the CLI run loop.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    /// Underlying admin client failure.
    #[error("{0}")]
    Admin(#[from] AdminError),
    /// Bad CLI argument that clap could not catch.
    #[error("bad argument: {0}")]
    BadArg(String),
    /// JSON serialization failure (for stdout output).
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Underlying magnetar (data-plane) façade failure.
    #[error("{0}")]
    Pulsar(#[from] magnetar::PulsarError),
    /// Underlying tokio engine failure (producer/consumer ops).
    #[error("{0}")]
    Client(#[from] ClientError),
    /// I/O error while reading stdin or writing stdout.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

fn parse_property(spec: &str) -> Result<(String, String), String> {
    let (k, v) = spec
        .split_once('=')
        .ok_or_else(|| format!("expected key=value, got `{spec}`"))?;
    Ok((k.to_owned(), v.to_owned()))
}

fn parse_sub_type(s: &str) -> Result<SubType, String> {
    match s.to_ascii_lowercase().as_str() {
        "exclusive" => Ok(SubType::Exclusive),
        "shared" => Ok(SubType::Shared),
        "failover" => Ok(SubType::Failover),
        "key-shared" | "keyshared" | "key_shared" => Ok(SubType::KeyShared),
        other => Err(format!(
            "unknown subscription type `{other}` (expected: exclusive | shared | failover | key-shared)"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_produce(
    service_url: &str,
    token: Option<String>,
    topic: &str,
    message: Option<String>,
    key: Option<String>,
    properties: Vec<(String, String)>,
    count: usize,
) -> Result<(), CliError> {
    let payload = if let Some(s) = message {
        s.into_bytes()
    } else {
        use std::io::Read;
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf)?;
        buf
    };

    let client = build_data_client(service_url, token.as_deref()).await?;
    let producer = client.producer(topic).create().await?;

    for idx in 0..count {
        let mut msg = OutgoingMessage::with_payload(payload.clone());
        if let Some(k) = key.as_deref() {
            msg = msg.key(k);
        }
        for (k, v) in &properties {
            msg = msg.property(k, v);
        }
        let receipt = producer.send(msg.into()).await?;
        println!(
            "produced #{idx} -> ledger={} entry={} partition={} batch_index={}",
            receipt.ledger_id, receipt.entry_id, receipt.partition, receipt.batch_index,
        );
    }
    producer.close().await?;
    client.close().await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_consume(
    service_url: &str,
    token: Option<String>,
    topic: &str,
    subscription: &str,
    sub_type: SubType,
    count: usize,
    ack: bool,
    replicate_subscription_state: bool,
) -> Result<(), CliError> {
    let client = build_data_client(service_url, token.as_deref()).await?;
    let consumer = client
        .consumer(topic)
        .subscription(subscription)
        .subscription_type(sub_type)
        .replicate_subscription_state(replicate_subscription_state)
        .subscribe()
        .await?;

    for idx in 0..count {
        let msg = consumer.receive().await?;
        let payload = String::from_utf8_lossy(&msg.payload);
        println!(
            "received #{idx} id=(ledger={} entry={} partition={} batch_index={}) payload={}",
            msg.message_id.ledger_id,
            msg.message_id.entry_id,
            msg.message_id.partition,
            msg.message_id.batch_index,
            payload,
        );
        if ack {
            consumer.ack(msg.message_id).await?;
        }
    }
    consumer.close().await?;
    client.close().await;
    Ok(())
}

async fn build_data_client(
    service_url: &str,
    token: Option<&str>,
) -> Result<PulsarClient, CliError> {
    let mut builder = PulsarClient::builder().service_url(service_url);
    if let Some(t) = token {
        let provider = std::sync::Arc::new(TokenAuth::from_string(t.to_owned()));
        builder = builder.auth(provider);
    }
    Ok(builder.build().await?)
}
