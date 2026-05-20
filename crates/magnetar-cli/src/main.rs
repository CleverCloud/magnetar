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

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use magnetar_admin::{AdminClient, AdminClientBuilder, AdminError, TenantInfo};

/// magnetar — produce, consume, inspect, and admin against an Apache Pulsar broker.
#[derive(Debug, Parser)]
#[command(name = "magnetar", version, about, long_about = None)]
pub(crate) struct Cli {
    /// Increase logging verbosity (-v, -vv, -vvv).
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub(crate) verbose: u8,

    /// Pulsar service URL for data-plane (`pulsar://` / `pulsar+ssl://`).
    #[arg(
        long,
        env = "MAGNETAR_SERVICE_URL",
        default_value = "pulsar://localhost:6650"
    )]
    pub(crate) service_url: String,

    /// Pulsar admin REST URL (`http://` / `https://`).
    #[arg(
        long,
        env = "MAGNETAR_ADMIN_URL",
        default_value = "http://localhost:8080"
    )]
    pub(crate) admin_url: String,

    /// Bearer token for admin auth. Reads from `MAGNETAR_TOKEN` if unset.
    #[arg(long, env = "MAGNETAR_TOKEN")]
    pub(crate) token: Option<String>,

    /// Admin request timeout in seconds.
    #[arg(long, env = "MAGNETAR_ADMIN_TIMEOUT_SECS", default_value_t = 60)]
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
        /// Inline message payload.
        #[arg(long)]
        message: Option<String>,
    },
    /// Consume from a topic.
    Consume {
        /// Topic (e.g. `persistent://public/default/orders`).
        topic: String,
        /// Subscription name.
        #[arg(long)]
        subscription: String,
        /// Number of messages to receive before exiting.
        #[arg(long, default_value = "1")]
        count: usize,
    },
    /// Admin commands (`/admin/v2/...`).
    Admin {
        #[command(subcommand)]
        sub: AdminCmd,
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
    /// Get topic stats.
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
    match cli.cmd {
        Cmd::Produce { topic, message: _ } => {
            println!("produce {topic}: not yet wired (M9)");
            Ok(())
        }
        Cmd::Consume {
            topic,
            subscription,
            count,
        } => {
            println!("consume {topic} sub={subscription} count={count}: not yet wired (M9)");
            Ok(())
        }
        Cmd::Admin { sub } => {
            run_admin(&cli.admin_url, cli.token, cli.admin_timeout_secs, sub).await
        }
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
            let stats = admin.topic_stats(&topic).await?;
            // `TopicStats` derives `Deserialize` but not `Serialize` (it is
            // permissive); re-emit it via a manual JSON object so the CLI
            // output is human-friendly.
            let json = serde_json::json!({
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
}
