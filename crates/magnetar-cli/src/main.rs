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

// The user-facing `magnetar` binary always needs TLS — both the admin
// REST client (reqwest + rustls) and the data-plane runtime
// (`magnetar-runtime-tokio` + tokio-rustls) bind a crypto provider at
// compile time. Mirror the ADR-0035 guard from
// `magnetar-runtime-tokio::tls_crypto` so a build with no provider
// selected fails fast at compile time instead of silently shipping a
// half-broken binary (admin HTTPS dead, runtime TLS only working via
// `magnetar-runtime-tokio`'s own default). The admin library crate
// keeps its no-TLS stub for HTTP-only library callers — this gate is
// the binary's responsibility.
#[cfg(not(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
)))]
compile_error!(
    "magnetar-cli: enable at least one of crypto-{aws-lc-rs,ring,openssl,fips}. \
     The default feature set covers this; only `--no-default-features` users \
     need to pick one explicitly."
);

mod version;

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use magnetar::proto::TokenAuth;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::runtime_tokio::ClientError;
use magnetar::{MessageId, OutgoingMessage, PulsarClient};
use magnetar_admin::{
    AdminClient, AdminClientBuilder, AdminError, BacklogQuota, BacklogQuotaType, BookieInfo,
    DelayedDeliveryPolicies, DispatchRate, FunctionConfig, PersistencePolicies, PostSchemaPayload,
    PublishRate, RetentionPolicies, SinkConfig, SourceConfig, TenantInfo, split_function_id,
};

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
    /// Admin commands (`/admin/v2/...`). Grouped by resource — clusters,
    /// tenants, namespaces, topics — following pulsarctl / kubectl
    /// conventions. Shadow-topic (PIP-180 / ADR-0033) management lives
    /// under `admin topics shadow`.
    Admin {
        #[command(subcommand)]
        sub: AdminCmd,
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

/// `admin` subcommands — grouped by resource. The nested layout matches
/// pulsarctl (`pulsarctl topics stats`) and kubectl (`kubectl pods get`)
/// rather than the older flat shape (`admin topic-stats`).
#[derive(Debug, Subcommand)]
pub(crate) enum AdminCmd {
    /// Cluster-level operations (`/admin/v2/clusters/...`).
    Clusters {
        #[command(subcommand)]
        sub: ClustersCmd,
    },
    /// Tenant CRUD (`/admin/v2/tenants/...`).
    Tenants {
        #[command(subcommand)]
        sub: TenantsCmd,
    },
    /// Namespace CRUD + policies (`/admin/v2/namespaces/...`).
    Namespaces {
        #[command(subcommand)]
        sub: NamespacesCmd,
    },
    /// Topic CRUD + stats + ops (`/admin/v2/persistent/...`). Shadow-topic
    /// (PIP-180) management lives under `admin topics shadow`.
    Topics {
        #[command(subcommand)]
        sub: TopicsCmd,
    },
    /// Subscription operations on a topic
    /// (`/admin/v2/persistent/.../{topic}/subscription/...`).
    Subscriptions {
        #[command(subcommand)]
        sub: SubscriptionsCmd,
    },
    /// Broker diagnostics (`/admin/v2/brokers/...`).
    Brokers {
        #[command(subcommand)]
        sub: BrokersCmd,
    },
    /// Bookie metadata + rack-aware placement (`/admin/v2/bookies/...`).
    Bookies {
        #[command(subcommand)]
        sub: BookiesCmd,
    },
    /// Schema-registry operations (`/admin/v2/schemas/...`).
    Schemas {
        #[command(subcommand)]
        sub: SchemasCmd,
    },
    /// Pulsar Functions management (`/admin/v3/functions/...`). The
    /// URL-based register / update path is supported; multipart JAR
    /// uploads from a local file are intentionally out of scope.
    Functions {
        #[command(subcommand)]
        sub: FunctionsCmd,
    },
    /// Pulsar IO Sources (`/admin/v3/sources/...`) — pull data INTO
    /// topics from external systems.
    Sources {
        #[command(subcommand)]
        sub: SourcesCmd,
    },
    /// Pulsar IO Sinks (`/admin/v3/sinks/...`) — push topic data OUT
    /// to external systems.
    Sinks {
        #[command(subcommand)]
        sub: SinksCmd,
    },
}

/// `admin clusters <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum ClustersCmd {
    /// List clusters.
    List,
    /// List failure-domains configured on a cluster.
    /// `GET /admin/v2/clusters/{cluster}/failureDomains`.
    ListFailureDomains {
        /// Cluster name.
        cluster: String,
    },
    /// Get one failure-domain by name.
    /// `GET /admin/v2/clusters/{cluster}/failureDomains/{domain}`.
    GetFailureDomain {
        /// Cluster name.
        cluster: String,
        /// Failure-domain name.
        domain: String,
    },
    /// List namespace-isolation policies on a cluster.
    /// `GET /admin/v2/clusters/{cluster}/namespaceIsolationPolicies`.
    ListNamespaceIsolationPolicies {
        /// Cluster name.
        cluster: String,
    },
}

/// `admin brokers <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum BrokersCmd {
    /// List active brokers in a cluster.
    /// `GET /admin/v2/brokers/{cluster}`.
    List {
        /// Cluster name.
        cluster: String,
    },
    /// Get the current cluster-level leader broker.
    /// `GET /admin/v2/brokers/leaderBroker`.
    Leader,
    /// List the names of every dynamic broker configuration key.
    /// `GET /admin/v2/brokers/configuration`.
    DynamicConfigKeys,
    /// Get every dynamic-config override the operator has set
    /// (static / default values stay out of the map).
    /// `GET /admin/v2/brokers/configuration/values`.
    DynamicConfigOverrides,
    /// Get the broker's runtime (static + dynamic) configuration.
    /// `GET /admin/v2/brokers/configuration/runtime`.
    RuntimeConfig,
    /// Get the broker's internal-stack endpoints (metadata-store
    /// URLs, BookKeeper service URI, ledger root paths).
    /// `GET /admin/v2/brokers/internal-configuration`.
    InternalConfig,
    /// Probe broker health — produces and consumes one heartbeat on
    /// an internal topic. Prints the broker's `"ok"` body on success.
    /// `GET /admin/v2/brokers/health`.
    HealthCheck,
    /// List the namespaces a specific broker currently owns.
    /// `GET /admin/v2/brokers/{cluster}/{broker}/ownedNamespaces`.
    OwnedNamespaces {
        /// Cluster name.
        cluster: String,
        /// Broker `host:port` (matches `brokers list` output).
        broker: String,
    },
    /// Override a dynamic configuration value.
    /// `POST /admin/v2/brokers/configuration/{name}/{value}`.
    SetDynamicConfig {
        /// Configuration key name.
        name: String,
        /// New value (sent in the URL path).
        value: String,
    },
    /// Drop a dynamic configuration override and revert to the
    /// static / default value.
    /// `DELETE /admin/v2/brokers/configuration/{name}`.
    DeleteDynamicConfig {
        /// Configuration key name.
        name: String,
    },
}

/// `admin bookies <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum BookiesCmd {
    /// List every bookie the broker knows about (writable +
    /// read-only) as registered in BookKeeper metadata.
    /// `GET /admin/v2/bookies/all`.
    List,
    /// Get every bookie's group + rack assignment.
    /// `GET /admin/v2/bookies/racks-info`.
    RacksInfo,
    /// Set (or update) a bookie's rack assignment.
    /// `POST /admin/v2/bookies/racks-info/{bookie}`.
    SetRack {
        /// Bookie `host:port` (matches BookKeeper metadata).
        bookie: String,
        /// Placement group.
        #[arg(long, default_value = "default")]
        group: String,
        /// Rack identifier within the group.
        #[arg(long)]
        rack: String,
        /// Resolved hostname for the bookie. The broker uses it for
        /// log lines; defaults to the bookie address if unset.
        #[arg(long)]
        hostname: Option<String>,
    },
    /// Remove a bookie's rack assignment.
    /// `DELETE /admin/v2/bookies/racks-info/{bookie}`.
    DeleteRack {
        /// Bookie `host:port`.
        bookie: String,
    },
}

/// `admin schemas <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum SchemasCmd {
    /// Get the latest schema attached to a topic.
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema`.
    GetLatest {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
    },
    /// Get a specific schema version.
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema/{version}`.
    GetVersion {
        /// Fully qualified topic.
        topic: String,
        /// Schema version (broker-assigned integer).
        #[arg(long)]
        version: i64,
    },
    /// List every registered schema version.
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schemas`.
    ListVersions {
        /// Fully qualified topic.
        topic: String,
    },
    /// Register a new schema version.
    /// `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/schema`.
    Post {
        /// Fully qualified topic.
        topic: String,
        /// Schema type (`AVRO` / `JSON` / `PROTOBUF` /
        /// `PROTOBUF_NATIVE` / `KEY_VALUE` / `STRING` / `BYTES` / ...).
        #[arg(long = "type")]
        schema_type: String,
        /// Schema definition (canonical-form blob).
        #[arg(long)]
        schema: String,
        /// User-defined property in `key=value` form. Repeatable.
        #[arg(long = "property", value_parser = parse_property)]
        properties: Vec<(String, String)>,
    },
    /// Delete a topic's schema.
    /// `DELETE /admin/v2/schemas/{tenant}/{ns}/{topic}/schema?force={force}`.
    Delete {
        /// Fully qualified topic.
        topic: String,
        /// Skip the "is the schema in use" guard.
        #[arg(long)]
        force: bool,
    },
    /// Check whether a candidate schema is compatible with the
    /// topic's current schema.
    /// `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/compatibility`.
    Compatibility {
        /// Fully qualified topic.
        topic: String,
        /// Schema type.
        #[arg(long = "type")]
        schema_type: String,
        /// Schema definition (canonical-form blob).
        #[arg(long)]
        schema: String,
        /// User-defined property in `key=value` form. Repeatable.
        #[arg(long = "property", value_parser = parse_property)]
        properties: Vec<(String, String)>,
    },
}

/// `admin functions <verb>` — Pulsar Functions management
/// (`/admin/v3/functions/...`).
///
/// Read verbs (`list` / `get` / `status` / `stats`) print the broker's
/// JSON envelope verbatim because the upstream `FunctionConfig` and
/// `FunctionStatus` shapes grow on every minor release.
///
/// Write verbs accept the fully qualified `tenant/namespace/name`
/// triple as a single positional argument (mirroring `pulsarctl
/// functions <verb> NAME --tenant ... --namespace ...` but without the
/// flag duplication); the parser sits in `magnetar_admin` as
/// `split_function_id`.
#[derive(Debug, Subcommand)]
pub(crate) enum FunctionsCmd {
    /// List every function under a namespace.
    /// `GET /admin/v3/functions/{tenant}/{namespace}`.
    List {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Get a function's configuration.
    /// `GET /admin/v3/functions/{tenant}/{namespace}/{name}`.
    Get {
        /// Fully qualified function name (`tenant/namespace/name`).
        name: String,
    },
    /// Get a function's status (aggregate or per-instance).
    /// `GET /admin/v3/functions/{tenant}/{namespace}/{name}[/{instance_id}]/status`.
    Status {
        /// Fully qualified function name (`tenant/namespace/name`).
        name: String,
        /// Restrict to one instance (`0..parallelism`).
        #[arg(long)]
        instance_id: Option<i32>,
    },
    /// Get a function's runtime statistics (aggregate or per-instance).
    /// `GET /admin/v3/functions/{tenant}/{namespace}/{name}[/{instance_id}]/stats`.
    Stats {
        /// Fully qualified function name.
        name: String,
        /// Restrict to one instance (`0..parallelism`).
        #[arg(long)]
        instance_id: Option<i32>,
    },
    /// Register a function from a remote package URL.
    /// `POST /admin/v3/functions/{tenant}/{namespace}/{name}` (multipart).
    CreateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace (bare name, not `tenant/namespace`).
        #[arg(long)]
        namespace: String,
        /// Function name (unique within the namespace).
        #[arg(long)]
        name: String,
        /// Broker-resolvable URL of the compiled package (HTTP / S3 /
        /// GCS / `function://` / `file://`).
        #[arg(long)]
        url: String,
        /// Entry-point class name (`com.acme.MyFunction` for Java).
        #[arg(long = "class-name")]
        class_name: String,
        /// Runtime — `JAVA`, `PYTHON`, or `GO`.
        #[arg(long, default_value = "JAVA")]
        runtime: String,
        /// Input topic the function subscribes to. Repeat for multiple.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Output topic the function produces to.
        #[arg(long, default_value = "")]
        output: String,
        /// Number of parallel instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Update an existing function from a remote package URL.
    /// `PUT /admin/v3/functions/{tenant}/{namespace}/{name}` (multipart).
    UpdateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace.
        #[arg(long)]
        namespace: String,
        /// Function name.
        #[arg(long)]
        name: String,
        /// Broker-resolvable package URL.
        #[arg(long)]
        url: String,
        /// Entry-point class name.
        #[arg(long = "class-name")]
        class_name: String,
        /// Runtime — `JAVA`, `PYTHON`, or `GO`.
        #[arg(long, default_value = "JAVA")]
        runtime: String,
        /// Input topic. Repeat for multiple.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Output topic.
        #[arg(long, default_value = "")]
        output: String,
        /// Number of parallel instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Deregister (delete) a function.
    /// `DELETE /admin/v3/functions/{tenant}/{namespace}/{name}`.
    Delete {
        /// Fully qualified function name.
        name: String,
    },
    /// Start a function (aggregate or per-instance).
    /// `POST /admin/v3/functions/{tenant}/{namespace}/{name}[/{instance_id}]/start`.
    Start {
        /// Fully qualified function name.
        name: String,
        /// Restrict to one instance.
        #[arg(long)]
        instance_id: Option<i32>,
    },
    /// Stop a function (aggregate or per-instance).
    /// `POST /admin/v3/functions/{tenant}/{namespace}/{name}[/{instance_id}]/stop`.
    Stop {
        /// Fully qualified function name.
        name: String,
        /// Restrict to one instance.
        #[arg(long)]
        instance_id: Option<i32>,
    },
    /// Restart every instance of a function.
    /// `POST /admin/v3/functions/{tenant}/{namespace}/{name}/restart`.
    Restart {
        /// Fully qualified function name.
        name: String,
    },
}

/// `admin sources <verb>` — Pulsar IO Sources.
///
/// `list` / `get` / `status` accept positional `tenant/namespace` or
/// `tenant/namespace/name`; `create-with-url` / `update-with-url` take
/// each segment as a flag for read-clarity around the multi-flag
/// connector config.
#[derive(Debug, Subcommand)]
pub(crate) enum SourcesCmd {
    /// List sources in a namespace.
    /// `GET /admin/v3/sources/{tenant}/{namespace}`.
    List {
        /// `tenant/namespace`.
        namespace: String,
    },
    /// Get a source's configuration.
    /// `GET /admin/v3/sources/{tenant}/{namespace}/{name}`.
    Get {
        /// `tenant/namespace/name`.
        source: String,
    },
    /// Get a source's running status.
    /// `GET /admin/v3/sources/{tenant}/{namespace}/{name}/status`.
    Status {
        /// `tenant/namespace/name`.
        source: String,
    },
    /// Register a source from a remote package URL.
    /// `POST /admin/v3/sources/{tenant}/{namespace}/{name}`.
    CreateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace.
        #[arg(long)]
        namespace: String,
        /// Source name.
        #[arg(long)]
        name: String,
        /// Package URL (`http(s)://`, `file://`, or `function://`).
        #[arg(long)]
        url: String,
        /// Fully-qualified connector class
        /// (e.g. `org.apache.pulsar.io.kafka.KafkaSource`).
        #[arg(long)]
        class_name: String,
        /// Destination topic the source writes to.
        #[arg(long)]
        topic_name: String,
        /// Number of source instances to schedule.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Update a source from a remote package URL.
    /// `PUT /admin/v3/sources/{tenant}/{namespace}/{name}`.
    UpdateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace.
        #[arg(long)]
        namespace: String,
        /// Source name.
        #[arg(long)]
        name: String,
        /// Package URL.
        #[arg(long)]
        url: String,
        /// Fully-qualified connector class.
        #[arg(long)]
        class_name: String,
        /// Destination topic.
        #[arg(long)]
        topic_name: String,
        /// Number of source instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Delete a source.
    /// `DELETE /admin/v3/sources/{tenant}/{namespace}/{name}`.
    Delete {
        /// `tenant/namespace/name`.
        source: String,
    },
    /// Start every instance of a source.
    /// `POST /admin/v3/sources/{tenant}/{namespace}/{name}/start`.
    Start {
        /// `tenant/namespace/name`.
        source: String,
    },
    /// Stop every instance of a source.
    /// `POST /admin/v3/sources/{tenant}/{namespace}/{name}/stop`.
    Stop {
        /// `tenant/namespace/name`.
        source: String,
    },
    /// Restart every instance of a source.
    /// `POST /admin/v3/sources/{tenant}/{namespace}/{name}/restart`.
    Restart {
        /// `tenant/namespace/name`.
        source: String,
    },
}

/// `admin sinks <verb>` — Pulsar IO Sinks.
///
/// Mirrors [`SourcesCmd`] exactly except for `--input` (repeatable on
/// `create-with-url` / `update-with-url`) replacing `--topic-name`.
#[derive(Debug, Subcommand)]
pub(crate) enum SinksCmd {
    /// List sinks in a namespace.
    /// `GET /admin/v3/sinks/{tenant}/{namespace}`.
    List {
        /// `tenant/namespace`.
        namespace: String,
    },
    /// Get a sink's configuration.
    /// `GET /admin/v3/sinks/{tenant}/{namespace}/{name}`.
    Get {
        /// `tenant/namespace/name`.
        sink: String,
    },
    /// Get a sink's running status.
    /// `GET /admin/v3/sinks/{tenant}/{namespace}/{name}/status`.
    Status {
        /// `tenant/namespace/name`.
        sink: String,
    },
    /// Register a sink from a remote package URL.
    /// `POST /admin/v3/sinks/{tenant}/{namespace}/{name}`.
    CreateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace.
        #[arg(long)]
        namespace: String,
        /// Sink name.
        #[arg(long)]
        name: String,
        /// Package URL.
        #[arg(long)]
        url: String,
        /// Fully-qualified connector class.
        #[arg(long)]
        class_name: String,
        /// Source topic the sink reads from. Repeatable.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Number of sink instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Update a sink from a remote package URL.
    /// `PUT /admin/v3/sinks/{tenant}/{namespace}/{name}`.
    UpdateWithUrl {
        /// Tenant.
        #[arg(long)]
        tenant: String,
        /// Namespace.
        #[arg(long)]
        namespace: String,
        /// Sink name.
        #[arg(long)]
        name: String,
        /// Package URL.
        #[arg(long)]
        url: String,
        /// Fully-qualified connector class.
        #[arg(long)]
        class_name: String,
        /// Source topic. Repeatable.
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Number of sink instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
    },
    /// Delete a sink.
    /// `DELETE /admin/v3/sinks/{tenant}/{namespace}/{name}`.
    Delete {
        /// `tenant/namespace/name`.
        sink: String,
    },
    /// Start every instance of a sink.
    /// `POST /admin/v3/sinks/{tenant}/{namespace}/{name}/start`.
    Start {
        /// `tenant/namespace/name`.
        sink: String,
    },
    /// Stop every instance of a sink.
    /// `POST /admin/v3/sinks/{tenant}/{namespace}/{name}/stop`.
    Stop {
        /// `tenant/namespace/name`.
        sink: String,
    },
    /// Restart every instance of a sink.
    /// `POST /admin/v3/sinks/{tenant}/{namespace}/{name}/restart`.
    Restart {
        /// `tenant/namespace/name`.
        sink: String,
    },
}

/// `admin tenants <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum TenantsCmd {
    /// List tenants.
    List,
    /// Create a tenant.
    Create {
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
    Delete {
        /// Tenant name.
        name: String,
    },
}

/// `admin namespaces <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum NamespacesCmd {
    /// List namespaces under a tenant.
    List {
        /// Tenant name.
        tenant: String,
    },
    /// Create a namespace.
    Create {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Delete a namespace.
    Delete {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Get a namespace's retention policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/retention`.
    GetRetention {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's retention policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/retention`.
    SetRetention {
        /// Fully qualified namespace.
        namespace: String,
        /// Retention time in minutes. `-1` = infinite, `0` = none.
        #[arg(long)]
        time_minutes: i32,
        /// Retention size in MB. `-1` = infinite, `0` = none.
        #[arg(long)]
        size_mb: i64,
    },
    /// Remove a namespace's retention policy (fall back to broker default).
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/retention`.
    RemoveRetention {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get all backlog-quota policies on a namespace.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/backlogQuotaMap`.
    GetBacklogQuotas {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a backlog-quota policy on a namespace.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/backlogQuota?backlogQuotaType=...`.
    SetBacklogQuota {
        /// Fully qualified namespace.
        namespace: String,
        /// Quota dimension: `destination-storage` (bytes) or `message-age` (seconds).
        #[arg(long = "type", value_parser = parse_backlog_quota_type)]
        quota_type: BacklogQuotaType,
        /// Maximum bytes for `destination-storage`. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        limit_size: i64,
        /// Maximum age in seconds for `message-age`. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        limit_time: i32,
        /// Action when the quota is exceeded — `producer_request_hold`,
        /// `producer_exception`, or `consumer_backlog_eviction`.
        #[arg(long)]
        policy: String,
    },
    /// Remove a backlog-quota policy from a namespace.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/backlogQuota?backlogQuotaType=...`.
    RemoveBacklogQuota {
        /// Fully qualified namespace.
        namespace: String,
        /// Quota dimension: `destination-storage` or `message-age`.
        #[arg(long = "type", value_parser = parse_backlog_quota_type)]
        quota_type: BacklogQuotaType,
    },
    /// Get a namespace's message-TTL (seconds, or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/messageTTL`.
    GetMessageTtl {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's message-TTL (seconds). `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/messageTTL`.
    SetMessageTtl {
        /// Fully qualified namespace.
        namespace: String,
        /// TTL in seconds.
        #[arg(long)]
        ttl_seconds: i32,
    },
    /// Remove a namespace's message-TTL (fall back to broker default).
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/messageTTL`.
    RemoveMessageTtl {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's persistence policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/persistence`.
    GetPersistence {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's persistence policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/persistence`.
    SetPersistence {
        /// Fully qualified namespace.
        namespace: String,
        /// BookKeeper ensemble size.
        #[arg(long)]
        ensemble: i32,
        /// BookKeeper write quorum.
        #[arg(long)]
        write_quorum: i32,
        /// BookKeeper ack quorum.
        #[arg(long)]
        ack_quorum: i32,
        /// Managed-ledger mark-delete-rate cap (ops/sec). `0` disables.
        #[arg(long, default_value_t = 0.0)]
        mark_delete_rate: f64,
    },
    /// Remove a namespace's persistence policy (fall back to broker default).
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/persistence`.
    RemovePersistence {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's consumer dispatch-rate policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/dispatchRate`.
    GetDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's consumer dispatch-rate policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/dispatchRate`.
    SetDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a namespace's consumer dispatch-rate policy.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/dispatchRate`.
    RemoveDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's per-subscription dispatch-rate policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`.
    GetSubscriptionDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's per-subscription dispatch-rate policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`.
    SetSubscriptionDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a namespace's per-subscription dispatch-rate policy.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`.
    RemoveSubscriptionDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's cross-cluster replicator dispatch-rate policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`.
    GetReplicatorDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's cross-cluster replicator dispatch-rate policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`.
    SetReplicatorDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a namespace's cross-cluster replicator dispatch-rate policy.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`.
    RemoveReplicatorDispatchRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's publish-rate policy.
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/publishRate`.
    GetPublishRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's publish-rate policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/publishRate`.
    SetPublishRate {
        /// Fully qualified namespace.
        namespace: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
    },
    /// Remove a namespace's publish-rate policy.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/publishRate`.
    RemovePublishRate {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's deduplication flag (or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/deduplication`.
    GetDeduplication {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's deduplication flag.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/deduplication`.
    SetDeduplication {
        /// Fully qualified namespace.
        namespace: String,
        /// Enable broker-side dedup on the namespace.
        #[arg(long)]
        enabled: bool,
    },
    /// Remove a namespace's deduplication flag (fall back to broker default).
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/deduplication`.
    RemoveDeduplication {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's deduplication-snapshot interval (or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`.
    GetDeduplicationSnapshotInterval {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's deduplication-snapshot interval (entry count).
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`.
    SetDeduplicationSnapshotInterval {
        /// Fully qualified namespace.
        namespace: String,
        /// Entry count between dedup cursor snapshots.
        #[arg(long)]
        interval_entries: i32,
    },
    /// Remove a namespace's deduplication-snapshot interval override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`.
    RemoveDeduplicationSnapshotInterval {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's compaction threshold (bytes, or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold`.
    GetCompactionThreshold {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's compaction threshold (bytes). `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold`.
    SetCompactionThreshold {
        /// Fully qualified namespace.
        namespace: String,
        /// Threshold in bytes. `0` disables automatic compaction.
        #[arg(long)]
        threshold_bytes: i64,
    },
    /// Remove a namespace's compaction threshold override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold`.
    RemoveCompactionThreshold {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's delayed-delivery policy (or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery`.
    GetDelayedDelivery {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's delayed-delivery policy.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery`.
    SetDelayedDelivery {
        /// Fully qualified namespace.
        namespace: String,
        /// Enable delayed delivery on the namespace.
        #[arg(long)]
        active: bool,
        /// Index-tick granularity in milliseconds.
        #[arg(long)]
        tick_time_millis: i64,
    },
    /// Remove a namespace's delayed-delivery policy override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery`.
    RemoveDelayedDelivery {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's max-producers-per-topic limit (or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/maxProducersPerTopic`.
    GetMaxProducersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's max-producers-per-topic limit. `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/maxProducersPerTopic`.
    SetMaxProducersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
        /// Max concurrent producers per topic. `0` disables.
        #[arg(long)]
        max_producers: i32,
    },
    /// Remove a namespace's max-producers-per-topic limit override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/maxProducersPerTopic`.
    RemoveMaxProducersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's max-consumers-per-topic limit (or `null` if unset).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/maxConsumersPerTopic`.
    GetMaxConsumersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's max-consumers-per-topic limit. `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/maxConsumersPerTopic`.
    SetMaxConsumersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
        /// Max concurrent consumers per topic. `0` disables.
        #[arg(long)]
        max_consumers: i32,
    },
    /// Remove a namespace's max-consumers-per-topic limit override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/maxConsumersPerTopic`.
    RemoveMaxConsumersPerTopic {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's max-unacked-messages-per-consumer limit (or `null`).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerConsumer`.
    GetMaxUnackedMessagesPerConsumer {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's max-unacked-messages-per-consumer limit. `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerConsumer`.
    SetMaxUnackedMessagesPerConsumer {
        /// Fully qualified namespace.
        namespace: String,
        /// Max in-flight unacked messages per consumer. `0` disables.
        #[arg(long)]
        max_unacked: i32,
    },
    /// Remove a namespace's max-unacked-messages-per-consumer override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerConsumer`.
    RemoveMaxUnackedMessagesPerConsumer {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Get a namespace's max-unacked-messages-per-subscription limit (or `null`).
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerSubscription`.
    GetMaxUnackedMessagesPerSubscription {
        /// Fully qualified namespace.
        namespace: String,
    },
    /// Set a namespace's max-unacked-messages-per-subscription limit. `0` disables.
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerSubscription`.
    SetMaxUnackedMessagesPerSubscription {
        /// Fully qualified namespace.
        namespace: String,
        /// Max unacked messages per subscription. `0` disables.
        #[arg(long)]
        max_unacked: i32,
    },
    /// Remove a namespace's max-unacked-messages-per-subscription override.
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/maxUnackedMessagesPerSubscription`.
    RemoveMaxUnackedMessagesPerSubscription {
        /// Fully qualified namespace.
        namespace: String,
    },
}

/// `admin topics <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCmd {
    /// List persistent topics in a namespace.
    List {
        /// Fully qualified namespace (`tenant/namespace`).
        namespace: String,
    },
    /// Create a partitioned topic.
    Create {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
        /// Number of partitions.
        #[arg(long)]
        partitions: u32,
    },
    /// Delete a partitioned topic.
    Delete {
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
    /// `topics stats` against each `<topic>-partition-N`.
    Stats {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
    },
    /// Trigger ledger compaction. Asynchronous — poll
    /// `topics compaction-status` to see progress.
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction`.
    Compact {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get the current compaction status (`NOT_RUN` / `RUNNING` / `SUCCESS` / `ERROR`).
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction`.
    CompactionStatus {
        /// Fully qualified topic.
        topic: String,
    },
    /// Unload a topic from its current broker — forces rebalancing.
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/unload`.
    Unload {
        /// Fully qualified topic.
        topic: String,
    },
    /// Terminate (seal) a topic. Returns the `MessageId` of the last
    /// message that landed before the seal.
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/terminate`.
    Terminate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Grow a partitioned topic's partition count. Only forward growth is
    /// supported; the broker returns 409 on shrink.
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`.
    UpdatePartitions {
        /// Fully qualified topic.
        topic: String,
        /// New partition count (must be > current).
        #[arg(long)]
        partitions: u32,
    },
    /// Resolve a broker-entry-metadata index to a `MessageId` (PIP-415).
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/getMessageIdByIndex?index={index}`.
    /// Requires the broker to have `brokerEntryMetadataInterceptors`
    /// configured with `AppendIndexMetadataInterceptor`; otherwise the
    /// broker returns 404 / 400. The Java `MessageIdImpl` cannot represent
    /// negative `ledgerId` values either, so a broker that returns one
    /// surfaces as `AdminError::Protocol`.
    GetMessageIdByIndex {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
        /// Broker-entry index to resolve.
        #[arg(long)]
        index: i64,
    },
    /// Get a topic's retention policy.
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/retention`.
    GetRetention {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's retention policy (overrides namespace default).
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/retention`.
    SetRetention {
        /// Fully qualified topic.
        topic: String,
        /// Retention time in minutes. `-1` = infinite, `0` = none.
        #[arg(long)]
        time_minutes: i32,
        /// Retention size in MB. `-1` = infinite, `0` = none.
        #[arg(long)]
        size_mb: i64,
    },
    /// Remove a topic's retention policy (fall back to namespace default).
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/retention`.
    RemoveRetention {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get all backlog-quota policies on a topic.
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuotaMap`.
    GetBacklogQuotas {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a backlog-quota policy on a topic.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuota?backlogQuotaType=...`.
    SetBacklogQuota {
        /// Fully qualified topic.
        topic: String,
        /// Quota dimension: `destination-storage` (bytes) or `message-age` (seconds).
        #[arg(long = "type", value_parser = parse_backlog_quota_type)]
        quota_type: BacklogQuotaType,
        /// Maximum bytes for `destination-storage`. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        limit_size: i64,
        /// Maximum age in seconds for `message-age`. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        limit_time: i32,
        /// Action when the quota is exceeded — `producer_request_hold`,
        /// `producer_exception`, or `consumer_backlog_eviction`.
        #[arg(long)]
        policy: String,
    },
    /// Remove a backlog-quota policy from a topic.
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuota?backlogQuotaType=...`.
    RemoveBacklogQuota {
        /// Fully qualified topic.
        topic: String,
        /// Quota dimension: `destination-storage` or `message-age`.
        #[arg(long = "type", value_parser = parse_backlog_quota_type)]
        quota_type: BacklogQuotaType,
    },
    /// Get a topic's message-TTL (seconds, or `null` if unset).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL`.
    GetMessageTtl {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's message-TTL (seconds). `0` disables.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL`.
    SetMessageTtl {
        /// Fully qualified topic.
        topic: String,
        /// TTL in seconds.
        #[arg(long)]
        ttl_seconds: i32,
    },
    /// Remove a topic's message-TTL (fall back to namespace default).
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL`.
    RemoveMessageTtl {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's persistence policy (or `null` if no override).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence`.
    GetPersistence {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's persistence policy (overrides namespace default).
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence`.
    SetPersistence {
        /// Fully qualified topic.
        topic: String,
        /// BookKeeper ensemble size.
        #[arg(long)]
        ensemble: i32,
        /// BookKeeper write quorum.
        #[arg(long)]
        write_quorum: i32,
        /// BookKeeper ack quorum.
        #[arg(long)]
        ack_quorum: i32,
        /// Managed-ledger mark-delete-rate cap (ops/sec). `0` disables.
        #[arg(long, default_value_t = 0.0)]
        mark_delete_rate: f64,
    },
    /// Remove a topic's persistence policy (fall back to namespace default).
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence`.
    RemovePersistence {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's consumer dispatch-rate policy (or `null` if no override).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate`.
    GetDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's consumer dispatch-rate policy (overrides namespace default).
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate`.
    SetDispatchRate {
        /// Fully qualified topic.
        topic: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a topic's consumer dispatch-rate policy.
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate`.
    RemoveDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's per-subscription dispatch-rate policy.
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`.
    GetSubscriptionDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's per-subscription dispatch-rate policy.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`.
    SetSubscriptionDispatchRate {
        /// Fully qualified topic.
        topic: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a topic's per-subscription dispatch-rate policy.
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`.
    RemoveSubscriptionDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's cross-cluster replicator dispatch-rate policy.
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`.
    GetReplicatorDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's cross-cluster replicator dispatch-rate policy.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`.
    SetReplicatorDispatchRate {
        /// Fully qualified topic.
        topic: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
        /// Averaging window in seconds.
        #[arg(long, default_value_t = 1)]
        period_seconds: i32,
        /// Treat rate as additive on top of namespace publish rate.
        #[arg(long, default_value_t = false)]
        relative_to_publish: bool,
    },
    /// Remove a topic's cross-cluster replicator dispatch-rate policy.
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`.
    RemoveReplicatorDispatchRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's publish-rate policy (or `null` if no override).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate`.
    GetPublishRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's publish-rate policy (overrides namespace default).
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate`.
    SetPublishRate {
        /// Fully qualified topic.
        topic: String,
        /// Throttle in messages/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_msg: i32,
        /// Throttle in bytes/sec. `-1` = unlimited.
        #[arg(long, default_value_t = -1)]
        rate_byte: i64,
    },
    /// Remove a topic's publish-rate policy.
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate`.
    RemovePublishRate {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's max-producers cap (or `null` if no override).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers`.
    GetMaxProducers {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's max-producers cap. `0` = unlimited.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers`.
    SetMaxProducers {
        /// Fully qualified topic.
        topic: String,
        /// Maximum number of concurrent producers on the topic.
        #[arg(long)]
        max_producers: i32,
    },
    /// Remove a topic's max-producers cap (fall back to namespace / broker default).
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers`.
    RemoveMaxProducers {
        /// Fully qualified topic.
        topic: String,
    },
    /// Get a topic's max-consumers cap (or `null` if no override).
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers`.
    GetMaxConsumers {
        /// Fully qualified topic.
        topic: String,
    },
    /// Set a topic's max-consumers cap. `0` = unlimited.
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers`.
    SetMaxConsumers {
        /// Fully qualified topic.
        topic: String,
        /// Maximum number of concurrent consumers on the topic.
        #[arg(long)]
        max_consumers: i32,
    },
    /// Remove a topic's max-consumers cap (fall back to namespace / broker default).
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers`.
    RemoveMaxConsumers {
        /// Fully qualified topic.
        topic: String,
    },
    /// Shadow-topic operations (PIP-180 / ADR-0033). A shadow topic shares
    /// its ledger storage with a source topic and exposes a read-only view
    /// of every entry to consumers — a lightweight fan-out alternative to
    /// geo-replication. See `docs/shadow-topic.md`.
    Shadow {
        #[command(subcommand)]
        sub: ShadowCmd,
    },
}

/// `admin subscriptions <verb>`.
#[derive(Debug, Subcommand)]
pub(crate) enum SubscriptionsCmd {
    /// List subscription names on a topic.
    List {
        /// Fully qualified topic (`[persistent://]tenant/namespace/topic`).
        topic: String,
    },
    /// Reset a subscription's cursor to a specific message position.
    /// `--message-id` accepts `LEDGER:ENTRY[:PARTITION[:BATCH]]`;
    /// partition and batch default to `-1` (non-partitioned, non-batched).
    ResetCursor {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
        /// Target message id, `LEDGER:ENTRY[:PARTITION[:BATCH]]`.
        #[arg(long = "message-id", value_parser = parse_message_id_position)]
        message_id: MessageId,
        /// Skip the message at `--message-id` itself (default: deliver it).
        #[arg(long)]
        is_excluded: bool,
    },
    /// Reset a subscription's cursor to a wall-clock timestamp.
    ResetCursorByTimestamp {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
        /// Target timestamp in **milliseconds** since the Unix epoch.
        #[arg(long)]
        timestamp_millis: u64,
    },
    /// Advance the cursor past N undelivered messages.
    Skip {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
        /// Number of messages to skip.
        #[arg(long)]
        count: u64,
    },
    /// Drain the entire backlog of a subscription (clear-backlog).
    SkipAll {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
    },
    /// Expire all messages older than `--expire-time-seconds`.
    Expire {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
        /// Age threshold in **seconds**.
        #[arg(long)]
        expire_time_seconds: u64,
    },
    /// Delete (unsubscribe) a subscription. `--force` disconnects
    /// active consumers first.
    Delete {
        /// Fully qualified topic.
        topic: String,
        /// Subscription name.
        subscription: String,
        /// Disconnect active consumers before deletion.
        #[arg(long)]
        force: bool,
    },
}

/// `admin topics shadow <verb>`.
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
            print_source_chain(&err);
            return ExitCode::from(1);
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("magnetar: {err}");
            print_source_chain(&err);
            ExitCode::from(1)
        }
    }
}

/// Print the `Display` chain of `err.source()` recursively to stderr,
/// indented under the caller's already-printed top-level message.
///
/// `reqwest::Error`'s `Display` only renders its own top-level message
/// (e.g. "error sending request for url (https://…)"). The underlying
/// cause — `hyper` connector error, `rustls` handshake failure, missing
/// TLS backend, DNS — sits in `.source()`. Walking the chain surfaces
/// it so operators don't have to bisect the binary's feature flags or
/// re-run under tcpdump just to find out *why* a request died.
fn print_source_chain(err: &dyn std::error::Error) {
    let mut source = err.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}

fn init_tracing(verbose: u8) {
    // Step 4+ pulls in the transport stack (`hyper`, `rustls`, `h2`) —
    // that is where TLS handshakes and connector errors actually log.
    // Without these directives `-vvvvv` is silent on the layer where
    // most admin REST failures happen.
    let default = match verbose {
        0 => "magnetar=info",
        1 => "magnetar=debug",
        2 => "magnetar=trace",
        3 => "magnetar=trace,reqwest=debug",
        4 => "magnetar=trace,reqwest=debug,hyper=debug,rustls=debug,h2=debug",
        _ => "magnetar=trace,reqwest=trace,hyper=trace,rustls=trace,h2=trace",
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

async fn run_admin(
    admin_url: &str,
    token: Option<String>,
    timeout_secs: u64,
    cmd: AdminCmd,
) -> Result<(), CliError> {
    let admin = build_admin(admin_url, token, timeout_secs)?;
    match cmd {
        AdminCmd::Clusters { sub } => run_admin_clusters(&admin, sub).await,
        AdminCmd::Tenants { sub } => run_admin_tenants(&admin, sub).await,
        AdminCmd::Namespaces { sub } => run_admin_namespaces(&admin, sub).await,
        AdminCmd::Topics { sub } => run_admin_topics(&admin, sub).await,
        AdminCmd::Subscriptions { sub } => run_admin_subscriptions(&admin, sub).await,
        AdminCmd::Brokers { sub } => run_admin_brokers(&admin, sub).await,
        AdminCmd::Bookies { sub } => run_admin_bookies(&admin, sub).await,
        AdminCmd::Schemas { sub } => run_admin_schemas(&admin, sub).await,
        AdminCmd::Functions { sub } => run_admin_functions(&admin, sub).await,
        AdminCmd::Sources { sub } => run_admin_sources(&admin, sub).await,
        AdminCmd::Sinks { sub } => run_admin_sinks(&admin, sub).await,
    }
}

async fn run_admin_subscriptions(
    admin: &AdminClient,
    cmd: SubscriptionsCmd,
) -> Result<(), CliError> {
    match cmd {
        SubscriptionsCmd::List { topic } => print_json(&admin.subscriptions_list(&topic).await?),
        SubscriptionsCmd::ResetCursor {
            topic,
            subscription,
            message_id,
            is_excluded,
        } => {
            admin
                .subscription_reset_cursor_to_position(
                    &topic,
                    &subscription,
                    message_id,
                    is_excluded,
                )
                .await?;
            Ok(())
        }
        SubscriptionsCmd::ResetCursorByTimestamp {
            topic,
            subscription,
            timestamp_millis,
        } => {
            admin
                .subscription_reset_cursor_to_timestamp(&topic, &subscription, timestamp_millis)
                .await?;
            Ok(())
        }
        SubscriptionsCmd::Skip {
            topic,
            subscription,
            count,
        } => {
            admin
                .subscription_skip_messages(&topic, &subscription, count)
                .await?;
            Ok(())
        }
        SubscriptionsCmd::SkipAll {
            topic,
            subscription,
        } => {
            admin
                .subscription_skip_all_messages(&topic, &subscription)
                .await?;
            Ok(())
        }
        SubscriptionsCmd::Expire {
            topic,
            subscription,
            expire_time_seconds,
        } => {
            admin
                .subscription_expire_messages(&topic, &subscription, expire_time_seconds)
                .await?;
            Ok(())
        }
        SubscriptionsCmd::Delete {
            topic,
            subscription,
            force,
        } => {
            admin
                .subscription_delete(&topic, &subscription, force)
                .await?;
            Ok(())
        }
    }
}

async fn run_admin_clusters(admin: &AdminClient, cmd: ClustersCmd) -> Result<(), CliError> {
    match cmd {
        ClustersCmd::List => print_json(&admin.cluster_list().await?),
        ClustersCmd::ListFailureDomains { cluster } => {
            print_json(&admin.cluster_failure_domains_list(&cluster).await?)
        }
        ClustersCmd::GetFailureDomain { cluster, domain } => {
            print_json(&admin.cluster_failure_domain_get(&cluster, &domain).await?)
        }
        ClustersCmd::ListNamespaceIsolationPolicies { cluster } => {
            print_json(&admin.namespace_isolation_policies_list(&cluster).await?)
        }
    }
}

async fn run_admin_brokers(admin: &AdminClient, cmd: BrokersCmd) -> Result<(), CliError> {
    match cmd {
        BrokersCmd::List { cluster } => print_json(&admin.brokers_list(&cluster).await?),
        BrokersCmd::Leader => print_json(&admin.brokers_leader().await?),
        BrokersCmd::DynamicConfigKeys => print_json(&admin.brokers_dynamic_config_keys().await?),
        BrokersCmd::DynamicConfigOverrides => {
            print_json(&admin.brokers_dynamic_config_overrides().await?)
        }
        BrokersCmd::RuntimeConfig => print_json(&admin.brokers_runtime_config().await?),
        BrokersCmd::InternalConfig => print_json(&admin.brokers_internal_config().await?),
        BrokersCmd::HealthCheck => {
            // The `/health` endpoint returns plain text (`"ok"`), not
            // JSON — print it verbatim rather than re-wrapping in a
            // JSON string for a script-friendly exit.
            let body = admin.brokers_health_check().await?;
            println!("{body}");
            Ok(())
        }
        BrokersCmd::OwnedNamespaces { cluster, broker } => {
            print_json(&admin.brokers_owned_namespaces(&cluster, &broker).await?)
        }
        BrokersCmd::SetDynamicConfig { name, value } => {
            admin.brokers_set_dynamic_config(&name, &value).await?;
            Ok(())
        }
        BrokersCmd::DeleteDynamicConfig { name } => {
            admin.brokers_delete_dynamic_config(&name).await?;
            Ok(())
        }
    }
}

async fn run_admin_bookies(admin: &AdminClient, cmd: BookiesCmd) -> Result<(), CliError> {
    match cmd {
        BookiesCmd::List => print_json(&admin.bookies_list_all().await?),
        BookiesCmd::RacksInfo => print_json(&admin.bookies_racks_info().await?),
        BookiesCmd::SetRack {
            bookie,
            group,
            rack,
            hostname,
        } => {
            let hostname = hostname.unwrap_or_else(|| bookie.clone());
            admin
                .bookies_set_rack(
                    &bookie,
                    BookieInfo {
                        group,
                        rack,
                        hostname,
                    },
                )
                .await?;
            Ok(())
        }
        BookiesCmd::DeleteRack { bookie } => {
            admin.bookies_delete_rack(&bookie).await?;
            Ok(())
        }
    }
}

async fn run_admin_schemas(admin: &AdminClient, cmd: SchemasCmd) -> Result<(), CliError> {
    match cmd {
        SchemasCmd::GetLatest { topic } => print_json(&admin.schema_get_latest(&topic).await?),
        SchemasCmd::GetVersion { topic, version } => {
            print_json(&admin.schema_get_version(&topic, version).await?)
        }
        SchemasCmd::ListVersions { topic } => {
            print_json(&admin.schema_list_versions(&topic).await?)
        }
        SchemasCmd::Post {
            topic,
            schema_type,
            schema,
            properties,
        } => {
            let payload = PostSchemaPayload {
                schema_type,
                schema,
                properties: properties.into_iter().collect(),
            };
            print_json(&admin.schema_post(&topic, payload).await?)
        }
        SchemasCmd::Delete { topic, force } => {
            admin.schema_delete(&topic, force).await?;
            Ok(())
        }
        SchemasCmd::Compatibility {
            topic,
            schema_type,
            schema,
            properties,
        } => {
            let payload = PostSchemaPayload {
                schema_type,
                schema,
                properties: properties.into_iter().collect(),
            };
            print_json(&admin.schema_compatibility_check(&topic, payload).await?)
        }
    }
}

async fn run_admin_functions(admin: &AdminClient, cmd: FunctionsCmd) -> Result<(), CliError> {
    match cmd {
        FunctionsCmd::List { namespace } => {
            // `tenant/namespace` parser: kept inline because the admin
            // helper for the three-segment form is `split_function_id`;
            // adding a separate two-segment public helper would
            // duplicate `split_namespace` (which stays private to the
            // admin crate). The inline parse mirrors the one the admin
            // crate runs on every namespace method.
            let (tenant, ns) = namespace.split_once('/').ok_or_else(|| {
                CliError::Admin(AdminError::InvalidName(format!(
                    "expected tenant/namespace, got {namespace:?}"
                )))
            })?;
            print_json(&admin.functions_list_by_namespace(tenant, ns).await?)
        }
        FunctionsCmd::Get { name } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            print_json(&admin.function_get(t, n, fn_name).await?)
        }
        FunctionsCmd::Status { name, instance_id } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            let value = match instance_id {
                Some(id) => admin.function_instance_status(t, n, fn_name, id).await?,
                None => admin.function_status(t, n, fn_name).await?,
            };
            print_json(&value)
        }
        FunctionsCmd::Stats { name, instance_id } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            let value = match instance_id {
                Some(id) => admin.function_instance_stats(t, n, fn_name, id).await?,
                None => admin.function_stats(t, n, fn_name).await?,
            };
            print_json(&value)
        }
        FunctionsCmd::CreateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            runtime,
            inputs,
            output,
            parallelism,
        } => {
            let cfg = FunctionConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                inputs,
                output,
                runtime,
                parallelism,
                user_config: None,
            };
            admin
                .function_create_with_url(&tenant, &namespace, &name, &url, cfg)
                .await?;
            Ok(())
        }
        FunctionsCmd::UpdateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            runtime,
            inputs,
            output,
            parallelism,
        } => {
            let cfg = FunctionConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                inputs,
                output,
                runtime,
                parallelism,
                user_config: None,
            };
            admin
                .function_update_with_url(&tenant, &namespace, &name, &url, cfg)
                .await?;
            Ok(())
        }
        FunctionsCmd::Delete { name } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            admin.function_delete(t, n, fn_name).await?;
            Ok(())
        }
        FunctionsCmd::Start { name, instance_id } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            match instance_id {
                Some(id) => admin.function_start_instance(t, n, fn_name, id).await?,
                None => admin.function_start(t, n, fn_name).await?,
            }
            Ok(())
        }
        FunctionsCmd::Stop { name, instance_id } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            match instance_id {
                Some(id) => admin.function_stop_instance(t, n, fn_name, id).await?,
                None => admin.function_stop(t, n, fn_name).await?,
            }
            Ok(())
        }
        FunctionsCmd::Restart { name } => {
            let (t, n, fn_name) = split_function_id(&name).map_err(CliError::Admin)?;
            admin.function_restart(t, n, fn_name).await?;
            Ok(())
        }
    }
}

async fn run_admin_tenants(admin: &AdminClient, cmd: TenantsCmd) -> Result<(), CliError> {
    match cmd {
        TenantsCmd::List => print_json(&admin.tenants_list().await?),
        TenantsCmd::Create {
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
        TenantsCmd::Delete { name } => {
            admin.tenant_delete(&name).await?;
            Ok(())
        }
    }
}

async fn run_admin_namespaces(admin: &AdminClient, cmd: NamespacesCmd) -> Result<(), CliError> {
    match cmd {
        NamespacesCmd::List { tenant } => print_json(&admin.namespaces_list(&tenant).await?),
        NamespacesCmd::Create { namespace } => {
            admin.namespace_create(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::Delete { namespace } => {
            admin.namespace_delete(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetRetention { namespace } => {
            print_json(&admin.namespace_get_retention(&namespace).await?)
        }
        NamespacesCmd::SetRetention {
            namespace,
            time_minutes,
            size_mb,
        } => {
            admin
                .namespace_set_retention(
                    &namespace,
                    RetentionPolicies {
                        retention_time_in_minutes: time_minutes,
                        retention_size_in_mb: size_mb,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveRetention { namespace } => {
            admin.namespace_remove_retention(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetBacklogQuotas { namespace } => {
            print_json(&admin.namespace_get_backlog_quotas(&namespace).await?)
        }
        NamespacesCmd::SetBacklogQuota {
            namespace,
            quota_type,
            limit_size,
            limit_time,
            policy,
        } => {
            admin
                .namespace_set_backlog_quota(
                    &namespace,
                    quota_type,
                    BacklogQuota {
                        limit_size,
                        limit_time,
                        policy,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveBacklogQuota {
            namespace,
            quota_type,
        } => {
            admin
                .namespace_remove_backlog_quota(&namespace, quota_type)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetMessageTtl { namespace } => {
            print_json(&admin.namespace_get_message_ttl(&namespace).await?)
        }
        NamespacesCmd::SetMessageTtl {
            namespace,
            ttl_seconds,
        } => {
            admin
                .namespace_set_message_ttl(&namespace, ttl_seconds)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveMessageTtl { namespace } => {
            admin.namespace_remove_message_ttl(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetPersistence { namespace } => {
            print_json(&admin.namespace_get_persistence(&namespace).await?)
        }
        NamespacesCmd::SetPersistence {
            namespace,
            ensemble,
            write_quorum,
            ack_quorum,
            mark_delete_rate,
        } => {
            admin
                .namespace_set_persistence(
                    &namespace,
                    PersistencePolicies {
                        bookkeeper_ensemble: ensemble,
                        bookkeeper_write_quorum: write_quorum,
                        bookkeeper_ack_quorum: ack_quorum,
                        managed_ledger_max_mark_delete_rate: mark_delete_rate,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemovePersistence { namespace } => {
            admin.namespace_remove_persistence(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetDispatchRate { namespace } => {
            print_json(&admin.namespace_get_dispatch_rate(&namespace).await?)
        }
        NamespacesCmd::SetDispatchRate {
            namespace,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .namespace_set_dispatch_rate(
                    &namespace,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveDispatchRate { namespace } => {
            admin.namespace_remove_dispatch_rate(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetSubscriptionDispatchRate { namespace } => print_json(
            &admin
                .namespace_get_subscription_dispatch_rate(&namespace)
                .await?,
        ),
        NamespacesCmd::SetSubscriptionDispatchRate {
            namespace,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .namespace_set_subscription_dispatch_rate(
                    &namespace,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveSubscriptionDispatchRate { namespace } => {
            admin
                .namespace_remove_subscription_dispatch_rate(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetReplicatorDispatchRate { namespace } => print_json(
            &admin
                .namespace_get_replicator_dispatch_rate(&namespace)
                .await?,
        ),
        NamespacesCmd::SetReplicatorDispatchRate {
            namespace,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .namespace_set_replicator_dispatch_rate(
                    &namespace,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveReplicatorDispatchRate { namespace } => {
            admin
                .namespace_remove_replicator_dispatch_rate(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetPublishRate { namespace } => {
            print_json(&admin.namespace_get_publish_rate(&namespace).await?)
        }
        NamespacesCmd::SetPublishRate {
            namespace,
            rate_msg,
            rate_byte,
        } => {
            admin
                .namespace_set_publish_rate(
                    &namespace,
                    PublishRate {
                        publish_throttling_rate_in_msg: rate_msg,
                        publish_throttling_rate_in_byte: rate_byte,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemovePublishRate { namespace } => {
            admin.namespace_remove_publish_rate(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetDeduplication { namespace } => {
            print_json(&admin.namespace_get_deduplication(&namespace).await?)
        }
        NamespacesCmd::SetDeduplication { namespace, enabled } => {
            admin
                .namespace_set_deduplication(&namespace, enabled)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveDeduplication { namespace } => {
            admin.namespace_remove_deduplication(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetDeduplicationSnapshotInterval { namespace } => print_json(
            &admin
                .namespace_get_deduplication_snapshot_interval(&namespace)
                .await?,
        ),
        NamespacesCmd::SetDeduplicationSnapshotInterval {
            namespace,
            interval_entries,
        } => {
            admin
                .namespace_set_deduplication_snapshot_interval(&namespace, interval_entries)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveDeduplicationSnapshotInterval { namespace } => {
            admin
                .namespace_remove_deduplication_snapshot_interval(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetCompactionThreshold { namespace } => {
            print_json(&admin.namespace_get_compaction_threshold(&namespace).await?)
        }
        NamespacesCmd::SetCompactionThreshold {
            namespace,
            threshold_bytes,
        } => {
            admin
                .namespace_set_compaction_threshold(&namespace, threshold_bytes)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveCompactionThreshold { namespace } => {
            admin
                .namespace_remove_compaction_threshold(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetDelayedDelivery { namespace } => {
            print_json(&admin.namespace_get_delayed_delivery(&namespace).await?)
        }
        NamespacesCmd::SetDelayedDelivery {
            namespace,
            active,
            tick_time_millis,
        } => {
            admin
                .namespace_set_delayed_delivery(
                    &namespace,
                    DelayedDeliveryPolicies {
                        active,
                        tick_time_millis,
                    },
                )
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveDelayedDelivery { namespace } => {
            admin.namespace_remove_delayed_delivery(&namespace).await?;
            Ok(())
        }
        NamespacesCmd::GetMaxProducersPerTopic { namespace } => print_json(
            &admin
                .namespace_get_max_producers_per_topic(&namespace)
                .await?,
        ),
        NamespacesCmd::SetMaxProducersPerTopic {
            namespace,
            max_producers,
        } => {
            admin
                .namespace_set_max_producers_per_topic(&namespace, max_producers)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveMaxProducersPerTopic { namespace } => {
            admin
                .namespace_remove_max_producers_per_topic(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetMaxConsumersPerTopic { namespace } => print_json(
            &admin
                .namespace_get_max_consumers_per_topic(&namespace)
                .await?,
        ),
        NamespacesCmd::SetMaxConsumersPerTopic {
            namespace,
            max_consumers,
        } => {
            admin
                .namespace_set_max_consumers_per_topic(&namespace, max_consumers)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveMaxConsumersPerTopic { namespace } => {
            admin
                .namespace_remove_max_consumers_per_topic(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetMaxUnackedMessagesPerConsumer { namespace } => print_json(
            &admin
                .namespace_get_max_unacked_messages_per_consumer(&namespace)
                .await?,
        ),
        NamespacesCmd::SetMaxUnackedMessagesPerConsumer {
            namespace,
            max_unacked,
        } => {
            admin
                .namespace_set_max_unacked_messages_per_consumer(&namespace, max_unacked)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveMaxUnackedMessagesPerConsumer { namespace } => {
            admin
                .namespace_remove_max_unacked_messages_per_consumer(&namespace)
                .await?;
            Ok(())
        }
        NamespacesCmd::GetMaxUnackedMessagesPerSubscription { namespace } => print_json(
            &admin
                .namespace_get_max_unacked_messages_per_subscription(&namespace)
                .await?,
        ),
        NamespacesCmd::SetMaxUnackedMessagesPerSubscription {
            namespace,
            max_unacked,
        } => {
            admin
                .namespace_set_max_unacked_messages_per_subscription(&namespace, max_unacked)
                .await?;
            Ok(())
        }
        NamespacesCmd::RemoveMaxUnackedMessagesPerSubscription { namespace } => {
            admin
                .namespace_remove_max_unacked_messages_per_subscription(&namespace)
                .await?;
            Ok(())
        }
    }
}

async fn run_admin_topics(admin: &AdminClient, cmd: TopicsCmd) -> Result<(), CliError> {
    match cmd {
        TopicsCmd::List { namespace } => print_json(&admin.topics_list(&namespace).await?),
        TopicsCmd::Create { topic, partitions } => {
            admin.topic_create_partitioned(&topic, partitions).await?;
            Ok(())
        }
        TopicsCmd::Delete { topic, force } => {
            admin.topic_delete(&topic, force).await?;
            Ok(())
        }
        TopicsCmd::Stats { topic } => {
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
        TopicsCmd::Compact { topic } => {
            admin.topic_compact(&topic).await?;
            Ok(())
        }
        TopicsCmd::CompactionStatus { topic } => {
            print_json(&admin.topic_compaction_status(&topic).await?)
        }
        TopicsCmd::Unload { topic } => {
            admin.topic_unload(&topic).await?;
            Ok(())
        }
        TopicsCmd::Terminate { topic } => {
            // `MessageId` doesn't derive `Serialize` — build the JSON manually
            // (same shape as `topics get-message-id-by-index`).
            let id = admin.topic_terminate(&topic).await?;
            let json = serde_json::json!({
                "ledgerId": id.ledger_id,
                "entryId": id.entry_id,
                "partition": id.partition,
                "batchIndex": id.batch_index,
                "batchSize": id.batch_size,
            });
            print_json(&json)
        }
        TopicsCmd::UpdatePartitions { topic, partitions } => {
            admin.topic_update_partitions(&topic, partitions).await?;
            Ok(())
        }
        TopicsCmd::GetMessageIdByIndex { topic, index } => {
            // `MessageId` doesn't derive `Serialize` (it's a pure proto
            // type); build the JSON shape manually so the CLI output
            // mirrors Java's `MessageIdImpl.toString()` field layout.
            let id = admin.topic_get_message_id_by_index(&topic, index).await?;
            let json = serde_json::json!({
                "ledgerId": id.ledger_id,
                "entryId": id.entry_id,
                "partition": id.partition,
                "batchIndex": id.batch_index,
                "batchSize": id.batch_size,
            });
            print_json(&json)
        }
        TopicsCmd::GetRetention { topic } => print_json(&admin.topic_get_retention(&topic).await?),
        TopicsCmd::SetRetention {
            topic,
            time_minutes,
            size_mb,
        } => {
            admin
                .topic_set_retention(
                    &topic,
                    RetentionPolicies {
                        retention_time_in_minutes: time_minutes,
                        retention_size_in_mb: size_mb,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemoveRetention { topic } => {
            admin.topic_remove_retention(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetBacklogQuotas { topic } => {
            print_json(&admin.topic_get_backlog_quotas(&topic).await?)
        }
        TopicsCmd::SetBacklogQuota {
            topic,
            quota_type,
            limit_size,
            limit_time,
            policy,
        } => {
            admin
                .topic_set_backlog_quota(
                    &topic,
                    quota_type,
                    BacklogQuota {
                        limit_size,
                        limit_time,
                        policy,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemoveBacklogQuota { topic, quota_type } => {
            admin.topic_remove_backlog_quota(&topic, quota_type).await?;
            Ok(())
        }
        TopicsCmd::GetMessageTtl { topic } => {
            print_json(&admin.topic_get_message_ttl(&topic).await?)
        }
        TopicsCmd::SetMessageTtl { topic, ttl_seconds } => {
            admin.topic_set_message_ttl(&topic, ttl_seconds).await?;
            Ok(())
        }
        TopicsCmd::RemoveMessageTtl { topic } => {
            admin.topic_remove_message_ttl(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetPersistence { topic } => {
            print_json(&admin.topic_get_persistence(&topic).await?)
        }
        TopicsCmd::SetPersistence {
            topic,
            ensemble,
            write_quorum,
            ack_quorum,
            mark_delete_rate,
        } => {
            admin
                .topic_set_persistence(
                    &topic,
                    PersistencePolicies {
                        bookkeeper_ensemble: ensemble,
                        bookkeeper_write_quorum: write_quorum,
                        bookkeeper_ack_quorum: ack_quorum,
                        managed_ledger_max_mark_delete_rate: mark_delete_rate,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemovePersistence { topic } => {
            admin.topic_remove_persistence(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetDispatchRate { topic } => {
            print_json(&admin.topic_get_dispatch_rate(&topic).await?)
        }
        TopicsCmd::SetDispatchRate {
            topic,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .topic_set_dispatch_rate(
                    &topic,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemoveDispatchRate { topic } => {
            admin.topic_remove_dispatch_rate(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetSubscriptionDispatchRate { topic } => {
            print_json(&admin.topic_get_subscription_dispatch_rate(&topic).await?)
        }
        TopicsCmd::SetSubscriptionDispatchRate {
            topic,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .topic_set_subscription_dispatch_rate(
                    &topic,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemoveSubscriptionDispatchRate { topic } => {
            admin
                .topic_remove_subscription_dispatch_rate(&topic)
                .await?;
            Ok(())
        }
        TopicsCmd::GetReplicatorDispatchRate { topic } => {
            print_json(&admin.topic_get_replicator_dispatch_rate(&topic).await?)
        }
        TopicsCmd::SetReplicatorDispatchRate {
            topic,
            rate_msg,
            rate_byte,
            period_seconds,
            relative_to_publish,
        } => {
            admin
                .topic_set_replicator_dispatch_rate(
                    &topic,
                    DispatchRate {
                        dispatch_throttling_rate_in_msg: rate_msg,
                        dispatch_throttling_rate_in_byte: rate_byte,
                        rate_period_in_second: period_seconds,
                        relative_to_publish_rate: relative_to_publish,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemoveReplicatorDispatchRate { topic } => {
            admin.topic_remove_replicator_dispatch_rate(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetPublishRate { topic } => {
            print_json(&admin.topic_get_publish_rate(&topic).await?)
        }
        TopicsCmd::SetPublishRate {
            topic,
            rate_msg,
            rate_byte,
        } => {
            admin
                .topic_set_publish_rate(
                    &topic,
                    PublishRate {
                        publish_throttling_rate_in_msg: rate_msg,
                        publish_throttling_rate_in_byte: rate_byte,
                    },
                )
                .await?;
            Ok(())
        }
        TopicsCmd::RemovePublishRate { topic } => {
            admin.topic_remove_publish_rate(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetMaxProducers { topic } => {
            print_json(&admin.topic_get_max_producers(&topic).await?)
        }
        TopicsCmd::SetMaxProducers {
            topic,
            max_producers,
        } => {
            admin.topic_set_max_producers(&topic, max_producers).await?;
            Ok(())
        }
        TopicsCmd::RemoveMaxProducers { topic } => {
            admin.topic_remove_max_producers(&topic).await?;
            Ok(())
        }
        TopicsCmd::GetMaxConsumers { topic } => {
            print_json(&admin.topic_get_max_consumers(&topic).await?)
        }
        TopicsCmd::SetMaxConsumers {
            topic,
            max_consumers,
        } => {
            admin.topic_set_max_consumers(&topic, max_consumers).await?;
            Ok(())
        }
        TopicsCmd::RemoveMaxConsumers { topic } => {
            admin.topic_remove_max_consumers(&topic).await?;
            Ok(())
        }
        TopicsCmd::Shadow { sub } => run_admin_topics_shadow(admin, sub).await,
    }
}

/// PIP-180 / ADR-0033: dispatch shadow-topic subcommands over the admin
/// REST client. Wraps `magnetar_admin::AdminClient::{create,delete,
/// get_shadow_topics, get_shadow_source}`.
async fn run_admin_topics_shadow(admin: &AdminClient, cmd: ShadowCmd) -> Result<(), CliError> {
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

async fn run_admin_sources(admin: &AdminClient, cmd: SourcesCmd) -> Result<(), CliError> {
    match cmd {
        SourcesCmd::List { namespace } => {
            let (tenant, ns) = split_namespace_ref(&namespace).map_err(CliError::BadArg)?;
            print_json(&admin.sources_list_by_namespace(tenant, ns).await?)
        }
        SourcesCmd::Get { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            print_json(&admin.source_get(tenant, ns, name).await?)
        }
        SourcesCmd::Status { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            print_json(&admin.source_status(tenant, ns, name).await?)
        }
        SourcesCmd::CreateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            topic_name,
            parallelism,
        } => {
            let config = SourceConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                topic_name,
                parallelism,
                configs: None,
            };
            admin
                .source_create_with_url(&tenant, &namespace, &name, &url, config)
                .await?;
            Ok(())
        }
        SourcesCmd::UpdateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            topic_name,
            parallelism,
        } => {
            let config = SourceConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                topic_name,
                parallelism,
                configs: None,
            };
            admin
                .source_update_with_url(&tenant, &namespace, &name, &url, config)
                .await?;
            Ok(())
        }
        SourcesCmd::Delete { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            admin.source_delete(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Start { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            admin.source_start(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Stop { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            admin.source_stop(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Restart { source } => {
            let (tenant, ns, name) = split_function_id(&source).map_err(CliError::BadArg)?;
            admin.source_restart(tenant, ns, name).await?;
            Ok(())
        }
    }
}

async fn run_admin_sinks(admin: &AdminClient, cmd: SinksCmd) -> Result<(), CliError> {
    match cmd {
        SinksCmd::List { namespace } => {
            let (tenant, ns) = split_namespace_ref(&namespace).map_err(CliError::BadArg)?;
            print_json(&admin.sinks_list_by_namespace(tenant, ns).await?)
        }
        SinksCmd::Get { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            print_json(&admin.sink_get(tenant, ns, name).await?)
        }
        SinksCmd::Status { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            print_json(&admin.sink_status(tenant, ns, name).await?)
        }
        SinksCmd::CreateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            inputs,
            parallelism,
        } => {
            let config = SinkConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                inputs,
                parallelism,
                configs: None,
            };
            admin
                .sink_create_with_url(&tenant, &namespace, &name, &url, config)
                .await?;
            Ok(())
        }
        SinksCmd::UpdateWithUrl {
            tenant,
            namespace,
            name,
            url,
            class_name,
            inputs,
            parallelism,
        } => {
            let config = SinkConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                class_name,
                inputs,
                parallelism,
                configs: None,
            };
            admin
                .sink_update_with_url(&tenant, &namespace, &name, &url, config)
                .await?;
            Ok(())
        }
        SinksCmd::Delete { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_delete(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Start { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_start(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Stop { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_stop(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Restart { sink } => {
            let (tenant, ns, name) = split_function_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_restart(tenant, ns, name).await?;
            Ok(())
        }
    }
}

/// Split `tenant/namespace/name` into its three segments. Used by
/// Pulsar IO Sources / Sinks and Pulsar Packages — the broker's
/// `SourcesBase` / `SinksBase` / `PackagesBase` resources all carry
/// the same `{tenant}/{namespace}/{name}` URL shape for a Function /
/// Source / Sink / Package id. Symmetric with the admin client's
/// `split_topic` helper (which accepts an optional `persistent://`
/// scheme); this variant intentionally rejects URI schemes — a
/// connector name is never a topic URL.
fn split_function_id(spec: &str) -> Result<(&str, &str, &str), String> {
    let mut parts = spec.splitn(3, '/');
    let tenant = parts.next().unwrap_or("");
    let namespace = parts.next().unwrap_or("");
    let name = parts.next().unwrap_or("");
    if tenant.is_empty() || namespace.is_empty() || name.is_empty() || name.contains('/') {
        return Err(format!("expected tenant/namespace/name, got `{spec}`"));
    }
    Ok((tenant, namespace, name))
}

/// Split `tenant/namespace` into its two segments. Used by the
/// namespace-scoped `list` verbs on Pulsar IO Sources / Sinks /
/// Packages. The admin client's own `split_namespace` is private —
/// duplicating the parser here keeps the CLI's error surface
/// (`CliError::BadArg`) cohesive without exposing internals.
fn split_namespace_ref(spec: &str) -> Result<(&str, &str), String> {
    let (tenant, namespace) = spec
        .split_once('/')
        .ok_or_else(|| format!("expected tenant/namespace, got `{spec}` (no '/')"))?;
    if tenant.is_empty() || namespace.is_empty() || namespace.contains('/') {
        return Err(format!("expected tenant/namespace, got `{spec}`"));
    }
    Ok((tenant, namespace))
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
    #[error(transparent)]
    Admin(#[from] AdminError),
    /// Bad CLI argument that clap could not catch.
    #[error("bad argument: {0}")]
    BadArg(String),
    /// JSON serialization failure (for stdout output).
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Underlying magnetar (data-plane) façade failure.
    #[error(transparent)]
    Pulsar(#[from] magnetar::PulsarError),
    /// Underlying tokio engine failure (producer/consumer ops).
    #[error(transparent)]
    Client(#[from] ClientError),
    /// I/O error while reading stdin or writing stdout.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Parse a `MessageId` from the canonical CLI form
/// `LEDGER:ENTRY[:PARTITION[:BATCH]]`. Partition and batch default to
/// `-1` (non-partitioned, non-batched). `batch_size` is always set to
/// `-1` — it's broker-internal metadata that callers can't observe at
/// the admin REST boundary.
fn parse_message_id_position(s: &str) -> Result<MessageId, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if !(2..=4).contains(&parts.len()) {
        return Err(format!(
            "expected LEDGER:ENTRY[:PARTITION[:BATCH]], got `{s}`"
        ));
    }
    let ledger_id: u64 = parts[0]
        .parse()
        .map_err(|e| format!("bad ledger id `{}`: {e}", parts[0]))?;
    let entry_id: u64 = parts[1]
        .parse()
        .map_err(|e| format!("bad entry id `{}`: {e}", parts[1]))?;
    let partition: i32 = parts
        .get(2)
        .map(|p| p.parse().map_err(|e| format!("bad partition `{p}`: {e}")))
        .transpose()?
        .unwrap_or(-1);
    let batch_index: i32 = parts
        .get(3)
        .map(|b| b.parse().map_err(|e| format!("bad batch `{b}`: {e}")))
        .transpose()?
        .unwrap_or(-1);
    Ok(MessageId {
        ledger_id,
        entry_id,
        partition,
        batch_index,
        batch_size: -1,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    })
}

/// Parse a `BacklogQuotaType` from the CLI form. Accepts both
/// kebab-case (operator-friendly) and the snake_case the broker REST
/// surface emits, so a JSON-driven script that round-trips the value
/// gets `--type destination_storage` for free.
fn parse_backlog_quota_type(s: &str) -> Result<BacklogQuotaType, String> {
    match s.to_ascii_lowercase().as_str() {
        "destination-storage" | "destination_storage" => Ok(BacklogQuotaType::DestinationStorage),
        "message-age" | "message_age" => Ok(BacklogQuotaType::MessageAge),
        other => Err(format!(
            "unknown backlog quota type `{other}` (expected: destination-storage | message-age)"
        )),
    }
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
