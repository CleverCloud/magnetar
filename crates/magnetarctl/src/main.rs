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
    "magnetarctl: enable at least one of crypto-{aws-lc-rs,ring,openssl,fips}. \
     The default feature set covers this; only `--no-default-features` users \
     need to pick one explicitly."
);

mod config;
mod version;

use std::process::ExitCode;
use std::time::Duration;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use magnetar::proto::TokenAuth;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::runtime_tokio::ClientError;
use magnetar::{MessageId, OutgoingMessage, PulsarClient};
use magnetar_admin::{
    AdminAuth, AdminClient, AdminClientBuilder, AdminError, BacklogQuota, BacklogQuotaType,
    BookieInfo, DelayedDeliveryPolicies, DispatchRate, FunctionConfig, PackageMetadata,
    PackageType, PersistencePolicies, PostSchemaPayload, PublishRate, RetentionPolicies,
    SinkConfig, SourceConfig, TenantInfo,
};

/// magnetarctl — produce, consume, inspect, and admin against an Apache Pulsar broker.
#[derive(Debug, Parser)]
#[command(
    name = "magnetarctl",
    version = version::short(),
    long_version = version::long(),
    about,
    long_about = None,
)]
pub(crate) struct Cli {
    /// Increase logging verbosity (-v, -vv, -vvv). Accepted at any level
    /// (`magnetar admin -vv tenant-list` is the same as
    /// `magnetar -vv admin tenant-list`).
    /// The default (no `-v`) is `magnetar=warn`; `-v` adds `info`, `-vv`
    /// `debug`, `-vvv` `trace`, and `-vvvv`+ widen to the transport stack.
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub(crate) verbose: u8,

    /// Pulsar service URL for data-plane (`pulsar://` / `pulsar+ssl://`).
    ///
    /// No `default_value`: when unset (and no env var), the URL is resolved
    /// from the active context's `admin-service-url` (derived data-plane URL),
    /// falling back to `pulsar://localhost:6650` only when no context applies.
    /// An explicit `--service-url` / `MAGNETAR_SERVICE_URL` always wins.
    #[arg(long, env = "MAGNETAR_SERVICE_URL", global = true)]
    pub(crate) service_url: Option<String>,

    /// Pulsar admin REST URL (`http://` / `https://`). pulsarctl-style short
    /// alias: `-s`.
    ///
    /// No `default_value`: when unset (and no env var), resolved from the
    /// active context's `admin-service-url`, falling back to
    /// `http://localhost:8080` only when no context applies. An explicit
    /// `--admin-url` / `-s` / `MAGNETAR_ADMIN_URL` always wins.
    ///
    /// NB: the long pulsarctl spelling `--admin-service-url` is the per-context
    /// write flag on `context set` (it would collide with this global flag if
    /// also aliased here), so the global connection flag keeps `-s` + the
    /// canonical `--admin-url`.
    #[arg(long, short = 's', env = "MAGNETAR_ADMIN_URL", global = true)]
    pub(crate) admin_url: Option<String>,

    /// Bearer token for admin auth. Reads from `MAGNETAR_TOKEN` if unset,
    /// then from the active context's `token` / `tokenFile`.
    #[arg(long, env = "MAGNETAR_TOKEN", global = true)]
    pub(crate) token: Option<String>,

    /// Path to a file containing a bearer token (pulsarctl `tokenFile`).
    #[arg(long, env = "MAGNETAR_TOKEN_FILE", global = true)]
    pub(crate) token_file: Option<String>,

    /// Path to a custom CA trust cert PEM (pulsarctl
    /// `tls_trust_certs_file_path`).
    #[arg(long, global = true)]
    pub(crate) tls_trust_cert_path: Option<String>,

    /// Disable TLS certificate verification (pulsarctl
    /// `tls_allow_insecure_connection`). **Insecure** — dev only.
    #[arg(long, global = true)]
    pub(crate) tls_allow_insecure: bool,

    /// Enable TLS hostname verification (pulsarctl flag; accepted for
    /// pulsarctl muscle-memory — verification is on by default in rustls and
    /// this flag is a no-op unless paired with `--tls-allow-insecure`).
    #[arg(long, global = true)]
    pub(crate) tls_enable_hostname_verification: bool,

    /// Client TLS certificate file (mTLS). Accepted for pulsarctl parity, but
    /// client-certificate mTLS is **not yet wired in** — setting it warns and
    /// otherwise has no effect.
    #[arg(long, global = true)]
    pub(crate) tls_cert_file: Option<String>,

    /// Client TLS private-key file (mTLS). Pairs with `--tls-cert-file`.
    /// **Not yet wired in** (see `--tls-cert-file`).
    #[arg(long, global = true)]
    pub(crate) tls_key_file: Option<String>,

    /// Path to a pulsarctl-compatible config file. Overrides the default
    /// `$HOME/.config/pulsar/config`. A named-but-missing file is an error.
    #[arg(long, env = "MAGNETAR_CONFIG", global = true)]
    pub(crate) config: Option<String>,

    /// Select a named context (overrides the config's `current-context`).
    #[arg(long, global = true)]
    pub(crate) context: Option<String>,

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
    /// Manage pulsarctl-compatible contexts in `~/.config/pulsar/config`.
    /// Mirrors `pulsarctl context` (`use` / `set` / `delete` / `get` /
    /// `current` / `rename`). A file written here stays readable by pulsarctl.
    Context {
        #[command(subcommand)]
        sub: ContextCmd,
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

/// `context` subcommands — pulsarctl-compatible context management. All verbs
/// operate on the resolved config file (`--config` › `MAGNETAR_CONFIG` ›
/// `$XDG_CONFIG_HOME/pulsar/config` › `$HOME/.config/pulsar/config`).
#[derive(Debug, Subcommand)]
pub(crate) enum ContextCmd {
    /// Set `current-context` to `<name>`. Prints `Switched to context "<name>".`.
    Use {
        /// Context name (must already exist).
        name: String,
    },
    /// Create or update a context. Alias: `create`. Flag values are MERGED
    /// onto any existing context — unset flags leave existing fields untouched.
    ///
    /// The credential / TLS flags `--token`, `--token-file`,
    /// `--tls-trust-cert-path`, `--tls-allow-insecure` are the GLOBAL
    /// connection flags (they apply to `context set` too): e.g.
    /// `magnetarctl context set prod --admin-service-url https://b:443 --token tok`.
    #[command(alias = "create")]
    Set {
        /// Context name.
        name: String,
        /// `admin-service-url` (REST endpoint).
        #[arg(long)]
        admin_service_url: Option<String>,
        /// `bookie-service-url` (`BookKeeper` HTTP).
        #[arg(long)]
        bookie_service_url: Option<String>,
        /// `issuer_endpoint` (`OAuth2`).
        #[arg(long, short = 'i')]
        issuer_endpoint: Option<String>,
        /// `client_id` (`OAuth2`).
        #[arg(long, short = 'c')]
        client_id: Option<String>,
        /// `audience` (`OAuth2`).
        #[arg(long, short = 'a')]
        audience: Option<String>,
        /// `scope` (`OAuth2`).
        #[arg(long)]
        scope: Option<String>,
        /// `key_file` (`OAuth2` Pulsar-style key file).
        #[arg(long, short = 'k')]
        key_file: Option<String>,
    },
    /// Delete a context (from BOTH `contexts` and `auth-info`). Alias: `del`.
    #[command(alias = "del")]
    Delete {
        /// Context name.
        name: String,
    },
    /// List all contexts as a table; `*` marks `current-context`.
    Get,
    /// Print the current context name. Errors when unset.
    Current,
    /// Rename a context (and its `auth-info` entry). Alias: `update`. Updates
    /// `current-context` when it pointed at `<old>`. Refuses to overwrite an
    /// existing `<new>` unless `--force` is given.
    #[command(alias = "update")]
    Rename {
        /// Existing context name.
        old: String,
        /// New context name.
        new: String,
        /// Overwrite `<new>` if it already exists. Without this the rename is
        /// refused, to avoid silently destroying an existing context.
        #[arg(long, short = 'f')]
        force: bool,
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
    /// Pulsar Packages (`/admin/v3/packages/...`) — the versioned
    /// binary registry that Functions / Sources / Sinks JARs and
    /// NARs live in.
    Packages {
        #[command(subcommand)]
        sub: PackagesCmd,
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
    /// URLs, `BookKeeper` service URI, ledger root paths).
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
    /// read-only) as registered in `BookKeeper` metadata.
    /// `GET /admin/v2/bookies/all`.
    List,
    /// Get every bookie's group + rack assignment.
    /// `GET /admin/v2/bookies/racks-info`.
    RacksInfo,
    /// Set (or update) a bookie's rack assignment.
    /// `POST /admin/v2/bookies/racks-info/{bookie}`.
    SetRack {
        /// Bookie `host:port` (matches `BookKeeper` metadata).
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
        /// At least one is required — Pulsar's
        /// `FunctionConfigUtils#inferMissingArguments` rejects a
        /// function with no inputs at HTTP 400.
        #[arg(long = "input", required = true, num_args = 1..)]
        inputs: Vec<String>,
        /// Output topic the function produces to.
        #[arg(long, default_value = "")]
        output: String,
        /// Number of parallel instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
        /// Per-function user-config JSON object. Operators can pass
        /// arbitrary key/value metadata the function reads at runtime.
        /// Accepted as a single JSON string (`--user-config '{"k":"v"}'`).
        #[arg(long = "user-config", value_parser = parse_json_object)]
        user_config: Option<serde_json::Value>,
    },
    /// Update an existing function from a remote package URL.
    /// `PUT /admin/v3/functions/{tenant}/{namespace}/{name}` (multipart).
    ///
    /// **Full replace**: Pulsar's `updateFunction` replaces the stored
    /// `FunctionConfig` in-toto from the body it receives, with no
    /// merge semantics. Fields you don't pass on the CLI — most
    /// notably `--user-config` — get wiped on the broker side.
    /// Re-pass `--user-config '<json>'` (or any other policy you
    /// expect to preserve) on every update.
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
        /// Input topic. Repeat for multiple. At least one required.
        #[arg(long = "input", required = true, num_args = 1..)]
        inputs: Vec<String>,
        /// Output topic.
        #[arg(long, default_value = "")]
        output: String,
        /// Number of parallel instances.
        #[arg(long, default_value_t = 1)]
        parallelism: i32,
        /// Per-function user-config JSON object. **Must be re-passed on
        /// every update** — Pulsar's `updateFunction` is a full
        /// replace, so omitting `--user-config` wipes the broker's
        /// stored map.
        #[arg(long = "user-config", value_parser = parse_json_object)]
        user_config: Option<serde_json::Value>,
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

/// `admin packages <verb>` — Pulsar Packages registry.
///
/// `TYPE` is parsed via [`parse_package_type`] (accepts both singular
/// and pluralised aliases — `function` / `functions`, etc.).
#[derive(Debug, Subcommand)]
pub(crate) enum PackagesCmd {
    /// List package names declared for one type under a namespace.
    /// `GET /admin/v3/packages/{type}/{tenant}/{namespace}`.
    List {
        /// Package type (`function` / `source` / `sink`).
        #[arg(value_parser = parse_package_type)]
        package_type: PackageType,
        /// `tenant/namespace`.
        namespace: String,
    },
    /// List the versions declared for one package.
    /// `GET /admin/v3/packages/{type}/{tenant}/{namespace}/{name}`.
    Versions {
        /// Package type.
        #[arg(value_parser = parse_package_type)]
        package_type: PackageType,
        /// `tenant/namespace/name`.
        package: String,
    },
    /// Get the metadata envelope for one package version.
    /// `GET .../{name}/{version}/metadata`.
    MetadataGet {
        /// Package type.
        #[arg(value_parser = parse_package_type)]
        package_type: PackageType,
        /// `tenant/namespace/name`.
        package: String,
        /// Package version (broker treats versions as opaque
        /// strings — `1.0.0`, `latest`, build hashes).
        #[arg(long)]
        version: String,
    },
    /// Replace the metadata envelope for one package version.
    /// `PUT .../{name}/{version}/metadata`.
    MetadataSet {
        /// Package type.
        #[arg(value_parser = parse_package_type)]
        package_type: PackageType,
        /// `tenant/namespace/name`.
        package: String,
        /// Package version.
        #[arg(long)]
        version: String,
        /// Free-form description.
        #[arg(long)]
        description: String,
        /// Maintainer contact (email / team handle).
        #[arg(long)]
        contact: String,
        /// Arbitrary property in `key=value` form. Repeatable.
        #[arg(long = "property", value_parser = parse_property)]
        properties: Vec<(String, String)>,
    },
    /// Delete one package version.
    /// `DELETE .../{name}/{version}`.
    Delete {
        /// Package type.
        #[arg(value_parser = parse_package_type)]
        package_type: PackageType,
        /// `tenant/namespace/name`.
        package: String,
        /// Package version.
        #[arg(long)]
        version: String,
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
        /// `BookKeeper` ensemble size.
        #[arg(long)]
        ensemble: i32,
        /// `BookKeeper` write quorum.
        #[arg(long)]
        write_quorum: i32,
        /// `BookKeeper` ack quorum.
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
        /// `BookKeeper` ensemble size.
        #[arg(long)]
        ensemble: i32,
        /// `BookKeeper` write quorum.
        #[arg(long)]
        write_quorum: i32,
        /// `BookKeeper` ack quorum.
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
    let matches = Cli::command().get_matches();
    // Distinguish an explicit `--token` flag from an inherited `MAGNETAR_TOKEN`
    // env var. `context set` persists the token to disk and must only do so for
    // an explicit flag — the env var is meant for the current connection, not a
    // durable write. (`token` is a global arg, whose source clap propagates to
    // the root matches.)
    let token_from_flag =
        matches.value_source("token") == Some(clap::parser::ValueSource::CommandLine);
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(err) => err.exit(),
    };
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

    match runtime.block_on(run(cli, token_from_flag)) {
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
    // Step 5+ pulls in the transport stack (`hyper`, `rustls`, `h2`) —
    // that is where TLS handshakes and connector errors actually log.
    // Without these directives `-vvvvvv` is silent on the layer where
    // most admin REST failures happen.
    let default = match verbose {
        0 => "magnetar=warn",
        1 => "magnetar=info",
        2 => "magnetar=debug",
        3 => "magnetar=trace",
        4 => "magnetar=trace,reqwest=debug",
        5 => "magnetar=trace,reqwest=debug,hyper=debug,rustls=debug,h2=debug",
        _ => "magnetar=trace,reqwest=trace,hyper=trace,rustls=trace,h2=trace",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

async fn run(cli: Cli, token_from_flag: bool) -> Result<(), CliError> {
    // `context` is config-file management only — it never opens a connection,
    // so resolve nothing and dispatch directly. The credential / TLS GLOBAL
    // flags (`--token` / `--token-file` / `--tls-trust-cert-path` /
    // `--tls-allow-insecure`) double as `context set` write values — they are
    // global, so they cannot also be declared on `set` without a clap name
    // clash; we thread them in here instead.
    if matches!(cli.cmd, Cmd::Context { .. }) {
        let globals = ContextSetGlobals {
            // Persist a token only when it came from an explicit `--token`; an
            // inherited `MAGNETAR_TOKEN` is for the live connection, not a write
            // to disk (it would silently leak a transient secret into the file).
            token: token_from_flag.then(|| cli.token.clone()).flatten(),
            token_file: cli.token_file.clone(),
            tls_trust_cert_path: cli.tls_trust_cert_path.clone(),
            // Only treat allow-insecure as a write when explicitly passed; the
            // default `false` must not clobber an existing `true`.
            tls_allow_insecure: cli.tls_allow_insecure.then_some(true),
        };
        // Move `sub` out without cloning the whole command.
        let Cmd::Context { sub } = cli.cmd else {
            unreachable!("matched Context above")
        };
        return run_context(cli.config.as_deref(), &globals, sub);
    }

    // Resolve the active context (if any) once, then merge with explicit
    // flags/env to produce the connection settings. Explicit flags always win;
    // a context fills the gaps; built-in localhost defaults are the last
    // resort. No config + no context → identical to today's behavior.
    let conn = resolve_connection(&cli)?;

    match cli.cmd {
        Cmd::Produce {
            topic,
            message,
            key,
            properties,
            count,
        } => {
            run_produce(
                &conn.service_url,
                conn.data_auth.clone(),
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
                &conn.service_url,
                conn.data_auth.clone(),
                &topic,
                &subscription,
                sub_type,
                count,
                ack,
                replicate_subscription_state,
            )
            .await
        }
        Cmd::Admin { sub } => run_admin(&conn, cli.admin_timeout_secs, sub).await,
        Cmd::Context { .. } => unreachable!("handled above"),
        #[cfg(feature = "scalable-topics")]
        Cmd::TopicInfo { topic } => {
            run_topic_info(&conn.service_url, conn.data_auth.clone(), &topic).await
        }
    }
}

/// Default admin REST URL when no flag/env/context applies (today's default).
const DEFAULT_ADMIN_URL: &str = "http://localhost:8080";
/// Default data-plane URL when no flag/env/context/derivation applies.
const DEFAULT_SERVICE_URL: &str = "pulsar://localhost:6650";

/// Auth for the data-plane (`produce` / `consume`) client: a bare bearer token
/// or an `OAuth2` flow (primed before the data client is built so its
/// `AuthProvider::initial` succeeds).
#[derive(Clone)]
enum DataAuth {
    /// No credentials.
    None,
    /// Inline bearer token.
    Token(String),
    /// `OAuth2` `client_credentials` flow. Its token cache is refreshed in
    /// `build_data_client` before the flow is handed to the client as an
    /// `AuthProvider`.
    OAuth2(std::sync::Arc<magnetar_auth_oauth2::ClientCredentialsFlow>),
}

/// Connection settings resolved from explicit flags/env, the active context,
/// and built-in defaults. Built once per run and shared by the admin and
/// data-plane paths.
struct ResolvedConnection {
    /// Admin REST URL.
    admin_url: String,
    /// Data-plane URL.
    service_url: String,
    /// Admin auth (already including any `OAuth2` flow).
    admin_auth: AdminAuth,
    /// Data-plane auth.
    data_auth: DataAuth,
    /// Custom CA trust cert PEM bytes for the admin client (read from a path).
    admin_trust_cert_pem: Option<Vec<u8>>,
    /// Allow-insecure TLS for the admin client.
    admin_allow_insecure: bool,
}

/// Resolve the connection settings for a connecting subcommand.
///
/// Precedence per setting: explicit flag / env › active context › built-in
/// localhost default. No config file AND no context → byte-identical to the
/// pre-context behavior. The derived data-plane URL is logged (structured
/// field per ADR-0054) so a wrong heuristic guess is visible at `-v`.
fn resolve_connection(cli: &Cli) -> Result<ResolvedConnection, CliError> {
    // Load + resolve the active context, if any.
    let resolved_ctx = load_active_context(cli)?;

    // --- Admin URL: flag/env › context.admin-service-url › default. ---
    let admin_url = cli
        .admin_url
        .clone()
        .or_else(|| resolved_ctx.as_ref().map(|c| c.admin_url.clone()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_ADMIN_URL.to_owned());

    // --- Service URL: flag/env › derived(context) › default. ---
    let service_url = cli
        .service_url
        .clone()
        .or_else(|| {
            resolved_ctx
                .as_ref()
                .and_then(|c| c.data_plane_url.clone())
                .inspect(|derived| {
                    tracing::info!(
                        target: "magnetar",
                        derived_service_url = %derived,
                        admin_service_url = %resolved_ctx.as_ref().map_or("", |c| c.admin_url.as_str()),
                        "deriving data-plane URL from context admin-service-url; \
                         pass --service-url to override",
                    );
                })
        })
        .unwrap_or_else(|| DEFAULT_SERVICE_URL.to_owned());

    // --- TLS settings: explicit flag › context. ---
    let trust_cert_path = cli
        .tls_trust_cert_path
        .clone()
        .or_else(|| {
            resolved_ctx
                .as_ref()
                .map(|c| c.tls.trust_cert_path.clone())
                .filter(|s| !s.is_empty())
        })
        .filter(|s| !s.is_empty());
    let admin_allow_insecure =
        cli.tls_allow_insecure || resolved_ctx.as_ref().is_some_and(|c| c.tls.allow_insecure);
    // When insecure TLS comes from the context rather than an explicit flag, the
    // operator gets no signal that certificate verification is off — unlike a
    // bad token (fail-closed → 401), this silently downgrades security. Warn.
    if !cli.tls_allow_insecure && admin_allow_insecure {
        tracing::warn!(
            target: "magnetar",
            context = resolved_ctx.as_ref().map_or("", |c| c.name.as_str()),
            "context has tls_allow_insecure_connection=true — TLS certificate verification is DISABLED",
        );
    }
    let admin_trust_cert_pem =
        match &trust_cert_path {
            Some(path) => Some(std::fs::read(path).map_err(|err| {
                CliError::BadArg(format!("--tls-trust-cert-path `{path}`: {err}"))
            })?),
            None => None,
        };
    // Client-certificate mTLS is not wired into either client yet. Accepting
    // the flags silently would hand a user a plain connection while they
    // believe mutual TLS is in effect — warn loudly that they are no-ops.
    if cli.tls_cert_file.is_some() || cli.tls_key_file.is_some() {
        tracing::warn!(
            target: "magnetar",
            cert_file = cli.tls_cert_file.as_deref().unwrap_or(""),
            key_file = cli.tls_key_file.as_deref().unwrap_or(""),
            "--tls-cert-file / --tls-key-file are accepted for pulsarctl parity but \
             client-certificate mTLS is not yet wired in; the connection will NOT present \
             a client certificate",
        );
    }

    // --- Auth: explicit token/token-file flag › context auth. ---
    let (admin_auth, data_auth) = resolve_auth(cli, resolved_ctx.as_ref())?;

    Ok(ResolvedConnection {
        admin_url,
        service_url,
        admin_auth,
        data_auth,
        admin_trust_cert_pem,
        admin_allow_insecure,
    })
}

/// Load + resolve the active context. `None` when there is no usable config /
/// context AND none was explicitly requested (caller uses defaults).
fn load_active_context(cli: &Cli) -> Result<Option<config::ResolvedContext>, CliError> {
    let resolved_path =
        config::resolve_path(cli.config.as_deref(), config::std_env).map_err(map_config_err)?;
    let Some(cfg) = config::load(&resolved_path).map_err(map_config_err)? else {
        return Ok(None);
    };
    config::resolve(&cfg, cli.context.as_deref()).map_err(|err| match err {
        config::ResolveError::NotFound(_) => CliError::BadArg(err.to_string()),
    })
}

/// Resolve admin + data-plane auth from explicit flags and the active context.
///
/// Explicit `--token` / `--token-file` win over the context. Within a context,
/// the single active method (token › token-file › `OAuth2`) was already selected
/// by [`config::resolve`].
fn resolve_auth(
    cli: &Cli,
    ctx: Option<&config::ResolvedContext>,
) -> Result<(AdminAuth, DataAuth), CliError> {
    // Explicit bearer token (flag/env) — highest precedence.
    if let Some(tok) = &cli.token {
        return Ok((AdminAuth::Token(tok.clone()), DataAuth::Token(tok.clone())));
    }
    // Explicit token file.
    if let Some(path) = &cli.token_file {
        let tok = read_token_file(path)?;
        return Ok((AdminAuth::Token(tok.clone()), DataAuth::Token(tok)));
    }

    // Context-derived auth.
    match ctx.map(|c| &c.auth) {
        Some(config::ResolvedAuth::Token(tok)) => {
            Ok((AdminAuth::Token(tok.clone()), DataAuth::Token(tok.clone())))
        }
        Some(config::ResolvedAuth::TokenFile(path)) => {
            let tok = read_token_file(path)?;
            Ok((AdminAuth::Token(tok.clone()), DataAuth::Token(tok)))
        }
        Some(config::ResolvedAuth::OAuth2(params)) => {
            let flow = std::sync::Arc::new(build_oauth2_flow(params)?);
            Ok((AdminAuth::OAuth2(flow.clone()), DataAuth::OAuth2(flow)))
        }
        Some(config::ResolvedAuth::None) | None => Ok((AdminAuth::None, DataAuth::None)),
    }
}

/// Read a bearer token from a file, trimming trailing whitespace/newline.
///
/// An empty (or whitespace-only) file is rejected here rather than producing a
/// malformed `Authorization: Bearer ` header that fails opaquely at the broker.
/// Guarding at the single read point covers both the `--token-file` flag and
/// the context-derived `tokenFile` arm.
fn read_token_file(path: &str) -> Result<String, CliError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| CliError::BadArg(format!("token file `{path}`: {err}")))?;
    let tok = raw.trim();
    if tok.is_empty() {
        return Err(CliError::BadArg(format!("token file `{path}` is empty")));
    }
    Ok(tok.to_owned())
}

/// Build an `OAuth2` `client_credentials` flow from resolved context params.
///
/// The `key_file` (pulsarctl Pulsar-style key file) carries the
/// `client_id` + `client_secret` as a JSON blob; an inline `client_id`
/// (context `client_id`) without a secret cannot complete the exchange, so the
/// key file is required when no other secret source is configured.
fn build_oauth2_flow(
    params: &config::resolve::OAuth2Params,
) -> Result<magnetar_auth_oauth2::ClientCredentialsFlow, CliError> {
    let issuer = params
        .issuer_endpoint
        .parse::<url::Url>()
        .map_err(|err| CliError::BadArg(format!("issuer_endpoint: {err}")))?;
    // The client_credentials flow POSTs client_id + client_secret as a form
    // body. Over a plaintext `http://` issuer that secret leaks on the wire, so
    // reject any non-https issuer endpoint up front.
    if issuer.scheme() != "https" {
        return Err(CliError::BadArg(
            "issuer_endpoint must use https (OAuth2 client_secret must not be sent over plaintext)"
                .to_owned(),
        ));
    }

    let credentials = oauth2_credentials(params)?;

    let mut builder = magnetar_auth_oauth2::ClientCredentialsFlow::builder()
        .issuer_url(issuer)
        .credentials(credentials);
    if !params.audience.is_empty() {
        builder = builder.audience(params.audience.clone());
    }
    if !params.scope.is_empty() {
        builder = builder.scope(params.scope.clone());
    }
    builder
        .build()
        .map_err(|err| CliError::BadArg(format!("oauth2 flow: {err}")))
}

/// Resolve `OAuth2` credentials from the `key_file` (a Pulsar-style JSON blob
/// with `client_id` + `client_secret`) or, as a fallback, fail with a clear
/// message: the context format has no inline `client_secret` field, so the key
/// file is the only secret source.
fn oauth2_credentials(
    params: &config::resolve::OAuth2Params,
) -> Result<magnetar_auth_oauth2::Credentials, CliError> {
    if params.key_file.is_empty() {
        return Err(CliError::BadArg(
            "OAuth2 context requires key_file (Pulsar-style client_id+client_secret JSON)"
                .to_owned(),
        ));
    }
    let text = std::fs::read_to_string(&params.key_file)
        .map_err(|err| CliError::BadArg(format!("key_file `{}`: {err}", params.key_file)))?;
    let blob: serde_json::Value = serde_json::from_str(&text)
        .map_err(|err| CliError::BadArg(format!("key_file `{}`: {err}", params.key_file)))?;
    // Pulsar's key file uses `client_id` / `client_secret`; fall back to the
    // context's `client_id` if the file omits it.
    let client_id = blob
        .get("client_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .filter(|s| !s.is_empty())
        .or_else(|| Some(params.client_id.clone()).filter(|s| !s.is_empty()))
        .ok_or_else(|| CliError::BadArg("key_file: missing client_id".to_owned()))?;
    let client_secret = blob
        .get("client_secret")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CliError::BadArg("key_file: missing client_secret".to_owned()))?;
    Ok(magnetar_auth_oauth2::Credentials::KeyFile {
        client_id,
        client_secret,
    })
}

/// Map a [`config::ConfigError`] into a [`CliError`]. Takes the error by value
/// so it can be used directly as a `.map_err(map_config_err)` adapter
/// (`FnOnce(ConfigError) -> CliError`).
#[allow(clippy::needless_pass_by_value)]
fn map_config_err(err: config::ConfigError) -> CliError {
    CliError::BadArg(err.to_string())
}

/// The GLOBAL credential / TLS flags that double as `context set` write
/// values (they cannot be redeclared on `set` without a clap name clash).
struct ContextSetGlobals {
    token: Option<String>,
    token_file: Option<String>,
    tls_trust_cert_path: Option<String>,
    /// `Some(true)` only when `--tls-allow-insecure` was explicitly passed, so
    /// the default `false` never clobbers an existing `true`.
    tls_allow_insecure: Option<bool>,
}

/// Dispatch the `context` command group. Operates on the config file resolved
/// from `--config` / `MAGNETAR_CONFIG` / the default path; never connects.
fn run_context(
    config_flag: Option<&str>,
    globals: &ContextSetGlobals,
    cmd: ContextCmd,
) -> Result<(), CliError> {
    let resolved_path =
        config::resolve_path(config_flag, config::std_env).map_err(map_config_err)?;
    // For reads/writes, a missing default-path file is an empty config we can
    // create on save; an explicit-but-missing path is only an error for verbs
    // that read existing state where that matters. `get` / `current` tolerate
    // an absent file (empty listing); mutating verbs create it.
    let mut cfg = load_or_default(&resolved_path)?;

    match cmd {
        ContextCmd::Use { name } => {
            if !cfg.contexts.contains_key(&name) {
                return Err(CliError::BadArg(format!("context not found: {name}")));
            }
            cfg.current_context.clone_from(&name);
            config::save(&resolved_path.path, &cfg).map_err(map_config_err)?;
            println!("Switched to context \"{name}\".");
            Ok(())
        }
        ContextCmd::Set { .. } => {
            let name = context_set(&mut cfg, globals, cmd);
            config::save(&resolved_path.path, &cfg).map_err(map_config_err)?;
            println!("Context \"{name}\" set.");
            Ok(())
        }
        ContextCmd::Delete { name } => {
            let had_ctx = cfg.contexts.remove(&name).is_some();
            let had_auth = cfg.auth_info.remove(&name).is_some();
            if !had_ctx && !had_auth {
                return Err(CliError::BadArg(format!("context not found: {name}")));
            }
            if cfg.current_context == name {
                // Match pulsarctl: warn but leave the (now-dangling) pointer.
                eprintln!(
                    "warning: deleted context \"{name}\" was the current context; \
                     set a new one with `magnetarctl context use <name>`"
                );
            }
            config::save(&resolved_path.path, &cfg).map_err(map_config_err)?;
            println!("Context \"{name}\" deleted.");
            Ok(())
        }
        ContextCmd::Get => {
            print_context_table(&cfg);
            Ok(())
        }
        ContextCmd::Current => {
            if cfg.current_context.is_empty() {
                return Err(CliError::BadArg("no current context set".to_owned()));
            }
            println!("{}", cfg.current_context);
            Ok(())
        }
        ContextCmd::Rename { old, new, force } => {
            // An existing destination would be silently destroyed by the move
            // below, so refuse unless `--force` is given.
            let overwriting =
                old != new && (cfg.contexts.contains_key(&new) || cfg.auth_info.contains_key(&new));
            if overwriting && !force {
                return Err(CliError::BadArg(format!(
                    "context \"{new}\" already exists; pass --force to overwrite it, \
                     delete it first, or pick another name"
                )));
            }
            let ctx = cfg
                .contexts
                .remove(&old)
                .ok_or_else(|| CliError::BadArg(format!("context not found: {old}")))?;
            cfg.contexts.insert(new.clone(), ctx);
            // The destination fully BECOMES the source: move `<old>`'s auth-info
            // onto `<new>`, and when `<old>` had none, drop any credentials that
            // were sitting on `<new>` so an overwrite never leaves a stale one.
            match cfg.auth_info.remove(&old) {
                Some(info) => {
                    cfg.auth_info.insert(new.clone(), info);
                }
                None => {
                    cfg.auth_info.remove(&new);
                }
            }
            if cfg.current_context == old {
                cfg.current_context.clone_from(&new);
            }
            config::save(&resolved_path.path, &cfg).map_err(map_config_err)?;
            if overwriting {
                eprintln!("warning: overwrote existing context \"{new}\".");
            }
            println!("Context \"{old}\" renamed to \"{new}\".");
            Ok(())
        }
    }
}

/// Apply a `context set` to `cfg`, merging flag values onto any existing
/// entries (unset flags leave fields untouched). Returns the context name.
/// The credential / TLS values come from the GLOBAL connection flags (they
/// cannot be redeclared on `set` without a clap name clash).
fn context_set(
    cfg: &mut config::PulsarConfig,
    globals: &ContextSetGlobals,
    cmd: ContextCmd,
) -> String {
    let ContextCmd::Set {
        name,
        admin_service_url,
        bookie_service_url,
        issuer_endpoint,
        client_id,
        audience,
        scope,
        key_file,
    } = cmd
    else {
        unreachable!("context_set is only called with ContextCmd::Set")
    };

    let ctx = cfg.contexts.entry(name.clone()).or_default();
    if let Some(v) = admin_service_url {
        ctx.admin_service_url = v;
    }
    if let Some(v) = bookie_service_url {
        ctx.bookie_service_url = v;
    }
    let info = cfg.auth_info.entry(name.clone()).or_default();
    if let Some(v) = &globals.tls_trust_cert_path {
        info.tls_trust_certs_file_path.clone_from(v);
    }
    if let Some(v) = globals.tls_allow_insecure {
        info.tls_allow_insecure_connection = v;
    }

    // Auth methods are mutually exclusive and resolved by precedence
    // (token › token_file › oauth2, see `config::resolve`). A `set` that
    // introduces one mode clears the others, so switching modes in a later
    // `set` cannot leave a stale higher-precedence credential that the resolver
    // keeps serving in preference to the one just configured.
    if let Some(v) = &globals.token {
        info.token.clone_from(v);
        info.token_file.clear();
        clear_oauth2(info);
    } else if let Some(v) = &globals.token_file {
        info.token_file.clone_from(v);
        info.token.clear();
        clear_oauth2(info);
    } else if issuer_endpoint.is_some() {
        info.token.clear();
        info.token_file.clear();
        set_oauth2(info, issuer_endpoint, client_id, audience, scope, key_file);
    } else {
        // No mode-defining flag: tweak oauth2 sub-fields in place (if provided)
        // without disturbing an existing token / token_file.
        set_oauth2(info, None, client_id, audience, scope, key_file);
    }
    name
}

/// Clear every `OAuth2` field on an `auth-info` entry.
fn clear_oauth2(info: &mut config::model::AuthInfo) {
    info.issuer_endpoint.clear();
    info.client_id.clear();
    info.audience.clear();
    info.scope.clear();
    info.key_file.clear();
}

/// Apply the provided `OAuth2` fields onto an `auth-info` entry; `None` fields
/// are left untouched.
fn set_oauth2(
    info: &mut config::model::AuthInfo,
    issuer_endpoint: Option<String>,
    client_id: Option<String>,
    audience: Option<String>,
    scope: Option<String>,
    key_file: Option<String>,
) {
    if let Some(v) = issuer_endpoint {
        info.issuer_endpoint = v;
    }
    if let Some(v) = client_id {
        info.client_id = v;
    }
    if let Some(v) = audience {
        info.audience = v;
    }
    if let Some(v) = scope {
        info.scope = v;
    }
    if let Some(v) = key_file {
        info.key_file = v;
    }
}

/// Load the config, treating an absent file (explicit OR default) as an empty
/// config the mutating verbs can create. `context` verbs are file-management,
/// so a not-yet-existing explicit path is a create target, not an error.
fn load_or_default(resolved: &config::ResolvedPath) -> Result<config::PulsarConfig, CliError> {
    if !resolved.path.exists() {
        return Ok(config::PulsarConfig::default());
    }
    match config::load(resolved).map_err(map_config_err)? {
        Some(cfg) => Ok(cfg),
        None => Ok(config::PulsarConfig::default()),
    }
}

/// Print the `context get` table: `CURRENT(*) NAME | ADMIN SERVICE URL |
/// BOOKIE SERVICE URL`, `*` on the current context.
#[allow(clippy::print_literal)]
fn print_context_table(cfg: &config::PulsarConfig) {
    println!(
        "{:<8} {:<28} {:<28} {}",
        "CURRENT", "NAME", "ADMIN SERVICE URL", "BOOKIE SERVICE URL"
    );
    for (name, ctx) in &cfg.contexts {
        let marker = if *name == cfg.current_context {
            "*"
        } else {
            ""
        };
        println!(
            "{:<8} {:<28} {:<28} {}",
            marker, name, ctx.admin_service_url, ctx.bookie_service_url
        );
    }
}

/// **Experimental** (PIP-460 / ADR-0031). Resolve a scalable topic's segment
/// DAG and print it as a table. Wraps
/// [`magnetar::PulsarClient::lookup_scalable_topic`].
// Width-formatted string-literal column headers are the idiomatic CLI table
// shape; `print_literal` would have us synthesise owned `String`s for no gain.
#[allow(clippy::print_literal)]
#[cfg(feature = "scalable-topics")]
async fn run_topic_info(service_url: &str, auth: DataAuth, topic: &str) -> Result<(), CliError> {
    if !magnetar::runtime_tokio::is_scalable_topic_url(topic) {
        return Err(CliError::BadArg(format!(
            "topic-info expects a scalable `topic://...` URL, got `{topic}`"
        )));
    }
    let client = build_data_client(service_url, auth).await?;
    let lookup = client
        .lookup_scalable_topic(topic)
        .await
        .map_err(|e| CliError::BadArg(format!("scalable lookup failed: {e}")))?;
    println!("topic: {topic}");
    if let Some(resolved) = lookup.resolved_topic_name.as_deref() {
        println!("resolved: {resolved}");
    }
    println!(
        "controller-broker: {}",
        lookup.controller_broker_url.as_deref().unwrap_or("-")
    );
    println!("layout-epoch: {}", lookup.epoch);
    println!(
        "{:<10} {:<18} {:<10} BROKER",
        "SEGMENT", "KEY-RANGE", "STATE"
    );
    for seg in &lookup.segments {
        let state = format!("{:?}", seg.state);
        // A sealed segment the broker no longer serves carries no placement.
        let broker = seg.broker_url.as_deref().unwrap_or("-");
        println!(
            "{:<10} [{:>5},{:>5}) {state:<10} {broker}",
            seg.segment_id.0, seg.key_range.start, seg.key_range.end,
        );
    }
    println!("({} segment(s))", lookup.segments.len());
    Ok(())
}

async fn run_admin(
    conn: &ResolvedConnection,
    timeout_secs: u64,
    cmd: AdminCmd,
) -> Result<(), CliError> {
    let admin = build_admin(conn, timeout_secs)?;
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
        AdminCmd::Packages { sub } => run_admin_packages(&admin, sub).await,
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
                .bookies_set_rack(&bookie, &group, BookieInfo { rack, hostname })
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

#[allow(clippy::too_many_lines)]
async fn run_admin_functions(admin: &AdminClient, cmd: FunctionsCmd) -> Result<(), CliError> {
    match cmd {
        FunctionsCmd::List { namespace } => {
            // Use the shared CLI helper so `tenant/ns/extra` (typo,
            // extra segment) fails fast with a clean BadArg error,
            // matching the symmetric `admin sources list` / `sinks
            // list` / `packages list` surfaces. Previously this used
            // an inline `split_once('/')` that accepted the right-hand
            // half verbatim — the admin client's `validate_segment` is
            // permissive on internal `/`, so the broker eventually
            // 404'd with a confusing `…/ns%2Fextra` URL.
            let (tenant, ns) = split_namespace_ref(&namespace).map_err(CliError::BadArg)?;
            print_json(&admin.functions_list_by_namespace(tenant, ns).await?)
        }
        FunctionsCmd::Get { name } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
            print_json(&admin.function_get(t, n, fn_name).await?)
        }
        FunctionsCmd::Status { name, instance_id } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
            let value = match instance_id {
                Some(id) => admin.function_instance_status(t, n, fn_name, id).await?,
                None => admin.function_status(t, n, fn_name).await?,
            };
            print_json(&value)
        }
        FunctionsCmd::Stats { name, instance_id } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
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
            user_config,
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
                user_config,
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
            user_config,
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
                user_config,
            };
            admin
                .function_update_with_url(&tenant, &namespace, &name, &url, cfg)
                .await?;
            Ok(())
        }
        FunctionsCmd::Delete { name } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
            admin.function_delete(t, n, fn_name).await?;
            Ok(())
        }
        FunctionsCmd::Start { name, instance_id } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
            match instance_id {
                Some(id) => admin.function_start_instance(t, n, fn_name, id).await?,
                None => admin.function_start(t, n, fn_name).await?,
            }
            Ok(())
        }
        FunctionsCmd::Stop { name, instance_id } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
            match instance_id {
                Some(id) => admin.function_stop_instance(t, n, fn_name, id).await?,
                None => admin.function_stop(t, n, fn_name).await?,
            }
            Ok(())
        }
        FunctionsCmd::Restart { name } => {
            let (t, n, fn_name) = split_io_id(&name).map_err(CliError::BadArg)?;
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

#[allow(clippy::too_many_lines)]
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

#[allow(clippy::too_many_lines)]
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
                "msgRateIn": stats.msg_rate_in,
                "msgRateOut": stats.msg_rate_out,
                "msgThroughputIn": stats.msg_throughput_in,
                "msgThroughputOut": stats.msg_throughput_out,
                "averageMsgSize": stats.average_msg_size,
                "msgInCounter": stats.msg_in_counter,
                "bytesInCounter": stats.bytes_in_counter,
                "storageSize": stats.storage_size,
                "backlogSize": stats.backlog_size,
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
            // `None` means the broker returned the `(-1, -1)` sentinel ("no
            // confirmed entry at terminate time") — surface as JSON `null` so
            // scripts can distinguish from a real message-id. A real id is
            // rendered by `message_id_to_json` (same shape as
            // `topics get-message-id-by-index`).
            let json = match admin.topic_terminate(&topic).await? {
                Some(id) => message_id_to_json(&id),
                None => serde_json::Value::Null,
            };
            print_json(&json)
        }
        TopicsCmd::UpdatePartitions { topic, partitions } => {
            admin.topic_update_partitions(&topic, partitions).await?;
            Ok(())
        }
        TopicsCmd::GetMessageIdByIndex { topic, index } => {
            let id = admin.topic_get_message_id_by_index(&topic, index).await?;
            print_json(&message_id_to_json(&id))
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
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
            print_json(&admin.source_get(tenant, ns, name).await?)
        }
        SourcesCmd::Status { source } => {
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
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
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
            admin.source_delete(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Start { source } => {
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
            admin.source_start(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Stop { source } => {
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
            admin.source_stop(tenant, ns, name).await?;
            Ok(())
        }
        SourcesCmd::Restart { source } => {
            let (tenant, ns, name) = split_io_id(&source).map_err(CliError::BadArg)?;
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
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
            print_json(&admin.sink_get(tenant, ns, name).await?)
        }
        SinksCmd::Status { sink } => {
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
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
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_delete(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Start { sink } => {
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_start(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Stop { sink } => {
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_stop(tenant, ns, name).await?;
            Ok(())
        }
        SinksCmd::Restart { sink } => {
            let (tenant, ns, name) = split_io_id(&sink).map_err(CliError::BadArg)?;
            admin.sink_restart(tenant, ns, name).await?;
            Ok(())
        }
    }
}

async fn run_admin_packages(admin: &AdminClient, cmd: PackagesCmd) -> Result<(), CliError> {
    match cmd {
        PackagesCmd::List {
            package_type,
            namespace,
        } => {
            let (tenant, ns) = split_namespace_ref(&namespace).map_err(CliError::BadArg)?;
            print_json(&admin.packages_list(package_type, tenant, ns).await?)
        }
        PackagesCmd::Versions {
            package_type,
            package,
        } => {
            let (tenant, ns, name) = split_io_id(&package).map_err(CliError::BadArg)?;
            print_json(
                &admin
                    .package_versions_list(package_type, tenant, ns, name)
                    .await?,
            )
        }
        PackagesCmd::MetadataGet {
            package_type,
            package,
            version,
        } => {
            let (tenant, ns, name) = split_io_id(&package).map_err(CliError::BadArg)?;
            print_json(
                &admin
                    .package_metadata_get(package_type, tenant, ns, name, &version)
                    .await?,
            )
        }
        PackagesCmd::MetadataSet {
            package_type,
            package,
            version,
            description,
            contact,
            properties,
        } => {
            let (tenant, ns, name) = split_io_id(&package).map_err(CliError::BadArg)?;
            let metadata = PackageMetadata {
                description,
                contact,
                // Read-only on the broker; it overwrites caller-supplied
                // values with its receive timestamp. Sending `0` keeps the
                // body shape honest without inviting a stale clock.
                modification_time: 0,
                properties: properties.into_iter().collect(),
            };
            admin
                .package_metadata_set(package_type, tenant, ns, name, &version, metadata)
                .await?;
            Ok(())
        }
        PackagesCmd::Delete {
            package_type,
            package,
            version,
        } => {
            let (tenant, ns, name) = split_io_id(&package).map_err(CliError::BadArg)?;
            admin
                .package_delete(package_type, tenant, ns, name, &version)
                .await?;
            Ok(())
        }
    }
}

/// Split `tenant/namespace/name` into its three segments. Used by
/// Pulsar IO Sources / Sinks and Pulsar Packages — the broker's
/// `SourcesBase` / `SinksBase` / `PackagesBase` resources all carry
/// the same `{tenant}/{namespace}/{name}` URL shape.
///
/// Thin wrapper over [`magnetar_admin::split_function_id`] so the
/// segment-validation rules (reject empty, `.`, `..`, `%2F` /
/// `%2f`, control bytes) match what every admin method enforces
/// internally — no parallel parsers, no divergent error categories.
/// The CLI surface stringifies the `AdminError` so it routes through
/// `CliError::BadArg` rather than `CliError::Admin`, matching the
/// "argument-parse-error → `BadArg`" convention every other CLI parser
/// uses.
fn split_io_id(spec: &str) -> Result<(&str, &str, &str), String> {
    magnetar_admin::split_function_id(spec).map_err(|e| e.to_string())
}

/// Split `tenant/namespace` into its two segments. Used by the
/// namespace-scoped `list` verbs on Pulsar Functions / IO Sources /
/// Sinks / Packages.
///
/// Thin wrapper over [`magnetar_admin::split_namespace`] (newly public)
/// so the validation rules match every admin method.
fn split_namespace_ref(spec: &str) -> Result<(&str, &str), String> {
    magnetar_admin::split_namespace(spec).map_err(|e| e.to_string())
}

fn build_admin(conn: &ResolvedConnection, timeout_secs: u64) -> Result<AdminClient, CliError> {
    let url = conn
        .admin_url
        .parse()
        .map_err(|err: url::ParseError| CliError::BadArg(format!("--admin-url: {err}")))?;
    let mut builder: AdminClientBuilder = AdminClient::builder()
        .service_url(url)
        .timeout(Duration::from_secs(timeout_secs));
    // Apply the resolved auth (token / oauth2 / none). The OAuth2 flow's token
    // cache is refreshed lazily on the first request inside the admin client.
    builder = match &conn.admin_auth {
        AdminAuth::None => builder,
        AdminAuth::Token(tok) => builder.token(tok.clone()),
        AdminAuth::OAuth2(flow) => builder.oauth2(flow.clone()),
    };
    if let Some(pem) = &conn.admin_trust_cert_pem {
        builder = builder.tls_trust_cert_pem(pem.clone());
    }
    if conn.admin_allow_insecure {
        builder = builder.tls_allow_insecure(true);
    }
    Ok(builder.build()?)
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), CliError> {
    let s = serde_json::to_string_pretty(value)?;
    println!("{s}");
    Ok(())
}

/// Render a [`MessageId`] as the canonical CLI JSON object, mirroring Java's
/// `MessageIdImpl.toString()` field layout. `MessageId` is a pure proto type
/// without a `Serialize` impl, so the shape is built by hand here and shared
/// by every command that emits a message id (`topics terminate`,
/// `topics get-message-id-by-index`). Under the `scalable-topics` feature the
/// id also carries an optional PIP-460 `segmentId`; we surface it (as JSON
/// `null` when absent) so the output faithfully represents the full type
/// rather than silently dropping the segment.
pub(crate) fn message_id_to_json(id: &MessageId) -> serde_json::Value {
    let value = serde_json::json!({
        "ledgerId": id.ledger_id,
        "entryId": id.entry_id,
        "partition": id.partition,
        "batchIndex": id.batch_index,
        "batchSize": id.batch_size,
    });
    #[cfg(feature = "scalable-topics")]
    let value = {
        let mut value = value;
        value["segmentId"] = id.segment_id.map(|s| s.0).into();
        value
    };
    value
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
/// kebab-case (operator-friendly) and the `snake_case` the broker REST
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

/// Parse a `--user-config '<json>'` argument into a `serde_json::Value`.
/// The broker stores a `Map<String, Object>` here, so we require an
/// object at the top level — a bare string / number / array would fail
/// at the broker boundary with a less helpful message.
fn parse_json_object(spec: &str) -> Result<serde_json::Value, String> {
    let value: serde_json::Value = serde_json::from_str(spec)
        .map_err(|err| format!("invalid JSON for --user-config: {err}"))?;
    if !value.is_object() {
        return Err(format!(
            "--user-config must be a JSON object, got {kind}",
            kind = match &value {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => unreachable!(),
            }
        ));
    }
    Ok(value)
}

/// Parse a Pulsar Packages `{type}` token from the CLI form. Delegates
/// to `PackageType::FromStr` (which accepts the broker's lowercase
/// tokens plus their pluralised aliases) and re-shapes the
/// `AdminError::InvalidName` into the plain `String` that clap's
/// `value_parser` expects.
fn parse_package_type(s: &str) -> Result<PackageType, String> {
    s.parse::<PackageType>().map_err(|e| e.to_string())
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
    auth: DataAuth,
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

    let client = build_data_client(service_url, auth).await?;
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
    auth: DataAuth,
    topic: &str,
    subscription: &str,
    sub_type: SubType,
    count: usize,
    ack: bool,
    replicate_subscription_state: bool,
) -> Result<(), CliError> {
    let client = build_data_client(service_url, auth).await?;
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

async fn build_data_client(service_url: &str, auth: DataAuth) -> Result<PulsarClient, CliError> {
    let mut builder = PulsarClient::builder().service_url(service_url);
    match auth {
        DataAuth::None => {}
        DataAuth::Token(t) => {
            let provider = std::sync::Arc::new(TokenAuth::from_string(t));
            builder = builder.auth(provider);
        }
        DataAuth::OAuth2(flow) => {
            // Prime the token cache so the flow's `AuthProvider::initial`
            // (called by the client at connect time) returns the access token
            // rather than erroring on an empty cache.
            flow.ensure_fresh()
                .await
                .map_err(|err| CliError::BadArg(format!("oauth2 token refresh: {err}")))?;
            builder = builder.auth(flow);
        }
    }
    Ok(builder.build().await?)
}
