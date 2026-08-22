// SPDX-License-Identifier: Apache-2.0

//! [`ClientBuilder`] — extracted from `client.rs` so the central
//! façade module stays focused on the [`crate::PulsarClient`] surface
//! and the per-surface builder types ([`crate::ProducerBuilder`],
//! [`crate::ConsumerBuilder`], [`crate::ReaderBuilder`]) that still
//! live alongside it.
//!
//! Re-exported via `pub use client_builder::ClientBuilder` from the
//! façade `lib.rs` so existing call sites
//! (`magnetar::ClientBuilder::default()`, `PulsarClient::builder()`)
//! keep working unchanged.

use std::time::Duration;

use magnetar_runtime_tokio::Client;

use crate::client::{MemoryLimit, MemoryLimitPolicy, PulsarClient, PulsarError};

/// Result alias used inside this module, mirroring the one in
/// `client.rs`.
type Result<T, E = PulsarError> = std::result::Result<T, E>;

/// Tri-state override for
/// [`ConnectionConfig::ack_response_timeout`](magnetar_proto::conn::ConnectionConfig) (issue #346).
/// A plain `Option<Duration>` can't represent "explicitly disabled" separately from "unset, inherit
/// the default" because the underlying config field is itself `Option<Duration>` with a non-`None`
/// default (`Some(30s)`) — unlike `operation_timeout`, whose config-level
/// type is a bare `Duration`. `clippy::option_option` steers this shape into
/// a named enum instead of `Option<Option<Duration>>`.
#[derive(Debug, Clone, Copy)]
enum AckResponseTimeoutOverride {
    /// Explicit deadline via [`ClientBuilder::ack_response_timeout`].
    Explicit(Duration),
    /// Explicitly disabled via [`ClientBuilder::disable_ack_response_timeout`].
    Disabled,
}

/// Builder for [`PulsarClient`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    service_url: Option<String>,
    service_url_provider: Option<std::sync::Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    client_version: Option<String>,
    keepalive: Option<Duration>,
    /// `None` = unset, inherit `ConnectionConfig::default()`'s `stats_interval`.
    /// `Some(Duration::ZERO)` is the Java `statsIntervalSeconds = 0` disable.
    stats_interval: Option<Duration>,
    /// `None` = unset, inherit `ConnectionConfig::default()`'s `None` (watchdog off).
    /// `Some(Duration::ZERO)` disables it explicitly (issue #414).
    consumer_stall_timeout: Option<Duration>,
    /// `None` = unset, inherit `ConnectionConfig::default()`'s `None` (automatic recovery
    /// off). `Some(0)` disables it explicitly (issue #414, ADR-0103).
    consumer_stall_auto_recovery: Option<u32>,
    operation_timeout: Option<Duration>,
    operation_retry: Option<magnetar_proto::OperationRetryConfig>,
    /// `None` = unset, inherit `ConnectionConfig::default()`'s `Some(30s)`.
    ack_response_timeout: Option<AckResponseTimeoutOverride>,
    auth_method_name: Option<String>,
    auth_data: Option<bytes::Bytes>,
    auth_provider: Option<std::sync::Arc<dyn magnetar_proto::AuthProvider>>,
    tls_trust_certs_pem: Option<Vec<u8>>,
    tls_allow_insecure_connection: bool,
    tls_hostname_verification_enable: bool,
    default_max_message_size: Option<usize>,
    proxy_to_broker_url: Option<String>,
    supervisor: Option<magnetar_proto::SupervisorConfig>,
    memory_limit: Option<MemoryLimit>,
    dns_resolver: Option<std::sync::Arc<dyn magnetar_runtime_tokio::DnsResolver>>,
    connections_per_broker: Option<usize>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            service_url: None,
            service_url_provider: None,
            client_version: None,
            keepalive: None,
            stats_interval: None,
            consumer_stall_timeout: None,
            consumer_stall_auto_recovery: None,
            operation_timeout: None,
            operation_retry: None,
            ack_response_timeout: None,
            auth_method_name: None,
            auth_data: None,
            auth_provider: None,
            tls_trust_certs_pem: None,
            tls_allow_insecure_connection: false,
            tls_hostname_verification_enable: true,
            default_max_message_size: None,
            proxy_to_broker_url: None,
            supervisor: None,
            memory_limit: None,
            dns_resolver: None,
            connections_per_broker: None,
        }
    }
}

impl ClientBuilder {
    /// Set the Pulsar service URL (`pulsar://` or `pulsar+ssl://`).
    #[must_use]
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = Some(url.into());
        self
    }

    /// Plug in a custom DNS resolver. Mirrors Java
    /// `ClientBuilder#dnsResolver`. Used on every connection attempt
    /// (initial + reconnect) instead of tokio's default
    /// [`tokio::net::lookup_host`]. Useful for service-mesh sidecar
    /// resolution, IPv4/IPv6 preference, pinning, etc.
    ///
    /// Default: tokio's built-in DNS via
    /// [`magnetar_runtime_tokio::TokioDnsResolver`].
    #[must_use]
    pub fn dns_resolver(
        mut self,
        resolver: std::sync::Arc<dyn magnetar_runtime_tokio::DnsResolver>,
    ) -> Self {
        self.dns_resolver = Some(resolver);
        self
    }

    /// Set the global publish memory budget for the client. Mirrors Java
    /// `ClientBuilder#memoryLimit(long, MemoryLimitPolicy)`. `bytes = 0`
    /// disables the limit (matches Java default).
    ///
    /// **Enforcement**: under `MemoryLimitPolicy::FailImmediately`, every
    /// `Producer::send` reserves the payload bytes against the budget via
    /// an `AtomicU64` CAS loop on `ConnectionShared::memory_used` BEFORE
    /// the payload reaches the sans-io state machine. Sends that would
    /// push past the limit are rejected synchronously with
    /// [`magnetar_runtime_tokio::ClientError::MemoryLimitExceeded`]. The
    /// reservation is released on `SendFut` completion (success or
    /// error) and on cancellation (via `Drop`).
    ///
    /// Under `MemoryLimitPolicy::ProducerBlock`, the send future parks
    /// on a `Notify`-based wait until the budget frees up — both engines
    /// (`TokioEngine`, `MoonpoolEngine<P>`) implement this policy; see
    /// [`docs/memory-limit.md`](https://github.com/FlorentinDUBOIS/magnetar/blob/main/docs/memory-limit.md).
    #[must_use]
    pub fn memory_limit(mut self, bytes: usize, policy: MemoryLimitPolicy) -> Self {
        self.memory_limit = Some(MemoryLimit { bytes, policy });
        self
    }

    /// Set the number of connections the client opens to **each broker**. Mirrors
    /// Java `ClientBuilder#connectionsPerBroker(int)` (issue #314, [ADR-0073]).
    ///
    /// Default (and `0`/`1`): **one** connection per broker — every producer and
    /// consumer for a given broker shares a single TCP connection, exactly as
    /// before. With `n > 1`, the client opens up to `n` connections per broker
    /// and round-robins producers / consumers across them, so a single logical
    /// producer fleet can spread its publish load over several independent
    /// connections instead of contending on one (the per-connection driver, its
    /// send path, and its receipt-read path are independent per connection).
    /// This removes the send-side back-pressure that otherwise forces
    /// applications to hand-roll a pool of [`PulsarClient`]s.
    ///
    /// `0` is treated as `1` (matching Java, where the floor is one connection).
    ///
    /// [ADR-0073]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0073-connections-per-broker.md
    #[must_use]
    pub fn connections_per_broker(mut self, n: usize) -> Self {
        self.connections_per_broker = Some(n.max(1));
        self
    }

    /// Set a pluggable [`magnetar_proto::ServiceUrlProvider`] consulted on every
    /// (re)connection attempt. Mirrors Java
    /// `ClientBuilder#serviceUrlProvider(ServiceUrlProvider)` — lays the groundwork
    /// for PIP-121 cluster failover (`AutoClusterFailover` /
    /// `ControlledClusterFailover`). When set, the provider's
    /// `get_service_url()` is used at connect time; the unset form retains the
    /// legacy `service_url(...)` shortcut and is internally wrapped in a
    /// [`magnetar_proto::StaticServiceUrlProvider`] at build time.
    #[must_use]
    pub fn service_url_provider(
        mut self,
        provider: std::sync::Arc<dyn magnetar_proto::ServiceUrlProvider>,
    ) -> Self {
        self.service_url_provider = Some(provider);
        self
    }

    /// Override the advertised client version.
    #[must_use]
    pub fn client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Set the keep-alive (ping) interval.
    #[must_use]
    pub fn keepalive(mut self, dur: Duration) -> Self {
        self.keepalive = Some(dur);
        self
    }

    /// Set the cadence at which the client re-samples every producer's and
    /// consumer's rolling rate window — the sampling that makes
    /// [`magnetar_proto::ProducerStats::msgs_per_sec`] / `bytes_per_sec` and
    /// their [`magnetar_proto::ConsumerStats`] counterparts nonzero. Mirrors
    /// Java `ClientBuilder#statsInterval(long, TimeUnit)`.
    ///
    /// The tick runs inside the sans-io state machine's existing
    /// `poll_timeout` / `handle_timeout` deadline loop (ADR-0089), so it
    /// applies to **every** producer and consumer on the client — including
    /// the per-partition and per-topic children behind
    /// [`crate::PartitionedProducer`], [`crate::MultiTopicsConsumer`] and
    /// [`crate::PatternConsumer`], whose `aggregate_stats()` folds therefore
    /// sum real rates rather than zeros. There is deliberately no per-wrapper
    /// fan-out method: Java's wrappers have none either, and one clock ticking
    /// every child is what makes the folded sum well-defined.
    ///
    /// `Duration::ZERO` disables the sweep, spelling Java's
    /// `statsIntervalSeconds = 0`. Leaving the knob unset inherits
    /// [`ConnectionConfig::stats_interval`](magnetar_proto::conn::ConnectionConfig)'s
    /// default.
    ///
    /// A producer or consumer created mid-window has no baseline yet, so its
    /// first sweep only seeds one and it reports `0.0` for one further
    /// interval. Java behaves identically.
    #[must_use]
    pub fn stats_interval(mut self, dur: Duration) -> Self {
        self.stats_interval = Some(dur);
        self
    }

    /// Arm the per-consumer stall watchdog (issue #414).
    ///
    /// A consumer that holds un-spent broker permits over an empty receive queue, in a
    /// dispatch-eligible state, for `dur` without a single dispatch unit arriving surfaces
    /// one `warn!` and one
    /// [`ConnectionEvent::ConsumerStalled`](magnetar_proto::event::ConnectionEvent::ConsumerStalled)
    /// — exactly one per stall episode, re-armed by the next dispatch. That is its only
    /// effect unless [`Self::consumer_stall_auto_recovery`] is also set; otherwise
    /// recovery stays an explicit call to `Consumer::resubscribe()`, escalating to an
    /// operator-side `pulsar-admin topics unload` for a dispatcher-wide broker fault.
    ///
    /// This is the one silence the ADR-0058 connection keepalive cannot see: a broker whose
    /// dispatcher has wedged for ONE subscription keeps answering `PING` with `PONG`.
    ///
    /// **Unset by default** — the mechanism ships disarmed, since an armed deadline
    /// perturbs the moonpool engine's simulated wake schedule even when it never fires, and
    /// Java has no per-consumer dispatch watchdog to inherit a parity value from.
    /// `Duration::from_secs(30)` is the recommended production value: it matches the
    /// keepalive and ack-response cadences, and is far longer than any legitimate dispatch
    /// gap on a subscription that holds permits over an empty queue.
    ///
    /// `Duration::ZERO` disables it explicitly, mirroring how
    /// [`Self::stats_interval`] spells its disable.
    #[must_use]
    pub fn consumer_stall_timeout(mut self, dur: Duration) -> Self {
        self.consumer_stall_timeout = Some(dur);
        self
    }

    /// Let the stall watchdog recover a wedged consumer by itself, at most `max_attempts`
    /// times per stall streak (issue #414, ADR-0103).
    ///
    /// Each attempt is the same in-place re-attach `Consumer::resubscribe()` performs —
    /// zero this client's permit mirrors, fail the orphaned in-flight acks, re-emit
    /// `CommandSubscribe` for the same consumer id on the live connection, and let the
    /// broker's `Success` release a fresh initial `CommandFlow`. No transport reconnect,
    /// no other consumer or producer disturbed, and the receiver queue left intact.
    ///
    /// The `ConsumerStalled` event and its `warn!` are emitted either way, so arming this
    /// adds a recovery attempt without ever hiding the diagnosis.
    ///
    /// **Requires [`Self::consumer_stall_timeout`]** — with no window there is no stall
    /// episode, and this knob is inert. **Unset by default**, and `0` disables it
    /// explicitly, mirroring how [`Self::consumer_stall_timeout`] spells its disable.
    ///
    /// # Choosing the bound
    ///
    /// At most one attempt is made per stall episode and an episode closes at most once
    /// per `consumer_stall_timeout`, so `max_attempts` caps a sequence already limited to
    /// one re-subscribe per window: with a 30 s window, `3` spends at most three
    /// re-subscribes over ninety seconds before giving up. The counter resets on **real
    /// progress only** — one broker dispatch unit actually arriving — so a consumer that
    /// recovers and later wedges again gets its full budget back, while a consumer the
    /// broker acks but never dispatches to does not.
    ///
    /// Keep it small. An attempt repairs **this client's own slot** in the broker's
    /// dispatcher and lifts the subscription's aggregate permit counter by one
    /// receiver-queue window; issue #414's production failure was dispatcher-WIDE, with
    /// that aggregate observed at `-177300`, which no realistic number of re-subscribes
    /// reaches. When the budget is exhausted the client stops and logs the escalation —
    /// `pulsar-admin topics unload` — instead of re-subscribing forever against a fault it
    /// cannot repair. See [`docs/consumer-stall-recovery.md`](https://github.com/CleverCloud/magnetar/blob/main/docs/consumer-stall-recovery.md).
    #[must_use]
    pub fn consumer_stall_auto_recovery(mut self, max_attempts: u32) -> Self {
        self.consumer_stall_auto_recovery = Some(max_attempts);
        self
    }

    /// Set the total deadline for one broker-facing setup operation.
    ///
    /// The budget includes partition metadata, topic-list snapshots, lookup
    /// and redirect routing, retry backoff, producer-open or subscribe
    /// attachment, and every child of a composite builder. The operation
    /// preserves the newest retryable broker diagnostic so a later deadline
    /// returns it instead of a generic timeout.
    #[must_use]
    pub fn operation_timeout(mut self, dur: Duration) -> Self {
        self.operation_timeout = Some(dur);
        self
    }

    /// Configure broker-operation retries independently from transport
    /// reconnection.
    ///
    /// Applies to lookup, partition metadata, producer-open, and subscribe.
    /// Producer-open additionally retries both producer-quota variants and
    /// `ProducerBusy`; subscribe additionally retries `ConsumerBusy`.
    /// Before first attachment, producer-open and subscribe retries re-run
    /// lookup and routing with a fresh provisional handle. Established
    /// reattachment remains driver-owned.
    /// `max_retries` counts re-issues after the initial attempt; `None`
    /// removes the count cap but the enclosing [`Self::operation_timeout`]
    /// deadline still bounds the operation.
    #[must_use]
    pub fn operation_retry(mut self, config: magnetar_proto::OperationRetryConfig) -> Self {
        self.operation_retry = Some(config);
        self
    }

    /// Bound how long the client waits for a `CommandAckResponse` after
    /// issuing a `CommandAck`. In-flight acks past `enqueued_at + timeout`
    /// resolve with a synthetic broker error carrying `code=-1,
    /// message="ack timeout"` on the next state-machine tick — mirrors
    /// [`crate::ProducerBuilder::send_timeout`]'s shape and rationale.
    ///
    /// The default is **30 s** (mirrors the `send_timeout` Java-parity
    /// default, ADR-0072), so an ack whose response is lost or dropped in
    /// flight fails deterministically rather than hanging the caller's
    /// `ack().await` forever. A same-broker `CloseConsumer` (bundle
    /// reassignment, issue #307) additionally fails every ack pending
    /// against the torn-down consumer id immediately, ahead of this
    /// deadline — this knob is the generic backstop for every other cause
    /// of a dropped response. Call [`Self::disable_ack_response_timeout`]
    /// for the unbounded (never-times-out) behavior.
    #[must_use]
    pub fn ack_response_timeout(mut self, timeout: Duration) -> Self {
        self.ack_response_timeout = Some(AckResponseTimeoutOverride::Explicit(timeout));
        self
    }

    /// Disable the ack-response timeout: in-flight acks never resolve with a
    /// synthetic timeout error — they wait indefinitely for the broker's
    /// `CommandAckResponse` (or a session-loss / terminal error, or the
    /// same-broker `CloseConsumer` orphan sweep, which is unaffected by this
    /// knob). Overrides the 30 s default.
    #[must_use]
    pub fn disable_ack_response_timeout(mut self) -> Self {
        self.ack_response_timeout = Some(AckResponseTimeoutOverride::Disabled);
        self
    }

    /// Override the default `max_message_size` used as the chunking threshold when the
    /// broker does not advertise one on `CommandConnected`. The Pulsar default is 5 MiB;
    /// match the broker's configured `maxMessageSize` to avoid mis-sized chunks. Mirrors
    /// Java `ClientBuilder#maxMessageSize`.
    #[must_use]
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.default_max_message_size = Some(size);
        self
    }

    /// Set the proxy-to-broker URL for the binary proxy path. The connection then opens
    /// against the proxy with the broker URL stamped on the `CommandConnect.proxy_to_broker_url`
    /// field. Mirrors Java `ClientBuilder#proxyServiceUrl(... ProxyProtocol.SNI)`. Leave
    /// unset for direct broker connections.
    #[must_use]
    pub fn proxy_to_broker_url(mut self, url: impl Into<String>) -> Self {
        self.proxy_to_broker_url = Some(url.into());
        self
    }

    /// Enable the auto-reconnect supervisor with the supplied
    /// [`magnetar_proto::SupervisorConfig`]. When set, runtime engines wrap the driver
    /// loop in a [`magnetar_proto::Backoff`]-driven reconnect cycle so the connection
    /// survives transport failures. Without this knob the driver exits on the first
    /// I/O error (matches the pre-supervisor behavior). Mirrors Java's
    /// `PulsarClientImpl` reconnect loop.
    ///
    /// Note: pending in-flight producer/consumer requests issued before the drop
    /// surface a "session lost" outcome on the new connection; transparent
    /// re-subscription and producer reattachment across reconnects is a future
    /// enhancement layered on top of this scaffold.
    #[must_use]
    pub fn enable_reconnect(mut self, config: magnetar_proto::SupervisorConfig) -> Self {
        self.supervisor = Some(config);
        self
    }

    /// Use the supplied auth provider to populate the initial CONNECT auth data,
    /// and keep the provider for in-band `CommandAuthChallenge` refresh
    /// (PIP-30 / PIP-292).
    ///
    /// **BREAKING CHANGE**: the provider's [`magnetar_proto::AuthProvider::initial`]
    /// is now invoked inside [`Self::build`] and any error it returns
    /// surfaces through [`PulsarError::Config`] — the previous behaviour
    /// silently dropped the error via `.ok()`, which would have let an
    /// uncached `OAuth2` flow / a missing token file / an expired credential
    /// open an *anonymous* connection (CWE-287). Callers using a provider
    /// whose `initial()` returns `Err(AuthError::Invalid)` until an
    /// out-of-band warm-up runs (e.g. `OAuth2Provider::ensure_fresh`) MUST
    /// warm the provider before calling [`Self::build`].
    #[must_use]
    pub fn auth(mut self, provider: std::sync::Arc<dyn magnetar_proto::AuthProvider>) -> Self {
        self.auth_method_name = Some(provider.method().to_owned());
        // NOTE: we deliberately do NOT call `provider.initial()` here. The
        // previous `.ok()` swallowed errors and let an unwarmed provider
        // produce an anonymous connection. The fetch + error propagation
        // now lives in `build()`.
        self.auth_provider = Some(provider);
        self
    }

    /// Mirrors Java `ClientBuilder#tlsTrustCertsFilePath` (PEM-supplied
    /// equivalent — magnetar keeps the façade I/O-free, callers read the
    /// file themselves via `std::fs::read(path)?` and pass the bytes).
    /// Supplies a PEM-encoded chain (typically a self-signed CA used by
    /// the broker). When set, the connection's TLS handshake validates
    /// the broker against this chain INSTEAD OF the system trust
    /// store. Only honoured for `pulsar+ssl://` URLs.
    #[must_use]
    pub fn tls_trust_certs_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls_trust_certs_pem = Some(pem.into());
        self
    }

    /// Mirror of Java `ClientBuilder#tlsAllowInsecureConnection`. When `true`,
    /// the TLS handshake accepts any server certificate without verifying its
    /// trust chain — useful for local development against a self-signed broker
    /// or for CI / e2e against an ephemeral container. **Insecure for
    /// production**: the client cannot tell a real broker from a MITM.
    ///
    /// Default: `false`. Only honoured for `pulsar+ssl://` URLs. Overrides any
    /// `tls_trust_certs_pem` chain when set.
    #[must_use]
    pub fn tls_allow_insecure_connection(mut self, on: bool) -> Self {
        self.tls_allow_insecure_connection = on;
        self
    }

    /// Mirror of Java `ClientBuilder#enableTlsHostnameVerification`. When
    /// `true` (the default), the handshake additionally checks the server
    /// certificate's CN / SAN matches the broker hostname from the URL. When
    /// `false`, the chain is still verified but the hostname mismatch is
    /// tolerated.
    ///
    /// Default: `true` (matches Java's secure default). When
    /// [`Self::tls_allow_insecure_connection`] is `true` this flag is moot —
    /// the verifier already accepts everything.
    ///
    /// **Note**: today only the "off + insecure both true" combination is
    /// runtime-enforced via [`magnetar_runtime_tokio::insecure_tls_config`].
    /// A hostname-only-skip verifier (chain on, hostname off) is a planned
    /// follow-up; passing `false` without also enabling
    /// `tls_allow_insecure_connection` is currently treated as the default
    /// (hostname verification stays on).
    #[must_use]
    pub fn tls_hostname_verification_enable(mut self, on: bool) -> Self {
        self.tls_hostname_verification_enable = on;
        self
    }

    /// Build and connect the client.
    ///
    /// # Errors
    /// Returns [`PulsarError::Config`] if the service URL is missing, or
    /// [`PulsarError::Client`] if the underlying tokio engine fails to
    /// connect.
    // The function is a flat config-translation: tls flavour cases on top, then config field
    // copies, then the connect-flavour dispatch. Inlined for readability — each branch is
    // straight-line and the dispatch is easier to follow without an extracted helper that
    // would have to forward every config field anyway.
    #[allow(clippy::too_many_lines)]
    pub async fn build(self) -> Result<PulsarClient> {
        let service_url = match (&self.service_url_provider, &self.service_url) {
            (Some(provider), _) => provider.get_service_url(),
            (None, Some(url)) => url.clone(),
            (None, None) => {
                return Err(PulsarError::Config(
                    "service_url or service_url_provider is required".to_owned(),
                ));
            }
        };
        // `connections_per_broker` is a runtime connection-pool policy (it never
        // reaches the sans-io `magnetar-proto` core — ADR-0004/ADR-0073), so it is
        // applied to the runtime `Client` after connect rather than threaded into
        // `ConnectionConfig`. Captured here (it is `Copy`) before `self` is moved
        // into the connect-flavour branches below.
        let connections_per_broker = self.connections_per_broker;
        let operation_retry = self.operation_retry.clone();
        let mut config = magnetar_proto::conn::ConnectionConfig::default();
        if let Some(v) = self.client_version {
            config.client_version = v;
        }
        if let Some(d) = self.keepalive {
            config.keepalive_interval = d;
        }
        // ADR-0089 / Java `statsIntervalSeconds`: zero disables the sweep, any
        // other value arms it. Unset leaves `ConnectionConfig::default()`'s
        // value untouched, which is Java's 60 s — so a caller who never touched
        // this knob still gets Java-parity sampling.
        if let Some(d) = self.stats_interval {
            config.stats_interval = (d != Duration::ZERO).then_some(d);
        }
        // Issue #414: same zero-disables shape as `stats_interval` above. Unset leaves
        // `ConnectionConfig::default()`'s `None` — the watchdog is opt-in.
        if let Some(d) = self.consumer_stall_timeout {
            config.consumer_stall_timeout = (d != Duration::ZERO).then_some(d);
        }
        // ADR-0103: the same zero-disables shape one knob up, spelled in attempts rather
        // than in a `Duration`. Unset leaves `ConnectionConfig::default()`'s `None`, and
        // the whole mechanism is inert anyway without `consumer_stall_timeout`.
        if let Some(n) = self.consumer_stall_auto_recovery {
            config.consumer_stall_auto_recovery = (n != 0).then_some(n);
        }
        if let Some(d) = self.operation_timeout {
            config.operation_timeout = d;
        }
        // Explicit or disabled always wins over `ConnectionConfig::default()`'s
        // `Some(30s)`; unset (`None`) leaves the default untouched.
        match self.ack_response_timeout {
            Some(AckResponseTimeoutOverride::Explicit(d)) => config.ack_response_timeout = Some(d),
            Some(AckResponseTimeoutOverride::Disabled) => config.ack_response_timeout = None,
            None => {}
        }
        if let Some(s) = self.default_max_message_size {
            config.default_max_message_size = s;
        }
        if let Some(url) = self.proxy_to_broker_url {
            config.proxy_to_broker_url = Some(url);
        }
        if let Some(sv) = self.supervisor {
            config.supervisor = Some(sv);
        }
        // Java `ClientBuilder#memoryLimit` — wire the configured budget into the runtime so
        // `Producer::send` reserves payload bytes against `ConnectionShared::memory_limit_bytes`
        // before queueing. Both `FailImmediately` and `ProducerBlock` are honored by the
        // tokio and moonpool engines (the latter parks the send future on a `Notify` wait
        // until the budget frees up).
        if let Some(limit) = self.memory_limit {
            // Cast saturates rather than truncates so a 64-bit limit on a 32-bit usize host
            // (effectively impossible — magnetar requires 64-bit pointers — but cheap to
            // future-proof) stays correct.
            config.memory_limit_bytes = limit.bytes as u64;
            config.memory_limit_policy = limit.policy.into();
        }
        if let Some(name) = self.auth_method_name {
            config.auth_method_name = name;
        }
        // BREAKING CHANGE: surface the provider's `initial()` failure here
        // rather than silently dropping it via `.ok()` in `auth(...)`. A
        // missing token file or an unwarmed OAuth2 cache used to slip
        // through and produce an anonymous CONNECT (CWE-287). The
        // direct-bytes `self.auth_data` set via internal call sites still
        // wins when present (matches the prior precedence).
        if let Some(data) = self.auth_data {
            config.auth_data = Some(data);
        } else if let Some(provider) = self.auth_provider.as_ref() {
            let bytes = provider.initial().map_err(|err| {
                PulsarError::Config(format!(
                    "auth provider initial() failed; cannot open authenticated connection: {err}"
                ))
            })?;
            config.auth_data = Some(bytes);
        }
        // Java `ClientBuilder#dnsResolver` — when configured, every reconnect (including the
        // initial dial) routes through `provider.resolve(host, port)` via
        // `Client::connect_with_resolver_and_provider`. When unset, the runtime falls back to
        // tokio's built-in `lookup_host` (and we can keep using the lighter `connect_auth`
        // shortcut when none of TLS / provider / resolver is configured).
        let inner = if self.tls_allow_insecure_connection {
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    Some(magnetar_runtime_tokio::insecure_tls_config())
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else if let Some(pem) = self.tls_trust_certs_pem {
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    // Java parity: `enableTlsHostnameVerification(false)` paired with a
                    // PEM trust store keeps the chain check but skips the hostname match.
                    if self.tls_hostname_verification_enable {
                        Some(Client::tls_config_from_pem(&pem)?)
                    } else {
                        Some(magnetar_runtime_tokio::tls_config_no_hostname(&pem)?)
                    }
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else if self.service_url_provider.is_some() || self.dns_resolver.is_some() {
            // Provider OR resolver configured but no explicit TLS / PEM. Go through the
            // provider+resolver-aware path so PIP-121 rotation AND custom DNS work on
            // reconnect — `connect_auth` doesn't accept either arg.
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => {
                    Some(magnetar_runtime_tokio::default_tls_config()?)
                }
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with_resolver_and_provider(
                parsed,
                tls_config,
                config,
                self.auth_provider,
                self.service_url_provider,
                self.dns_resolver,
            )
            .await?
        } else {
            Client::connect_auth(&service_url, config, self.auth_provider).await?
        };
        // Java `ClientBuilder#connectionsPerBroker` — apply the fan-out to the
        // runtime client now that the bootstrap connection is up (ADR-0073, #314).
        let inner = match operation_retry {
            Some(config) => inner.with_operation_retry(config),
            None => inner,
        };
        let inner = match connections_per_broker {
            Some(n) => inner.with_connections_per_broker(n),
            None => inner,
        };
        Ok(PulsarClient {
            inner,
            memory_limit: self.memory_limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use magnetar_proto::{AuthError, AuthProvider, OperationRetryConfig};

    use super::ClientBuilder;
    use crate::{MemoryLimitPolicy, PulsarError};

    #[test]
    fn memory_limit_policy_converts_exhaustively_to_proto() {
        assert_eq!(
            magnetar_proto::MemoryLimitPolicy::from(MemoryLimitPolicy::FailImmediately),
            magnetar_proto::MemoryLimitPolicy::FailImmediately,
        );
        assert_eq!(
            magnetar_proto::MemoryLimitPolicy::from(MemoryLimitPolicy::ProducerBlock),
            magnetar_proto::MemoryLimitPolicy::ProducerBlock,
        );
    }

    /// Stub provider whose `initial()` returns `Err(AuthError::Invalid)`.
    /// Models an unwarmed `OAuth2` cache, a missing token file, or any other
    /// provider whose credential-fetch failed.
    #[derive(Debug)]
    struct FailingProvider;

    impl AuthProvider for FailingProvider {
        fn method(&self) -> &str {
            "token"
        }
        fn initial(&self) -> Result<Bytes, AuthError> {
            Err(AuthError::Invalid("forced failure (test)".to_owned()))
        }
    }

    #[test]
    fn operation_retry_builder_knob_stores_the_independent_policy() {
        let policy = OperationRetryConfig {
            initial_backoff: std::time::Duration::from_millis(25),
            max_backoff: std::time::Duration::from_millis(200),
            max_retries: Some(4),
        };
        let builder = ClientBuilder::default().operation_retry(policy.clone());
        assert_eq!(builder.operation_retry, Some(policy));
        assert!(
            builder.supervisor.is_none(),
            "operation retry must not implicitly enable transport reconnection"
        );
    }

    /// BREAKING CHANGE regression (F6, CWE-287): `ClientBuilder::auth(...)`
    /// used to call `provider.initial().ok()`, silently dropping the error
    /// and leaving `auth_data = None`. The resulting CONNECT carried no
    /// credentials and the broker happily opened an *anonymous* session
    /// when its auth plugin allowed it — a textbook authentication-bypass
    /// vector when the provider is the only thing standing between the
    /// caller and an anonymous connection.
    ///
    /// The fix defers `provider.initial()` to `build()` and surfaces the
    /// failure through `PulsarError::Config`. This test pins that contract:
    /// no anonymous fallback, no broker dial, just an early `Err`.
    #[tokio::test(flavor = "current_thread")]
    async fn build_propagates_auth_provider_initial_error() {
        let provider = std::sync::Arc::new(FailingProvider);
        let result = ClientBuilder::default()
            // Localhost target is fine — `build()` must surface the auth
            // error BEFORE the dial, so no listener is required.
            .service_url("pulsar://127.0.0.1:1")
            .auth(provider)
            .build()
            .await;
        let err = result.expect_err(
            "build() must surface auth provider initial() error, not silently \
             fall back to an anonymous CONNECT (CWE-287)",
        );
        match err {
            PulsarError::Config(msg) => {
                assert!(
                    msg.contains("auth provider initial()"),
                    "error must point at the auth path: {msg}"
                );
                assert!(
                    msg.contains("forced failure (test)"),
                    "error must propagate the provider's message: {msg}"
                );
            }
            other => {
                panic!("expected PulsarError::Config carrying the auth failure, got: {other:?}")
            }
        }
    }
}
