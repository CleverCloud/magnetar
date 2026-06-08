// SPDX-License-Identifier: Apache-2.0

//! Type definitions used by the [`Connection`](crate::Connection) sans-io
//! state machine. Extracted from `conn.rs` so the 5700-line state-machine
//! file stays focused on the impl side.
//!
//! All types in this module are re-exported from `crate::conn::*` so
//! downstream `use magnetar_proto::conn::{ConnectionConfig, OpOutcome};`
//! call sites stay unchanged.

use core::time::Duration;

use bytes::Bytes;

use crate::event::LookupOutcome;
use crate::pb;
use crate::txn::{TxnError, TxnId, TxnState};
use crate::types::{CompressionKind, MessageId, ProducerHandle, RequestId, SequenceId};

/// Handshake state — modelled after `HandlerState`.
///
/// The state diagram is:
/// `Uninitialized → ConnectSent → Connected ⇄ AuthChallenging → Closing → Closed | Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// Constructed but no bytes sent yet.
    Uninitialized,
    /// Local sent `CommandConnect`, waiting for `CommandConnected`.
    ConnectSent,
    /// Handshake done; producers/consumers can be created.
    Connected,
    /// Mid-connection re-auth (PIP-30/292). Returns to `Connected` after `CommandAuthResponse`.
    AuthChallenging,
    /// Local-initiated close; waiting for the driver to flush.
    Closing,
    /// Closed cleanly.
    Closed,
    /// Failed (handshake error or peer-initiated abort).
    Failed,
}

/// Identifier for a pending operation. Used by
/// [`Connection::register_waker`](crate::conn::Connection::register_waker) /
/// [`Connection::take_outcome`](crate::conn::Connection::take_outcome).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PendingOpKey {
    /// A pending request keyed by request id (lookup, seek, ack-response, etc.).
    Request(RequestId),
    /// A pending publish keyed by `(producer_id, sequence_id)`.
    Send(ProducerHandle, SequenceId),
}

/// Allow-list policy for broker-advertised redirect URLs.
///
/// **Threat model.** The broker is the source of every URL that triggers a
/// client-side re-dial under the *same* auth provider:
///
/// - `CommandTopicMigrated.broker_service_url{,_tls}` (PIP-188).
/// - `CommandLookupTopicResponse.broker_service_url{,_tls}` (data-plane lookup; relevant in proxy
///   mode where the URL becomes `CommandConnect.proxy_to_broker_url` on the next handshake).
/// - `CommandCloseProducer.assigned_broker_service_url{,_tls}` /
///   `CommandCloseConsumer.assigned_broker_service_url{,_tls}`.
///
/// The runtime's
/// [`AuthProvider`](crate::auth::AuthProvider)::initial / `respond_to_challenge`
/// implementations replay credential bytes (OAuth2 bearer, SASL PLAIN,
/// Kerberos GSSAPI, Athenz N-token, ...) on every `CommandConnect`. A
/// compromised broker (or a MITM downstream of TLS termination) that
/// advertises an attacker-controlled URL would therefore harvest the
/// reused credentials on the next handshake.
///
/// The runtime engines (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`)
/// consult this allow-list **before** honouring any broker-advertised URL.
/// Rejection surfaces a
/// [`ConnectionEvent::RedirectUrlRejected`](crate::event::ConnectionEvent::RedirectUrlRejected) and
/// short-circuits the re-dial so the original auth provider is not handed to the unverified host.
///
/// # Defaults
///
/// [`ConnectionConfig::redirect_url_allow_list`] is `None` by default —
/// permissive, preserving the pre-allow-list behaviour. Operators opt in
/// when they have a known set of brokers / hostnames; defence-in-depth.
///
/// # Variants
///
/// - [`RedirectUrlAllowList::Hosts`] — accept URLs whose host literal is in the set
///   (case-insensitive). The most common shape: pin the redirect target to a known set of broker
///   hostnames.
/// - [`RedirectUrlAllowList::Exact`] — accept only URLs whose full URL string is in the set.
///   Strict; useful when the broker fleet has a small, stable URL surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectUrlAllowList {
    /// Accept URLs whose `host` component (lowercased) is a member of
    /// this set. The host is parsed by the in-crate ASCII scanner —
    /// [`magnetar-proto`](crate) keeps its zero-I/O dependency surface.
    /// URLs that fail to parse (missing authority, non-Pulsar scheme)
    /// are rejected; that's the safe default. Port is not constrained —
    /// pin it via [`RedirectUrlAllowList::Exact`] if the deployment
    /// surface justifies it.
    Hosts(Vec<String>),
    /// Accept URLs whose full URL string (verbatim) is in this set. Strict.
    Exact(Vec<String>),
}

impl RedirectUrlAllowList {
    /// `true` when `url` is admitted by this allow-list.
    ///
    /// - [`Self::Hosts`] extracts the host component of a `pulsar://` or `pulsar+ssl://` URL and
    ///   matches case-insensitively against the set. URLs with no extractable host (malformed,
    ///   unsupported scheme, missing authority) are rejected — the safe default.
    /// - [`Self::Exact`] matches the full URL literal byte-for-byte.
    ///
    /// The host extractor is deliberately ASCII-only (no IDNA / punycode
    /// fold). Pulsar broker URLs use ASCII hostnames or IP literals in
    /// practice; an IDNA-flavoured URL would simply miss the literal
    /// match in the allow-list and be rejected, which is the safe outcome
    /// for an unverified host.
    ///
    /// Sans-io: parses with a small hand-rolled scanner so
    /// [`magnetar-proto`](crate) keeps its zero-I/O dependency surface
    /// (no `url` crate).
    #[must_use]
    pub fn is_allowed(&self, url: &str) -> bool {
        match self {
            Self::Hosts(hosts) => match extract_pulsar_host(url) {
                Some(host) => hosts
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(host)),
                None => false,
            },
            Self::Exact(urls) => urls.iter().any(|allowed| allowed == url),
        }
    }
}

/// Pull the host literal out of a `pulsar://host[:port][/path...]` or
/// `pulsar+ssl://host[:port][/path...]` URL. Returns `None` for any other
/// scheme, missing authority, or malformed input (e.g. empty host between
/// `://` and `:`). Used by [`RedirectUrlAllowList::is_allowed`] and unit
/// tested below — see also the IPv6-bracket carve-out.
fn extract_pulsar_host(url: &str) -> Option<&str> {
    // Strip the scheme. Only the two Pulsar schemes are accepted; anything
    // else is rejected to keep the allow-list strict — a `http://` URL the
    // broker invented to confuse us must miss the gate.
    let rest = url
        .strip_prefix("pulsar://")
        .or_else(|| url.strip_prefix("pulsar+ssl://"))?;

    // IPv6 literal: `[::1]:6650`. Honour the bracket form so a future
    // dual-stack broker doesn't inadvertently fall into the "no host" path.
    if let Some(after_lbrack) = rest.strip_prefix('[') {
        let close = after_lbrack.find(']')?;
        // Tail (after `]`) must start with `:` or `/` or be empty.
        let tail = &after_lbrack[close + 1..];
        if !tail.is_empty() && !tail.starts_with(':') && !tail.starts_with('/') {
            return None;
        }
        let host = &after_lbrack[..close];
        if host.is_empty() {
            return None;
        }
        return Some(host);
    }

    let host_end = rest.find([':', '/']).unwrap_or(rest.len());
    let host = &rest[..host_end];
    if host.is_empty() { None } else { Some(host) }
}

/// Connection configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Client version string advertised in `CommandConnect`.
    pub client_version: String,
    /// Authentication method name (e.g. `"none"`, `"token"`).
    pub auth_method_name: String,
    /// Initial auth data (when an auth provider already has a token).
    pub auth_data: Option<Bytes>,
    /// Protocol version to advertise; `21` covers Pulsar 4.x.
    pub protocol_version: i32,
    /// Capabilities to advertise on connect.
    pub feature_flags: pb::FeatureFlags,
    /// Keepalive (ping) interval. Default `30 s`.
    pub keepalive_interval: Duration,
    /// Operation timeout (e.g. lookup + send). Default `30 s`.
    pub operation_timeout: Duration,
    /// Per-attempt timeout for the *initial* TCP/TLS dial. A dial that does not
    /// complete within this budget is abandoned and retried (see
    /// [`Self::connect_max_retries`]). Mirrors Java's `connectionTimeoutMs`.
    /// Default `10 s` (Java `connectionTimeoutMs`).
    /// ([ADR-0052](../../specs/adr/0052-initial-connect-timeout-retry.md))
    pub connect_timeout: Duration,
    /// Bounded retries for the *initial* dial when it times out or fails with a
    /// transient I/O error. `0` means a single attempt (no retry). Each retry
    /// re-dials after an exponential backoff. The Pulsar handshake that follows
    /// a successful dial is NOT retried here — surviving mid-stream transport
    /// drops is the supervisor's job ([`Self::supervisor`]). Default `8`.
    pub connect_max_retries: u32,
    /// Default compression for producers (overridable per producer).
    pub default_compression: CompressionKind,
    /// Default max-message-size if the broker omits it. Pulsar default = 5 MiB.
    pub default_max_message_size: usize,
    /// Optional proxy-to-broker URL for the binary proxy path.
    pub proxy_to_broker_url: Option<String>,
    /// Optional auto-reconnect supervisor. When `Some`, runtime engines wrap the
    /// driver loop in a backoff-driven reconnect cycle that survives transport
    /// failures. `None` (the default) keeps the pre-supervisor behavior — driver
    /// exits on the first I/O error. Mirrors Java's `PulsarClientImpl` reconnect
    /// loop.
    pub supervisor: Option<crate::supervisor::SupervisorConfig>,
    /// Global publish memory budget in bytes. `0` (the default) disables
    /// the limit. Runtime engines that honour this enforce a CAS-reserve on
    /// every `Producer::send` before queueing into the sans-io state
    /// machine; sends that would push the in-flight bytes past the limit
    /// are gated by [`memory_limit_policy`](Self::memory_limit_policy).
    /// Mirrors Java `ClientBuilder#memoryLimit`.
    pub memory_limit_bytes: u64,
    /// Policy applied when the global publish memory budget is exhausted.
    /// Defaults to [`MemoryLimitPolicy::FailImmediately`] to match the Java
    /// client default. [`MemoryLimitPolicy::ProducerBlock`] makes the
    /// runtime park the offending send future on a waker slab until enough
    /// budget frees up. Ignored when
    /// [`memory_limit_bytes`](Self::memory_limit_bytes) is `0`.
    pub memory_limit_policy: MemoryLimitPolicy,
    /// Cap on the *total* number of in-flight broker LOOKUP +
    /// partitioned-topic-metadata requests on this connection. `0` (the
    /// default) preserves the historical unbounded behaviour and matches
    /// Java.
    ///
    /// Set to a small positive value (e.g. `1024`) to harden the client
    /// against pending-lookup memory amplification by a misbehaving or
    /// hostile broker — closes the "pending-lookup memory amplification"
    /// finding from the lookup multi-agent review. Requests that would
    /// exceed the cap surface synchronously as `LookupOutcome::Failed
    /// { code: 0, message: "lookup rejected: max pending" }` without ever
    /// touching the wire.
    pub max_pending_lookups: usize,
    /// Allow-list for broker-advertised redirect URLs (PIP-188
    /// `TopicMigrated`, `CommandLookupTopicResponse` proxy URLs,
    /// `CommandCloseProducer` / `CommandCloseConsumer` reassignment URLs).
    ///
    /// `None` (the default) is permissive: the runtime trusts every
    /// URL the broker advertises and honours it on the next handshake
    /// under the same [`AuthProvider`](crate::auth::AuthProvider). This
    /// preserves pre-allow-list behaviour.
    ///
    /// `Some(allow_list)` is defence-in-depth: the runtime validates every
    /// broker-advertised URL through
    /// [`RedirectUrlAllowList::is_allowed`] before re-dialling. A
    /// rejected URL surfaces a
    /// [`ConnectionEvent::RedirectUrlRejected`](crate::event::ConnectionEvent::RedirectUrlRejected)
    /// and short-circuits the re-dial; the runtime keeps using the
    /// original URL (no credentials are sent to the rejected host).
    ///
    /// See [`RedirectUrlAllowList`] for the threat-model rationale.
    pub redirect_url_allow_list: Option<RedirectUrlAllowList>,
}

/// Policy applied when the configured global publish memory budget is
/// exhausted. Mirrors Java `org.apache.pulsar.client.api.MemoryLimitPolicy`.
///
/// The proto crate exposes this enum so the runtime engines can read the
/// policy from [`ConnectionConfig`] without going through a higher-level
/// re-export. The user-facing `magnetar::MemoryLimitPolicy` re-export
/// in the facade crate is the same shape and converts 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryLimitPolicy {
    /// Reject the send synchronously with `MemoryLimitExceeded`. Mirrors
    /// Java `MemoryLimitPolicy.FAIL_IMMEDIATELY` (the Java default).
    #[default]
    FailImmediately,
    /// Park the send future until enough budget frees up. Releases are
    /// observed via a waker-slab fan-out on the runtime's
    /// `ConnectionShared`. Mirrors Java `MemoryLimitPolicy.PRODUCER_BLOCK`.
    ///
    /// Implemented per
    /// [ADR-0020](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0020-memory-limit-producer-block.md)
    /// — the wait uses a `parking_lot::Mutex<Slab<Waker>>` (not a channel)
    /// honouring [ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md).
    ProducerBlock,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            client_version: format!("magnetar/{}", env!("CARGO_PKG_VERSION")),
            auth_method_name: "none".to_owned(),
            auth_data: None,
            protocol_version: crate::SUPPORTED_PROTOCOL_VERSION,
            feature_flags: pb::FeatureFlags::default(),
            keepalive_interval: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            connect_max_retries: 8,
            default_compression: CompressionKind::None,
            default_max_message_size: 5 * 1024 * 1024,
            proxy_to_broker_url: None,
            supervisor: None,
            memory_limit_bytes: 0,
            memory_limit_policy: MemoryLimitPolicy::FailImmediately,
            max_pending_lookups: 0,
            redirect_url_allow_list: None,
        }
    }
}

#[cfg(test)]
mod redirect_url_allow_list_tests {
    //! Unit tests for [`RedirectUrlAllowList`] — sans-io behaviour only.
    //! The runtime-side wiring is exercised in
    //! `magnetar-runtime-tokio/tests/topic_migrated_allow_list.rs` and the
    //! mirrored moonpool / differential layers.

    use super::{ConnectionConfig, RedirectUrlAllowList};

    #[test]
    fn default_redirect_allow_list_is_permissive() {
        let cfg = ConnectionConfig::default();
        assert!(
            cfg.redirect_url_allow_list.is_none(),
            "default must be permissive (None) to preserve pre-allow-list behaviour",
        );
    }

    #[test]
    fn hosts_variant_matches_on_host_case_insensitive() {
        let list = RedirectUrlAllowList::Hosts(vec![
            "broker-a.example.com".to_owned(),
            "broker-b.example.com".to_owned(),
        ]);
        assert!(list.is_allowed("pulsar://broker-a.example.com:6650"));
        assert!(list.is_allowed("pulsar+ssl://broker-b.example.com:6651"));
        // Case-insensitive: the broker may advertise upper-case authority.
        assert!(list.is_allowed("pulsar://BROKER-A.example.com:6650"));
    }

    #[test]
    fn hosts_variant_rejects_unlisted_hosts() {
        let list = RedirectUrlAllowList::Hosts(vec!["broker-a.example.com".to_owned()]);
        assert!(!list.is_allowed("pulsar://attacker.example.com:6650"));
        assert!(!list.is_allowed("pulsar://10.0.0.1:6650"));
    }

    #[test]
    fn hosts_variant_rejects_unparseable_url() {
        // Defensive default: parse-failure means "we cannot verify the
        // host", so we reject. A misbehaving broker should not be able to
        // bypass the gate by sending a malformed URL.
        let list = RedirectUrlAllowList::Hosts(vec!["broker-a.example.com".to_owned()]);
        assert!(!list.is_allowed("not-a-url"));
        assert!(!list.is_allowed(""));
    }

    #[test]
    fn exact_variant_requires_literal_match() {
        let list = RedirectUrlAllowList::Exact(vec![
            "pulsar://broker-a.example.com:6650".to_owned(),
            "pulsar+ssl://broker-b.example.com:6651".to_owned(),
        ]);
        assert!(list.is_allowed("pulsar://broker-a.example.com:6650"));
        assert!(list.is_allowed("pulsar+ssl://broker-b.example.com:6651"));
        // Different port: rejected (the host alone is not enough under Exact).
        assert!(!list.is_allowed("pulsar://broker-a.example.com:7000"));
        // Different scheme: rejected.
        assert!(!list.is_allowed("pulsar+ssl://broker-a.example.com:6650"));
    }

    #[test]
    fn empty_list_rejects_every_url() {
        let hosts = RedirectUrlAllowList::Hosts(Vec::new());
        let exact = RedirectUrlAllowList::Exact(Vec::new());
        assert!(!hosts.is_allowed("pulsar://broker.example.com:6650"));
        assert!(!exact.is_allowed("pulsar://broker.example.com:6650"));
    }

    #[test]
    fn hosts_variant_admits_ip_literal_authority() {
        let list = RedirectUrlAllowList::Hosts(vec!["10.0.0.5".to_owned()]);
        assert!(list.is_allowed("pulsar://10.0.0.5:6650"));
        assert!(!list.is_allowed("pulsar://10.0.0.6:6650"));
    }

    #[test]
    fn hosts_variant_admits_ipv6_bracketed_authority() {
        let list = RedirectUrlAllowList::Hosts(vec!["::1".to_owned()]);
        assert!(list.is_allowed("pulsar://[::1]:6650"));
        assert!(list.is_allowed("pulsar+ssl://[::1]:6651"));
        assert!(!list.is_allowed("pulsar://[::2]:6650"));
    }

    #[test]
    fn hosts_variant_rejects_non_pulsar_scheme() {
        let list = RedirectUrlAllowList::Hosts(vec!["broker.example.com".to_owned()]);
        // A malicious broker that advertises `http://`, `tcp://` or any
        // other scheme should fail the gate even when the host matches —
        // we only ever re-dial Pulsar URLs.
        assert!(!list.is_allowed("http://broker.example.com:6650"));
        assert!(!list.is_allowed("tcp://broker.example.com:6650"));
        assert!(!list.is_allowed("javascript:broker.example.com"));
    }
}

#[cfg(test)]
mod connect_resilience_config_tests {
    //! Layer (a) of the ADR-0024 four-layer policy for the dual-cap
    //! initial-dial retry (ADR-0052). Pins the [`ConnectionConfig`]
    //! defaults the moonpool + tokio engines read when bounding the
    //! initial connect, and confirms the dual-cap fields survive a
    //! round-trip through `Clone` (the engines clone the config off the
    //! `ConnectionShared` per dial site).
    //!
    //! The runtime-side wiring is exercised in
    //! `magnetar-runtime-moonpool/tests/connect_resilience.rs`, the
    //! mirrored `magnetar-runtime-tokio/tests/connect_resilience.rs`, the
    //! `magnetar-differential` equivalence layer, and the e2e suite.

    use core::time::Duration;

    use super::ConnectionConfig;

    #[test]
    fn default_dual_cap_matches_java_client() {
        let cfg = ConnectionConfig::default();
        // Per-attempt initial-dial budget — exact Java `connectionTimeoutMs`.
        assert_eq!(
            cfg.connect_timeout,
            Duration::from_secs(10),
            "connect_timeout default must be 10s (Java connectionTimeoutMs parity)",
        );
        // Count backstop — Java's reconnect-attempt count for the initial dial.
        assert_eq!(
            cfg.connect_max_retries, 8,
            "connect_max_retries default must be 8 (count backstop)",
        );
        // Total connect-operation budget — Java `operationTimeoutMs`. The
        // dual cap stops the retry loop on whichever of count / budget
        // trips first.
        assert_eq!(
            cfg.operation_timeout,
            Duration::from_secs(30),
            "operation_timeout default must be 30s (total connect-operation budget)",
        );
    }

    #[test]
    fn dual_cap_fields_round_trip_through_clone() {
        // The engines clone the config at each dial site
        // (`connect_plain_with_resolver`, `connect_tls`, pool `build_entry`),
        // so the dual-cap fields must survive `Clone` byte-identically.
        let cfg = ConnectionConfig {
            connect_timeout: Duration::from_millis(1_234),
            connect_max_retries: 3,
            operation_timeout: Duration::from_millis(7_777),
            ..ConnectionConfig::default()
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.connect_timeout, Duration::from_millis(1_234));
        assert_eq!(cloned.connect_max_retries, 3);
        assert_eq!(cloned.operation_timeout, Duration::from_millis(7_777));
    }

    #[test]
    fn zero_retries_is_a_valid_fail_fast_config() {
        // `connect_max_retries = 0` means a single dial attempt with no
        // retry — the fail-fast shape the tokio integration test exercises.
        // Confirm the field accepts it and the other caps stay independent.
        let cfg = ConnectionConfig {
            connect_max_retries: 0,
            ..ConnectionConfig::default()
        };
        assert_eq!(cfg.connect_max_retries, 0);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
        assert_eq!(cfg.operation_timeout, Duration::from_secs(30));
    }
}

/// Result of consuming a pending op via
/// [`Connection::take_outcome`](crate::conn::Connection::take_outcome).
#[derive(Debug, Clone)]
pub enum OpOutcome {
    /// A `CommandSendReceipt` correlated with the publish.
    SendReceipt {
        /// Sequence id of the publish.
        sequence_id: SequenceId,
        /// Broker-assigned message id.
        message_id: MessageId,
    },
    /// A `CommandSendError` correlated with the publish.
    SendError {
        /// Sequence id of the publish.
        sequence_id: SequenceId,
        /// `ServerError` code.
        code: i32,
        /// Broker error message.
        message: String,
    },
    /// Generic broker success (request id matched but no payload).
    Success {
        /// Request id of the originating request.
        request_id: RequestId,
    },
    /// Generic broker error.
    Error {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `ServerError` code.
        code: i32,
        /// Broker error message.
        message: String,
    },
    /// Lookup outcome.
    LookupResponse {
        /// Request id of the originating lookup.
        request_id: RequestId,
        /// The outcome of the lookup.
        outcome: LookupOutcome,
    },
    /// Partitioned-topic metadata.
    PartitionedMetadata {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Number of partitions.
        partitions: u32,
        /// Optional error if the request failed.
        error: Option<(i32, String)>,
    },
    /// `CommandNewTxnResponse` correlated with a `new_txn` call.
    NewTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Resulting transaction id on success, or the [`TxnError`] on failure.
        result: Result<TxnId, TxnError>,
    },
    /// `CommandAddPartitionToTxnResponse` correlated with an `add_partition_to_txn` call.
    AddPartitionToTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `Ok(())` on success.
        result: Result<(), TxnError>,
    },
    /// `CommandAddSubscriptionToTxnResponse` correlated with an `add_subscription_to_txn` call.
    AddSubscriptionToTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `Ok(())` on success.
        result: Result<(), TxnError>,
    },
    /// `CommandEndTxnResponse` correlated with an `end_txn` call.
    EndTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Final transaction state on success.
        result: Result<TxnState, TxnError>,
    },
    /// `CommandGetLastMessageIdResponse` correlated with a `get_last_message_id` call.
    LastMessageId {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Broker's view of the last published message id on the topic.
        last_message_id: MessageId,
        /// Optional consumer mark-delete position (where the broker thinks this consumer's
        /// cursor is).
        consumer_mark_delete_position: Option<MessageId>,
    },
    /// `CommandWatchTopicListSuccess` correlated with a `watch_topic_list` call —
    /// the initial snapshot for a topic-list watcher (PIP-145).
    TopicListSnapshot {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Topics currently matching the watcher's namespace + pattern.
        topics: Vec<String>,
    },
    /// `CommandGetSchemaResponse` correlated with a
    /// [`Connection::get_schema`](crate::conn::Connection::get_schema) call.
    ///
    /// Carries the schema-registry round-trip outcome: `Ok((schema, version))` on success,
    /// `Err((code, message))` on failure.
    GetSchemaResponse {
        /// Request id of the originating `CommandGetSchema`.
        request_id: RequestId,
        /// The schema-registry round-trip outcome.
        result: crate::event::GetSchemaResult,
    },
    /// Synthetic outcome surfaced to every waiter when the underlying broker
    /// connection drops and the supervisor begins a reconnect. Callers detect
    /// the lost session via the embedded `PendingOpKey` and decide whether to
    /// retry the operation against the freshly-handshaked connection. Mirrors
    /// the "session-lost" failure mode of Java
    /// `ClientCnx#handleConnectionClosed`.
    SessionLost {
        /// The original op key (request id or `(producer, sequence_id)`)
        /// whose future is being woken up with this outcome.
        key: PendingOpKey,
    },
    /// Synthetic *terminal* outcome surfaced to every waiter when the
    /// connection drops with no recovery path — a plain (non-supervised)
    /// driver hitting a fatal decode, a peer close, or an I/O error.
    ///
    /// Distinct from [`OpOutcome::SessionLost`], which is load-bearing for the
    /// supervisor's transparent at-least-once replay (it deliberately keeps
    /// `Send` keys pending for re-issue). `Terminal` instead resolves *every*
    /// pending op — including `Send` keys — so the user-facing future returns a
    /// terminal error (`PeerClosed`) promptly instead of hanging forever.
    /// Installed by [`Connection::fail_all_pending`](crate::conn::Connection::fail_all_pending).
    Terminal {
        /// The original op key (request id or `(producer, sequence_id)`) whose
        /// future is being woken up with this terminal outcome.
        key: PendingOpKey,
        /// Human-readable reason for the terminal drop (peer close, decode
        /// error, I/O error). Never carries secret material (ADR-0054).
        reason: String,
    },
}

/// Parameters for opening a producer.
#[derive(Debug, Clone)]
pub struct CreateProducerRequest {
    /// Topic name.
    pub topic: String,
    /// Optional producer name (broker assigns one if `None`).
    pub producer_name: Option<String>,
    /// Compression codec.
    pub compression: CompressionKind,
    /// Whether the producer wishes to enable batching.
    pub enable_batching: bool,
    /// Whether the producer wishes to enable chunking.
    pub enable_chunking: bool,
    /// Max batch size in bytes.
    pub max_batch_size_bytes: usize,
    /// Max messages per batch.
    pub max_messages_in_batch: usize,
    /// Optional schema to advertise.
    pub schema: Option<pb::Schema>,
    /// Mirrors Java `ProducerBuilder#initialSequenceId`. When `Some(n)`, the producer starts
    /// allocating sequence ids from `n` instead of `0`. Useful for at-least-once
    /// resume-on-restart from a known checkpoint.
    pub initial_sequence_id: Option<u64>,
    /// Producer access mode. Mirrors `CommandProducer.producer_access_mode`. Defaults to
    /// `Shared`; switch to `Exclusive` / `WaitForExclusive` / `ExclusiveWithFencing` for
    /// single-writer-per-topic patterns.
    pub access_mode: pb::ProducerAccessMode,
    /// Mirrors `CommandProducer.metadata` — broker-side KV metadata advertised at producer
    /// open. Surfaces on the broker dashboard alongside the producer.
    pub producer_metadata: Vec<(String, String)>,
    /// Mirrors Java `ProducerBuilder#sendTimeout`. When set, any in-flight send whose
    /// `enqueued_at + timeout` has elapsed surfaces a synthetic
    /// `SendError(code=11008, "send timeout")` on the next `Connection::handle_timeout`
    /// tick. `None` disables the sweep (the default).
    pub send_timeout: Option<Duration>,
    /// Mirrors Java `ProducerBuilder#batchingMaxPublishDelay`. When set and batching is
    /// enabled, the state machine flushes any non-empty batch whose oldest message has
    /// been waiting longer than this duration. Caps end-to-end latency for batched sends
    /// that would otherwise sit until the batch fills. `None` (the default) means the
    /// batch only flushes on size / count limits.
    pub batching_max_publish_delay: Option<Duration>,
}

impl Default for CreateProducerRequest {
    fn default() -> Self {
        Self {
            topic: String::new(),
            producer_name: None,
            compression: CompressionKind::None,
            enable_batching: false,
            enable_chunking: false,
            max_batch_size_bytes: 128 * 1024,
            max_messages_in_batch: 1000,
            schema: None,
            initial_sequence_id: None,
            access_mode: pb::ProducerAccessMode::Shared,
            producer_metadata: Vec::new(),
            send_timeout: None,
            batching_max_publish_delay: None,
        }
    }
}

/// Parameters for opening a consumer.
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
    /// Subscription type (`Exclusive`, `Shared`, `Failover`, `Key_Shared`).
    pub sub_type: pb::command_subscribe::SubType,
    /// Receiver queue size.
    pub receiver_queue_size: usize,
    /// Initial position to read from.
    pub initial_position: pb::command_subscribe::InitialPosition,
    /// Consumer name (optional — broker assigns one).
    pub consumer_name: Option<String>,
    /// Optional schema.
    pub schema: Option<pb::Schema>,
    /// Whether the subscription is durable.
    pub durable: bool,
    /// Read from the compacted (key-deduplicated) view of the topic. Required by TableView and
    /// by any "latest-value-per-key" workflow against compacted topics. Mirrors
    /// `CommandSubscribe.read_compacted`.
    pub read_compacted: bool,
    /// Mirrors `CommandSubscribe.priority_level`. The broker uses it for Shared / Failover
    /// dispatch ordering. `None` means default (broker treats as 0).
    pub priority_level: Option<i32>,
    /// Mirrors `CommandSubscribe.subscription_properties` — per-subscription key/value
    /// metadata visible to the broker dashboard.
    pub subscription_properties: Vec<(String, String)>,
    /// Optional [`KeySharedConfig`] for `Key_Shared` subscriptions. Ignored for other
    /// subscription types.
    pub key_shared: Option<KeySharedConfig>,
    /// Optional starting message id for a fresh subscription. Mirrors Java
    /// `ReaderBuilder#startMessageId` / `ConsumerBuilder#startMessageId` and the
    /// `CommandSubscribe.start_message_id` wire field. Has no effect on a subscription
    /// that already has a persisted cursor.
    pub start_message_id: Option<MessageId>,
    /// Mirrors `CommandSubscribe.replicate_subscription_state`. When `Some(true)`, the broker
    /// replicates this subscription's cursor across geo-replicated clusters. Defaults to
    /// `None` (broker decision).
    pub replicate_subscription_state: Option<bool>,
    /// Mirrors `CommandSubscribe.force_topic_creation`. When `Some(false)` the broker fails
    /// the subscribe if the topic doesn't already exist. Defaults to `None` (broker default,
    /// which is `true`).
    pub force_topic_creation: Option<bool>,
    /// Mirrors `CommandSubscribe.start_message_rollback_duration_sec`. Rolls the subscription
    /// cursor back by N seconds at subscribe time, so the consumer re-reads recent history.
    pub start_message_rollback_duration_sec: Option<u64>,
    /// Mirrors Java `DeadLetterPolicy#maxRedeliverCount`. When a message has been redelivered
    /// more than this many times, the consumer routes it into the dead-letter queue instead
    /// of the user-facing queue. `0` disables DLQ routing.
    pub max_redeliver_count: u32,
    /// Mirrors Java `DeadLetterPolicy#deadLetterTopic`. Where the consumer republishes
    /// messages that exceeded `max_redeliver_count`. Convention if `None`:
    /// `<topic>-<subscription>-DLQ` (matches the Java client default).
    pub dead_letter_topic: Option<String>,
    /// Mirrors `CommandSubscribe.metadata` — broker-side KV metadata advertised at
    /// subscribe time. Surfaces on the broker dashboard alongside the consumer.
    pub consumer_metadata: Vec<(String, String)>,
    /// Mirrors Java `ConsumerBuilder#negativeAckRedeliveryDelay`. When `Some(d)`, nacked
    /// messages stay locally tracked for `d` before the redelivery command goes out. `None`
    /// means the redelivery is sent immediately (the default).
    pub negative_ack_redelivery_delay: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#ackTimeout`. When `Some(d)`, every delivered message
    /// is tracked client-side; if no positive ack arrives within `d`, the consumer forces a
    /// redelivery. `None` disables the tracker (the default).
    pub ack_timeout: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#ackTimeoutRedeliveryBackoff`. PIP-37: when set together
    /// with [`Self::ack_timeout`], the ack-timeout deadline for each delivered message is
    /// computed via
    /// [`crate::trackers::nack::MultiplierRedeliveryBackoff::delay_for`] using the
    /// broker-reported `redelivery_count` on the incoming message. `None` keeps the flat
    /// `ack_timeout` window.
    pub ack_timeout_backoff: Option<crate::trackers::nack::MultiplierRedeliveryBackoff>,
    /// Mirrors Java `ConsumerBuilder#acknowledgmentGroupTime`. When `Some(d)`, calls to
    /// the runtime `Consumer::ack_grouped` family stage acks in an in-memory tracker and
    /// flush them as a single coalesced `CommandAck` every `d`. Trades broker-confirmation
    /// guarantees for lower ack-bandwidth on high-throughput consumers. `None` keeps every
    /// ack synchronous (the default).
    pub ack_group_time: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#cryptoFailureAction`. Controls what the consumer does
    /// when payload decryption fails (PIP-4). `Fail` (default) propagates the error to the
    /// caller; `Discard` silently drops the message; `Consume` delivers the encrypted
    /// ciphertext as-is.
    pub crypto_failure_action: CryptoFailureAction,
}

/// PIP-4 decryption failure handling. Mirrors Java
/// `org.apache.pulsar.client.api.ConsumerCryptoFailureAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CryptoFailureAction {
    /// Surface the decryption error to the caller (default — fail-fast). Matches the
    /// pre-PIP-4 behavior.
    #[default]
    Fail,
    /// Silently drop the message and continue receiving. The caller never sees the
    /// undecryptable payload — useful when some keys are rotated out and lingering
    /// messages encrypted with retired keys should be ignored.
    Discard,
    /// Deliver the encrypted ciphertext + the `EncryptionKeys` metadata as-is to the
    /// caller, who can then attempt out-of-band decryption.
    Consume,
}

/// Mirrors Java's `KeySharedPolicy`. Configures how a `Key_Shared` subscription distributes
/// messages with the same partition key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySharedConfig {
    /// Routing mode — broker-managed `AutoSplit` or client-pinned `Sticky`.
    pub mode: pb::KeySharedMode,
    /// For `Sticky` mode: the hash ranges this consumer claims. Ignored for `AutoSplit`.
    pub sticky_hash_ranges: Vec<(i32, i32)>,
    /// Tolerate out-of-order delivery within the same key group. Mirrors
    /// `KeySharedMeta.allow_out_of_order_delivery`.
    pub allow_out_of_order_delivery: bool,
}

impl Default for KeySharedConfig {
    fn default() -> Self {
        Self {
            mode: pb::KeySharedMode::AutoSplit,
            sticky_hash_ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }
    }
}

impl Default for SubscribeRequest {
    fn default() -> Self {
        Self {
            topic: String::new(),
            subscription: String::new(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            initial_position: pb::command_subscribe::InitialPosition::Latest,
            consumer_name: None,
            schema: None,
            durable: true,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            start_message_id: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
            max_redeliver_count: 0,
            dead_letter_topic: None,
            consumer_metadata: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_timeout_backoff: None,
            ack_group_time: None,
            crypto_failure_action: CryptoFailureAction::Fail,
        }
    }
}

/// Ack request — covers both individual and cumulative semantics.
#[derive(Debug, Clone)]
pub struct AckRequest {
    /// The message ids to ack.
    pub message_ids: Vec<MessageId>,
    /// Whether this is an `Individual` or `Cumulative` ack.
    pub ack_type: pb::command_ack::AckType,
    /// Optional ack-time properties. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Map<String, Long>)`. The broker stores them
    /// alongside the cursor for diagnostic / replay tooling.
    pub properties: Vec<(String, i64)>,
    /// Optional transaction id (PIP-31). When set, the ack participates in the open
    /// transaction — it only takes effect when the transaction commits. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    pub txn_id: Option<crate::txn::TxnId>,
}

/// Seek target — either to a message id or to a publish-time.
#[derive(Debug, Clone)]
pub enum SeekTarget {
    /// Seek to a specific message id.
    MessageId(MessageId),
    /// Seek to a specific publish timestamp (ms since UNIX epoch).
    PublishTime(u64),
}
