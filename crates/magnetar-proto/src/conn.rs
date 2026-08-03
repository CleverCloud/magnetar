// SPDX-License-Identifier: Apache-2.0

//! The central `Connection` sans-io state machine.
//!
//! Public surface mirrors `quinn-proto::Connection`:
//!
//! - [`Connection::handle_bytes`] takes inbound bytes and updates internal state.
//! - [`Connection::poll_transmit`] drains queued outbound bytes.
//! - [`Connection::poll_event`] yields semantic [`ConnectionEvent`]s.
//! - [`Connection::poll_timeout`] / [`Connection::handle_timeout`] drive keepalives + trackers.
//!
//! On top of that, a handle-based façade lets callers (the runtime crate) open producers /
//! consumers, send, ack, seek, look up, etc. — all without I/O.
//!
//! Waker registration uses a small slab keyed by `op_id` per
//! [GUIDELINES.md] §"No-channels rule" — no `tokio::sync::*`, no `crossbeam`, no `flume`.
//!
//! # References
//!
//! - `ClientCnx.java:117` (channel constants and request id seed)
//! - `ClientCnx.java:132-158` (constructor wiring)
//! - `ClientCnx.java:432` (handleConnected)
//! - `ClientCnx.java:464` (handleAuthChallenge)
//! - `ClientCnx.java:515` (request dispatch)
//! - `HandlerState.java` (handshake states)

use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::task::Waker;
use std::time::{Instant, SystemTime};

use bytes::{Bytes, BytesMut};

// Type definitions used by this state machine live in
// `crate::conn_types` (extracted to keep conn.rs focused on the impl
// side). Re-exported here so `magnetar_proto::conn::*` paths stay
// unchanged.
pub use crate::conn_types::*;
use crate::consumer::ConsumerState;
use crate::error::ProtocolError;
use crate::event::{ConnectionEvent, IncomingMessage, LookupOutcome, TxnRoundTrip};
use crate::frame::{Frame, decode_one, encode_command, encode_payload, encode_payload_head};
use crate::lookup::{LookupRegistry, LookupRequest, LookupSubmitError, is_partition_topic};
use crate::pb;
use crate::producer::{ProducerState, SendDecision};
use crate::topic_watcher::{TopicWatcher, TopicWatcherRegistry};
use crate::txn::{TxnAction, TxnClient, TxnId};
use crate::types::{ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId};

/// The central sans-io state machine.
pub struct Connection {
    config: ConnectionConfig,
    /// Broker-operation retry policy, deliberately kept outside the public
    /// `ConnectionConfig` struct so adding the feature does not break
    /// downstream exhaustive config literals.
    operation_retry: crate::OperationRetryConfig,
    state: HandshakeState,
    broker_max_message_size: Option<usize>,
    broker_protocol_version: i32,
    feature_flags: pb::FeatureFlags,
    /// Last broker `CommandError` observed while the handshake was in
    /// `ConnectSent` or `AuthChallenging` state. Captured so a
    /// transport-drop-driven flip to [`HandshakeState::Failed`] can
    /// surface the broker's explanation instead of an opaque "handshake
    /// failed" error. Cleared by [`Self::reset`]. Mirrors what Java's
    /// `ClientCnx#handleError` logs when the broker tears the connection
    /// down mid-handshake.
    handshake_failure_reason: Option<String>,
    /// Outbound bytes buffer drained by [`Self::poll_transmit`].
    outbound: BytesMut,
    /// Wave-1.1 staging slot for [`Self::poll_transmit_vectored`].
    /// Holds the most recently drained outbound `Bytes` so the
    /// `Transmit::Contiguous(&slice)` return borrows against an owned
    /// buffer the [`Connection`] keeps alive. Replaced on every
    /// `poll_transmit_vectored` call; the borrow checker prevents
    /// concurrent re-entry. `None` before the first vectored drain.
    pending_vectored_drain: Option<Bytes>,
    /// Wave-1.2 producer-batch segment buffer (ADR-0040). Drained by
    /// [`Self::drain_producer_outbound_vectored`] — each producer
    /// frame contributes a `[head, payload]` pair via
    /// `frame::encode_payload_head`. Consumed by
    /// [`Self::poll_transmit_vectored`], which returns
    /// `Transmit::Vectored(&segments)` when this is non-empty and the
    /// contiguous `outbound` buffer is empty (handshake / non-producer
    /// frames take the `Contiguous` arm to preserve wire-order
    /// correctness when both buffers carry pending bytes).
    outbound_segments: Vec<Bytes>,
    /// Wave-1.2 staging slot mirroring [`Self::pending_vectored_drain`]
    /// for the segment list: holds the most recently drained vector so
    /// the `Transmit::Vectored(&slice)` return borrows against memory
    /// the [`Connection`] keeps alive across the runtime's `.await`.
    pending_vectored_segments: Vec<Bytes>,
    /// Inbound bytes buffer; framed into commands by [`Self::handle_bytes`].
    inbound: BytesMut,
    /// Event queue.
    events: VecDeque<ConnectionEvent>,
    /// Exact failed generations for built-in runtime retry legs.
    driver_retries: VecDeque<crate::DriverRetry>,
    /// Outcomes ready to be consumed by user futures.
    outcomes: HashMap<PendingOpKey, OpOutcome>,
    /// Waker slab keyed by op id.
    wakers: HashMap<PendingOpKey, Waker>,
    /// Pending requests keyed by request id, with the kind of operation that produced them.
    pending_requests: HashMap<RequestId, PendingRequestKind>,
    /// Open producers.
    ///
    /// `ProducerState` lives behind a per-slot [`parking_lot::Mutex`] so the
    /// runtime crates can read identity / push hot-path operations without
    /// taking the global Connection mutex (split-connection-mutex
    /// refactor, ADR-0038). For Phase 1 every Connection method that mutates
    /// per-producer state still does so under the global mutex — it just
    /// takes the slot lock briefly first. Lock-ordering: **global → per-slot,
    /// never the reverse**.
    producers: HashMap<ProducerHandle, std::sync::Arc<crate::producer::ProducerSlot>>,
    /// Original [`CreateProducerRequest`] for every still-open producer. Stashed at
    /// [`Self::create_producer`] time so the supervisor can replay `CommandProducer` on a
    /// freshly-handshaked transport via [`Self::rebuild_producers`]. Mirrors the parameters
    /// Java keeps inside `ProducerImpl#conf` for the same purpose.
    producer_create_requests: HashMap<ProducerHandle, CreateProducerRequest>,
    /// In-flight publish snapshots — populated by [`Self::reset`] and consumed by
    /// [`Self::rebuild_producers`]. Keyed by producer handle; each value is the in-FIFO-order
    /// list of [`crate::producer::OpSend`] entries that were unconfirmed at reset time, with
    /// their wakers already cleared. Mirrors Java `ProducerImpl#pendingMessages` which is
    /// preserved across the reconnect so `resendMessages()` can re-issue each `OpSendMsg`
    /// verbatim onto the new session. Implements at-least-once publish parity (the
    /// `OpOutcome::SessionLost` short-circuit is *not* installed on the outcome slab for
    /// snapshotted sends — the user-facing future sees the eventual `CommandSendReceipt`
    /// without ever observing the reset).
    in_flight_publish_snapshots: HashMap<ProducerHandle, Vec<crate::producer::OpSend>>,
    /// Open consumers.
    ///
    /// `ConsumerState` lives behind a per-slot [`parking_lot::Mutex`] for
    /// the same reasons as [`Self::producers`] — see ADR-0038. Lock-ordering:
    /// **global → per-slot, never the reverse**.
    consumers: HashMap<ConsumerHandle, std::sync::Arc<crate::consumer::ConsumerSlot>>,
    /// Original [`SubscribeRequest`] for every still-open consumer. Stashed at
    /// [`Self::subscribe`] time so the supervisor can replay `CommandSubscribe` on a
    /// freshly-handshaked transport via [`Self::rebuild_consumers`]. Mirrors the parameters
    /// Java keeps inside `ConsumerImpl#conf` for the same purpose.
    consumer_subscribe_requests: HashMap<ConsumerHandle, SubscribeRequest>,
    /// Lookup registry.
    lookup: LookupRegistry,
    /// Topic watcher registry.
    topic_watchers: TopicWatcherRegistry,
    /// Transaction-coordinator client (PIP-31). One per connection — the connection only opens
    /// transactions against the TC that lives behind it.
    txn_client: TxnClient,
    /// Next request id.
    next_request_id: u64,
    /// Next producer id.
    next_producer_id: u64,
    /// Next consumer id.
    next_consumer_id: u64,
    /// Next watcher id.
    next_watcher_id: u64,
    /// Time the keepalive watchdog last observed **forward progress** — a
    /// fully decoded inbound frame or a freshly sent keepalive ping. Refreshed
    /// per *decoded frame*, never per raw inbound chunk: a desynced-but-chatty
    /// socket (e.g. a bit-flip on the un-checksummed outer `total_size` prefix
    /// that yields a plausible-but-never-satisfied length, so
    /// [`crate::frame::peek_full_frame_len`] returns `Incomplete` forever) must
    /// NOT keep resetting this baseline by dribbling bytes that never frame.
    /// ([ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md))
    last_activity: Option<Instant>,
    /// Whether a keepalive `CommandPing` has been emitted but not yet answered
    /// by any inbound frame. Armed when [`Self::handle_timeout`] sends a ping;
    /// cleared the moment a decoded frame proves the peer is still talking. If a
    /// second consecutive keepalive interval elapses with the flag still armed,
    /// the watchdog escalates to [`Self::mark_disconnected`] (→
    /// [`HandshakeState::Failed`], which the driver treats as `should_close` →
    /// supervised reconnect) instead of dead-pinging a wedged socket forever.
    /// ([ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md))
    keepalive_ping_outstanding: bool,
    /// Wall-clock time of the most recent transition to [`HandshakeState::Connected`].
    /// Mirrors Java's `Producer/Consumer#getLastDisconnectedTimestamp` companion: useful
    /// for application-level health probes and reconnect diagnostics.
    last_connected_at: Option<SystemTime>,
    /// Wall-clock time of the most recent transition out of [`HandshakeState::Connected`]
    /// (to `Closing`, `Closed`, or `Failed`). Mirrors
    /// `org.apache.pulsar.client.api.Producer#getLastDisconnectedTimestamp` (millis since
    /// the UNIX epoch in Java; an [`Option<SystemTime>`] here so the caller picks its own
    /// epoch conversion).
    last_disconnected_at: Option<SystemTime>,
    /// Monotonic counter incremented each time [`Self::reset`] is called. Lets
    /// callers detect that an in-flight operation was severed by a supervisor
    /// reconnect: capture the epoch before issuing an op, then re-check after
    /// the outcome arrives. Mirrors Java's `ClientCnx#getEpoch` semantics for
    /// session-bound operations.
    session_epoch: u64,
    /// Wall-clock provider — the sans-io state machine never calls
    /// [`SystemTime::now`] directly. Mandatory constructor parameter of
    /// [`Self::new`]: the tokio engine wraps `SystemTime::now`,
    /// moonpool / deterministic-simulation engines plug in a virtual clock.
    /// Forcing the choice at construction time keeps the state machine
    /// genuinely sans-io and lets `xtask check-no-internal-clock` validate
    /// the engine construction site (ADR-0011).
    wall_clock: std::sync::Arc<dyn Fn() -> SystemTime + Send + Sync>,
    /// Anti-thrash detector (ADR-0028). Disabled by default; opted in by the
    /// engine driver via [`Self::set_anti_thrash`] when the user configures
    /// [`crate::supervisor::SupervisorConfig::anti_thrash_threshold`]. The
    /// detector is purely an observable: the driver records re-attach
    /// outcomes into it and polls [`Self::anti_thrash_tick`] to decide
    /// whether to delay the next redial.
    anti_thrash: crate::anti_thrash::AntiThrashState,
    /// PIP-460 (ADR-0093) scalable-topic sessions, keyed by the
    /// client-allocated `session_id` that `CommandScalableTopicLookup` carries.
    /// Each tracks the current segment DAG and its monotonic layout epoch. The
    /// lookup *is* the watch subscribe upstream, so there is no separate
    /// in-flight lookup registry. See [`crate::dag_watch::DagWatchSession`].
    #[cfg(feature = "scalable-topics")]
    scalable_sessions: HashMap<u64, crate::dag_watch::DagWatchSession>,
    /// PIP-460 (ADR-0093) next client-allocated scalable-topic session id.
    #[cfg(feature = "scalable-topics")]
    next_scalable_session_id: u64,
    /// FoundationDB-style buggify fault-injection helper (ADR-0048).
    /// Default state is [`crate::Buggify::disabled`] — every choice
    /// point's `should_fire` call returns `false` and the buggified
    /// branch compiles out. Engines opt the connection into seeded
    /// fault injection via [`Self::set_buggify`].
    buggify: crate::Buggify,
}

impl core::fmt::Debug for Connection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("state", &self.state)
            .field("producers", &self.producers.len())
            .field("consumers", &self.consumers.len())
            .field("pending_requests", &self.pending_requests.len())
            .field("events_queue", &self.events.len())
            .field("outbound_bytes", &self.outbound.len())
            .finish_non_exhaustive()
    }
}

/// Compatibility constant for the default operation retry count.
///
/// Runtime decisions read the connection's installed
/// [`crate::OperationRetryConfig`]; callers can override the count through
/// [`crate::OperationRetryConfig::max_retries`].
pub const MAX_TRANSIENT_OPEN_RETRIES: u32 = 8;

/// Error code surfaced when a publish exceeds its configured `send_timeout`.
///
/// Pulsar's wire-protocol `ServerError` enum has no `TimeoutError` variant, so both
/// send-timeout sweeps use the same `-1` sentinel the Java client surfaces as
/// `TimeoutException`, paired with [`SEND_TIMEOUT_MESSAGE`] so callers can pattern-match on
/// the error string.
const SEND_TIMEOUT_CODE: i32 = -1;

/// Error message paired with [`SEND_TIMEOUT_CODE`]. Callers match on this exact string.
const SEND_TIMEOUT_MESSAGE: &str = "send timeout";

/// Whether a producer or consumer slot's rolling rate window is due for a
/// re-sample at `now`, given the slot's existing `last_rate_snapshot` baseline
/// and the connection's [`ConnectionConfig::stats_interval`] (ADR-0089).
///
/// Shared by [`Connection::handle_timeout`]'s consumer and producer sweeps so
/// the two sides cannot drift; both `ProducerState::last_rate_snapshot` and
/// `ConsumerState::last_rate_snapshot` are the same
/// `Option<(u64, u64, Instant)>` shape, which is why one free function serves
/// both without a trait.
///
/// `None` — no baseline yet, i.e. a slot created since the last sweep — is
/// **due**: `record_rate_window` computes no rate without a previous snapshot,
/// so that first visit only installs the baseline and the slot reports `0.0`
/// for one further interval. Java's recorders behave identically for a
/// producer or consumer constructed mid-window, so this is parity-correct
/// rather than a rounding artefact; it is also the property
/// `PartitionedProducer::aggregate_stats` and its consumer counterparts
/// document for a child added by `add_topic` or partition growth.
///
/// `deadline_with_clamp` keeps a near-`Duration::MAX` interval panic-free
/// (invariant #6).
fn rate_window_due(
    baseline: Option<(u64, u64, std::time::Instant)>,
    interval: std::time::Duration,
    now: std::time::Instant,
) -> bool {
    match baseline {
        None => true,
        Some((_, _, at)) => now >= crate::time::deadline_with_clamp(at, interval),
    }
}

/// Install a producer's or consumer's initial rolling-rate baseline at slot
/// creation, so its first sample lands one [`ConnectionConfig::stats_interval`]
/// later (ADR-0089). Java's stats recorder is likewise constructed with the
/// producer/consumer and arms its first `pulsarClient.timer()` tick then.
///
/// Seeding here rather than on the first sweep is load-bearing, not cosmetic.
/// The only deadline a bare producer/consumer connection arms is keepalive, and
/// its base (`last_activity`) is refreshed by every decoded frame (ADR-0058),
/// so on a continuously busy connection it keeps sliding and `handle_timeout`
/// may not run for a long time. A slot left unseeded would then go unswept for
/// exactly as long. A baseline, by contrast, is a fixed instant, so the
/// deadline `poll_timeout` arms from it cannot slide.
///
/// Both counters are zero at slot creation, so `(0, 0, at)` is precisely what
/// `record_rate_window` would have written.
///
/// No-ops in two cases, both of which leave `handle_timeout`'s
/// [`rate_window_due`] backstop to seed on the first sweep instead:
/// - the sweep is disabled (`None`), so the disabled default stays bit-for-bit what it was before
///   this feature existed — `last_rate_snapshot` is never written and no deadline is ever armed;
/// - the connection has no `last_activity` yet, i.e. the slot was opened before the handshake
///   response landed and there is no instant to anchor to.
fn seed_rate_window_baseline(
    baseline: &mut Option<(u64, u64, std::time::Instant)>,
    interval: Option<std::time::Duration>,
    last_activity: Option<std::time::Instant>,
) {
    if interval.is_some()
        && let Some(at) = last_activity
    {
        *baseline = Some((0, 0, at));
    }
}

#[derive(Debug, Clone, Copy)]
enum SubscribeAckAction {
    NotifyWaiter,
    ReleaseFlow,
}

// reason: variant payloads (handle, watcher_id) are carried for the derived
// `Debug` trace context and may be read by future dispatch paths; the compiler ignores derived
// traits for dead-code analysis so we scope a single allow here rather than reverting to a
// crate-wide blanket.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
enum PendingRequestKind {
    Lookup,
    PartitionedMetadata,
    ProducerOpen {
        handle: ProducerHandle,
    },
    ConsumerSubscribe {
        handle: ConsumerHandle,
    },
    ConsumerSeek {
        handle: ConsumerHandle,
    },
    ConsumerUnsubscribe {
        handle: ConsumerHandle,
    },
    ConsumerGetLastMessageId {
        handle: ConsumerHandle,
    },
    Ack {
        handle: ConsumerHandle,
        /// Injected-clock (ADR-0011) timestamp `Connection::ack` recorded this
        /// request at. Feeds the `ack_response_timeout` backstop deadline
        /// (`poll_timeout` / `handle_timeout`, issue #346) — a `CommandAck`
        /// whose response never arrives is reaped once
        /// `enqueued_at + ack_response_timeout` elapses.
        enqueued_at: Instant,
    },
    ProducerClose {
        handle: ProducerHandle,
    },
    /// Fire-and-forget close issued by the engines' last-clone drop guard
    /// ([`Connection::close_producer_forget`]). No `RequestFut` will ever
    /// drain the broker's ack, so the `Success`/`Error` handlers consume it
    /// in-place instead of recording an [`OpOutcome`] — recording one would
    /// leak a permanent `outcomes` entry per dropped producer on a
    /// long-lived connection.
    ProducerCloseForgotten {
        handle: ProducerHandle,
    },
    ConsumerClose {
        handle: ConsumerHandle,
    },
    /// Fire-and-forget close issued by the engines' last-clone drop guard.
    /// No `RequestFut` will ever drain the broker's ack, so the
    /// `Success`/`Error` handlers consume it in-place rather than leaking an
    /// [`OpOutcome`] entry.
    ConsumerCloseForgotten {
        handle: ConsumerHandle,
    },
    TopicWatcher {
        watcher_id: u64,
    },
    NewTxn,
    AddPartitionToTxn,
    AddSubscriptionToTxn,
    EndTxn,
    TcClientConnect,
    GetSchema,
}

impl Connection {
    /// Construct a fresh, unconnected sans-io `Connection`.
    ///
    /// `wall_clock` is mandatory — the sans-io state machine never reaches
    /// for the host clock on its own (ADR-0011). Engines pass:
    /// - tokio: `Arc::new(SystemTime::now)`
    /// - moonpool: a closure reading the virtual clock atomic
    pub fn new(
        config: ConnectionConfig,
        wall_clock: std::sync::Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Self {
        let lookup = LookupRegistry {
            max_pending: config.max_pending_lookups,
            ..LookupRegistry::default()
        };
        Self {
            config,
            operation_retry: crate::OperationRetryConfig::default(),
            state: HandshakeState::Uninitialized,
            broker_max_message_size: None,
            broker_protocol_version: 0,
            feature_flags: pb::FeatureFlags::default(),
            handshake_failure_reason: None,
            outbound: BytesMut::with_capacity(4 * 1024),
            pending_vectored_drain: None,
            outbound_segments: Vec::new(),
            pending_vectored_segments: Vec::new(),
            inbound: BytesMut::with_capacity(4 * 1024),
            events: VecDeque::new(),
            driver_retries: VecDeque::new(),
            outcomes: HashMap::new(),
            wakers: HashMap::new(),
            pending_requests: HashMap::new(),
            producers: HashMap::new(),
            producer_create_requests: HashMap::new(),
            in_flight_publish_snapshots: HashMap::new(),
            consumers: HashMap::new(),
            consumer_subscribe_requests: HashMap::new(),
            lookup,
            topic_watchers: TopicWatcherRegistry::default(),
            txn_client: TxnClient::new(0),
            next_request_id: 0,
            next_producer_id: 0,
            next_consumer_id: 0,
            next_watcher_id: 0,
            last_activity: None,
            keepalive_ping_outstanding: false,
            last_connected_at: None,
            last_disconnected_at: None,
            session_epoch: 0,
            wall_clock,
            anti_thrash: crate::anti_thrash::AntiThrashState::disabled(),
            #[cfg(feature = "scalable-topics")]
            scalable_sessions: HashMap::new(),
            #[cfg(feature = "scalable-topics")]
            next_scalable_session_id: 1,
            buggify: crate::Buggify::disabled(),
        }
    }

    /// Install a [`crate::Buggify`] helper on this connection. The
    /// helper is consulted at the four named choice points defined in
    /// [ADR-0048](../specs/adr/0048-buggify-fault-injection.md):
    /// `connection.reset.delay`, `batch_container.flush.split`,
    /// `handle_bytes.short_read`, and (via [`crate::Backoff`])
    /// `retry_clock.skew`.
    ///
    /// Engines call this once at construction time. The moonpool
    /// engine routes the RNG closure through `Providers::Random` for
    /// seed-controlled fault injection; the tokio engine ships the
    /// default [`crate::Buggify::disabled`] so production binaries
    /// never see synthetic faults even when compiled with the
    /// `buggify` feature on.
    ///
    /// Returns a clone of the installed helper so the engine can share
    /// the same fire-counter map with its `Backoff` schedule via
    /// [`crate::Backoff::install_buggify`].
    pub fn set_buggify(&mut self, buggify: crate::Buggify) -> crate::Buggify {
        self.buggify = buggify;
        self.buggify.clone()
    }

    /// Borrow the connection's [`crate::Buggify`] helper. Useful from
    /// engine driver loops that need to thread the same instance into
    /// out-of-state-machine fault points (e.g.
    /// [`crate::Backoff::install_buggify`]).
    #[must_use]
    pub fn buggify(&self) -> &crate::Buggify {
        &self.buggify
    }

    /// Returns the current handshake state.
    pub fn state(&self) -> HandshakeState {
        self.state
    }

    /// Transition the handshake state machine, logging the edge at `debug!`
    /// (ADR-0054 §5: proto owns the handshake state-transition logs — the
    /// state machine is the point of detection). Only the state names are
    /// logged; `auth_data` / challenge bytes never appear (ADR-0054 §3).
    /// No-op transitions (same state) are not logged.
    fn set_handshake_state(&mut self, next: HandshakeState) {
        if self.state != next {
            tracing::debug!(
                target: "magnetar_proto::conn",
                from = ?self.state,
                to = ?next,
                "handshake state transition",
            );
        }
        self.state = next;
    }

    /// Returns whether the connection is ready to accept producer / consumer opens.
    pub fn is_connected(&self) -> bool {
        matches!(self.state, HandshakeState::Connected)
    }

    /// `true` once the connection has entered any terminal state (`Closing`, `Closed`, or
    /// `Failed`). Mirrors Java `PulsarClient#isClosed`. Returns `false` for an active or
    /// still-handshaking connection — pair with [`Self::is_connected`] for the live test.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(
            self.state,
            HandshakeState::Closing | HandshakeState::Closed | HandshakeState::Failed
        )
    }

    /// `true` only when the **user** has asked for a graceful close — `Closing` (close in
    /// progress) or `Closed` (close complete). `Failed` (transport drop) returns `false`
    /// so the auto-reconnect supervisor can distinguish "user wants out" from "broker went
    /// away". Without this split, `mark_disconnected()` (called on `PeerClosed`) flipped
    /// the state to `Failed` and the supervisor's `is_closed()` check bailed out instead
    /// of running its reconnect loop. Mirrors Java `PulsarClient#getState()` returning
    /// `Closing` / `Closed` but NOT `Failed` when callers want to gate user-initiated
    /// shutdown.
    #[must_use]
    pub fn is_user_closed(&self) -> bool {
        matches!(self.state, HandshakeState::Closing | HandshakeState::Closed)
    }

    /// Wall-clock time the connection last reached [`HandshakeState::Connected`], if ever.
    /// Returns `None` before the first successful handshake.
    pub fn last_connected_timestamp(&self) -> Option<SystemTime> {
        self.last_connected_at
    }

    /// Wall-clock time the connection most recently left [`HandshakeState::Connected`] (to
    /// `Closing`, `Closed`, or `Failed`), if ever. Mirrors Java's
    /// `Producer/Consumer#getLastDisconnectedTimestamp`.
    pub fn last_disconnected_timestamp(&self) -> Option<SystemTime> {
        self.last_disconnected_at
    }

    /// Mark the connection as failed (e.g. peer EOF, I/O error) and record the disconnect
    /// timestamp. Called by the runtime driver when the underlying socket dies before a
    /// graceful close has been initiated.
    pub fn mark_disconnected(&mut self) {
        if !matches!(
            self.state,
            HandshakeState::Closed | HandshakeState::Failed | HandshakeState::Closing
        ) {
            self.last_disconnected_at = Some((self.wall_clock)());
        }
        self.set_handshake_state(HandshakeState::Failed);
    }

    /// Monotonic session epoch — incremented each time the supervisor invokes
    /// [`Self::reset`]. Callers that need to detect whether an in-flight operation
    /// survived a reconnect snapshot this value before issuing the op and compare
    /// after the response arrives. Mirrors Java `ClientCnx#getEpoch`.
    #[must_use]
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    /// Borrow the auto-reconnect supervisor configuration, if one was set. The
    /// runtime driver reads this between disconnects to decide whether to
    /// re-handshake. Returning `None` keeps the pre-supervisor behavior (driver
    /// exits on first I/O failure).
    #[must_use]
    pub fn supervisor_config(&self) -> Option<&crate::supervisor::SupervisorConfig> {
        self.config.supervisor.as_ref()
    }

    /// Borrow the broker-operation retry policy.
    #[must_use]
    pub fn operation_retry_config(&self) -> &crate::OperationRetryConfig {
        &self.operation_retry
    }

    /// Replace the broker-operation retry policy.
    pub fn set_operation_retry_config(&mut self, config: crate::OperationRetryConfig) {
        self.operation_retry = config;
    }

    /// Total deadline applied by runtime engines to one logical setup operation.
    #[must_use]
    pub fn operation_timeout(&self) -> Duration {
        self.config.operation_timeout
    }

    /// The per-attempt initial-dial timeout ([`ConnectionConfig::connect_timeout`]).
    /// The runtime supervisor reads this to bound each reconnect dial under the
    /// engine clock, matching the initial connect's retry path (ADR-0052).
    #[must_use]
    pub fn connect_timeout(&self) -> std::time::Duration {
        self.config.connect_timeout
    }

    /// Configure the anti-thrash detector (ADR-0028). Pass `threshold = None`
    /// to disable. Engines call this once at supervisor start time after
    /// reading [`crate::supervisor::SupervisorConfig::anti_thrash_threshold`]
    /// + [`crate::supervisor::SupervisorConfig::max_backoff_after_thrash`].
    ///
    /// The detector is a pure observable — it tracks re-attach outcomes and
    /// emits cooldown decisions via [`Self::anti_thrash_tick`]; it never
    /// queues frames or events.
    pub fn set_anti_thrash(
        &mut self,
        threshold: Option<crate::anti_thrash::AntiThrashThreshold>,
        cooldown: Duration,
    ) {
        self.anti_thrash.set_threshold(threshold, cooldown);
    }

    /// Borrow the anti-thrash state. Engines use this for diagnostics + the
    /// `tick`-based supervisor gate.
    #[must_use]
    pub fn anti_thrash_state(&self) -> &crate::anti_thrash::AntiThrashState {
        &self.anti_thrash
    }

    /// Mutable borrow of the anti-thrash state. Used by tests and the engine
    /// drivers that need to call [`crate::anti_thrash::AntiThrashState::clear_cooldown`]
    /// after a cooldown sleep has elapsed.
    pub fn anti_thrash_state_mut(&mut self) -> &mut crate::anti_thrash::AntiThrashState {
        &mut self.anti_thrash
    }

    /// Record a re-attach outcome into the anti-thrash detector. No-op when
    /// the detector is disabled (the default).
    pub fn record_reattach_outcome(
        &mut self,
        now: Instant,
        handle: crate::anti_thrash::ReAttachHandle,
        kind: crate::anti_thrash::ReAttachOutcomeKind,
    ) {
        // ADR-0049 pair-assertion (positive): when the anti-thrash
        // detector is ARMED, a `TcpDropAfterReAttach` outcome must
        // come from a connection that has previously observed at
        // least one re-attach. Three valid signals of that
        // observation:
        //   1. The session has been reset at least once (`session_epoch > 0`) — live-driver path:
        //      the supervisor calls `reset()` before each redial.
        //   2. The anti-thrash detector itself recorded a prior `ReAttachOk`
        //      (`last_reattach_at().is_some()`) — synthetic-test path used by the differential
        //      anti-thrash equivalence harness.
        //   3. The detector is DISABLED (no threshold). In that state `record_reattach_outcome` is
        //      a no-op anyway (the detector's `record` exits early), so the call cannot misclassify
        //      anything — tests exercising the "default-off" path drive the same surface and must
        //      not panic.
        // With the detector armed AND neither of (1) or (2)
        // holding, the supervisor is misclassifying the very first
        // socket as a re-attach — exactly what ADR-0028 must
        // refuse.
        debug_assert!(
            !matches!(
                kind,
                crate::anti_thrash::ReAttachOutcomeKind::TcpDropAfterReAttach
            ) || self.anti_thrash.threshold().is_none()
                || self.session_epoch > 0
                || self.anti_thrash.last_reattach_at().is_some(),
            "TcpDropAfterReAttach recorded with session_epoch=0 AND no prior re-attach — \
             supervisor misclassified first-connect as a re-attach"
        );
        // ADR-0049 pair-assertion (negative space): a `ReAttachOk`
        // outcome must reference a producer or consumer that this
        // Connection actually has open — i.e. the broker acked a
        // CommandProducer/CommandSubscribe we hold the slot for. A
        // stale `ReAttachHandle` surviving past `close_producer` /
        // `close_consumer` would be a "ghost handle" bug that leaks
        // cooldown weight into the anti-thrash detector against a
        // slot we no longer own. `TcpDropAfterReAttach` is exempt:
        // the engine driver records it with a placeholder
        // `ProducerHandle(0)` because the close signal is
        // connection-wide, not per-handle.
        debug_assert!(
            !matches!(kind, crate::anti_thrash::ReAttachOutcomeKind::ReAttachOk)
                || match handle {
                    crate::anti_thrash::ReAttachHandle::Producer(h) =>
                        self.producers.contains_key(&h),
                    crate::anti_thrash::ReAttachHandle::Consumer(h) =>
                        self.consumers.contains_key(&h),
                },
            "record_reattach_outcome(ReAttachOk) with unknown handle (post-close ghost?)"
        );
        let was_cooldown = self.anti_thrash.tick(now);
        self.anti_thrash.record(now, kind, handle);
        let is_cooldown = self.anti_thrash.tick(now);
        match (was_cooldown, is_cooldown) {
            (
                crate::anti_thrash::AntiThrashDisposition::Normal,
                crate::anti_thrash::AntiThrashDisposition::Cooldown { until },
            ) => {
                self.events
                    .push_back(ConnectionEvent::AntiThrashCooldown { until });
            }
            (
                crate::anti_thrash::AntiThrashDisposition::Cooldown { .. },
                crate::anti_thrash::AntiThrashDisposition::Normal,
            ) => {
                self.events.push_back(ConnectionEvent::AntiThrashCleared);
            }
            _ => {}
        }
    }

    /// Tell the anti-thrash detector that a healthy first-op-after-attach
    /// completed (e.g. a `SendReceipt` or delivered `Message`). Per ADR-0028,
    /// this is the explicit reset signal that proves the broker has
    /// stabilised. Clears any active cooldown and emits
    /// [`ConnectionEvent::AntiThrashCleared`] if the cooldown was active.
    pub fn record_first_op_success(&mut self, now: Instant) {
        // ADR-0049 pair-assertion (positive): a first-op-success
        // must come AFTER a user-driven `create_producer` /
        // `subscribe`, so the connection must hold at least one
        // producer or consumer slot. An empty slot map at this point
        // means the engine driver fired the signal speculatively
        // before the user opened any handle — there's no first op to
        // succeed against. (The handshake state itself is
        // INTENTIONALLY not checked here because the differential
        // anti-thrash test sequences `record_first_op_success`
        // against a `Failed` state to exercise the cooldown-clear
        // path in isolation; live drivers always re-handshake before
        // signalling first-op-success but the assertion would
        // pessimise that test surface.)
        debug_assert!(
            !self.producers.is_empty() || !self.consumers.is_empty(),
            "record_first_op_success with empty producer + consumer maps — \
             nothing has been opened yet"
        );
        // ADR-0049 pair-assertion (negative space): the connection
        // must NOT be in a user-closed terminal state. `Closing` /
        // `Closed` means the user explicitly asked us to tear down;
        // recording a first-op-success against that state would
        // resurrect the anti-thrash detector against a connection
        // that no longer matters and could even race the close path
        // into a leaked cooldown. `Failed` is allowed because it is
        // a transport-level drop the supervisor recovers from.
        debug_assert!(
            !matches!(self.state, HandshakeState::Closing | HandshakeState::Closed),
            "record_first_op_success called on user-closed connection (state={:?})",
            self.state
        );
        let was_cooldown = matches!(
            self.anti_thrash.tick(now),
            crate::anti_thrash::AntiThrashDisposition::Cooldown { .. }
        );
        self.anti_thrash.record_first_op_success();
        if was_cooldown {
            self.events.push_back(ConnectionEvent::AntiThrashCleared);
        }
    }

    /// Inspect the current anti-thrash disposition. `now` is the engine's
    /// `Instant::now()` snapshot. Sans-io: the state machine never reads the
    /// clock itself.
    #[must_use]
    pub fn anti_thrash_tick(&self, now: Instant) -> crate::anti_thrash::AntiThrashDisposition {
        self.anti_thrash.tick(now)
    }

    /// Reset the state machine for a fresh handshake on a new transport. Used by the
    /// runtime supervisor between [`mark_disconnected`](Self::mark_disconnected) and the
    /// new TCP / TLS handshake.
    ///
    /// Semantics, in order:
    ///
    /// 1. Bump [`Self::session_epoch`].
    /// 2. Emit [`OpOutcome::SessionLost`] for every pending request (lookup, seek, ack, transaction
    ///    round-trip, …). The corresponding user futures are woken with that outcome.
    /// 3. Snapshot every in-flight producer publish into `in_flight_publish_snapshots` (key =
    ///    `ProducerHandle`, value = ordered `Vec<OpSend>` with wakers cleared). Wake each original
    ///    send-future waker exactly once — but do *not* install a `SessionLost` outcome on the
    ///    publish key. The user future re-polls, finds no outcome, re-registers, and stays pending
    ///    until the replayed [`crate::producer::OpSend`] surfaces its eventual `CommandSendReceipt`
    ///    (transparent at-least-once replay). Clear every producer's batch container so unflushed
    ///    partial batches do not survive the reconnect — the caller is responsible for those.
    /// 4. Reset every consumer's queue + pending seek + ack tracker. Producers and consumers
    ///    themselves are *not* removed — [`Self::rebuild_producers`] and
    ///    [`Self::rebuild_consumers`] replay their `CommandProducer` / `CommandSubscribe` against
    ///    the new transport.
    /// 5. Clear connection-level outbound + inbound byte buffers; flush queued events.
    /// 6. Snap the state machine back to [`HandshakeState::Uninitialized`] so
    ///    [`Self::begin_handshake`] can fire again on the new socket.
    pub fn reset(&mut self) {
        self.session_epoch = self.session_epoch.wrapping_add(1);

        // (2) Fail every pending request and wake its waiter. Forgotten
        // producer/consumer closes have no waiter by construction, so consume
        // them without materializing an undrainable outcome.
        for (request_id, kind) in std::mem::take(&mut self.pending_requests) {
            let key = PendingOpKey::Request(request_id);
            if let PendingRequestKind::ConsumerUnsubscribe { handle } = kind {
                self.clear_consumer_unsubscribe(handle, request_id);
                if !self.wakers.contains_key(&key) {
                    continue;
                }
            }
            if matches!(
                kind,
                PendingRequestKind::ProducerCloseForgotten { .. }
                    | PendingRequestKind::ConsumerCloseForgotten { .. }
            ) {
                let _ = self.wakers.remove(&key);
                continue;
            }
            self.outcomes.insert(key, OpOutcome::SessionLost { key });
            if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }

        // (3) Snapshot every in-flight publish so [`rebuild_producers`] can replay it on
        // the freshly-handshaked session. We pluck the wakers out of each `OpSend` (so we
        // wake the user's future without double-firing on the replayed receipt) and stash
        // the now-wakerless `OpSend` under its producer's snapshot bucket. We deliberately
        // do *not* install a `SessionLost` outcome on the Send key — the user future polls
        // after the wake-up, finds the slot empty, re-registers, and will eventually see
        // the receipt from the replayed publish.
        //
        // We APPEND new snapshots onto the existing `in_flight_publish_snapshots` rather
        // than clearing first — the supervisor may cycle through `reset()` multiple times
        // (broker rejects the rebuild, drops the connection, supervisor redials, calls
        // `reset()` again) before `rebuild_producers` actually drains the snapshots onto
        // a successful session. Pre-fix the second `reset()` wiped the first reset's
        // snapshots so the user's pre-restart send was silently lost. The
        // `rebuild_producers` path is the single consumer of this map (it `.remove()`s
        // each handle's vector) so accumulation is safe — there's no double-replay
        // because anything successfully replayed is gone from the map.
        let producer_handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in producer_handles {
            let snap = self
                .producers
                .get(&handle)
                .map(|slot| slot.state.lock().snapshot_pending_sends());
            if let Some((wakers, snapshots)) = snap {
                for (seq, waker_opt) in wakers {
                    // Prefer the producer-stored waker (registered via
                    // ProducerState::register_waker); fall back to the connection-level
                    // slab when no producer-stored waker was set. Wake exactly once —
                    // no outcome is installed, so the future will re-register on its
                    // next poll and stay pending until the replayed receipt lands.
                    let key = PendingOpKey::Send(handle, seq);
                    if let Some(w) = waker_opt {
                        // Drop the connection-level waker too so the next call to
                        // `register_waker` from the re-polling future is the one that
                        // gets fired on receipt — no stale wakers linger.
                        let _ = self.wakers.remove(&key);
                        w.wake();
                    } else if let Some(w) = self.wakers.remove(&key) {
                        w.wake();
                    }
                }
                if !snapshots.is_empty() {
                    self.in_flight_publish_snapshots
                        .entry(handle)
                        .or_default()
                        .extend(snapshots);
                }
            }
        }

        // Sweep the remaining slab wakers. Request keys get `SessionLost` —
        // their broker round-trip died with the session. Send keys must NOT:
        // a send future that re-polled during a PREVIOUS reset's snapshot
        // window parks its waker on the slab (the slot op is in the snapshot,
        // see `register_waker`), and the transparent-replay contract keeps it
        // pending across any number of resets until the replayed receipt
        // lands. Wake it without an outcome so it re-registers, exactly like
        // the snapshot path above.
        let leftover_keys: Vec<PendingOpKey> = self.wakers.keys().copied().collect();
        for key in leftover_keys {
            if let Some(w) = self.wakers.remove(&key) {
                if !matches!(key, PendingOpKey::Send(..)) {
                    self.outcomes.insert(key, OpOutcome::SessionLost { key });
                }
                w.wake();
            }
        }

        // (3) Reset consumer-side per-session state. We keep the ConsumerState struct
        // itself (Stage 3 will replay CommandSubscribe), but clear anything that was
        // pinned to the now-dead session: in-flight seek, in-memory queue, ack-tracker
        // state, broker permits. The runtime layer is responsible for re-subscribing
        // and re-issuing the initial flow.
        for slot in self.consumers.values() {
            let mut consumer = slot.state.lock();
            consumer.queue.clear();
            consumer.pending_seek = None;
            consumer.granted_permits = 0;
            consumer.permit_balance = 0;
            consumer.consumed_since_flow = 0;
            consumer.dead_letter_pending.clear();
            consumer.batch_ack_tracker.clear();
            // Wake every in-flight receive so they observe the queue is empty
            // and re-register on the freshly-handshaked connection.
            let wakers: Vec<std::task::Waker> = consumer.receive_wakers.drain().collect();
            drop(consumer); // Release the slot lock BEFORE calling user-supplied wakers.
            for w in wakers {
                w.wake();
            }
        }

        // (4) Drop queued events + raw bytes. Anything not yet observed by the runtime
        // belongs to the dead session.
        self.events.clear();
        self.driver_retries.clear();
        self.outbound.clear();
        self.inbound.clear();

        // Lookup / topic-watcher registries hold no Wakers themselves — their futures
        // poll via the per-request waker slab we already drained above. Clearing the
        // registries avoids replaying stale `Connect`/`Redirect` traffic on the new
        // socket.
        //
        // **Belt-and-suspenders drain** (lookup multi-agent review HIGH-3): every
        // in-flight lookup / partitioned-metadata request is *also* keyed in
        // `pending_requests` on the happy path, so the first loop above
        // (lines ~649-661) has already published `OpOutcome::SessionLost` and
        // woken the registered waker for each one. We re-iterate the lookup
        // registry's own key set here as a defensive measure: any future
        // refactor that desynchronises `pending_requests` from the lookup
        // registry (e.g. an internal retry path that inserts into `lookup`
        // before allocating its `pending_requests` slot) would silently
        // re-introduce the "lookup parked until 30s operation_timeout" race
        // without this guard. The publish path is idempotent — if the first
        // loop already wrote a `SessionLost` outcome, the second write is a
        // no-op overwrite of an identical value; if a waker was already
        // consumed, the second `wake_for_request` call finds nothing to
        // wake. Order strictly: write outcomes → wake → clear the registry.
        // The waker invocation may race with the eventual registry clear,
        // but the outcome is already published so the freshly-woken future
        // observes `SessionLost` on its next `take_outcome` call regardless
        // of the registry's state.
        let stranded_lookup_ids = self.lookup.pending_request_ids();
        for rid in stranded_lookup_ids {
            let key = PendingOpKey::Request(rid);
            self.outcomes.insert(key, OpOutcome::SessionLost { key });
            self.wake_for_request(rid);
        }
        self.lookup = LookupRegistry {
            max_pending: self.config.max_pending_lookups,
            ..LookupRegistry::default()
        };
        self.topic_watchers = TopicWatcherRegistry::default();

        // (5) Back to Uninitialized so begin_handshake on the freshly-handshaked socket
        // succeeds.
        self.set_handshake_state(HandshakeState::Uninitialized);
        self.broker_max_message_size = None;
        self.broker_protocol_version = 0;
        self.feature_flags = pb::FeatureFlags::default();
        self.handshake_failure_reason = None;
        // ADR-0048 buggify point: when the `connection.reset.delay` label
        // fires, leave the prior `last_activity` timestamp intact so the
        // post-reset state machine inherits an older keepalive baseline.
        // The engine's keepalive timer therefore arms one extra idle
        // tick before the next ping. Sans-io: the fault is a pure
        // state-skip, no clock read, no event-queue mutation.
        if !self
            .buggify
            .should_fire(crate::buggify::labels::CONNECTION_RESET_DELAY, 0.05)
        {
            self.last_activity = None;
        }
        // A fresh session starts with a clean keepalive watchdog: no ping is in
        // flight against the new socket (ADR-0058). Cleared unconditionally — the
        // `connection.reset.delay` buggify only ages `last_activity`, it must not
        // carry a stale outstanding-ping flag across the reconnect.
        self.keepalive_ping_outstanding = false;
    }

    /// Resolve **every** pending operation with a terminal
    /// [`OpOutcome::Terminal`] outcome, wake its future, and queue a
    /// [`ConnectionEvent::Closed`] so event-stream waiters
    /// (`ProducerReady` / `SubscribeAcked`) unblock.
    ///
    /// Called by a **plain** (non-supervised) driver on terminal exit — a
    /// fatal decode, a peer close, or an I/O error — where there is no
    /// reconnect to replay against. Unlike [`Self::reset`], which deliberately
    /// keeps `Send` keys pending for the supervisor's transparent
    /// at-least-once replay, this terminates `Send` keys too: with no session
    /// to come back to, a parked `send()` / `subscribe()` / `receive()`
    /// future must observe a terminal error promptly instead of hanging
    /// forever (the no-progress stall this method exists to kill).
    ///
    /// Does NOT change the handshake state — the driver pairs this with
    /// [`Self::mark_disconnected`]. Idempotent: a later [`Self::close`] only
    /// overwrites identical terminal outcomes and re-queues a `Closed` event.
    ///
    /// Lock-ordering (ADR-0038): runs under the global connection mutex and
    /// takes each per-slot mutex *below* it, never above; every slot guard is
    /// dropped BEFORE the user wakers fire, so a waker that re-enters the
    /// connection cannot deadlock.
    pub fn fail_all_pending(&mut self, reason: &str) {
        // (1) Terminate every pending request and wake its waiter. Forgotten
        // producer/consumer closes have no waiter by construction, so consume
        // them without materializing an undrainable outcome.
        for (request_id, kind) in std::mem::take(&mut self.pending_requests) {
            let key = PendingOpKey::Request(request_id);
            if let PendingRequestKind::ConsumerUnsubscribe { handle } = kind {
                self.clear_consumer_unsubscribe(handle, request_id);
                if !self.wakers.contains_key(&key) {
                    continue;
                }
            }
            if matches!(
                kind,
                PendingRequestKind::ProducerCloseForgotten { .. }
                    | PendingRequestKind::ConsumerCloseForgotten { .. }
            ) {
                let _ = self.wakers.remove(&key);
                continue;
            }
            self.outcomes.insert(
                key,
                OpOutcome::Terminal {
                    key,
                    reason: reason.to_owned(),
                },
            );
            if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }

        // (2) Terminate every in-flight publish. Drain each producer's pending
        // `OpSend`s WITHOUT a replay snapshot (there is no session to replay
        // onto), install a `Terminal` outcome on each `Send` key, and wake the
        // future. Take the per-slot lock, drain, DROP it, then wake.
        //
        // We ALSO flip the slot's `closed` flag inside this same per-slot lock
        // scope (ADR-0059): a terminal drop is final, so a
        // `queue_send` issued AFTER it must fast-fail synchronously with
        // `ProducerError::Closed` via the existing `if self.closed` guard
        // (`producer.rs`) instead of registering a doomed pending op that no
        // driver is left to resolve. Setting `closed` here keeps the ADR-0038
        // lock order intact — the global connection mutex is already held
        // above, the per-slot mutex is taken below it in ONE acquisition (no
        // second slot loop, no connection-mutex read on the send hot path),
        // and the guard is dropped before the user wakers fire.
        let producer_handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in producer_handles {
            let drained = self.producers.get(&handle).map(|slot| {
                let mut slot_state = slot.state.lock();
                slot_state.closed = true;
                slot_state.drain_pending_sends()
            });
            let Some(drained) = drained else { continue };
            for (seq, waker_opt) in drained {
                let key = PendingOpKey::Send(handle, seq);
                self.outcomes.insert(
                    key,
                    OpOutcome::Terminal {
                        key,
                        reason: reason.to_owned(),
                    },
                );
                // Prefer the producer-stored waker; drop any connection-level
                // slab waker for this key too so it is not double-fired below.
                if let Some(w) = waker_opt {
                    let _ = self.wakers.remove(&key);
                    w.wake();
                } else if let Some(w) = self.wakers.remove(&key) {
                    w.wake();
                }
            }
        }

        // (2b) Issue #369, Change 2 (separable from Change 1 — a reviewer can drop this
        // block without unpicking the send-timeout sweep above): terminalize every
        // publish RELOCATED by a prior `reset()` into `in_flight_publish_snapshots` too.
        // Symmetric with `fail_producer_open_with_broker_error`'s snapshot drain — without
        // it, a send parked in the snapshot bucket at give-up time depends on its future
        // having already re-polled and re-registered into the connection-wide waker slab
        // (the step-(3) sweep below only wakes `self.wakers`, which the snapshot's own
        // wakerless `OpSend` is not a member of). Draining the bucket directly removes
        // that dependency instead of relying on the woken task being rescheduled in time.
        let snapshot_handles: Vec<ProducerHandle> =
            self.in_flight_publish_snapshots.keys().copied().collect();
        for handle in snapshot_handles {
            self.terminalize_snapshot_bucket(handle, reason);
        }

        // (3) Sweep any leftover slab wakers — BOTH `Request` and `Send` keys
        // get a `Terminal` outcome (no replay carve-out here, unlike `reset`).
        let leftover_keys: Vec<PendingOpKey> = self.wakers.keys().copied().collect();
        for key in leftover_keys {
            self.outcomes.insert(
                key,
                OpOutcome::Terminal {
                    key,
                    reason: reason.to_owned(),
                },
            );
            if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }

        // (4) Wake every in-flight receive so it observes the terminal drop and
        // returns an error (the engine's receive future re-polls, sees
        // `!is_connected()` after the paired `mark_disconnected`, and errors).
        // Drop the slot lock BEFORE waking. Issue #348: a parked
        // `next_active_change()` future is a terminal-close observer too —
        // wake its waker slab in the same pass so it resolves promptly
        // instead of hanging forever.
        for slot in self.consumers.values() {
            let (receive_wakers, active_change_wakers): (
                Vec<std::task::Waker>,
                Vec<std::task::Waker>,
            ) = {
                let mut consumer = slot.state.lock();
                (
                    consumer.receive_wakers.drain().collect(),
                    consumer.active_change_wakers.drain().collect(),
                )
            };
            for w in receive_wakers {
                w.wake();
            }
            for w in active_change_wakers {
                w.wake();
            }
        }

        // (5) Terminate any stranded lookup requests not already keyed in
        // `pending_requests` (belt-and-suspenders, mirrors `reset`).
        let stranded_lookup_ids = self.lookup.pending_request_ids();
        for rid in stranded_lookup_ids {
            let key = PendingOpKey::Request(rid);
            self.outcomes.insert(
                key,
                OpOutcome::Terminal {
                    key,
                    reason: reason.to_owned(),
                },
            );
            self.wake_for_request(rid);
        }

        // (6) Queue a `Closed` event. Producer/subscribe readiness waiters park
        // on the event queue plus a runtime notification, NOT the waker slab,
        // so the `Closed` event is the only thing that unblocks them on a
        // terminal drop.
        self.driver_retries.clear();
        self.events.push_back(ConnectionEvent::Closed {
            reason: Some(reason.to_owned()),
        });
    }

    /// Install a TERMINAL failure for a SINGLE producer handle whose open could
    /// not be recovered — the configured operation-retry budget was exhausted
    /// (issue #302, ADR-0080). Scoped
    /// per-handle counterpart of [`Self::fail_all_pending`] step (2): it drains
    /// every staged / in-flight `OpSend` for this producer, including replay
    /// snapshots captured by [`Self::reset`] (there is no recoverable session
    /// to replay onto), installs an
    /// [`OpOutcome::Terminal`] on each `Send` key, flips the slot's `closed`
    /// flag so any later `queue_send` fast-fails with `ProducerError::Closed`
    /// instead of registering a doomed pending op, wakes each parked send
    /// waker so `Producer::send` resolves `Err(PeerClosed)` promptly, drops the
    /// producer state so it is not re-emitted on the next reconnect, and pushes
    /// a [`ConnectionEvent::ProducerOpenFailed`] so an open future parked on the
    /// event stream also observes the terminal disposition.
    ///
    /// Before this method existed, a one-shot transient retry that gave up left
    /// the per-slot `broker_ready` gate closed forever:
    /// [`Self::drain_producer_outbound`] refuses to flush staged frames while
    /// `!broker_ready`, so `send()` stayed PENDING with no error and no
    /// progress. Surfacing the terminal error lets caller-side
    /// reconnect/rebuild logic fire.
    ///
    /// Lock-ordering (ADR-0038): the global connection mutex is held by the
    /// `&mut self` receiver; each per-slot mutex is taken BELOW it in a single
    /// acquisition, and the guard is dropped before user wakers fire.
    pub fn fail_producer_open(&mut self, handle: ProducerHandle, reason: &str) {
        self.fail_producer_open_with_broker_error(
            handle,
            pb::ServerError::MetadataError as i32,
            reason,
        );
    }

    /// Terminalize one opening producer while preserving the broker's exact
    /// error code and message.
    ///
    /// Runtime retry legs use this when a prerequisite lookup returns a
    /// terminal broker error before another producer-open can be issued.
    pub fn fail_producer_open_with_broker_error(
        &mut self,
        handle: ProducerHandle,
        code: i32,
        reason: &str,
    ) {
        // Drain + terminalize every pending send under the per-slot lock,
        // flip `closed` in the same scope, then wake outside the lock.
        let drained = self.producers.get(&handle).map(|slot| {
            let mut slot_state = slot.state.lock();
            slot_state.closed = true;
            slot_state.broker_ready = false;
            slot_state.drain_pending_sends()
        });
        if let Some(drained) = drained {
            for (seq, waker_opt) in drained {
                let key = PendingOpKey::Send(handle, seq);
                self.outcomes.insert(
                    key,
                    OpOutcome::Terminal {
                        key,
                        reason: reason.to_owned(),
                    },
                );
                if let Some(w) = waker_opt {
                    let _ = self.wakers.remove(&key);
                    w.wake();
                } else if let Some(w) = self.wakers.remove(&key) {
                    w.wake();
                }
            }
        }
        // Sends extracted by `reset()` no longer live in the producer slot.
        // Terminalize those replay snapshots too; their futures re-registered
        // in the connection-wide waker slab after reset and otherwise have no
        // remaining correlation surface.
        self.terminalize_snapshot_bucket(handle, reason);
        // Drop the now-dead producer state so a subsequent reconnect rebuild
        // does not re-emit a `CommandProducer` for a handle the user has been
        // told is terminally failed.
        self.producers.remove(&handle);
        self.producer_create_requests.remove(&handle);
        self.events.retain(|event| {
            !matches!(event, ConnectionEvent::ProducerReady { handle: event_handle, .. } if *event_handle == handle)
        });
        self.events.push_back(ConnectionEvent::ProducerOpenFailed {
            handle,
            code,
            message: reason.to_owned(),
        });
        self.driver_retries.retain(|retry| {
            !matches!(retry, crate::DriverRetry::Producer { handle: event_handle, .. } if *event_handle == handle)
        });
    }

    /// Install a TERMINAL failure for a SINGLE consumer handle whose subscribe
    /// could not be recovered — the configured operation-retry budget was
    /// exhausted (issue #302, ADR-0080). Sets the
    /// per-consumer [`crate::consumer::ConsumerState::terminal_failure`] marker
    /// so [`Self::consumer_handle_is_terminal`] returns `true` for this handle,
    /// drains + wakes every parked `receive()` waker so the future re-polls and
    /// resolves `Err` (instead of blocking forever on a subscription that will
    /// never reattach — `granted_permits`/`permit_balance` stay `0`), drops the per-session
    /// subscribe request so a reconnect rebuild does not re-attach it, and
    /// pushes a [`ConnectionEvent::SubscribeFailed`] so an open future parked on
    /// the event stream also observes the terminal disposition.
    ///
    /// This is the consumer twin of [`Self::fail_producer_open`]. Receive
    /// futures distinguish this genuinely-terminal state from a recoverable
    /// supervised `Failed` window (issue #299) via
    /// [`Self::consumer_handle_is_terminal`]: a recoverable `Failed` re-parks,
    /// a terminal failure resolves `Err`.
    ///
    /// Lock-ordering (ADR-0038): global mutex held by `&mut self`; the per-slot
    /// mutex is taken below it, and the receive wakers fire after the slot lock
    /// is dropped.
    pub fn fail_consumer_subscribe(&mut self, handle: ConsumerHandle, reason: &str) {
        self.fail_consumer_subscribe_with_broker_error(
            handle,
            pb::ServerError::MetadataError as i32,
            reason,
        );
    }

    /// Terminalize one opening consumer while preserving the broker's exact
    /// error code and message.
    ///
    /// Runtime retry legs use this when a prerequisite lookup returns a
    /// terminal broker error before another subscribe can be issued.
    pub fn fail_consumer_subscribe_with_broker_error(
        &mut self,
        handle: ConsumerHandle,
        code: i32,
        reason: &str,
    ) {
        // Set the terminal marker + drain the parked receive wakers under the
        // slot lock, then wake them outside it. Issue #348: also drain the
        // active-change wakers in the same pass — a parked
        // `next_active_change()` future must observe this terminal failure
        // exactly like `receive()` does.
        let (wakers, active_change_wakers): (Vec<std::task::Waker>, Vec<std::task::Waker>) =
            match self.consumers.get(&handle) {
                Some(slot) => {
                    let mut c = slot.state.lock();
                    c.terminal_failure = Some(reason.to_owned());
                    c.granted_permits = 0;
                    c.permit_balance = 0;
                    (
                        c.receive_wakers.drain().collect(),
                        c.active_change_wakers.drain().collect(),
                    )
                }
                None => (Vec::new(), Vec::new()),
            };
        for w in wakers {
            w.wake();
        }
        for w in active_change_wakers {
            w.wake();
        }
        // Drop the per-session subscribe request so a reconnect rebuild does
        // not re-attach a consumer the user has been told is terminally
        // failed. The `ConsumerState` slot itself is RETAINED so a parked
        // `receive()` future can still read `terminal_failure` on re-poll.
        self.consumer_subscribe_requests.remove(&handle);
        self.events.push_back(ConnectionEvent::SubscribeFailed {
            handle,
            code,
            message: reason.to_owned(),
        });
        self.driver_retries.retain(|retry| {
            !matches!(retry, crate::DriverRetry::Consumer { handle: event_handle, .. } if *event_handle == handle)
        });
    }

    /// `true` only when the connection is GENUINELY terminal for *every* handle
    /// — there is no recovery path left: the user asked for a graceful close
    /// ([`Self::is_user_closed`]), OR the transport is `Failed` AND no
    /// supervisor is configured to reconnect it.
    ///
    /// Distinct from [`Self::is_closed`], which also returns `true` for the
    /// TRANSIENT `Failed` window a supervisor walks through between
    /// [`Self::mark_disconnected`] and its [`Self::reset`] + re-handshake.
    /// Issue #299: a `receive()` / `send()` future woken across a transport
    /// drop must re-park (not error) while that window is recoverable, and only
    /// surface `Err` once the state is truly terminal. The receive futures gate
    /// on [`Self::consumer_handle_is_terminal`] (which also folds in a
    /// per-handle terminal failure from [`Self::fail_consumer_subscribe`]); the
    /// send futures already re-park naturally on a `Send` key staying PENDING
    /// and surface `Err` on the [`OpOutcome::Terminal`] that
    /// [`Self::fail_producer_open`] installs.
    #[must_use]
    pub fn is_terminally_closed(&self) -> bool {
        if self.is_user_closed() {
            return true;
        }
        matches!(self.state, HandshakeState::Failed) && self.supervisor_config().is_none()
    }

    /// `true` when a `receive()` on this consumer handle must resolve a terminal
    /// `Err` instead of re-parking — the connection is terminally closed
    /// ([`Self::is_terminally_closed`]), OR a per-handle terminal subscribe
    /// failure has been installed ([`Self::fail_consumer_subscribe`], issue
    /// #302), OR the handle is no longer registered (closed / removed).
    ///
    /// Returns `false` for a recoverable supervised `Failed` window so a
    /// `receive()` woken by [`Self::reset`] (which drains the parked receive
    /// wakers WHILE still `Failed`) re-parks and transparently resumes once the
    /// supervisor reconnects + the rebuild replays `CommandSubscribe` (issue
    /// #299).
    #[must_use]
    pub fn consumer_handle_is_terminal(&self, handle: ConsumerHandle) -> bool {
        if self.is_terminally_closed() {
            return true;
        }
        match self.consumers.get(&handle) {
            Some(slot) => {
                let c = slot.state.lock();
                c.closed || c.terminal_failure.is_some()
            }
            // Unknown handle ⇒ treat as terminal (mirrors `consumer_is_closed`).
            None => true,
        }
    }

    /// Number of consecutive transient `CommandProducer` rejections recorded
    /// for this producer since its last success (issue #302). The runtime
    /// drivers read this to size their exponential-backoff sleep before the
    /// next lookup + retry. Returns `0` for an unknown handle.
    #[must_use]
    pub fn producer_transient_open_attempts(&self, handle: ProducerHandle) -> u32 {
        self.producers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().transient_open_attempts)
    }

    /// Number of consecutive transient `CommandSubscribe` rejections recorded
    /// for this consumer since its last success (issue #302). The runtime
    /// drivers read this to size their exponential-backoff sleep before the
    /// next lookup + retry. Returns `0` for an unknown handle.
    #[must_use]
    pub fn consumer_transient_subscribe_attempts(&self, handle: ConsumerHandle) -> u32 {
        self.consumers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().transient_subscribe_attempts)
    }

    /// Whether this producer completed at least one broker attachment.
    ///
    /// Unlike the current-session `broker_ready` gate, this remains true across
    /// a supervised reset so an opening future can observe a success that was
    /// immediately followed by a broker-requested migration.
    #[must_use]
    pub fn producer_has_ever_attached(&self, handle: ProducerHandle) -> bool {
        self.producers
            .get(&handle)
            .is_some_and(|slot| slot.state.lock().has_ever_attached)
    }

    /// Whether this consumer completed at least one broker attachment.
    ///
    /// This is a durable lifecycle fact for retry ownership and diagnostics.
    /// Subscribe waiters use their stable logical token instead: a reset
    /// transfers that token to the rebuilt wire request, so an old-session
    /// acknowledgement cannot complete a new attachment.
    #[must_use]
    pub fn consumer_has_ever_attached(&self, handle: ConsumerHandle) -> bool {
        self.consumers
            .get(&handle)
            .is_some_and(|slot| slot.state.lock().has_ever_attached)
    }

    /// Consume a durable user-owned subscribe/seek acknowledgement.
    ///
    /// Completion remains in per-consumer state until the runtime observes
    /// the stable waiter token. A reset before observation transfers the same
    /// token to the rebuilt wire request instead of losing the wakeup with the
    /// semantic event queue.
    pub fn consume_consumer_subscribe_waiter_completion(
        &mut self,
        handle: ConsumerHandle,
        waiter_id: RequestId,
    ) -> bool {
        if self.state != HandshakeState::Connected {
            return false;
        }
        let Some(slot) = self.consumers.get(&handle) else {
            return false;
        };
        let mut consumer = slot.state.lock();
        if consumer.subscribe_waiter_id == Some(waiter_id) && consumer.subscribe_waiter_completed {
            consumer.subscribe_waiter_id = None;
            consumer.subscribe_waiter_completed = false;
            return true;
        }
        false
    }

    /// Consume the current completed subscribe waiter without a caller-known
    /// token. Used only by initial subscribe setup, whose handle cannot have a
    /// prior user-owned waiter.
    pub fn consume_initial_consumer_subscribe_completion(
        &mut self,
        handle: ConsumerHandle,
    ) -> bool {
        if self.state != HandshakeState::Connected {
            return false;
        }
        let Some(slot) = self.consumers.get(&handle) else {
            return false;
        };
        let mut consumer = slot.state.lock();
        if consumer.subscribe_waiter_id.is_some() && consumer.subscribe_waiter_completed {
            consumer.subscribe_waiter_id = None;
            consumer.subscribe_waiter_completed = false;
            return true;
        }
        false
    }

    /// Abandon a user-owned seek subscribe waiter without abandoning the
    /// established consumer. The active wire request becomes flow-owned so a
    /// later success still resumes dispatch; if success already landed, flow
    /// is staged immediately and the now-unowned semantic event is removed.
    ///
    /// `now` is forwarded to [`Self::initial_flow`] on the immediate-release
    /// path so the receiver-queue auto-adjust schedule is armed there too
    /// (ADR-0011 injected clock).
    pub fn abandon_consumer_subscribe_waiter(
        &mut self,
        handle: ConsumerHandle,
        waiter_id: RequestId,
        now: Instant,
    ) -> bool {
        let (active_request, release_flow_now) = {
            let Some(slot) = self.consumers.get(&handle) else {
                return false;
            };
            let mut consumer = slot.state.lock();
            if consumer.subscribe_waiter_id != Some(waiter_id) {
                return false;
            }
            let active_request = consumer.subscribe_waiter_request.take();
            let release_flow_now =
                consumer.subscribe_waiter_completed && self.state == HandshakeState::Connected;
            consumer.subscribe_waiter_id = None;
            consumer.subscribe_waiter_completed = false;
            if let Some(request_id) = active_request {
                consumer.flow_on_subscribe_ack = true;
                consumer.flow_on_subscribe_ack_request = Some(request_id);
            }
            (active_request, release_flow_now)
        };
        self.events.retain(|event| {
            !matches!(
                event,
                ConnectionEvent::SubscribeAcked { handle: event_handle }
                    if *event_handle == handle
            )
        });
        if release_flow_now {
            let _ = self.initial_flow(handle, now);
        }
        active_request.is_some() || release_flow_now
    }

    /// Last retryable broker rejection observed for an opening producer.
    #[must_use]
    pub fn producer_last_open_error(&self, handle: ProducerHandle) -> Option<(i32, String)> {
        self.producers
            .get(&handle)
            .and_then(|slot| slot.state.lock().last_open_error.clone())
    }

    /// Last retryable broker rejection observed for a subscribing consumer.
    #[must_use]
    pub fn consumer_last_subscribe_error(&self, handle: ConsumerHandle) -> Option<(i32, String)> {
        self.consumers
            .get(&handle)
            .and_then(|slot| slot.state.lock().last_subscribe_error.clone())
    }

    /// Cancel a producer that has not completed its opening operation.
    ///
    /// Idempotent and local-only: the broker has not acknowledged the handle,
    /// so no `CommandCloseProducer` is required. Any detached retry leg sees
    /// the missing request/slot and exits without re-issuing.
    pub fn cancel_producer_open(&mut self, handle: ProducerHandle) {
        let request_ids: Vec<RequestId> = self
            .pending_requests
            .iter()
            .filter_map(|(request_id, kind)| {
                matches!(kind, PendingRequestKind::ProducerOpen { handle: h } if *h == handle)
                    .then_some(*request_id)
            })
            .collect();
        for request_id in request_ids {
            self.cancel_request(request_id);
        }
        self.producers.remove(&handle);
        self.producer_create_requests.remove(&handle);
        self.in_flight_publish_snapshots.remove(&handle);
        self.events.retain(|event| {
            !matches!(
                event,
                ConnectionEvent::ProducerReady { handle: event_handle, .. }
                    | ConnectionEvent::ProducerClosedByBroker { handle: event_handle, .. }
                    | ConnectionEvent::ProducerOpenFailed { handle: event_handle, .. }
                    | ConnectionEvent::ProducerOpenFailedTransient {
                        handle: event_handle,
                        ..
                    }
                    if *event_handle == handle
            )
        });
        self.driver_retries.retain(|retry| {
            !matches!(retry, crate::DriverRetry::Producer { handle: event_handle, .. } if *event_handle == handle)
        });
    }

    /// Cancel a consumer that has not completed its subscribe operation.
    ///
    /// Idempotent and local-only; detached retry legs stop once the slot and
    /// replay request disappear.
    pub fn cancel_consumer_subscribe(&mut self, handle: ConsumerHandle) {
        let request_ids: Vec<RequestId> = self
            .pending_requests
            .iter()
            .filter_map(|(request_id, kind)| {
                matches!(kind, PendingRequestKind::ConsumerSubscribe { handle: h, .. } if *h == handle)
                    .then_some(*request_id)
            })
            .collect();
        for request_id in request_ids {
            self.cancel_request(request_id);
        }
        self.consumers.remove(&handle);
        self.consumer_subscribe_requests.remove(&handle);
        self.events.retain(|event| {
            !matches!(
                event,
                ConnectionEvent::SubscribeAcked { handle: event_handle, .. }
                    | ConnectionEvent::ConsumerClosedByBroker { handle: event_handle, .. }
                    | ConnectionEvent::SubscribeFailed { handle: event_handle, .. }
                    | ConnectionEvent::SubscribeFailedTransient {
                        handle: event_handle,
                        ..
                    }
                    if *event_handle == handle
            )
        });
        self.driver_retries.retain(|retry| {
            !matches!(retry, crate::DriverRetry::Consumer { handle: event_handle, .. } if *event_handle == handle)
        });
    }

    /// Reason the last handshake attempt failed, if the broker sent a
    /// `CommandError` while in `ConnectSent` / `AuthChallenging` state.
    /// Engines surface this in the user-facing connect error so
    /// operators see broker-side reasons (auth rejection, permission
    /// denied, namespace-not-found, etc.) instead of an opaque
    /// "handshake failed" string. `None` if the handshake never started,
    /// is in progress, or failed for a non-protocol reason (raw transport
    /// drop, TLS error).
    #[must_use]
    pub fn handshake_failure_reason(&self) -> Option<&str> {
        self.handshake_failure_reason.as_deref()
    }

    /// Re-emit a `CommandProducer` for every still-open producer that was created before the
    /// most recent [`Self::reset`], then re-issue every in-flight publish snapshotted by that
    /// reset onto the new session. The supervisor calls this after the new socket's handshake
    /// completes so user-facing producer handles transparently survive the reconnect — once each
    /// returned [`RequestId`] surfaces an [`OpOutcome::Success`], the producer is "live" again
    /// and queued sends can flow on the new transport.
    ///
    /// Each replay increments the producer's [`crate::producer::ProducerState::epoch`] field so
    /// the broker can detect — and accept — the re-attach (rejecting stale reconnects of older
    /// epochs). Mirrors Java `ProducerImpl#reconnectLater`.
    ///
    /// Snapshotted publishes (see `in_flight_publish_snapshots`) are NOT replayed here —
    /// they stay in the map until the broker acks each producer's re-attachment with
    /// `CommandProducerSuccess`, whose handler replays them onto `producer.outbound` in
    /// their original FIFO order with their original sequence ids (each replayed
    /// [`crate::producer::OpSend`] goes back into the producer's `pending` queue verbatim —
    /// its `waker` field is `None`, cleared by [`Self::reset`], so the user-facing send
    /// future re-registers on its next poll and the eventual `CommandSendReceipt` resolves
    /// the future normally). Replaying before the ack made the broker close the whole
    /// connection ("Received message, but the producer is not ready") in an endless
    /// reconnect cycle. Mirrors Java `ProducerImpl#handleProducerSuccess` →
    /// `resendMessages`.
    ///
    /// Producers explicitly closed via [`Self::close_producer`] (or by the broker via
    /// `CommandCloseProducer`) are skipped — their `closed` flag is honoured. Any snapshot
    /// for a now-closed producer is discarded along with the rest of its state.
    pub fn rebuild_producers(&mut self) -> Vec<RequestId> {
        // ADR-0049 negative-space assertion (the canonical one called
        // out in `docs/simulation-patterns.md` §3 takeaway 2): a
        // non-empty `in_flight_publish_snapshots` map is only legal
        // when at least one `reset()` has fired — i.e.
        // `session_epoch > 0`. The reverse direction (snapshots
        // accumulating on a fresh, never-reset connection) would have
        // caught the `0e47e14` regression in which a second `reset()`
        // wiped the first reset's snapshots and silently dropped a
        // user-queued send. The map being empty is always legal
        // (some `reset()`s happen with nothing pending).
        debug_assert!(
            self.in_flight_publish_snapshots.is_empty() || self.session_epoch > 0,
            "rebuild_producers entered with non-empty snapshot map and zero session_epoch"
        );
        // ADR-0049 positive assertion: every snapshot key must
        // reference a producer this connection has open. A snapshot
        // without a matching producer slot would be a memory leak
        // (the snapshot never drains; nobody owns the resend).
        debug_assert!(
            self.in_flight_publish_snapshots
                .keys()
                .all(|h| self.producers.contains_key(h)),
            "rebuild_producers entered with snapshot keys not in producers map"
        );
        // Snapshot the (handle, request) pairs we want to replay so the borrow of
        // `producer_create_requests` doesn't conflict with `emit_command_producer`'s mutable
        // borrow of `self`.
        let pending: Vec<(ProducerHandle, CreateProducerRequest)> = self
            .producer_create_requests
            .iter()
            .filter(|(handle, _)| {
                self.producers
                    .get(*handle)
                    .is_some_and(|slot| !slot.state.lock().closed)
            })
            .map(|(handle, req)| (*handle, req.clone()))
            .collect();
        let live_handles: std::collections::HashSet<ProducerHandle> =
            pending.iter().map(|(h, _)| *h).collect();
        let mut request_ids = Vec::with_capacity(pending.len());
        for (handle, req) in pending {
            if let Some(slot) = self.producers.get(&handle) {
                let mut p = slot.state.lock();
                p.epoch = p.epoch.saturating_add(1);
            }
            let request_id = self.emit_command_producer(handle, &req);
            request_ids.push(request_id);
            // Snapshotted in-flight publishes are deliberately NOT replayed here. The
            // wire-frame data stays in `in_flight_publish_snapshots` until this handle's
            // `CommandProducerSuccess` arrives — the broker attaches asynchronously and
            // closes the whole connection on a `CommandSend` that lands before the attach
            // completes ("Received message, but the producer is not ready"), which turned
            // every reconnect-with-in-flight-sends into an endless cycle. The
            // `ProducerSuccess` handler replays the snapshots and opens the per-slot
            // drain gate (`broker_ready`).
        }
        // Drop any snapshots that belong to producers we did NOT rebuild (e.g. ones closed
        // between reset and rebuild). Their `OpSend`s never reach a future — the user-facing
        // close path is responsible for surfacing the disposition (`Closed` error).
        self.in_flight_publish_snapshots
            .retain(|h, _| live_handles.contains(h));
        request_ids
    }

    /// Number of in-flight publish snapshots stashed for `handle` by the most recent
    /// [`Self::reset`]. Returns `0` when the snapshot has already been drained by
    /// [`Self::rebuild_producers`] or the producer never had any in-flight publish at
    /// reset time. Test-facing observability hook — runtimes do not call this in the
    /// hot path.
    #[must_use]
    pub fn in_flight_publish_snapshot_len(&self, handle: ProducerHandle) -> usize {
        self.in_flight_publish_snapshots
            .get(&handle)
            .map_or(0, Vec::len)
    }

    /// Re-emit a `CommandSubscribe` + initial `CommandFlow` for every still-open consumer that
    /// was created before the most recent [`Self::reset`]. The supervisor calls this after the
    /// new socket's handshake completes so user-facing consumer handles transparently survive
    /// the reconnect — once each returned [`RequestId`] surfaces an [`OpOutcome::Success`], the
    /// consumer's receive queue is "live" again and the broker resumes dispatching messages.
    ///
    /// When a consumer has acknowledged at least one message before the reconnect, the
    /// replayed `CommandSubscribe` uses the highest acked id as `start_message_id` so the
    /// broker resumes from the post-ack position. This avoids double-delivery of pre-reconnect
    /// messages on subscriptions where the cursor was not yet persisted broker-side. Mirrors
    /// Java `ConsumerImpl#connectionOpened`.
    ///
    /// Consumers explicitly closed via [`Self::close_consumer`] / [`Self::unsubscribe`] (or by
    /// the broker via `CommandCloseConsumer`) are skipped — their `closed` flag is honoured.
    pub fn rebuild_consumers(&mut self) -> Vec<RequestId> {
        let pending: Vec<(ConsumerHandle, SubscribeRequest, Option<MessageId>)> = self
            .consumer_subscribe_requests
            .iter()
            .filter_map(|(handle, req)| {
                let slot = self.consumers.get(handle)?;
                let state = slot.state.lock();
                if state.closed || state.unsubscribe_request_id.is_some() {
                    return None;
                }
                Some((*handle, req.clone(), state.last_acked_message_id))
            })
            .collect();
        let mut request_ids = Vec::with_capacity(pending.len());
        for (handle, req, resume_from) in pending {
            // Resume position: prefer the post-ack id when known, else fall back to the
            // original `start_message_id` from the subscribe request (broker uses its
            // persisted cursor if both are absent).
            let resume = resume_from.or(req.start_message_id);
            let subscribe_request_id =
                self.emit_command_subscribe(handle, &req, resume, SubscribeAckAction::ReleaseFlow);
            request_ids.push(subscribe_request_id);
        }
        request_ids
    }

    /// Re-subscribe a single consumer after a successful seek. The Pulsar broker
    /// **disconnects the consumer** as part of `CommandSeek` processing (it has to
    /// quiesce the subscription before resetting the cursor) but does NOT send a
    /// `CommandCloseConsumer` on the wire — the client is expected to know that
    /// `seek` implies "consumer needs to be re-established". Without this step the
    /// broker's internal consumer-id map no longer has this handle and subsequent
    /// `CommandFlow`/dispatch silently no-op.
    ///
    /// Returns the new `CommandSubscribe` request id (so the caller can wait on a
    /// `SubscribeAcked` event for it), or `None` if the handle is unknown or its
    /// consumer is closed. An initial FLOW is queued alongside; the broker
    /// processes commands in order so dispatch resumes as soon as the new
    /// subscribe is acked.
    /// Re-subscribe a single consumer after a successful seek.
    ///
    /// Re-emit `CommandSubscribe` for a consumer after a successful seek,
    /// in the case where the broker tore down the subscription as part of
    /// resetting the cursor and did so via a wire-level
    /// `CommandCloseConsumer` (some Pulsar broker versions disconnect the
    /// consumer to quiesce the dispatcher before persisting the new
    /// cursor position).
    ///
    /// Mirrors Java's `ConsumerImpl.connectionOpened` flow that
    /// `seekAsync` triggers indirectly through the connection-level
    /// supervisor — magnetar runs it inline because there is no
    /// connection-level reconnect happening (the TCP socket is fine).
    ///
    /// Returns the new request id (so callers can wait on a
    /// `SubscribeAcked` event for it), or `None` if the handle is
    /// unknown. Drops any stale `ConsumerClosedByBroker(handle)` events
    /// from the queue first — those were emitted when the broker tore
    /// the subscription down and would otherwise trip the runtime's
    /// `wait_subscribe_acked` future before it sees the fresh
    /// `SubscribeAcked`.
    ///
    /// Critically, does **NOT** clear `consumer.queue`: the broker may
    /// have already dispatched messages from the just-reset cursor
    /// position into the TCP buffer by the time this runs. Those
    /// messages are post-seek and the user wants them. `begin_seek`
    /// already cleared pre-seek messages at seek-issue time.
    pub fn resubscribe_consumer_after_seek(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
        let req = self.consumer_subscribe_requests.get(&handle)?.clone();
        // `consumer.closed` is no longer flipped by `handle_close_consumer`
        // (see the comment block in `CloseConsumer` branch above), so we
        // don't need to reset it here. Drain stale close and acknowledgement
        // events before replacing the logical waiter token.
        if self
            .consumers
            .get(&handle)?
            .state
            .lock()
            .unsubscribe_request_id
            .is_some()
        {
            return None;
        }
        self.events.retain(|ev| {
            !matches!(
                ev,
                ConnectionEvent::ConsumerClosedByBroker { handle: h, .. }
                    | ConnectionEvent::SubscribeAcked { handle: h, .. }
                    if *h == handle
            )
        });
        // `None` here = use the broker's persisted cursor (just reset by the seek).
        //
        // NOTE: we ONLY emit `CommandSubscribe` here. The runtime layer is
        // responsible for awaiting `SubscribeAcked` and THEN issuing
        // `CommandFlow` + `CommandRedeliverUnacknowledgedMessages`. Pulsar's
        // broker drops `CommandFlow` for a consumer that doesn't exist yet —
        // `ServerCnx.handleFlow` logs "Couldn't find consumer to handle flow"
        // and returns silently. Sending Flow inline (before the broker's
        // SubscribeSuccess) loses the permits: the broker creates the
        // consumer with `available_permits = 0` and never dispatches the
        // post-seek backlog. This was #67's root cause — the broker
        // confirmed `backlog 10` after the cursor reset, but no message ever
        // arrived because the permits were dropped on the floor.
        let request_id =
            self.emit_command_subscribe(handle, &req, None, SubscribeAckAction::NotifyWaiter);
        Some(request_id)
    }

    /// Returns the feature flags negotiated with the broker (empty until `Connected`).
    pub fn feature_flags(&self) -> &pb::FeatureFlags {
        &self.feature_flags
    }

    /// Begin the handshake. Enqueues a `CommandConnect` for the driver to send.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Handshake`] if the connection is not in
    /// [`HandshakeState::Uninitialized`].
    pub fn begin_handshake(&mut self) -> Result<(), ProtocolError> {
        if self.state != HandshakeState::Uninitialized {
            return Err(ProtocolError::Handshake("handshake already started"));
        }
        // PIP-460 (ADR-0093) v4 compatibility. Advertising the capability is
        // what lets the broker tell a scalable-topics-aware client from a v4
        // one; the broker answers in kind on `CommandConnected`, and
        // `broker_supports_scalable_topics` gates every scalable command on
        // that answer. A client compiled without the feature never advertises,
        // and a v4 broker never answers, so both directions degrade to the
        // pre-PIP-460 wire exactly as before.
        #[cfg_attr(
            not(feature = "scalable-topics"),
            expect(
                unused_mut,
                reason = "the scalable-topics capability is the only mutation"
            )
        )]
        let mut connect_feature_flags = self.config.feature_flags;
        #[cfg(feature = "scalable-topics")]
        {
            connect_feature_flags.supports_scalable_topics = Some(true);
        }
        let connect = pb::CommandConnect {
            client_version: self.config.client_version.clone(),
            auth_method: None,
            auth_method_name: Some(self.config.auth_method_name.clone()),
            auth_data: self.config.auth_data.clone(),
            protocol_version: Some(self.config.protocol_version),
            proxy_to_broker_url: self.config.proxy_to_broker_url.clone(),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            feature_flags: Some(connect_feature_flags),
            proxy_version: None,
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(connect),
            ..Default::default()
        };
        self.encode_command(&cmd)?;
        self.set_handshake_state(HandshakeState::ConnectSent);
        Ok(())
    }

    /// Feed inbound bytes to the state machine — **owned-chunk** entry
    /// point (ADR-0040 wave 3 — read-path ownership pass-through).
    ///
    /// When the protocol's internal `inbound` buffer is empty (the
    /// common case: every full frame consumed by the previous call
    /// left an empty queue), this **swaps** the caller's `BytesMut`
    /// directly into `self.inbound` — zero memcpy. Otherwise falls
    /// back to the legacy `extend_from_slice` path (one memcpy of the
    /// new chunk, unavoidable when partial-frame bytes are still
    /// queued).
    ///
    /// Runtimes that read into their own `BytesMut` (the tokio and
    /// moonpool drivers do, via `tokio::io::AsyncReadExt::read_buf`
    /// then `BytesMut::split()`) call this entry to skip the
    /// user-space memcpy the [`Self::handle_bytes`] `&[u8]` entry
    /// must perform. Callers holding a borrowed slice should keep
    /// using [`Self::handle_bytes`] — both share the same framing
    /// and decode loop.
    pub fn handle_bytes_owned(
        &mut self,
        now: Instant,
        chunk: BytesMut,
    ) -> Result<(), ProtocolError> {
        // NB: the keepalive baseline (`last_activity`) is refreshed per *decoded
        // frame* inside `handle_bytes_decode_loop`, NOT here per raw chunk. A
        // desynced socket that keeps dribbling bytes which never satisfy the
        // announced `total_size` must not reset the watchdog (ADR-0058).
        if self.inbound.is_empty() {
            // Common case: the previous call drained a full frame and
            // left `inbound` empty. Replace the empty staging buffer
            // with the caller's chunk — zero memcpy.
            self.inbound = chunk;
        } else {
            // Mid-frame fall-back: the previous call partially decoded;
            // splice the new chunk onto the existing buffer.
            self.inbound.extend_from_slice(&chunk);
        }
        self.handle_bytes_decode_loop(now)
    }

    /// Feed inbound bytes to the state machine.
    pub fn handle_bytes(&mut self, now: Instant, bytes: &[u8]) -> Result<(), ProtocolError> {
        // The keepalive baseline is refreshed per *decoded frame* in
        // `handle_bytes_decode_loop`, not here per raw chunk — see the note on
        // [`Self::handle_bytes_owned`] and ADR-0058.
        self.inbound.extend_from_slice(bytes);
        self.handle_bytes_decode_loop(now)
    }

    /// Shared framing / decode loop — pulled out so
    /// [`Self::handle_bytes`] and [`Self::handle_bytes_owned`] both
    /// dispatch the same per-frame logic without code duplication.
    fn handle_bytes_decode_loop(&mut self, now: Instant) -> Result<(), ProtocolError> {
        // ADR-0048 buggify point: `handle_bytes.short_read` — when the
        // label fires, break out of the per-frame decode loop after
        // processing exactly one frame even if `inbound` still holds
        // complete additional frames. Mirrors a short-read at the
        // socket boundary; the next `handle_bytes` call resumes the
        // drain on the surviving bytes. Tracked as a per-loop bool so
        // the roll happens at most once per `handle_bytes` entry.
        let short_read_armed = self
            .buggify
            .should_fire(crate::buggify::labels::HANDLE_BYTES_SHORT_READ, 0.05);
        let mut frames_processed = 0_usize;
        loop {
            // Peek the front of the inbound buffer to find out whether a
            // complete frame is ready. If not, park and wait for more
            // bytes — `self.inbound` retains everything we've seen so far.
            let frame_len = match crate::frame::peek_full_frame_len(&self.inbound) {
                Ok(None) => return Ok(()),
                Ok(Some(len)) => len,
                Err(err) => return Err(err.into()),
            };
            // Carve the complete frame off the front of `inbound` via an
            // O(1) `split_to` (no copy) and freeze the resulting BytesMut
            // into a refcounted Bytes for `decode_one` to advance through.
            //
            // Earlier shapes of this loop called
            // `Bytes::copy_from_slice(&self.inbound)` on every iteration —
            // a full memcpy of the entire remaining inbound buffer per
            // frame — and then `advance`d `self.inbound` by the consumed
            // count. Now we know the exact frame length up front and
            // never copy.
            let mut frame_bytes = self.inbound.split_to(frame_len).freeze();

            // Forward progress: a complete frame was carved off the wire. Refresh
            // the keepalive watchdog baseline and clear any outstanding ping —
            // the peer is demonstrably still framing. This is the ONLY
            // `last_activity` refresh on the read path (ADR-0058); doing it here
            // rather than per raw chunk means a desynced-but-chatty socket whose
            // bytes never satisfy the announced `total_size` cannot keep the
            // watchdog alive. Covers every decode outcome below (a decoded frame
            // and a CRC-mismatch drop alike): both consumed a real, fully-framed
            // unit off the stream.
            self.last_activity = Some(now);
            self.keepalive_ping_outstanding = false;
            match decode_one(&mut frame_bytes) {
                Ok(frame) => {
                    self.handle_frame(now, frame)?;
                }
                Err(crate::frame::FrameError::ChecksumMismatch { computed, expected }) => {
                    // CRC mismatch — drop the corrupt frame, emit the
                    // observation event, and keep decoding.
                    //
                    // ADR-0054 §5 single-owner rule: this is the point of
                    // detection (`computed` / `expected` in scope), so the
                    // `error!` lives here; the engines drain the companion
                    // event silently. `error!` per §1: the drop is never
                    // surfaced as `Err` to any caller.
                    tracing::error!(
                        target: "magnetar_proto::conn",
                        computed,
                        expected,
                        "CRC32C checksum mismatch; corrupt frame dropped",
                    );
                    self.events
                        .push_back(ConnectionEvent::ChecksumMismatch { computed, expected });
                }
                Err(other) => {
                    // Any other error — including internal `Incomplete`
                    // arising from a malformed payload whose declared
                    // `total_size` promised contents it lacks — is
                    // fatal on this connection. We've already split the
                    // declared bytes off `self.inbound`; waiting for more
                    // cannot fix a frame whose own length field lied.
                    return Err(other.into());
                }
            }
            // ADR-0048 buggify point: `handle_bytes.short_read` —
            // after the first processed frame, fire the synthetic
            // short-read by returning to the caller with `inbound`
            // still holding any remaining complete frames. The next
            // `handle_bytes` call resumes the drain on the surviving
            // bytes. Firing exits the loop directly, so we never need
            // to "disarm" the flag — the local goes out of scope.
            frames_processed = frames_processed.saturating_add(1);
            if short_read_armed && frames_processed >= 1 && !self.inbound.is_empty() {
                return Ok(());
            }
        }
    }

    fn handle_frame(&mut self, now: Instant, frame: Frame) -> Result<(), ProtocolError> {
        let Frame { command, payload } = frame;
        let cmd_type = pb::base_command::Type::try_from(command.r#type)
            .map_err(|_| ProtocolError::UnsupportedCommand(command.r#type))?;

        match cmd_type {
            pb::base_command::Type::Connected => {
                let connected = command
                    .connected
                    .ok_or(ProtocolError::Handshake("missing CommandConnected"))?;
                self.set_handshake_state(HandshakeState::Connected);
                self.last_connected_at = Some((self.wall_clock)());
                self.broker_max_message_size = connected.max_message_size.map(|v| v as usize);
                self.broker_protocol_version = connected.protocol_version.unwrap_or(0);
                self.feature_flags = connected.feature_flags.unwrap_or_default();
                self.events.push_back(ConnectionEvent::Connected {
                    protocol_version: self.broker_protocol_version,
                    max_message_size: connected.max_message_size.unwrap_or(0) as u32,
                    feature_flags: self.feature_flags,
                });
            }
            pb::base_command::Type::Ping => {
                // Pong back immediately.
                let pong = pb::BaseCommand {
                    r#type: pb::base_command::Type::Pong as i32,
                    pong: Some(pb::CommandPong {}),
                    ..Default::default()
                };
                self.encode_command(&pong)?;
            }
            pb::base_command::Type::Pong => {
                // Nothing to do — the keepalive baseline was refreshed and any
                // outstanding ping cleared by the per-decoded-frame progress
                // update in `handle_bytes_decode_loop` (ADR-0058). A pong is the
                // direct answer to our ping, but ANY decoded frame proves the
                // peer is alive, so the watchdog reset is not pong-specific.
            }
            pb::base_command::Type::AuthChallenge => {
                let challenge = command
                    .auth_challenge
                    .ok_or(ProtocolError::Handshake("missing CommandAuthChallenge"))?;
                self.set_handshake_state(HandshakeState::AuthChallenging);
                self.events.push_back(ConnectionEvent::AuthChallenge {
                    method: challenge
                        .challenge
                        .as_ref()
                        .and_then(|d| d.auth_method_name.clone()),
                    challenge: challenge.challenge.and_then(|d| d.auth_data),
                });
            }
            pb::base_command::Type::SendReceipt => {
                let receipt = command
                    .send_receipt
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandSendReceipt body",
                    ))?;
                let handle = ProducerHandle(receipt.producer_id);
                tracing::trace!(
                    target: "magnetar_proto::conn",
                    producer_id = receipt.producer_id,
                    sequence_id = receipt.sequence_id,
                    "send receipt received"
                );
                let resolved: Vec<(SequenceId, MessageId, Option<Waker>)> =
                    if let Some(slot) = self.producers.get(&handle) {
                        let mut producer = slot.state.lock();
                        // Batched sends now mint a per-message `OpSend` (`add_to_batch`); a
                        // single broker receipt with `sequence_id = lowest` and
                        // `highest_sequence_id = highest` must fan out across every entry in
                        // `[lowest, highest]`. Collect first (no nested mut-borrow of `self`),
                        // then drain outside the producer borrow.
                        let lowest = receipt.sequence_id;
                        // Pulsar's broker uses the Java `-1L` sentinel for "no batch" on the
                        // wire — `uint64` re-encodes `-1` as `u64::MAX`, so receipts for
                        // single-message sends arrive with `highest_sequence_id == u64::MAX`.
                        // Treat that AND any value strictly below `lowest` as "single
                        // message"; only `highest >= lowest && highest != u64::MAX` is a real
                        // batch range. Java client side: see `CommandSendReceipt` parsing in
                        // `ClientCnx#handleSendReceipt` checking `highestSequenceId >= 0`.
                        let highest_raw = receipt.highest_sequence_id.unwrap_or(0);
                        let highest = if highest_raw >= lowest && highest_raw != u64::MAX {
                            highest_raw
                        } else {
                            lowest
                        };
                        let mut resolved: Vec<(SequenceId, MessageId, Option<Waker>)> = Vec::new();
                        for seq in lowest..=highest {
                            let mut synth = receipt.clone();
                            synth.sequence_id = seq;
                            synth.highest_sequence_id = None;
                            if let Some(tuple) = producer.apply_receipt(&synth, now) {
                                resolved.push(tuple);
                            }
                        }
                        let count = resolved.len() as u64;
                        if count > 0 {
                            producer.total_acks_received =
                                producer.total_acks_received.saturating_add(count);
                        }
                        resolved
                    } else {
                        Vec::new()
                    };
                if !resolved.is_empty() {
                    for (seq, mid, waker) in resolved {
                        let key = PendingOpKey::Send(handle, seq);
                        self.outcomes.insert(
                            key,
                            OpOutcome::SendReceipt {
                                sequence_id: seq,
                                message_id: mid,
                            },
                        );
                        if let Some(w) = waker {
                            w.wake();
                        } else if let Some(w) = self.wakers.remove(&key) {
                            w.wake();
                        }
                        self.events.push_back(ConnectionEvent::SendReceipt {
                            handle,
                            sequence_id: seq,
                            message_id: mid,
                        });
                    }
                }
            }
            pb::base_command::Type::SendError => {
                let err = command.send_error.ok_or(ProtocolError::InvariantViolation(
                    "missing CommandSendError",
                ))?;
                let handle = ProducerHandle(err.producer_id);
                let resolved: Option<(SequenceId, Option<Waker>, i32, String)> = if let Some(slot) =
                    self.producers.get(&handle)
                {
                    let mut producer = slot.state.lock();
                    let outcome = producer.apply_send_error(&err);
                    if outcome.is_some() {
                        producer.total_send_failed = producer.total_send_failed.saturating_add(1);
                    }
                    outcome
                } else {
                    None
                };
                if let Some((seq, waker, code, message)) = resolved {
                    let key = PendingOpKey::Send(handle, seq);
                    self.outcomes.insert(
                        key,
                        OpOutcome::SendError {
                            sequence_id: seq,
                            code,
                            message: message.clone(),
                        },
                    );
                    if let Some(w) = waker {
                        w.wake();
                    } else if let Some(w) = self.wakers.remove(&key) {
                        w.wake();
                    }
                    self.events.push_back(ConnectionEvent::SendError {
                        handle,
                        sequence_id: seq,
                        code,
                        message,
                    });
                }
            }
            pb::base_command::Type::Message => {
                let msg = command
                    .message
                    .ok_or(ProtocolError::InvariantViolation("missing CommandMessage"))?;
                let payload = payload.ok_or(ProtocolError::InvariantViolation(
                    "Message frame missing payload",
                ))?;
                let handle = ConsumerHandle(msg.consumer_id);
                // PIP-33 ([ADR-0034]): if the payload carries a REPLICATED_SUBSCRIPTION_*
                // marker (`MarkerType` 10..=13), filter it off the user-visible event
                // stream and emit an observation event instead. The broker manages the
                // marker's cursor position independently — we bump the consumer's
                // permit counter via `record_marker_consumed` so flow control stays
                // symmetric. Txn markers (20..=22) and any future / unknown kind fall
                // through to the existing `deliver` path (decoder returns `Ok(None)`).
                //
                // [ADR-0034]: ../../specs/adr/0034-pip-33-replicated-subscriptions-scope.md
                if let Some(marker_type) = payload.metadata.marker_type {
                    match crate::markers::decode_replicated_subscription_marker(
                        marker_type,
                        &payload.body,
                    ) {
                        Ok(Some(marker)) => {
                            if let Some(slot) = self.consumers.get(&handle) {
                                slot.state.lock().record_marker_consumed();
                            }
                            self.events.push_back(
                                ConnectionEvent::ReplicatedSubscriptionMarkerObserved {
                                    handle,
                                    marker,
                                },
                            );
                            return Ok(());
                        }
                        Ok(None) => {
                            // Not a replicated-subscription marker — fall through to the
                            // existing deliver path (preserves txn-marker behaviour).
                        }
                        Err(_) => {
                            // Malformed RS marker payload: drop quietly. The broker should
                            // not be emitting truncated markers; logging it would couple
                            // magnetar-proto to a logging facade.
                            if let Some(slot) = self.consumers.get(&handle) {
                                slot.state.lock().record_marker_consumed();
                            }
                            return Ok(());
                        }
                    }
                }
                let (staged_events, flow_permits): (Vec<ConnectionEvent>, Option<u32>) =
                    if let Some(slot) = self.consumers.get(&handle) {
                        let mut consumer = slot.state.lock();
                        let outcome = consumer.deliver(
                            &msg,
                            payload.metadata.clone(),
                            payload.broker_entry_metadata.clone(),
                            payload.body.clone(),
                            now,
                        );
                        let mut events = Vec::new();
                        if let Ok(crate::consumer::DeliverOutcome::Delivered { count }) = outcome {
                            // Emit one observational event per newly delivered payload by
                            // *cloning* the tail of the queue — the runtime drains the actual
                            // payloads via `Connection::pop_message`, so the queue must remain
                            // intact for `ReceiveFut::poll`. The newly delivered messages are the
                            // last `count` entries (`deliver` appends in order).
                            //
                            // PIP-180 / ADR-0033: when the consumer is shadow-attached AND the
                            // inbound entry carries `MessageMetadata.replicated_from`, the
                            // classifier emits `MessageReceivedFromShadow` so callers see the
                            // source-topic context without an out-of-band lookup. Regular
                            // (non-shadow) topics keep emitting `Message` — receive-path
                            // wire byte-identical.
                            let queue_len = consumer.queue.len();
                            let start = queue_len.saturating_sub(count);
                            for idx in start..queue_len {
                                if let Some(im) = consumer.queue.get(idx) {
                                    if let Some((source_topic, source_message_id)) =
                                        consumer.classify_for_shadow(im)
                                    {
                                        let shadow_message_id = im.message_id;
                                        events.push(ConnectionEvent::MessageReceivedFromShadow {
                                            handle,
                                            source_topic,
                                            source_message_id,
                                            shadow_message_id,
                                            message: im.clone(),
                                        });
                                    } else {
                                        events.push(ConnectionEvent::Message {
                                            handle,
                                            message: im.clone(),
                                        });
                                    }
                                }
                            }
                        }
                        let flow_permits = consumer.maybe_flow().map(|flow| flow.message_permits);
                        (events, flow_permits)
                    } else {
                        (Vec::new(), None)
                    };
                for ev in staged_events {
                    self.events.push_back(ev);
                }
                if let Some(permits) = flow_permits {
                    self.flow(handle, permits);
                }
            }
            pb::base_command::Type::ProducerSuccess => {
                let ok = command
                    .producer_success
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandProducerSuccess",
                    ))?;
                let request_id = RequestId(ok.request_id);
                if let Some(PendingRequestKind::ProducerOpen { handle }) =
                    self.pending_requests.remove(&request_id)
                {
                    let current = self
                        .producers
                        .get(&handle)
                        .is_some_and(|slot| slot.state.lock().open_request_id == Some(request_id));
                    if !current {
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            "ignored producer success for superseded same-handle request"
                        );
                        return Ok(());
                    }
                    let snapshots = self.in_flight_publish_snapshots.remove(&handle);
                    if let Some(slot) = self.producers.get(&handle) {
                        let mut producer = slot.state.lock();
                        producer.open_request_id = None;
                        producer.name = Some(ok.producer_name.clone());
                        producer.last_sequence_id_published = ok.last_sequence_id.unwrap_or(-1);
                        // Java `ProducerImpl#handleProducerSuccess` →
                        // `resendMessages` parity (producer-not-ready livelock
                        // fix): the broker has acked the (re-)attachment — only
                        // NOW may queued sends flow. Re-emit pending frames the
                        // broker silently dropped during a transient window,
                        // reinstall reset-time snapshots at the front (they
                        // predate anything staged during the rebuild window),
                        // then open the drain gate.
                        let pending_before = producer.pending.len();
                        let snapshot_count = snapshots.as_ref().map_or(0, Vec::len);
                        producer.replay_pending_outbound();
                        if let Some(snapshots) = snapshots {
                            producer.replay_snapshots(snapshots);
                        }
                        producer.broker_ready = true;
                        producer.has_ever_attached = true;
                        // The re-attach succeeded — clear the transient-retry
                        // budget so a future bundle reshuffle starts its backoff
                        // schedule fresh (issue #302).
                        producer.transient_open_attempts = 0;
                        producer.last_open_error = None;
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            pending = pending_before,
                            replayed_snapshots = snapshot_count,
                            "producer re-attach acked; replay staged and drain gate opened"
                        );
                    }
                    self.events.push_back(ConnectionEvent::ProducerReady {
                        handle,
                        producer_name: ok.producer_name,
                        last_sequence_id: ok.last_sequence_id.unwrap_or(-1),
                        schema_version: ok.schema_version.unwrap_or_default(),
                    });
                    // ADR-0028 anti-thrash: feed the successful re-attach into the
                    // detector. No-op when the detector is disabled (default).
                    self.record_reattach_outcome(
                        now,
                        crate::anti_thrash::ReAttachHandle::Producer(handle),
                        crate::anti_thrash::ReAttachOutcomeKind::ReAttachOk,
                    );
                }
            }
            pb::base_command::Type::Success => {
                let ok = command
                    .success
                    .ok_or(ProtocolError::InvariantViolation("missing CommandSuccess"))?;
                let request_id = RequestId(ok.request_id);
                let kind = self.pending_requests.remove(&request_id);
                let unsubscribe_has_waiter =
                    matches!(kind, Some(PendingRequestKind::ConsumerUnsubscribe { .. }))
                        && self.wakers.contains_key(&PendingOpKey::Request(request_id));
                if let Some(PendingRequestKind::ConsumerUnsubscribe { handle }) = kind {
                    self.complete_consumer_unsubscribe(handle, request_id);
                }
                match kind {
                    Some(PendingRequestKind::ProducerCloseForgotten { handle }) => {
                        // Fire-and-forget drop-close: no waiter will ever drain
                        // this outcome — recording it would leak one permanent
                        // `outcomes` entry per dropped producer (issue #241's
                        // continuous-eviction workload). Consume the ack here.
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            "fire-and-forget producer close acked by broker"
                        );
                    }
                    Some(PendingRequestKind::ConsumerCloseForgotten { handle }) => {
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            "fire-and-forget consumer close acked by broker"
                        );
                    }
                    Some(PendingRequestKind::ConsumerSubscribe { .. }) => {}
                    Some(PendingRequestKind::ConsumerUnsubscribe { .. })
                        if !unsubscribe_has_waiter =>
                    {
                        // The user cancelled the unsubscribe future, but the
                        // broker-side operation still completed. Lifecycle is
                        // finalized above; without a waiter, retaining an
                        // outcome would leak it permanently.
                    }
                    Some(_) => {
                        self.outcomes.insert(
                            PendingOpKey::Request(request_id),
                            OpOutcome::Success { request_id },
                        );
                        self.wake_for_request(request_id);
                    }
                    None => {}
                }
                if let Some(PendingRequestKind::ConsumerSubscribe { handle }) = kind {
                    let (waiter_id, flow_now) = self
                        .consumers
                        .get(&handle)
                        .map(|slot| {
                            let mut consumer = slot.state.lock();
                            let waiter_id = if consumer.subscribe_waiter_request == Some(request_id)
                            {
                                consumer.subscribe_waiter_completed = true;
                                consumer.subscribe_waiter_id
                            } else {
                                None
                            };
                            let flow_now = consumer.flow_on_subscribe_ack
                                && consumer
                                    .flow_on_subscribe_ack_request
                                    .is_none_or(|active| active == request_id);
                            if waiter_id.is_some() {
                                consumer.subscribe_waiter_request = None;
                            }
                            if flow_now {
                                consumer.flow_on_subscribe_ack = false;
                                consumer.flow_on_subscribe_ack_request = None;
                            }
                            if waiter_id.is_some() || flow_now {
                                consumer.has_ever_attached = true;
                                consumer.transient_subscribe_attempts = 0;
                                consumer.last_subscribe_error = None;
                            }
                            (waiter_id, flow_now)
                        })
                        .unwrap_or_default();
                    if waiter_id.is_some() {
                        self.events
                            .push_back(ConnectionEvent::SubscribeAcked { handle });
                    }
                    if waiter_id.is_some() || flow_now {
                        // ADR-0028 anti-thrash: feed only the current successful
                        // attachment into the detector. Late replies from a
                        // superseded same-handle request own neither gate.
                        self.record_reattach_outcome(
                            now,
                            crate::anti_thrash::ReAttachHandle::Consumer(handle),
                            crate::anti_thrash::ReAttachOutcomeKind::ReAttachOk,
                        );
                    }
                    if flow_now {
                        let _ = self.initial_flow(handle, now);
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            "consumer re-attach acked; initial flow re-issued"
                        );
                    }
                }
                if let Some(PendingRequestKind::ConsumerSeek { handle }) = kind {
                    if let Some(slot) = self.consumers.get(&handle) {
                        let _ = slot.state.lock().seek_acked();
                    }
                }
            }
            pb::base_command::Type::Error => {
                let err = command
                    .error
                    .ok_or(ProtocolError::InvariantViolation("missing CommandError"))?;
                // Mid-handshake `CommandError` (proxy auth rejection, namespace not
                // found via proxy_to_broker_url, etc.) carries the broker's
                // explanation but does NOT correlate with a `request_id` the
                // outcomes map will route. Capture it so the engine's
                // handshake future surfaces a useful error instead of opaque
                // "handshake failed" once the peer drops the socket. Mirrors
                // Java `ClientCnx#handleError` which logs the server error
                // + message and tears the connection down.
                if matches!(
                    self.state,
                    HandshakeState::ConnectSent | HandshakeState::AuthChallenging
                ) {
                    // Resolve the i32 ServerError into the human-readable
                    // variant name when possible — the integer code by
                    // itself is opaque to operators reading the log.
                    let server_error_name = pb::ServerError::try_from(err.error)
                        .map(|v| format!("{v:?}"))
                        .unwrap_or_else(|_| format!("Unknown({})", err.error));
                    // The broker `message` is hostile-peer-controlled input
                    // (ADR-0054 §3 broker-string sanitisation). Bound it ONCE
                    // here at the capture site — before it is owned into
                    // `handshake_failure_reason` — so every downstream sink
                    // (tokio `ClientError`, moonpool `EngineError::HandshakeFailed`)
                    // inherits the bound. This is the broker-text sink that
                    // escaped ADR-0054 §3; ADR-0062 closes it.
                    let bounded_message = crate::log_fields::truncate_broker_str(&err.message);
                    let reason = format!(
                        "broker rejected handshake (server_error={server_error_name}): {bounded_message}"
                    );
                    tracing::warn!(
                        target: "magnetar_proto::conn",
                        state = ?self.state,
                        server_error = %server_error_name,
                        message = %bounded_message,
                        "captured CommandError during handshake — surfacing as handshake_failure_reason",
                    );
                    self.handshake_failure_reason = Some(reason);
                }
                let request_id = RequestId(err.request_id);
                let kind = self.pending_requests.remove(&request_id);
                let unsubscribe_has_waiter =
                    matches!(kind, Some(PendingRequestKind::ConsumerUnsubscribe { .. }))
                        && self.wakers.contains_key(&PendingOpKey::Request(request_id));
                if let Some(PendingRequestKind::ConsumerUnsubscribe { handle }) = kind {
                    let _ = self.resume_consumer_after_unsubscribe_failure(handle, request_id);
                }
                match kind {
                    Some(PendingRequestKind::ProducerCloseForgotten { handle }) => {
                        let bounded_message = crate::log_fields::truncate_broker_str(&err.message);
                        // Fire-and-forget drop-close: no waiter exists, so an
                        // `OpOutcome::Error` would leak (see the `Success` arm).
                        // Surface the rejection to operators instead — a broker
                        // rejecting a close storm (overload, fencing) must stay
                        // diagnosable (issue #241).
                        tracing::warn!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            code = err.error,
                            message = %bounded_message,
                            "broker rejected fire-and-forget producer close (producer dropped without explicit close)"
                        );
                    }
                    Some(PendingRequestKind::ConsumerCloseForgotten { handle }) => {
                        let bounded_message = crate::log_fields::truncate_broker_str(&err.message);
                        tracing::warn!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            request_id = ?request_id,
                            code = err.error,
                            message = %bounded_message,
                            "broker rejected fire-and-forget consumer close (consumer dropped without explicit close)"
                        );
                    }
                    Some(
                        PendingRequestKind::ProducerOpen { .. }
                        | PendingRequestKind::ConsumerSubscribe { .. },
                    ) => {}
                    Some(PendingRequestKind::ConsumerUnsubscribe { .. })
                        if !unsubscribe_has_waiter =>
                    {
                        // The retry generation was restored above. A cancelled
                        // waiter cannot drain an error outcome, so consume it
                        // in-place after applying the protocol-owned lifecycle.
                    }
                    Some(_) => {
                        self.outcomes.insert(
                            PendingOpKey::Request(request_id),
                            OpOutcome::Error {
                                request_id,
                                code: err.error,
                                message: err.message.clone(),
                            },
                        );
                        self.wake_for_request(request_id);
                    }
                    None => {}
                }
                // When a request id correlates with producer-open or subscribe, surface a
                // typed event so the runtime waiter cannot hang.
                //
                // Provisional handles have never attached: every rejection removes their
                // state and emits `ProducerOpenFailed` / `SubscribeFailed`, allowing the
                // client-owned retry loop to re-run lookup and routing with a fresh handle.
                //
                // Established handles retain state only for an ADR-0080 retryable rejection
                // and emit the corresponding `*Transient` event for driver-owned
                // reattachment. Terminal errors still remove the state and emit the ordinary
                // failure event. Producer-open additionally retries both quota variants and
                // `ProducerBusy`; subscribe additionally retries `ConsumerBusy`.
                match kind {
                    Some(PendingRequestKind::ProducerOpen { handle }) => {
                        let current = self.producers.get(&handle).is_some_and(|slot| {
                            slot.state.lock().open_request_id == Some(request_id)
                        });
                        if !current {
                            tracing::debug!(
                                target: "magnetar_proto::conn",
                                handle = ?handle,
                                request_id = ?request_id,
                                code = err.error,
                                "ignored producer-open error for superseded same-handle request"
                            );
                            return Ok(());
                        }
                        // ADR-0080 classifies broker failures per operation.
                        // The common retryable set covers metadata/persistence
                        // loading, service readiness, and rate limiting;
                        // producer-open additionally accepts both quota variants and
                        // ProducerBusy. TopicNotFound, auth/schema, fencing,
                        // termination, and unknown codes remain terminal.
                        // Without this classification, magnetar removed the producer
                        // state on every transient post-`docker restart` rebuild and
                        // left every subsequent `producer.send()` hanging on a
                        // "unknown producer handle".
                        let retryable = crate::is_retryable_broker_error(
                            crate::OperationKind::ProducerOpen,
                            err.error,
                        );
                        let established = self
                            .producers
                            .get(&handle)
                            .is_some_and(|slot| slot.state.lock().has_ever_attached);
                        if retryable && established {
                            // The attachment failed — close the drain gate so no
                            // staged send reaches the wire before the retry's
                            // `ProducerSuccess` (the broker closes the whole
                            // connection on a send to a not-ready producer).
                            //
                            // Bump the per-handle failure counter under the same
                            // slot lock. Once the configured policy rejects
                            // another re-issue (issue #302, ADR-0080), stop
                            // emitting the recoverable `*Transient` event —
                            // which would re-arm the driver's lookup+retry leg
                            // forever — and install a TERMINAL failure so the
                            // parked `send()` future surfaces `Err(PeerClosed)`
                            // instead of hanging.
                            let attempts = {
                                let mut slot_state =
                                    self.producers.get(&handle).map(|slot| slot.state.lock());
                                match slot_state.as_mut() {
                                    Some(s) => {
                                        s.broker_ready = false;
                                        s.last_open_error = Some((err.error, err.message.clone()));
                                        s.transient_open_attempts =
                                            s.transient_open_attempts.saturating_add(1);
                                        s.transient_open_attempts
                                    }
                                    // Producer already gone (closed between the
                                    // broker error and here) — nothing to retry.
                                    None => u32::MAX,
                                }
                            };
                            if self.operation_retry.should_retry_after_failure(attempts) {
                                self.driver_retries.push_back(crate::DriverRetry::Producer {
                                    handle,
                                    failed_request_id: request_id,
                                    code: err.error,
                                    message: err.message.clone(),
                                });
                                self.events.push_back(
                                    ConnectionEvent::ProducerOpenFailedTransient {
                                        handle,
                                        code: err.error,
                                        message: err.message.clone(),
                                    },
                                );
                            } else {
                                tracing::warn!(
                                    target: "magnetar_proto::conn",
                                    handle = ?handle,
                                    code = err.error,
                                    attempts,
                                    "producer-open transient retry budget exhausted; \
                                     surfacing terminal failure"
                                );
                                self.fail_producer_open_with_broker_error(
                                    handle,
                                    err.error,
                                    &err.message,
                                );
                            }
                        } else if established {
                            self.fail_producer_open_with_broker_error(
                                handle,
                                err.error,
                                &err.message,
                            );
                        } else {
                            self.producers.remove(&handle);
                            self.producer_create_requests.remove(&handle);
                            self.events.push_back(ConnectionEvent::ProducerOpenFailed {
                                handle,
                                code: err.error,
                                message: err.message.clone(),
                            });
                        }
                    }
                    Some(PendingRequestKind::ConsumerSubscribe { handle, .. }) => {
                        let current = self.consumers.get(&handle).is_some_and(|slot| {
                            let consumer = slot.state.lock();
                            consumer.subscribe_waiter_request == Some(request_id)
                                || (consumer.flow_on_subscribe_ack
                                    && consumer
                                        .flow_on_subscribe_ack_request
                                        .is_none_or(|active| active == request_id))
                        });
                        if !current {
                            tracing::debug!(
                                target: "magnetar_proto::conn",
                                handle = ?handle,
                                request_id = ?request_id,
                                code = err.error,
                                "ignored subscribe error for superseded same-handle request"
                            );
                            return Ok(());
                        }
                        let retryable = crate::is_retryable_broker_error(
                            crate::OperationKind::Subscribe,
                            err.error,
                        );
                        let established = self
                            .consumers
                            .get(&handle)
                            .is_some_and(|slot| slot.state.lock().has_ever_attached);
                        if retryable && established {
                            // Bump the per-handle attempt counter; give up
                            // terminally once it crosses the cap (issue #302 —
                            // companion to the producer arm above). A terminal
                            // give-up installs a per-consumer terminal failure +
                            // wakes parked `receive()` wakers so the future
                            // resolves `Err` instead of blocking forever on a
                            // subscription that will never come back.
                            let attempts = match self.consumers.get(&handle) {
                                Some(slot) => {
                                    let mut c = slot.state.lock();
                                    c.last_subscribe_error = Some((err.error, err.message.clone()));
                                    c.transient_subscribe_attempts =
                                        c.transient_subscribe_attempts.saturating_add(1);
                                    c.transient_subscribe_attempts
                                }
                                // Consumer already gone — nothing to retry.
                                None => u32::MAX,
                            };
                            if self.operation_retry.should_retry_after_failure(attempts) {
                                self.events
                                    .push_back(ConnectionEvent::SubscribeFailedTransient {
                                        handle,
                                        code: err.error,
                                        message: err.message.clone(),
                                    });
                                self.driver_retries.push_back(crate::DriverRetry::Consumer {
                                    handle,
                                    failed_request_id: request_id,
                                    code: err.error,
                                    message: err.message.clone(),
                                });
                            } else {
                                tracing::warn!(
                                    target: "magnetar_proto::conn",
                                    handle = ?handle,
                                    code = err.error,
                                    attempts,
                                    "consumer-subscribe transient retry budget exhausted; \
                                     surfacing terminal failure"
                                );
                                self.fail_consumer_subscribe_with_broker_error(
                                    handle,
                                    err.error,
                                    &err.message,
                                );
                            }
                        } else if established {
                            self.fail_consumer_subscribe_with_broker_error(
                                handle,
                                err.error,
                                &err.message,
                            );
                        } else {
                            self.consumers.remove(&handle);
                            self.consumer_subscribe_requests.remove(&handle);
                            self.events.push_back(ConnectionEvent::SubscribeFailed {
                                handle,
                                code: err.error,
                                message: err.message,
                            });
                        }
                    }
                    _ => {}
                }
            }
            pb::base_command::Type::AckResponse => {
                let ack = command
                    .ack_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandAckResponse",
                    ))?;
                let result = if let Some(message) = ack.message.clone() {
                    Err(message)
                } else {
                    Ok(())
                };
                let request_id = ack.request_id.map(RequestId);
                if let Some(rid) = request_id {
                    let kind = self.pending_requests.remove(&rid);
                    if result.is_err() {
                        if let Some(PendingRequestKind::Ack { handle, .. }) = kind {
                            if let Some(slot) = self.consumers.get(&handle) {
                                let mut consumer = slot.state.lock();
                                consumer.total_acks_failed =
                                    consumer.total_acks_failed.saturating_add(1);
                            }
                        }
                    }
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        match &result {
                            Ok(()) => OpOutcome::Success { request_id: rid },
                            Err(msg) => OpOutcome::Error {
                                request_id: rid,
                                code: ack.error.unwrap_or(0),
                                message: msg.clone(),
                            },
                        },
                    );
                    self.wake_for_request(rid);
                }
                self.events
                    .push_back(ConnectionEvent::AckResponse { request_id, result });
            }
            pb::base_command::Type::LookupResponse => {
                let resp =
                    command
                        .lookup_topic_response
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandLookupTopicResponse",
                        ))?;
                let rid = RequestId(resp.request_id);
                if let Some(req) = self.lookup.take_lookup(rid) {
                    // Every lookup response is terminal on THIS connection.
                    // The sans-io core no longer chases a `Redirect` on the
                    // same socket (ADR-0004 — that re-asked a non-owner broker
                    // and looped to the cap on multi-broker clusters). A
                    // `Redirect` now resolves to a driveable
                    // `LookupOutcome::Redirected` that the engine acts on by
                    // dialing the redirect target and re-issuing the lookup
                    // there via `Connection::lookup_redirect`. So each hop is
                    // single-hop-per-connection: `chain_origin == rid` and the
                    // outcome (`Connect` / `Redirected` / `Failed`) is published
                    // straight to the user-facing request-id.
                    let chain_origin = req.chain_origin;
                    let outcome = crate::lookup::translate_lookup_response(&resp, &req);
                    // ADR-0054 §5 single-owner rule: proto owns the redirect
                    // detection log; engines drain the companion event silently.
                    // Broker-advertised URLs are truncated per §3.
                    if let LookupOutcome::Redirected {
                        broker_service_url,
                        broker_service_url_tls,
                        hops_remaining,
                        ..
                    } = &outcome
                    {
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            topic = %req.topic,
                            hops_remaining = *hops_remaining,
                            broker_service_url = broker_service_url
                                .as_deref()
                                .map_or("", crate::log_fields::truncate_broker_str),
                            broker_service_url_tls = broker_service_url_tls
                                .as_deref()
                                .map_or("", crate::log_fields::truncate_broker_str),
                            "lookup redirected; engine will dial the redirect target",
                        );
                    }
                    self.pending_requests.remove(&chain_origin);
                    self.outcomes.insert(
                        PendingOpKey::Request(chain_origin),
                        OpOutcome::LookupResponse {
                            request_id: chain_origin,
                            outcome: outcome.clone(),
                        },
                    );
                    self.wake_for_request(chain_origin);
                    self.events.push_back(ConnectionEvent::LookupResponse {
                        request_id: chain_origin,
                        result: outcome,
                    });
                }
            }
            pb::base_command::Type::PartitionedMetadataResponse => {
                let resp = command.partition_metadata_response.ok_or(
                    ProtocolError::InvariantViolation(
                        "missing CommandPartitionedTopicMetadataResponse",
                    ),
                )?;
                let rid = RequestId(resp.request_id);
                if self.lookup.take_partition(rid) {
                    self.pending_requests.remove(&rid);
                    let error = resp
                        .error
                        .map(|code| (code, resp.message.clone().unwrap_or_default()));
                    let partitions = resp.partitions.unwrap_or(0);
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        OpOutcome::PartitionedMetadata {
                            request_id: rid,
                            partitions,
                            error: error.clone(),
                        },
                    );
                    self.wake_for_request(rid);
                    self.events
                        .push_back(ConnectionEvent::PartitionedMetadataResponse {
                            request_id: rid,
                            partitions,
                            error,
                        });
                }
            }
            pb::base_command::Type::GetLastMessageIdResponse => {
                let resp = command.get_last_message_id_response.ok_or(
                    ProtocolError::InvariantViolation("missing CommandGetLastMessageIdResponse"),
                )?;
                let rid = RequestId(resp.request_id);
                self.pending_requests.remove(&rid);
                let last_message_id = MessageId::from_pb(&resp.last_message_id);
                let consumer_mark_delete_position = resp
                    .consumer_mark_delete_position
                    .as_ref()
                    .map(MessageId::from_pb);
                self.outcomes.insert(
                    PendingOpKey::Request(rid),
                    OpOutcome::LastMessageId {
                        request_id: rid,
                        last_message_id,
                        consumer_mark_delete_position,
                    },
                );
                self.wake_for_request(rid);
            }
            pb::base_command::Type::CloseProducer => {
                let close = command
                    .close_producer
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandCloseProducer",
                    ))?;
                let handle = ProducerHandle(close.producer_id);
                // Broker reasons for `CommandCloseProducer`:
                //   - PIP-188 topic migration (`assigned_broker_service_url` set): producer is
                //     supposed to reconnect on the new URL.
                //   - Broker restart / failover / cluster swap via `ServiceUrlProvider`: TCP drops
                //     next; supervised reconnect re-attaches via `rebuild_producers`.
                //   - Admin-initiated forced delete: a subsequent send will surface a broker-side
                //     rejection (`ProducerFenced`, etc.) which is the right place to surface the
                //     error.
                //
                // All cases are *transient at the protocol level* — the
                // user-facing producer handle keeps being valid. Mirroring
                // Java's `ProducerImpl.connectionClosed`, we surface the
                // event for observability but do NOT permanently mark
                // `closed=true`. Marking it closed would cause
                // `rebuild_producers` to filter it out (`!p.closed` at
                // conn.rs:933), so the supervised reconnect would never
                // re-establish the producer and the next user `send()`
                // would surface `ProducerError::Closed →
                // InvariantViolation("producer rejected send")` even
                // though the broker is willing to re-accept it.
                //
                // Refs: Task #56.
                // The broker detached the producer — close the drain gate so no
                // staged send reaches the wire before the re-attachment's
                // `ProducerSuccess` (send-to-detached-producer closes the whole
                // connection broker-side).
                if let Some(slot) = self.producers.get(&handle) {
                    slot.state.lock().broker_ready = false;
                }
                self.events
                    .push_back(ConnectionEvent::ProducerClosedByBroker {
                        handle,
                        assigned_broker_service_url: close.assigned_broker_service_url,
                    });
            }
            pb::base_command::Type::CloseConsumer => {
                let close = command
                    .close_consumer
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandCloseConsumer",
                    ))?;
                let handle = ConsumerHandle(close.consumer_id);
                // Mirroring #56 (producer-side fix): the broker sends
                // `CommandCloseConsumer` for several transient reasons —
                // PIP-188 topic migration, broker restart, supervised
                // failover, and as part of seek processing (the broker
                // tears the dispatcher's consumer down before resetting
                // the cursor; this fires for **every** seek). All these
                // cases are transient at the protocol level; the
                // supervised reconnect path (`Connection::reset` +
                // `rebuild_consumers`) or the post-seek resubscribe
                // (`resubscribe_consumer_after_seek`) re-attaches the
                // consumer.
                //
                // Flipping `closed=true` here would make any subsequent
                // `consumer.deliver()` call drop the broker's freshly
                // dispatched post-seek messages on the floor — exactly
                // the symptom Java's `duringSeek` flag was added to
                // prevent (apache/pulsar PR #21945). Surface the event
                // for observability but DO NOT mark the consumer
                // closed.
                //
                // Refs: Task #65 (and the equivalent producer fix #56).
                //
                // Issue #307: the broker tearing the dispatcher down resets its
                // side of the flow-control window — the re-created consumer
                // starts at `availablePermits = 0`. Re-sync the client's permit
                // MIRRORS to 0 here so they track broker reality. Without this
                // `granted_permits` stays stale (it is purely additive on the
                // client side; it is never decremented as messages arrive).
                // Issue #349: `permit_balance` (the REAL, decrementing balance)
                // is zeroed in lock-step for the same reason — this is a churn
                // boundary, not a natural drain, so both mirrors reset together.
                // `consumed_since_flow` is reset in lock-step so the
                // `maybe_flow` threshold restarts cleanly once flow is re-armed
                // (mirrors the `reset` re-attach path, which zeroes both). The
                // receiver queue itself is left intact: any already-dispatched
                // messages still in the TCP buffer / queue remain user-visible
                // (the #65 / `duringSeek` invariant).
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    consumer.granted_permits = 0;
                    consumer.permit_balance = 0;
                    consumer.consumed_since_flow = 0;
                }
                // Issue #307 ROOT CAUSE: a broker-initiated `CommandCloseConsumer`
                // with `assigned_broker_service_url = None` is a *same-broker*
                // bundle reassignment on a LIVE socket — no TCP drop, so no
                // `Connection::reset` + `rebuild_consumers` ever fires. The
                // broker has torn its dispatcher's consumer id down; nothing
                // re-subscribes it, so the consumer parks at
                // `available_permits = 0` against a non-empty backlog forever
                // (the production wedge: every bundle reassignment kills another
                // partition). Resetting the permit mirror alone is NOT enough —
                // the broker no longer has this consumer id, so any
                // `CommandFlow` (e.g. an `ActiveConsumerChange` re-arm) is
                // dropped silently ("Couldn't find consumer to handle flow").
                //
                // Re-subscribe the single running consumer in place, on the same
                // connection, exactly like `rebuild_consumers` does per consumer
                // on a reset and `resubscribe_consumer_after_seek` does after a
                // seek: re-emit `CommandSubscribe` (resuming from the last acked
                // id) and defer the initial `CommandFlow` to the broker's
                // re-subscribe `Success` (the re-attach gate at the `Success`
                // arm), so flow lands on a live consumer id rather than being
                // dropped mid-subscribe.
                //
                // ONLY the same-broker (`None`) case is handled here. For
                // `assigned_broker_service_url = Some(url)` (PIP-188 topic
                // migration) the existing supervised-reconnect / migration path
                // is left untouched — that consumer is supposed to reconnect on
                // the new URL, not re-subscribe on this socket.
                if close.assigned_broker_service_url.is_none() {
                    // Issue #346: an ack in flight when the broker tears this
                    // consumer's dispatcher down is orphaned — the old consumer id
                    // is gone, so no `CommandAckResponse` for it will EVER arrive on
                    // this connection; `resubscribe_consumer_after_broker_close`
                    // below attaches a FRESH consumer id via `CommandSubscribe`
                    // (see the #307 ROOT CAUSE comment above). Fail every pending
                    // ack for `handle` fast, right here, so the caller's
                    // `ack().await` resolves immediately instead of parking until
                    // the `ack_response_timeout` backstop (or, if that knob is
                    // disabled, forever).
                    //
                    // MUST run before `resubscribe_consumer_after_broker_close` —
                    // that helper early-returns on `closed` /
                    // `terminal_failure.is_some()` / `pending_seek.is_some()` /
                    // `flow_on_subscribe_ack`, any of which would otherwise skip
                    // this sweep entirely on some re-attach paths.
                    //
                    // Two-phase collect-then-mutate (mirrors the send-timeout sweep
                    // shape in `handle_timeout`) avoids mutating `pending_requests`
                    // while iterating it.
                    let orphaned_acks: Vec<RequestId> = self
                        .pending_requests
                        .iter()
                        .filter_map(|(rid, kind)| match kind {
                            PendingRequestKind::Ack { handle: h, .. } if *h == handle => Some(*rid),
                            _ => None,
                        })
                        .collect();
                    for rid in orphaned_acks {
                        self.pending_requests.remove(&rid);
                        let message = "ack orphaned by broker consumer close".to_owned();
                        self.outcomes.insert(
                            PendingOpKey::Request(rid),
                            OpOutcome::Error {
                                request_id: rid,
                                code: -1,
                                message: message.clone(),
                            },
                        );
                        self.wake_for_request(rid);
                        if let Some(slot) = self.consumers.get(&handle) {
                            let mut consumer = slot.state.lock();
                            consumer.total_acks_failed =
                                consumer.total_acks_failed.saturating_add(1);
                        }
                        self.events.push_back(ConnectionEvent::AckResponse {
                            request_id: Some(rid),
                            result: Err(message),
                        });
                    }
                    // Same-broker bundle reassignment: re-subscribe in place and
                    // DO NOT surface a `ConsumerClosedByBroker` event — the
                    // re-attach is transparent to the runtime (which would
                    // otherwise either drop the event or, if a `SubscribeAcked`
                    // wait is parked, mistake it for a terminal close). The
                    // method drains any older stale close events for this handle.
                    self.resubscribe_consumer_after_broker_close(handle);
                } else {
                    // PIP-188 topic migration (`assigned_broker_service_url`
                    // set): the supervised reconnect / migration path owns the
                    // re-attach on the new URL — surface the event for it.
                    self.events
                        .push_back(ConnectionEvent::ConsumerClosedByBroker {
                            handle,
                            assigned_broker_service_url: close.assigned_broker_service_url,
                        });
                }
            }
            pb::base_command::Type::ReachedEndOfTopic => {
                let rc = command
                    .reached_end_of_topic
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandReachedEndOfTopic",
                    ))?;
                let handle = ConsumerHandle(rc.consumer_id);
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    consumer.reached_end_of_topic = true;
                    // Wake every parked receive so they can observe the
                    // terminal end-of-topic flag instead of waiting forever.
                    let wakers: Vec<std::task::Waker> = consumer.receive_wakers.drain().collect();
                    drop(consumer);
                    for w in wakers {
                        w.wake();
                    }
                }
                self.events
                    .push_back(ConnectionEvent::ReachedEndOfTopic { handle });
            }
            pb::base_command::Type::ActiveConsumerChange => {
                let acc =
                    command
                        .active_consumer_change
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandActiveConsumerChange",
                        ))?;
                let handle = ConsumerHandle(acc.consumer_id);
                let active = acc.is_active.unwrap_or(false);
                self.events
                    .push_back(ConnectionEvent::ActiveConsumerChanged { handle, active });
                // Issue #348: record the transition into the per-slot
                // active-change ring (`is_active` + `active_changes` +
                // waker fan-out) under the SAME per-slot lock acquisition
                // the #307 reflow predicate below reads — one lock section
                // for record + predicate, dropped before `initial_flow`
                // re-acquires it (ADR-0038 lock ordering).
                //
                // Failover re-arm (issue #307): when a standby consumer is
                // promoted to active and is sitting at zero broker-side
                // permits, nothing else re-issues flow — `initial_flow` only
                // runs at subscribe time / re-attach ack, and `maybe_flow`
                // only fires once messages have been consumed (which can never
                // happen at `granted_permits == 0`, since the broker pushes
                // nothing). Such a promoted-but-starved consumer would block
                // `receive()` forever with a non-empty broker backlog. Re-arm
                // flow exactly once on promotion.
                //
                // Guarded so an already-fed consumer is left untouched (no
                // double-flow): only when `granted_permits == 0` — issue #349
                // kept this gate on the additive grant mirror (not
                // `permit_balance`), the same "has the broker been told it may
                // use anything yet" question the want-have delta answers — and
                // only when the consumer is in a dispatch-eligible state — not
                // user-closed, not paused, no in-flight seek freezing the
                // queue, not terminal, and not mid-re-attach (the re-attach
                // gate at the `Success` arm owns that flow). The predicate read
                // takes only the per-slot lock and is dropped before
                // `initial_flow` re-acquires it (ADR-0038 lock ordering).
                let needs_reflow = self.consumers.get(&handle).is_some_and(|slot| {
                    let mut consumer = slot.state.lock();
                    consumer.record_active_change(active);
                    active
                        && consumer.granted_permits == 0
                        && !consumer.closed
                        && !consumer.paused
                        && consumer.pending_seek.is_none()
                        && !consumer.reached_end_of_topic
                        && !consumer.flow_on_subscribe_ack
                });
                if needs_reflow {
                    let _ = self.initial_flow(handle, now);
                    tracing::debug!(
                        target: "magnetar_proto::conn",
                        handle = ?handle,
                        "failover consumer promoted to active; initial flow re-armed"
                    );
                }
            }
            pb::base_command::Type::TopicMigrated => {
                let migrated = command
                    .topic_migrated
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandTopicMigrated",
                    ))?;
                use pb::command_topic_migrated::ResourceType;
                let producer = if migrated.resource_type == ResourceType::Producer as i32 {
                    Some(ProducerHandle(migrated.resource_id))
                } else {
                    None
                };
                let consumer = if migrated.resource_type == ResourceType::Consumer as i32 {
                    Some(ConsumerHandle(migrated.resource_id))
                } else {
                    None
                };
                // Defence-in-depth (medium-1 in the lookup multi-agent
                // review): when the user has configured a
                // `redirect_url_allow_list`, validate every
                // broker-advertised URL **before** letting the runtime
                // act on it. A rejected URL surfaces
                // `RedirectUrlRejected` instead of `TopicMigrated`, so
                // the supervised-reconnect arm in the runtime drivers
                // does not fire and the original
                // `AuthProvider::initial()` credentials are not handed
                // to the unverified host. The mechanism is opt-in
                // (default `None` = permissive) — see
                // `RedirectUrlAllowList` and ADR-0018 §"Redirect URL
                // allow-list (2026-06-01)".
                if let Some(allow_list) = self.config.redirect_url_allow_list.as_ref() {
                    let plain_ok = migrated
                        .broker_service_url
                        .as_deref()
                        .is_some_and(|u| allow_list.is_allowed(u));
                    let tls_ok = migrated
                        .broker_service_url_tls
                        .as_deref()
                        .is_some_and(|u| allow_list.is_allowed(u));
                    if !plain_ok && !tls_ok {
                        self.events.push_back(ConnectionEvent::RedirectUrlRejected {
                            source: "CommandTopicMigrated",
                            broker_service_url: migrated.broker_service_url,
                            broker_service_url_tls: migrated.broker_service_url_tls,
                        });
                        return Ok(());
                    }
                }
                self.events.push_back(ConnectionEvent::TopicMigrated {
                    producer,
                    consumer,
                    broker_service_url: migrated.broker_service_url,
                    broker_service_url_tls: migrated.broker_service_url_tls,
                });
            }
            pb::base_command::Type::WatchTopicListSuccess => {
                let ok =
                    command
                        .watch_topic_list_success
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandWatchTopicListSuccess",
                        ))?;
                let rid = RequestId(ok.request_id);
                if let Some(watcher) = self.topic_watchers.lookup_by_request(rid) {
                    watcher.topics_hash = Some(ok.topics_hash.clone());
                    watcher.initialised = true;
                }
                self.pending_requests.remove(&rid);
                let topics = ok.topic.clone();
                self.outcomes.insert(
                    PendingOpKey::Request(rid),
                    OpOutcome::TopicListSnapshot {
                        request_id: rid,
                        topics: topics.clone(),
                    },
                );
                self.wake_for_request(rid);
                self.events.push_back(ConnectionEvent::TopicListSnapshot {
                    request_id: rid,
                    topics,
                });
            }
            pb::base_command::Type::WatchTopicUpdate => {
                let upd = command
                    .watch_topic_update
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandWatchTopicUpdate",
                    ))?;
                if let Some(watcher) = self.topic_watchers.lookup_by_watcher_id(upd.watcher_id) {
                    watcher.topics_hash = Some(upd.topics_hash.clone());
                }
                self.events.push_back(ConnectionEvent::TopicListChanged {
                    added: upd.new_topics,
                    removed: upd.deleted_topics,
                });
            }
            pb::base_command::Type::NewTxnResponse => {
                let resp = command
                    .new_txn_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandNewTxnResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = match self.txn_client.handle_new_txn_response(resp) {
                    Ok(Some(id)) => Ok(id),
                    Ok(None) => {
                        // Unknown request id — drop the outcome silently. The driver will not
                        // surface a future for a request we never enqueued.
                        return Ok(());
                    }
                    Err(err) => Err(err),
                };
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::NewTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::NewTxn(result),
                });
            }
            pb::base_command::Type::AddPartitionToTxnResponse => {
                let resp = command.add_partition_to_txn_response.ok_or(
                    ProtocolError::InvariantViolation("missing CommandAddPartitionToTxnResponse"),
                )?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_add_partition_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::AddPartitionToTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::AddPartition(result),
                });
            }
            pb::base_command::Type::AddSubscriptionToTxnResponse => {
                let resp = command.add_subscription_to_txn_response.ok_or(
                    ProtocolError::InvariantViolation(
                        "missing CommandAddSubscriptionToTxnResponse",
                    ),
                )?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_add_subscription_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::AddSubscriptionToTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::AddSubscription(result),
                });
            }
            pb::base_command::Type::EndTxnResponse => {
                let resp = command
                    .end_txn_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandEndTxnResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_end_txn_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::EndTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::EndTxn(result),
                });
            }
            pb::base_command::Type::TcClientConnectResponse => {
                let resp =
                    command
                        .tc_client_connect_response
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandTcClientConnectResponse",
                        ))?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                // Broker reports success by omitting `error` (`ServerError::None`); any other
                // code maps to a generic `OpOutcome::Error` so the driver-side future can
                // surface the broker message verbatim.
                let outcome = match resp.error {
                    None | Some(0) => OpOutcome::Success { request_id },
                    Some(code) => OpOutcome::Error {
                        request_id,
                        code,
                        message: resp.message.unwrap_or_default(),
                    },
                };
                self.outcomes
                    .insert(PendingOpKey::Request(request_id), outcome);
                self.wake_for_request(request_id);
            }
            pb::base_command::Type::GetSchemaResponse => {
                let resp = command
                    .get_schema_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandGetSchemaResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                if matches!(
                    self.pending_requests.get(&request_id),
                    Some(PendingRequestKind::GetSchema)
                ) {
                    self.pending_requests.remove(&request_id);
                    let result = match (resp.schema, resp.error_code) {
                        (Some(schema), None) => Ok((schema, resp.schema_version)),
                        (_, Some(code)) => Err((code, resp.error_message.unwrap_or_default())),
                        (None, None) => Err((
                            0,
                            "broker returned empty CommandGetSchemaResponse".to_owned(),
                        )),
                    };
                    self.outcomes.insert(
                        PendingOpKey::Request(request_id),
                        OpOutcome::GetSchemaResponse {
                            request_id,
                            result: result.clone(),
                        },
                    );
                    self.wake_for_request(request_id);
                    self.events
                        .push_back(ConnectionEvent::GetSchemaResponse { request_id, result });
                }
            }
            // PIP-460 (ADR-0093). One inbound command carries both the reply to
            // `CommandScalableTopicLookup` and every later pushed layout.
            #[cfg(feature = "scalable-topics")]
            pb::base_command::Type::ScalableTopicUpdate => {
                let upd =
                    command
                        .scalable_topic_update
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandScalableTopicUpdate",
                        ))?;
                self.handle_scalable_topic_update(upd);
            }
            _ => {
                // Unhandled command — we tolerate them silently for forward compatibility, but
                // we DO push an event for the driver to log.
                tracing::trace!(target: "magnetar_proto", cmd_type = ?cmd_type, "unhandled command type");
            }
        }
        // Drain producer outbound frames opportunistically — we accumulate them into the
        // central byte buffer so the driver can flush them in one syscall.
        self.drain_producer_outbound();
        let _ = now;
        Ok(())
    }

    fn wake_for_request(&mut self, request_id: RequestId) {
        if let Some(w) = self.wakers.remove(&PendingOpKey::Request(request_id)) {
            w.wake();
        }
    }

    /// Drain queued outbound bytes via O(1) ownership transfer.
    ///
    /// Returns the previously-buffered bytes as a refcounted [`Bytes`] —
    /// proto's internal `outbound` is left empty (capacity preserved). An
    /// empty return signals "nothing to send".
    ///
    /// This is the hot path on every driver iteration: `BytesMut::split`
    /// is O(1) (just a refcount bump on the shared buffer header) whereas
    /// the prior `extend_from_slice(&outbound)` signature copied the
    /// entire outbound buffer once per flush.
    pub fn poll_transmit(&mut self) -> Bytes {
        self.drain_producer_outbound();
        let out = self.outbound.split().freeze();
        // Restore a pre-sized scratch buffer so the next encode does not
        // start from zero capacity. `split` leaves `self.outbound` as a
        // view at the tail of the (now-shared) underlying buffer with
        // length 0; subsequent writes would force a realloc on first
        // touch. Replacing with a fresh buffer keeps the next iteration's
        // small writes fast and detaches us cleanly from the buffer the
        // caller now owns.
        self.outbound = BytesMut::with_capacity(4 * 1024);
        out
    }

    /// Drain queued outbound bytes as a [`crate::Transmit`] descriptor
    /// (ADR-0040 waves 1.0 / 1.1).
    ///
    /// **Today** this always returns [`crate::Transmit::Contiguous`]
    /// pointing at the same `BytesMut`-backed slice
    /// [`Self::poll_transmit`] would have produced. The
    /// [`crate::Transmit::Vectored`] variant exists in the type but is
    /// never produced yet — wave 1.2 (proto encoder split) introduces
    /// the segment shape; wave 2 (moonpool
    /// `Providers::Network::write_vectored`) wires the chaos pack.
    ///
    /// Runtimes adopting `poll_write_vectored` / `IoSlice` should match
    /// on the returned [`crate::Transmit`] and extract the byte data
    /// into an owned form before any `.await` (the borrow is tied to
    /// `&mut self` against the connection). For the
    /// [`crate::Transmit::Contiguous`] arm, `Bytes::copy_from_slice`
    /// produces an owned `Bytes` with the same shape that
    /// [`Self::poll_transmit`] returns directly; the vectored arm
    /// (wave 1.2+) hands the runtime an owned segment list it can pass
    /// into the kernel as an `IoSlice` array.
    /// Drain queued outbound bytes as an owned [`crate::TransmitOwned`]
    /// descriptor (ADR-0040 wave 2 — runtime adoption).
    ///
    /// The owned variant is what runtimes use in practice: the
    /// borrowed [`crate::Transmit`] returned by
    /// [`Self::poll_transmit_vectored`] is tied to `&mut Connection`
    /// and cannot cross the runtime's `.await`. The owned variant
    /// drains via the same O(1) ownership transfer
    /// [`Self::poll_transmit`] uses for the contiguous arm, and via
    /// `std::mem::take` for the segment list — no extra memcpy in
    /// either case.
    ///
    /// Dispatch rule mirrors [`Self::poll_transmit_vectored`]:
    ///   1. If the contiguous `outbound` buffer is empty, drain producers vectored; if
    ///      `outbound_segments` is non-empty after the drain, return `Vectored`.
    ///   2. Otherwise drain producers contiguous and return `Contiguous` (legacy path, preserves
    ///      wire order when both buffers carry pending bytes).
    pub fn poll_transmit_owned(&mut self) -> crate::TransmitOwned {
        if self.outbound.is_empty() {
            self.drain_producer_outbound_vectored();
            if !self.outbound_segments.is_empty() {
                return crate::TransmitOwned::Vectored(std::mem::take(&mut self.outbound_segments));
            }
        }
        self.drain_producer_outbound();
        let out = self.outbound.split().freeze();
        self.outbound = BytesMut::with_capacity(4 * 1024);
        crate::TransmitOwned::Contiguous(out)
    }

    pub fn poll_transmit_vectored(&mut self) -> crate::Transmit<'_> {
        // Wave 1.2: prefer the `Vectored` arm when:
        //   1. The producer batch path has segments to emit, AND
        //   2. The contiguous `outbound` buffer is empty.
        //
        // If both buffers carry pending bytes the contiguous path wins
        // — `outbound` may carry handshake / ack / lookup frames whose
        // wire order matters relative to the per-producer frames. The
        // segments stay queued and emerge on the next call. This keeps
        // wire-order semantics identical to the legacy `poll_transmit`
        // (which always drains `outbound` first via
        // `drain_producer_outbound`).
        //
        // The legacy `drain_producer_outbound` is intentionally NOT
        // called here — that path is reserved for `poll_transmit` (the
        // contiguous-coalesce route). Wave 1.2 runtimes that want the
        // segment optimisation call `poll_transmit_vectored`, which
        // drains via `drain_producer_outbound_vectored`. Runtimes that
        // continue to call `poll_transmit` get the legacy behaviour
        // unchanged.
        if self.outbound.is_empty() {
            self.drain_producer_outbound_vectored();
            if !self.outbound_segments.is_empty() {
                self.pending_vectored_segments = std::mem::take(&mut self.outbound_segments);
                return crate::Transmit::Vectored(&self.pending_vectored_segments[..]);
            }
        }
        // Contiguous arm — same drain + ownership-transfer dance as
        // wave 1.1: `drain_producer_outbound` flushes any per-producer
        // frames into `outbound` (using the legacy contiguous encoder),
        // `split().freeze()` hands us the owned `Bytes`, and
        // `pending_vectored_drain` holds it alive across the runtime's
        // `.await`.
        self.drain_producer_outbound();
        let out = self.outbound.split().freeze();
        self.outbound = BytesMut::with_capacity(4 * 1024);
        crate::Transmit::Contiguous(&self.pending_vectored_drain.insert(out)[..])
    }

    /// Pull the next [`ConnectionEvent`], if any.
    pub fn poll_event(&mut self) -> Option<ConnectionEvent> {
        let event = self.events.pop_front()?;
        self.remove_driver_retry_for_event(&event);
        Some(event)
    }

    fn driver_retry_matches_event(retry: &crate::DriverRetry, event: &ConnectionEvent) -> bool {
        match (retry, event) {
            (
                crate::DriverRetry::Producer {
                    handle,
                    code,
                    message,
                    ..
                },
                ConnectionEvent::ProducerOpenFailedTransient {
                    handle: event_handle,
                    code: event_code,
                    message: event_message,
                },
            ) => event_handle == handle && event_code == code && event_message == message,
            (
                crate::DriverRetry::Consumer {
                    handle,
                    code,
                    message,
                    ..
                },
                ConnectionEvent::SubscribeFailedTransient {
                    handle: event_handle,
                    code: event_code,
                    message: event_message,
                },
            ) => event_handle == handle && event_code == code && event_message == message,
            _ => false,
        }
    }

    fn remove_driver_retry_for_event(&mut self, event: &ConnectionEvent) {
        if let Some(index) = self
            .driver_retries
            .iter()
            .position(|retry| Self::driver_retry_matches_event(retry, event))
        {
            let _ = self.driver_retries.remove(index);
        }
    }

    /// Pull the next generation-correlated retry owned by a runtime driver.
    ///
    /// The matching public transient event is removed at the same time. This
    /// preserves the stable [`ConnectionEvent`] shape while the built-in
    /// runtimes retain the exact failed request id needed for ABA-safe retries.
    #[doc(hidden)]
    pub fn poll_driver_retry(&mut self) -> Option<crate::DriverRetry> {
        let retry = self.driver_retries.pop_front()?;
        let matching_event = self
            .events
            .iter()
            .position(|event| Self::driver_retry_matches_event(&retry, event));
        if let Some(index) = matching_event {
            let _ = self.events.remove(index);
        }
        Some(retry)
    }

    /// Pop the first [`ConnectionEvent`] that satisfies `predicate`,
    /// leaving non-matching events at their original positions in the
    /// queue.
    ///
    /// Intended for the runtime driver, which only acts on a small
    /// subset of event variants (`AuthChallenge`, `TopicListChanged`,
    /// `TopicMigrated`) and must *not* swallow events
    /// (`ProducerReady`, `SubscribeAcked`, …) that user-facing
    /// futures are parked on. See the M8 differential broker_smoke
    /// regression: a driver that blindly drained the queue would race
    /// every event-based wait future and stall the producer-open
    /// round-trip.
    pub fn poll_event_if<F>(&mut self, predicate: F) -> Option<ConnectionEvent>
    where
        F: Fn(&ConnectionEvent) -> bool,
    {
        let idx = self.events.iter().position(predicate)?;
        let event = self.events.remove(idx)?;
        self.remove_driver_retry_for_event(&event);
        Some(event)
    }

    /// Time of the next scheduled wake-up — the earliest of the keepalive deadline and any
    /// per-consumer tracker deadline (negative-ack delay + unacked-message timeout).
    ///
    /// All `Instant + Duration` sites route through
    /// [`crate::time::deadline_with_clamp`] so a near-`Duration::MAX`
    /// keepalive interval cannot panic (invariant #6).
    pub fn poll_timeout(&self) -> Option<Instant> {
        let mut next = self
            .last_activity
            .map(|t| crate::time::deadline_with_clamp(t, self.config.keepalive_interval));
        let mut consider = |deadline: Instant| {
            next = Some(match next {
                Some(current) => current.min(deadline),
                None => deadline,
            });
        };
        for slot in self.consumers.values() {
            let consumer = slot.state.lock();
            if let Some(t) = consumer.nack_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
            if let Some(t) = consumer.unacked_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
            if let Some(t) = consumer.ack_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
            // Bounded chunk reassembly: surface the earliest incomplete-chunk
            // expiry deadline so the driver schedules a deterministic wake for
            // [`Self::handle_timeout`]'s sweep. Without this the sweep would
            // only fire opportunistically on an unrelated tick — seed-divergent
            // under the moonpool engine.
            if let Some(d) = consumer.next_chunk_expiry_deadline() {
                consider(d);
            }
            // Issue #301: surface the next receiver-queue auto-adjust deadline so
            // the driver wakes us deterministically for the adjust tick in
            // `handle_timeout`. `None` when the consumer uses the default
            // `Fixed` policy (no auto-adjust), so this is a no-op there.
            if let Some(d) = consumer.next_adjust_deadline() {
                consider(d);
            }
        }
        for slot in self.producers.values() {
            let producer = slot.state.lock();
            if let Some(d) = producer.next_send_deadline() {
                consider(d);
            }
            if let Some(d) = producer.next_batch_deadline() {
                consider(d);
            }
        }
        // Issue #369: a publish relocated by `reset()` into
        // `in_flight_publish_snapshots` is no longer visible to
        // `ProducerState::next_send_deadline` (that only walks the live
        // `pending` queue), so without this loop `poll_timeout` would never
        // arm a wake-up for a send parked across a reconnect and the
        // `handle_timeout` sweep below would only fire opportunistically.
        // Each bucket is documented FIFO (oldest first), so the front entry
        // alone carries the earliest deadline for that producer.
        for (handle, snapshots) in &self.in_flight_publish_snapshots {
            let Some(front) = snapshots.first() else {
                continue;
            };
            let Some(slot) = self.producers.get(handle) else {
                continue;
            };
            let Some(timeout) = slot.state.lock().send_timeout else {
                continue;
            };
            consider(crate::time::deadline_with_clamp(front.enqueued_at, timeout));
        }
        // Issue #346: surface the earliest pending-ack deadline
        // (`enqueued_at + ack_response_timeout`) so the driver schedules a
        // deterministic wake for `handle_timeout`'s reap sweep. Skipped
        // entirely when the knob is `None` (disabled) — no spurious wakeups,
        // load-bearing for moonpool determinism (an armed-but-never-firing
        // deadline would still perturb the simulated wake schedule).
        if let Some(timeout) = self.config.ack_response_timeout {
            for kind in self.pending_requests.values() {
                if let PendingRequestKind::Ack { enqueued_at, .. } = kind {
                    consider(crate::time::deadline_with_clamp(*enqueued_at, timeout));
                }
            }
        }
        // ADR-0089: rolling-rate sampling. Java's stats recorders self-tick on
        // the client-wide `HashedWheelTimer`; magnetar expresses the same
        // periodic obligation as a deadline here, so it costs no task, no
        // `select!` arm and no new state — each slot's existing
        // `last_rate_snapshot` timestamp is its own baseline, and the next
        // sample is due `stats_interval` after it.
        //
        // Skipped entirely when the knob is `None` (the default) — no spurious
        // wakeups, same load-bearing determinism rationale as the
        // `ack_response_timeout` arm above.
        //
        // A slot with no baseline yet arms nothing here: there is no instant to
        // arm from. `handle_timeout` seeds it on the next tick the connection
        // takes for any reason — the keepalive deadline is unconditionally
        // armed above while `last_activity` is set — and this arm drives every
        // sample after that. The one-interval delay that costs a fresh slot is
        // the same one Java's mid-window recorders carry.
        if let Some(interval) = self.config.stats_interval {
            for slot in self.consumers.values() {
                if let Some((_, _, at)) = slot.state.lock().last_rate_snapshot {
                    consider(crate::time::deadline_with_clamp(at, interval));
                }
            }
            for slot in self.producers.values() {
                if let Some((_, _, at)) = slot.state.lock().last_rate_snapshot {
                    consider(crate::time::deadline_with_clamp(at, interval));
                }
            }
        }
        next
    }

    /// Tick the state machine — fires keepalive pings + any per-consumer tracker actions
    /// whose deadlines have elapsed.
    pub fn handle_timeout(&mut self, now: Instant) {
        // Keepalive. `deadline_with_clamp` keeps near-`Duration::MAX`
        // keepalive intervals panic-free per invariant #6.
        let due = match self.last_activity {
            Some(last)
                if now
                    >= crate::time::deadline_with_clamp(last, self.config.keepalive_interval) =>
            {
                true
            }
            None => false,
            _ => false,
        };
        if due && self.is_connected() {
            if self.keepalive_ping_outstanding {
                // Second consecutive keepalive interval elapsed without a single
                // decoded inbound frame to clear the prior ping — the socket is
                // wedged (desynced framing, half-open TCP, or a black-holing
                // peer). Escalate instead of dead-pinging forever: flip to
                // `Failed`, which the driver reads as `should_close` and hands to
                // the supervisor for a reconnect (ADR-0058). `mark_disconnected`
                // records the disconnect timestamp and snaps the handshake state.
                tracing::warn!(
                    target: "magnetar_proto::conn",
                    keepalive_interval_ms = self.config.keepalive_interval.as_millis() as u64,
                    "keepalive ping unanswered for two intervals; failing connection",
                );
                self.mark_disconnected();
            } else {
                let ping = pb::BaseCommand {
                    r#type: pb::base_command::Type::Ping as i32,
                    ping: Some(pb::CommandPing {}),
                    ..Default::default()
                };
                let _ = self.encode_command(&ping);
                self.keepalive_ping_outstanding = true;
                self.last_activity = Some(now);
            }
        }

        // Tracker-driven redeliveries — both negative-ack delay and unacked-message timeout
        // produce the same CommandRedeliverUnacknowledgedMessages payload, so we collect
        // then emit through the shared helper.
        let mut redeliveries: Vec<(ConsumerHandle, Vec<MessageId>)> = Vec::new();
        let mut ack_actions: Vec<crate::trackers::AckAction> = Vec::new();
        let mut chunk_acks: Vec<(ConsumerHandle, Vec<MessageId>)> = Vec::new();
        // Issue #301: receiver-queue auto-adjust flow commands staged inside the
        // per-slot loop, emitted after it under `&mut self`.
        let mut adjust_flows: Vec<pb::CommandFlow> = Vec::new();
        // ADR-0089: hoisted so both the CONSUMER loop below and the PRODUCER
        // send-timeout loop further down read one `Copy` value instead of
        // re-borrowing `self.config` while `self.consumers` / `self.producers`
        // are borrowed.
        let stats_interval = self.config.stats_interval;
        for (handle, slot) in &self.consumers {
            let mut consumer = slot.state.lock();
            if let Some(tracker) = consumer.nack_tracker.as_mut() {
                for action in tracker.poll(now) {
                    let crate::trackers::NackAction::RedeliverUnacked { message_ids, .. } = action;
                    redeliveries.push((*handle, message_ids));
                }
            }
            if let Some(tracker) = consumer.unacked_tracker.as_mut() {
                for action in tracker.poll(now) {
                    let crate::trackers::UnackedAction::RedeliverExpired { message_ids, .. } =
                        action;
                    redeliveries.push((*handle, message_ids));
                }
            }
            if let Some(tracker) = consumer.ack_tracker.as_mut() {
                ack_actions.extend(tracker.poll(now));
            }
            // Bounded chunk reassembly: expire incomplete chunked messages
            // older than `expire_time_of_incomplete_chunked_message`. The
            // matching deadline is surfaced through `poll_timeout` so the
            // driver wakes us deterministically (mirrors Java
            // `removeExpireIncompleteChunkedMessages`). Drain any first-chunk
            // ids the sweep staged for auto-ack (only populated when
            // `auto_ack_oldest_chunked_message_on_queue_full` is set; the
            // default `false` path drops without acking so the broker
            // redelivers).
            consumer.sweep_expired_chunks(now);
            if !consumer.chunk_auto_ack_pending.is_empty() {
                let ids = std::mem::take(&mut consumer.chunk_auto_ack_pending);
                chunk_acks.push((*handle, ids));
            }
            // ---- BEGIN issue #301: receiver-queue auto-adjust (CONSUMER slot) ----
            // Kept in its own clearly-delineated block inside the CONSUMER-slot
            // loop so the eventual merge with the producer send-timeout drain
            // (#304, in the PRODUCER loop further down) is trivial. Runs entirely
            // under the per-slot lock with the injected `now` (ADR-0038 lock
            // ordering, ADR-0011 clock injection) — `adjust_receiver_queue` never
            // takes the connection-wide mutex. A grown target yields an
            // incremental `CommandFlow`, staged here and emitted after the loop.
            // `next_adjust_deadline`/`poll_timeout` gate when this actually fires;
            // a sub-interval tick is a cheap no-op (the policy recomputes the
            // same target and the `delta == 0` guard suppresses the flow).
            match consumer.next_adjust_deadline() {
                Some(d) if now >= d => {
                    // Per-partition proto consumer: partition count is 1 here;
                    // the façade scopes the `Auto` byte budget per-partition (see
                    // the policy ADR / docs/pip-features.md), so the budget
                    // division is already baked into the policy the façade
                    // supplies.
                    if let Some(flow) = consumer.adjust_receiver_queue(now, 1) {
                        adjust_flows.push(flow);
                    }
                }
                Some(_) => {}
                None => {
                    // Backstop only. `Connection::initial_flow` owns the arming
                    // bootstrap now (follow-ups §4): it seeds `last_adjust_at`
                    // at subscribe-ack time, so a consumer reaching this arm
                    // unarmed is one that got a tick without ever having had an
                    // initial flow issued. Seeding here still lets
                    // `poll_timeout` surface the deadline next round. No-op for
                    // the default `Fixed` policy (auto-adjust disabled) and for
                    // an already-armed consumer (`arm_adjust_clock` only fires
                    // while `last_adjust_at` is `None`).
                    consumer.arm_adjust_clock(now);
                }
            }
            // ---- END issue #301 ----
            // ADR-0089: rolling-rate sample for this consumer, taken under the
            // per-slot lock this loop already holds (ADR-0038 lock ordering —
            // `record_rate_window` touches only `ConsumerState`). No-op when
            // the knob is disabled, in which case `poll_timeout` never arms the
            // matching deadline either.
            if let Some(interval) = stats_interval
                && rate_window_due(consumer.last_rate_snapshot, interval, now)
            {
                consumer.record_rate_window(now);
            }
        }
        for (handle, ids) in redeliveries {
            self.emit_redeliver_unacked(handle, ids);
        }
        // Issue #301: emit the staged incremental receiver-queue flow commands.
        for flow in adjust_flows {
            let base = pb::BaseCommand {
                r#type: pb::base_command::Type::Flow as i32,
                flow: Some(flow),
                ..Default::default()
            };
            let _ = self.encode_command(&base);
        }
        // Auto-ack the first-chunk ids of partials evicted/expired under the
        // `auto_ack = true` policy. Individual acks; the broker treats the
        // partial as consumed and stops redelivering it.
        for (handle, ids) in chunk_acks {
            self.ack(
                handle,
                AckRequest {
                    message_ids: ids,
                    ack_type: pb::command_ack::AckType::Individual,
                    properties: Vec::new(),
                    txn_id: None,
                },
                now,
            );
        }
        // Flush the ack-grouping tracker. The actions go through the shared dispatcher
        // which allocates a `RequestId` per coalesced `CommandAck`; the response is
        // routed back through the existing pending-requests slot, but no user future is
        // tied to it (ack_grouped_* is fire-and-forget).
        if !ack_actions.is_empty() {
            self.dispatch_ack_actions(ack_actions, now);
        }

        // Per-producer batch flush sweep — Java `ProducerBuilder#batchingMaxPublishDelay`.
        // Any non-empty batch whose first message has been waiting longer than the
        // configured delay flushes now, capping end-to-end batch latency.
        let publish_time_ms = (self.wall_clock)()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0u64, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let due_batch_handles: Vec<ProducerHandle> = self
            .producers
            .iter()
            .filter(|(_, slot)| slot.state.lock().batch_deadline_elapsed(now))
            .map(|(h, _)| *h)
            .collect();
        for handle in due_batch_handles {
            if let Some(slot) = self.producers.get(&handle) {
                let _ = slot.state.lock().flush_batch(publish_time_ms, now);
            }
        }
        // Drain any frames the batch flush queued so callers don't need an extra
        // poll_transmit round-trip just to wake them up.
        self.drain_producer_outbound();

        // Per-producer send-timeout sweep. Surface each timed-out send as an
        // `OpOutcome::SendError` so the caller's send future resolves with the configured
        // timeout error.
        let mut send_timeouts: Vec<(ProducerHandle, SequenceId, Option<Waker>)> = Vec::new();
        for (handle, slot) in &self.producers {
            let mut producer = slot.state.lock();
            for (seq, waker) in producer.drain_timed_out_sends(now) {
                producer.total_send_failed = producer.total_send_failed.saturating_add(1);
                send_timeouts.push((*handle, seq, waker));
            }
            // ADR-0089: rolling-rate sample for this producer — the PRODUCER-side
            // twin of the tick in the CONSUMER loop above, taken under the
            // per-slot lock this loop already holds.
            if let Some(interval) = stats_interval
                && rate_window_due(producer.last_rate_snapshot, interval, now)
            {
                producer.record_rate_window(now);
            }
        }
        for (handle, seq, waker) in send_timeouts {
            // Pulsar's ServerError enum has no TimeoutError; use the same `-1` sentinel
            // Java surfaces as TimeoutException with a descriptive message so callers can
            // pattern-match on the error string.
            self.resolve_send_error(handle, seq, SEND_TIMEOUT_CODE, SEND_TIMEOUT_MESSAGE, waker);
        }

        // Issue #369: send-timeout sweep for publishes RELOCATED by `reset()` into
        // `in_flight_publish_snapshots`. The live-queue sweep above (`drain_timed_out_sends`)
        // only ever sees `ProducerState::pending`; a send parked across a supervisor
        // reconnect moves out of `pending` at `reset()` time and would otherwise be
        // invisible to `send_timeout` enforcement until either a successful rebuild
        // replays it or the supervisor gives up and calls `fail_all_pending`. Two-phase
        // collect-then-mutate, same shape as the ack-deadline backstop below: phase 1
        // only immutably borrows `self.producers` / `self.in_flight_publish_snapshots`
        // to find which FRONT snapshots have elapsed; phase 2 removes them and installs
        // outcomes. The deadline base is the op's ORIGINAL `enqueued_at` (preserved
        // verbatim by `snapshot_pending_sends`), NOT a fresh reset()-relative budget —
        // otherwise a send could silently outlive its configured send_timeout by an
        // unbounded number of reconnect cycles.
        let mut expired_snapshot_sends: Vec<(ProducerHandle, SequenceId)> = Vec::new();
        for (handle, snapshots) in &self.in_flight_publish_snapshots {
            let Some(slot) = self.producers.get(handle) else {
                continue;
            };
            let Some(timeout) = slot.state.lock().send_timeout else {
                continue;
            };
            for op in snapshots {
                if now < crate::time::deadline_with_clamp(op.enqueued_at, timeout) {
                    // FIFO order (oldest first) — once an entry has not yet
                    // elapsed, none behind it have either.
                    break;
                }
                expired_snapshot_sends.push((*handle, op.sequence_id));
            }
        }
        for (handle, seq) in expired_snapshot_sends {
            let Some(bucket) = self.in_flight_publish_snapshots.get_mut(&handle) else {
                continue;
            };
            if bucket.first().map(|op| op.sequence_id) != Some(seq) {
                // Already popped by an earlier iteration (defensive; the FIFO
                // scan above only ever queues each front entry once).
                continue;
            }
            let mut op = bucket.remove(0);
            if let Some(slot) = self.producers.get(&handle) {
                let mut producer = slot.state.lock();
                producer.total_send_failed = producer.total_send_failed.saturating_add(1);
            }
            // The snapshot's own waker was cleared at `reset()` time — the
            // re-polled future's waker lives in the connection-wide slab
            // instead, which `resolve_send_error`'s fallback covers (mirrors
            // `fail_producer_open_with_broker_error`'s snapshot drain).
            self.resolve_send_error(
                handle,
                seq,
                SEND_TIMEOUT_CODE,
                SEND_TIMEOUT_MESSAGE,
                op.waker.take(),
            );
            tracing::warn!(
                target: "magnetar_proto::conn",
                producer = handle.0,
                sequence_id = seq.0,
                "send timed out while relocated across reconnect",
            );
        }
        // `rebuild_producers`'s debug_asserts require every remaining snapshot
        // key to reference a live producer with at least one entry — drop any
        // bucket the sweep above emptied.
        self.in_flight_publish_snapshots
            .retain(|_, v| !v.is_empty());

        // Issue #346: `ack_response_timeout` backstop — reap any pending ack
        // whose `enqueued_at + ack_response_timeout` has elapsed. This is the
        // generic backstop for a broker that goes silent without ever
        // tearing the consumer down (dropped `CommandAckResponse` on an
        // otherwise healthy connection); the same-broker `CloseConsumer`
        // sweep above handles the fast-path for the broker-torn-the-consumer-
        // down case. Two-phase collect-then-mutate, same shape as the
        // send-timeout sweep above. Skipped entirely when the knob is
        // disabled (`None`) — `poll_timeout` never arms a deadline in that
        // case either, so this loop is then a guaranteed no-op.
        if let Some(timeout) = self.config.ack_response_timeout {
            let expired_acks: Vec<(RequestId, ConsumerHandle)> = self
                .pending_requests
                .iter()
                .filter_map(|(rid, kind)| match kind {
                    PendingRequestKind::Ack {
                        handle,
                        enqueued_at,
                    } if now >= crate::time::deadline_with_clamp(*enqueued_at, timeout) => {
                        Some((*rid, *handle))
                    }
                    _ => None,
                })
                .collect();
            for (rid, handle) in expired_acks {
                self.pending_requests.remove(&rid);
                let message = "ack timeout".to_owned();
                self.outcomes.insert(
                    PendingOpKey::Request(rid),
                    OpOutcome::Error {
                        request_id: rid,
                        code: -1,
                        message: message.clone(),
                    },
                );
                self.wake_for_request(rid);
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    consumer.total_acks_failed = consumer.total_acks_failed.saturating_add(1);
                }
                self.events.push_back(ConnectionEvent::AckResponse {
                    request_id: Some(rid),
                    result: Err(message),
                });
            }
        }
    }

    /// Terminalize every publish relocated into `in_flight_publish_snapshots` for `handle`.
    ///
    /// Removes the whole bucket and installs an `OpOutcome::Terminal` per snapshot. The wake
    /// goes through the snapshot's own waker when it still carries one and falls back to the
    /// connection-wide slab otherwise: `reset()` clears a relocated `OpSend`'s waker, so which
    /// of the two holds the wake depends on whether the future has re-polled since.
    ///
    /// Shared by [`Self::fail_all_pending`] (every handle, at supervisor give-up) and
    /// [`Self::fail_producer_open_with_broker_error`] (a single handle). No-op when the handle
    /// has no bucket.
    fn terminalize_snapshot_bucket(&mut self, handle: ProducerHandle, reason: &str) {
        let Some(snapshots) = self.in_flight_publish_snapshots.remove(&handle) else {
            return;
        };
        for mut snapshot in snapshots {
            let key = PendingOpKey::Send(handle, snapshot.sequence_id);
            self.outcomes.insert(
                key,
                OpOutcome::Terminal {
                    key,
                    reason: reason.to_owned(),
                },
            );
            if let Some(w) = snapshot.waker.take() {
                let _ = self.wakers.remove(&key);
                w.wake();
            } else if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }
    }

    /// Resolve a publish as failed: install the outcome, wake whoever is parked on it, and
    /// queue the matching event.
    ///
    /// Shared by both send-timeout sweeps in [`Self::handle_timeout`] — the live-queue sweep
    /// over `ProducerState::pending` and the issue #369 sweep over
    /// `in_flight_publish_snapshots`. The waker fallback matters because the two sweeps source
    /// it differently: a live-queue entry carries its own waker, while a relocated snapshot had
    /// its waker cleared at `reset()` time and the re-polled future's waker lives in the
    /// connection-wide slab instead.
    ///
    /// Callers keep their own `total_send_failed` accounting and logging — the two sweeps bump
    /// that counter at different points (phase 1 vs. inline).
    fn resolve_send_error(
        &mut self,
        handle: ProducerHandle,
        seq: SequenceId,
        code: i32,
        message: &str,
        waker: Option<Waker>,
    ) {
        let key = PendingOpKey::Send(handle, seq);
        self.outcomes.insert(
            key,
            OpOutcome::SendError {
                sequence_id: seq,
                code,
                message: message.to_owned(),
            },
        );
        if let Some(w) = waker {
            w.wake();
        } else if let Some(w) = self.wakers.remove(&key) {
            w.wake();
        }
        self.events.push_back(ConnectionEvent::SendError {
            handle,
            sequence_id: seq,
            code,
            message: message.to_owned(),
        });
    }

    /// Register a waker for a pending op. The waker will be woken when an outcome lands.
    pub fn register_waker(&mut self, key: PendingOpKey, waker: Waker) {
        if let Some(_outcome) = self.outcomes.get(&key) {
            // Wake immediately if outcome is already present.
            waker.wake();
            return;
        }
        match key {
            PendingOpKey::Send(handle, seq) => {
                if let Some(slot) = self.producers.get(&handle) {
                    // Attach to the pending OpSend when it exists. During the
                    // reset → `ProducerSuccess` window the op is parked in the
                    // reset snapshot (NOT in `pending`), so the slot
                    // registration no-ops — fall through to the
                    // connection-wide slab; the receipt / send-error /
                    // timeout arms all fall back to the slab when the op
                    // carries no waker. Unconditionally returning here
                    // silently dropped the waker and left the user's send
                    // future starved forever after a replayed receipt.
                    if slot.state.lock().register_waker(seq, waker.clone()) {
                        return;
                    }
                }
            }
            PendingOpKey::Request(_) => {}
        }
        self.wakers.insert(key, waker);
    }

    /// Unregister the waker for a pending op, if one is registered.
    ///
    /// Mirrors [`Self::register_waker`]'s dispatch: for [`PendingOpKey::Send`] the
    /// waker may live on the matching [`crate::producer::ProducerSlot`] instead of
    /// the connection-wide slab, so we clear both sites unconditionally. For
    /// [`PendingOpKey::Request`] only the connection-wide slab is touched.
    ///
    /// Called from the [`Drop`] impls on the runtime-side request futures
    /// (`magnetar_runtime_tokio` / `magnetar_runtime_moonpool`
    /// `RequestFut`) so a future that is cancelled
    /// before its outcome lands does not leave an orphaned [`Waker`] in the
    /// `wakers` map. The leak is otherwise inert (the dispatcher would later
    /// `remove(&key)` and wake a no-op waker when the outcome arrives, or
    /// [`Self::reset`] would garbage-collect on the next reconnect) but
    /// defense-in-depth keeps the slab bounded for long-running connections
    /// that issue many short-lived lookups whose request ids never resolve
    /// (e.g. callers that drop the future before broker round-trip). See the
    /// lookup multi-agent review MEDIUM-4 finding and ADR-0024.
    pub fn unregister_waker(&mut self, key: PendingOpKey) {
        // Drop the connection-wide entry first.
        let _ = self.wakers.remove(&key);
        // For Send keys the waker may have been stashed on the producer slot
        // instead — clear it there too so the dispatcher never wakes a stale
        // task. The reverse-lookup is O(pending) on the matching producer's
        // pending vector; this is only hit on future-drop so the cost is
        // amortised against the user's drop.
        if let PendingOpKey::Send(handle, seq) = key {
            if let Some(slot) = self.producers.get(&handle) {
                slot.state.lock().clear_waker(seq);
            }
        }
    }

    /// Cancel a request-id-correlated operation.
    ///
    /// Removes every local correlation surface: parked waker, landed outcome,
    /// pending-request discriminator, and lookup/partition registry capacity.
    /// The already-encoded wire frame cannot be recalled; a late broker reply
    /// is ignored because its registry and request-kind entries are gone.
    /// Idempotent so response-vs-drop races are harmless.
    pub fn cancel_request(&mut self, request_id: RequestId) {
        let key = PendingOpKey::Request(request_id);
        self.unregister_waker(key);
        let _ = self.outcomes.remove(&key);
        if let Some(PendingRequestKind::TopicWatcher { watcher_id }) =
            self.pending_requests.remove(&request_id)
        {
            self.topic_watchers.close(watcher_id);
        }
        let _ = self.lookup.take_lookup(request_id);
        let _ = self.lookup.take_partition(request_id);
    }

    /// Consume the outcome of a pending op, if one is ready.
    pub fn take_outcome(&mut self, key: PendingOpKey) -> Option<OpOutcome> {
        self.outcomes.remove(&key)
    }

    /// Test/diagnostic accessor: number of wakers currently parked in the
    /// connection-wide [`Self::wakers`] slab. Used by the
    /// `lookup_drop_unregister` integration tests on both runtime engines to
    /// assert that dropping a [`PendingOpKey::Request`]-correlated future
    /// drains its [`Waker`] off the connection. **Not** counted: per-producer
    /// per-sequence wakers stashed on [`crate::producer::ProducerSlot`].
    #[doc(hidden)]
    pub fn pending_waker_count(&self) -> usize {
        self.wakers.len()
    }

    /// Open a producer. The state machine emits a `CommandProducer` and assigns a
    /// [`ProducerHandle`]. The corresponding [`ConnectionEvent::ProducerReady`] arrives on the
    /// next `poll_event` cycle after the broker responds.
    pub fn create_producer(&mut self, req: CreateProducerRequest) -> ProducerHandle {
        let handle = ProducerHandle(self.next_producer_id);
        self.next_producer_id = self.next_producer_id.wrapping_add(1);
        let max_size = self
            .broker_max_message_size
            .unwrap_or(self.config.default_max_message_size);
        let mut state = ProducerState::new(handle, req.topic.clone(), req.compression, max_size);
        state.batching_enabled = req.enable_batching;
        state.chunking_enabled = req.enable_chunking;
        state.max_batch_size_bytes = req.max_batch_size_bytes;
        state.max_messages_in_batch = req.max_messages_in_batch;
        state.name = req.producer_name.clone();
        if let Some(initial) = req.initial_sequence_id {
            state.set_initial_sequence_id(initial);
        }
        state.send_timeout = req.send_timeout;
        state.batching_max_publish_delay = req.batching_max_publish_delay;
        state.access_mode = req.access_mode;
        seed_rate_window_baseline(
            &mut state.last_rate_snapshot,
            self.config.stats_interval,
            self.last_activity,
        );
        let identity = crate::producer::ProducerIdentity {
            handle,
            topic: req.topic.clone(),
            access_mode: req.access_mode,
        };
        let slot = crate::producer::ProducerSlot::new(identity, state);
        self.producers.insert(handle, slot);
        // Stash the request so [`Self::rebuild_producers`] can replay it on a freshly-handshaked
        // session.
        self.producer_create_requests.insert(handle, req.clone());

        let _ = self.emit_command_producer(handle, &req);
        handle
    }

    /// Emit a `CommandProducer` carrying `req`'s parameters for the producer identified by
    /// `handle`. Used by both [`Self::create_producer`] (initial open) and
    /// [`Self::rebuild_producers`] (post-reconnect replay).
    ///
    /// Returns the allocated [`RequestId`] so the caller can correlate the broker's
    /// `CommandProducerSuccess` (via [`OpOutcome::Success`]) against it.
    fn emit_command_producer(
        &mut self,
        handle: ProducerHandle,
        req: &CreateProducerRequest,
    ) -> RequestId {
        let request_id = self.alloc_request_id();
        let epoch = self
            .producers
            .get(&handle)
            .map(|slot| slot.state.lock().epoch)
            .unwrap_or(0);
        let producer_metadata: Vec<pb::KeyValue> = req
            .producer_metadata
            .iter()
            .map(|(k, v)| pb::KeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        let cmd = pb::CommandProducer {
            topic: req.topic.clone(),
            producer_id: handle.0,
            request_id: request_id.0,
            producer_name: req.producer_name.clone(),
            encrypted: None,
            metadata: producer_metadata,
            schema: req.schema.clone(),
            // Only stamp the epoch on the wire once it's non-zero — Java's `ProducerImpl`
            // omits the field on the initial create and stamps it on every subsequent
            // re-attach. Matching that keeps brokers that predate the field happy.
            epoch: if epoch == 0 { None } else { Some(epoch) },
            user_provided_producer_name: Some(req.producer_name.is_some()),
            producer_access_mode: Some(req.access_mode as i32),
            topic_epoch: None,
            txn_enabled: None,
            initial_subscription_name: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Producer as i32,
            producer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::ProducerOpen { handle });
        if let Some(slot) = self.producers.get(&handle) {
            slot.state.lock().open_request_id = Some(request_id);
        }
        request_id
    }

    /// Open a consumer. Returns the handle and emits `CommandSubscribe`. The driver receives
    /// [`ConnectionEvent::SubscribeAcked`] on success and should then call
    /// [`Self::initial_flow`] to feed the broker an initial flow.
    pub fn subscribe(&mut self, req: SubscribeRequest) -> ConsumerHandle {
        let handle = ConsumerHandle(self.next_consumer_id);
        self.next_consumer_id = self.next_consumer_id.wrapping_add(1);
        // Issue #301: build with the request's receiver-queue policy. `None`
        // resolves to the default `Fixed(receiver_queue_size)`, so the raw
        // `receiver_queue_size` path is unchanged.
        let policy = req
            .receiver_queue_policy
            .clone()
            .unwrap_or_else(|| crate::receiver_queue::fixed(req.receiver_queue_size));
        let mut state = ConsumerState::with_policy(
            handle,
            req.topic.clone(),
            req.subscription.clone(),
            policy,
            req.receiver_queue_adjust_interval,
        );
        state.max_redeliver_count = req.max_redeliver_count;
        state.consumer_name = req.consumer_name.clone();
        if let Some(delay) = req.negative_ack_redelivery_delay {
            state.nack_tracker = Some(crate::trackers::NegativeAcksTracker::new(handle, delay));
        }
        if let Some(timeout) = req.ack_timeout {
            let mut tracker = crate::trackers::UnackedMessageTracker::new(handle, timeout);
            if let Some(backoff) = req.ack_timeout_backoff {
                tracker = tracker.with_backoff(backoff);
            }
            state.unacked_tracker = Some(tracker);
        }
        if let Some(group_time) = req.ack_group_time {
            state.ack_tracker = Some(crate::trackers::AckGroupingTracker::new(handle, group_time));
        }
        state.crypto_failure_action = req.crypto_failure_action;
        state.max_pending_chunked_message = req.max_pending_chunked_message;
        state.auto_ack_oldest_chunked_message_on_queue_full =
            req.auto_ack_oldest_chunked_message_on_queue_full;
        state.expire_time_of_incomplete_chunked_message =
            req.expire_time_of_incomplete_chunked_message;
        seed_rate_window_baseline(
            &mut state.last_rate_snapshot,
            self.config.stats_interval,
            self.last_activity,
        );
        let identity = crate::consumer::ConsumerIdentity {
            handle,
            topic: req.topic.clone(),
            subscription: req.subscription.clone(),
        };
        let slot = crate::consumer::ConsumerSlot::new(identity, state);
        self.consumers.insert(handle, slot);
        // Stash the request so [`Self::rebuild_consumers`] can replay it on a freshly-handshaked
        // session.
        self.consumer_subscribe_requests.insert(handle, req.clone());

        let _ = self.emit_command_subscribe(
            handle,
            &req,
            req.start_message_id,
            SubscribeAckAction::NotifyWaiter,
        );
        handle
    }

    /// Emit a `CommandSubscribe` carrying `req`'s parameters for the consumer identified by
    /// `handle`. `resume_from` overrides `req.start_message_id` — used by
    /// [`Self::rebuild_consumers`] to point the broker at the post-ack position after a
    /// reconnect. `ack_action` transfers acknowledgement ownership to the new
    /// request while preserving an existing user waiter across driver-owned
    /// retries and reconnect rebuilds.
    fn emit_command_subscribe(
        &mut self,
        handle: ConsumerHandle,
        req: &SubscribeRequest,
        resume_from: Option<MessageId>,
        ack_action: SubscribeAckAction,
    ) -> RequestId {
        let request_id = self.alloc_request_id();
        let subscription_properties: Vec<pb::KeyValue> = req
            .subscription_properties
            .iter()
            .map(|(key, value)| pb::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let key_shared_meta = req.key_shared.as_ref().map(|cfg| pb::KeySharedMeta {
            key_shared_mode: cfg.mode as i32,
            hash_ranges: cfg
                .sticky_hash_ranges
                .iter()
                .map(|(start, end)| pb::IntRange {
                    start: *start,
                    end: *end,
                })
                .collect(),
            allow_out_of_order_delivery: Some(cfg.allow_out_of_order_delivery),
        });
        let start_message_id = resume_from.map(MessageId::to_pb);
        let consumer_metadata: Vec<pb::KeyValue> = req
            .consumer_metadata
            .iter()
            .map(|(key, value)| pb::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let cmd = pb::CommandSubscribe {
            topic: req.topic.clone(),
            subscription: req.subscription.clone(),
            sub_type: req.sub_type as i32,
            consumer_id: handle.0,
            request_id: request_id.0,
            consumer_name: req.consumer_name.clone(),
            priority_level: req.priority_level,
            durable: Some(req.durable),
            start_message_id,
            metadata: consumer_metadata,
            read_compacted: if req.read_compacted { Some(true) } else { None },
            schema: req.schema.clone(),
            initial_position: Some(req.initial_position as i32),
            replicate_subscription_state: req.replicate_subscription_state,
            force_topic_creation: req.force_topic_creation,
            start_message_rollback_duration_sec: req.start_message_rollback_duration_sec,
            key_shared_meta,
            subscription_properties,
            consumer_epoch: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Subscribe as i32,
            subscribe: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerSubscribe { handle });
        if let Some(slot) = self.consumers.get(&handle) {
            let mut consumer = slot.state.lock();
            match ack_action {
                SubscribeAckAction::NotifyWaiter => {
                    consumer.subscribe_waiter_id = Some(request_id);
                    consumer.subscribe_waiter_request = Some(request_id);
                    consumer.subscribe_waiter_completed = false;
                    consumer.flow_on_subscribe_ack = false;
                    consumer.flow_on_subscribe_ack_request = None;
                }
                SubscribeAckAction::ReleaseFlow => {
                    if consumer.subscribe_waiter_id.is_some() {
                        consumer.subscribe_waiter_request = Some(request_id);
                        consumer.subscribe_waiter_completed = false;
                        consumer.flow_on_subscribe_ack = false;
                        consumer.flow_on_subscribe_ack_request = None;
                    } else {
                        consumer.flow_on_subscribe_ack = true;
                        consumer.flow_on_subscribe_ack_request = Some(request_id);
                    }
                }
            }
        }
        request_id
    }

    /// Emit the initial flow command for a consumer once it's been acked, and
    /// bootstrap its receiver-queue auto-adjust schedule from the injected
    /// `now` (ADR-0011).
    ///
    /// The arming is the point of the `now` parameter. `ConsumerState::next_adjust_deadline`
    /// yields `None` until `last_adjust_at` is set, and `Self::poll_timeout` folds that `None`
    /// away — so before this bootstrap existed the only live deadline for a fresh `Auto`
    /// consumer was the keepalive one, and every decoded inbound frame pushes that out
    /// (the single `last_activity` refresh site, ADR-0058). A connection with continuous
    /// inbound traffic — message deliveries, or the `CommandAckResponse` stream produced by
    /// a consumer that awaits each individual ack — therefore deferred the keepalive
    /// deadline indefinitely, `Self::handle_timeout` never ran, and the adjust schedule
    /// never armed at all. Arming here makes the first tick's timing a function of the
    /// subscribe-ack moment alone.
    ///
    /// `ConsumerState::arm_adjust_clock` is idempotent (it only fires while
    /// `last_adjust_at` is `None`) and a no-op for the default `Fixed` policy
    /// (`adjust_interval == None`), so the re-attach and Failover-promotion
    /// re-flows that also route through here neither restart nor skew an
    /// already-running schedule.
    ///
    /// Both the flow command and the arming happen under a single per-slot lock
    /// acquisition, dropped before the connection-wide encode (ADR-0038).
    pub fn initial_flow(&mut self, handle: ConsumerHandle, now: Instant) -> Option<RequestId> {
        let flow_cmd = {
            let mut consumer = self.consumers.get(&handle)?.state.lock();
            let flow_cmd = consumer.initial_flow();
            consumer.arm_adjust_clock(now);
            flow_cmd
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Flow as i32,
            flow: Some(flow_cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        None
    }

    /// Send a message via the given producer.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvariantViolation`] if the handle is unknown, and propagates
    /// the producer's own [`crate::error::ProducerError`] (wrapped) if the send is rejected.
    pub fn send(
        &mut self,
        handle: ProducerHandle,
        msg: crate::producer::OutgoingMessage,
        publish_time_ms: u64,
        now: Instant,
    ) -> Result<SequenceId, ProtocolError> {
        let slot = self
            .producers
            .get(&handle)
            .ok_or(ProtocolError::InvariantViolation("unknown producer handle"))?;
        let seq_id = {
            let mut producer = slot.state.lock();
            let decision = producer
                .queue_send(msg, publish_time_ms, now)
                .map_err(|_| ProtocolError::InvariantViolation("producer rejected send"))?;
            match decision {
                SendDecision::Emit { .. } | SendDecision::Batched => {}
            }
            SequenceId(producer.last_sequence_id_pushed.max(0) as u64)
        };
        self.drain_producer_outbound();
        Ok(seq_id)
    }

    /// Force a batch flush for a producer.
    pub fn flush_producer(
        &mut self,
        handle: ProducerHandle,
        publish_time_ms: u64,
        now: Instant,
    ) -> usize {
        // ADR-0048 buggify point: `batch_container.flush.split` —
        // when the label fires AND the producer's batch holds more
        // than one message, return 0 without draining the
        // BatchContainer. The batch survives untouched; the next
        // caller-driven flush picks it up. Effect: a chunk of the
        // payload that would have left this tick is deferred to the
        // next flush, exercising the framing-resume + receipt
        // ordering paths under simulation. No invariant violation:
        // OpSends already in `pending` stay in `pending`, the wire
        // frame simply gets built one flush later.
        let batch_holds_multiple = self
            .producers
            .get(&handle)
            .is_some_and(|slot| slot.state.lock().batch.len() > 1);
        if batch_holds_multiple
            && self
                .buggify
                .should_fire(crate::buggify::labels::BATCH_CONTAINER_FLUSH_SPLIT, 0.05)
        {
            return 0;
        }
        let n = self
            .producers
            .get(&handle)
            .map(|slot| slot.state.lock().flush_batch(publish_time_ms, now))
            .unwrap_or(0);
        self.drain_producer_outbound();
        n
    }

    /// Number of in-flight sends on a producer (i.e. sends with no `CommandSendReceipt` yet).
    /// Used by the runtime engines' `Producer::flush` to know when it's safe to return.
    #[must_use]
    pub fn producer_pending_count(&self, handle: ProducerHandle) -> usize {
        self.producers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().pending.len())
    }

    /// Number of messages currently buffered in the producer's batch container (waiting
    /// for the next flush cycle). Returns `0` for unknown handles or when batching is
    /// disabled / the batch is empty.
    #[must_use]
    pub fn producer_batch_len(&self, handle: ProducerHandle) -> usize {
        self.producers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().batch.len())
    }

    /// Sum of payload bytes currently buffered in the producer's batch container.
    #[must_use]
    pub fn producer_batch_bytes(&self, handle: ProducerHandle) -> usize {
        self.producers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().batch.current_size_bytes)
    }

    /// Access mode the producer was opened with. Returns
    /// `ProducerAccessMode::Shared` (the broker default) for unknown handles. Mirrors Java
    /// `Producer#getProducerAccessMode`.
    ///
    /// Identity-only read — does not take the per-slot mutex.
    #[must_use]
    pub fn producer_access_mode(&self, handle: ProducerHandle) -> pb::ProducerAccessMode {
        self.producers
            .get(&handle)
            .map_or(pb::ProducerAccessMode::Shared, |slot| {
                slot.identity.access_mode
            })
    }

    /// Last sequence id this client has pushed onto the wire. `-1` if the producer has
    /// never sent. Mirrors Java's `Producer#getLastSequenceId` (which counts pushes,
    /// not broker acknowledgements).
    #[must_use]
    pub fn producer_last_sequence_id_pushed(&self, handle: ProducerHandle) -> i64 {
        self.producers
            .get(&handle)
            .map_or(-1, |slot| slot.state.lock().last_sequence_id_pushed)
    }

    /// Last sequence id the broker has acknowledged via `CommandSendReceipt`. `-1` if the
    /// producer has no acknowledged sends yet. Useful for at-least-once resume-on-restart.
    #[must_use]
    pub fn producer_last_sequence_id_published(&self, handle: ProducerHandle) -> i64 {
        self.producers
            .get(&handle)
            .map_or(-1, |slot| slot.state.lock().last_sequence_id_published)
    }

    /// Cumulative producer counters snapshot. Returns `None` if the producer handle is unknown.
    #[must_use]
    pub fn producer_stats(&self, handle: ProducerHandle) -> Option<crate::producer::ProducerStats> {
        self.producers
            .get(&handle)
            .map(|slot| slot.state.lock().stats())
    }

    /// Cumulative consumer counters snapshot. Returns `None` if the consumer handle is unknown.
    #[must_use]
    pub fn consumer_stats(&self, handle: ConsumerHandle) -> Option<crate::consumer::ConsumerStats> {
        self.consumers
            .get(&handle)
            .map(|slot| slot.state.lock().stats())
    }

    /// Take a rolling-window stats snapshot on the consumer identified by `handle`. Mirrors Java
    /// `ConsumerStatsRecorder`'s rolling-window rate calculation. No-op if the handle is
    /// unknown.
    ///
    /// This is the manual sample point. The connection samples every registered
    /// consumer on its own once [`ConnectionConfig::stats_interval`] is set,
    /// off a `poll_timeout` deadline swept in `handle_timeout` (ADR-0089) —
    /// magnetar's equivalent of Java's per-recorder tick on the client-wide
    /// timer. That knob defaults to `None`, and with it disabled sampling is
    /// **caller-driven**: no engine calls this, so a caller that never invokes
    /// it leaves `ConsumerStats::msgs_per_sec` / `bytes_per_sec` at `0.0`
    /// forever.
    ///
    /// Pick one cadence. Calling this while the sweep is armed re-seeds the
    /// window, so the two interleave and neither reports the interval it
    /// intended.
    pub fn consumer_record_rate_window(&mut self, handle: ConsumerHandle, now: std::time::Instant) {
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().record_rate_window(now);
        }
    }

    /// Take a rolling-window stats snapshot on the producer identified by `handle`. Same
    /// shape as [`Self::consumer_record_rate_window`] but for the producer side.
    pub fn producer_record_rate_window(&mut self, handle: ProducerHandle, now: std::time::Instant) {
        if let Some(slot) = self.producers.get(&handle) {
            slot.state.lock().record_rate_window(now);
        }
    }

    /// `true` if the producer with this handle has been closed (locally via
    /// [`Self::close_producer`] or remotely via a broker `CloseProducer`). Returns `true`
    /// for unknown handles so callers can treat "handle dropped" as "closed". Mirrors Java
    /// `Producer#isConnected` inversion — Pulsar Java has no direct `isClosed` on
    /// Producer, but ProducerImpl exposes `getState() == CLOSED` for this exact purpose.
    #[must_use]
    pub fn producer_is_closed(&self, handle: ProducerHandle) -> bool {
        self.producers
            .get(&handle)
            .is_none_or(|slot| slot.state.lock().closed)
    }

    /// `true` if the consumer with this handle has been closed (locally via
    /// [`Self::close_consumer`] / [`Self::unsubscribe`] or remotely via a broker
    /// `CloseConsumer`). Returns `true` for unknown handles. Mirrors Java
    /// `Consumer#isClosed` semantics via ConsumerImpl's `getState() == CLOSED`.
    #[must_use]
    pub fn consumer_is_closed(&self, handle: ConsumerHandle) -> bool {
        self.consumers
            .get(&handle)
            .is_none_or(|slot| slot.state.lock().closed)
    }

    /// Number of messages currently buffered in the consumer's receiver queue, waiting for
    /// a `receive()` call to pull them out. Returns `0` for unknown handles. Mirrors Java
    /// `ConsumerImpl#numMessagesInQueue` / `getTotalIncomingMessages` (the in-memory side).
    #[must_use]
    pub fn consumer_queue_len(&self, handle: ConsumerHandle) -> usize {
        self.consumers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().queue.len())
    }

    /// Number of dispatch permits the consumer still has with the broker — i.e. messages
    /// it has authorised the broker to push without an explicit `CommandFlow`. Returns `0`
    /// for unknown handles. Mirrors Java `ConsumerBase#getAvailablePermits`.
    ///
    /// Issue #349 scope note: this reads [`crate::consumer::ConsumerState::granted_permits`]
    /// (the additive grant mirror), unchanged by the permit-balance split — out of scope per
    /// the issue's four locked design items, which name only
    /// [`crate::receiver_queue::FlowStats::available_permits`]. For the REAL, decrementing
    /// balance see [`crate::consumer::ConsumerState::permit_balance`]; extending Java-parity
    /// (a genuinely decrementing counter) to this public accessor is a separate, unscoped
    /// change.
    #[must_use]
    pub fn consumer_available_permits(&self, handle: ConsumerHandle) -> u32 {
        self.consumers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().granted_permits)
    }

    /// Last broker-reported Failover active/standby state for `handle`
    /// (issue #348). `None` for an unknown handle OR a consumer that has
    /// never received a `CommandActiveConsumerChange` (e.g. a `Shared` /
    /// `Exclusive` subscription, which the broker never sends the command
    /// for). Mirrors Java `ConsumerEventListener`'s implicit state — the
    /// runtime `Consumer::is_active()` accessor reads this via the per-slot
    /// lock, no global lock required.
    #[must_use]
    pub fn consumer_is_active(&self, handle: ConsumerHandle) -> Option<bool> {
        self.consumers
            .get(&handle)
            .and_then(|slot| slot.state.lock().is_active)
    }

    /// The consumer's CURRENT receiver-queue target (issue #301). For the
    /// default [`crate::receiver_queue::Fixed`] policy this is the constant the
    /// user configured; for [`crate::receiver_queue::Auto`] it is the live,
    /// auto-tuned target after the latest adjust tick. Returns `0` for unknown
    /// handles. Mirrors Java `ConsumerImpl#getCurrentReceiverQueueSize` under
    /// PIP-74 auto-scaling.
    #[must_use]
    pub fn consumer_receiver_queue_size(&self, handle: ConsumerHandle) -> usize {
        self.consumers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().receiver_queue_size)
    }

    /// PIP-4 decryption failure handling configured for this consumer. Returns
    /// [`CryptoFailureAction::Fail`] (the safe default) for unknown handles so callers can
    /// treat a missing consumer as fail-fast. Mirrors Java `Consumer#getCryptoFailureAction`.
    #[must_use]
    pub fn consumer_crypto_failure_action(&self, handle: ConsumerHandle) -> CryptoFailureAction {
        self.consumers
            .get(&handle)
            .map_or(CryptoFailureAction::Fail, |slot| {
                slot.state.lock().crypto_failure_action()
            })
    }

    /// Walk every registered producer slot, drain its staged outbound
    /// frames, and encode them into the connection-wide outbound byte
    /// buffer. The runtime drivers MUST call this immediately before
    /// [`Self::poll_transmit`] so any sends queued by the per-slot
    /// hot-path entry point ([`crate::ProducerSlot::queue_send`]) — which
    /// bypasses the global Connection mutex (ADR-0038 Phase 3) — land on
    /// the wire.
    ///
    /// Lock-ordering: requires `&mut self` on Connection (i.e. the global
    /// lock is held). Takes each per-slot mutex briefly to drain frames —
    /// the canonical global → per-slot order.
    pub fn drain_producer_outbound(&mut self) {
        // Producer-not-ready gate (Java `handleProducerSuccess` parity): no
        // SEND frame may reach the wire before the handshake is `Connected`
        // AND the slot's (re-)attachment is acked — Pulsar closes the WHOLE
        // connection on a send to a not-ready producer ("Received message,
        // but the producer is not ready"). Frames stay staged in the slot;
        // the `ProducerSuccess` handler opens the per-slot gate.
        if self.state != HandshakeState::Connected {
            return;
        }
        // Pull every queued frame from every ready producer and emit it into
        // the connection's outbound byte buffer.
        let handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in handles {
            // SAFETY (lock-ordering): the global Connection mutex is held by the
            // caller (Connection's `&mut self`); we take the per-slot mutex
            // BELOW it, never above. See ADR-0038.
            let mut emitted: u32 = 0;
            loop {
                let frame = self.producers.get(&handle).and_then(|slot| {
                    let mut state = slot.state.lock();
                    if !state.broker_ready {
                        return None;
                    }
                    state.next_outbound_frame()
                });
                let Some(frame) = frame else { break };
                emitted = emitted.saturating_add(1);
                let _ = encode_payload(
                    &mut self.outbound,
                    &frame.command,
                    &frame.metadata,
                    &frame.payload,
                );
            }
            if emitted > 0 {
                tracing::trace!(
                    target: "magnetar_proto::conn",
                    handle = ?handle,
                    frames = emitted,
                    "drained staged producer frames into connection buffer"
                );
            }
        }
    }

    /// Wave-1.2 (ADR-0040) — drain producer frames into the
    /// segment-list buffer instead of the contiguous outbound buffer.
    ///
    /// Each frame contributes a `[head, payload]` pair of `Bytes`
    /// segments. `payload` is the producer's `Bytes` payload re-used
    /// unchanged (zero-copy); `head` is freshly encoded via
    /// [`encode_payload_head`] and frozen. The runtime adapter pulls
    /// the resulting list via [`Self::poll_transmit_vectored`] and
    /// feeds it to `poll_write_vectored` / `IoSlice`, skipping the
    /// user-space memcpy that [`Self::drain_producer_outbound`]
    /// performs at the `dst.extend_from_slice(payload)` line.
    ///
    /// Lock-ordering: requires `&mut self` on Connection (i.e. the
    /// global lock is held). Takes each per-slot mutex briefly to
    /// drain frames — the canonical global → per-slot order.
    pub fn drain_producer_outbound_vectored(&mut self) {
        // Same producer-not-ready gate as [`Self::drain_producer_outbound`].
        if self.state != HandshakeState::Connected {
            return;
        }
        let handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in handles {
            let mut emitted: u32 = 0;
            loop {
                let frame = self.producers.get(&handle).and_then(|slot| {
                    let mut state = slot.state.lock();
                    if !state.broker_ready {
                        return None;
                    }
                    state.next_outbound_frame()
                });
                let Some(frame) = frame else {
                    if emitted > 0 {
                        tracing::trace!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            frames = emitted,
                            "drained staged producer frames into segment list"
                        );
                    }
                    break;
                };
                emitted = emitted.saturating_add(1);
                let Ok(head) = encode_payload_head(&frame.command, &frame.metadata, &frame.payload)
                else {
                    // Encoding only fails for `BadLength` (>u32::MAX
                    // frame) — the producer state machine has already
                    // bounded the payload at `broker_max_message_size`.
                    // Skip the frame rather than panicking; preserves
                    // invariant #6 (no proto-side panics) and matches
                    // the legacy `let _ = encode_payload(...)` swallow.
                    continue;
                };
                self.outbound_segments.push(head.freeze());
                self.outbound_segments.push(frame.payload);
            }
        }
    }

    /// Acknowledge messages.
    pub fn ack(&mut self, handle: ConsumerHandle, ack: AckRequest, now: Instant) -> RequestId {
        let request_id = self.alloc_request_id();
        let n_ids = ack.message_ids.len() as u64;
        // Stop tracking the acked ids in both the unacked-message tracker and the nack tracker
        // (caller may have nacked then acked the same id). Also remember the highest acked
        // id so [`Self::rebuild_consumers`] resumes from the post-ack position after a
        // reconnect.
        if let Some(slot) = self.consumers.get(&handle) {
            let mut consumer = slot.state.lock();
            for id in &ack.message_ids {
                if let Some(t) = consumer.unacked_tracker.as_mut() {
                    t.remove(id);
                }
                if let Some(t) = consumer.nack_tracker.as_mut() {
                    t.remove(id);
                }
                // Track the highest acked id. `MessageId` derives `Ord` and orders on
                // `(ledger_id, entry_id, partition, batch_index, batch_size)`, which matches the
                // broker's cursor order on the leading `(ledger_id, entry_id)` pair.
                if consumer.last_acked_message_id.is_none_or(|prev| *id > prev) {
                    consumer.last_acked_message_id = Some(*id);
                }
            }
        }
        // PIP-54: for any message id with `batch_index >= 0`, look up the per-batch ack
        // tracker, clear the bit at `batch_index`, and emit either a "full" MessageIdData
        // (no ack_set; the batch is now fully acked, so the broker can advance the cursor
        // past it) or a partial-ack MessageIdData carrying the bitset of still-unacked
        // positions so the broker holds the cursor.
        let pb_ids: Vec<pb::MessageIdData> =
            if matches!(ack.ack_type, pb::command_ack::AckType::Individual) {
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    ack.message_ids
                        .iter()
                        .map(|id| {
                            let mut pb_id = id.to_pb();
                            if id.batch_index >= 0 {
                                let key = (id.ledger_id, id.entry_id);
                                let fully = if let Some(entry) =
                                    consumer.batch_ack_tracker.get_mut(&key)
                                {
                                    let fully = entry.ack_position(id.batch_index);
                                    if !fully {
                                        pb_id.ack_set = entry.ack_set_i64();
                                    }
                                    fully
                                } else {
                                    // No tracker entry — either the batch's first delivery happened
                                    // before PIP-54 wiring or the tracker was already cleared by a
                                    // prior full-batch ack. Fall through as a regular ack.
                                    true
                                };
                                if fully {
                                    consumer.batch_ack_tracker.remove(&key);
                                }
                            }
                            pb_id
                        })
                        .collect()
                } else {
                    ack.message_ids.iter().map(|m| m.to_pb()).collect()
                }
            } else {
                // Cumulative ack — every position up to the supplied id is implicitly acked,
                // so any per-batch tracker entries AT OR BELOW the cumulative position are
                // stale, not just the entry of the supplied id itself. Prune them all: a
                // cumulative-only acking pattern (e.g. a watermark acker) otherwise leaks one
                // `BatchAckEntry` per batched broker entry for the lifetime of the connection
                // (issue #326). `(ledger_id, entry_id)` ordering matches the broker's cursor
                // order within this consumer's partition, and `retain` runs once per
                // cumulative ack — which is coalesced by construction — so the O(map) sweep
                // is negligible.
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    let horizon = ack
                        .message_ids
                        .iter()
                        .map(|id| (id.ledger_id, id.entry_id))
                        .max();
                    if let Some(horizon) = horizon {
                        consumer.batch_ack_tracker.retain(|key, _| *key > horizon);
                    }
                }
                ack.message_ids.iter().map(|m| m.to_pb()).collect()
            };
        let properties: Vec<pb::KeyLongValue> = ack
            .properties
            .iter()
            .map(|(k, v)| pb::KeyLongValue {
                key: k.clone(),
                value: *v as u64,
            })
            .collect();
        let cmd = pb::CommandAck {
            consumer_id: handle.0,
            ack_type: ack.ack_type as i32,
            message_id: pb_ids,
            validation_error: None,
            properties,
            txnid_least_bits: ack.txn_id.map(|t| t.least_sig_bits),
            txnid_most_bits: ack.txn_id.map(|t| t.most_sig_bits),
            request_id: Some(request_id.0),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Ack as i32,
            ack: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::Ack {
                handle,
                enqueued_at: now,
            },
        );
        if let Some(slot) = self.consumers.get(&handle) {
            let mut consumer = slot.state.lock();
            consumer.total_acks_sent = consumer.total_acks_sent.saturating_add(n_ids);
        }
        request_id
    }

    /// Stage an individual ack into this consumer's ack-grouping tracker. The state
    /// machine flushes the tracker once `ack_group_time` has elapsed since the first
    /// staged ack, emitting one coalesced `CommandAck` for the whole batch. Fire-and-
    /// forget: there is no per-call `RequestId` because the broker response will not be
    /// tied to any one ack call. Falls back to an immediate `CommandAck` (synchronous,
    /// allocated `RequestId` is discarded) when no tracker is configured so the message
    /// is never silently dropped. Mirrors Java's `acknowledgmentGroupTime` path.
    pub fn ack_grouped_individual(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        now: Instant,
    ) {
        let actions = self.consumers.get(&handle).and_then(|slot| {
            let mut consumer = slot.state.lock();
            consumer
                .ack_tracker
                .as_mut()
                .map(|t| t.add_individual(message_id, now))
        });
        if let Some(actions) = actions {
            self.dispatch_ack_actions(actions, now);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Individual,
                    properties: Vec::new(),
                    txn_id: None,
                },
                now,
            );
        }
    }

    /// Stage a cumulative ack into this consumer's ack-grouping tracker. See
    /// [`Self::ack_grouped_individual`] for the semantics.
    pub fn ack_grouped_cumulative(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        now: Instant,
    ) {
        let actions = self.consumers.get(&handle).and_then(|slot| {
            let mut consumer = slot.state.lock();
            consumer
                .ack_tracker
                .as_mut()
                .map(|t| t.add_cumulative(message_id, now))
        });
        if let Some(actions) = actions {
            self.dispatch_ack_actions(actions, now);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Cumulative,
                    properties: Vec::new(),
                    txn_id: None,
                },
                now,
            );
        }
    }

    fn dispatch_ack_actions(&mut self, actions: Vec<crate::trackers::AckAction>, now: Instant) {
        for action in actions {
            match action {
                crate::trackers::AckAction::SendIndividualAck {
                    handle,
                    message_ids,
                } => {
                    let _ = self.ack(
                        handle,
                        AckRequest {
                            message_ids,
                            ack_type: pb::command_ack::AckType::Individual,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                        now,
                    );
                }
                crate::trackers::AckAction::SendCumulativeAck { handle, message_id } => {
                    let _ = self.ack(
                        handle,
                        AckRequest {
                            message_ids: vec![message_id],
                            ack_type: pb::command_ack::AckType::Cumulative,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                        now,
                    );
                }
            }
        }
    }

    /// Issue `CommandRedeliverUnacknowledgedMessages` with an empty
    /// `message_ids` list, which the broker treats as "redeliver everything
    /// currently tracked as in-flight for this consumer". Used by the
    /// post-seek resubscribe path: after the cursor reset the broker still
    /// holds the pre-seek `consumerId → unacked` map open, and the dispatcher
    /// will not push fresh entries until those slots free up. Mirrors what
    /// Java's `ConsumerImpl#redeliverUnacknowledgedMessages` does
    /// implicitly on the connection-reset path. Caller is responsible for
    /// only firing this AFTER the matching `SubscribeAcked` so the broker
    /// has the consumer registered (the broker drops the command for an
    /// unknown consumer id without error).
    pub fn redeliver_unacked_all(&mut self, handle: ConsumerHandle) {
        self.emit_redeliver_unacked(handle, Vec::new());
    }

    /// Issue an explicit FLOW for a consumer.
    pub fn flow(&mut self, handle: ConsumerHandle, permits: u32) {
        let cmd = pb::CommandFlow {
            consumer_id: handle.0,
            message_permits: permits,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Flow as i32,
            flow: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
    }

    /// Mark a consumer as paused / resumed. Mirrors Java `Consumer#pause` / `#resume`. While
    /// paused the consumer skips automatic flow refills, so the broker stops dispatching new
    /// messages once already-issued permits drain. Buffered messages remain available via
    /// [`Self::pop_message`].
    pub fn set_paused(&mut self, handle: ConsumerHandle, paused: bool) {
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().paused = paused;
        }
    }

    /// Drain every message the consumer has classified as dead-letter (redelivery count
    /// strictly greater than `max_redeliver_count` at subscribe time). Returns an empty
    /// vec when the consumer is unknown or has no DLQ-flagged messages. Mirrors Java
    /// `ConsumerImpl#getDeadLetterMessages` behavior — the caller is responsible for
    /// republishing them to the configured DLQ topic.
    pub fn drain_dead_letter(&mut self, handle: ConsumerHandle) -> Vec<IncomingMessage> {
        self.consumers
            .get(&handle)
            .map(|slot| std::mem::take(&mut slot.state.lock().dead_letter_pending))
            .unwrap_or_default()
    }

    /// Returns the per-consumer pause flag, or `None` if the consumer handle is unknown.
    #[must_use]
    pub fn is_paused(&self, handle: ConsumerHandle) -> Option<bool> {
        self.consumers
            .get(&handle)
            .map(|slot| slot.state.lock().paused)
    }

    /// Returns `true` once the broker has sent `CommandReachedEndOfTopic` for this
    /// consumer. Mirrors Java `Consumer#hasReachedEndOfTopic`.
    #[must_use]
    pub fn consumer_reached_end_of_topic(&self, handle: ConsumerHandle) -> bool {
        self.consumers
            .get(&handle)
            .map(|slot| slot.state.lock().reached_end_of_topic)
            .unwrap_or(false)
    }

    /// Topic name this consumer is bound to. Returns `None` if the consumer handle is
    /// unknown.
    #[must_use]
    pub fn consumer_topic(&self, handle: ConsumerHandle) -> Option<&str> {
        self.consumers
            .get(&handle)
            .map(|slot| slot.identity.topic.as_str())
    }

    /// Subscription name of this consumer. Returns `None` if the consumer handle is unknown.
    ///
    /// Identity-only read — does not take the per-slot mutex.
    #[must_use]
    pub fn consumer_subscription(&self, handle: ConsumerHandle) -> Option<&str> {
        self.consumers
            .get(&handle)
            .map(|slot| slot.identity.subscription.as_str())
    }

    /// Caller-supplied consumer name advertised at subscribe time. Returns `None` if the
    /// consumer handle is unknown or no name was supplied.
    ///
    /// Returns an owned `String` because `consumer_name` lives behind the
    /// per-slot mutex.
    #[must_use]
    pub fn consumer_name(&self, handle: ConsumerHandle) -> Option<String> {
        self.consumers
            .get(&handle)
            .and_then(|slot| slot.state.lock().consumer_name.clone())
    }

    /// Topic name this producer is bound to. Returns `None` if the producer handle is
    /// unknown.
    ///
    /// Identity-only read — does not take the per-slot mutex.
    #[must_use]
    pub fn producer_topic(&self, handle: ProducerHandle) -> Option<&str> {
        self.producers
            .get(&handle)
            .map(|slot| slot.identity.topic.as_str())
    }

    /// Broker-assigned producer name (set after the CommandProducer / CommandProducerSuccess
    /// round-trip). Returns `None` if the producer handle is unknown or the name has not
    /// arrived yet.
    ///
    /// Returns an owned `String` (rather than `&str`) because the underlying
    /// field is per-slot mutex-guarded mutable state — the borrow cannot
    /// outlive the lock guard.
    #[must_use]
    pub fn producer_name(&self, handle: ProducerHandle) -> Option<String> {
        self.producers
            .get(&handle)
            .and_then(|slot| slot.state.lock().name.clone())
    }

    /// Negatively acknowledge messages — request the broker to redeliver them.
    /// Mirrors `ConsumerImpl#negativeAcknowledge`.
    ///
    /// Empty `message_ids` means "redeliver every unacked message on this consumer"
    /// (Java's `consumer.redeliverUnacknowledgedMessages()`) and is always sent immediately.
    /// Otherwise, if the consumer has a negative-ack tracker configured (via
    /// [`SubscribeRequest::negative_ack_redelivery_delay`]), the supplied ids are deferred
    /// until [`Self::handle_timeout`] notices the delay has elapsed. With no tracker the
    /// redelivery is sent immediately.
    pub fn negative_ack(
        &mut self,
        handle: ConsumerHandle,
        message_ids: Vec<MessageId>,
        now: Instant,
    ) {
        if !message_ids.is_empty() {
            if let Some(slot) = self.consumers.get(&handle) {
                let mut consumer = slot.state.lock();
                // Stop tracking the nacked ids in the ack-timeout (unacked-message)
                // tracker. Without this, an id that was both nacked and ack-timeout
                // tracked is redelivered twice — once by the nack tracker and once by
                // the ack-timeout sweep in [`Self::handle_timeout`] — corrupting
                // at-least-once-without-duplication. Mirrors Java's unconditional
                // `unAckedMessageTracker.remove(...)` in `ConsumerImpl#negativeAcknowledge`
                // (ConsumerImpl.java:859). Kept in its own sequential block so it runs on
                // BOTH the nack-present and nack-absent paths (the early `return` below
                // must not skip it) and avoids a second `&mut consumer` borrow conflicting
                // with the `nack_tracker.as_mut()` scope. Symmetric with the positive-ack
                // path in [`Self::ack`].
                if let Some(t) = consumer.unacked_tracker.as_mut() {
                    for id in &message_ids {
                        t.remove(id);
                    }
                }
                if let Some(tracker) = consumer.nack_tracker.as_mut() {
                    for id in &message_ids {
                        tracker.add(*id, now);
                    }
                    return;
                }
            }
        }
        self.emit_redeliver_unacked(handle, message_ids);
    }

    /// Negative-ack a single message with an explicit per-message delay, bypassing the
    /// consumer's default `negative_ack_redelivery_delay`. Falls back to an immediate
    /// redelivery when the subscription was opened without a nack tracker (so the message
    /// is never silently lost). Mirrors PIP-37's per-message backoff path — the caller
    /// computes `delay` from the message's redelivery count via
    /// [`crate::trackers::nack::MultiplierRedeliveryBackoff::delay_for`].
    pub fn negative_ack_with_delay(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        delay: core::time::Duration,
        now: Instant,
    ) {
        if let Some(slot) = self.consumers.get(&handle) {
            let mut consumer = slot.state.lock();
            // Drop the nacked id from the ack-timeout tracker before deferring it to the
            // nack tracker — same double-redelivery fix as [`Self::negative_ack`], and
            // unconditional (runs even when no nack tracker is configured) so the
            // fall-through to [`Self::emit_redeliver_unacked`] below cannot leave a second
            // redelivery shape. Mirrors `ConsumerImpl.java:859`.
            if let Some(t) = consumer.unacked_tracker.as_mut() {
                t.remove(&message_id);
            }
            if let Some(tracker) = consumer.nack_tracker.as_mut() {
                tracker.add_with_delay(message_id, delay, now);
                return;
            }
        }
        self.emit_redeliver_unacked(handle, vec![message_id]);
    }

    fn emit_redeliver_unacked(&mut self, handle: ConsumerHandle, message_ids: Vec<MessageId>) {
        let pb_ids = message_ids.into_iter().map(MessageId::to_pb).collect();
        let cmd = pb::CommandRedeliverUnacknowledgedMessages {
            consumer_id: handle.0,
            message_ids: pb_ids,
            consumer_epoch: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
            redeliver_unacknowledged_messages: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
    }

    /// Request the broker's last-published message id for the topic this consumer is
    /// subscribed to. Java equivalent: `consumer.getLastMessageId()`. Useful for
    /// "more messages?" checks against the consumer's most-recently-received id (or for
    /// Reader's `hasMessageAvailable()` semantics).
    pub fn get_last_message_id(&mut self, handle: ConsumerHandle) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandGetLastMessageId {
            consumer_id: handle.0,
            request_id: request_id.0,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::GetLastMessageId as i32,
            get_last_message_id: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::ConsumerGetLastMessageId { handle },
        );
        request_id
    }

    /// Issue a seek.
    pub fn seek(&mut self, handle: ConsumerHandle, target: SeekTarget) -> RequestId {
        let request_id = self.alloc_request_id();
        let (message_id, publish_time) = match target {
            SeekTarget::MessageId(mid) => (Some(mid.to_pb()), None),
            SeekTarget::PublishTime(t) => (None, Some(t)),
        };
        let cmd = pb::CommandSeek {
            consumer_id: handle.0,
            request_id: request_id.0,
            message_id,
            message_publish_time: publish_time,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Seek as i32,
            seek: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().begin_seek(request_id);
        }
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerSeek { handle });
        request_id
    }

    /// Issue a fresh topic lookup against this connection. The response
    /// resolves to exactly one terminal [`LookupOutcome`] on the returned
    /// request-id:
    ///
    /// - `Connect` — the topic resolved here; route the data ops.
    /// - `Redirected` — this broker is not the bundle owner. The sans-io core does **not** dial the
    ///   redirect target itself (ADR-0004); the engine dials it and re-issues the lookup there via
    ///   [`Self::lookup_redirect`]. The outcome carries the target URL, the next-hop
    ///   `authoritative` flag, and the remaining hop budget.
    /// - `Failed` — the lookup failed (including the synthetic cap-exhausted `Failed`).
    ///
    /// Redirects are capped at [`crate::lookup::MAX_LOOKUP_REDIRECTS`] hops
    /// (Java parity). If [`ConnectionConfig::max_pending_lookups`] is set
    /// and the in-flight registry is already at the cap, the call surfaces
    /// synchronously as a synthetic `LookupOutcome::Failed { code: 0,
    /// message: "lookup rejected: max pending" }` against the freshly
    /// allocated request-id — the frame never touches the wire.
    pub fn lookup(&mut self, topic: &str, authoritative: bool) -> RequestId {
        self.lookup_with_budget(topic, authoritative, crate::lookup::MAX_LOOKUP_REDIRECTS)
    }

    /// Re-issue a lookup on a **redirect target** connection after the engine
    /// dialed the broker advertised by a [`LookupOutcome::Redirected`].
    ///
    /// `hops_remaining` is the budget the previous hop's `Redirected` outcome
    /// carried out to the engine. It is clamped to
    /// [`crate::lookup::MAX_LOOKUP_REDIRECTS`] here — the proto-side floor
    /// check — so a buggy or hostile engine that inflates the budget cannot
    /// re-open the redirect-loop DoS the cap closes. The lookup is otherwise
    /// identical to [`Self::lookup`]: it resolves to one terminal outcome on
    /// the returned request-id (which, like every lookup, is its own
    /// `chain_origin`).
    pub fn lookup_redirect(
        &mut self,
        topic: &str,
        authoritative: bool,
        hops_remaining: u8,
    ) -> RequestId {
        // Proto-side floor: never trust an engine-supplied budget above the
        // cap. The translate layer still short-circuits to `Failed` at zero,
        // so the cap holds end-to-end regardless of engine behaviour.
        let clamped = hops_remaining.min(crate::lookup::MAX_LOOKUP_REDIRECTS);
        self.lookup_with_budget(topic, authoritative, clamped)
    }

    /// Shared body of [`Self::lookup`] / [`Self::lookup_redirect`]: allocate a
    /// request-id, build a [`LookupRequest`] seeded with `hops_remaining`, and
    /// submit it (or synthesize a `Failed` if the pending-lookup cap is full).
    fn lookup_with_budget(
        &mut self,
        topic: &str,
        authoritative: bool,
        hops_remaining: u8,
    ) -> RequestId {
        let request_id = self.alloc_request_id();
        let req = LookupRequest {
            topic: topic.to_owned(),
            authoritative,
            hops_remaining,
            // The request-id IS the anchor — each lookup is single-hop on its
            // connection and delivers its terminal outcome here.
            chain_origin: request_id,
        };
        if matches!(
            self.send_lookup_internal(request_id, req),
            Err(LookupSubmitError::Rejected),
        ) {
            self.synthesize_lookup_failed(
                request_id,
                "lookup rejected: max pending (ConnectionConfig::max_pending_lookups)",
            );
        }
        request_id
    }

    fn send_lookup_internal(
        &mut self,
        request_id: RequestId,
        req: LookupRequest,
    ) -> Result<(), LookupSubmitError> {
        // Check the cap BEFORE building / encoding so a hostile broker cannot
        // make us pay encode cost on rejected hops. The encode_command path
        // already enforces the connection-wide outbound buffer cap.
        let cmd = pb::CommandLookupTopic {
            topic: req.topic.clone(),
            request_id: request_id.0,
            authoritative: Some(req.authoritative),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            advertised_listener_name: None,
            properties: Vec::new(),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Lookup as i32,
            lookup_topic: Some(cmd),
            ..Default::default()
        };
        // Reserve a slot in the registry first; on capacity exhaustion we
        // refuse to encode the frame.
        self.lookup
            .insert_lookup(request_id, req)
            .map_err(|_| LookupSubmitError::Rejected)?;
        self.encode_command(&base)
            .map_err(|_| LookupSubmitError::Encode)?;
        self.pending_requests
            .insert(request_id, PendingRequestKind::Lookup);
        Ok(())
    }

    /// Write a synthetic `LookupOutcome::Failed { code: 0, message }`
    /// outcome on `request_id` and wake the registered waker, without ever
    /// emitting a `CommandLookupTopic` frame. Used when the cap kicks in
    /// (either at the public entry point or on a redirect retry) so the
    /// engine sees a clean terminal outcome rather than an indefinite
    /// pending lookup.
    fn synthesize_lookup_failed(&mut self, request_id: RequestId, message: &str) {
        let outcome = LookupOutcome::Failed {
            code: 0,
            message: message.to_owned(),
        };
        self.pending_requests.remove(&request_id);
        self.outcomes.insert(
            PendingOpKey::Request(request_id),
            OpOutcome::LookupResponse {
                request_id,
                outcome: outcome.clone(),
            },
        );
        self.wake_for_request(request_id);
        self.events.push_back(ConnectionEvent::LookupResponse {
            request_id,
            result: outcome,
        });
    }

    /// Issue a `CommandGetSchema` to look up the schema declared for `topic` in the broker's
    /// schema registry.
    ///
    /// Mirrors Java `PulsarClientImpl#getSchema` and the `LookupService#getSchema` round-trip.
    /// The state machine surfaces the response via [`OpOutcome::GetSchemaResponse`] and
    /// [`ConnectionEvent::GetSchemaResponse`].
    ///
    /// `version` is the requested schema version when known (e.g. when re-decoding a historical
    /// payload). Pass `None` to ask the broker for the topic's current schema.
    ///
    /// Used by [`crate::schema::AutoConsumeSchema`] and
    /// [`crate::schema::AutoProduceBytesSchema`] to populate their per-instance schema cache
    /// (PIP-87 broker-side schema lookup).
    pub fn get_schema(&mut self, topic: &str, version: Option<Bytes>) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandGetSchema {
            request_id: request_id.0,
            topic: topic.to_owned(),
            schema_version: version,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::GetSchema as i32,
            get_schema: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::GetSchema);
        request_id
    }

    /// Request partitioned-topic metadata.
    ///
    /// # Fast-path for per-partition child names
    ///
    /// If `topic` already encodes a partition index per Java's
    /// `TopicName#isPartitioned` (i.e. its tail matches `-partition-<N>`
    /// where `<N>` is a non-negative `u32`), the call short-circuits to
    /// a synthetic `OpOutcome::PartitionedMetadata { partitions: 0,
    /// error: None }` without touching the wire. Mirrors Java's
    /// `PulsarClientImpl#getPartitionsForTopic` early-return and the
    /// streamnative-pulsar-rs #327 service-discovery fix. For a topic
    /// with `N` partitions, this cuts the per-partition LOOKUP
    /// amplification from `N+1` round-trips to `1` and reduces load on
    /// the broker's metadata store (ZooKeeper / etcd). Complements the
    /// F1 hardening pass (redirect cap + pending-lookup cap).
    ///
    /// The detection uses [`crate::lookup::is_partition_topic`] — strict
    /// end-of-string `-partition-\d+` match, not the looser
    /// `contains("-partition-")` from the streamnative patch which
    /// false-positives on names like `my-partition-thing-3`.
    ///
    /// # Max-pending cap
    ///
    /// Subject to the same `max_pending_lookups` cap as [`Self::lookup`]:
    /// if the registry is already full, the call surfaces synchronously as
    /// a synthetic `PartitionedMetadata { error: Some((0, "max pending"))
    /// }` outcome — the frame never touches the wire. The fast-path
    /// above bypasses the cap because no registry slot is consumed.
    pub fn get_partitioned_topic_metadata(&mut self, topic: &str) -> RequestId {
        let request_id = self.alloc_request_id();
        // Fast-path: the input is already a per-partition child name —
        // synthesize partitions=0 immediately. No registry slot, no
        // outbound frame, no broker round-trip. Mirrors Java's
        // `TopicName#getPartitionedTopicName` early-return when the name
        // is already partitioned.
        if is_partition_topic(topic) {
            self.synthesize_partitioned_metadata_outcome(request_id, 0, None);
            return request_id;
        }
        // Reserve the slot before encoding so we can short-circuit the
        // outbound frame when the cap is hit.
        if self.lookup.insert_partition(request_id).is_err() {
            self.synthesize_partitioned_metadata_outcome(
                request_id,
                0,
                Some((
                    0,
                    "partitioned-metadata rejected: max pending \
                     (ConnectionConfig::max_pending_lookups)"
                        .to_owned(),
                )),
            );
            return request_id;
        }
        let cmd = pb::CommandPartitionedTopicMetadata {
            topic: topic.to_owned(),
            request_id: request_id.0,
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            metadata_auto_creation_enabled: Some(true),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::PartitionedMetadata as i32,
            partition_metadata: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::PartitionedMetadata);
        request_id
    }

    /// Write a synthetic `OpOutcome::PartitionedMetadata` on `request_id`
    /// and wake the registered waker, without ever emitting a
    /// `CommandPartitionedTopicMetadata` frame. Used by:
    ///
    /// * the partition-topic fast-path (success outcome, `partitions = 0`, `error = None`),
    /// * the `max_pending_lookups` cap rejection (failure outcome, `error = Some((0, "max pending
    ///   …"))`).
    ///
    /// Mirror of [`Self::synthesize_lookup_failed`] for the
    /// partition-metadata path, generalised to handle both the success
    /// and failure synthetic outcomes the public entry point needs.
    fn synthesize_partitioned_metadata_outcome(
        &mut self,
        request_id: RequestId,
        partitions: u32,
        error: Option<(i32, String)>,
    ) {
        self.pending_requests.remove(&request_id);
        self.outcomes.insert(
            PendingOpKey::Request(request_id),
            OpOutcome::PartitionedMetadata {
                request_id,
                partitions,
                error: error.clone(),
            },
        );
        self.wake_for_request(request_id);
        self.events
            .push_back(ConnectionEvent::PartitionedMetadataResponse {
                request_id,
                partitions,
                error,
            });
    }

    /// Start a topic-list watcher (PIP-145).
    pub fn watch_topic_list(&mut self, namespace: &str, pattern: &str) -> RequestId {
        let request_id = self.alloc_request_id();
        let watcher_id = self.next_watcher_id;
        self.next_watcher_id = self.next_watcher_id.wrapping_add(1);
        let cmd = pb::CommandWatchTopicList {
            request_id: request_id.0,
            watcher_id,
            namespace: namespace.to_owned(),
            topics_pattern: pattern.to_owned(),
            topics_hash: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicList as i32,
            watch_topic_list: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.topic_watchers.insert(
            watcher_id,
            request_id,
            TopicWatcher {
                pattern: pattern.to_owned(),
                namespace: namespace.to_owned(),
                topics_hash: None,
                initialised: false,
            },
        );
        self.pending_requests
            .insert(request_id, PendingRequestKind::TopicWatcher { watcher_id });
        request_id
    }

    /// Close a producer. The caller is expected to await the broker ack via
    /// a `RequestFut`-style waiter that drains the recorded [`OpOutcome`]
    /// with [`Self::take_outcome`].
    pub fn close_producer(&mut self, handle: ProducerHandle) -> RequestId {
        self.close_producer_inner(handle, false)
    }

    /// Fire-and-forget variant of [`Self::close_producer`] for the engines'
    /// last-clone drop guard: no waiter will ever drain the broker ack, so
    /// the request is registered as
    /// `PendingRequestKind::ProducerCloseForgotten` and the
    /// `Success`/`Error` handlers consume the ack in-place instead of
    /// recording an [`OpOutcome`] (which would leak one permanent entry per
    /// dropped producer). A broker rejection is surfaced as a `warn!`
    /// (ADR-0054) rather than silently swallowed.
    pub fn close_producer_forget(&mut self, handle: ProducerHandle) -> RequestId {
        self.close_producer_inner(handle, true)
    }

    fn close_producer_inner(&mut self, handle: ProducerHandle, forget: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandCloseProducer {
            producer_id: handle.0,
            request_id: request_id.0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseProducer as i32,
            close_producer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(slot) = self.producers.get(&handle) {
            slot.state.lock().close();
        }
        let kind = if forget {
            PendingRequestKind::ProducerCloseForgotten { handle }
        } else {
            PendingRequestKind::ProducerClose { handle }
        };
        self.pending_requests.insert(request_id, kind);
        request_id
    }

    /// Close a consumer. The caller is expected to await the broker ack via
    /// a `RequestFut`-style waiter that drains the recorded [`OpOutcome`]
    /// with [`Self::take_outcome`].
    ///
    /// `now` (ADR-0011 clock injection) is only consumed when this consumer
    /// has a non-empty ack-grouping tracker to flush — the flush routes
    /// through [`Self::ack`], which stamps the flushed `CommandAck`'s
    /// `enqueued_at` for the `ack_response_timeout` backstop (issue #346).
    pub fn close_consumer(&mut self, handle: ConsumerHandle, now: Instant) -> RequestId {
        self.close_consumer_inner(handle, false, now)
    }

    /// Fire-and-forget variant of [`Self::close_consumer`] for the engines'
    /// last-clone drop guard. The broker ack is consumed in-place because no
    /// waiter exists to drain it.
    pub fn close_consumer_forget(&mut self, handle: ConsumerHandle, now: Instant) -> RequestId {
        self.close_consumer_inner(handle, true, now)
    }

    fn close_consumer_inner(
        &mut self,
        handle: ConsumerHandle,
        forget: bool,
        now: Instant,
    ) -> RequestId {
        let ack_actions = self.consumers.get(&handle).and_then(|slot| {
            slot.state
                .lock()
                .ack_tracker
                .as_mut()
                .map(crate::trackers::AckGroupingTracker::flush)
        });
        if let Some(actions) = ack_actions {
            self.dispatch_ack_actions(actions, now);
        }

        let request_id = self.alloc_request_id();
        let cmd = pb::CommandCloseConsumer {
            consumer_id: handle.0,
            request_id: request_id.0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().close();
        }
        let kind = if forget {
            PendingRequestKind::ConsumerCloseForgotten { handle }
        } else {
            PendingRequestKind::ConsumerClose { handle }
        };
        self.pending_requests.insert(request_id, kind);
        request_id
    }

    /// Unsubscribe — remove this consumer's subscription from the broker.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#unsubscribe`. Unlike
    /// [`close_consumer`](Self::close_consumer) which keeps the subscription
    /// cursor alive on the broker, `unsubscribe` deletes the subscription
    /// entirely — useful for tear-down + cleanup. A successful broker reply
    /// closes and removes the local handle even when the runtime waiter was
    /// cancelled; a rejection restores the suspended attachment generation.
    /// `force=true` (PIP-313) drops the subscription even if other consumers
    /// are still attached.
    pub fn unsubscribe(&mut self, handle: ConsumerHandle, force: bool) -> RequestId {
        if let Some(request_id) = self
            .consumers
            .get(&handle)
            .and_then(|slot| slot.state.lock().unsubscribe_request_id)
        {
            return request_id;
        }
        self.stage_unsubscribe(handle, force)
    }

    /// Stage an unsubscribe unless one is already in flight for this handle.
    ///
    /// Built-in runtimes use this idempotent admission gate so overlapping
    /// user futures cannot both claim ownership of the same broker operation.
    pub fn try_unsubscribe(&mut self, handle: ConsumerHandle, force: bool) -> Option<RequestId> {
        if self
            .consumers
            .get(&handle)
            .is_some_and(|slot| slot.state.lock().unsubscribe_request_id.is_some())
        {
            return None;
        }
        Some(self.stage_unsubscribe(handle, force))
    }

    fn stage_unsubscribe(&mut self, handle: ConsumerHandle, force: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().unsubscribe_request_id = Some(request_id);
        }
        self.events.retain(|event| {
            !matches!(
                event,
                ConnectionEvent::SubscribeFailedTransient { handle: event_handle, .. }
                    if *event_handle == handle
            )
        });
        self.driver_retries.retain(|retry| {
            !matches!(retry, crate::DriverRetry::Consumer { handle: event_handle, .. } if *event_handle == handle)
        });
        let cmd = pb::CommandUnsubscribe {
            consumer_id: handle.0,
            request_id: request_id.0,
            force: Some(force),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Unsubscribe as i32,
            unsubscribe: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::ConsumerUnsubscribe { handle },
        );
        request_id
    }

    /// Restore a consumer after the broker rejects an unsubscribe request.
    ///
    /// Staging `CommandUnsubscribe` suspends the active subscribe generation so
    /// no detached retry can enqueue `CommandSubscribe` behind it. Java returns
    /// a failed unsubscribe future to `Ready`; equivalently, this clears that
    /// suspension and re-emits a previously failed established attachment when
    /// the connection is still live.
    fn resume_consumer_after_unsubscribe_failure(
        &mut self,
        handle: ConsumerHandle,
        unsubscribe_request_id: RequestId,
    ) -> Option<RequestId> {
        let failed_request_id = {
            let slot = self.consumers.get(&handle)?;
            let mut consumer = slot.state.lock();
            if consumer.unsubscribe_request_id != Some(unsubscribe_request_id) || consumer.closed {
                return None;
            }
            consumer.unsubscribe_request_id = None;
            consumer.last_subscribe_error.as_ref()?;
            consumer
                .subscribe_waiter_request
                .or(consumer.flow_on_subscribe_ack_request)
        }?;
        if !self.is_connected() {
            return None;
        }
        self.retry_consumer_subscribe_if_current(handle, failed_request_id)
    }

    fn complete_consumer_unsubscribe(
        &mut self,
        handle: ConsumerHandle,
        unsubscribe_request_id: RequestId,
    ) {
        let current = self.consumers.get(&handle).is_some_and(|slot| {
            let mut consumer = slot.state.lock();
            if consumer.unsubscribe_request_id != Some(unsubscribe_request_id) {
                return false;
            }
            consumer.unsubscribe_request_id = None;
            consumer.close();
            true
        });
        if current {
            self.cancel_consumer_subscribe(handle);
        }
    }

    fn clear_consumer_unsubscribe(
        &mut self,
        handle: ConsumerHandle,
        unsubscribe_request_id: RequestId,
    ) {
        if let Some(slot) = self.consumers.get(&handle) {
            let mut consumer = slot.state.lock();
            if consumer.unsubscribe_request_id == Some(unsubscribe_request_id) {
                consumer.unsubscribe_request_id = None;
            }
        }
    }

    /// Mutable accessor for the embedded [`TxnClient`].
    ///
    /// Drivers needing to register a waker against a pending TC request (`new_txn`,
    /// `add_partition_to_txn`, …) reach in via this accessor — the [`Connection`] otherwise
    /// owns and drives the client.
    pub fn txn_client_mut(&mut self) -> &mut TxnClient {
        &mut self.txn_client
    }

    /// Read-only accessor for the embedded [`TxnClient`].
    pub fn txn_client(&self) -> &TxnClient {
        &self.txn_client
    }

    /// Issue a `CommandTcClientConnectRequest` for the given TC partition (`tc_id`). Pulsar's
    /// broker only loads the per-partition transaction-metadata store on demand; without this
    /// handshake, the first `CommandNewTxn` lands while `TransactionMetadataStoreService.stores
    /// .get(tcId)` is still `null` and the broker replies `TransactionCoordinatorNotFound`.
    ///
    /// The matching response surfaces as [`OpOutcome::Success`] (on `ServerError::None`) or
    /// [`OpOutcome::Error`] (with the broker-supplied code + message) and is consumed via
    /// [`Self::take_outcome`]. Mirrors Java
    /// `TransactionMetaStoreHandler.connectionOpened` →
    /// `Commands.newTcClientConnectRequest`.
    pub fn tc_client_connect(&mut self, tc_id: u64) -> RequestId {
        let request_id = self.alloc_request_id();
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::TcClientConnectRequest as i32,
            tc_client_connect_request: Some(pb::CommandTcClientConnectRequest {
                request_id: request_id.0,
                tc_id,
                // PIP-473 scalable transaction coordinator. Absent = the legacy coordinator.
                scalable: None,
            }),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::TcClientConnect);
        request_id
    }

    /// Open a new transaction at the broker-side transaction coordinator. Returns the request
    /// id; the matching [`OpOutcome::NewTxn`] is consumed via [`Self::take_outcome`].
    pub fn new_txn(&mut self, timeout: Duration) -> RequestId {
        let request_id = self.alloc_request_id();
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let cmd = self.txn_client.new_txn(request_id.0, timeout_ms);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxn as i32,
            new_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::NewTxn);
        request_id
    }

    /// Register `topic` as a partition that the transaction will write to. Returns the request
    /// id; the matching [`OpOutcome::AddPartitionToTxn`] is consumed via [`Self::take_outcome`].
    pub fn add_partition_to_txn(&mut self, txn: TxnId, topic: String) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self.txn_client.add_partition(request_id.0, txn, topic);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AddPartitionToTxn as i32,
            add_partition_to_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::AddPartitionToTxn);
        request_id
    }

    /// Register `(subscription, topic)` as a subscription the transaction will acknowledge on.
    /// Returns the request id; the matching [`OpOutcome::AddSubscriptionToTxn`] is consumed via
    /// [`Self::take_outcome`].
    pub fn add_subscription_to_txn(
        &mut self,
        txn: TxnId,
        subscription: String,
        topic: String,
    ) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self
            .txn_client
            .add_subscription(request_id.0, txn, subscription, topic);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AddSubscriptionToTxn as i32,
            add_subscription_to_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::AddSubscriptionToTxn);
        request_id
    }

    /// Commit or abort the transaction. Returns the request id; the matching
    /// [`OpOutcome::EndTxn`] is consumed via [`Self::take_outcome`] once the broker replies.
    pub fn end_txn(&mut self, txn: TxnId, action: TxnAction) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self.txn_client.end_txn(request_id.0, txn, action);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::EndTxn as i32,
            end_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::EndTxn);
        request_id
    }

    /// Close the whole connection.
    pub fn close(&mut self) {
        if matches!(
            self.state,
            HandshakeState::Connected | HandshakeState::AuthChallenging
        ) {
            self.last_disconnected_at = Some((self.wall_clock)());
        }
        self.set_handshake_state(HandshakeState::Closing);
        self.events
            .push_back(ConnectionEvent::Closed { reason: None });
    }

    /// Submit a `CommandAuthResponse` in answer to a server `CommandAuthChallenge`.
    pub fn submit_auth_response(&mut self, auth_data: Bytes, auth_method: Option<String>) {
        let resp = pb::CommandAuthResponse {
            client_version: Some(self.config.client_version.clone()),
            response: Some(pb::AuthData {
                auth_method_name: auth_method,
                auth_data: Some(auth_data),
            }),
            protocol_version: Some(self.config.protocol_version),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AuthResponse as i32,
            auth_response: Some(resp),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if self.state == HandshakeState::AuthChallenging {
            self.set_handshake_state(HandshakeState::Connected);
        }
    }

    /// Access a producer's slot — useful in tests + driver instrumentation.
    /// Returns the `Arc<ProducerSlot>` so callers can take `.state.lock()`
    /// to read or mutate the per-producer state machine. Lock-ordering:
    /// **global Connection mutex → per-slot mutex, never the reverse**
    /// (see ADR-0038).
    pub fn producer(
        &self,
        handle: ProducerHandle,
    ) -> Option<&std::sync::Arc<crate::producer::ProducerSlot>> {
        self.producers.get(&handle)
    }

    /// Access a producer's slot for mutation — returns the same `Arc<ProducerSlot>` as
    /// [`Self::producer`]; the per-slot mutex provides interior mutability.
    /// Retained as a separate method for source-compat with the pre-split call sites.
    pub fn producer_mut(
        &mut self,
        handle: ProducerHandle,
    ) -> Option<&std::sync::Arc<crate::producer::ProducerSlot>> {
        self.producers.get(&handle)
    }

    /// Access a consumer's slot — returns the `Arc<ConsumerSlot>` so callers
    /// can take `.state.lock()` to read or mutate per-consumer state. See
    /// [`Self::producer`] for the symmetric API rationale.
    pub fn consumer(
        &self,
        handle: ConsumerHandle,
    ) -> Option<&std::sync::Arc<crate::consumer::ConsumerSlot>> {
        self.consumers.get(&handle)
    }

    /// Mutable access to a consumer's slot — returns the same `Arc<ConsumerSlot>` as
    /// [`Self::consumer`]; the per-slot mutex provides interior mutability.
    pub fn consumer_mut(
        &mut self,
        handle: ConsumerHandle,
    ) -> Option<&std::sync::Arc<crate::consumer::ConsumerSlot>> {
        self.consumers.get(&handle)
    }

    /// Number of bytes pending transmit.
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    /// Payload size (post-decompression / post-decryption — payload as it sits in the
    /// queue, which is the bytes the runtime layer will hand to user code) of the next
    /// message that [`Self::pop_message`] would return. Returns `None` for unknown
    /// handles or empty queues. Lets the runtime peek before committing to a pop —
    /// useful for size-capped batch receive (Java `BatchReceivePolicy.maxNumBytes`).
    #[must_use]
    pub fn peek_message_payload_size(&self, handle: ConsumerHandle) -> Option<usize> {
        self.consumers
            .get(&handle)
            .and_then(|slot| slot.state.lock().queue.front().map(|m| m.payload.len()))
    }

    /// Register a per-consumer receive waker. Returns `Some(slab_key)` if the
    /// consumer is alive (the caller MUST evict the slot via
    /// [`Self::cancel_consumer_receive_waker`] on drop), or `None` if the
    /// consumer has been closed in the meantime.
    ///
    /// This is the per-consumer waker slab the runtime crates park
    /// `receive()` futures on. Multiple in-flight receives on the same
    /// consumer get independent slab slots and all fan out on message arrival
    /// (see [`ConsumerState::register_receive_waker`]).
    pub fn register_consumer_receive_waker(
        &mut self,
        handle: ConsumerHandle,
        waker: Waker,
    ) -> Option<usize> {
        let slot = self.consumers.get(&handle)?;
        Some(slot.state.lock().register_receive_waker(waker))
    }

    /// Evict a previously-registered per-consumer receive waker. Idempotent —
    /// safe to call from a `Drop` impl even if the consumer has been removed
    /// or the slot already drained.
    pub fn cancel_consumer_receive_waker(&mut self, handle: ConsumerHandle, slab_key: usize) {
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().cancel_receive_waker(slab_key);
        }
    }

    /// Register a per-consumer active-change waker (issue #348). Returns
    /// `Some(slab_key)` if the consumer is alive (the caller MUST evict the
    /// slot via [`Self::cancel_consumer_active_change_waker`] on drop), or
    /// `None` if the consumer has been closed in the meantime. Mirrors
    /// [`Self::register_consumer_receive_waker`].
    pub fn register_consumer_active_change_waker(
        &mut self,
        handle: ConsumerHandle,
        waker: Waker,
    ) -> Option<usize> {
        let slot = self.consumers.get(&handle)?;
        Some(slot.state.lock().register_active_change_waker(waker))
    }

    /// Evict a previously-registered per-consumer active-change waker.
    /// Idempotent — safe to call from a `Drop` impl even if the consumer has
    /// been removed or the slot already drained. Mirrors
    /// [`Self::cancel_consumer_receive_waker`].
    pub fn cancel_consumer_active_change_waker(&mut self, handle: ConsumerHandle, slab_key: usize) {
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().cancel_active_change_waker(slab_key);
        }
    }

    /// Pop the oldest not-yet-observed active-change transition for `handle`
    /// (issue #348). Returns `None` for an unknown handle or an empty ring.
    /// Mirrors [`Self::pop_message`].
    pub fn pop_consumer_active_change(&mut self, handle: ConsumerHandle) -> Option<bool> {
        self.consumers
            .get(&handle)?
            .state
            .lock()
            .pop_active_change()
    }

    /// Drain a single message from the given consumer's queue, stamping its receive latency
    /// against the engine-injected `now` (ADR-0011, ADR-0086).
    pub fn pop_message(&mut self, handle: ConsumerHandle, now: Instant) -> Option<IncomingMessage> {
        let (msg, flow_cmd) = {
            let slot = self.consumers.get(&handle)?;
            let mut consumer = slot.state.lock();
            let msg = consumer.pop_message(now);
            // After popping, opportunistically check whether we owe the broker a FLOW.
            let flow_cmd = consumer.maybe_flow();
            (msg, flow_cmd)
        };
        if let Some(flow_cmd) = flow_cmd {
            let base = pb::BaseCommand {
                r#type: pb::base_command::Type::Flow as i32,
                flow: Some(flow_cmd),
                ..Default::default()
            };
            let _ = self.encode_command(&base);
        }
        msg
    }

    fn alloc_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        RequestId(id)
    }

    // -------------------------------------------------------------------
    // PIP-460 scalable topics (ADR-0093). The wire commands are ordinary
    // `BaseCommand` fields in the vendored proto, so they encode through
    // `encode_command` and decode through `decode_one` like every other
    // command — there is no separate envelope and no frame interception.
    // -------------------------------------------------------------------

    /// **Experimental** (PIP-460, ADR-0093). Whether the connected broker
    /// advertised `supports_scalable_topics` in its `CommandConnected`
    /// feature flags.
    ///
    /// This is the v4-compatibility gate: a Pulsar 4.x broker leaves the flag
    /// absent, and the client must not emit a scalable-topic command it cannot
    /// parse. Meaningful only after the handshake completes; `false` before.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn broker_supports_scalable_topics(&self) -> bool {
        self.feature_flags.supports_scalable_topics.unwrap_or(false)
    }

    /// **Experimental** (PIP-460, ADR-0093). Open a scalable-topic session for
    /// `topic`, emitting `CommandScalableTopicLookup`.
    ///
    /// Upstream folds lookup and DAG-watch subscribe into this one command: the
    /// client allocates the `session_id`, and the broker replies with a
    /// `CommandScalableTopicUpdate` carrying the initial layout and then keeps
    /// pushing updates on the same session until
    /// [`Self::close_scalable_topic_session`]. The returned id correlates the
    /// resulting [`ConnectionEvent::ScalableTopicLookupResolved`] and every
    /// later [`ConnectionEvent::SegmentDagUpdated`].
    ///
    /// `topic` may be a `topic://`, a `persistent://`, or a short name; the
    /// broker normalises it and returns the canonical identity in the update.
    ///
    /// # Errors
    ///
    /// [`crate::dag_watch::ScalableTopicError::BrokerUnsupported`] when the peer did not
    /// advertise `supports_scalable_topics`. Nothing is written to the outbound
    /// buffer in that case.
    #[cfg(feature = "scalable-topics")]
    pub fn open_scalable_topic_session(
        &mut self,
        topic: &str,
    ) -> Result<u64, crate::dag_watch::ScalableTopicError> {
        if !self.broker_supports_scalable_topics() {
            return Err(crate::dag_watch::ScalableTopicError::BrokerUnsupported);
        }
        let session_id = self.next_scalable_session_id;
        self.next_scalable_session_id = self.next_scalable_session_id.wrapping_add(1);
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicLookup as i32,
            scalable_topic_lookup: Some(pb::CommandScalableTopicLookup {
                session_id,
                topic: topic.to_owned(),
            }),
            ..Default::default()
        };
        let _ = self.encode_command(&cmd);
        self.scalable_sessions.insert(
            session_id,
            crate::dag_watch::DagWatchSession::new(session_id),
        );
        Ok(session_id)
    }

    /// **Experimental** (PIP-460, ADR-0093). Close a scalable-topic session,
    /// emitting `CommandScalableTopicClose` and dropping the session state.
    ///
    /// A no-op for an id this connection does not track, so a double close is
    /// harmless.
    #[cfg(feature = "scalable-topics")]
    pub fn close_scalable_topic_session(&mut self, session_id: u64) {
        if self.scalable_sessions.remove(&session_id).is_none() {
            return;
        }
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicClose as i32,
            scalable_topic_close: Some(pb::CommandScalableTopicClose { session_id }),
            ..Default::default()
        };
        let _ = self.encode_command(&cmd);
        self.events.push_back(ConnectionEvent::DagWatchClosed {
            session_id,
            reason: Some("client-initiated close".to_owned()),
        });
    }

    /// Snapshot the current DAG for a session (for the CLI `topic-info`
    /// and tests). `None` if no session with that id is open.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn dag_snapshot(&self, session_id: u64) -> Option<Vec<crate::types::SegmentDescriptor>> {
        self.scalable_sessions
            .get(&session_id)
            .map(crate::dag_watch::DagWatchSession::snapshot)
    }

    /// The canonical `topic://...` identity the broker resolved a session to,
    /// once its first update has landed.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn scalable_resolved_topic_name(&self, session_id: u64) -> Option<&str> {
        self.scalable_sessions
            .get(&session_id)
            .and_then(crate::dag_watch::DagWatchSession::resolved_topic_name)
    }

    /// Apply one inbound `CommandScalableTopicUpdate` to its session.
    ///
    /// The update is both the reply to the lookup and every subsequent pushed
    /// layout, so the first one that lands on a session emits
    /// [`ConnectionEvent::ScalableTopicLookupResolved`] and later ones emit
    /// [`ConnectionEvent::SegmentDagUpdated`].
    #[cfg(feature = "scalable-topics")]
    fn handle_scalable_topic_update(&mut self, upd: pb::CommandScalableTopicUpdate) {
        let session_id = upd.session_id;
        let Some(session) = self.scalable_sessions.get_mut(&session_id) else {
            // Update for an unknown session — drop silently (a stale frame
            // after a close, mirroring the lookup-registry one-shot guard).
            return;
        };
        let first_layout = !session.is_resolved();
        match session.handle_update(&upd) {
            Ok(delta) => {
                if first_layout {
                    let segments = session.snapshot();
                    let controller_broker_url =
                        session.controller_broker_url().map(ToOwned::to_owned);
                    let resolved_topic_name = session.resolved_topic_name().map(ToOwned::to_owned);
                    self.events
                        .push_back(ConnectionEvent::ScalableTopicLookupResolved {
                            session_id,
                            resolved_topic_name,
                            controller_broker_url,
                            segments,
                            epoch: delta.epoch,
                        });
                    return;
                }
                let consume_affecting = delta.is_consume_affecting();
                let reason = delta.change_reason();
                self.events
                    .push_back(ConnectionEvent::SegmentDagUpdated { session_id, delta });
                if consume_affecting {
                    self.events
                        .push_back(ConnectionEvent::DagChangedDuringConsume { session_id, reason });
                }
            }
            Err(err) => {
                // A rejected update closes the session (drop-on-change). The
                // runtime re-resolves.
                self.scalable_sessions.remove(&session_id);
                self.events.push_back(ConnectionEvent::DagWatchClosed {
                    session_id,
                    reason: Some(format!("scalable-topic update rejected: {err}")),
                });
            }
        }
    }

    /// Whether this failed wire request still owns the producer's active open
    /// generation.
    ///
    /// Delayed driver retry legs use this as their liveness check before and
    /// after lookup. A reconnect rebuild or newer retry replaces the active
    /// request id, making older legs harmless no-ops.
    #[must_use]
    pub fn producer_open_retry_is_current(
        &self,
        handle: ProducerHandle,
        failed_request_id: RequestId,
    ) -> bool {
        self.producer_create_requests.contains_key(&handle)
            && self.producers.get(&handle).is_some_and(|slot| {
                let producer = slot.state.lock();
                !producer.closed && producer.open_request_id == Some(failed_request_id)
            })
    }

    /// Re-emit `CommandProducer` for a single producer handle.
    ///
    /// This preserves the original low-level API, whose caller owns generation
    /// coordination. Built-in asynchronous runtimes use
    /// [`Self::retry_producer_open_if_current`] instead.
    pub fn retry_producer_open(&mut self, handle: ProducerHandle) -> Option<RequestId> {
        self.retry_producer_open_inner(handle)
    }

    /// Re-emit `CommandProducer` only if `failed_request_id` still owns the
    /// producer's active open generation (ADR-0080).
    ///
    /// The failed request id prevents a delayed retry leg from superseding a
    /// newer reconnect rebuild or retry. The full [`Self::rebuild_producers`]
    /// sweep would re-emit `CommandProducer` for every still-open producer;
    /// this targeted variant is cheaper and avoids stepping on producers that
    /// are already successfully reattached on this session. It bumps `epoch`
    /// so the broker associates the new attachment with a strictly newer
    /// generation and returns the new request id, or `None` when the failed
    /// generation is no longer current.
    pub fn retry_producer_open_if_current(
        &mut self,
        handle: ProducerHandle,
        failed_request_id: RequestId,
    ) -> Option<RequestId> {
        if !self.producer_open_retry_is_current(handle, failed_request_id) {
            return None;
        }
        self.retry_producer_open_inner(handle)
    }

    fn retry_producer_open_inner(&mut self, handle: ProducerHandle) -> Option<RequestId> {
        let req = self.producer_create_requests.get(&handle)?.clone();
        {
            let slot = self.producers.get(&handle)?;
            let mut p = slot.state.lock();
            if p.closed {
                return None;
            }
            p.epoch = p.epoch.saturating_add(1);
        }
        let request_id = self.emit_command_producer(handle, &req);
        // Pending `OpSend`s from the transient window had their wire frames written and
        // silently dropped by the broker (Pulsar discards `CommandSend` for an unknown
        // `producer_id` without an error). Their replay is DEFERRED to the
        // `ProducerSuccess` handler (`replay_pending_outbound` there) — wire ordering
        // alone is not enough, because the broker attaches asynchronously and closes the
        // whole connection on a send that arrives before the attach completes ("Received
        // message, but the producer is not ready"). Java parity:
        // `ProducerImpl#handleProducerSuccess` → `resendMessages`.
        Some(request_id)
    }

    /// Whether this failed wire request still owns the consumer's active
    /// subscribe generation.
    ///
    /// Delayed driver retry legs use this as their liveness check before and
    /// after lookup. A seek, reconnect rebuild, or newer retry replaces the
    /// active request id and makes the older leg inert.
    #[must_use]
    pub fn consumer_subscribe_retry_is_current(
        &self,
        handle: ConsumerHandle,
        failed_request_id: RequestId,
    ) -> bool {
        self.consumer_subscribe_requests.contains_key(&handle)
            && self.consumers.get(&handle).is_some_and(|slot| {
                let consumer = slot.state.lock();
                !consumer.closed
                    && consumer.unsubscribe_request_id.is_none()
                    && consumer.terminal_failure.is_none()
                    && (consumer.subscribe_waiter_request == Some(failed_request_id)
                        || (consumer.flow_on_subscribe_ack
                            && consumer.flow_on_subscribe_ack_request == Some(failed_request_id)))
            })
    }

    /// Companion to [`Self::retry_producer_open`] for consumers. Re-emits the
    /// `CommandSubscribe` + initial `CommandFlow` for a single consumer handle, used
    /// when the broker rejected a previous `CommandSubscribe` with a transient code
    /// (`NamespaceBundleNotServed`, `ServiceNotReady`, …). The full
    /// [`Self::rebuild_consumers`] sweep is too coarse: it would re-emit every
    /// still-open consumer's `CommandSubscribe`, which would double-attach the ones
    /// that already succeeded on this session.
    pub fn retry_consumer_subscribe(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
        self.retry_consumer_subscribe_inner(handle)
    }

    /// Re-emit `CommandSubscribe` only if `failed_request_id` still owns the
    /// active subscribe generation.
    ///
    /// The failed request id is both a correlation key and cancellation token:
    /// a delayed retry returns `None` after seek, rebuild, unsubscribe, or
    /// another retry replaces the active wire request for the same handle.
    pub fn retry_consumer_subscribe_if_current(
        &mut self,
        handle: ConsumerHandle,
        failed_request_id: RequestId,
    ) -> Option<RequestId> {
        if !self.consumer_subscribe_retry_is_current(handle, failed_request_id) {
            return None;
        }
        self.retry_consumer_subscribe_inner(handle)
    }

    fn retry_consumer_subscribe_inner(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
        let req = self.consumer_subscribe_requests.get(&handle)?.clone();
        let resume_from = {
            let slot = self.consumers.get(&handle)?;
            let c = slot.state.lock();
            if c.closed {
                return None;
            }
            // Resume from the last acked id when we have one (same logic
            // `rebuild_consumers` uses). The broker treats an unset
            // `start_message_id` as "from the configured initial position".
            c.last_acked_message_id
        };
        let request_id =
            self.emit_command_subscribe(handle, &req, resume_from, SubscribeAckAction::ReleaseFlow);
        Some(request_id)
    }

    /// Re-subscribe a single consumer in place after the broker tore its
    /// dispatcher down via a same-broker `CommandCloseConsumer`
    /// (`assigned_broker_service_url = None`) on a still-live socket — issue
    /// #307's proven root cause.
    ///
    /// Unlike [`Self::rebuild_consumers`] (whole-connection sweep, runs after a
    /// `reset`) this re-attaches exactly one consumer without a transport
    /// reconnect, the way [`Self::resubscribe_consumer_after_seek`] does after a
    /// seek and [`Self::retry_consumer_subscribe`] does after a transient
    /// subscribe rejection. It reuses the same machinery: re-emit
    /// `CommandSubscribe` (resuming from the last acked id so the broker picks
    /// up where draining stopped) and set `flow_on_subscribe_ack` so the initial
    /// `CommandFlow` is deferred to the broker's re-subscribe `Success` (Pulsar
    /// silently drops `CommandFlow` for a consumer id whose subscribe is still
    /// being processed — `ServerCnx.handleFlow` "Couldn't find consumer").
    ///
    /// Skips (no-op) when the consumer is unknown, was never subscribed, is
    /// user-closed, terminal, mid-seek (the seek's own
    /// [`Self::resubscribe_consumer_after_seek`] owns re-attach), or already has
    /// a re-attach pending (`flow_on_subscribe_ack` — `rebuild_consumers` /
    /// transient-retry in flight). Drains any stale `ConsumerClosedByBroker`
    /// event for this handle first so a runtime wait-future cannot trip on it
    /// before the fresh `SubscribeAcked` (mirrors
    /// `resubscribe_consumer_after_seek`).
    ///
    /// The receiver queue is left intact — already-dispatched messages stay
    /// user-visible (the #65 / `duringSeek` invariant).
    fn resubscribe_consumer_after_broker_close(&mut self, handle: ConsumerHandle) {
        let Some(req) = self.consumer_subscribe_requests.get(&handle).cloned() else {
            // Never subscribed (e.g. the broker closed a consumer whose
            // subscribe never landed) — nothing to replay.
            return;
        };
        let resume_from = {
            let Some(slot) = self.consumers.get(&handle) else {
                return;
            };
            let c = slot.state.lock();
            if c.closed
                || c.unsubscribe_request_id.is_some()
                || c.terminal_failure.is_some()
                || c.pending_seek.is_some()
                || c.flow_on_subscribe_ack
            {
                return;
            }
            // Resume from the last acked id when known (same logic
            // `rebuild_consumers` / `retry_consumer_subscribe` use); the broker
            // treats an unset `start_message_id` as "from the configured initial
            // position" and otherwise resumes from its persisted cursor.
            c.last_acked_message_id
        };
        // Drop the stale close-by-broker event(s) for this handle: the runtime's
        // `EventWaitFut` only consumes them while parked on the initial
        // `SubscribeAcked`, but draining keeps the queue from accumulating one
        // per partition under bundle churn and prevents any future wait from
        // observing a close that the re-subscribe has already superseded.
        self.events.retain(
            |ev| !matches!(ev, ConnectionEvent::ConsumerClosedByBroker { handle: h, .. } if *h == handle),
        );
        let _ =
            self.emit_command_subscribe(handle, &req, resume_from, SubscribeAckAction::ReleaseFlow);
        tracing::debug!(
            target: "magnetar_proto::conn",
            handle = ?handle,
            "broker closed running consumer (same-broker, url=None); re-subscribed in place (#307)"
        );
    }

    fn encode_command(&mut self, cmd: &pb::BaseCommand) -> Result<(), ProtocolError> {
        encode_command(&mut self.outbound, cmd)?;
        Ok(())
    }

    /// Peek the next request id the state machine will hand out. Used by runtime-crate tests
    /// that need to know the request id before the operation has been issued (e.g. to inject
    /// a broker response). Not part of the stable public API.
    #[doc(hidden)]
    #[must_use]
    pub fn peek_next_request_id_for_test(&self) -> u64 {
        self.next_request_id
    }

    /// Returns `true` if the state machine has registered a pending request for `id`. Used by
    /// runtime-crate tests to gate broker-response injection until the request future has
    /// actually issued the command. Not part of the stable public API.
    #[doc(hidden)]
    #[must_use]
    pub fn has_pending_request_for_test(&self, id: RequestId) -> bool {
        self.pending_requests.contains_key(&id)
    }
}

#[cfg(test)]
mod conn_state_tests {
    use super::*;
    use crate::frame::encode_command;

    fn handshake_response_bytes() -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(crate::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    #[test]
    fn handle_bytes_owned_swaps_empty_inbound_with_zero_copy() {
        // ADR-0040 wave 3: when the proto's inbound buffer is empty,
        // `handle_bytes_owned` must take ownership of the caller's
        // `BytesMut` without an `extend_from_slice` memcpy.
        // We verify by feeding a complete handshake frame and
        // confirming the state machine reaches Connected. (Direct
        // `Bytes::as_ptr()` equality would assert the no-copy
        // invariant, but the `inbound.split_to(...)` inside the
        // decode loop moves the buffer into a Bytes that's no longer
        // pointer-identical to the input — the no-copy property is
        // confirmed structurally by the swap branch in the source
        // and the runtime parity tests below.)
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let chunk = handshake_response_bytes();
        conn.handle_bytes_owned(Instant::now(), chunk)
            .expect("handle_bytes_owned");
        assert!(
            conn.is_connected(),
            "handshake completes via owned-chunk entry"
        );
    }

    #[test]
    fn handle_bytes_owned_extends_when_inbound_holds_partial_frame() {
        // Mid-frame fall-back: when proto already holds a partial
        // frame in `inbound`, `handle_bytes_owned` must splice the
        // new chunk on top (extend_from_slice) without dropping the
        // earlier bytes. We split the full handshake frame in two,
        // feed the first half via `handle_bytes` (legacy entry, which
        // populates `inbound`), then the second half via
        // `handle_bytes_owned`, and assert the state machine
        // converges on Connected.
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let full = handshake_response_bytes();
        let split = full.len() / 2;
        let (first, second) = full.split_at(split);
        conn.handle_bytes(Instant::now(), first)
            .expect("first half");
        // Mid-frame: `inbound` now holds `first.len()` bytes.
        assert!(
            !conn.is_connected(),
            "handshake still pending after first half"
        );
        let mut second_buf = bytes::BytesMut::with_capacity(second.len());
        second_buf.extend_from_slice(second);
        conn.handle_bytes_owned(Instant::now(), second_buf)
            .expect("second half via owned");
        assert!(
            conn.is_connected(),
            "handshake completes after mid-frame owned-chunk extend"
        );
    }

    #[test]
    fn handle_bytes_owned_rejects_malformed_mid_session_frame() {
        // Layer (a) of the ADR-0024 four-layer policy for the driver
        // re-entrant-mutex deadlock fix (ADR-0038).
        //
        // This pins the *proto contract the runtime read loop relies
        // on*: a malformed inbound frame received **mid-session**
        // (after the handshake) is a hard reject — `handle_bytes_owned`
        // returns `Err`, not `Ok` and not a silent park. That `Err` is
        // exactly what drives the driver's error arm, where the
        // deadlock used to live: the engines' read loop re-locked the
        // already-held `shared.inner` `parking_lot::Mutex` to call
        // `mark_disconnected()` and self-deadlocked. The fix (binding
        // the result to a `let` so the guard drops first) is only
        // *reachable* because this reject path exists, so the contract
        // is pinned here and the no-deadlock behaviour in the runtime
        // layers (b)/(c).
        //
        // The cheapest deterministic reject is a frame whose 4-byte
        // big-endian `total_size` prefix is zero: `peek_full_frame_len`
        // rejects `total_size == 0` up front with
        // `FrameError::BadLength(0)` — no CRC / protobuf subtlety, only
        // four bytes on the wire (matching the swizzle-clog seeds
        // #65/#136, which reorder the clog/restore sequence into a
        // frame the state machine rejects).
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes_owned(Instant::now(), handshake_response_bytes())
            .expect("handshake completes");
        assert!(conn.is_connected(), "mid-session precondition");

        let mut malformed = bytes::BytesMut::with_capacity(4);
        malformed.extend_from_slice(&[0u8; 4]); // total_size == 0
        let err = conn
            .handle_bytes_owned(Instant::now(), malformed)
            .expect_err("a total_size=0 frame must be a hard reject, not Ok / a park");
        assert!(
            matches!(
                err,
                ProtocolError::Frame(crate::frame::FrameError::BadLength(0))
            ),
            "malformed mid-session frame must surface as a framing BadLength reject, got {err:?}",
        );
    }

    #[test]
    fn poll_transmit_vectored_emits_segments_when_outbound_empty() {
        // ADR-0040 wave 1.2: when `outbound_segments` is non-empty and
        // the contiguous `outbound` buffer is empty,
        // `poll_transmit_vectored` must return `Vectored` carrying the
        // segments. Directly populates `outbound_segments` to keep the
        // test focused on the dispatch logic without the producer
        // Ready-state setup (covered separately by the runtime
        // integration tests in
        // `crates/magnetar-runtime-{tokio,moonpool}/tests/poll_transmit_vectored_parity.rs`).
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Drain any handshake-init bytes left in `outbound` (a fresh
        // Connection starts empty, but explicit-drain keeps the
        // pre-condition obvious).
        let _ = conn.poll_transmit();
        assert!(
            conn.outbound.is_empty(),
            "outbound starts empty for this test"
        );

        // Inject two `[head, payload]` segments (4 entries) as if a
        // producer batch had been drained via
        // `drain_producer_outbound_vectored`.
        let head_a = bytes::Bytes::from_static(b"HEAD-A");
        let payload_a = bytes::Bytes::from_static(b"PAYLOAD-AAAA");
        let head_b = bytes::Bytes::from_static(b"HEAD-B");
        let payload_b = bytes::Bytes::from_static(b"PAYLOAD-BB");
        conn.outbound_segments.push(head_a.clone());
        conn.outbound_segments.push(payload_a.clone());
        conn.outbound_segments.push(head_b.clone());
        conn.outbound_segments.push(payload_b.clone());

        match conn.poll_transmit_vectored() {
            crate::Transmit::Vectored(segs) => {
                assert_eq!(segs.len(), 4, "all four segments must be emitted");
                assert_eq!(&segs[0][..], b"HEAD-A");
                assert_eq!(&segs[1][..], b"PAYLOAD-AAAA");
                assert_eq!(&segs[2][..], b"HEAD-B");
                assert_eq!(&segs[3][..], b"PAYLOAD-BB");
            }
            crate::Transmit::Contiguous(_) => {
                panic!("expected Vectored arm — outbound is empty and segments are populated");
            }
        }
    }

    #[test]
    fn poll_transmit_vectored_prefers_contiguous_when_outbound_has_bytes() {
        // ADR-0040 wave 1.2 wire-order invariant: when both
        // `outbound` (handshake / ack / lookup) and `outbound_segments`
        // (producer batch) carry pending bytes, the contiguous arm
        // wins so wire-order is preserved. Segments stay queued and
        // emerge on the next call.
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Mid-handshake `outbound` carries the pending Connect frame.
        conn.begin_handshake().expect("handshake");
        assert!(
            !conn.outbound.is_empty(),
            "post-begin_handshake: outbound must have the Connect frame"
        );
        conn.outbound_segments
            .push(bytes::Bytes::from_static(b"queued-producer-segment"));

        match conn.poll_transmit_vectored() {
            crate::Transmit::Contiguous(slice) => {
                assert!(
                    !slice.is_empty(),
                    "Contiguous arm must drain the Connect frame"
                );
            }
            crate::Transmit::Vectored(_) => {
                panic!(
                    "expected Contiguous arm — outbound was non-empty so wire-order requires it first"
                );
            }
        }
        // The segment must still be queued for the next call.
        assert_eq!(
            conn.outbound_segments.len(),
            1,
            "queued segment must persist until outbound drains"
        );
        // Now outbound is empty — next call switches to Vectored.
        match conn.poll_transmit_vectored() {
            crate::Transmit::Vectored(segs) => {
                assert_eq!(segs.len(), 1);
                assert_eq!(&segs[0][..], b"queued-producer-segment");
            }
            crate::Transmit::Contiguous(_) => {
                panic!("expected Vectored arm after outbound drained");
            }
        }
    }

    #[test]
    fn poll_transmit_vectored_matches_poll_transmit() {
        // ADR-0040 wave 1.1: the new `Transmit<'_>` entry point must
        // hand the runtime the same bytes the legacy `poll_transmit`
        // path produces today. Wave 1.2 will start emitting `Vectored`
        // for producer batches; until then `Contiguous` is the only
        // variant produced and it must be byte-identical to the legacy
        // `BytesMut::split().freeze()` payload.
        let mut conn_a = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let mut conn_b = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Drive both connections through the same handshake so both
        // outbound buffers carry an identical pending Connect frame.
        conn_a.begin_handshake().expect("handshake a");
        conn_b.begin_handshake().expect("handshake b");

        let legacy = conn_a.poll_transmit();
        let vectored = conn_b.poll_transmit_vectored();
        match vectored {
            crate::Transmit::Contiguous(slice) => {
                assert_eq!(
                    slice,
                    &legacy[..],
                    "poll_transmit_vectored::Contiguous must match poll_transmit bytes"
                );
                assert!(!slice.is_empty(), "handshake Connect frame is non-empty");
            }
            crate::Transmit::Vectored(_) => {
                panic!("wave 1.1 must not emit Vectored — that is wave 1.2");
            }
        }
        // Empty case: after the next round-trip with no queued ops,
        // both entry points must report empty (poll_transmit returns an
        // empty Bytes, poll_transmit_vectored returns an empty
        // Contiguous slice).
        let legacy_empty = conn_a.poll_transmit();
        assert!(legacy_empty.is_empty());
        let vectored_empty = conn_b.poll_transmit_vectored();
        assert!(vectored_empty.is_empty());
    }

    #[test]
    fn timestamps_track_connect_and_disconnect() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        assert!(conn.last_connected_timestamp().is_none());
        assert!(conn.last_disconnected_timestamp().is_none());
        assert!(!conn.is_connected());

        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());
        let connected_at = conn
            .last_connected_timestamp()
            .expect("connected timestamp set");
        assert!(conn.last_disconnected_timestamp().is_none());

        conn.mark_disconnected();
        assert!(!conn.is_connected());
        let disconnected_at = conn
            .last_disconnected_timestamp()
            .expect("disconnected timestamp set");
        assert!(disconnected_at >= connected_at);

        // Marking disconnected again should not bump the timestamp now that we're already in
        // a terminal state (idempotency for repeated mark_disconnected calls on Failed).
        let pinned = disconnected_at;
        conn.mark_disconnected();
        assert_eq!(conn.last_disconnected_timestamp(), Some(pinned));
    }

    #[test]
    fn local_close_records_disconnect() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());

        conn.close();
        assert!(conn.last_disconnected_timestamp().is_some());
    }

    #[test]
    fn is_closed_tracks_terminal_states() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        assert!(!conn.is_closed(), "uninitialized is not closed");
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(!conn.is_closed(), "connected is not closed");
        conn.close();
        assert!(conn.is_closed(), "after close, is_closed is true");

        // Mark_disconnected (Failed) is also a terminal state.
        let mut conn2 = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn2.begin_handshake().expect("handshake");
        let frame2 = handshake_response_bytes();
        conn2.handle_bytes(Instant::now(), &frame2).expect("handle");
        conn2.mark_disconnected();
        assert!(conn2.is_closed(), "Failed state counts as closed");
    }

    /// `is_user_closed` MUST distinguish user-initiated close (Closing /
    /// Closed) from transport drop (Failed). The supervisor's reconnect loop
    /// uses this to decide "exit cleanly" vs "redial" — collapsing them (as
    /// `is_closed` does) made the supervisor bail out the instant
    /// `mark_disconnected` flipped state to `Failed`, defeating the whole
    /// auto-reconnect feature. Locks the contract.
    #[test]
    fn is_user_closed_excludes_failed_so_supervisor_can_reconnect() {
        // (a) Connected: neither closed nor user-closed.
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");
        assert!(!conn.is_closed());
        assert!(!conn.is_user_closed());

        // (b) After `close()` (user-initiated): both flip true.
        let mut user_closed = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        user_closed.begin_handshake().expect("handshake");
        user_closed
            .handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");
        user_closed.close();
        assert!(user_closed.is_closed());
        assert!(
            user_closed.is_user_closed(),
            "user close MUST be observable via is_user_closed",
        );

        // (c) After `mark_disconnected()` (transport drop): `is_closed` is
        // true but `is_user_closed` is FALSE — this is the gate the
        // supervisor relies on to decide "redial, don't exit".
        let mut dropped = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        dropped.begin_handshake().expect("handshake");
        dropped
            .handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");
        dropped.mark_disconnected();
        assert!(dropped.is_closed());
        assert!(
            !dropped.is_user_closed(),
            "transport drop must NOT short-circuit the supervisor reconnect loop",
        );
    }

    #[test]
    fn consumer_crypto_failure_action_defaults_to_fail_for_unknown_handle() {
        let conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // No consumer has been created; an arbitrary handle must map to the safe default.
        let action = conn.consumer_crypto_failure_action(ConsumerHandle(42));
        assert_eq!(action, CryptoFailureAction::Fail);
    }

    #[test]
    fn consumer_crypto_failure_action_round_trips_from_subscribe_request() {
        // Spin up a handshake-complete connection so `subscribe` runs cleanly. We never
        // observe the broker response — we only need the locally-stored consumer state.
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");

        for action in [
            CryptoFailureAction::Fail,
            CryptoFailureAction::Discard,
            CryptoFailureAction::Consume,
        ] {
            let req = SubscribeRequest {
                topic: "persistent://public/default/t".to_owned(),
                subscription: "s".to_owned(),
                crypto_failure_action: action,
                ..Default::default()
            };
            let handle = conn.subscribe(req);
            assert_eq!(
                conn.consumer_crypto_failure_action(handle),
                action,
                "crypto_failure_action {action:?} should round-trip through subscribe",
            );
        }
    }

    /// PIP-188: feeding a `CommandTopicMigrated` BaseCommand surfaces a
    /// [`ConnectionEvent::TopicMigrated`] carrying the resource handle and the new broker URLs
    /// so the engine layer can re-bind the affected producer/consumer to the new broker.
    #[test]
    fn topic_migrated_command_surfaces_event() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake response");

        // Drain the `Connected` event so subsequent `poll_event` returns the migration event.
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected event, got {other:?}"),
        }

        // Feed a CommandTopicMigrated for a producer being moved to a new broker.
        let migrated = pb::BaseCommand {
            r#type: pb::base_command::Type::TopicMigrated as i32,
            topic_migrated: Some(pb::CommandTopicMigrated {
                resource_id: 7,
                resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
                broker_service_url: Some("pulsar://new-broker:6650".to_owned()),
                broker_service_url_tls: Some("pulsar+ssl://new-broker:6651".to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &migrated).expect("encode CommandTopicMigrated");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandTopicMigrated");

        match conn.poll_event() {
            Some(ConnectionEvent::TopicMigrated {
                producer,
                consumer,
                broker_service_url,
                broker_service_url_tls,
            }) => {
                assert_eq!(producer, Some(ProducerHandle(7)));
                assert_eq!(consumer, None);
                assert_eq!(
                    broker_service_url.as_deref(),
                    Some("pulsar://new-broker:6650")
                );
                assert_eq!(
                    broker_service_url_tls.as_deref(),
                    Some("pulsar+ssl://new-broker:6651")
                );
            }
            other => panic!("expected TopicMigrated event, got {other:?}"),
        }

        // A consumer migration must surface in the `consumer` slot of the same variant.
        let migrated_cons = pb::BaseCommand {
            r#type: pb::base_command::Type::TopicMigrated as i32,
            topic_migrated: Some(pb::CommandTopicMigrated {
                resource_id: 42,
                resource_type: pb::command_topic_migrated::ResourceType::Consumer as i32,
                broker_service_url: None,
                broker_service_url_tls: None,
            }),
            ..Default::default()
        };
        let mut buf2 = bytes::BytesMut::new();
        encode_command(&mut buf2, &migrated_cons)
            .expect("encode consumer-side CommandTopicMigrated");
        conn.handle_bytes(Instant::now(), &buf2)
            .expect("handle consumer-side CommandTopicMigrated");

        match conn.poll_event() {
            Some(ConnectionEvent::TopicMigrated {
                producer,
                consumer,
                broker_service_url,
                broker_service_url_tls,
            }) => {
                assert_eq!(producer, None);
                assert_eq!(consumer, Some(ConsumerHandle(42)));
                assert!(broker_service_url.is_none());
                assert!(broker_service_url_tls.is_none());
            }
            other => panic!("expected consumer-side TopicMigrated event, got {other:?}"),
        }
    }

    #[test]
    fn reset_bumps_epoch_and_fails_pending_ops_with_session_lost() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());
        let epoch_before = conn.session_epoch();
        assert_eq!(epoch_before, 0);

        // Issue a request-bound op (partitioned-metadata lookup) — pending until broker reply.
        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);
        assert!(
            conn.take_outcome(key).is_none(),
            "no outcome before broker reply"
        );

        // Also queue an in-flight publish so we exercise the producer-side drain branch.
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/p".to_owned(),
            ..Default::default()
        });
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("send queues");
        let send_key = PendingOpKey::Send(producer, seq);
        // The send should have been queued as pending.
        assert!(
            conn.take_outcome(send_key).is_none(),
            "publish stays pending until broker replies"
        );
        // Sanity: the producer reports the publish as pending.
        assert_eq!(
            conn.producer_pending_count(producer),
            1,
            "send must produce a pending OpSend"
        );

        // Now reset — request-bound ops must surface SessionLost; in-flight publishes are
        // snapshotted for transparent replay (no SessionLost outcome installed).
        conn.reset();
        assert_eq!(conn.session_epoch(), epoch_before + 1);
        assert!(
            matches!(
                conn.take_outcome(key),
                Some(OpOutcome::SessionLost { key: k }) if k == key
            ),
            "request-bound op fails with SessionLost after reset"
        );
        // Transparent publish replay: no `SessionLost` outcome lands on the publish key.
        // The user-facing send future re-polls after the wake-up, finds the slot empty,
        // re-registers, and stays pending until the replayed `CommandSendReceipt`.
        assert!(
            conn.take_outcome(send_key).is_none(),
            "in-flight publish is snapshotted for replay — no SessionLost outcome installed"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            1,
            "the snapshot must hold the one in-flight publish until rebuild consumes it",
        );
        assert_eq!(
            conn.state(),
            HandshakeState::Uninitialized,
            "reset snaps state back to Uninitialized so begin_handshake can fire on a new socket"
        );
    }

    /// `fail_all_pending` is the terminal counterpart of `reset`: it resolves
    /// EVERY pending op — including `Send` keys — with `OpOutcome::Terminal`
    /// (not the replay-oriented `SessionLost`), wakes each waiter exactly once,
    /// queues a `Closed` event for the event-stream waiters, installs NO replay
    /// snapshot, and leaves the handshake state untouched (the driver pairs it
    /// with `mark_disconnected`). ADR-0055.
    #[test]
    fn fail_all_pending_terminalizes_every_pending_op() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        assert!(conn.is_connected());
        // Drain the `Connected` event so the only event left after
        // `fail_all_pending` is the `Closed` we assert on.
        while conn.poll_event().is_some() {}

        // A request-bound op + an in-flight publish, each with a registered
        // user-future waker.
        let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&counter).into();

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let req_key = PendingOpKey::Request(request_id);
        conn.register_waker(req_key, waker.clone());

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/p".to_owned(),
            ..Default::default()
        });
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("send queues");
        let send_key = PendingOpKey::Send(producer, seq);
        conn.register_waker(send_key, waker);
        assert_eq!(conn.producer_pending_count(producer), 1);

        conn.fail_all_pending("peer closed");

        // (1) BOTH keys — including the Send key — surface `Terminal`.
        assert!(
            matches!(
                conn.take_outcome(req_key),
                Some(OpOutcome::Terminal { key: k, .. }) if k == req_key
            ),
            "request-bound op fails with Terminal"
        );
        assert!(
            matches!(
                conn.take_outcome(send_key),
                Some(OpOutcome::Terminal { key: k, .. }) if k == send_key
            ),
            "in-flight publish fails with Terminal (no replay, unlike reset)"
        );
        // (2) Each future was woken exactly once.
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            2,
            "both wakers fired once"
        );
        // (3) A `Closed` event carries the reason for the event-stream waiters.
        assert!(
            matches!(
                conn.poll_event(),
                Some(ConnectionEvent::Closed { reason: Some(r) }) if r == "peer closed"
            ),
            "a Closed event is queued so ProducerReady/SubscribeAcked unblock"
        );
        // (4) NO replay snapshot — terminal means terminal.
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            0,
            "fail_all_pending installs no replay snapshot"
        );
        // (5) State is untouched — the driver pairs this with `mark_disconnected`.
        assert_eq!(conn.state(), HandshakeState::Connected);

        // (6) Idempotent: a second call (e.g. a later `close()`) must not panic.
        conn.fail_all_pending("peer closed");
    }

    /// ADR-0059: `fail_all_pending` must ALSO mark every
    /// producer slot `closed` so a NEW `queue_send` issued AFTER the terminal
    /// drop fast-fails synchronously with `ProducerError::Closed` (via the
    /// existing per-slot `if self.closed` guard) instead of registering a
    /// doomed pending op no driver is left to resolve. The original
    /// `fail_all_pending` only terminalized ops that were ALREADY pending at
    /// the drop; this asserts the slot-close extension.
    ///
    /// Also pins the ADR-0038 lock order: the slot-close happens INSIDE the
    /// per-slot lock taken below the global connection mutex (`fail_all_pending`
    /// runs `&mut self`), never the reverse. We prove the order holds by taking
    /// the per-slot lock here AFTER `fail_all_pending` has returned (so the
    /// global mutex is conceptually released) and reading `closed` — a reverse
    /// acquisition inside `fail_all_pending` would have deadlocked the call
    /// above, so reaching this assertion at all is the no-reverse-order witness.
    #[test]
    fn fail_all_pending_closes_slots_so_new_sends_fast_fail() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        assert!(conn.is_connected());
        while conn.poll_event().is_some() {}

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/p".to_owned(),
            ..Default::default()
        });
        // A baseline send BEFORE the drop must succeed — the producer is open
        // and the slot is not closed yet.
        conn.send(
            producer,
            crate::producer::OutgoingMessage {
                payload: bytes::Bytes::from_static(b"pre-drop"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 8,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            Instant::now(),
        )
        .expect("pre-drop send queues");

        // Terminal drop.
        conn.fail_all_pending("peer closed");

        // (1) The slot is now marked `closed`. Reaching this `producer()`
        // accessor — which takes the per-slot lock AFTER `fail_all_pending`
        // returned — is the witness that `fail_all_pending` did NOT hold the
        // per-slot lock across a re-acquisition of the global mutex (ADR-0038
        // global → per-slot order preserved; a reverse path would have
        // deadlocked the call above).
        let slot = conn
            .producer(producer)
            .cloned()
            .expect("producer slot still registered after fail_all_pending");
        assert!(
            slot.state.lock().closed,
            "fail_all_pending must flip the per-slot closed flag (ADR-0059)"
        );

        // (2) A NEW send issued AFTER the terminal drop fast-fails. At the
        // proto layer the slot's `queue_send` returns `ProducerError::Closed`
        // via the existing `if self.closed` guard — this is the cheap signal
        // the engines map to `ClientError::PeerClosed`.
        let err = slot
            .state
            .lock()
            .queue_send(
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"post-drop"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 9,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect_err("a post-terminal send must be rejected, not registered");
        assert!(
            matches!(err, crate::error::ProducerError::Closed),
            "post-terminal queue_send must fast-fail with ProducerError::Closed, got {err:?}"
        );

        // (3) The `Connection::send` entry collapses the same rejection into
        // the opaque `InvariantViolation` it uses for every slot rejection —
        // the engines do not rely on the inner variant, they read the
        // `no_driver` latch, but the send must still be REJECTED (never
        // silently queued onto a dead connection).
        let send_err = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"post-drop-via-conn"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 18,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect_err("Connection::send must reject a post-terminal send");
        assert!(
            matches!(send_err, ProtocolError::InvariantViolation(_)),
            "Connection::send rejection surfaces as InvariantViolation, got {send_err:?}"
        );
    }

    #[test]
    fn op_outcome_session_lost_round_trips_through_outcome_slab() {
        // The slab itself is HashMap<PendingOpKey, OpOutcome>; this test exercises the
        // SessionLost variant end-to-end so the runtime-side dispatcher can pattern-match.
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);

        // No outcome before reset.
        assert!(conn.take_outcome(key).is_none());

        conn.reset();

        match conn.take_outcome(key) {
            Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
            other => panic!("expected SessionLost, got {other:?}"),
        }
        // Second take is empty — outcomes are one-shot.
        assert!(conn.take_outcome(key).is_none());
    }

    /// `begin_handshake` is the only `Uninitialized -> ConnectSent` edge; calling it twice
    /// must return `Err(ProtocolError::Handshake)` rather than silently re-emitting a
    /// second `CommandConnect`. Mirrors Java `ClientCnx#channelActive` which guards the
    /// connect path with a state check.
    #[test]
    fn begin_handshake_twice_returns_handshake_error() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("first call succeeds");
        let err = conn
            .begin_handshake()
            .expect_err("second call must fail because state is ConnectSent");
        match err {
            ProtocolError::Handshake(msg) => {
                assert!(
                    msg.contains("already"),
                    "expected an 'already started' diagnostic, got {msg:?}"
                );
            }
            other => panic!("expected Handshake error, got {other:?}"),
        }
        // Calling again after Connected is also a no-go.
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handshake");
        assert!(conn.is_connected());
        assert!(matches!(
            conn.begin_handshake(),
            Err(ProtocolError::Handshake(_))
        ));
    }

    /// Feeding a `CommandPartitionedTopicMetadataResponse` to a connection that holds the
    /// matching in-flight request must surface the partition count via both `take_outcome`
    /// and a `ConnectionEvent::PartitionedMetadataResponse`. Ports the behaviour exercised
    /// in Java `BinaryProtoLookupServiceTest#testPartitionedMetadataDeduplicationAndCleanup`
    /// — without the dedup layer (which lives at the runtime level, not the sans-io
    /// state machine).
    #[test]
    fn partitioned_metadata_response_surfaces_partition_count() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        // Drain the `Connected` event so subsequent `poll_event` returns ours.
        let _ = conn.poll_event();

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);
        assert!(conn.take_outcome(key).is_none(), "pending until reply");

        // Feed back a successful 8-partition response.
        let resp = pb::BaseCommand {
            r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
            partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
                partitions: Some(8),
                request_id: request_id.0,
                response: Some(
                    pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
                ),
                error: None,
                message: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &resp).expect("encode partitioned-metadata response");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle partitioned-metadata response");

        // Outcome arrived.
        match conn.take_outcome(key) {
            Some(OpOutcome::PartitionedMetadata {
                request_id: rid,
                partitions,
                error,
            }) => {
                assert_eq!(rid, request_id);
                assert_eq!(partitions, 8);
                assert!(error.is_none());
            }
            other => panic!("expected PartitionedMetadata outcome, got {other:?}"),
        }

        // ConnectionEvent surfaces the same information for observers (e.g. metrics).
        match conn.poll_event() {
            Some(ConnectionEvent::PartitionedMetadataResponse {
                request_id: rid,
                partitions,
                error,
            }) => {
                assert_eq!(rid, request_id);
                assert_eq!(partitions, 8);
                assert!(error.is_none());
            }
            other => panic!("expected PartitionedMetadataResponse event, got {other:?}"),
        }
    }

    /// A partitioned-metadata response carrying an error must surface as an
    /// `OpOutcome::PartitionedMetadata { error: Some((code, message)), .. }` so user
    /// futures can fail with the broker's diagnostics. Ports Java
    /// `BinaryProtoLookupService#getPartitionedTopicMetadata` failure handling.
    #[test]
    fn partitioned_metadata_response_propagates_broker_error() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);

        let resp = pb::BaseCommand {
            r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
            partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
                partitions: None,
                request_id: request_id.0,
                response: Some(
                    pb::command_partitioned_topic_metadata_response::LookupType::Failed as i32,
                ),
                error: Some(pb::ServerError::AuthorizationError as i32),
                message: Some("no perms".to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &resp).expect("encode partitioned-metadata failure");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle partitioned-metadata failure");

        match conn.take_outcome(key) {
            Some(OpOutcome::PartitionedMetadata {
                partitions, error, ..
            }) => {
                assert_eq!(partitions, 0, "no partitions on failure");
                let (code, msg) = error.expect("error populated");
                assert_eq!(code, pb::ServerError::AuthorizationError as i32);
                assert_eq!(msg, "no perms");
            }
            other => panic!("expected PartitionedMetadata outcome, got {other:?}"),
        }
    }

    /// F11 fast-path: when the topic name already encodes a partition
    /// index (`<base>-partition-<N>` per Java `TopicName#isPartitioned`),
    /// `get_partitioned_topic_metadata` must short-circuit to
    /// `partitions = 0` synthetically — no `CommandPartitionedTopicMetadata`
    /// frame is emitted, no broker round-trip is needed, no registry slot
    /// is consumed. Mirrors streamnative-pulsar-rs #327 and cuts the
    /// per-partition lookup amplification on partitioned consumers from
    /// `N+1` to `1`.
    #[test]
    fn get_partitioned_topic_metadata_fast_path_on_partition_suffix() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        // Drain the `Connected` event so subsequent `poll_event` returns ours.
        let _ = conn.poll_event();
        // Drain the handshake's `CommandConnect` from the outbound buffer.
        let _ = conn.poll_transmit();
        assert_eq!(
            conn.outbound_len(),
            0,
            "outbound must be empty after draining the handshake frames"
        );

        let request_id =
            conn.get_partitioned_topic_metadata("persistent://public/default/foo-partition-0");

        // No frame on the wire — the fast-path skipped the encode.
        assert_eq!(
            conn.outbound_len(),
            0,
            "fast-path must NOT emit a CommandPartitionedTopicMetadata frame"
        );

        // Outcome is immediately available, with partitions = 0 and no error.
        let key = PendingOpKey::Request(request_id);
        match conn.take_outcome(key) {
            Some(OpOutcome::PartitionedMetadata {
                request_id: rid,
                partitions,
                error,
            }) => {
                assert_eq!(rid, request_id);
                assert_eq!(partitions, 0, "fast-path always reports 0 partitions");
                assert!(error.is_none(), "fast-path is a success, not an error");
            }
            other => panic!("expected synthetic PartitionedMetadata outcome, got {other:?}"),
        }

        // The companion event surfaces for observers (metrics / tracing).
        match conn.poll_event() {
            Some(ConnectionEvent::PartitionedMetadataResponse {
                request_id: rid,
                partitions,
                error,
            }) => {
                assert_eq!(rid, request_id);
                assert_eq!(partitions, 0);
                assert!(error.is_none());
            }
            other => panic!("expected PartitionedMetadataResponse event, got {other:?}"),
        }
    }

    /// F11 negative path: non-partition topic names must NOT trip the
    /// fast-path — the state machine still issues a
    /// `CommandPartitionedTopicMetadata` frame and waits for the broker's
    /// response. Guards against future regressions where the detection
    /// rule accidentally widens.
    #[test]
    fn get_partitioned_topic_metadata_emits_frame_for_non_partition_topic() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();
        let _ = conn.poll_transmit();
        assert_eq!(conn.outbound_len(), 0);

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/orders");

        // Frame is on the wire — the state machine is waiting for the broker.
        assert!(
            conn.outbound_len() > 0,
            "non-partition topic must emit a CommandPartitionedTopicMetadata frame"
        );
        // No outcome until the broker replies.
        let key = PendingOpKey::Request(request_id);
        assert!(
            conn.take_outcome(key).is_none(),
            "outcome stays pending until broker reply on the slow path"
        );
    }

    /// F11 false-positive trap: a topic name like `my-partition-thing-3`
    /// contains the substring `-partition-` (as the streamnative
    /// `contains` heuristic checked) but the tail segment `thing-3` is
    /// not a partition index. Magnetar's stricter regex-equivalent rule
    /// rejects it, so the state machine MUST issue a frame and wait for
    /// the broker. This pins the divergence from streamnative's looser
    /// rule.
    #[test]
    fn get_partitioned_topic_metadata_rejects_streamnative_false_positive() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();
        let _ = conn.poll_transmit();

        // Trap names from the F11 spec — must NOT short-circuit.
        for trap in [
            "persistent://public/default/my-partition-thing-3",
            "persistent://public/default/foo-partition-foo",
            "persistent://public/default/foo-partition-",
            "persistent://public/default/foo",
        ] {
            let outbound_before = conn.outbound_len();
            let request_id = conn.get_partitioned_topic_metadata(trap);
            assert!(
                conn.outbound_len() > outbound_before,
                "topic {trap:?} must NOT short-circuit (no frame emitted)"
            );
            let key = PendingOpKey::Request(request_id);
            assert!(
                conn.take_outcome(key).is_none(),
                "topic {trap:?} must stay pending until broker reply"
            );
            // Drain the buffered frame so the next iteration starts clean.
            let _ = conn.poll_transmit();
        }
    }

    /// fix(proto): a `CommandLookupTopicResponse` with `response = Redirect`
    /// surfaces a **driveable** terminal `LookupOutcome::Redirected` on the
    /// lookup's request-id — carrying the redirect target URL, the next-hop
    /// `authoritative` flag, and the remaining hop budget — and does **NOT**
    /// re-issue a `CommandLookupTopic` on this (same, non-owner) connection.
    /// The engine dials the redirect target and re-issues the lookup there.
    ///
    /// FAIL-on-main proof: before this change, proto chased the redirect on
    /// self (`send_lookup_internal`), so a second `CommandLookupTopic` grew
    /// the outbound buffer and the `Redirected` outcome was diagnostic-only
    /// (NOT in the outcomes slot). Both of those flip here.
    #[test]
    fn lookup_redirect_response_surfaces_driveable_outcome_without_self_chase() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();

        let request_id = conn.lookup("persistent://public/default/foo", false);
        // Drain the initial CommandLookupTopic so we can detect any further
        // outbound frame (a self-chase would write a second one).
        let initial_ids = drain_outbound_lookup_ids(&mut conn);
        assert_eq!(
            initial_ids,
            vec![request_id],
            "initial lookup must enqueue exactly one CommandLookupTopic"
        );

        // Feed a Redirect response on the lookup's request-id.
        let redirect = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: Some("pulsar://other:6650".to_owned()),
                broker_service_url_tls: Some("pulsar+ssl://other:6651".to_owned()),
                response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                request_id: request_id.0,
                authoritative: Some(true),
                error: None,
                message: None,
                proxy_through_service_url: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &redirect).expect("encode redirect");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle redirect");

        // No self-chase: the proto layer must NOT re-issue a CommandLookupTopic
        // on this connection. (On main this fails — proto wrote a second frame.)
        assert!(
            drain_outbound_lookup_ids(&mut conn).is_empty(),
            "redirect must NOT trigger a self-chase CommandLookupTopic — \
             the engine dials the redirect target instead"
        );

        // The driveable `Redirected` IS published to the outcomes slot on the
        // request-id (so the engine's RequestFut wakes with it). (On main this
        // fails — the Redirected was diagnostic-only, never in outcomes.)
        match conn.take_outcome(PendingOpKey::Request(request_id)) {
            Some(OpOutcome::LookupResponse {
                request_id: rid,
                outcome:
                    crate::event::LookupOutcome::Redirected {
                        broker_service_url,
                        broker_service_url_tls,
                        authoritative,
                        hops_remaining,
                    },
            }) => {
                assert_eq!(rid, request_id);
                assert_eq!(broker_service_url.as_deref(), Some("pulsar://other:6650"));
                assert_eq!(
                    broker_service_url_tls.as_deref(),
                    Some("pulsar+ssl://other:6651")
                );
                assert!(
                    authoritative,
                    "next-hop authoritative carried out to engine"
                );
                assert_eq!(
                    hops_remaining,
                    crate::lookup::MAX_LOOKUP_REDIRECTS - 1,
                    "remaining hop budget carried out so the engine can re-thread it"
                );
            }
            other => panic!("expected driveable Redirected outcome at the anchor, got {other:?}"),
        }
    }

    /// The proto-side floor check: `Connection::lookup_redirect` clamps an
    /// engine-supplied hop budget to `MAX_LOOKUP_REDIRECTS`. A buggy or
    /// hostile engine that inflates the budget (e.g. `u8::MAX`) cannot
    /// re-open the redirect-loop DoS — after the clamp, a chain of redirects
    /// still terminates in the synthetic cap `Failed` within the bound.
    #[test]
    fn lookup_redirect_clamps_inflated_budget_to_cap() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        // Engine claims a wildly inflated budget. The clamp pins it to the cap.
        let mut current = conn.lookup_redirect("persistent://public/default/foo", true, u8::MAX);
        let _ = drain_outbound_lookup_ids(&mut conn);

        // Drive redirects on the SAME connection (simulating a broker that keeps
        // redirecting). With the clamp, the chain must terminate in the cap
        // `Failed` within MAX_LOOKUP_REDIRECTS + 1 redirects, NOT u8::MAX.
        let mut failed = None;
        for _ in 0..=crate::lookup::MAX_LOOKUP_REDIRECTS {
            let redirect = pb::BaseCommand {
                r#type: pb::base_command::Type::LookupResponse as i32,
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some("pulsar://loop:6650".to_owned()),
                    broker_service_url_tls: None,
                    response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                    request_id: current.0,
                    authoritative: Some(true),
                    error: None,
                    message: None,
                    proxy_through_service_url: None,
                }),
                ..Default::default()
            };
            let mut buf = bytes::BytesMut::new();
            encode_command(&mut buf, &redirect).expect("encode redirect");
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle redirect");
            let _ = drain_outbound_lookup_ids(&mut conn);

            match conn.take_outcome(PendingOpKey::Request(current)) {
                Some(OpOutcome::LookupResponse {
                    outcome: crate::event::LookupOutcome::Redirected { hops_remaining, .. },
                    ..
                }) => {
                    // Re-issue on self via lookup_redirect (engine would dial a
                    // target; here we keep it on one connection to count hops).
                    current = conn.lookup_redirect(
                        "persistent://public/default/foo",
                        true,
                        hops_remaining,
                    );
                    let _ = drain_outbound_lookup_ids(&mut conn);
                }
                Some(OpOutcome::LookupResponse {
                    outcome: crate::event::LookupOutcome::Failed { message, .. },
                    ..
                }) => {
                    failed = Some(message);
                    break;
                }
                other => panic!("unexpected outcome during clamp walk: {other:?}"),
            }
        }
        let message = failed.expect("clamped chain must terminate in the cap Failed within bound");
        assert!(
            message.contains("redirect cap exceeded"),
            "expected the cap diagnostic, got: {message}"
        );
    }

    /// Decode every complete `CommandLookupTopic` frame currently in the
    /// outbound buffer and return the list of wire request-ids in the
    /// order they were emitted. Drains the buffer.
    ///
    /// Test helper for the chain tests below — the proto state machine
    /// allocates a fresh wire request-id on every redirect hop, and we
    /// need to know the latest one to encode the broker's reply against
    /// the right correlator.
    fn drain_outbound_lookup_ids(conn: &mut Connection) -> Vec<RequestId> {
        let bytes = conn.poll_transmit();
        let mut cursor: bytes::Bytes = bytes;
        let mut ids = Vec::new();
        while !cursor.is_empty() {
            let frame =
                crate::frame::decode_one(&mut cursor).expect("decode outbound lookup frame");
            if let Ok(pb::base_command::Type::Lookup) =
                pb::base_command::Type::try_from(frame.command.r#type)
            {
                if let Some(l) = frame.command.lookup_topic {
                    ids.push(RequestId(l.request_id));
                }
            }
        }
        ids
    }

    /// fix(proto): driving a redirect chain the engine way — each hop
    /// re-issued via `lookup_redirect` on a fresh request-id — delivers the
    /// terminal `Connect` URL on the LAST hop's request-id. This is the
    /// connection-layer mirror of the engine's dial loop (the engine dials a
    /// new broker per hop; here we keep it on one in-memory connection and
    /// re-issue per hop to exercise the hop accounting + terminal delivery).
    #[test]
    fn lookup_redirect_chain_delivers_terminal_connect_per_hop() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        // Initial user lookup.
        let mut current = conn.lookup("persistent://public/default/foo", false);
        let _ = drain_outbound_lookup_ids(&mut conn);

        // Two redirects. Each surfaces a driveable Redirected on `current`;
        // the engine would dial the target — we re-issue via `lookup_redirect`.
        for hop in 0..2 {
            let redirect = pb::BaseCommand {
                r#type: pb::base_command::Type::LookupResponse as i32,
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some(format!("pulsar://hop-{hop}:6650")),
                    broker_service_url_tls: None,
                    response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                    request_id: current.0,
                    authoritative: Some(true),
                    error: None,
                    message: None,
                    proxy_through_service_url: None,
                }),
                ..Default::default()
            };
            let mut buf = bytes::BytesMut::new();
            encode_command(&mut buf, &redirect).expect("encode redirect");
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle redirect");
            let _ = drain_outbound_lookup_ids(&mut conn);

            let hops = match conn.take_outcome(PendingOpKey::Request(current)) {
                Some(OpOutcome::LookupResponse {
                    outcome:
                        crate::event::LookupOutcome::Redirected {
                            broker_service_url,
                            hops_remaining,
                            ..
                        },
                    ..
                }) => {
                    assert_eq!(
                        broker_service_url.as_deref(),
                        Some(&*format!("pulsar://hop-{hop}:6650"))
                    );
                    hops_remaining
                }
                other => panic!("hop {hop}: expected driveable Redirected, got {other:?}"),
            };
            current = conn.lookup_redirect("persistent://public/default/foo", true, hops);
            let _ = drain_outbound_lookup_ids(&mut conn);
        }

        // Terminal Connect on the latest request-id.
        let terminal = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: Some("pulsar://terminal:6650".to_owned()),
                broker_service_url_tls: None,
                response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
                request_id: current.0,
                authoritative: Some(true),
                error: None,
                message: None,
                proxy_through_service_url: Some(false),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &terminal).expect("encode terminal Connect");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle terminal Connect");

        match conn.take_outcome(PendingOpKey::Request(current)) {
            Some(OpOutcome::LookupResponse {
                request_id,
                outcome:
                    crate::event::LookupOutcome::Connect {
                        broker_service_url, ..
                    },
            }) => {
                assert_eq!(request_id, current);
                assert_eq!(
                    broker_service_url.as_deref(),
                    Some("pulsar://terminal:6650"),
                    "the engine must see the TERMINAL broker URL on the last hop"
                );
            }
            other => panic!("expected terminal Connect outcome, got {other:?}"),
        }
    }

    /// fix(proto): a hostile broker that keeps redirecting must surface a
    /// synthetic `Failed { code: 0, message: "lookup redirect cap exceeded …"
    /// }` once the hop budget is exhausted — even when the engine drives the
    /// chain via `lookup_redirect`. This is the connection-layer cap floor.
    #[test]
    fn lookup_redirect_chain_cap_exceeded_surfaces_failed() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        let mut current = conn.lookup("persistent://public/default/foo", false);
        let _ = drain_outbound_lookup_ids(&mut conn);

        // Drive redirects the engine way, threading the budget each hop. The
        // chain must terminate in the cap `Failed` within the bound.
        let mut failed_message = None;
        for _ in 0..=crate::lookup::MAX_LOOKUP_REDIRECTS {
            let redirect = pb::BaseCommand {
                r#type: pb::base_command::Type::LookupResponse as i32,
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some("pulsar://hostile:6650".to_owned()),
                    broker_service_url_tls: None,
                    response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                    request_id: current.0,
                    authoritative: Some(true),
                    error: None,
                    message: None,
                    proxy_through_service_url: None,
                }),
                ..Default::default()
            };
            let mut buf = bytes::BytesMut::new();
            encode_command(&mut buf, &redirect).expect("encode redirect");
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle redirect");
            let _ = drain_outbound_lookup_ids(&mut conn);

            match conn.take_outcome(PendingOpKey::Request(current)) {
                Some(OpOutcome::LookupResponse {
                    outcome: crate::event::LookupOutcome::Redirected { hops_remaining, .. },
                    ..
                }) => {
                    current = conn.lookup_redirect(
                        "persistent://public/default/foo",
                        true,
                        hops_remaining,
                    );
                    let _ = drain_outbound_lookup_ids(&mut conn);
                }
                Some(OpOutcome::LookupResponse {
                    request_id,
                    outcome: crate::event::LookupOutcome::Failed { code, message },
                }) => {
                    assert_eq!(request_id, current);
                    assert_eq!(code, 0);
                    failed_message = Some(message);
                    break;
                }
                other => panic!("unexpected outcome during cap walk: {other:?}"),
            }
        }
        let message = failed_message.expect("chain must hit the cap Failed within the bound");
        assert!(
            message.contains("redirect cap exceeded"),
            "expected cap diagnostic, got: {message}"
        );
    }

    /// Local `close()` from a state that was never connected (still `Uninitialized` or
    /// mid-handshake) must NOT record a disconnect timestamp — there was no live session
    /// to lose. Pinned because the metrics layer subtracts `connected_at` from
    /// `disconnected_at`, and a phantom disconnect-without-connect would yield a negative
    /// "session lifetime".
    #[test]
    fn close_before_connected_does_not_set_disconnected_timestamp() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Don't even call begin_handshake — we're still Uninitialized.
        conn.close();
        assert!(
            conn.last_disconnected_timestamp().is_none(),
            "close() from Uninitialized must not record a disconnect"
        );
        assert!(conn.is_closed(), "state is now Closing");

        // Also from ConnectSent (mid-handshake) the disconnect must stay absent.
        let mut conn2 = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn2.begin_handshake().expect("handshake");
        // No handshake response — still in ConnectSent.
        conn2.close();
        assert!(
            conn2.last_disconnected_timestamp().is_none(),
            "close() from ConnectSent must not record a disconnect either"
        );
    }

    /// Decode every command currently sitting in the connection's outbound buffer. Used by
    /// the rebuild_* tests to assert that the supervisor replay landed the right frames on
    /// the new socket. Drains [`Connection::poll_transmit`] (clearing internal state) and
    /// returns the parsed [`pb::BaseCommand`]s in wire order.
    fn drain_outbound_commands(conn: &mut Connection) -> Vec<pb::BaseCommand> {
        let mut cursor = conn.poll_transmit();
        let mut commands = Vec::new();
        while !cursor.is_empty() {
            let frame = crate::frame::decode_one(&mut cursor).expect("decode frame");
            commands.push(frame.command);
        }
        commands
    }

    /// Feed a `CommandProducerSuccess` for `request_id` — the broker ack that
    /// opens the producer-not-ready drain gate (`ProducerState::broker_ready`)
    /// and triggers the snapshot/pending replay. Every create/rebuild in these
    /// tests needs this step before SEND frames may reach the wire, mirroring
    /// the real protocol (Java `ProducerImpl#handleProducerSuccess`).
    fn ack_producer_success(conn: &mut Connection, request_id: u64) {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "p-test".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode ProducerSuccess");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle ProducerSuccess");
        while let Some(_e) = conn.poll_event() {}
    }

    #[test]
    fn rebuild_producers_re_emits_command_producer_after_reset() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        // Open two producers with different parameters so we can assert per-producer fields
        // (topic, access_mode) survived the replay verbatim.
        let p_a = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/topic-a".to_owned(),
            producer_name: Some("alpha".to_owned()),
            access_mode: pb::ProducerAccessMode::Shared,
            ..Default::default()
        });
        let p_b = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/topic-b".to_owned(),
            producer_name: Some("beta".to_owned()),
            access_mode: pb::ProducerAccessMode::Exclusive,
            ..Default::default()
        });
        // Discard the initial CommandProducer frames — we only want to inspect the rebuild.
        let _initial = drain_outbound_commands(&mut conn);

        // Simulate a supervisor reconnect: reset, replay the handshake on the new socket,
        // then rebuild.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect");
        // Drop the post-handshake CONNECT we just emitted.
        let _post_handshake = drain_outbound_commands(&mut conn);

        let request_ids = conn.rebuild_producers();
        assert_eq!(
            request_ids.len(),
            2,
            "one RequestId per still-open producer"
        );

        // Two `Producer` commands must hit the wire — one per re-attached producer.
        let cmds = drain_outbound_commands(&mut conn);
        let producer_cmds: Vec<&pb::CommandProducer> = cmds
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Producer as i32)
            .filter_map(|c| c.producer.as_ref())
            .collect();
        assert_eq!(producer_cmds.len(), 2);

        // Topics + access modes must match the original create requests; the request_ids
        // returned by rebuild_producers must match the ones embedded in the frames.
        let by_id: std::collections::HashMap<u64, &pb::CommandProducer> = producer_cmds
            .iter()
            .copied()
            .map(|c| (c.producer_id, c))
            .collect();
        let cmd_a = by_id.get(&p_a.0).expect("producer a re-emitted");
        let cmd_b = by_id.get(&p_b.0).expect("producer b re-emitted");
        assert_eq!(cmd_a.topic, "persistent://public/default/topic-a");
        assert_eq!(cmd_a.producer_name.as_deref(), Some("alpha"));
        assert_eq!(
            cmd_a.producer_access_mode,
            Some(pb::ProducerAccessMode::Shared as i32)
        );
        assert_eq!(cmd_b.topic, "persistent://public/default/topic-b");
        assert_eq!(cmd_b.producer_name.as_deref(), Some("beta"));
        assert_eq!(
            cmd_b.producer_access_mode,
            Some(pb::ProducerAccessMode::Exclusive as i32)
        );

        let emitted_ids: std::collections::HashSet<u64> =
            producer_cmds.iter().map(|c| c.request_id).collect();
        for rid in request_ids {
            assert!(
                emitted_ids.contains(&rid.0),
                "RequestId returned by rebuild_producers must match a wire frame"
            );
        }
    }

    #[test]
    fn rebuild_consumers_re_emits_subscribe_and_flow_after_reset() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let initial_subscribe_request_id = conn.peek_next_request_id_for_test();
        let c_handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/topic".to_owned(),
            subscription: "sub-x".to_owned(),
            sub_type: pb::command_subscribe::SubType::Shared,
            receiver_queue_size: 128,
            priority_level: Some(7),
            durable: true,
            ..Default::default()
        });
        // Drop the initial subscribe traffic.
        let _initial = drain_outbound_commands(&mut conn);
        feed_subscribe_success(&mut conn, initial_subscribe_request_id);
        assert!(conn.consume_initial_consumer_subscribe_completion(c_handle));
        while conn.poll_event().is_some() {}

        // Simulate the consumer having acked a message before the disconnect, so the rebuild
        // should resume from the post-ack id (not from `start_message_id == None`).
        let acked = MessageId {
            ledger_id: 42,
            entry_id: 17,
            partition: -1,
            batch_index: -1,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let _ = conn.ack(
            c_handle,
            AckRequest {
                message_ids: vec![acked],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            Instant::now(),
        );
        let _ = drain_outbound_commands(&mut conn);

        // Reconnect.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect");
        let _ = drain_outbound_commands(&mut conn);

        let request_ids = conn.rebuild_consumers();
        assert_eq!(request_ids.len(), 1);

        let cmds = drain_outbound_commands(&mut conn);

        let subscribe_cmd = cmds
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Subscribe as i32)
            .find_map(|c| c.subscribe.as_ref())
            .expect("CommandSubscribe re-emitted");
        assert_eq!(subscribe_cmd.topic, "persistent://public/default/topic");
        assert_eq!(subscribe_cmd.subscription, "sub-x");
        assert_eq!(
            subscribe_cmd.sub_type,
            pb::command_subscribe::SubType::Shared as i32
        );
        assert_eq!(subscribe_cmd.priority_level, Some(7));
        // Resume from post-ack: the start_message_id field must carry the acked id, not
        // None (which is what the original subscribe used).
        let smid = subscribe_cmd
            .start_message_id
            .as_ref()
            .expect("start_message_id stamped from last_acked_message_id");
        assert_eq!(smid.ledger_id, acked.ledger_id);
        assert_eq!(smid.entry_id, acked.entry_id);

        // NO CommandFlow may ride alongside the subscribe: the broker
        // silently drops flow for a consumer id whose subscribe is still
        // being processed (post-restart cursor recovery makes that window
        // seconds long), starving the re-attached consumer of broker-side
        // permits. Java `ConsumerImpl#reconnectLater` ordering: flow goes
        // out only on the subscribe ACK.
        assert!(
            cmds.iter()
                .all(|c| c.r#type != pb::base_command::Type::Flow as i32),
            "no CommandFlow may go out before the subscribe ack"
        );

        // The returned RequestId must match the one stamped on the subscribe frame.
        assert_eq!(request_ids[0].0, subscribe_cmd.request_id);
        let subscribe_rid = subscribe_cmd.request_id;

        // Broker acks the re-subscribe — the initial flow goes out NOW.
        let ack = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: subscribe_rid,
                schema: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ack).expect("encode Success");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle subscribe ack");
        while let Some(_e) = conn.poll_event() {}

        let post_ack = drain_outbound_commands(&mut conn);
        let flow_cmd = post_ack
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Flow as i32)
            .find_map(|c| c.flow.as_ref())
            .expect("CommandFlow re-emitted on the subscribe ack");
        assert_eq!(flow_cmd.consumer_id, c_handle.0);
        assert_eq!(flow_cmd.message_permits, 128);
    }

    #[test]
    fn producer_epoch_increments_on_rebuild() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/topic".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);

        // First rebuild — epoch was 0 (initial create) and must bump to 1.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect");
        let _ = drain_outbound_commands(&mut conn);
        conn.rebuild_producers();
        assert_eq!(
            conn.producer(handle)
                .expect("producer alive")
                .state
                .lock()
                .epoch,
            1,
            "first rebuild bumps producer epoch from 0 to 1"
        );

        // Inspect the wire frame — its `CommandProducer.epoch` field must carry the new
        // epoch so the broker can detect (and accept) the re-attach.
        let cmds = drain_outbound_commands(&mut conn);
        let cmd = cmds
            .iter()
            .find_map(|c| c.producer.as_ref())
            .expect("CommandProducer re-emitted");
        assert_eq!(cmd.epoch, Some(1));

        // Second rebuild — epoch must bump again.
        conn.reset();
        conn.begin_handshake().expect("re-handshake 2");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect 2");
        let _ = drain_outbound_commands(&mut conn);
        conn.rebuild_producers();
        assert_eq!(
            conn.producer(handle)
                .expect("producer alive")
                .state
                .lock()
                .epoch,
            2,
            "second rebuild bumps producer epoch from 1 to 2"
        );
        let cmds = drain_outbound_commands(&mut conn);
        let cmd = cmds
            .iter()
            .find_map(|c| c.producer.as_ref())
            .expect("CommandProducer re-emitted");
        assert_eq!(cmd.epoch, Some(2));
    }

    /// A `CommandError` correlated with a pending producer-open must surface a
    /// `ProducerOpenFailed` event (and clear the producer state) so engines waiting on the
    /// event stream observe the rejection instead of hanging. Regression for the CLI
    /// "produce hangs against fresh broker" bug: the broker rejects with
    /// `ServiceNotReady`/"Please redo the lookup". A provisional producer has no
    /// connection-stable façade yet, so the connection MUST surface the exact retryable
    /// broker error and clear the provisional state; the routing-aware client then re-runs
    /// lookup and can move the next attempt to a redirected owner. Established-handle
    /// reattachment remains driver-owned. The permanent-failure path is covered by
    /// [`command_error_on_producer_open_with_permanent_code_emits_producer_open_failed`].
    #[test]
    fn retryable_error_on_provisional_producer_open_is_client_routable() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();
        let _ = drain_outbound_commands(&mut conn);

        let request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/no-bundle".to_owned(),
            ..Default::default()
        });
        assert!(conn.has_pending_request_for_test(request_id));
        assert!(conn.producer(handle).is_some());

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "namespace bundle not served".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        match conn.poll_event() {
            Some(ConnectionEvent::ProducerOpenFailed {
                handle: ev_handle,
                code,
                message,
            }) => {
                assert_eq!(ev_handle, handle);
                assert_eq!(code, pb::ServerError::ServiceNotReady as i32);
                assert_eq!(message, "namespace bundle not served");
            }
            other => panic!("expected ProducerOpenFailed event, got {other:?}"),
        }
        assert!(
            conn.producer(handle).is_none(),
            "provisional state must be cleared so the client can retry on a redirected owner"
        );
        assert!(
            !conn.has_pending_request_for_test(request_id),
            "pending request slot freed"
        );
    }

    /// Sibling of the transient test above: a hard error code
    /// (`AuthorizationError`, `ProducerFenced`, …) MUST drop the producer state and
    /// emit `ProducerOpenFailed` so the user's open future fails fast. The transient
    /// retry path only applies to ADR-0080's operation-specific allowlist.
    #[test]
    fn command_error_on_producer_open_with_permanent_code_emits_producer_open_failed() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();
        let _ = drain_outbound_commands(&mut conn);

        let request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/forbidden".to_owned(),
            ..Default::default()
        });

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::AuthorizationError as i32,
                message: "not authorized".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        match conn.poll_event() {
            Some(ConnectionEvent::ProducerOpenFailed {
                handle: ev_handle,
                code,
                ..
            }) => {
                assert_eq!(ev_handle, handle);
                assert_eq!(code, pb::ServerError::AuthorizationError as i32);
            }
            other => panic!("expected ProducerOpenFailed event, got {other:?}"),
        }
        assert!(
            conn.producer(handle).is_none(),
            "permanent producer-open failure must drop the producer state"
        );
    }

    /// Same routing-aware provisional-failure contract as the producer-open path.
    #[test]
    fn retryable_error_on_provisional_subscribe_is_client_routable() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();
        let _ = drain_outbound_commands(&mut conn);

        let request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/no-bundle".to_owned(),
            subscription: "regression".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        assert!(conn.has_pending_request_for_test(request_id));

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "namespace bundle not served".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        match conn.poll_event() {
            Some(ConnectionEvent::SubscribeFailed {
                handle: ev_handle,
                code,
                message,
            }) => {
                assert_eq!(ev_handle, handle);
                assert_eq!(code, pb::ServerError::ServiceNotReady as i32);
                assert_eq!(message, "namespace bundle not served");
            }
            other => panic!("expected SubscribeFailed event, got {other:?}"),
        }
        assert!(
            conn.consumer(handle).is_none(),
            "provisional state must be cleared so the client can retry on a redirected owner"
        );
        assert!(
            !conn.has_pending_request_for_test(request_id),
            "pending request slot freed"
        );
    }

    // ============================================================================
    // Issue #302 — bounded transient retry: repeated transient open / subscribe
    // failures must back off and eventually SURFACE a terminal error to the
    // parked send / receive future instead of giving up forever (one-shot
    // retry) or re-arming forever. The per-handle attempt counter bumps on each
    // transient rejection and a give-up past the configured retry count
    // terminalizes the handle, waking the parked waker.
    // ============================================================================

    /// Feed a transient `CommandError` correlated with `request_id` into `conn`.
    fn feed_transient_error(conn: &mut Connection, request_id: RequestId) {
        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "namespace bundle not served".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");
    }

    /// #302 (proto unit, producer): the FIRST transient producer-open failure
    /// bumps the attempt counter to 1 and re-emits a recoverable
    /// `ProducerOpenFailedTransient`; a SECOND transient failure on the retry
    /// bumps it to 2 and re-emits again (proving the retry re-arms across
    /// MULTIPLE failures — no existing test exercised a repeated transient
    /// failure). Driving the counter past the default configured retry count
    /// then terminalizes the open: the parked `send()` future's waker fires and
    /// the `Send` outcome flips to `Terminal` (so `send()` returns `Err` instead
    /// of hanging on the closed `broker_ready` drain gate forever).
    #[test]
    fn transient_producer_open_retries_then_terminalizes_and_wakes_send() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        // Model an established producer being reattached: provisional failures
        // return to the routing-aware client and do not use this per-handle path.
        let mut request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/no-bundle".to_owned(),
            ..Default::default()
        });
        conn.producer(handle)
            .expect("producer exists")
            .state
            .lock()
            .has_ever_attached = true;

        // A staged send parks a user waker on the Send key. It can never flow
        // while `broker_ready` is false (the drain gate), so its future stays
        // PENDING until the open terminalizes (issue #302's hang).
        let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&counter).into();
        let seq = conn
            .send(
                handle,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("send queues");
        let send_key = PendingOpKey::Send(handle, seq);
        conn.register_waker(send_key, waker);

        // First transient rejection → attempt 1, recoverable event re-emitted.
        feed_transient_error(&mut conn, request_id);
        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "producer-open uses typed events; its request outcome must not leak"
        );
        assert_eq!(conn.producer_transient_open_attempts(handle), 1);
        assert!(matches!(
            conn.poll_event(),
            Some(ConnectionEvent::ProducerOpenFailedTransient { .. })
        ));

        // Drive a SECOND retry + transient rejection: re-issue the open (as the
        // engine's retry leg would) and bounce it again. Attempt → 2, still
        // recoverable.
        let _ = drain_outbound_commands(&mut conn);
        request_id = conn
            .retry_producer_open_if_current(handle, request_id)
            .expect("retry re-issues open");
        feed_transient_error(&mut conn, request_id);
        assert_eq!(conn.producer_transient_open_attempts(handle), 2);
        assert!(matches!(
            conn.poll_event(),
            Some(ConnectionEvent::ProducerOpenFailedTransient { .. })
        ));

        // Now exhaust the budget: keep re-issuing + bouncing until the counter
        // crosses the default configured retry count. The crossing event must
        // be a TERMINAL `ProducerOpenFailed`, NOT another transient.
        let mut saw_terminal = false;
        for _ in 0..(MAX_TRANSIENT_OPEN_RETRIES + 4) {
            let _ = drain_outbound_commands(&mut conn);
            let Some(rid) = conn.retry_producer_open_if_current(handle, request_id) else {
                // Producer was removed by the terminal give-up.
                break;
            };
            request_id = rid;
            feed_transient_error(&mut conn, rid);
            match conn.poll_event() {
                Some(ConnectionEvent::ProducerOpenFailed { handle: h, .. }) => {
                    assert_eq!(h, handle);
                    saw_terminal = true;
                    break;
                }
                Some(ConnectionEvent::ProducerOpenFailedTransient { .. }) => {}
                other => panic!("unexpected event during retry loop: {other:?}"),
            }
        }
        assert!(
            saw_terminal,
            "transient retries must terminalize once the budget is exhausted"
        );

        // The parked send waker fired and the Send key now resolves Terminal so
        // `send()` returns Err — no permanent hang.
        assert!(
            counter.0.load(Ordering::SeqCst) >= 1,
            "parked send waker must fire on terminal give-up"
        );
        assert!(
            matches!(
                conn.take_outcome(send_key),
                Some(OpOutcome::Terminal { .. })
            ),
            "staged send surfaces Terminal so send() returns Err"
        );
        assert!(
            conn.producer(handle).is_none(),
            "terminal give-up drops the producer state"
        );
    }

    #[test]
    fn operation_retry_config_can_disable_producer_open_reissues() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.set_operation_retry_config(crate::OperationRetryConfig {
            max_retries: Some(0),
            ..crate::OperationRetryConfig::default()
        });
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/no-retry".to_owned(),
            ..Default::default()
        });
        feed_transient_error(&mut conn, request_id);

        assert!(
            matches!(
                conn.poll_event(),
                Some(ConnectionEvent::ProducerOpenFailed {
                    handle: failed,
                    code,
                    ..
                }) if failed == handle && code == pb::ServerError::ServiceNotReady as i32
            ),
            "max_retries=Some(0) must surface the first broker error"
        );
        assert!(
            conn.producer(handle).is_none(),
            "disabled retries must terminalize the producer immediately"
        );
    }

    #[test]
    fn permanent_reattachment_errors_terminalize_established_operations() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let producer_request_id = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/permanent-reattach-producer".to_owned(),
            ..Default::default()
        });
        ack_producer_success(&mut conn, producer_request_id);

        let consumer_request_id = conn.peek_next_request_id_for_test();
        let consumer = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/permanent-reattach-consumer".to_owned(),
            subscription: "permanent-reattach".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        feed_subscribe_success(&mut conn, consumer_request_id);
        while conn.poll_event().is_some() {}

        let snapshot_sequence_id = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"before-reset"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 12,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send before reset");
        let reset_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let reset_waker: Waker = Arc::clone(&reset_counter).into();
        conn.register_waker(
            PendingOpKey::Send(producer, snapshot_sequence_id),
            reset_waker,
        );
        conn.reset();
        assert_eq!(
            reset_counter.0.load(Ordering::SeqCst),
            1,
            "reset must wake the send so its future re-registers"
        );
        let snapshot_terminal_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let snapshot_terminal_waker: Waker = Arc::clone(&snapshot_terminal_counter).into();
        conn.register_waker(
            PendingOpKey::Send(producer, snapshot_sequence_id),
            snapshot_terminal_waker,
        );
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect handshake");
        while conn.poll_event().is_some() {}
        let producer_retry = conn.rebuild_producers()[0];
        let consumer_retry = conn.rebuild_consumers()[0];

        let sequence_id = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"pending"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 7,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send during producer reattachment");
        let send_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let send_waker: Waker = Arc::clone(&send_counter).into();
        conn.register_waker(PendingOpKey::Send(producer, sequence_id), send_waker);

        let receive_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let receive_waker: Waker = Arc::clone(&receive_counter).into();
        conn.register_consumer_receive_waker(consumer, receive_waker)
            .expect("register receive waker during consumer reattachment");

        for request_id in [producer_retry, consumer_retry] {
            let error = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: request_id.0,
                    error: pb::ServerError::TopicNotFound as i32,
                    message: "topic was deleted".to_owned(),
                }),
                ..Default::default()
            };
            let mut frame = bytes::BytesMut::new();
            encode_command(&mut frame, &error).expect("encode CommandError");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle terminal reattachment error");
        }

        assert_eq!(send_counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            conn.take_outcome(PendingOpKey::Send(producer, sequence_id)),
            Some(OpOutcome::Terminal { .. })
        ));
        assert_eq!(snapshot_terminal_counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            conn.take_outcome(PendingOpKey::Send(producer, snapshot_sequence_id)),
            Some(OpOutcome::Terminal { .. })
        ));
        assert_eq!(receive_counter.0.load(Ordering::SeqCst), 1);
        assert!(
            conn.consumer(consumer).is_some(),
            "the terminal marker must remain readable by the parked receive future"
        );
        assert!(conn.consumer_handle_is_terminal(consumer));
    }

    /// #302 (proto unit, consumer): the twin of the producer test. Repeated
    /// transient subscribe failures bump the attempt counter; the give-up past
    /// the cap installs a per-consumer terminal failure, wakes the parked
    /// `receive()` waker, and makes `consumer_handle_is_terminal` return true
    /// (so `receive()` resolves `Err` instead of blocking on a subscription
    /// that will never reattach with `available_permits = 0`).
    #[test]
    fn transient_subscribe_retries_then_terminalizes_and_wakes_receive() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let mut request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/no-bundle".to_owned(),
            subscription: "retry-302".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        conn.consumer(handle)
            .expect("consumer exists")
            .state
            .lock()
            .has_ever_attached = true;

        // Park a receive waker on the consumer (the future that would hang).
        let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&counter).into();
        conn.register_consumer_receive_waker(handle, waker)
            .expect("register receive waker");

        // First transient → attempt 1, recoverable.
        feed_transient_error(&mut conn, request_id);
        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "subscribe uses typed events; its request outcome must not leak"
        );
        assert_eq!(conn.consumer_transient_subscribe_attempts(handle), 1);
        assert!(matches!(
            conn.poll_event(),
            Some(ConnectionEvent::SubscribeFailedTransient { .. })
        ));
        assert!(
            !conn.consumer_handle_is_terminal(handle),
            "a recoverable transient subscribe must NOT be terminal"
        );

        // Exhaust the budget.
        let mut saw_terminal = false;
        for _ in 0..(MAX_TRANSIENT_OPEN_RETRIES + 4) {
            let _ = drain_outbound_commands(&mut conn);
            let Some(rid) = conn.retry_consumer_subscribe_if_current(handle, request_id) else {
                break;
            };
            request_id = rid;
            feed_transient_error(&mut conn, request_id);
            match conn.poll_event() {
                Some(ConnectionEvent::SubscribeFailed { handle: h, .. }) => {
                    assert_eq!(h, handle);
                    saw_terminal = true;
                    break;
                }
                Some(ConnectionEvent::SubscribeFailedTransient { .. }) => {}
                other => panic!("unexpected event during subscribe retry loop: {other:?}"),
            }
        }
        assert!(
            saw_terminal,
            "transient subscribe retries must terminalize once the budget is exhausted"
        );

        // The parked receive waker fired, the handle is now terminal, and the
        // consumer slot is RETAINED so the parked future can read the marker.
        assert!(
            counter.0.load(Ordering::SeqCst) >= 1,
            "parked receive waker must fire on terminal give-up"
        );
        assert!(
            conn.consumer_handle_is_terminal(handle),
            "terminal give-up makes the handle terminal so receive() returns Err"
        );
        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "terminal give-up leaves no broker permits"
        );
    }

    #[test]
    fn unsubscribe_suspends_and_rejection_restores_established_retry_generation() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);
        let failed_request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/unsubscribe-retry-generation".to_owned(),
            subscription: "s".to_owned(),
            ..Default::default()
        });
        conn.consumer(handle)
            .expect("consumer exists")
            .state
            .lock()
            .has_ever_attached = true;
        feed_transient_error(&mut conn, failed_request_id);
        assert!(conn.consumer_subscribe_retry_is_current(handle, failed_request_id));

        let unsubscribe_request_id = conn
            .try_unsubscribe(handle, false)
            .expect("first unsubscribe must be staged");
        assert_eq!(
            conn.try_unsubscribe(handle, true),
            None,
            "overlapping unsubscribe must be rejected"
        );
        assert!(!conn.consumer_subscribe_retry_is_current(handle, failed_request_id));
        assert_eq!(
            conn.retry_consumer_subscribe_if_current(handle, failed_request_id),
            None
        );
        assert!(
            !matches!(
                conn.poll_event(),
                Some(ConnectionEvent::SubscribeFailedTransient { .. })
            ),
            "staging unsubscribe must remove an unclaimed retry event"
        );

        let error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: unsubscribe_request_id.0,
                error: pb::ServerError::MetadataError as i32,
                message: "unsubscribe rejected".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = bytes::BytesMut::new();
        encode_command(&mut frame, &error).expect("encode unsubscribe error");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle unsubscribe error");
        assert!(
            conn.take_outcome(PendingOpKey::Request(unsubscribe_request_id))
                .is_none(),
            "a cancelled waiter must not leave an undrainable outcome"
        );
        let resumed_request_id = RequestId(conn.peek_next_request_id_for_test() - 1);
        assert!(conn.consumer_subscribe_retry_is_current(handle, resumed_request_id));
    }

    #[test]
    fn unsubscribe_success_finalizes_after_waiter_cancellation() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let subscribe_request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/unsubscribe-cancelled-waiter".to_owned(),
            subscription: "s".to_owned(),
            ..Default::default()
        });
        let subscribe_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: subscribe_request_id.0,
                schema: None,
            }),
            ..Default::default()
        };
        let mut frame = bytes::BytesMut::new();
        encode_command(&mut frame, &subscribe_success).expect("encode subscribe success");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle subscribe success");
        assert!(conn.consume_initial_consumer_subscribe_completion(handle));
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let unsubscribe_request_id = conn
            .try_unsubscribe(handle, false)
            .expect("unsubscribe must be staged");
        let unsubscribe_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: unsubscribe_request_id.0,
                schema: None,
            }),
            ..Default::default()
        };
        let mut frame = bytes::BytesMut::new();
        encode_command(&mut frame, &unsubscribe_success).expect("encode unsubscribe success");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle unsubscribe success");

        assert!(
            conn.consumer(handle).is_none(),
            "broker success must finalize the local handle without a runtime waiter"
        );
        assert!(
            conn.take_outcome(PendingOpKey::Request(unsubscribe_request_id))
                .is_none(),
            "a cancelled waiter must not leave an undrainable success outcome"
        );
    }

    // ============================================================================
    // Issue #299 — recoverable-vs-terminal receive gating: a consumer parked in
    // `receive()` across a transport drop must distinguish a TRANSIENT,
    // supervisor-recoverable `Failed` window (re-park) from a GENUINELY terminal
    // state (resolve Err). `is_terminally_closed` / `consumer_handle_is_terminal`
    // encode that distinction.
    // ============================================================================

    /// #299 (proto unit): `is_terminally_closed` is FALSE for a supervised
    /// `Failed` window (recoverable — the receive future must re-park) and TRUE
    /// for a non-supervised `Failed` (terminal — the receive future must Err).
    /// This is the predicate the runtime receive guard switched to (replacing
    /// the old blanket `is_closed()`, which erroneously errored during the
    /// recoverable window).
    #[test]
    fn is_terminally_closed_distinguishes_recoverable_failed_from_terminal() {
        // Supervised connection: a `Failed` window is RECOVERABLE.
        let supervised_cfg = ConnectionConfig {
            supervisor: Some(crate::supervisor::SupervisorConfig::default()),
            ..ConnectionConfig::default()
        };
        let mut supervised = Connection::new(
            supervised_cfg,
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        supervised.begin_handshake().expect("handshake");
        supervised
            .handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        supervised.mark_disconnected();
        assert_eq!(supervised.state(), HandshakeState::Failed);
        assert!(
            supervised.is_closed(),
            "is_closed() is true for Failed (the old, too-coarse guard)"
        );
        assert!(
            !supervised.is_terminally_closed(),
            "a SUPERVISED Failed window is recoverable — receive() must re-park"
        );

        // Non-supervised connection: a `Failed` window is TERMINAL.
        let mut plain = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        plain.begin_handshake().expect("handshake");
        plain
            .handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        plain.mark_disconnected();
        assert_eq!(plain.state(), HandshakeState::Failed);
        assert!(
            plain.is_terminally_closed(),
            "a NON-supervised Failed is terminal — receive() must Err"
        );
    }

    /// #299 (proto unit): a consumer parked in `receive()` across a transport
    /// drop on a SUPERVISED connection re-parks (`consumer_handle_is_terminal`
    /// is false) during the recoverable `Failed` window — even though `reset()`
    /// drains + wakes the parked receive waker while still `Failed`. After the
    /// supervisor re-handshakes + replays the subscribe, a delivered message
    /// pops normally. The companion branch — a per-handle terminal failure
    /// (#302) — DOES make the handle terminal.
    #[test]
    fn consumer_handle_terminal_false_during_recoverable_failed_true_on_terminal() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let cfg = ConnectionConfig {
            supervisor: Some(crate::supervisor::SupervisorConfig::default()),
            ..ConnectionConfig::default()
        };
        let mut conn = Connection::new(cfg, std::sync::Arc::new(std::time::SystemTime::now));
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}
        let _ = drain_outbound_commands(&mut conn);

        let request_id = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/recoverable".to_owned(),
            subscription: "recover-299".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        // Ack the subscribe so the consumer is live.
        let ok = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ok).expect("encode CommandSuccess");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandSuccess");
        while conn.poll_event().is_some() {}

        // Park a receive waker, then drop the transport.
        let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&counter).into();
        conn.register_consumer_receive_waker(handle, waker)
            .expect("register receive waker");
        // Transport drop → `Failed`. A receive future re-polled here (e.g. woken
        // by `fail_all_pending` on a per-attempt drop, or simply re-scheduled)
        // must NOT see a terminal handle: the supervisor will reconnect.
        conn.mark_disconnected();
        assert_eq!(conn.state(), HandshakeState::Failed);
        assert!(
            !conn.consumer_handle_is_terminal(handle),
            "recoverable supervised Failed must NOT be terminal — receive() re-parks \
             (issue #299: the old is_closed()-based guard erroneously Err'd here)"
        );

        // `reset()` drains + wakes the parked receive waker (the canonical
        // wake-across-drop the bug report hit) and snaps the state to
        // `Uninitialized` for the fresh handshake. The woken future re-polls and
        // STILL sees a non-terminal handle, so it re-parks instead of erroring.
        conn.reset();
        assert_eq!(conn.state(), HandshakeState::Uninitialized);
        assert!(
            counter.0.load(Ordering::SeqCst) >= 1,
            "reset() wakes the parked receive future"
        );
        assert!(
            !conn.consumer_handle_is_terminal(handle),
            "recoverable Uninitialized (post-reset, pre-handshake) must NOT be terminal"
        );

        // Now the OTHER branch: a per-handle terminal failure makes it terminal.
        conn.fail_consumer_subscribe(handle, "test terminal");
        assert!(
            conn.consumer_handle_is_terminal(handle),
            "an installed terminal failure makes the handle terminal — receive() Errs"
        );
    }

    // ============================================================================
    // Stage 3 — transparent in-flight publish replay across reconnect
    //
    // Pins the contract that `Connection::reset` snapshots in-flight publishes (rather than
    // discarding them with a `SessionLost` outcome), and `Connection::rebuild_producers`
    // re-issues them onto the freshly-handshaked session preserving ordering and sequence
    // ids. Mirrors Java `ProducerImpl#resendMessages`.
    // ============================================================================

    /// Build a `CommandSendReceipt` wire frame for the given producer + sequence id.
    /// Returns the frame-encoded bytes (a single `BaseCommand` ready to feed into
    /// `Connection::handle_bytes`).
    fn send_receipt_bytes(producer: ProducerHandle, sequence_id: SequenceId) -> bytes::BytesMut {
        let receipt = pb::CommandSendReceipt {
            producer_id: producer.0,
            sequence_id: sequence_id.0,
            message_id: Some(pb::MessageIdData {
                ledger_id: 1,
                entry_id: sequence_id.0,
                ..Default::default()
            }),
            highest_sequence_id: None,
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(receipt),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandSendReceipt");
        buf
    }

    /// ADR-0086: `handle_frame(now, …)` must forward its injected `now` into
    /// `ProducerState::apply_receipt`. This is the only test pinning the receipt fan-out call
    /// at the `SendReceipt` arm — a local "fix" that reached for `Instant::now()` there would
    /// pass every `ProducerState`-level test and fail only here.
    #[test]
    fn connection_handle_frame_threads_now_into_send_latency() {
        /// Scripted broker round-trip; `<= 2047` keeps `hdrhistogram` sigfig-3 exact.
        const RTT_MS: u64 = 250;

        let base = Instant::now();
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(base, &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-latency-injected-clock".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        // Enqueue at `base` …
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"x"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                base,
            )
            .expect("queue");
        let _ = drain_outbound_commands(&mut conn);

        // … and land the receipt RTT_MS later, purely by injection.
        let receipt_bytes = send_receipt_bytes(producer, seq);
        conn.handle_bytes(base + Duration::from_millis(RTT_MS), &receipt_bytes)
            .expect("apply receipt");
        while conn.poll_event().is_some() {}

        let hist = conn
            .producer(producer)
            .expect("producer slot registered")
            .state
            .lock()
            .send_latency_histogram()
            .expect("send_latency_hist initialised");
        assert_eq!(hist.len(), 1, "one sample per applied CommandSendReceipt");
        assert_eq!(
            hist.max(),
            RTT_MS,
            "handle_frame must forward its injected `now` verbatim to apply_receipt"
        );
    }

    /// ADR-0086 sibling of the producer test above: `Connection::pop_message` must forward its
    /// `now` argument into `ConsumerState::pop_message` rather than reading the host clock.
    #[test]
    fn connection_pop_message_threads_now_into_receive_latency() {
        /// Scripted receive dwell; `<= 2047` keeps `hdrhistogram` sigfig-3 exact.
        const DWELL_MS: u64 = 250;

        let base = Instant::now();
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(base, &handshake_response_bytes())
            .expect("handle");

        let sub_rid = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/receive-latency-injected-clock".to_owned(),
            subscription: "sub-receive-latency-injected-clock".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        feed_subscribe_success(&mut conn, sub_rid);
        conn.initial_flow(handle, base);
        let _ = drain_outbound_commands(&mut conn);

        // Deliver at `base` …
        let meta = regular_metadata();
        let frame = message_frame(handle.0, &meta, b"payload");
        conn.handle_bytes(base, &frame).expect("deliver message");
        while conn.poll_event().is_some() {}

        // … and pop DWELL_MS later.
        let _msg = conn
            .pop_message(handle, base + Duration::from_millis(DWELL_MS))
            .expect("queued message");

        let hist = conn
            .consumer(handle)
            .expect("consumer slot registered")
            .state
            .lock()
            .receive_latency_histogram()
            .expect("receive_latency_hist initialised");
        assert_eq!(hist.len(), 1, "one sample per popped message");
        assert_eq!(
            hist.max(),
            DWELL_MS,
            "Connection::pop_message must forward `now` verbatim to ConsumerState::pop_message"
        );
    }

    /// ADR-0089 layer (a) fixture: a handshaked connection carrying exactly one
    /// consumer and one producer, both attached and flow-controlled, built with
    /// the supplied `stats_interval`.
    ///
    /// Returns the connection plus both handles so each test below can drive
    /// real traffic through the production delivery / publish paths — the rate
    /// window is a function of `total_msgs_received` / `total_msgs_sent`, and
    /// hand-poking those counters would test the arithmetic rather than the
    /// sweep that is supposed to call it.
    ///
    /// Every inbound frame is fed at `base`, deliberately: the creation-time
    /// baseline is anchored to `last_activity`, so the shared
    /// `feed_subscribe_success` / `ack_producer_success` helpers — which stamp
    /// `Instant::now()` — would seed the two slots microseconds apart and make
    /// the exact-deadline assertions below meaningless.
    fn stats_interval_conn(
        interval: Option<Duration>,
        base: Instant,
    ) -> (Connection, ConsumerHandle, ProducerHandle) {
        let mut conn = Connection::new(
            ConnectionConfig {
                stats_interval: interval,
                ..ConnectionConfig::default()
            },
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(base, &handshake_response_bytes())
            .expect("handle handshake");

        let sub_rid = conn.peek_next_request_id_for_test();
        let consumer = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/stats-interval".to_owned(),
            subscription: "sub-stats-interval".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        let ok = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: sub_rid,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ok).expect("encode CommandSuccess");
        conn.handle_bytes(base, &buf)
            .expect("handle CommandSuccess");
        while conn.poll_event().is_some() {}
        conn.initial_flow(consumer, base);
        let _ = drain_outbound_commands(&mut conn);

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/stats-interval".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        let ok = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: create_rid,
                producer_name: "p-test".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ok).expect("encode ProducerSuccess");
        conn.handle_bytes(base, &buf)
            .expect("handle ProducerSuccess");
        while conn.poll_event().is_some() {}

        (conn, consumer, producer)
    }

    /// The instant each slot's rolling-rate baseline is anchored at, or `None`
    /// when it has no baseline yet.
    fn stats_baselines(
        conn: &Connection,
        consumer: ConsumerHandle,
        producer: ProducerHandle,
    ) -> (Option<Instant>, Option<Instant>) {
        (
            conn.consumer(consumer)
                .expect("consumer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .map(|(_, _, at)| at),
            conn.producer(producer)
                .expect("producer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .map(|(_, _, at)| at),
        )
    }

    /// Push `count` messages of `payload` at the consumer and publish `count`
    /// of them from the producer, so both slots' rate-window counters move by a
    /// known amount between two sweeps.
    fn drive_stats_traffic(
        conn: &mut Connection,
        consumer: ConsumerHandle,
        producer: ProducerHandle,
        count: u64,
        at: Instant,
    ) {
        let meta = regular_metadata();
        for _ in 0..count {
            let frame = message_frame(consumer.0, &meta, b"payload");
            conn.handle_bytes(at, &frame).expect("deliver message");
            while conn.poll_event().is_some() {}
        }
        for seq in 0..count {
            conn.send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"payload"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 7,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                seq,
                at,
            )
            .expect("queue send");
        }
        let _ = drain_outbound_commands(conn);
    }

    /// `(msgs_per_sec, bytes_per_sec)` currently published by each slot.
    fn stats_rates(
        conn: &Connection,
        consumer: ConsumerHandle,
        producer: ProducerHandle,
    ) -> ((f64, f64), (f64, f64)) {
        let c = conn
            .consumer(consumer)
            .expect("consumer slot registered")
            .state
            .lock()
            .stats();
        let p = conn
            .producer(producer)
            .expect("producer slot registered")
            .state
            .lock()
            .stats();
        (
            (c.msgs_per_sec, c.bytes_per_sec),
            (p.msgs_per_sec, p.bytes_per_sec),
        )
    }

    /// ADR-0089: `stats_interval: None` — the default this commit ships —
    /// leaves sampling entirely caller-driven. No slot ever gets a baseline, no
    /// rate ever moves off `0.0` however far the injected clock advances, and
    /// (load-bearing for moonpool determinism) neither slot contributes a
    /// deadline to `poll_timeout`, so the simulated wake schedule is
    /// bit-for-bit what it was before this feature existed.
    #[test]
    fn stats_interval_none_arms_no_deadline_and_never_samples() {
        let base = Instant::now();
        let (mut conn, consumer, producer) = stats_interval_conn(None, base);

        // `last_activity` is stamped by the subscribe / producer-ack frames the
        // fixture feeds, so the keepalive deadline is at `>= base + keepalive`
        // rather than exactly there. That lower bound is what makes this
        // assertion load-bearing: any per-slot stats deadline would be armed off
        // a baseline at `base` with an interval far below the 30 s keepalive, so
        // it could only land BELOW the bound. The armed case in
        // `stats_interval_seeds_then_samples_producer_and_consumer_rates` shows
        // exactly that.
        let deadline = conn
            .poll_timeout()
            .expect("keepalive is armed once connected");
        assert!(
            deadline >= base + ConnectionConfig::default().keepalive_interval,
            "with the sweep disabled the only armed deadline is keepalive — a slot \
             must contribute nothing"
        );

        drive_stats_traffic(&mut conn, consumer, producer, 4, base);
        // An hour is far past any plausible interval: if the sweep were armed at
        // all, this single tick would both seed and sample.
        conn.handle_timeout(base + Duration::from_hours(1));

        assert!(
            conn.consumer(consumer)
                .expect("consumer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .is_none(),
            "a disabled sweep must never install a consumer baseline"
        );
        assert!(
            conn.producer(producer)
                .expect("producer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .is_none(),
            "a disabled sweep must never install a producer baseline"
        );
        let ((c_msgs, c_bytes), (p_msgs, p_bytes)) = stats_rates(&conn, consumer, producer);
        for (label, rate) in [
            ("consumer msgs_per_sec", c_msgs),
            ("consumer bytes_per_sec", c_bytes),
            ("producer msgs_per_sec", p_msgs),
            ("producer bytes_per_sec", p_bytes),
        ] {
            assert!(
                rate.abs() < f64::EPSILON,
                "{label} must stay 0.0 while stats_interval is None, got {rate}"
            );
        }
    }

    /// ADR-0089 headline behaviour: with the knob armed, each slot's baseline is
    /// installed at creation, `poll_timeout` arms the next sample off it, and
    /// the sweep at the deadline publishes the real per-second rates — with no
    /// engine, task, or caller involvement.
    #[test]
    fn stats_interval_seeds_at_creation_then_samples_one_interval_later() {
        /// One synthetic second per window, so a delta of N is exactly N/sec.
        const INTERVAL: Duration = Duration::from_secs(1);
        /// Messages per window. `7` bytes of payload each (`b"payload"`).
        const COUNT: u64 = 4;
        const PAYLOAD_LEN: u64 = 7;

        let base = Instant::now();
        let (mut conn, consumer, producer) = stats_interval_conn(Some(INTERVAL), base);

        // Creation-time seeding, mirroring Java's recorder-per-producer. This is
        // load-bearing rather than cosmetic: were the baseline installed by the
        // first sweep instead, a slot on a continuously busy connection could go
        // unswept indefinitely, because the only other deadline such a
        // connection arms is keepalive and its base slides forward on every
        // decoded frame (ADR-0058).
        assert_eq!(
            stats_baselines(&conn, consumer, producer),
            (Some(base), Some(base)),
            "both slots must carry a rate-window baseline the moment they are \
             created, without waiting for a sweep"
        );

        // So the next sample is armed off that baseline — with no `handle_timeout`
        // having run at all — and preempts the 30 s keepalive deadline.
        assert_eq!(
            conn.poll_timeout(),
            Some(base + INTERVAL),
            "poll_timeout must arm the next sample at last_rate_snapshot + stats_interval"
        );

        drive_stats_traffic(&mut conn, consumer, producer, COUNT, base);
        conn.handle_timeout(base + INTERVAL);

        #[allow(
            clippy::cast_precision_loss,
            reason = "COUNT and PAYLOAD_LEN are single-digit test constants"
        )]
        let expected_msgs = COUNT as f64;
        #[allow(clippy::cast_precision_loss, reason = "same as above")]
        let expected_bytes = (COUNT * PAYLOAD_LEN) as f64;
        let ((c_msgs, c_bytes), (p_msgs, p_bytes)) = stats_rates(&conn, consumer, producer);
        for (label, got, want) in [
            ("consumer msgs_per_sec", c_msgs, expected_msgs),
            ("consumer bytes_per_sec", c_bytes, expected_bytes),
            ("producer msgs_per_sec", p_msgs, expected_msgs),
            ("producer bytes_per_sec", p_bytes, expected_bytes),
        ] {
            assert!(
                (got - want).abs() < 1e-9,
                "{label}: the sweep must publish the real delta over the window — \
                 expected {want}, got {got}"
            );
        }
    }

    /// No-false-positive companion: a tick landing INSIDE the window must not
    /// re-sample. Without the `rate_window_due` gate the sweep would re-seed on
    /// every unrelated wake-up (keepalive, a nack redelivery, an ack-grouping
    /// flush), which both shortens the window arbitrarily and makes the
    /// published rate depend on traffic the client happens to be doing —
    /// seed-divergent under moonpool.
    #[test]
    fn stats_interval_sub_window_tick_does_not_resample() {
        const INTERVAL: Duration = Duration::from_secs(10);
        const COUNT: u64 = 4;

        let base = Instant::now();
        let (mut conn, consumer, producer) = stats_interval_conn(Some(INTERVAL), base);

        drive_stats_traffic(&mut conn, consumer, producer, COUNT, base);

        // One second into a ten-second window: due only at `base + INTERVAL`.
        conn.handle_timeout(base + Duration::from_secs(1));
        let ((c_msgs, _), (p_msgs, _)) = stats_rates(&conn, consumer, producer);
        assert!(
            c_msgs.abs() < f64::EPSILON && p_msgs.abs() < f64::EPSILON,
            "a sub-window tick must leave the published rate untouched"
        );
        assert_eq!(
            conn.consumer(consumer)
                .expect("consumer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .map(|(_, _, at)| at),
            Some(base),
            "a sub-window tick must not move the baseline either — otherwise the \
             window silently restarts on every unrelated wake-up"
        );

        // At the deadline it samples, and the window is the full INTERVAL: the
        // same COUNT messages now read as COUNT/10 per second, not COUNT/1.
        conn.handle_timeout(base + INTERVAL);
        let ((c_msgs, _), (p_msgs, _)) = stats_rates(&conn, consumer, producer);
        #[allow(
            clippy::cast_precision_loss,
            reason = "COUNT and INTERVAL are single-digit test constants"
        )]
        let expected = COUNT as f64 / INTERVAL.as_secs_f64();
        assert!(
            (c_msgs - expected).abs() < 1e-9 && (p_msgs - expected).abs() < 1e-9,
            "the published rate must divide by the FULL window, expected {expected}, \
             got consumer={c_msgs} producer={p_msgs}"
        );
    }

    /// The one case creation-time seeding cannot cover: a slot opened BEFORE the
    /// handshake response lands has no `last_activity` to anchor to, so it gets
    /// no baseline at creation. `rate_window_due` treats an unseeded slot as due,
    /// so the first sweep seeds it instead and the slot joins the cadence from
    /// there — rather than being stranded at `0.0` forever.
    #[test]
    fn stats_interval_slot_opened_before_handshake_seeds_on_first_sweep() {
        const INTERVAL: Duration = Duration::from_secs(1);

        let base = Instant::now();
        let mut conn = Connection::new(
            ConnectionConfig {
                stats_interval: Some(INTERVAL),
                ..ConnectionConfig::default()
            },
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Deliberately NO handshake response: `last_activity` is still `None`.
        conn.begin_handshake().expect("handshake");
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/stats-interval-preconnect".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);

        assert!(
            conn.producer(producer)
                .expect("producer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .is_none(),
            "with no last_activity there is no instant to anchor a baseline to"
        );

        // The backstop: the first sweep seeds it, at the sweep's own `now`.
        conn.handle_timeout(base);
        assert_eq!(
            conn.producer(producer)
                .expect("producer slot registered")
                .state
                .lock()
                .last_rate_snapshot
                .map(|(_, _, at)| at),
            Some(base),
            "the first sweep must seed a slot that missed creation-time seeding, \
             otherwise it would never publish a rate at all"
        );
        assert_eq!(
            conn.poll_timeout(),
            Some(base + INTERVAL),
            "and the slot joins the normal cadence from that baseline"
        );
    }

    /// (a) Snapshot formation: a publish in-flight at reset time is moved into
    /// `in_flight_publish_snapshots` and OUT of the producer's `pending` queue, with no
    /// `SessionLost` outcome installed on the publish key.
    #[test]
    fn reset_snapshots_in_flight_publishes_keyed_by_producer_handle() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let producer_a = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-a".to_owned(),
            ..Default::default()
        });
        let producer_b = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-b".to_owned(),
            ..Default::default()
        });

        // Queue three in-flight publishes on A, one on B.
        let mut seqs_a: Vec<SequenceId> = Vec::new();
        for payload in [&b"a0"[..], &b"a1"[..], &b"a2"[..]] {
            let seq = conn
                .send(
                    producer_a,
                    crate::producer::OutgoingMessage {
                        payload: bytes::Bytes::copy_from_slice(payload),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: payload.len() as u32,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    Instant::now(),
                )
                .expect("queue A");
            seqs_a.push(seq);
        }
        let seq_b = conn
            .send(
                producer_b,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"b0"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue B");

        assert_eq!(conn.producer_pending_count(producer_a), 3);
        assert_eq!(conn.producer_pending_count(producer_b), 1);
        // Reset → snapshot.
        conn.reset();
        // No `SessionLost` outcomes for the snapshotted sends (transparent replay).
        for seq in &seqs_a {
            assert!(
                conn.take_outcome(PendingOpKey::Send(producer_a, *seq))
                    .is_none(),
                "no SessionLost outcome for snapshotted send seq={seq:?}"
            );
        }
        assert!(
            conn.take_outcome(PendingOpKey::Send(producer_b, seq_b))
                .is_none(),
            "no SessionLost outcome for snapshotted send on producer B"
        );
        // Snapshot bucket per producer carries the publishes in original FIFO order.
        assert_eq!(conn.in_flight_publish_snapshot_len(producer_a), 3);
        assert_eq!(conn.in_flight_publish_snapshot_len(producer_b), 1);
        // Producer-side pending queue is now empty (drained into the snapshot).
        assert_eq!(conn.producer_pending_count(producer_a), 0);
        assert_eq!(conn.producer_pending_count(producer_b), 0);
    }

    /// (a) SessionLost wake fires exactly once: the user-registered waker on each in-flight
    /// publish is fired by `reset`, and is NOT fired again when the eventual receipt arrives
    /// after the rebuild (the waker is cleared from the snapshot before storage).
    #[test]
    fn reset_wakes_send_future_exactly_once() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn counting_waker() -> (Arc<CountingWake>, Waker) {
            let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
            let waker: Waker = Arc::clone(&inner).into();
            (inner, waker)
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-wake".to_owned(),
            ..Default::default()
        });
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"x"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send");

        let (counter, waker) = counting_waker();
        let key = PendingOpKey::Send(producer, seq);
        // Register the waker on the connection-level slab (the path that the runtime's
        // SendFut uses; the producer-side `register_waker` path is exercised via
        // `apply_receipt` in another test).
        conn.register_waker(key, waker);

        // Reset → exactly one wake fires.
        conn.reset();
        let after_reset = counter.0.load(Ordering::SeqCst);
        assert_eq!(after_reset, 1, "reset must wake the registered waker once");

        // Re-handshake + rebuild → re-issues the publish on the new session.
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = drain_outbound_commands(&mut conn);
        let rebuild_rids = conn.rebuild_producers();
        // The replay is gated on the broker acking the re-attachment
        // (producer-not-ready fix) — feed the `ProducerSuccess` so the
        // snapshot reinstalls into `pending` before the receipt arrives.
        ack_producer_success(&mut conn, rebuild_rids[0].0);
        let _ = drain_outbound_commands(&mut conn);

        // The future "re-polled" — the runtime SendFut would register a fresh waker now.
        let (counter2, waker2) = counting_waker();
        conn.register_waker(key, waker2);

        // Feed the broker's CommandSendReceipt — the replayed OpSend resolves.
        let receipt_bytes = send_receipt_bytes(producer, seq);
        conn.handle_bytes(Instant::now(), &receipt_bytes)
            .expect("handle SendReceipt");

        // Original counter is still at 1 (no double-fire); new counter fired once.
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the original waker must NOT fire again — it was cleared from the snapshot"
        );
        assert_eq!(
            counter2.0.load(Ordering::SeqCst),
            1,
            "the freshly-registered waker fires exactly once on the replayed receipt"
        );
    }

    /// Issue #369, positive case: a send relocated into
    /// `in_flight_publish_snapshots` by `reset()` still surfaces the configured
    /// `send_timeout` at the ORIGINAL `enqueued_at` deadline — it must not hang
    /// for the whole reconnect budget. Models the future's re-poll-and-reregister
    /// behaviour documented on `reset()`: the waker fires once at reset (no
    /// outcome installed), the future re-registers, and only the later
    /// `handle_timeout` sweep past the deadline resolves it.
    #[test]
    fn send_timeout_fires_for_publish_relocated_across_reset() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn counting_waker() -> (Arc<CountingWake>, Waker) {
            let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
            let waker: Waker = Arc::clone(&inner).into();
            (inner, waker)
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-timeout-relocated".to_owned(),
            send_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let t0 = Instant::now();
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"relocated"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 9,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        let (counter, waker) = counting_waker();
        conn.register_waker(key, waker);

        // Relocate: reset() moves the op out of `pending` into the snapshot
        // bucket and wakes the future exactly once with no outcome installed.
        conn.reset();
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "reset must wake the parked send future once"
        );
        assert!(
            conn.take_outcome(key).is_none(),
            "reset installs no outcome on the Send key — transparent replay contract"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            1,
            "the relocated send must land in the snapshot bucket"
        );

        // The future re-polls after the wake-up and re-registers, exactly as
        // `reset()`'s doc comment describes.
        let (counter2, waker2) = counting_waker();
        conn.register_waker(key, waker2);

        // `poll_timeout` must surface a wake-up deadline for the relocated
        // send too — this is what lets the moonpool engine arm a deterministic
        // virtual timer for it (issue #369, Change 1a).
        assert!(
            conn.poll_timeout().is_some(),
            "poll_timeout must surface a deadline for a relocated in-flight send"
        );

        // Past the ORIGINAL enqueued_at + send_timeout deadline (measured from
        // t0, NOT from the reset): the sweep resolves the send with the same
        // timeout error the live-queue path installs and drains the snapshot.
        conn.handle_timeout(t0 + Duration::from_secs(31));
        match conn.take_outcome(key) {
            Some(OpOutcome::SendError {
                sequence_id,
                code,
                message,
            }) => {
                assert_eq!(sequence_id, seq);
                assert_eq!(code, -1, "send-timeout SendError uses the -1 sentinel");
                assert_eq!(message, "send timeout");
            }
            other => panic!("expected a send-timeout SendError, got {other:?}"),
        }
        assert_eq!(
            counter2.0.load(Ordering::SeqCst),
            1,
            "the re-registered waker must fire exactly once on the send-timeout sweep"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            0,
            "the timed-out relocated send must drain out of the snapshot bucket"
        );
    }

    /// Issue #369, negative case (no false positive): a send relocated by
    /// `reset()` must NOT resolve — and its snapshot must survive — while the
    /// configured `send_timeout` has not yet elapsed. Guards against an
    /// overly-eager sweep that fires on every `reset()` regardless of deadline.
    #[test]
    fn send_timeout_does_not_fire_early_for_publish_relocated_across_reset() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-timeout-relocated-early".to_owned(),
            send_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let t0 = Instant::now();
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"relocated-early"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 15,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        conn.reset();
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 1);

        // Well before the 30s deadline.
        conn.handle_timeout(t0 + Duration::from_secs(10));
        assert!(
            conn.take_outcome(key).is_none(),
            "no send-timeout outcome before the deadline for a relocated send"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            1,
            "the snapshot must survive an early handle_timeout tick"
        );
    }

    /// Issue #369, no-timeout guard: a publish relocated by `reset()` on a
    /// producer opened with `send_timeout: None` must never resolve via the
    /// synthetic timeout sweep, no matter how far the virtual clock advances.
    /// Exercises the `None`-timeout branch in both the `poll_timeout` and
    /// `handle_timeout` sweeps added for the relocated-snapshot case, mirroring
    /// `drain_timed_out_sends_without_timeout_returns_empty`'s coverage of the
    /// same guard on the live-queue path.
    #[test]
    fn send_timeout_disabled_never_fires_for_publish_relocated_across_reset() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-timeout-disabled-relocated".to_owned(),
            send_timeout: None,
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let t0 = Instant::now();
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"relocated-no-timeout"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 20,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        conn.reset();
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 1);

        // poll_timeout must not contribute a deadline sourced from this
        // relocated send (it may still return Some from keepalive — that is
        // fine and orthogonal; the assertion below is on handle_timeout's
        // effect, which is the authoritative check that this producer's
        // relocated send is never touched by the sweep).
        let _ = conn.poll_timeout();

        // Advance the virtual clock a full hour — an eternity relative to
        // any realistic send_timeout — and tick handle_timeout. With no
        // timeout configured, the relocated send must survive untouched.
        conn.handle_timeout(t0 + Duration::from_hours(1));
        assert!(
            conn.take_outcome(key).is_none(),
            "send_timeout: None must never synthesize a timeout outcome for a relocated send"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            1,
            "the snapshot must survive indefinitely when send_timeout is disabled"
        );
    }

    /// Issue #369, Change 2: `fail_all_pending` must be self-sufficient for
    /// publishes relocated by a prior `reset()` — it must terminalize them from
    /// `in_flight_publish_snapshots` directly rather than depending on the
    /// woken send future having already re-registered a waker into the
    /// connection-wide slab. Calls `fail_all_pending` immediately after
    /// `reset()`, WITHOUT any prior waker re-registration (the race the
    /// superseded scheduler-timing justification described — the woken task
    /// has not yet been rescheduled and re-polled), mirroring
    /// `fail_producer_open_with_broker_error`'s snapshot drain.
    #[test]
    fn fail_all_pending_terminalizes_publishes_relocated_across_reset() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker_inner = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&waker_inner).into();

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/fail-all-pending-relocated".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"give-up"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 7,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        conn.register_waker(key, waker);

        // Relocate via reset() — this is the ONLY waker registration in this
        // test. `reset()` fires it once (transparent-replay contract, no
        // outcome installed) and clears it from every correlation surface; we
        // deliberately do NOT re-register a fresh waker afterwards, so when
        // `fail_all_pending` runs below there is nothing left in
        // `self.wakers` for this key — proving the Terminal outcome lands
        // even when the woken task has not yet re-polled and re-registered.
        conn.reset();
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            1,
            "reset must wake the parked send future exactly once"
        );
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 1);
        assert!(conn.take_outcome(key).is_none());

        // Give up: the supervisor exhausted its reconnect budget.
        conn.fail_all_pending("reconnect attempts exhausted");

        match conn.take_outcome(key) {
            Some(OpOutcome::Terminal { key: k, reason }) => {
                assert_eq!(k, key);
                assert_eq!(reason, "reconnect attempts exhausted");
            }
            other => panic!("expected a Terminal outcome, got {other:?}"),
        }
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            0,
            "fail_all_pending must drain the relocated snapshot bucket"
        );
        // No waker was registered for this key at `fail_all_pending` time (it
        // was consumed by `reset()` and never re-registered), so the counter
        // stays at 1 — the outcome install does not depend on a waker being
        // present, and nothing double-fires.
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            1,
            "fail_all_pending must not double-fire a waker it cannot find"
        );
    }

    /// Issue #369, Change 2 — companion to
    /// `fail_all_pending_terminalizes_publishes_relocated_across_reset`: this
    /// time the send future DOES re-poll and re-register a fresh waker into
    /// the connection-wide slab after `reset()` relocates it (mirroring
    /// `send_timeout_fires_for_publish_relocated_across_reset`'s shape for the
    /// Change-1 path), so `snapshot.waker` is `None` and `fail_all_pending`
    /// must fall through to the `self.wakers.remove(&key)` arm instead —
    /// exercising the more common real-world case where the parked send has
    /// already been rescheduled by the time the reconnect budget is
    /// exhausted.
    #[test]
    fn fail_all_pending_wakes_reregistered_waker_for_publish_relocated_across_reset() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn counting_waker() -> (Arc<CountingWake>, Waker) {
            let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
            let waker: Waker = Arc::clone(&inner).into();
            (inner, waker)
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/fail-all-pending-relocated-reregistered".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"give-up-reregistered"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 21,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        let (counter, waker) = counting_waker();
        conn.register_waker(key, waker);

        // Relocate: reset() moves the op into the snapshot bucket and wakes
        // the parked future exactly once with no outcome installed.
        conn.reset();
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "reset must wake the parked send future once"
        );
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 1);
        assert!(conn.take_outcome(key).is_none());

        // The future re-polls after the wake-up and re-registers, exactly as
        // `send_timeout_fires_for_publish_relocated_across_reset` models —
        // this leaves `snapshot.waker` empty (it was taken by `reset()`) but
        // populates `self.wakers` for the same key.
        let (counter2, waker2) = counting_waker();
        conn.register_waker(key, waker2);

        // Give up: the supervisor exhausted its reconnect budget. The
        // snapshot's own `waker` field is `None`, so the Change-2 drain must
        // take the `else if let Some(w) = self.wakers.remove(&key)` arm to
        // fire the re-registered waker rather than silently dropping it.
        conn.fail_all_pending("reconnect attempts exhausted");

        match conn.take_outcome(key) {
            Some(OpOutcome::Terminal { key: k, reason }) => {
                assert_eq!(k, key);
                assert_eq!(reason, "reconnect attempts exhausted");
            }
            other => panic!("expected a Terminal outcome, got {other:?}"),
        }
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            0,
            "fail_all_pending must drain the relocated snapshot bucket"
        );
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the original reset-time waker must not fire again"
        );
        assert_eq!(
            counter2.0.load(Ordering::SeqCst),
            1,
            "the re-registered connection-slab waker must fire exactly once via the \
             self.wakers fallback arm"
        );
    }

    /// `unregister_waker` removes the registered waker so a subsequent dispatch
    /// (or `reset`) does not wake the now-discarded task. The companion waker
    /// for an unrelated request must still fire. Covers the lookup multi-agent
    /// review MEDIUM-4 finding: futures that register wakers must clear them
    /// on drop or the slab leaks one entry per cancelled request.
    #[test]
    fn unregister_waker_drops_request_entry_without_disturbing_siblings() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn counting_waker() -> (Arc<CountingWake>, Waker) {
            let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
            let waker: Waker = Arc::clone(&inner).into();
            (inner, waker)
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        // Register two request wakers (request ids 100 and 101 — neither will
        // ever receive a broker response in this test).
        let key_a = PendingOpKey::Request(RequestId(100));
        let key_b = PendingOpKey::Request(RequestId(101));
        let (counter_a, waker_a) = counting_waker();
        let (counter_b, waker_b) = counting_waker();
        conn.register_waker(key_a, waker_a);
        conn.register_waker(key_b, waker_b);
        assert_eq!(
            conn.pending_waker_count(),
            2,
            "two distinct request wakers parked"
        );

        // Drop request A's waker via `unregister_waker` (the path the runtime's
        // `RequestFut::drop` will take).
        conn.unregister_waker(key_a);
        assert_eq!(
            conn.pending_waker_count(),
            1,
            "unregister_waker drains exactly one slot"
        );

        // Re-registering is idempotent — it inserts a fresh entry, so the slab
        // grows back to two.
        let (_counter_a_redo, waker_a_redo) = counting_waker();
        conn.register_waker(key_a, waker_a_redo);
        assert_eq!(conn.pending_waker_count(), 2);
        conn.unregister_waker(key_a);

        // Tear the connection down — `reset` must NOT fire the unregistered
        // waker, but should fire request B's waker (siblings are untouched).
        conn.reset();
        assert_eq!(
            counter_a.0.load(Ordering::SeqCst),
            0,
            "the un-unregistered waker must NOT fire on reset"
        );
        assert_eq!(
            counter_b.0.load(Ordering::SeqCst),
            1,
            "the un-touched sibling waker fires exactly once on reset"
        );
    }

    #[test]
    fn cancel_request_releases_lookup_capacity_and_is_idempotent() {
        let mut conn = Connection::new(
            ConnectionConfig {
                max_pending_lookups: 1,
                ..ConnectionConfig::default()
            },
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let first = conn.lookup("persistent://public/default/cancel-first", false);
        conn.register_waker(
            PendingOpKey::Request(first),
            std::task::Waker::noop().clone(),
        );
        assert!(conn.has_pending_request_for_test(first));
        assert_eq!(conn.pending_waker_count(), 1);

        let rejected = conn.lookup("persistent://public/default/rejected", false);
        assert!(
            !conn.has_pending_request_for_test(rejected),
            "the one-slot lookup registry must reject while the first request is pending"
        );

        conn.cancel_request(first);
        conn.cancel_request(first);
        assert!(!conn.has_pending_request_for_test(first));
        assert_eq!(conn.pending_waker_count(), 0);
        assert!(
            conn.take_outcome(PendingOpKey::Request(first)).is_none(),
            "cancellation must discard an already-landed outcome too"
        );

        let after_cancel = conn.lookup("persistent://public/default/after-cancel", false);
        assert!(
            conn.has_pending_request_for_test(after_cancel),
            "cancellation must return lookup registry capacity to the caller"
        );
    }

    #[test]
    fn cancelled_attachment_late_replies_do_not_recreate_outcomes() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}

        let producer_request_id = RequestId(conn.peek_next_request_id_for_test());
        let _producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/cancelled-producer-open".to_owned(),
            ..Default::default()
        });
        conn.cancel_request(producer_request_id);
        feed_transient_error(&mut conn, producer_request_id);
        assert!(
            conn.take_outcome(PendingOpKey::Request(producer_request_id))
                .is_none(),
            "a late producer-open error must stay ignored after cancellation"
        );

        let consumer_request_id = RequestId(conn.peek_next_request_id_for_test());
        let _consumer = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/cancelled-subscribe".to_owned(),
            subscription: "cancelled".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        conn.cancel_request(consumer_request_id);
        feed_subscribe_success(&mut conn, consumer_request_id.0);
        assert!(
            conn.take_outcome(PendingOpKey::Request(consumer_request_id))
                .is_none(),
            "a late subscribe success must stay ignored after cancellation"
        );
    }

    #[test]
    fn cancelling_completed_attachment_purges_unowned_ready_event() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        while conn.poll_event().is_some() {}

        let producer_request_id = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/completed-cancel-producer".to_owned(),
            ..Default::default()
        });
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: producer_request_id,
                producer_name: "cancelled".to_owned(),
                last_sequence_id: Some(-1),
                producer_ready: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = bytes::BytesMut::new();
        encode_command(&mut frame, &success).expect("encode ProducerSuccess");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle ProducerSuccess");
        conn.events
            .push_back(ConnectionEvent::ProducerClosedByBroker {
                handle: producer,
                assigned_broker_service_url: None,
            });
        conn.events.push_back(ConnectionEvent::ProducerOpenFailed {
            handle: producer,
            code: pb::ServerError::TopicNotFound as i32,
            message: "late failure".to_owned(),
        });
        conn.events
            .push_back(ConnectionEvent::ProducerOpenFailedTransient {
                handle: producer,
                code: pb::ServerError::ServiceNotReady as i32,
                message: "late retry".to_owned(),
            });
        conn.driver_retries.push_back(crate::DriverRetry::Producer {
            handle: producer,
            failed_request_id: RequestId(producer_request_id),
            code: pb::ServerError::ServiceNotReady as i32,
            message: "late retry".to_owned(),
        });
        conn.cancel_producer_open(producer);
        assert!(
            !conn.events.iter().any(|event| matches!(
                event,
                ConnectionEvent::ProducerReady { handle, .. }
                    | ConnectionEvent::ProducerClosedByBroker { handle, .. }
                    | ConnectionEvent::ProducerOpenFailed { handle, .. }
                    | ConnectionEvent::ProducerOpenFailedTransient { handle, .. }
                    if *handle == producer
            )),
            "cancelling must remove every now-unowned producer attachment event"
        );
        assert!(!conn.driver_retries.iter().any(
            |retry| matches!(retry, crate::DriverRetry::Producer { handle, .. } if *handle == producer)
        ));

        let consumer_request_id = conn.peek_next_request_id_for_test();
        let consumer = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/completed-cancel-consumer".to_owned(),
            subscription: "cancelled".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        feed_subscribe_success(&mut conn, consumer_request_id);
        conn.events
            .push_back(ConnectionEvent::ConsumerClosedByBroker {
                handle: consumer,
                assigned_broker_service_url: None,
            });
        conn.events.push_back(ConnectionEvent::SubscribeFailed {
            handle: consumer,
            code: pb::ServerError::TopicNotFound as i32,
            message: "late failure".to_owned(),
        });
        conn.events
            .push_back(ConnectionEvent::SubscribeFailedTransient {
                handle: consumer,
                code: pb::ServerError::ServiceNotReady as i32,
                message: "late retry".to_owned(),
            });
        conn.driver_retries.push_back(crate::DriverRetry::Consumer {
            handle: consumer,
            failed_request_id: RequestId(consumer_request_id),
            code: pb::ServerError::ServiceNotReady as i32,
            message: "late retry".to_owned(),
        });
        conn.cancel_consumer_subscribe(consumer);
        assert!(
            !conn.events.iter().any(|event| matches!(
                event,
                ConnectionEvent::SubscribeAcked { handle, .. }
                    | ConnectionEvent::ConsumerClosedByBroker { handle, .. }
                    | ConnectionEvent::SubscribeFailed { handle, .. }
                    | ConnectionEvent::SubscribeFailedTransient { handle, .. }
                    if *handle == consumer
            )),
            "cancelling must remove every now-unowned consumer attachment event"
        );
        assert!(!conn.driver_retries.iter().any(
            |retry| matches!(retry, crate::DriverRetry::Consumer { handle, .. } if *handle == consumer)
        ));
    }

    #[test]
    fn cancel_request_removes_an_uninitialised_topic_watcher() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let request_id = conn.watch_topic_list("public/default", "events-.*");

        assert!(conn.topic_watchers.lookup_by_request(request_id).is_some());
        conn.cancel_request(request_id);

        assert!(
            conn.topic_watchers.lookup_by_request(request_id).is_none(),
            "cancelling the initial snapshot must remove the watcher registry entry"
        );
    }

    /// `unregister_waker` on a [`PendingOpKey::Send`] key clears the
    /// producer-slot waker too (the dispatcher prefers the slot-stored
    /// waker over the connection-wide slab, per `register_waker`'s split).
    /// Otherwise dropping a `SendFut` could leave a stale waker on the
    /// `ProducerState::pending` entry that fires when the receipt arrives.
    #[test]
    fn unregister_waker_clears_producer_slot_send_waker() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/unregister-send".to_owned(),
            ..Default::default()
        });
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"x"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send");

        let key = PendingOpKey::Send(producer, seq);
        let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&inner).into();
        conn.register_waker(key, waker);

        // Connection-wide slab is empty because `register_waker` stashed the
        // waker on the matching `ProducerSlot` instead.
        assert_eq!(
            conn.pending_waker_count(),
            0,
            "Send waker lives on the producer slot, not the connection slab"
        );

        // Unregister — the producer-slot waker is dropped too.
        conn.unregister_waker(key);

        // Reset must NOT fire the (now-dropped) waker.
        conn.reset();
        assert_eq!(
            inner.0.load(Ordering::SeqCst),
            0,
            "unregister_waker clears the producer-slot waker; reset must not fire it"
        );
    }

    /// (a) Rebuild re-populates pending: after `rebuild_producers`, the snapshot bucket is
    /// drained and the producer's `pending` queue contains the same OpSends in the same
    /// order. The replayed `CommandSend` frames hit the outbound buffer.
    #[test]
    fn rebuild_producers_replays_snapshotted_publishes_with_original_sequence_ids() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-pending".to_owned(),
            ..Default::default()
        });
        // Discard initial `CommandProducer` frame, then ack it so the drain
        // gate opens and the pre-reset sends can reach the wire.
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        // Queue three publishes and drain their wire frames so the post-replay drain is
        // isolated.
        let mut seqs: Vec<SequenceId> = Vec::new();
        for i in 0..3 {
            let seq = conn
                .send(
                    producer,
                    crate::producer::OutgoingMessage {
                        payload: bytes::Bytes::copy_from_slice(format!("p{i}").as_bytes()),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: 2,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    Instant::now(),
                )
                .expect("queue");
            seqs.push(seq);
        }
        let _ = drain_outbound_commands(&mut conn);

        // Snapshot, re-handshake, rebuild.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect");
        let _ = drain_outbound_commands(&mut conn);

        let rebuild_rids = conn.rebuild_producers();

        // Producer-not-ready gate: until the broker acks the re-attachment, the
        // snapshots stay parked and NO send frame may reach the wire — only the
        // rebuild's `CommandProducer` goes out (a premature send makes the real
        // broker close the whole connection).
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 3);
        assert_eq!(conn.producer_pending_count(producer), 0);
        let pre_ack_cmds = drain_outbound_commands(&mut conn);
        assert!(
            pre_ack_cmds
                .iter()
                .any(|c| c.r#type == pb::base_command::Type::Producer as i32),
            "rebuild must re-emit CommandProducer"
        );
        assert!(
            pre_ack_cmds
                .iter()
                .all(|c| c.r#type != pb::base_command::Type::Send as i32),
            "no CommandSend may go out before ProducerSuccess"
        );

        // Broker acks the re-attachment — the snapshot is consumed; pending now
        // holds the three replayed OpSends in original order.
        ack_producer_success(&mut conn, rebuild_rids[0].0);
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 0);
        assert_eq!(conn.producer_pending_count(producer), 3);

        // The outbound buffer now carries the three `CommandSend` frames in the
        // original `[0, 1, 2]` sequence-id order.
        let cmds = drain_outbound_commands(&mut conn);
        let sends: Vec<&pb::CommandSend> = cmds
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Send as i32)
            .filter_map(|c| c.send.as_ref())
            .collect();
        assert_eq!(sends.len(), 3, "three sends must be re-issued");
        let observed_seqs: Vec<u64> = sends.iter().map(|s| s.sequence_id).collect();
        let expected_seqs: Vec<u64> = seqs.iter().map(|s| s.0).collect();
        assert_eq!(
            observed_seqs, expected_seqs,
            "replay preserves FIFO + original sequence ids"
        );
    }

    /// (a) `apply_receipt` resolves the re-issued send: after rebuild, feeding a
    /// `CommandSendReceipt` for one of the replayed sequence ids drops it from `pending`
    /// and surfaces the `OpOutcome::SendReceipt` on the outcome slab — the user-facing
    /// SendFut observes the outcome as if the original session had simply lasted longer.
    #[test]
    fn apply_receipt_resolves_replayed_send_after_rebuild() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-receipt".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue");
        let _ = drain_outbound_commands(&mut conn);

        // Snapshot + replay.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = drain_outbound_commands(&mut conn);
        let rebuild_rids = conn.rebuild_producers();
        // Broker acks the re-attachment — only then is the snapshot replayed
        // (producer-not-ready gate).
        ack_producer_success(&mut conn, rebuild_rids[0].0);
        let _ = drain_outbound_commands(&mut conn);

        // Replayed OpSend is back in pending.
        assert_eq!(conn.producer_pending_count(producer), 1);
        let key = PendingOpKey::Send(producer, seq);
        assert!(
            conn.take_outcome(key).is_none(),
            "no outcome before broker receipt lands"
        );

        // Feed the receipt for the replayed sequence id — pending drains and the outcome
        // lands.
        let receipt_bytes = send_receipt_bytes(producer, seq);
        conn.handle_bytes(Instant::now(), &receipt_bytes)
            .expect("handle SendReceipt");

        assert_eq!(
            conn.producer_pending_count(producer),
            0,
            "the replayed OpSend must drain on receipt"
        );
        match conn.take_outcome(key) {
            Some(OpOutcome::SendReceipt {
                sequence_id,
                message_id,
            }) => {
                assert_eq!(sequence_id, seq);
                assert_eq!(message_id.entry_id, seq.0);
            }
            other => panic!("expected SendReceipt for the replayed send, got {other:?}"),
        }
    }

    /// ADR-0072 — Java-parity default `send_timeout`. A producer opened from a
    /// `CreateProducerRequest::default()` carries `Some(30s)`, so a send whose
    /// `CommandSendReceipt` never arrives (lost / corrupted on the wire — the
    /// receipt has no CRC32C, invariant #4) does NOT hang forever: once the
    /// INJECTED clock (ADR-0011) crosses `enqueued_at + 30s`, the
    /// `handle_timeout` sweep resolves the `PendingOpKey::Send` future with a
    /// `code=-1, "send timeout"` `SendError` and wakes the parked waker. The
    /// companion `send_resolves_before_default_deadline_without_false_timeout`
    /// pins the no-false-positive direction.
    #[test]
    fn default_send_timeout_fires_when_receipt_lost() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker_inner = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&waker_inner).into();

        // Pin the default itself: the Java client's sendTimeoutMs = 30000.
        let default_req = CreateProducerRequest::default();
        assert_eq!(
            default_req.send_timeout,
            Some(Duration::from_secs(30)),
            "CreateProducerRequest::default() must carry the 30s Java-parity send_timeout"
        );

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-timeout-default".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        // Enqueue a single send at a fixed `t0` on the injected clock.
        let t0 = Instant::now();
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"lost-receipt"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 12,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);

        let key = PendingOpKey::Send(producer, seq);
        conn.register_waker(key, waker.clone());

        // A wake-up deadline is surfaced via poll_timeout against the injected
        // clock (the earliest of keepalive + the new send deadline), so the
        // driver schedules a deterministic wake to drive the sweep.
        assert!(
            conn.poll_timeout().is_some(),
            "a wake-up deadline must be scheduled while a send is in flight"
        );

        // Just BEFORE the deadline: no timeout, no wake, no outcome — the
        // broker still had a chance to ack.
        conn.handle_timeout(t0 + Duration::from_secs(29));
        assert!(
            conn.take_outcome(key).is_none(),
            "no send-timeout outcome before the 30s deadline"
        );
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            0,
            "waker must not fire before the deadline"
        );

        // Past the deadline: the sweep resolves the send with a timeout error
        // and wakes the parked waker.
        conn.handle_timeout(t0 + Duration::from_secs(31));
        match conn.take_outcome(key) {
            Some(OpOutcome::SendError {
                sequence_id,
                code,
                message,
            }) => {
                assert_eq!(sequence_id, seq);
                assert_eq!(code, -1, "send-timeout SendError uses the -1 sentinel");
                assert_eq!(message, "send timeout");
            }
            other => panic!("expected a send-timeout SendError, got {other:?}"),
        }
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            1,
            "the parked waker must be woken exactly once on timeout"
        );
        assert_eq!(
            conn.producer_pending_count(producer),
            0,
            "the timed-out send must drain out of the pending queue"
        );
    }

    /// ADR-0072 no-false-positive: a send whose `CommandSendReceipt` lands
    /// BEFORE the 30s default deadline resolves normally with a `SendReceipt`
    /// outcome — the default timeout must not spuriously fail a healthy send.
    #[test]
    fn send_resolves_before_default_deadline_without_false_timeout() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/send-timeout-happy".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let t0 = Instant::now();
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"acked"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 5,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        let _ = drain_outbound_commands(&mut conn);
        let key = PendingOpKey::Send(producer, seq);

        // Broker acks well within the 30s window.
        let receipt_bytes = send_receipt_bytes(producer, seq);
        conn.handle_bytes(t0 + Duration::from_secs(1), &receipt_bytes)
            .expect("handle SendReceipt");

        match conn.take_outcome(key) {
            Some(OpOutcome::SendReceipt { sequence_id, .. }) => assert_eq!(sequence_id, seq),
            other => panic!("expected a SendReceipt before the deadline, got {other:?}"),
        }

        // A later sweep past the would-be deadline finds nothing to time out —
        // the send already drained, so no spurious second outcome.
        conn.handle_timeout(t0 + Duration::from_secs(31));
        assert!(
            conn.take_outcome(key).is_none(),
            "no spurious send-timeout outcome after a healthy ack"
        );
    }

    /// Ordering invariant: when a producer has multiple in-flight publishes with
    /// non-contiguous sequence ids (one batched + one single), the snapshot replays them
    /// in original FIFO order, preserving the per-producer wire ordering.
    #[test]
    fn replay_preserves_ordering_across_rebuild() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-order".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        // Three single sends — sequence ids 0, 1, 2.
        let mut expected_payloads: Vec<&'static [u8]> = Vec::new();
        for payload in [&b"first"[..], &b"second"[..], &b"third"[..]] {
            let _ = conn
                .send(
                    producer,
                    crate::producer::OutgoingMessage {
                        payload: bytes::Bytes::from_static(payload),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: payload.len() as u32,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    Instant::now(),
                )
                .expect("queue");
            expected_payloads.push(payload);
        }
        let _ = drain_outbound_commands(&mut conn);

        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle reconnect");
        let _ = drain_outbound_commands(&mut conn);
        let rebuild_rids = conn.rebuild_producers();
        // Broker acks the re-attachment — only then are the snapshots replayed
        // (producer-not-ready gate).
        ack_producer_success(&mut conn, rebuild_rids[0].0);

        // The post-ack outbound buffer carries the three replayed CommandSend
        // frames in FIFO order. Decode payloads to verify.
        let raw_bytes = conn.poll_transmit();
        let mut cursor = bytes::Bytes::copy_from_slice(&raw_bytes);
        let mut send_payloads: Vec<Vec<u8>> = Vec::new();
        while !cursor.is_empty() {
            let frame = crate::frame::decode_one(&mut cursor).expect("decode frame");
            if frame.command.r#type == pb::base_command::Type::Send as i32 {
                let body = frame
                    .payload
                    .as_ref()
                    .expect("SEND frame must carry a payload region")
                    .body
                    .clone();
                send_payloads.push(body.to_vec());
            }
        }
        assert_eq!(send_payloads.len(), 3, "all three replayed sends present");
        for (i, expected) in expected_payloads.iter().enumerate() {
            assert_eq!(
                send_payloads[i].as_slice(),
                *expected,
                "replay preserves original payload at position {i}"
            );
        }
    }

    /// A send future that re-polls DURING the reset → `ProducerSuccess`
    /// window (its op parked in the reset snapshot, not in `pending`) must
    /// still be woken by the replayed receipt. `Connection::register_waker`
    /// used to hand the waker to the slot unconditionally — where it
    /// silently no-oped for snapshot-parked ops — instead of falling back
    /// to the connection-wide slab; the receipt then resolved with no waker
    /// anywhere and the user's send hung forever (the e2e_reconnect
    /// starvation, root cause #2 behind the pre-ack-replay livelock).
    #[test]
    fn waker_registered_during_snapshot_window_fires_on_replayed_receipt() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/snapshot-window-waker".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"x"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue");
        let _ = drain_outbound_commands(&mut conn);

        // Drop the session: the op moves into the reset snapshot.
        conn.reset();

        // The send future re-polls NOW — mid-window, before rebuild/ack.
        // This registration must not be silently dropped.
        let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&counter).into();
        conn.register_waker(PendingOpKey::Send(producer, seq), waker);

        // Re-handshake, rebuild, broker ack → snapshot replays.
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = drain_outbound_commands(&mut conn);
        let rebuild_rids = conn.rebuild_producers();
        ack_producer_success(&mut conn, rebuild_rids[0].0);
        let _ = drain_outbound_commands(&mut conn);

        // The replayed publish's receipt lands — the mid-window waker MUST fire.
        let receipt_bytes = send_receipt_bytes(producer, seq);
        conn.handle_bytes(Instant::now(), &receipt_bytes)
            .expect("handle SendReceipt");
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the waker registered during the snapshot window must fire on the replayed receipt"
        );
        assert!(
            matches!(
                conn.take_outcome(PendingOpKey::Send(producer, seq)),
                Some(OpOutcome::SendReceipt { .. })
            ),
            "the outcome must be present for the woken future to consume"
        );
    }

    /// The live e2e_reconnect flow, at the proto layer: a send queued while
    /// disconnected, then reset → rebuild → broker answers the rebuild's
    /// `CommandProducer` with a TRANSIENT error (`ServiceNotReady` — the
    /// post-restart "namespace bundle not served, redo the lookup" case) →
    /// `retry_producer_open` → broker acks the retry with
    /// `ProducerSuccess`. The queued send must reach the wire exactly once,
    /// only after the ack (producer-not-ready gate), with its original
    /// sequence id.
    #[test]
    fn transient_rebuild_error_then_retry_ack_replays_queued_send() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let create_rid = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/transient-replay".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);
        ack_producer_success(&mut conn, create_rid);

        // Queue one send and let it reach the wire (in-flight at drop time).
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"inflight"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 8,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue");
        let _ = drain_outbound_commands(&mut conn);

        // Drop + reconnect + rebuild.
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = drain_outbound_commands(&mut conn);
        let rebuild_rids = conn.rebuild_producers();
        let _ = drain_outbound_commands(&mut conn);

        // Broker rejects the rebuild's CommandProducer with a TRANSIENT code
        // (ServiceNotReady = 6) — the post-restart bundle-not-served case.
        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: rebuild_rids[0].0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "Please redo the lookup".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle transient error");
        // The transient event surfaces; producer state survives.
        let mut saw_transient = false;
        while let Some(ev) = conn.poll_event() {
            if matches!(ev, ConnectionEvent::ProducerOpenFailedTransient { .. }) {
                saw_transient = true;
            }
        }
        assert!(saw_transient, "transient open failure must surface");

        // Driver retry path: re-emit CommandProducer for the single handle.
        let retry_rid = conn
            .retry_producer_open_if_current(producer, rebuild_rids[0])
            .expect("retry must re-emit");
        let pre_ack = drain_outbound_commands(&mut conn);
        assert!(
            pre_ack
                .iter()
                .any(|c| c.r#type == pb::base_command::Type::Producer as i32),
            "retry must re-emit CommandProducer"
        );
        assert!(
            pre_ack
                .iter()
                .all(|c| c.r#type != pb::base_command::Type::Send as i32),
            "no CommandSend may go out before the retry's ProducerSuccess"
        );

        // Broker acks the retry — the queued send must now reach the wire,
        // exactly once, with its original sequence id.
        ack_producer_success(&mut conn, retry_rid.0);
        let post_ack = drain_outbound_commands(&mut conn);
        let sends: Vec<&pb::CommandSend> = post_ack
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Send as i32)
            .filter_map(|c| c.send.as_ref())
            .collect();
        assert_eq!(
            sends.len(),
            1,
            "exactly one replayed send after the retry ack; got commands: {:?}",
            post_ack.iter().map(|c| c.r#type).collect::<Vec<_>>()
        );
        assert_eq!(
            sends[0].sequence_id, seq.0,
            "original sequence id preserved"
        );
    }

    /// Batch cleared on reset: messages buffered in the producer's batch container (i.e.
    /// not yet flushed to a wire frame) do not survive the reset — caller is responsible
    /// for re-sending those (matches Java `ProducerImpl#connectionClosed` which drops the
    /// in-progress batch). Only frames that already hit the wire's pending queue replay.
    #[test]
    fn reset_clears_batch_container_does_not_replay_unbatched_stragglers() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle");

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-batch".to_owned(),
            enable_batching: true,
            max_batch_size_bytes: 4096,
            max_messages_in_batch: 100,
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);

        // Two batched (un-flushed) sends — neither hits the wire.
        for payload in [&b"a"[..], &b"b"[..]] {
            let _ = conn
                .send(
                    producer,
                    crate::producer::OutgoingMessage {
                        payload: bytes::Bytes::from_static(payload),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: 1,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    Instant::now(),
                )
                .expect("queue");
        }
        // Batched: each send now mints its own per-message `OpSend` so the user-side
        // `SendFut` has a unique key to wait on. The batch container also still holds the
        // raw bytes until `flush_batch` builds the wire frame, so we expect two pending
        // entries AND two batch entries.
        assert_eq!(conn.producer_pending_count(producer), 2);
        assert_eq!(conn.producer_batch_len(producer), 2);

        // Reset: the batch is dropped; the per-message `OpSend` entries are also dropped
        // and carry no `replay_frames`, so `in_flight_publish_snapshot` is empty —
        // matching Java `ProducerImpl#connectionClosed` which fails an in-progress batch
        // rather than re-emitting the partial bytes.
        conn.reset();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(producer),
            0,
            "unflushed batched sends are NOT replayed — caller's responsibility"
        );
        assert_eq!(conn.producer_batch_len(producer), 0);
    }

    // -------------------------------------------------------------------
    // PIP-33 — Replicated-subscription tests (ADR-0034).
    //
    // - command_subscribe_with_replicate_state_{true,false}: assert encoder sets / omits
    //   CommandSubscribe field 14 (`replicate_subscription_state`).
    // - consumer_filters_replicated_marker_*: assert receive-path filter drops kinds 10..=13 from
    //   the user-visible event stream and emits `ReplicatedSubscriptionMarkerObserved` instead.
    // - consumer_passes_through_*: regression guards for non-marker messages and txn markers
    //   (unchanged behaviour).
    // -------------------------------------------------------------------

    fn marker_metadata(kind: i32) -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "broker-marker".to_owned(),
            sequence_id: 0,
            publish_time: 1_700_000_000_000,
            marker_type: Some(kind),
            ..Default::default()
        }
    }

    fn regular_metadata() -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "producer".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000_000,
            num_messages_in_batch: Some(1),
            ..Default::default()
        }
    }

    fn message_frame(consumer_id: u64, meta: &pb::MessageMetadata, payload: &[u8]) -> Vec<u8> {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id,
                message_id: pb::MessageIdData {
                    ledger_id: 1,
                    entry_id: 1,
                    partition: None,
                    batch_index: None,
                    ack_set: Vec::new(),
                    batch_size: None,
                    first_chunk_message_id: None,
                },
                redelivery_count: Some(0),
                ack_set: Vec::new(),
                consumer_epoch: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        crate::frame::encode_payload(&mut buf, &cmd, meta, payload).expect("encode_payload");
        buf.to_vec()
    }

    fn handshake_subscribe(replicate: Option<bool>) -> (Connection, ConsumerHandle) {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        // Drain the Connected event so later poll_event calls return our test events.
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected, got {other:?}"),
        }
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/replicated".to_owned(),
            subscription: "sub-pip-33".to_owned(),
            replicate_subscription_state: replicate,
            ..Default::default()
        });
        (conn, handle)
    }

    fn drain_command_subscribe(conn: &mut Connection) -> pb::CommandSubscribe {
        let mut bytes = conn.poll_transmit();
        loop {
            let frame = crate::frame::decode_one(&mut bytes).expect("decode subscribe");
            if frame.command.r#type == pb::base_command::Type::Subscribe as i32 {
                return frame.command.subscribe.expect("CommandSubscribe");
            }
            assert!(!bytes.is_empty(), "no CommandSubscribe in outbound");
        }
    }

    // -------------------------------------------------------------------
    // Failover active-consumer-change re-flow (issue #307).
    //
    // A Failover standby promoted to active while sitting at
    // `available_permits == 0` must have its flow re-armed — otherwise
    // `receive()` starves forever against a non-empty broker backlog. The
    // re-arm is guarded so an already-fed / paused / closed / terminal /
    // mid-re-attach consumer is left untouched.
    // -------------------------------------------------------------------

    /// Subscribe a Failover consumer over a handshaked connection and drain the
    /// outbound `CommandSubscribe`, leaving the consumer registered at zero
    /// permits (no initial flow issued yet).
    fn handshake_subscribe_failover() -> (Connection, ConsumerHandle) {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected, got {other:?}"),
        }
        let initial_subscribe_request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/failover".to_owned(),
            subscription: "sub-failover".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: 100,
            ..Default::default()
        });
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.poll_transmit();
        feed_subscribe_success(&mut conn, initial_subscribe_request_id);
        assert!(conn.consume_initial_consumer_subscribe_completion(handle));
        while conn.poll_event().is_some() {}
        (conn, handle)
    }

    /// Subscribe a Failover consumer with the issue-#331 queue shape and
    /// drain only the subscribe frame. The caller explicitly issues the
    /// initial flow so no helper-side grant is hidden from wire assertions.
    fn handshake_subscribe_chunk_flow() -> (Connection, ConsumerHandle) {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected, got {other:?}"),
        }
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/chunk-flow".to_owned(),
            subscription: "sub-chunk-flow".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: 2,
            ..Default::default()
        });
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.poll_transmit();
        (conn, handle)
    }

    fn chunk_metadata(uuid: &str, chunk_id: i32) -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: "chunk-flow-producer".to_owned(),
            sequence_id: 7,
            publish_time: 1_700_000_000_000,
            uuid: Some(uuid.to_owned()),
            num_chunks_from_msg: Some(3),
            chunk_id: Some(chunk_id),
            total_chunk_msg_size: Some(6),
            ..Default::default()
        }
    }

    /// Encode a broker-initiated `CommandCloseConsumer` frame for `handle`
    /// (issue #307 repro): the broker quiesces the dispatcher on a bundle
    /// reassignment (`code=6` / `ServiceNotReady`) by closing the consumer on
    /// the live socket WITHOUT a connection-level reset. `assigned_broker_service_url`
    /// is left `None` so it mirrors the same-broker re-attach the supervised
    /// reconnect does not fire on.
    fn close_consumer_frame(handle: ConsumerHandle) -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(pb::CommandCloseConsumer {
                consumer_id: handle.0,
                request_id: 0,
                assigned_broker_service_url: None,
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandCloseConsumer");
        buf
    }

    /// Encode a `CommandActiveConsumerChange` frame for `handle`.
    fn active_consumer_change_frame(handle: ConsumerHandle, is_active: bool) -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::ActiveConsumerChange as i32,
            active_consumer_change: Some(pb::CommandActiveConsumerChange {
                consumer_id: handle.0,
                is_active: Some(is_active),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandActiveConsumerChange");
        buf
    }

    /// Decode the next `CommandFlow` for `handle` out of the connection's
    /// outbound buffer, if one was emitted.
    fn drain_command_flow(
        conn: &mut Connection,
        handle: ConsumerHandle,
    ) -> Option<pb::CommandFlow> {
        let mut bytes = conn.poll_transmit();
        while !bytes.is_empty() {
            let frame = crate::frame::decode_one(&mut bytes).expect("decode outbound");
            if frame.command.r#type == pb::base_command::Type::Flow as i32 {
                let flow = frame.command.flow.expect("CommandFlow body");
                if flow.consumer_id == handle.0 {
                    return Some(flow);
                }
            }
        }
        None
    }

    #[test]
    fn accepted_incomplete_chunks_replenish_flow_before_reassembly() {
        let (mut conn, handle) = handshake_subscribe_chunk_flow();
        conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        for (chunk_id, body) in [(0, b"aa".as_slice()), (2, b"cc"), (1, b"bb")] {
            let frame = message_frame(handle.0, &chunk_metadata("chunk-flow", chunk_id), body);
            conn.handle_bytes(Instant::now(), &frame)
                .expect("deliver chunk");
            let flow = drain_command_flow(&mut conn, handle);
            if chunk_id == 1 {
                assert!(
                    flow.is_none(),
                    "the completing chunk is repaid on logical pop"
                );
            } else {
                assert_eq!(flow.expect("incomplete chunk flow").message_permits, 1);
            }
        }

        assert_eq!(conn.consumer_queue_len(handle), 1);
        let message = conn
            .pop_message(handle, Instant::now())
            .expect("reassembled message");
        assert_eq!(message.payload.as_ref(), b"aabbcc");
        assert_eq!(
            drain_command_flow(&mut conn, handle)
                .expect("completing chunk flow")
                .message_permits,
            1,
        );
    }

    #[test]
    fn failover_promotion_rearms_flow_when_permits_zeroed() {
        // A Failover consumer that re-attached via the gated path (or was
        // reset) sits at `available_permits == 0`. On promotion to active the
        // handler must re-arm flow.
        let (mut conn, handle) = handshake_subscribe_failover();
        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "a freshly-subscribed consumer holds zero permits until flow is issued"
        );

        let frame = active_consumer_change_frame(handle, true);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        // Flow re-armed: permits back to the receiver queue size.
        assert_eq!(
            conn.consumer_available_permits(handle),
            100,
            "promotion must re-arm initial flow"
        );
        // And a CommandFlow actually went out on the wire.
        let flow = drain_command_flow(&mut conn, handle).expect("CommandFlow emitted on promotion");
        assert_eq!(flow.message_permits, 100);

        // Issue #348: the per-slot active-state surface reflects the promotion.
        assert_eq!(
            conn.consumer_is_active(handle),
            Some(true),
            "consumer_is_active must track the broker-reported promotion"
        );

        // The ActiveConsumerChanged event is still surfaced.
        let mut saw_event = false;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::ActiveConsumerChanged { handle: h, active } = ev {
                if h == handle && active {
                    saw_event = true;
                }
            }
        }
        assert!(saw_event, "ActiveConsumerChanged event must still fire");
    }

    #[test]
    fn failover_promotion_does_not_double_flow_when_permits_outstanding() {
        // A consumer that already holds permits must NOT be given extra flow on
        // a redundant promotion (no double-flow).
        let (mut conn, handle) = handshake_subscribe_failover();
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();
        assert_eq!(conn.consumer_available_permits(handle), 100);

        let frame = active_consumer_change_frame(handle, true);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        assert_eq!(
            conn.consumer_available_permits(handle),
            100,
            "an already-fed consumer keeps its permits, no re-arm"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "no extra CommandFlow when permits are already outstanding"
        );
        assert_eq!(
            conn.consumer_is_active(handle),
            Some(true),
            "consumer_is_active must track the broker-reported redundant promotion"
        );
    }

    #[test]
    fn failover_promotion_does_not_flow_when_paused() {
        // A paused consumer is intentionally starved by the user; promotion must
        // not override the pause.
        let (mut conn, handle) = handshake_subscribe_failover();
        if let Some(slot) = conn.consumers.get(&handle) {
            slot.state.lock().paused = true;
        }
        assert_eq!(conn.consumer_available_permits(handle), 0);

        let frame = active_consumer_change_frame(handle, true);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "a paused consumer must not be re-flowed on promotion"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "no CommandFlow for a paused consumer"
        );
        assert_eq!(
            conn.consumer_is_active(handle),
            Some(true),
            "consumer_is_active tracks the broker report independently of the reflow gate"
        );
    }

    #[test]
    fn failover_promotion_does_not_flow_when_closed() {
        // A user-closed consumer must not be re-flowed.
        let (mut conn, handle) = handshake_subscribe_failover();
        if let Some(slot) = conn.consumers.get(&handle) {
            slot.state.lock().closed = true;
        }

        let frame = active_consumer_change_frame(handle, true);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "a closed consumer must not be re-flowed on promotion"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "no CommandFlow for a closed consumer"
        );
        assert_eq!(
            conn.consumer_is_active(handle),
            Some(true),
            "consumer_is_active tracks the broker report independently of the reflow gate"
        );
    }

    #[test]
    fn failover_demotion_to_standby_does_not_flow() {
        // `is_active == false` (promoted active → standby) must never re-arm
        // flow — only promotion to active does.
        let (mut conn, handle) = handshake_subscribe_failover();
        assert_eq!(conn.consumer_available_permits(handle), 0);

        let frame = active_consumer_change_frame(handle, false);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "demotion to standby must not re-arm flow"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "no CommandFlow on demotion"
        );
        assert_eq!(
            conn.consumer_is_active(handle),
            Some(false),
            "consumer_is_active must track the broker-reported demotion"
        );
    }

    /// Issue #348: a recorded active-change transition is poppable exactly
    /// once via [`Connection::pop_consumer_active_change`], mirroring
    /// `pop_message`'s single-consumption semantics.
    #[test]
    fn active_change_recorded_and_poppable() {
        let (mut conn, handle) = handshake_subscribe_failover();

        let frame = active_consumer_change_frame(handle, true);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle active-change");

        assert_eq!(
            conn.pop_consumer_active_change(handle),
            Some(true),
            "the recorded transition must be poppable"
        );
        assert_eq!(
            conn.pop_consumer_active_change(handle),
            None,
            "a popped transition is consumed — the ring drains once"
        );
    }

    /// Issue #348: the `active_changes` ring is capped at `ACTIVE_CHANGES_CAP`
    /// (32) — pushing a 33rd transition drops the oldest, not the newest.
    #[test]
    fn active_change_ring_caps_at_32_dropping_oldest() {
        let (mut conn, handle) = handshake_subscribe_failover();

        let pushed: Vec<bool> = (0..33u32).map(|i| i % 2 == 0).collect();
        for &active in &pushed {
            let frame = active_consumer_change_frame(handle, active);
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle active-change");
        }

        let mut drained = Vec::new();
        while let Some(active) = conn.pop_consumer_active_change(handle) {
            drained.push(active);
        }

        assert_eq!(drained.len(), 32, "the ring caps at ACTIVE_CHANGES_CAP");
        assert_eq!(
            drained,
            pushed[1..],
            "the OLDEST transition (index 0) is dropped, the rest survive in order"
        );
    }

    /// Issue #348: `consumer_is_active` mirrors the last broker report —
    /// `None` before any `CommandActiveConsumerChange`, then flips with each
    /// subsequent frame regardless of direction.
    #[test]
    fn is_active_tracks_last_broker_report() {
        let (mut conn, handle) = handshake_subscribe_failover();
        assert_eq!(
            conn.consumer_is_active(handle),
            None,
            "no active-change frame observed yet"
        );

        conn.handle_bytes(Instant::now(), &active_consumer_change_frame(handle, true))
            .expect("handle active-change (promote)");
        assert_eq!(conn.consumer_is_active(handle), Some(true));

        conn.handle_bytes(Instant::now(), &active_consumer_change_frame(handle, false))
            .expect("handle active-change (demote)");
        assert_eq!(conn.consumer_is_active(handle), Some(false));

        conn.handle_bytes(Instant::now(), &active_consumer_change_frame(handle, true))
            .expect("handle active-change (re-promote)");
        assert_eq!(conn.consumer_is_active(handle), Some(true));
    }

    /// Drain the next `CommandSubscribe` for `handle` out of the outbound
    /// buffer, returning its body (with the `request_id` the client allocated),
    /// or `None` if none was emitted.
    fn drain_command_subscribe_for(
        conn: &mut Connection,
        handle: ConsumerHandle,
    ) -> Option<pb::CommandSubscribe> {
        let mut bytes = conn.poll_transmit();
        while !bytes.is_empty() {
            let frame = crate::frame::decode_one(&mut bytes).expect("decode outbound");
            if frame.command.r#type == pb::base_command::Type::Subscribe as i32 {
                let sub = frame.command.subscribe.expect("CommandSubscribe body");
                if sub.consumer_id == handle.0 {
                    return Some(sub);
                }
            }
        }
        None
    }

    /// Feed a broker `CommandSuccess` for `request_id` into the connection (acks
    /// a re-subscribe so the re-attach flow gate at the `Success` arm fires).
    fn feed_subscribe_success(conn: &mut Connection, request_id: u64) {
        let ok = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ok).expect("encode CommandSuccess");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandSuccess");
    }

    #[test]
    fn failover_resubscribes_and_rearms_flow_after_same_broker_close() {
        // Issue #307 ROOT CAUSE: an ALREADY-RUNNING Failover consumer on a
        // backlogged topic freezes after a same-broker (`code=6`) bundle
        // reassignment because nothing re-subscribes it.
        //
        // Lifecycle reproduced here (all on ONE live socket — no reset):
        //   1. subscribe + initial flow -> available_permits = 100 (rqs).
        //   2. broker dispatches the full grant; the app consumes it, drawing the broker's permits
        //      down to 0 (steady state).
        //   3. bundle reassignment: broker sends `CommandCloseConsumer{assigned_broker_service_url:
        //      None}` to quiesce the dispatcher (NOT a socket drop -> no `Connection::reset`, so
        //      the supervised reconnect / `rebuild_consumers` path never fires). The broker has
        //      torn its consumer id down; a bare `CommandFlow` against it would be dropped.
        //
        // The fix re-subscribes the single consumer in place at close-time
        // (re-emit `CommandSubscribe`, defer flow to its `Success`). Before the
        // fix NO re-subscribe was issued, so the consumer wedged at
        // broker-permits=0 with a full backlog: `availablePermits=0`,
        // `msgRateOut=0`, frozen `receive()`.
        let (mut conn, handle) = handshake_subscribe_failover();

        // (1) Initial flow on the active consumer.
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();
        assert_eq!(conn.consumer_available_permits(handle), 100);

        // (2) Consume the whole grant so the live consumer is genuinely active
        // and has drained its receiver queue (mirrors the steady-state active
        // consumer the production symptom describes).
        for i in 0..100u64 {
            let meta = regular_metadata();
            let frame = message_frame(handle.0, &meta, format!("m{i}").as_bytes());
            conn.handle_bytes(Instant::now(), &frame)
                .expect("deliver message");
        }
        while conn.pop_message(handle, Instant::now()).is_some() {}
        // Drain any maybe_flow top-ups the consume path emitted.
        while drain_command_flow(&mut conn, handle).is_some() {}

        // (3) Bundle reassignment: broker closes the consumer on the live socket.
        // The fix MUST re-emit a `CommandSubscribe` for this handle here.
        let resubscribe_rid = conn.peek_next_request_id_for_test();
        let close = close_consumer_frame(handle);
        conn.handle_bytes(Instant::now(), &close)
            .expect("handle broker close");
        // The consumer is intentionally left open (post-#65); no reset happened.
        assert!(
            !conn.consumer_is_closed(handle),
            "broker close must leave the consumer open for re-attach (issue #65)"
        );

        // SUCCESS CONDITION (a): a fresh `CommandSubscribe` re-attaches the
        // consumer on the same socket. Before the fix NONE was emitted — the
        // root-cause wedge. The permit mirror was reset to 0 and NO flow is sent
        // yet (it is deferred to the re-subscribe ack — the broker drops pre-ack
        // flow).
        let sub = drain_command_subscribe_for(&mut conn, handle)
            .expect("a fresh CommandSubscribe must re-attach the running consumer (#307)");
        assert_eq!(
            sub.request_id, resubscribe_rid,
            "the re-subscribe should use the freshly-allocated request id"
        );
        assert_eq!(conn.consumer_available_permits(handle), 0);
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "flow MUST be deferred to the re-subscribe Success (pre-ack flow is dropped broker-side)"
        );

        // (4) Broker acks the re-subscribe -> the re-attach gate re-arms flow.
        feed_subscribe_success(&mut conn, resubscribe_rid);

        // SUCCESS CONDITION (b): a `CommandFlow` now goes out so the broker
        // resumes dispatching the backlog.
        let flow = drain_command_flow(&mut conn, handle);
        assert!(
            flow.is_some(),
            "after the re-subscribe is acked the consumer MUST re-arm flow; \
             otherwise it wedges at broker-permits=0 with a non-empty backlog (issue #307)"
        );
        assert_eq!(
            flow.expect("flow re-armed").message_permits,
            100,
            "re-arm must restore the full receiver-queue grant"
        );
    }

    #[test]
    fn topic_migration_close_consumer_does_not_resubscribe_in_place() {
        // For `assigned_broker_service_url = Some(url)` (PIP-188 topic
        // migration) the supervised reconnect / migration path on the new URL
        // owns the re-attach — the proto layer must NOT re-subscribe in place on
        // this socket, and MUST still surface the `ConsumerClosedByBroker`
        // event the runtime drives the migration from.
        let (mut conn, handle) = handshake_subscribe_failover();
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(pb::CommandCloseConsumer {
                consumer_id: handle.0,
                request_id: 0,
                assigned_broker_service_url: Some("pulsar://new-broker:6650".to_owned()),
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandCloseConsumer");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle migration close");

        assert!(
            drain_command_subscribe_for(&mut conn, handle).is_none(),
            "topic-migration close (url=Some) must NOT re-subscribe in place"
        );
        let saw_close_event = std::iter::from_fn(|| conn.poll_event()).any(|ev| {
            matches!(
                ev,
                ConnectionEvent::ConsumerClosedByBroker {
                    handle: h,
                    assigned_broker_service_url: Some(_),
                } if h == handle
            )
        });
        assert!(
            saw_close_event,
            "topic-migration close must surface ConsumerClosedByBroker for the migration path"
        );
    }

    // -------------------------------------------------------------------
    // Issue #346 — ack orphaned by same-broker CloseConsumer + no deadline.
    //
    // Two complementary fixes: (1) the same-broker CloseConsumer arm above
    // fails every pending ack for the torn-down handle immediately (fast
    // path); (2) `ack_response_timeout` is a generic backstop that reaps a
    // pending ack whose CommandAckResponse never arrives for ANY reason
    // (broker silently drops it, etc.), independent of a CloseConsumer ever
    // landing.
    // -------------------------------------------------------------------

    /// Primary sweep (fast path): an ack in flight when the broker tears this
    /// consumer's dispatcher down via a same-broker `CommandCloseConsumer`
    /// (`assigned_broker_service_url = None`) is orphaned — the old consumer
    /// id is gone, so no `CommandAckResponse` for it will ever arrive on this
    /// connection (`resubscribe_consumer_after_broker_close` attaches a FRESH
    /// id). The close-handler sweep must fail the pending ack immediately
    /// with `Error{code: -1, message: "ack orphaned by broker consumer
    /// close"}` instead of leaving it parked until the `ack_response_timeout`
    /// backstop (or forever, if that knob is disabled).
    #[test]
    fn ack_orphaned_by_same_broker_close_fails_fast() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker_inner = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&waker_inner).into();

        let (mut conn, handle) = handshake_subscribe_failover();
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        let t0 = Instant::now();
        let acked = MessageId {
            ledger_id: 1,
            entry_id: 1,
            partition: -1,
            batch_index: -1,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![acked],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();

        let key = PendingOpKey::Request(rid);
        conn.register_waker(key, waker.clone());
        assert!(
            conn.has_pending_request_for_test(rid),
            "the ack must be pending before the close frame lands"
        );

        let close = close_consumer_frame(handle);
        conn.handle_bytes(t0 + Duration::from_millis(1), &close)
            .expect("handle broker close");

        match conn.take_outcome(key) {
            Some(OpOutcome::Error {
                request_id,
                code,
                message,
            }) => {
                assert_eq!(request_id, rid);
                assert_eq!(code, -1, "orphaned-ack uses the -1 sentinel");
                assert_eq!(message, "ack orphaned by broker consumer close");
            }
            other => panic!("expected an orphaned-ack Error outcome, got {other:?}"),
        }
        assert!(
            !conn.has_pending_request_for_test(rid),
            "the orphaned ack must drain out of pending_requests"
        );
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            1,
            "the parked waker must be woken exactly once"
        );
        assert_eq!(
            conn.consumer_stats(handle)
                .expect("consumer stats")
                .total_acks_failed,
            1,
            "the orphaned ack must bump total_acks_failed"
        );
    }

    /// Backstop deadline: an ack whose `CommandAckResponse` never arrives (the
    /// broker goes silent without ever tearing the consumer down) must not
    /// hang the caller's `ack().await` forever. Once the INJECTED clock
    /// (ADR-0011) crosses `enqueued_at + ack_response_timeout`,
    /// `handle_timeout`'s reap sweep resolves the pending ack with
    /// `code=-1, "ack timeout"` and wakes the parked waker. Mirrors the
    /// `default_send_timeout_fires_when_receipt_lost` shape (this file).
    #[test]
    fn pending_ack_deadline_reaps_when_broker_silent() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker_inner = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker: Waker = Arc::clone(&waker_inner).into();

        assert_eq!(
            ConnectionConfig::default().ack_response_timeout,
            Some(Duration::from_secs(30)),
            "ack_response_timeout default must be 30s (Java-parity, mirrors the #304 \
             send_timeout default, ADR-0072)"
        );

        let (mut conn, handle) = handshake_subscribe_failover();
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        let t0 = Instant::now();
        let acked = MessageId {
            ledger_id: 2,
            entry_id: 2,
            partition: -1,
            batch_index: -1,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![acked],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();
        let key = PendingOpKey::Request(rid);
        conn.register_waker(key, waker.clone());

        assert!(
            conn.poll_timeout().is_some(),
            "a wake-up deadline must be scheduled while an ack is pending"
        );

        // Just BEFORE the deadline: no timeout, no wake, no outcome.
        conn.handle_timeout(t0 + Duration::from_secs(29));
        assert!(
            conn.take_outcome(key).is_none(),
            "no ack-timeout outcome before the 30s deadline"
        );
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            0,
            "waker must not fire before the deadline"
        );

        // Past the deadline: the sweep resolves the ack with a timeout error
        // and wakes the parked waker.
        conn.handle_timeout(t0 + Duration::from_secs(31));
        match conn.take_outcome(key) {
            Some(OpOutcome::Error {
                request_id,
                code,
                message,
            }) => {
                assert_eq!(request_id, rid);
                assert_eq!(code, -1, "ack-timeout uses the -1 sentinel");
                assert_eq!(message, "ack timeout");
            }
            other => panic!("expected an ack-timeout Error outcome, got {other:?}"),
        }
        assert_eq!(
            waker_inner.0.load(Ordering::SeqCst),
            1,
            "the parked waker must be woken exactly once on timeout"
        );
        assert!(
            !conn.has_pending_request_for_test(rid),
            "the timed-out ack must drain out of pending_requests"
        );
    }

    /// `ack_response_timeout: None` disables the backstop entirely: an ack
    /// left pending is never reaped no matter how far the injected clock
    /// advances, and it contributes no deadline to `poll_timeout` (load-
    /// bearing for moonpool determinism — an armed-but-never-firing deadline
    /// would still perturb the simulated wake schedule).
    #[test]
    fn disabled_ack_timeout_never_reaps() {
        let mut conn = Connection::new(
            ConnectionConfig {
                ack_response_timeout: None,
                ..ConnectionConfig::default()
            },
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-timeout-disabled".to_owned(),
            subscription: "sub-ack-timeout-disabled".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        });
        let _ = conn.poll_transmit();

        let before = conn.poll_timeout();

        let t0 = Instant::now();
        let acked = MessageId {
            ledger_id: 3,
            entry_id: 3,
            partition: -1,
            batch_index: -1,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![acked],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();

        assert_eq!(
            conn.poll_timeout(),
            before,
            "a disabled ack_response_timeout must not perturb poll_timeout — the pending \
             ack contributes no deadline"
        );

        conn.handle_timeout(t0 + Duration::from_hours(1));
        assert!(
            conn.has_pending_request_for_test(rid),
            "a disabled ack_response_timeout must never reap the pending ack"
        );
        let key = PendingOpKey::Request(rid);
        assert!(
            conn.take_outcome(key).is_none(),
            "no outcome must materialize for a disabled-timeout ack"
        );
    }

    /// No-false-positive companion to `pending_ack_deadline_reaps_when_broker_silent`:
    /// an ack whose `CommandAckResponse` lands BEFORE the deadline resolves
    /// normally with a `Success` outcome, and ticking `handle_timeout` well
    /// past the default 30s deadline afterwards must not spuriously re-fire —
    /// the entry already drained out of `pending_requests` on the real
    /// response.
    #[test]
    fn ack_response_before_deadline_resolves_normally() {
        let (mut conn, handle) = handshake_subscribe_failover();
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        let t0 = Instant::now();
        let acked = MessageId {
            ledger_id: 4,
            entry_id: 4,
            partition: -1,
            batch_index: -1,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![acked],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();

        // Broker acks well within the 30s window.
        let ack_response = pb::BaseCommand {
            r#type: pb::base_command::Type::AckResponse as i32,
            ack_response: Some(pb::CommandAckResponse {
                consumer_id: handle.0,
                request_id: Some(rid.0),
                error: None,
                message: None,
                txnid_least_bits: None,
                txnid_most_bits: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ack_response).expect("encode CommandAckResponse");
        conn.handle_bytes(t0 + Duration::from_secs(1), &buf)
            .expect("handle AckResponse");

        let key = PendingOpKey::Request(rid);
        match conn.take_outcome(key) {
            Some(OpOutcome::Success { request_id }) => assert_eq!(request_id, rid),
            other => {
                panic!("expected a Success outcome for the timely ack response, got {other:?}")
            }
        }

        // Well past the default 30s deadline: the reap sweep must be a no-op
        // — the entry already drained out of pending_requests when the real
        // response landed.
        conn.handle_timeout(t0 + Duration::from_hours(1));
        assert!(
            conn.take_outcome(key).is_none(),
            "no stale ack-timeout outcome must appear after the real response already resolved it"
        );
    }

    // -------------------------------------------------------------------
    // Issue #301 — pluggable receiver-queue policy, connection-driven adjust.
    //
    // The `Auto` policy ticks from `handle_timeout`'s injected `now`; a grown
    // target emits an incremental `CommandFlow`. These tests drive the whole
    // path through the public `Connection` surface (subscribe → initial flow →
    // adjust tick → outbound flow) so the proto integration covers the
    // connection plumbing, not just the pure policy unit tests.
    // -------------------------------------------------------------------

    /// Subscribe an `Auto`-policy consumer over a handshaked connection, drain
    /// the outbound `CommandSubscribe`, and feed the initial flow. Returns the
    /// connection, handle, and the adjust interval used.
    ///
    /// `at` pins the whole setup — handshake, initial flow, and therefore the
    /// adjust schedule's arming instant, which `initial_flow` now seeds
    /// (follow-ups §4). Callers advance from `at` so every deadline in the test
    /// is expressible relative to a single known origin.
    fn handshake_subscribe_auto(
        min: usize,
        max_bytes: usize,
        adjust_interval: Duration,
        at: Instant,
    ) -> (Connection, ConsumerHandle, Duration) {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("handle handshake");
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected, got {other:?}"),
        }
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/auto-rq".to_owned(),
            subscription: "sub-auto-rq".to_owned(),
            receiver_queue_policy: Some(std::sync::Arc::new(crate::receiver_queue::Auto::new(
                min, max_bytes,
            ))),
            receiver_queue_adjust_interval: Some(adjust_interval),
            ..Default::default()
        });
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.initial_flow(handle, at);
        let _ = conn.poll_transmit();
        (conn, handle, adjust_interval)
    }

    #[test]
    fn auto_policy_seeds_initial_flow_at_the_floor() {
        // `Auto::initial()` returns the floor; the consumer's first flow grants
        // exactly that, not the (ignored) raw `receiver_queue_size`.
        let (conn, handle, _) = handshake_subscribe_auto(
            100,
            128 * 1024 * 1024,
            Duration::from_secs(1),
            Instant::now(),
        );
        assert_eq!(
            conn.consumer_available_permits(handle),
            100,
            "Auto seeds the initial flow at its floor"
        );
    }

    #[test]
    fn auto_policy_grows_target_under_starvation_and_emits_incremental_flow() {
        // Issue #349: the broker's real permit BALANCE is drained by genuine
        // dispatch (not a synthetic field write) and the byte budget is wide
        // open — the adjust tick doubles the target and emits an incremental
        // flow for the delta.
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (mut conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);

        // Drain the broker-side permit BALANCE via real dispatch — 100
        // single-message deliveries against the 100-permit initial grant —
        // so `available_permits == 0` is the genuine starvation signal at
        // tick time, not a manually-zeroed mirror.
        for i in 0..100u64 {
            let meta = regular_metadata();
            let frame = message_frame(handle.0, &meta, format!("m{i}").as_bytes());
            conn.handle_bytes(t0, &frame).expect("deliver message");
        }

        // The schedule was armed at `t0` by `initial_flow` (follow-ups §4), so a
        // tick at `t0` itself is still short of the `t0 + interval` deadline.
        conn.handle_timeout(t0);
        let _ = conn.poll_transmit();
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "a tick before the first deadline does not adjust"
        );

        // Second tick, one interval later, runs the adjust: 100 -> 200.
        conn.handle_timeout(t0 + interval);
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            200,
            "starvation doubles the target"
        );
        let flow = drain_command_flow(&mut conn, handle)
            .expect("growing the target emits an incremental flow");
        assert_eq!(
            flow.message_permits, 100,
            "the broker had already been granted 100 permits (untouched by dispatch — \
             `granted_permits` is a purely additive mirror); the delta tops it up from \
             100 to the new 200 target"
        );
        assert_eq!(
            conn.consumer_available_permits(handle),
            200,
            "available permits track the new target after the incremental grant"
        );
    }

    #[test]
    fn auto_policy_does_not_flow_when_target_holds_steady() {
        // Permits remain and bytes are within budget: the target holds and no
        // flow is emitted (invariant: no thrash).
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (mut conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);
        // Healthy: the initial flow left 100 permits in place.
        conn.handle_timeout(t0); // before the first deadline — no adjust
        let _ = conn.poll_transmit();
        conn.handle_timeout(t0 + interval); // adjust — holds
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "a healthy consumer holds its target"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "no flow when the target does not grow"
        );
    }

    #[test]
    fn fixed_policy_default_never_adjusts() {
        // The default `Fixed` policy disables auto-adjust: no adjust deadline is
        // surfaced and the target never moves even under genuine dispatch-driven
        // starvation (real deliveries draining the permit balance to zero, not a
        // synthetic field write).
        let (mut conn, handle) = handshake_subscribe_failover(); // receiver_queue_size: 100, Fixed
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();
        let t0 = Instant::now();
        for i in 0..100u64 {
            let meta = regular_metadata();
            let frame = message_frame(handle.0, &meta, format!("m{i}").as_bytes());
            conn.handle_bytes(t0, &frame).expect("deliver message");
        }
        // Many ticks: a Fixed consumer never grows and never flows from adjust.
        for i in 0..10u32 {
            conn.handle_timeout(t0 + Duration::from_secs(u64::from(i)));
        }
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "Fixed default never auto-adjusts"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "Fixed default never emits an adjust-driven flow"
        );
    }

    #[test]
    fn auto_policy_grows_under_real_dispatch_starvation() {
        // Issue #349 regression: before the permit-balance split, the
        // permit mirror was purely additive (grants only, never decremented
        // as messages actually arrived), so a REAL dispatch-driven
        // starvation never registered as zero and `Auto` could never
        // observe the starvation signal it needs to grow. This test drives
        // genuine message deliveries — not a synthetic field write — until
        // the broker-side permit balance is truly exhausted, then asserts
        // the adjust tick grows the target and emits an incremental flow.
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (mut conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);

        // Real dispatch-unit starvation: deliver exactly the 100 permits'
        // worth of single messages the initial flow granted, so the REAL
        // balance drains to zero — no manual field write anywhere.
        for i in 0..100u64 {
            let meta = regular_metadata();
            let frame = message_frame(handle.0, &meta, format!("m{i}").as_bytes());
            conn.handle_bytes(t0, &frame).expect("deliver message");
        }

        // Armed at `t0` by `initial_flow`; a tick at `t0` is still short of the
        // `t0 + interval` deadline.
        conn.handle_timeout(t0);
        let _ = conn.poll_transmit();
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "a tick before the first deadline does not adjust"
        );

        // Second tick, one interval later: real dispatch-driven starvation
        // must be observed and must double the target.
        conn.handle_timeout(t0 + interval);
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            200,
            "real dispatch-driven starvation must double the target (issue #349)"
        );
        let flow = drain_command_flow(&mut conn, handle)
            .expect("growing the target under real starvation must emit an incremental flow");
        assert_eq!(
            flow.message_permits, 100,
            "the broker had already been granted 100 permits (untouched by dispatch); \
             the delta tops it up from 100 to the new 200 target"
        );
    }

    #[test]
    fn adjust_skips_growth_during_churn_window() {
        // Issue #349 churn-window guard: a same-broker `CloseConsumer`
        // zeroes the permit mirror as part of the #307 re-attach dance —
        // that zero means "no outstanding grant to have starved against",
        // NOT "the user is falling behind". Without the guard, an adjust
        // tick landing in this window would misread the churn-zeroed
        // balance as starvation and grow the target / emit a flow the
        // broker would drop (it no longer knows this consumer id until the
        // resubscribe's `Success` lands).
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (mut conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);

        // The schedule is already armed at `t0` (`initial_flow`); this tick just
        // settles the connection before the churn event.
        conn.handle_timeout(t0);
        let _ = conn.poll_transmit();

        // Same-broker bundle reassignment: the broker tears the consumer
        // down and re-subscribes it in place, zeroing the permit mirror.
        let close = close_consumer_frame(handle);
        conn.handle_bytes(t0, &close).expect("handle broker close");
        let _ = conn.poll_transmit();

        // Tick past the adjust interval while sitting in the churn window.
        conn.handle_timeout(t0 + interval);
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "a churn-window tick must not grow the target"
        );
        assert!(
            drain_command_flow(&mut conn, handle).is_none(),
            "a churn-window tick must not emit an adjust-driven flow"
        );
    }

    /// Encode a broker `CommandAckResponse` resolving `request_id` for `handle`.
    fn ack_response_frame(handle: ConsumerHandle, request_id: RequestId) -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::AckResponse as i32,
            ack_response: Some(pb::CommandAckResponse {
                consumer_id: handle.0,
                request_id: Some(request_id.0),
                error: None,
                message: None,
                txnid_least_bits: None,
                txnid_most_bits: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandAckResponse");
        buf
    }

    /// Ack one message id and feed the broker's `CommandAckResponse` back at
    /// `at`, draining the resulting outcome. One full ack round-trip — the
    /// traffic shape a consumer that awaits every individual ack produces.
    fn ack_round_trip(conn: &mut Connection, handle: ConsumerHandle, entry_id: u64, at: Instant) {
        let request_id = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![MessageId {
                    ledger_id: 4,
                    entry_id,
                    partition: -1,
                    batch_index: -1,
                    batch_size: -1,
                    #[cfg(feature = "scalable-topics")]
                    segment_id: None,
                }],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            at,
        );
        let _ = conn.poll_transmit();
        conn.handle_bytes(at, &ack_response_frame(handle, request_id))
            .expect("handle AckResponse");
        let _ = conn.take_outcome(PendingOpKey::Request(request_id));
    }

    #[test]
    fn auto_policy_arms_adjust_schedule_at_initial_flow() {
        // follow-ups §4: `initial_flow` is the adjust schedule's dedicated
        // bootstrap. Without ever calling `handle_timeout`, `poll_timeout` must
        // already surface the adjust deadline — that is what makes the first
        // tick's timing a function of the subscribe-ack moment rather than of
        // whichever unrelated deadline happens to fire first.
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            100,
            "Auto seeds at its floor"
        );
        assert_eq!(
            conn.poll_timeout(),
            Some(t0 + interval),
            "the adjust deadline must be armed by `initial_flow` alone, and must win the \
             `poll_timeout` minimum against the far-away default keepalive deadline"
        );
    }

    #[test]
    fn auto_adjust_schedule_arms_under_continuous_ack_response_traffic() {
        // follow-ups §4 regression. Every decoded inbound frame refreshes
        // `last_activity` (the single refresh site, ADR-0058), so the keepalive
        // deadline slides forward forever on a busy connection. While arming
        // lived only in `handle_timeout`'s fallback arm, a consumer awaiting each
        // individual ack — a continuous `CommandAckResponse` stream — kept the
        // keepalive deadline (the ONLY deadline an unarmed `Auto` consumer has)
        // permanently out of reach, so `handle_timeout` never ran, the schedule
        // never armed, and `Auto` never scaled regardless of `keepalive_interval`.
        //
        // With the bootstrap at `initial_flow`, the armed adjust deadline is
        // fixed at `t0 + interval` and no amount of inbound traffic can defer it.
        let interval = Duration::from_secs(1);
        let t0 = Instant::now();
        let (mut conn, handle, _) = handshake_subscribe_auto(100, 128 * 1024 * 1024, interval, t0);

        // Real dispatch-driven starvation: drain the 100-permit initial grant.
        for i in 0..100u64 {
            let meta = regular_metadata();
            let frame = message_frame(handle.0, &meta, format!("m{i}").as_bytes());
            conn.handle_bytes(t0, &frame).expect("deliver message");
        }
        let _ = conn.poll_transmit();

        // Nine ack round-trips at 100 ms cadence — continuous inbound traffic
        // for the whole sub-interval window, each frame pushing the keepalive
        // deadline further out.
        let step = Duration::from_millis(100);
        for k in 1..=9u64 {
            let at = t0 + step * u32::try_from(k).expect("small loop counter");
            ack_round_trip(&mut conn, handle, k, at);
            assert_eq!(
                conn.poll_timeout(),
                Some(t0 + interval),
                "ack-response traffic must not defer the armed adjust deadline (round {k})"
            );
        }

        // The driver wakes on the armed deadline and the adjust runs there.
        conn.handle_timeout(t0 + interval);
        assert_eq!(
            conn.consumer_receiver_queue_size(handle),
            200,
            "the armed tick must observe the drained permit balance and double the target"
        );
        let flow = drain_command_flow(&mut conn, handle)
            .expect("growing the target emits an incremental flow");
        assert_eq!(
            flow.message_permits, 100,
            "the delta tops the additive grant mirror up from 100 to the new 200 target"
        );
    }

    #[test]
    fn command_subscribe_with_replicate_state_true_emits_field() {
        let (mut conn, _h) = handshake_subscribe(Some(true));
        let sub = drain_command_subscribe(&mut conn);
        // Wire field 14 must be present and set.
        assert_eq!(sub.replicate_subscription_state, Some(true));
    }

    #[test]
    fn command_subscribe_with_replicate_state_false_byte_identical_to_v01() {
        // Default subscribe (None) MUST omit field 14 entirely so the wire bytes match the baseline
        // (preserves backward compat for callers that never touched the flag).
        let (mut conn_none, _) = handshake_subscribe(None);
        let _ = conn_none.poll_transmit();

        let sub = drain_command_subscribe(&mut {
            let (c, _) = handshake_subscribe(None);
            c
        });
        assert_eq!(sub.replicate_subscription_state, None);

        // Explicit Some(false) is semantically equivalent and must round-trip.
        let (mut conn_false, _) = handshake_subscribe(Some(false));
        let sub_false = drain_command_subscribe(&mut conn_false);
        assert_eq!(sub_false.replicate_subscription_state, Some(false));
    }

    #[test]
    fn consumer_filters_replicated_marker_from_event_stream() {
        let (mut conn, handle) = handshake_subscribe(Some(true));
        // Drain the outbound subscribe so it doesn't interfere with subsequent inspection.
        let _ = drain_command_subscribe(&mut conn);

        // Feed a Snapshot marker (kind 12) on this consumer.
        let snap = pb::ReplicatedSubscriptionsSnapshot {
            snapshot_id: "snap-99".to_owned(),
            local_message_id: Some(pb::MarkersMessageIdData {
                ledger_id: 1,
                entry_id: 1,
            }),
            clusters: Vec::new(),
        };
        let mut payload = Vec::new();
        prost::Message::encode(&snap, &mut payload).expect("encode snapshot");
        let frame = message_frame(handle.0, &marker_metadata(12), &payload);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle marker frame");

        // No Message event must surface for this consumer.
        let mut seen_message = false;
        while let Some(ev) = conn.poll_event() {
            if matches!(ev, ConnectionEvent::Message { handle: h, .. } if h == handle) {
                seen_message = true;
            }
        }
        assert!(!seen_message, "marker leaked as Message event");
    }

    #[test]
    fn consumer_emits_marker_observation_event() {
        let (mut conn, handle) = handshake_subscribe(Some(true));
        let _ = drain_command_subscribe(&mut conn);

        let update = pb::ReplicatedSubscriptionsUpdate {
            subscription_name: "sub-pip-33".to_owned(),
            clusters: vec![pb::ClusterMessageId {
                cluster: "cluster-b".to_owned(),
                message_id: pb::MarkersMessageIdData {
                    ledger_id: 7,
                    entry_id: 13,
                },
            }],
        };
        let mut payload = Vec::new();
        prost::Message::encode(&update, &mut payload).expect("encode update");
        let frame = message_frame(handle.0, &marker_metadata(13), &payload);
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle update marker");

        let mut observed = None;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle: h, marker } = ev
            {
                if h == handle {
                    observed = Some(marker);
                    break;
                }
            }
        }
        let marker = observed.expect("ReplicatedSubscriptionMarkerObserved event");
        assert_eq!(
            marker.kind,
            crate::markers::ReplicatedSubscriptionMarkerKind::Update
        );
        match marker.details {
            crate::markers::ReplicatedSubscriptionMarkerDetails::Update {
                subscription_name,
                clusters,
            } => {
                assert_eq!(subscription_name, "sub-pip-33");
                assert_eq!(clusters.len(), 1);
                assert_eq!(clusters[0].cluster, "cluster-b");
                assert_eq!(clusters[0].message_id.entry_id, 13);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn consumer_passes_through_non_marker_messages() {
        // Regression guard: regular messages (no marker_type) must still surface as
        // ConnectionEvent::Message.
        let (mut conn, handle) = handshake_subscribe(None);
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.initial_flow(handle, Instant::now());
        // Drain any flow command on the wire.
        let _ = conn.poll_transmit();

        let frame = message_frame(handle.0, &regular_metadata(), b"hello-pip-33");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle regular message");

        let mut seen_message = false;
        let mut seen_marker = false;
        while let Some(ev) = conn.poll_event() {
            match ev {
                ConnectionEvent::Message { handle: h, .. } if h == handle => seen_message = true,
                ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle: h, .. }
                    if h == handle =>
                {
                    seen_marker = true;
                }
                _ => {}
            }
        }
        assert!(seen_message, "regular message must surface as Message");
        assert!(!seen_marker, "regular message must NOT surface as marker");
    }

    #[test]
    fn consumer_passes_through_txn_markers() {
        // Txn markers (kinds 20..=22) fall through to the existing deliver path —
        // the receive-path filter is intentionally scoped to PIP-33 marker kinds
        // only (decoder returns Ok(None) for txn kinds).
        let (mut conn, handle) = handshake_subscribe(None);
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        let mut meta = marker_metadata(21); // TXN_COMMIT
        meta.num_messages_in_batch = Some(1);
        let frame = message_frame(handle.0, &meta, b"txn-payload");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle txn marker frame");

        let mut saw_rs_marker = false;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle: h, .. } = ev {
                if h == handle {
                    saw_rs_marker = true;
                }
            }
        }
        assert!(
            !saw_rs_marker,
            "txn markers must not fire the PIP-33 observation event",
        );
    }

    #[test]
    fn message_events_do_not_amplify_with_queue_depth() {
        // Regression: `ConsumerState::classify_and_queue` used to return
        // `count: self.queue.len()`, so the connection emitted one
        // `ConnectionEvent::Message` per *queued* entry on every new arrival —
        // O(n²) events for n messages received without an interleaved
        // `pop_message`. Each event carried a full `IncomingMessage` clone.
        // The fix returns `count: 1` for the single-append path; the batched
        // path in `deliver` already counts its own loop iterations.
        let (mut conn, handle) = handshake_subscribe(None);
        let _ = drain_command_subscribe(&mut conn);
        let _ = conn.initial_flow(handle, Instant::now());
        let _ = conn.poll_transmit();

        // Feed three single-message frames back-to-back with no `pop_message`
        // in between. Each must produce exactly one Message event.
        for payload in [b"msg-a".as_slice(), b"msg-b", b"msg-c"] {
            let frame = message_frame(handle.0, &regular_metadata(), payload);
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle regular message");
        }

        let mut message_event_count = 0_usize;
        while let Some(ev) = conn.poll_event() {
            if matches!(ev, ConnectionEvent::Message { handle: h, .. } if h == handle) {
                message_event_count += 1;
            }
        }
        assert_eq!(
            message_event_count, 3,
            "expected one Message event per arrival, not O(n²) amplification",
        );
    }

    /// Lookup multi-agent review HIGH-3: `Connection::reset()` MUST publish
    /// `OpOutcome::SessionLost` for every in-flight lookup +
    /// partitioned-metadata request **before** the registry is cleared. A
    /// future polled after the reset must observe `SessionLost` on its
    /// next `take_outcome` call — it must NOT park on a now-orphaned waker
    /// until the runtime's 30-second `operation_timeout` fires.
    ///
    /// Ordering invariant exercised: outcomes written → wakers fired →
    /// registry maps cleared. The first loop in `reset` (drains
    /// `pending_requests`) handles the happy path; the
    /// belt-and-suspenders re-drain right before `self.lookup = …`
    /// catches any orphan entry whose `pending_requests` slot was
    /// already removed (e.g. a future internal retry path that
    /// decouples the two maps).
    #[test]
    fn reset_drains_in_flight_lookup_with_session_lost_before_clearing_registry() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Wake, Waker};

        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn counting_waker() -> (Arc<CountingWake>, Waker) {
            let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
            let waker: Waker = Arc::clone(&inner).into();
            (inner, waker)
        }

        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        // Issue two in-flight requests against the lookup registry — one
        // bare `CommandLookupTopic` and one
        // `CommandPartitionedTopicMetadata`. The runtime would normally
        // create a `RequestFut` per request id and register a waker on
        // it; we mimic that registration directly.
        let lookup_rid = conn.lookup("persistent://public/default/foo", false);
        let partition_rid = conn.get_partitioned_topic_metadata("persistent://public/default/bar");
        let lookup_key = PendingOpKey::Request(lookup_rid);
        let partition_key = PendingOpKey::Request(partition_rid);

        let (lookup_counter, lookup_waker) = counting_waker();
        let (partition_counter, partition_waker) = counting_waker();
        conn.register_waker(lookup_key, lookup_waker);
        conn.register_waker(partition_key, partition_waker);

        // Pre-reset invariants: both rids live in the lookup registry
        // (so a late broker response could still correlate against them),
        // and the wakers are parked but unfired.
        assert!(
            conn.lookup.lookups.contains_key(&lookup_rid),
            "lookup registry holds the in-flight lookup pre-reset"
        );
        assert!(
            conn.lookup.partitions.contains(&partition_rid),
            "lookup registry holds the in-flight partition request pre-reset"
        );
        assert_eq!(
            lookup_counter.0.load(Ordering::SeqCst),
            0,
            "no waker fires pre-reset"
        );
        assert_eq!(
            partition_counter.0.load(Ordering::SeqCst),
            0,
            "no waker fires pre-reset"
        );

        conn.reset();

        // (1) Wakers fired exactly once each — the user's task is now
        // schedulable on the runtime, and the next poll will inspect
        // `take_outcome`.
        assert_eq!(
            lookup_counter.0.load(Ordering::SeqCst),
            1,
            "the lookup waker must fire exactly once on reset"
        );
        assert_eq!(
            partition_counter.0.load(Ordering::SeqCst),
            1,
            "the partitioned-metadata waker must fire exactly once on reset"
        );

        // (2) `OpOutcome::SessionLost` is published for both rids — the
        // user future observes the lost session immediately on its next
        // poll, NOT after the 30-second operation_timeout.
        match conn.take_outcome(lookup_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, lookup_key),
            other => panic!("expected SessionLost on lookup rid, got {other:?}"),
        }
        match conn.take_outcome(partition_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, partition_key),
            other => panic!("expected SessionLost on partition rid, got {other:?}"),
        }

        // (3) Registry is empty after reset — a stale broker response that
        // arrives on the dying socket's recv buffer mid-reconnect cannot
        // correlate against a still-pending entry (defensive cleanup).
        assert!(
            conn.lookup.lookups.is_empty(),
            "lookup registry is cleared after reset"
        );
        assert!(
            conn.lookup.partitions.is_empty(),
            "partition registry is cleared after reset"
        );
    }

    /// Companion to the test above: `reset` preserves the configured
    /// `max_pending_lookups` cap on the fresh registry, so the
    /// connection-wide DoS protection (lookup multi-agent review
    /// MEDIUM-2 / F1's hardening pass) survives the reconnect cycle.
    /// Pre-fix `self.lookup = LookupRegistry::default()` reset the cap to
    /// `0` (unbounded), silently disabling the cap until the next process
    /// restart.
    #[test]
    fn reset_preserves_max_pending_lookups_cap_across_reconnect() {
        let mut conn = Connection::new(
            ConnectionConfig {
                max_pending_lookups: 4,
                ..ConnectionConfig::default()
            },
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        assert_eq!(
            conn.lookup.max_pending, 4,
            "fresh connection inherits the configured cap"
        );

        // Drive a lookup, then reset. The cap must still be `4` on the
        // freshly-allocated registry — otherwise a misbehaving broker
        // could DoS the client by inducing a reconnect to clear the cap.
        let _rid = conn.lookup("persistent://public/default/foo", false);
        conn.reset();
        assert_eq!(
            conn.lookup.max_pending, 4,
            "max_pending_lookups cap MUST be re-applied to the freshly-allocated lookup registry"
        );
    }

    /// Drive a connection through the handshake and return it Connected with a
    /// known `keepalive_interval`, the outbound buffer drained. Shared setup for
    /// the ADR-0058 keepalive-watchdog tests below.
    fn connected_conn(keepalive: Duration) -> Connection {
        let cfg = ConnectionConfig {
            keepalive_interval: keepalive,
            ..ConnectionConfig::default()
        };
        let mut conn = Connection::new(cfg, std::sync::Arc::new(std::time::SystemTime::now));
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes_owned(Instant::now(), handshake_response_bytes())
            .expect("handshake completes");
        assert!(conn.is_connected(), "fixture must reach Connected");
        let _ = conn.poll_transmit(); // drain Connect frame + any pong
        conn
    }

    /// A `ping` is a self-contained no-payload frame; a fresh decode of one
    /// proves "the peer is still framing" without any session state.
    fn ping_frame_bytes() -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Ping as i32,
            ping: Some(pb::CommandPing {}),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandPing");
        buf
    }

    #[test]
    fn keepalive_baseline_refreshes_per_decoded_frame_not_per_raw_chunk() {
        // ADR-0058: the keepalive watchdog baseline (`last_activity`) must be
        // refreshed by a *decoded frame*, never by a raw inbound chunk. A
        // desynced-but-chatty socket — bytes whose announced `total_size`
        // (4-byte big-endian prefix, NOT checksummed) is plausible
        // (0 < N < MAX_FRAME_SIZE) but whose promised bytes never arrive — makes
        // `peek_full_frame_len` return `Incomplete` forever. Feeding such bytes
        // must NOT reset the baseline, otherwise the watchdog never fires on a
        // wedged-but-noisy connection (issues #187, #221).
        let keepalive = Duration::from_secs(30);
        let mut conn = connected_conn(keepalive);

        let t0 = Instant::now();
        // A complete ping frame at t0 sets the baseline.
        conn.handle_bytes_owned(t0, ping_frame_bytes())
            .expect("decode ping");
        let _ = conn.poll_transmit(); // drain the auto-pong
        assert_eq!(
            conn.last_activity,
            Some(t0),
            "a decoded frame refreshes the keepalive baseline",
        );

        // Now feed a desynced chunk: total_size = 1024 (plausible) but only a
        // few bytes of the promised 1024 follow. `peek_full_frame_len` parks
        // (Incomplete) — no frame decodes.
        let mut desync = bytes::BytesMut::new();
        desync.extend_from_slice(&1024u32.to_be_bytes()); // announced total_size
        desync.extend_from_slice(b"only-a-handful-of-bytes"); // far short of 1024
        let t1 = t0 + Duration::from_secs(5);
        conn.handle_bytes_owned(t1, desync)
            .expect("desync chunk parks, not an error");

        // The baseline must still be t0 — the chatty-but-frameless chunk did NOT
        // reset it. The pre-ADR-0058 code refreshed per raw chunk and this would
        // be `Some(t1)`, wedging the watchdog.
        assert_eq!(
            conn.last_activity,
            Some(t0),
            "a desynced chunk that decodes no frame must NOT refresh the baseline",
        );
    }

    #[test]
    fn keepalive_watchdog_escalates_to_failed_on_second_missed_interval() {
        // ADR-0058: when a keepalive ping goes unanswered for a second
        // consecutive interval, the watchdog escalates to `Failed` (via
        // `mark_disconnected`) instead of dead-pinging a wedged socket forever.
        // The driver reads `Failed` as `should_close` → supervised reconnect.
        let keepalive = Duration::from_secs(30);
        let mut conn = connected_conn(keepalive);

        let t0 = Instant::now();
        // Seed a baseline with a decoded frame so the first deadline is t0 + 30s.
        conn.handle_bytes_owned(t0, ping_frame_bytes())
            .expect("decode ping");
        let _ = conn.poll_transmit();
        assert_eq!(conn.last_activity, Some(t0));

        // First interval elapses with no inbound frame → emit a ping, arm the
        // outstanding flag, stay Connected.
        let t1 = t0 + keepalive;
        conn.handle_timeout(t1);
        assert!(conn.is_connected(), "first missed interval only pings");
        let out = conn.poll_transmit();
        assert!(
            !out.is_empty(),
            "first missed interval emits a keepalive ping",
        );

        // Second interval elapses still with no inbound frame → escalate to
        // Failed. The driver treats Failed as should_close.
        let t2 = t1 + keepalive;
        conn.handle_timeout(t2);
        assert!(
            !conn.is_connected(),
            "second consecutive unanswered interval must fail the connection",
        );
        assert_eq!(
            conn.state(),
            HandshakeState::Failed,
            "watchdog escalates to Failed, not another ping",
        );
    }

    #[test]
    fn keepalive_inbound_frame_clears_outstanding_ping() {
        // ADR-0058: a single decoded inbound frame between two keepalive
        // intervals clears the outstanding ping, so the watchdog re-arms from
        // scratch rather than escalating — a live-but-slow peer is never failed.
        let keepalive = Duration::from_secs(30);
        let mut conn = connected_conn(keepalive);

        let t0 = Instant::now();
        conn.handle_bytes_owned(t0, ping_frame_bytes())
            .expect("decode ping");
        let _ = conn.poll_transmit();

        // First interval: ping goes out, outstanding flag armed.
        let t1 = t0 + keepalive;
        conn.handle_timeout(t1);
        assert!(conn.keepalive_ping_outstanding, "ping is now outstanding");
        let _ = conn.poll_transmit();

        // The peer answers (any decoded frame counts) before the next interval.
        let t_reply = t1 + Duration::from_secs(1);
        conn.handle_bytes_owned(t_reply, ping_frame_bytes())
            .expect("decode peer frame");
        let _ = conn.poll_transmit();
        assert!(
            !conn.keepalive_ping_outstanding,
            "a decoded inbound frame clears the outstanding ping",
        );

        // Next interval: because the flag was cleared, this only pings again —
        // it does NOT escalate.
        let t2 = t_reply + keepalive;
        conn.handle_timeout(t2);
        assert!(
            conn.is_connected(),
            "a live peer is never failed; the watchdog re-arms cleanly",
        );
    }

    /// ADR-0060 (layer a): the proto-level surface the
    /// engine-side bounded lookup-retry loop consults. An in-flight lookup
    /// severed by `reset()` surfaces `OpOutcome::SessionLost` (the signal to
    /// re-issue, NOT a terminal error), and after a fresh handshake the
    /// connection is `is_connected()` again so a re-issued lookup lands a NEW,
    /// resolvable request-id on the new session. This is what lets the engine
    /// loop TERMINATE on the next reconnect instead of spinning: one
    /// `SessionLost` → one re-issue → a real broker round-trip on the fresh
    /// session. The bound [`crate::lookup::MAX_LOOKUP_SESSION_REISSUES`] caps the
    /// number of such re-issues; this test proves the happy-path re-issue is
    /// resolvable (so the bound is a ceiling, not the steady state).
    #[test]
    fn reissued_lookup_after_reset_resolves_on_fresh_session() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();
        assert!(conn.is_connected(), "connected after the first handshake");

        // First lookup goes in flight, then the supervised reconnect severs it.
        let first_rid = conn.lookup("persistent://public/default/foo", false);
        conn.reset();

        // The engine loop's `matches!(outcome, OpOutcome::SessionLost { .. })`
        // arm fires on exactly this outcome — the re-issue signal.
        match conn.take_outcome(PendingOpKey::Request(first_rid)) {
            Some(OpOutcome::SessionLost { key }) => {
                assert_eq!(key, PendingOpKey::Request(first_rid));
            }
            other => panic!("expected SessionLost on the severed lookup, got {other:?}"),
        }

        // `reset()` snapped the state machine back to `Uninitialized`; the
        // engine loop parks on readiness (`await_reconnect_or_terminal`) until
        // the supervisor re-handshakes the socket.
        assert!(
            !conn.is_connected(),
            "post-reset the connection is not yet live — the loop must park, not re-issue"
        );

        // Supervisor re-handshakes the new socket → back to Connected. This is
        // the `Reconnected` branch the engine loop waits for.
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle re-handshake");
        let _ = conn.poll_event();
        assert!(
            conn.is_connected(),
            "connection is live again after the supervised re-handshake"
        );

        // The re-issued lookup lands a fresh request-id resolvable on the new
        // session — distinct from the severed one, and present in the registry
        // so a `LookupResponse` on the new socket correlates against it.
        let reissued_rid = conn.lookup("persistent://public/default/foo", false);
        assert_ne!(
            reissued_rid, first_rid,
            "the re-issued lookup uses a fresh request-id on the new session"
        );
        assert!(
            conn.lookup.lookups.contains_key(&reissued_rid),
            "the re-issued lookup is registered against the fresh session, so its \
             broker response will resolve it — the loop terminates",
        );
    }

    /// ADR-0060 (layer a): the terminal short-circuit the
    /// engine loop's `await_reconnect_or_terminal` returns. When the connection
    /// has gone `is_closed()` (here: a transport `Failed`) and the runtime's
    /// `no_driver` latch is set, the engine maps a severed lookup to a clean
    /// `PeerClosed` instead of re-issuing — composing with ADR-0059. The proto layer
    /// owns the `is_closed()` half of that decision; this pins it.
    #[test]
    fn failed_connection_is_closed_so_lookup_loop_short_circuits() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        let _ = conn.poll_event();

        // A transport drop with NO supervisor reconnect → `Failed`, which
        // `is_closed()` reports `true`. Paired with the runtime `no_driver`
        // latch (ADR-0059), the engine loop returns `Terminal` → `PeerClosed`.
        conn.mark_disconnected();
        assert!(
            conn.is_closed(),
            "a Failed connection reports is_closed(); the engine pairs this with \
             no_driver to short-circuit the lookup loop to PeerClosed"
        );
        assert!(
            !conn.is_connected(),
            "a Failed connection is not connected — the loop never re-issues"
        );
    }

    // --- Negative-ack must remove the nacked id from the ack-timeout tracker ---
    //
    // Regression coverage for the double-redelivery bug: a message that is both
    // ack-timeout tracked and nacked must be redelivered EXACTLY ONCE. Before the
    // fix, [`Connection::negative_ack`] only added the id to the nack tracker, so
    // [`Connection::handle_timeout`] redelivered it twice — once from the nack
    // tracker, once from the unacked (ack-timeout) sweep. Each of these tests FAILS
    // on `main` (sees TWO redeliveries) and PASSES after the unconditional
    // `unacked_tracker.remove(...)` lands.

    /// Build a connected connection plus a subscription configured with `ack_timeout`
    /// and an optional `negative_ack_redelivery_delay`. The unacked-message tracker is
    /// armed at subscribe time, so deliveries recorded afterward participate in the
    /// ack-timeout sweep.
    fn nack_test_conn(
        ack_timeout: Duration,
        nack_delay: Option<Duration>,
    ) -> (Connection, ConsumerHandle) {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected, got {other:?}"),
        }
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/nack-unacked".to_owned(),
            subscription: "sub-nack-unacked".to_owned(),
            ack_timeout: Some(ack_timeout),
            negative_ack_redelivery_delay: nack_delay,
            ..Default::default()
        });
        // Drain the outbound CommandSubscribe + initial flow so later poll_transmit
        // calls observe only the redelivery frames the sweep queues.
        let _ = conn.poll_transmit();
        (conn, handle)
    }

    /// Deliver one synthetic non-batched message carrying the `(ledger, entry)` identity
    /// onto `handle`, then drain its `Message` event and any outbound bytes. The unacked
    /// tracker keys on the per-index `MessageId` the delivery path produces — for a
    /// non-batched entry that is `batch_index = -1, batch_size = 0`.
    fn deliver_one(
        conn: &mut Connection,
        handle: ConsumerHandle,
        now: Instant,
        ledger: u64,
        entry: u64,
    ) {
        let meta = pb::MessageMetadata {
            producer_name: "magnetar-test-prod".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000_000,
            num_messages_in_batch: Some(1),
            ..Default::default()
        };
        let cmd = deliver_cmd(handle, ledger, entry);
        let mut frame = bytes::BytesMut::new();
        crate::frame::encode_payload(&mut frame, &cmd, &meta, b"nack-unacked-payload")
            .expect("encode message frame");
        deliver_frame(conn, now, &frame);
    }

    /// Deliver one synthetic BATCH of `count` messages on `(ledger, entry)`. Each
    /// sub-message lands in the unacked tracker keyed on the per-index id
    /// `batch_index = idx, batch_size = count` (consumer.rs batch-explosion path), so a
    /// nack of one batch-index id exercises the batched removal.
    fn deliver_batch(
        conn: &mut Connection,
        handle: ConsumerHandle,
        now: Instant,
        ledger: u64,
        entry: u64,
        count: i32,
    ) {
        let meta = pb::MessageMetadata {
            producer_name: "magnetar-test-prod".to_owned(),
            sequence_id: 1,
            publish_time: 1_700_000_000_000,
            num_messages_in_batch: Some(count),
            ..Default::default()
        };
        // Batched body: `(u32 single_size)(SingleMessageMetadata)(payload)` per entry,
        // matching the wire format `ConsumerState::deliver` parses.
        let mut body = bytes::BytesMut::new();
        for idx in 0..count {
            let payload = format!("batch-{idx}").into_bytes();
            let sm = pb::SingleMessageMetadata {
                payload_size: payload.len() as i32,
                ..Default::default()
            };
            let sm_len = prost::Message::encoded_len(&sm);
            body.extend_from_slice(&(sm_len as u32).to_be_bytes());
            prost::Message::encode(&sm, &mut body).expect("encode SingleMessageMetadata");
            body.extend_from_slice(&payload);
        }
        let cmd = deliver_cmd(handle, ledger, entry);
        let mut frame = bytes::BytesMut::new();
        crate::frame::encode_payload(&mut frame, &cmd, &meta, &body).expect("encode batch frame");
        deliver_frame(conn, now, &frame);
    }

    /// Build the `CommandMessage` for a `(ledger, entry)` delivery on `handle`. The
    /// broker-supplied id carries no batch fields; the consumer fills them per sub-message.
    fn deliver_cmd(handle: ConsumerHandle, ledger: u64, entry: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: handle.0,
                message_id: pb::MessageIdData {
                    ledger_id: ledger,
                    entry_id: entry,
                    partition: None,
                    batch_index: None,
                    ack_set: Vec::new(),
                    batch_size: None,
                    first_chunk_message_id: None,
                },
                redelivery_count: Some(0),
                ack_set: Vec::new(),
                consumer_epoch: None,
            }),
            ..Default::default()
        }
    }

    /// Feed a synthetic delivery `frame` into the connection, then drain the resulting
    /// `Message` event(s) and any flow/ack bytes so a later `poll_transmit` observes only
    /// the redelivery frames the timeout sweep queues.
    fn deliver_frame(conn: &mut Connection, now: Instant, frame: &[u8]) {
        conn.handle_bytes(now, frame).expect("deliver message");
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    /// Count how many `CommandRedeliverUnacknowledgedMessages` frames the connection
    /// has queued on its outbound buffer (one per redelivery the sweep emitted).
    fn count_redeliver_frames(conn: &mut Connection) -> usize {
        let mut bytes = conn.poll_transmit();
        let mut count = 0;
        while !bytes.is_empty() {
            let frame = crate::frame::decode_one(&mut bytes).expect("decode outbound frame");
            if frame.command.r#type
                == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32
            {
                count += 1;
            }
        }
        count
    }

    #[test]
    fn negative_ack_removes_id_from_unacked_tracker_so_redelivery_fires_once() {
        let t0 = Instant::now();
        let ack_timeout = Duration::from_secs(10);
        let nack_delay = Duration::from_secs(2);
        let (mut conn, handle) = nack_test_conn(ack_timeout, Some(nack_delay));
        // Deliver a non-batched message; the unacked tracker arms its ack-timeout deadline.
        deliver_one(&mut conn, handle, t0, 7, 3);
        // The single-message delivery path normalises a non-batched id to
        // `batch_index = -1, batch_size = 0` (consumer.rs), so the nacked id the user
        // would hold (and that the unacked tracker keyed on) carries `batch_size: 0`.
        let nacked_id = MessageId {
            ledger_id: 7,
            entry_id: 3,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        // Nack it. The nack tracker defers the redelivery to t0 + nack_delay; the fix
        // also drops it from the unacked tracker so the ack-timeout sweep won't fire.
        conn.negative_ack(handle, vec![nacked_id], t0);
        // Advance past BOTH the nack delay AND the ack timeout in one sweep. On `main`
        // this produces TWO redelivery frames (nack + ack-timeout); the fix yields ONE.
        conn.handle_timeout(t0 + Duration::from_secs(11));
        assert_eq!(
            count_redeliver_frames(&mut conn),
            1,
            "a nacked + ack-timeout-tracked message must be redelivered exactly once, \
             not twice (nack tracker + ack-timeout sweep)"
        );
    }

    #[test]
    fn negative_ack_removes_batched_id_from_unacked_tracker_so_redelivery_fires_once() {
        let t0 = Instant::now();
        let ack_timeout = Duration::from_secs(10);
        let nack_delay = Duration::from_secs(2);
        let (mut conn, handle) = nack_test_conn(ack_timeout, Some(nack_delay));
        // Deliver a 2-message batch; both sub-messages land in the unacked tracker keyed on
        // `batch_index = idx, batch_size = 2`. Nack BOTH so no un-nacked id is left to time
        // out — the nack tracker coalesces them into ONE redelivery frame, and the fix must
        // drop both from the unacked tracker so the ack-timeout sweep adds none. On `main`
        // the sweep adds a second frame for the still-tracked batch ids → two frames.
        deliver_batch(&mut conn, handle, t0, 9, 4, 2);
        let batched = |idx: i32| MessageId {
            ledger_id: 9,
            entry_id: 4,
            partition: -1,
            batch_index: idx,
            batch_size: 2,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        conn.negative_ack(handle, vec![batched(0), batched(1)], t0);
        conn.handle_timeout(t0 + Duration::from_secs(11));
        assert_eq!(
            count_redeliver_frames(&mut conn),
            1,
            "nacked batch-index messages must be redelivered exactly once (one coalesced \
             nack frame), not twice (nack tracker + ack-timeout sweep)"
        );
    }

    #[test]
    fn negative_ack_without_nack_tracker_removes_id_from_unacked_tracker() {
        // NACK-ABSENT path: ack_timeout configured but NO nack tracker. `negative_ack`
        // emits an immediate redelivery (fall-through to `emit_redeliver_unacked`) AND
        // must still drop the id from the unacked tracker — otherwise the ack-timeout
        // sweep adds a SECOND redelivery later. The unconditional removal (not nested in
        // the `nack_tracker.as_mut()` block, which early-returns) guarantees one redelivery.
        let t0 = Instant::now();
        let ack_timeout = Duration::from_secs(10);
        let (mut conn, handle) = nack_test_conn(ack_timeout, None);
        deliver_one(&mut conn, handle, t0, 5, 1);
        // Non-batched delivery normalises to `batch_size: 0` (consumer.rs).
        let nacked_id = MessageId {
            ledger_id: 5,
            entry_id: 1,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        // Immediate redelivery #1 (no nack tracker to defer it).
        conn.negative_ack(handle, vec![nacked_id], t0);
        assert_eq!(
            count_redeliver_frames(&mut conn),
            1,
            "with no nack tracker, negative_ack emits exactly one immediate redelivery"
        );
        // The ack-timeout sweep must NOT emit a second redelivery: the id was removed.
        conn.handle_timeout(t0 + Duration::from_secs(11));
        assert_eq!(
            count_redeliver_frames(&mut conn),
            0,
            "the ack-timeout sweep must not re-redeliver an id already nacked + removed"
        );
    }

    // --- Cumulative ack must prune every batch-ack tracker entry it covers ---
    //
    // Regression coverage for issue #326: a consumer that only ever acks cumulatively
    // (e.g. a watermark acker) used to leak one `BatchAckEntry` per batched broker
    // entry — the cumulative branch removed only the tracker entry of the acked id
    // itself, never the entries below it, and only a reconnect cleared the map.

    /// Snapshot the `(ledger, entry)` keys currently held by `handle`'s batch-ack
    /// tracker, sorted for deterministic assertions.
    fn batch_tracker_keys(conn: &Connection, handle: ConsumerHandle) -> Vec<(u64, u64)> {
        let slot = conn.consumers.get(&handle).expect("consumer slot");
        let state = slot.state.lock();
        let mut keys: Vec<(u64, u64)> = state.batch_ack_tracker.keys().copied().collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn cumulative_ack_prunes_batch_ack_tracker_entries_at_and_below_the_acked_position() {
        let t0 = Instant::now();
        let (mut conn, handle) = nack_test_conn(Duration::from_secs(10), None);
        // Four batched entries across a ledger rollover; each stamps one tracker entry.
        deliver_batch(&mut conn, handle, t0, 7, 1, 2);
        deliver_batch(&mut conn, handle, t0, 7, 2, 2);
        deliver_batch(&mut conn, handle, t0, 7, 3, 2);
        deliver_batch(&mut conn, handle, t0, 8, 0, 2);
        assert_eq!(
            batch_tracker_keys(&conn, handle),
            vec![(7, 1), (7, 2), (7, 3), (8, 0)],
            "every batched delivery stamps one batch-ack tracker entry"
        );
        // Cumulative ack at (7, 3): (7, 1) and (7, 2) are covered by the cumulative
        // position and must be pruned along with (7, 3) itself; (8, 0) is above the
        // horizon and must survive.
        let _ = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![MessageId {
                    ledger_id: 7,
                    entry_id: 3,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                    #[cfg(feature = "scalable-topics")]
                    segment_id: None,
                }],
                ack_type: pb::command_ack::AckType::Cumulative,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        assert_eq!(
            batch_tracker_keys(&conn, handle),
            vec![(8, 0)],
            "a cumulative ack must prune every tracker entry at or below its position, \
             not just the exact (ledger, entry) of the acked id (issue #326)"
        );
    }

    #[test]
    fn cumulative_only_acking_keeps_the_batch_ack_tracker_bounded() {
        // The production workload that surfaced #326: a stream of batched entries with a
        // cumulative ack every N messages and never an individual ack. The tracker must
        // stay bounded by the un-acked window instead of growing with every entry consumed.
        let t0 = Instant::now();
        let (mut conn, handle) = nack_test_conn(Duration::from_secs(10), None);
        let ack_every = 10u64;
        for entry in 0..100u64 {
            deliver_batch(&mut conn, handle, t0, 12, entry, 2);
            if (entry + 1) % ack_every == 0 {
                let _ = conn.ack(
                    handle,
                    AckRequest {
                        message_ids: vec![MessageId {
                            ledger_id: 12,
                            entry_id: entry,
                            partition: -1,
                            batch_index: -1,
                            batch_size: 0,
                            #[cfg(feature = "scalable-topics")]
                            segment_id: None,
                        }],
                        ack_type: pb::command_ack::AckType::Cumulative,
                        properties: Vec::new(),
                        txn_id: None,
                    },
                    t0,
                );
                assert_eq!(
                    batch_tracker_keys(&conn, handle),
                    Vec::<(u64, u64)>::new(),
                    "after a cumulative ack at the consume front the tracker is empty; \
                     before the #326 fix it kept every entry below the acked position"
                );
            }
        }
    }
}

#[cfg(all(test, feature = "scalable-topics"))]
mod scalable_conn_tests {
    use super::*;

    /// Build a connected `Connection` whose peer advertised
    /// `supports_scalable_topics` (a PIP-460-capable Pulsar 5.x broker).
    fn connected_conn() -> Connection {
        connected_conn_with_scalable_support(true)
    }

    /// Build a connected `Connection`, choosing whether the peer advertises the
    /// PIP-460 capability. `false` models a Pulsar 4.x broker.
    fn connected_conn_with_scalable_support(supported: bool) -> Connection {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(crate::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags {
                    supports_scalable_topics: supported.then_some(true),
                    ..pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        crate::frame::encode_command(&mut buf, &cmd).expect("encode Connected");
        conn.handle_bytes(Instant::now(), &buf).expect("connected");
        // Drain the handshake `Connected` event and the outbound
        // `CommandConnect` so per-test assertions only see the scalable-topic
        // traffic.
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
        conn
    }

    fn info(id: u64, start: u32, end: u32, parents: &[u64]) -> pb::SegmentInfoProto {
        pb::SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: pb::SegmentState::Active as i32,
            parent_ids: parents.to_vec(),
            child_ids: Vec::new(),
            created_at_epoch: 0,
            sealed_at_epoch: None,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: None,
        }
    }

    /// Encode a broker→client `CommandScalableTopicUpdate` frame.
    fn update_frame(
        session_id: u64,
        epoch: u64,
        segments: Vec<pb::SegmentInfoProto>,
    ) -> bytes::BytesMut {
        let segment_brokers = segments
            .iter()
            .map(|s| pb::SegmentBrokerAddress {
                segment_id: s.segment_id,
                broker_url: format!("pulsar://seg{}:6650", s.segment_id),
                broker_url_tls: None,
            })
            .collect();
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
            scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
                session_id,
                dag: Some(pb::ScalableTopicDag {
                    epoch,
                    segments,
                    segment_brokers,
                    controller_broker_url: Some("pulsar://controller:6650".to_owned()),
                    controller_broker_url_tls: None,
                }),
                error: None,
                message: None,
                resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        crate::frame::encode_command(&mut buf, &cmd).expect("encode update");
        buf
    }

    /// Layer (a) test: the lookup rides the ordinary `BaseCommand` framing —
    /// a `SCALABLE_TOPIC_LOOKUP` frame carrying the client-allocated session id
    /// and the requested topic, decodable by any Pulsar-compatible peer.
    #[test]
    fn conn_encodes_scalable_topic_lookup_as_base_command() {
        let mut conn = connected_conn();
        let session_id = conn
            .open_scalable_topic_session("topic://public/default/scaled")
            .expect("broker supports scalable topics");

        let mut out = conn.poll_transmit();
        let frame = crate::frame::decode_one(&mut out).expect("decodes as a v5 BaseCommand");
        assert_eq!(
            frame.command.r#type,
            pb::base_command::Type::ScalableTopicLookup as i32
        );
        let lookup = frame
            .command
            .scalable_topic_lookup
            .expect("lookup payload present");
        assert_eq!(lookup.session_id, session_id);
        assert_eq!(lookup.topic, "topic://public/default/scaled");
    }

    /// Layer (a) test: the first `CommandScalableTopicUpdate` resolves the
    /// session and emits `ScalableTopicLookupResolved` with the layout, its
    /// epoch, and the canonical topic identity.
    #[test]
    fn conn_emits_scalable_topic_lookup_resolved() {
        let mut conn = connected_conn();
        let session_id = conn
            .open_scalable_topic_session("persistent://public/default/scaled")
            .expect("broker supports scalable topics");
        let _ = conn.poll_transmit();

        let buf = update_frame(
            session_id,
            3,
            vec![info(1, 0, 32_768, &[]), info(2, 32_768, 65_536, &[])],
        );
        conn.handle_bytes(Instant::now(), &buf).expect("update");

        let mut resolved = None;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::ScalableTopicLookupResolved {
                session_id: got,
                resolved_topic_name,
                controller_broker_url,
                segments,
                epoch,
            } = ev
            {
                resolved = Some((
                    got,
                    resolved_topic_name,
                    controller_broker_url,
                    segments,
                    epoch,
                ));
            }
        }
        let (got, resolved_topic_name, controller_broker_url, segments, epoch) =
            resolved.expect("ScalableTopicLookupResolved emitted");
        assert_eq!(got, session_id);
        assert_eq!(epoch, 3);
        assert_eq!(
            resolved_topic_name.as_deref(),
            Some("topic://public/default/scaled"),
            "the broker's canonical identity is surfaced, not the requested form"
        );
        assert_eq!(
            controller_broker_url.as_deref(),
            Some("pulsar://controller:6650")
        );
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].broker_url.as_deref(),
            Some("pulsar://seg1:6650"),
            "placement is joined from the parallel address list"
        );
    }

    /// Layer (a) test: a second layout on the same session emits
    /// `SegmentDagUpdated` plus `DagChangedDuringConsume { Split }`, with the
    /// split derived from the children's `parent_ids`.
    #[test]
    fn conn_emits_dag_changed_on_split() {
        let mut conn = connected_conn();
        let session_id = conn
            .open_scalable_topic_session("topic://public/default/scaled")
            .expect("broker supports scalable topics");
        let _ = conn.poll_transmit();

        // First layout resolves the session.
        let buf = update_frame(session_id, 1, vec![info(1, 0, 65_536, &[])]);
        conn.handle_bytes(Instant::now(), &buf).expect("initial");
        while conn.poll_event().is_some() {}

        // Second layout splits segment 1 into 2 and 3.
        let buf = update_frame(
            session_id,
            2,
            vec![info(2, 0, 32_768, &[1]), info(3, 32_768, 65_536, &[1])],
        );
        conn.handle_bytes(Instant::now(), &buf).expect("split");

        let mut saw_updated = false;
        let mut saw_changed = false;
        while let Some(ev) = conn.poll_event() {
            match ev {
                ConnectionEvent::SegmentDagUpdated {
                    session_id: got,
                    delta,
                } => {
                    assert_eq!(got, session_id);
                    assert_eq!(delta.epoch, 2);
                    assert_eq!(delta.removed, vec![crate::types::SegmentId(1)]);
                    assert_eq!(delta.split_events.len(), 1);
                    saw_updated = true;
                }
                ConnectionEvent::DagChangedDuringConsume {
                    session_id: got,
                    reason,
                } => {
                    assert_eq!(got, session_id);
                    assert_eq!(reason, crate::dag_watch::DagChangeReason::Split);
                    saw_changed = true;
                }
                _ => {}
            }
        }
        assert!(saw_updated, "SegmentDagUpdated emitted");
        assert!(saw_changed, "DagChangedDuringConsume emitted on split");
        // Post-split DAG: parent gone, two children present.
        let snap = conn.dag_snapshot(session_id).expect("session still open");
        assert_eq!(snap.len(), 2);
    }

    /// Layer (a) test — **v4 compatibility**. Against a broker that did not
    /// advertise `supports_scalable_topics`, opening a session is refused and
    /// **nothing is written to the wire**. This is the guard that keeps a
    /// scalable-topics build usable against Pulsar 4.x.
    #[test]
    fn conn_refuses_scalable_lookup_against_v4_broker() {
        let mut conn = connected_conn_with_scalable_support(false);
        let _ = conn.poll_transmit();

        assert!(!conn.broker_supports_scalable_topics());
        let err = conn
            .open_scalable_topic_session("topic://public/default/scaled")
            .expect_err("v4 broker refuses the scalable surface");
        assert_eq!(err, crate::dag_watch::ScalableTopicError::BrokerUnsupported);
        assert!(
            conn.poll_transmit().is_empty(),
            "no scalable command may reach a broker that cannot parse it"
        );
    }

    /// Layer (a) test — **v4 compatibility**, outbound half. A client compiled
    /// with `scalable-topics` advertises the capability in `CommandConnect`, so
    /// the broker can answer in kind. The flag is additive: a v4 broker ignores
    /// an unknown feature-flag field.
    #[test]
    fn conn_advertises_scalable_capability_on_connect() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let mut out = conn.poll_transmit();
        let frame = crate::frame::decode_one(&mut out).expect("decodes CommandConnect");
        let connect = frame.command.connect.expect("connect payload");
        assert_eq!(
            connect
                .feature_flags
                .and_then(|f| f.supports_scalable_topics),
            Some(true)
        );
        assert_eq!(
            connect.protocol_version,
            Some(crate::SUPPORTED_PROTOCOL_VERSION),
            "PIP-460 is gated on a feature flag, not on a protocol-version bump"
        );
    }

    /// Closing a session emits `CommandScalableTopicClose` and drops the state;
    /// a second close is a no-op rather than a duplicate frame.
    #[test]
    fn conn_close_scalable_session_is_idempotent() {
        let mut conn = connected_conn();
        let session_id = conn
            .open_scalable_topic_session("topic://public/default/scaled")
            .expect("broker supports scalable topics");
        let _ = conn.poll_transmit();

        conn.close_scalable_topic_session(session_id);
        let mut out = conn.poll_transmit();
        let frame = crate::frame::decode_one(&mut out).expect("decodes close");
        assert_eq!(
            frame.command.r#type,
            pb::base_command::Type::ScalableTopicClose as i32
        );
        assert_eq!(
            frame
                .command
                .scalable_topic_close
                .expect("close payload")
                .session_id,
            session_id
        );
        assert!(conn.dag_snapshot(session_id).is_none(), "session dropped");

        conn.close_scalable_topic_session(session_id);
        assert!(
            conn.poll_transmit().is_empty(),
            "closing an unknown session writes nothing"
        );
    }
}

#[cfg(test)]
mod consumer_close_contract_tests {
    use super::*;
    use crate::frame::{decode_one, encode_command};

    fn handshake_complete(now: Instant) -> Connection {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("begin_handshake");
        let connected = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &connected).expect("encode connected");
        conn.handle_bytes(now, &buf).expect("apply connected");
        let _ = conn.poll_transmit();
        conn
    }

    fn subscribe(conn: &mut Connection, suffix: &str) -> ConsumerHandle {
        conn.subscribe(SubscribeRequest {
            topic: format!("persistent://public/default/{suffix}"),
            subscription: suffix.to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
    }

    fn drain_command_types(conn: &mut Connection) -> Vec<i32> {
        let mut bytes = conn.poll_transmit();
        let mut kinds = Vec::new();
        while !bytes.is_empty() {
            let frame = decode_one(&mut bytes).expect("staged frame must decode");
            kinds.push(frame.command.r#type);
        }
        kinds
    }

    fn ack_success(conn: &mut Connection, request_id: u64, now: Instant) {
        let ack = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ack).expect("encode Success");
        conn.handle_bytes(now, &buf).expect("apply Success");
    }

    fn ack_error(conn: &mut Connection, request_id: u64, now: Instant) {
        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "synthetic close rejection".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode Error");
        conn.handle_bytes(now, &buf).expect("apply Error");
    }

    #[test]
    fn close_consumer_marks_slot_closed_synchronously() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "close-sync");
        let slot = conn.consumer(handle).expect("slot exists").clone();
        assert!(!slot.state.lock().closed, "fresh consumer must be open");

        let _request_id = conn.close_consumer(handle, now);

        assert!(
            slot.state.lock().closed,
            "closed flag must flip synchronously inside close_consumer"
        );
    }

    #[test]
    fn close_consumer_stages_close_frame() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "close-frame");
        let _ = conn.poll_transmit();

        let _request_id = conn.close_consumer(handle, now);

        assert_eq!(
            drain_command_types(&mut conn),
            vec![pb::base_command::Type::CloseConsumer as i32],
            "close_consumer must stage exactly one CloseConsumer frame"
        );
    }

    #[test]
    fn close_consumer_flushes_grouped_acks_before_close_frame() {
        let message_id = MessageId {
            ledger_id: 7,
            entry_id: 11,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };

        for forget in [false, true] {
            let now = Instant::now();
            let mut conn = handshake_complete(now);
            let handle = conn.subscribe(SubscribeRequest {
                topic: format!("persistent://public/default/close-ack-order-{forget}"),
                subscription: format!("close-ack-order-{forget}"),
                receiver_queue_size: 16,
                durable: true,
                ack_group_time: Some(Duration::from_mins(1)),
                ..Default::default()
            });
            let _ = conn.poll_transmit();
            conn.ack_grouped_individual(handle, message_id, now);

            if forget {
                let _request_id = conn.close_consumer_forget(handle, now);
            } else {
                let _request_id = conn.close_consumer(handle, now);
            }

            assert_eq!(
                drain_command_types(&mut conn),
                vec![
                    pb::base_command::Type::Ack as i32,
                    pb::base_command::Type::CloseConsumer as i32,
                ],
                "{forget:?} close must flush grouped acknowledgements before CloseConsumer"
            );
        }
    }

    #[test]
    fn close_consumer_forget_records_no_outcome_on_success() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "forget-success");
        let slot = conn.consumer(handle).expect("slot exists").clone();

        let request_id = conn.close_consumer_forget(handle, now);
        assert!(
            slot.state.lock().closed,
            "forget variant must still flip the closed flag synchronously"
        );
        ack_success(&mut conn, request_id.0, now);

        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "fire-and-forget close ack must be consumed in-place, not recorded"
        );
    }

    #[test]
    fn close_consumer_forget_records_no_outcome_on_broker_error() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "forget-error");

        let request_id = conn.close_consumer_forget(handle, now);
        ack_error(&mut conn, request_id.0, now);

        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "rejected fire-and-forget close must not leak an OpOutcome entry"
        );
    }

    #[test]
    fn close_consumer_forget_records_no_outcome_on_reset() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "forget-reset");

        let request_id = conn.close_consumer_forget(handle, now);
        conn.reset();

        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "reset must not materialize an outcome for a forgotten close"
        );
    }

    #[test]
    fn close_consumer_forget_records_no_outcome_on_fail_all_pending() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "forget-fail-all");

        let request_id = conn.close_consumer_forget(handle, now);
        conn.fail_all_pending("synthetic terminal drop");

        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "fail_all_pending must not materialize an outcome for a forgotten close"
        );
    }

    #[test]
    fn close_consumer_awaited_still_records_outcome() {
        let now = Instant::now();
        let mut conn = handshake_complete(now);
        let handle = subscribe(&mut conn, "awaited-close");

        let request_id = conn.close_consumer(handle, now);
        ack_success(&mut conn, request_id.0, now);

        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_some(),
            "awaited close must record the outcome its RequestFut drains"
        );
    }
}

#[cfg(test)]
mod handshake_failure_reason_tests {
    use super::*;

    fn fresh_conn() -> Connection {
        Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(SystemTime::now),
        )
    }

    fn handshake_response_bytes() -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "test-broker/1.0".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    /// A broker `CommandError` arriving while the connection is still in
    /// `ConnectSent` (or `AuthChallenging`) must be captured as the
    /// connection's `handshake_failure_reason`, so the engine can surface
    /// it instead of the opaque "handshake failed" / "peer closed" message
    /// when the supervisor flips the state to `Failed` after the socket
    /// drops.
    #[test]
    fn command_error_during_handshake_is_captured_as_failure_reason() {
        let mut conn = fresh_conn();
        conn.begin_handshake().expect("begin");
        assert_eq!(conn.state(), HandshakeState::ConnectSent);
        assert!(conn.handshake_failure_reason().is_none());

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: 0,
                error: pb::ServerError::AuthenticationError as i32,
                message: "token expired".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        let reason = conn
            .handshake_failure_reason()
            .expect("handshake CommandError must populate failure reason");
        assert!(
            reason.contains("AuthenticationError"),
            "reason should carry the ServerError variant: {reason}",
        );
        assert!(
            reason.contains("token expired"),
            "reason should carry the broker message verbatim: {reason}",
        );

        // Simulate the supervisor noticing the peer close and flipping
        // state. The reason persists across the flip so the engine can
        // surface it on the user-facing future.
        conn.mark_disconnected();
        assert_eq!(conn.state(), HandshakeState::Failed);
        assert!(
            conn.handshake_failure_reason().is_some(),
            "reason must survive the Failed transition until reset()",
        );

        // `reset()` clears it so a redial doesn't replay the previous failure.
        conn.reset();
        assert_eq!(conn.state(), HandshakeState::Uninitialized);
        assert!(
            conn.handshake_failure_reason().is_none(),
            "reset() must clear the reason for the next handshake attempt",
        );
    }

    /// `CommandError` arriving on an already-`Connected` connection (e.g.
    /// a stale producer-open error) MUST NOT pollute the handshake reason
    /// — the failure-reason field is exclusively for ConnectSent /
    /// AuthChallenging state.
    #[test]
    fn command_error_post_handshake_does_not_populate_failure_reason() {
        let mut conn = fresh_conn();
        let handshake = handshake_response_bytes();
        conn.begin_handshake().expect("begin");
        conn.handle_bytes(Instant::now(), &handshake)
            .expect("handle CONNECTED");
        assert_eq!(conn.state(), HandshakeState::Connected);

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: 99,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "namespace bundle not served".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        assert!(
            conn.handshake_failure_reason().is_none(),
            "post-handshake CommandError must not leak into handshake_failure_reason",
        );
    }

    /// ADR-0062: a hostile broker can return an arbitrarily long `message`
    /// in its mid-handshake `CommandError`. The capture site must bound the
    /// broker text to [`MAX_BROKER_STR`] bytes at a char boundary BEFORE it
    /// is stored in `handshake_failure_reason`, so every downstream sink
    /// (tokio `ClientError`, moonpool `EngineError::HandshakeFailed`) inherits
    /// the bound. Mirrors the `truncation_respects_char_boundaries` unit test
    /// in `log_fields`.
    #[test]
    fn handshake_failure_reason_bounds_oversized_broker_message() {
        let mut conn = fresh_conn();
        conn.begin_handshake().expect("begin");
        assert_eq!(conn.state(), HandshakeState::ConnectSent);

        // 'é' is 2 bytes; 400 of them = 800 bytes, with the 256-byte cut
        // falling mid-char so the boundary back-off is exercised too.
        let oversized = "é".repeat(400);
        assert!(oversized.len() > crate::log_fields::MAX_BROKER_STR);
        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: 0,
                error: pb::ServerError::AuthenticationError as i32,
                message: oversized.clone(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");

        let reason = conn
            .handshake_failure_reason()
            .expect("oversized broker message must still populate the reason");
        // The stored reason is "broker rejected handshake (server_error=…): <bounded>".
        // The bounded broker-text slice it embeds must be ≤ MAX_BROKER_STR bytes
        // at a valid char boundary — the fixed prefix is the only extra length.
        let prefix = "broker rejected handshake (server_error=AuthenticationError): ";
        let embedded = reason
            .strip_prefix(prefix)
            .expect("reason must carry the fixed envelope prefix");
        assert!(
            embedded.len() <= crate::log_fields::MAX_BROKER_STR,
            "embedded broker text must be bounded to MAX_BROKER_STR (got {} bytes)",
            embedded.len(),
        );
        assert!(
            oversized.starts_with(embedded),
            "the bounded text must be a verbatim char-boundary prefix of the broker message",
        );
        // The bound is the char-boundary back-off, so the embedded slice is
        // strictly shorter than the budget when byte 256 split a 2-byte char.
        assert!(embedded.len() <= crate::log_fields::MAX_BROKER_STR);
        assert!(oversized.is_char_boundary(embedded.len()));
    }

    /// ADR-0062 companion: a SHORT broker message still round-trips verbatim
    /// (the bound only ever fires above the budget) — pins that the
    /// truncation is a ceiling, not a fixed-width truncation.
    #[test]
    fn handshake_failure_reason_preserves_short_broker_message() {
        let mut conn = fresh_conn();
        conn.begin_handshake().expect("begin");
        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: 0,
                error: pb::ServerError::AuthenticationError as i32,
                message: "token expired".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandError");
        let reason = conn.handshake_failure_reason().expect("reason");
        assert!(
            reason.contains("token expired"),
            "short broker message must round-trip verbatim: {reason}",
        );
    }

    // ----------------------------------------------------------------
    // ADR-0048 / ADR-0049 — buggify wiring + assertion-density tests.
    // ----------------------------------------------------------------

    /// ADR-0048 baseline: a `Connection` with no buggify installed
    /// (the production default) treats every `should_fire` call as a
    /// miss, so all four labels are inert. Holds whether the
    /// `buggify` feature is on or off.
    #[test]
    fn buggify_default_is_disabled() {
        let conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        assert!(!conn.buggify().is_armed());
        assert!(
            !conn
                .buggify()
                .should_fire(crate::buggify::labels::CONNECTION_RESET_DELAY, 1.0)
        );
    }

    /// ADR-0048 wiring: `set_buggify` returns a clone of the
    /// installed helper. Engines use this to share the helper with
    /// `Backoff::install_buggify` so the four labels' fire counts
    /// accumulate against a single map.
    #[cfg(feature = "buggify")]
    #[test]
    fn buggify_install_returns_shared_handle() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let helper = conn.set_buggify(crate::Buggify::with_rng(std::sync::Arc::new(|| 0_u64)));
        assert!(helper.is_armed());
        assert!(conn.buggify().is_armed());
        // The returned clone shares the underlying counter Arc, so
        // firing on one side observes from the other.
        assert!(helper.should_fire(crate::buggify::labels::CONNECTION_RESET_DELAY, 1.0));
        assert_eq!(
            conn.buggify()
                .fire_count(crate::buggify::labels::CONNECTION_RESET_DELAY),
            1
        );
    }

    /// ADR-0048 `connection.reset.delay`: when the label fires,
    /// `last_activity` is NOT cleared by `reset()`. Without buggify
    /// (or with the label not firing) the field is `None` after
    /// reset, matching the production semantics.
    #[cfg(feature = "buggify")]
    #[test]
    fn buggify_reset_delay_preserves_last_activity() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Install a buggify that always fires.
        conn.set_buggify(crate::Buggify::with_rng(std::sync::Arc::new(|| 0_u64)));
        // Drive `last_activity` to a real value through a handshake.
        conn.begin_handshake().expect("handshake");
        let probe_now = Instant::now();
        conn.handle_bytes(probe_now, &handshake_response_bytes())
            .expect("handle handshake");
        assert!(conn.last_activity.is_some());
        let before_reset = conn.last_activity;
        conn.reset();
        // Label fired → `last_activity` survives the reset.
        assert_eq!(conn.last_activity, before_reset);
        assert!(
            conn.buggify()
                .fire_count(crate::buggify::labels::CONNECTION_RESET_DELAY)
                >= 1
        );
    }

    /// Baseline of the previous test: with buggify disabled (default),
    /// `reset()` clears `last_activity`. Confirms the choice-point
    /// branch is genuinely conditional.
    #[test]
    fn buggify_reset_without_armed_helper_clears_last_activity() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("handle handshake");
        assert!(conn.last_activity.is_some());
        conn.reset();
        assert!(conn.last_activity.is_none());
    }

    /// ADR-0048 `handle_bytes.short_read`: when the label fires AND
    /// the inbound buffer carries more than one complete frame after
    /// the first frame's decode, `handle_bytes` returns early
    /// leaving the surviving bytes in `inbound`. The next call
    /// resumes the drain.
    #[cfg(feature = "buggify")]
    #[test]
    fn buggify_short_read_breaks_decode_loop_after_one_frame() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Always-fire buggify so the label triggers on entry.
        conn.set_buggify(crate::Buggify::with_rng(std::sync::Arc::new(|| 0_u64)));
        conn.begin_handshake().expect("handshake");

        // Splice TWO frames into a single handle_bytes input:
        // the handshake `Connected` response + a `Ping` (purely a
        // keepalive ack, never errors). Pre-buggify the loop would
        // drain both in one call.
        let mut splice = bytes::BytesMut::new();
        splice.extend_from_slice(&handshake_response_bytes());
        let ping = pb::BaseCommand {
            r#type: pb::base_command::Type::Ping as i32,
            ping: Some(pb::CommandPing {}),
            ..Default::default()
        };
        let mut ping_buf = bytes::BytesMut::new();
        encode_command(&mut ping_buf, &ping).expect("encode Ping");
        splice.extend_from_slice(&ping_buf);

        conn.handle_bytes(Instant::now(), &splice)
            .expect("handle splice under short_read");

        // The handshake completed, but the buggified short read
        // means the trailing Ping is still queued in `inbound`.
        // Disarm buggify so the resume call drains everything.
        assert!(conn.is_connected());
        assert!(
            conn.buggify()
                .fire_count(crate::buggify::labels::HANDLE_BYTES_SHORT_READ)
                >= 1
        );
        conn.set_buggify(crate::Buggify::disabled());
        // Resume — empty input is enough to retrigger the decode
        // loop on the residual bytes.
        conn.handle_bytes(Instant::now(), &[]).expect("resume");
        // After the resume the inbound buffer must be empty (Pong
        // queued on outbound, residual Ping consumed).
        assert!(conn.is_connected());
    }

    /// ADR-0049 negative-space assertion at `rebuild_producers`
    /// entry: when constructed under the buggy state (manually
    /// stuffing the snapshot map while `session_epoch == 0`) the
    /// `debug_assert!` panics. Confirms the assertion is wired and
    /// can be triggered from a constructed bad state.
    #[test]
    #[should_panic(expected = "rebuild_producers entered with non-empty snapshot map")]
    #[cfg(debug_assertions)]
    fn rebuild_producers_panics_on_snapshots_with_zero_epoch() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Stuff one snapshot bucket without bumping `session_epoch`.
        // Session epoch is `0` for a freshly-constructed Connection
        // that has never been reset.
        let phantom = ProducerHandle(424_242);
        conn.in_flight_publish_snapshots.insert(phantom, Vec::new());
        // Fire the assertion. We DO NOT care about the return value;
        // the panic from `debug_assert!` is the test signal.
        let _ = conn.rebuild_producers();
    }

    /// ADR-0049 positive assertion at `rebuild_producers` entry:
    /// snapshot keys must reference producers we still own. A snapshot
    /// for an unknown handle is a memory leak (the resend never
    /// fires); the assertion forces tests / drivers to surface it.
    #[test]
    #[should_panic(expected = "rebuild_producers entered with snapshot keys not in producers map")]
    #[cfg(debug_assertions)]
    fn rebuild_producers_panics_on_orphan_snapshot_key() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Bump session_epoch via a reset() so the negative-space
        // assert above doesn't fire first.
        conn.reset();
        assert!(conn.session_epoch > 0);
        let phantom = ProducerHandle(424_242);
        conn.in_flight_publish_snapshots.insert(phantom, Vec::new());
        let _ = conn.rebuild_producers();
    }

    /// ADR-0049 positive assertion on `record_first_op_success`:
    /// the call must happen with at least one open producer or
    /// consumer. A fresh Connection with no opens is the canonical
    /// "supervisor fired first-op-success before the user opened
    /// anything" bug.
    #[test]
    #[should_panic(expected = "record_first_op_success with empty producer + consumer maps")]
    #[cfg(debug_assertions)]
    fn record_first_op_success_panics_with_no_opens() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.record_first_op_success(Instant::now());
    }

    /// ADR-0049 negative-space assertion on `record_reattach_outcome`
    /// for `TcpDropAfterReAttach`: the kind requires either
    /// `session_epoch > 0` (the live-driver path: supervisor reset
    /// happened) or a prior re-attach already recorded in the
    /// anti-thrash detector (the synthetic-test path used by the
    /// differential harness). With BOTH absent — fresh Connection
    /// that never reset and never observed a `ReAttachOk` — the
    /// drop signal would be the driver misclassifying the first
    /// connect as a re-attach.
    #[test]
    #[should_panic(expected = "TcpDropAfterReAttach recorded with session_epoch=0")]
    #[cfg(debug_assertions)]
    fn record_reattach_outcome_panics_tcp_drop_with_zero_epoch() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        // Arm the anti-thrash detector so the assertion fires. With
        // the detector disabled (the default) the assertion's
        // bypass clause #3 would mask the bug under test.
        conn.set_anti_thrash(
            Some(crate::anti_thrash::AntiThrashThreshold::recommended()),
            std::time::Duration::from_secs(30),
        );
        // session_epoch == 0 (fresh Connection), no prior
        // ReAttachOk recorded. Recording a TCP drop is illegal —
        // we've never had a re-attach in this state.
        conn.record_reattach_outcome(
            Instant::now(),
            crate::anti_thrash::ReAttachHandle::Producer(ProducerHandle(0)),
            crate::anti_thrash::ReAttachOutcomeKind::TcpDropAfterReAttach,
        );
    }
}

/// ADR-0053 — OpenTelemetry context propagation relies on message
/// properties (`traceparent`, `tracestate`) surviving the Connection's
/// send path. This test pins the property round-trip at the sans-io
/// layer.
#[cfg(test)]
mod otel_property_round_trip_tests {
    use super::*;

    fn fresh_handshaked(at: Instant) -> Connection {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(SystemTime::now),
        );
        conn.begin_handshake().expect("begin");
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode");
        conn.handle_bytes(at, &buf).expect("connected");
        while let Some(_e) = conn.poll_event() {}
        conn
    }

    fn open_ready_producer(conn: &mut Connection, at: Instant) -> ProducerHandle {
        let req = CreateProducerRequest {
            topic: "persistent://public/default/otel-props-t".to_owned(),
            ..Default::default()
        };
        // Peek BEFORE create — `create_producer` consumes the next request id
        // for its `CommandProducer`, and the ack below must correlate with it
        // (the producer-not-ready drain gate only opens on a matching
        // `ProducerSuccess`).
        let rid = RequestId(conn.peek_next_request_id_for_test());
        let handle = conn.create_producer(req);
        let ack = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: rid.0,
                producer_name: "p-0".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &ack).expect("encode");
        conn.handle_bytes(at, &buf).expect("ack");
        while let Some(_e) = conn.poll_event() {}
        let _ = conn.poll_transmit();
        handle
    }

    /// `traceparent` and `tracestate` properties on an `OutgoingMessage`
    /// survive the Connection send path and appear in the wire frame.
    #[test]
    fn otel_properties_survive_send_path() {
        let at = Instant::now();
        let mut conn = fresh_handshaked(at);
        let handle = open_ready_producer(&mut conn, at);

        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";
        let tracestate = "rojo=00f067aa0ba902b7";

        let mut metadata = pb::MessageMetadata::default();
        metadata.properties.push(pb::KeyValue {
            key: "traceparent".to_owned(),
            value: traceparent.to_owned(),
        });
        metadata.properties.push(pb::KeyValue {
            key: "tracestate".to_owned(),
            value: tracestate.to_owned(),
        });

        let msg = crate::producer::OutgoingMessage {
            payload: bytes::Bytes::from_static(b"otel"),
            metadata,
            uncompressed_size: 4,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        };
        conn.send(handle, msg, 1_700_000_000, at).expect("send");

        let wire = conn.poll_transmit();
        let frame = crate::decode_one(&mut wire.clone()).expect("decode");
        let meta = frame
            .payload
            .as_ref()
            .map(|p| &p.metadata)
            .expect("payload present");
        let tp = meta
            .properties
            .iter()
            .find(|kv| kv.key == "traceparent")
            .expect("traceparent in wire frame");
        assert_eq!(tp.value, traceparent);
        let ts = meta
            .properties
            .iter()
            .find(|kv| kv.key == "tracestate")
            .expect("tracestate in wire frame");
        assert_eq!(ts.value, tracestate);
    }

    /// ADR-0053 §D2 — a retry-letter / DLQ message carries the re-injected
    /// `traceparent` alongside the `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID`
    /// correlation stamps; all three survive the Connection send path so the
    /// republished copy is traceable while still pointing back to its source.
    #[test]
    fn otel_reinjected_traceparent_survives_with_correlation_stamps() {
        let at = Instant::now();
        let mut conn = fresh_handshaked(at);
        let handle = open_ready_producer(&mut conn, at);

        let reinjected = "00-11111111111111111111111111111111-2222222222222222-01";

        let mut metadata = pb::MessageMetadata::default();
        // Shape produced by the runtime retry/DLQ paths: re-injected trace +
        // correlation stamps (the inbound traceparent has already been replaced
        // in place by `apply_property_overrides`, so only one is present here).
        metadata.properties.push(pb::KeyValue {
            key: "traceparent".to_owned(),
            value: reinjected.to_owned(),
        });
        metadata.properties.push(pb::KeyValue {
            key: "REAL_TOPIC".to_owned(),
            value: "persistent://public/default/otel-props-t".to_owned(),
        });
        metadata.properties.push(pb::KeyValue {
            key: "ORIGINAL_MESSAGE_ID".to_owned(),
            value: "1:0:-1:-1".to_owned(),
        });

        let msg = crate::producer::OutgoingMessage {
            payload: bytes::Bytes::from_static(b"retry"),
            metadata,
            uncompressed_size: 5,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        };
        conn.send(handle, msg, 1_700_000_000, at).expect("send");

        let wire = conn.poll_transmit();
        let frame = crate::decode_one(&mut wire.clone()).expect("decode");
        let meta = frame
            .payload
            .as_ref()
            .map(|p| &p.metadata)
            .expect("payload present");
        let value_of = |key: &str| {
            meta.properties
                .iter()
                .find(|kv| kv.key == key)
                .map(|kv| kv.value.as_str())
        };
        assert_eq!(value_of("traceparent"), Some(reinjected));
        assert_eq!(
            value_of("REAL_TOPIC"),
            Some("persistent://public/default/otel-props-t")
        );
        assert_eq!(value_of("ORIGINAL_MESSAGE_ID"), Some("1:0:-1:-1"));
        assert_eq!(
            meta.properties
                .iter()
                .filter(|kv| kv.key == "traceparent")
                .count(),
            1,
            "exactly one traceparent on the republished frame"
        );
    }

    #[test]
    fn driver_retry_dequeue_preserves_unrelated_semantic_event() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let handle = ProducerHandle(7);
        conn.events.push_back(ConnectionEvent::Connected {
            protocol_version: 21,
            max_message_size: 0,
            feature_flags: pb::FeatureFlags::default(),
        });
        conn.events
            .push_back(ConnectionEvent::ProducerOpenFailedTransient {
                handle,
                code: pb::ServerError::ServiceNotReady as i32,
                message: "retry".to_owned(),
            });
        conn.driver_retries.push_back(crate::DriverRetry::Producer {
            handle,
            failed_request_id: RequestId(9),
            code: pb::ServerError::ServiceNotReady as i32,
            message: "retry".to_owned(),
        });

        assert!(matches!(
            conn.poll_driver_retry(),
            Some(crate::DriverRetry::Producer {
                handle: event_handle,
                failed_request_id: RequestId(9),
                ..
            }) if event_handle == handle
        ));
        assert!(matches!(
            conn.poll_event(),
            Some(ConnectionEvent::Connected { .. })
        ));
        assert!(conn.poll_event().is_none());
    }

    #[test]
    fn public_transient_event_dequeue_removes_private_retry_context() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let handle = ProducerHandle(7);
        conn.events
            .push_back(ConnectionEvent::ProducerOpenFailedTransient {
                handle,
                code: pb::ServerError::ServiceNotReady as i32,
                message: "retry".to_owned(),
            });
        conn.driver_retries.push_back(crate::DriverRetry::Producer {
            handle,
            failed_request_id: RequestId(9),
            code: pb::ServerError::ServiceNotReady as i32,
            message: "retry".to_owned(),
        });

        assert!(matches!(
            conn.poll_event(),
            Some(ConnectionEvent::ProducerOpenFailedTransient { .. })
        ));
        assert!(conn.poll_driver_retry().is_none());
    }

    #[test]
    fn mixed_retry_dequeue_keeps_identical_failure_pairs_in_order() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let handle = ConsumerHandle(7);
        for request_id in [RequestId(9), RequestId(10)] {
            conn.events
                .push_back(ConnectionEvent::SubscribeFailedTransient {
                    handle,
                    code: pb::ServerError::ServiceNotReady as i32,
                    message: "retry".to_owned(),
                });
            conn.driver_retries.push_back(crate::DriverRetry::Consumer {
                handle,
                failed_request_id: request_id,
                code: pb::ServerError::ServiceNotReady as i32,
                message: "retry".to_owned(),
            });
        }

        assert!(
            conn.poll_event_if(|event| matches!(
                event,
                ConnectionEvent::SubscribeFailedTransient { .. }
            ))
            .is_some()
        );
        assert!(matches!(
            conn.poll_driver_retry(),
            Some(crate::DriverRetry::Consumer {
                failed_request_id: RequestId(10),
                ..
            })
        ));
        assert!(conn.poll_event().is_none());
    }

    #[test]
    fn legacy_consumer_retry_does_not_resubscribe_closed_handle() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/closed-retry".to_owned(),
            subscription: "closed".to_owned(),
            ..Default::default()
        });
        let _ = conn.poll_transmit();
        let _ = conn.close_consumer(handle, std::time::Instant::now());
        let _ = conn.poll_transmit();

        assert_eq!(conn.retry_consumer_subscribe(handle), None);
        assert!(conn.poll_transmit().is_empty());
    }
}
