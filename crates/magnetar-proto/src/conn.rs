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
    /// Time of last outbound or inbound traffic (for keepalive).
    last_activity: Option<Instant>,
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
    /// PIP-460 (ADR-0031) scalable-topic lookup registry: in-flight
    /// `CommandScalableTopicLookup` request id → topic name. Drained when the
    /// matching `CommandScalableTopicLookupResponse` arrives.
    #[cfg(feature = "scalable-topics")]
    scalable_lookups: HashMap<RequestId, String>,
    /// PIP-460 (ADR-0031) DAG-watch sessions, keyed by client-allocated watch
    /// session id. Each tracks the current segment DAG + monotonic
    /// `update_seq`. See [`crate::dag_watch::DagWatchSession`].
    #[cfg(feature = "scalable-topics")]
    dag_watch_sessions: HashMap<u64, crate::dag_watch::DagWatchSession>,
    /// PIP-460 (ADR-0031) next client-allocated watch session id.
    #[cfg(feature = "scalable-topics")]
    next_watch_session_id: u64,
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

/// Classify a `ServerError` code as a transient producer-open / consumer-subscribe error
/// — one where the broker is asking the client to retry rather than treating the
/// attachment as permanently failed. Mirrors the retry-classification Java's
/// `ProducerImpl.handleProducerCreationError` and `ConsumerImpl.connectionFailed` apply.
///
/// Codes covered:
/// - `MetadataError` (1): metadata store is still loading; usually transient post-restart.
/// - `ServiceNotReady` (6): broker isn't done initialising the topic / bundle.
/// - `TopicNotFound` (11): topic load timed out or autocreate hasn't happened yet.
///
/// Everything else (auth failures, fenced producer, "topic already deleted", quota
/// exceeded, …) stays on the permanent-failure path so the user-facing future surfaces
/// the error instead of silently looping.
fn is_transient_open_error(code: i32) -> bool {
    matches!(
        pb::ServerError::try_from(code),
        Ok(pb::ServerError::MetadataError
            | pb::ServerError::ServiceNotReady
            | pb::ServerError::TopicNotFound)
    )
}

// reason: variant payloads (handle, watcher_id, watch_session_id) are carried for the derived
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
    },
    ProducerClose {
        handle: ProducerHandle,
    },
    ConsumerClose {
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
    /// PIP-460 (ADR-0031) scalable-topic lookup in flight.
    #[cfg(feature = "scalable-topics")]
    ScalableTopicLookup,
    /// PIP-460 (ADR-0031) DAG-watch subscribe in flight.
    #[cfg(feature = "scalable-topics")]
    DagWatch {
        watch_session_id: u64,
    },
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
            last_connected_at: None,
            last_disconnected_at: None,
            session_epoch: 0,
            wall_clock,
            anti_thrash: crate::anti_thrash::AntiThrashState::disabled(),
            #[cfg(feature = "scalable-topics")]
            scalable_lookups: HashMap::new(),
            #[cfg(feature = "scalable-topics")]
            dag_watch_sessions: HashMap::new(),
            #[cfg(feature = "scalable-topics")]
            next_watch_session_id: 1,
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

        // (2) Fail every pending request and wake its waiter.
        let pending_request_keys: Vec<PendingOpKey> = self
            .pending_requests
            .keys()
            .copied()
            .map(PendingOpKey::Request)
            .collect();
        for key in pending_request_keys {
            self.outcomes.insert(key, OpOutcome::SessionLost { key });
            if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }
        self.pending_requests.clear();

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
            consumer.available_permits = 0;
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
                if state.closed {
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
            let subscribe_request_id = self.emit_command_subscribe(handle, &req, resume);
            // The initial flow is DEFERRED to the broker's subscribe ack (the
            // `Success` arm reads this flag): Pulsar silently drops
            // `CommandFlow` for a consumer id whose subscribe is still being
            // processed — post-restart cursor recovery makes that window
            // seconds long, and flow-alongside-subscribe starved the
            // re-attached consumer with zero broker-side permits. Java
            // `ConsumerImpl#reconnectLater` ordering (ARCHITECTURE.md
            // §Supervised reconnect step 6).
            if let Some(slot) = self.consumers.get(&handle) {
                slot.state.lock().flow_on_subscribe_ack = true;
            }
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
        // don't need to reset it here. We only need to drain the stale
        // close-by-broker events so the runtime's wait future doesn't trip
        // on them.
        let _ = self.consumers.get(&handle)?;
        self.events.retain(
            |ev| !matches!(ev, ConnectionEvent::ConsumerClosedByBroker { handle: h, .. } if *h == handle),
        );
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
        let request_id = self.emit_command_subscribe(handle, &req, None);
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
            feature_flags: Some(self.config.feature_flags),
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
        self.last_activity = Some(now);
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
        self.last_activity = Some(now);
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

            // PIP-460 (ADR-0031): the scalable-topic commands (`BaseCommand`
            // types 80-85) are hand-encoded and NOT present in the generated
            // `pb::BaseCommand`, so `decode_one` → `Type::try_from` would
            // reject them as `UnsupportedCommand`. Intercept them here: the
            // command region decodes as a `ScalableBaseCommand` (which
            // captures the shared field-1 `type` tag plus the additive 80-85
            // fields, skipping every v4 field it doesn't know). A v4 frame
            // decoded this way carries a non-scalable `type`, so we fall
            // through to the normal path untouched.
            #[cfg(feature = "scalable-topics")]
            {
                if let Some(scmd) = Self::try_decode_scalable_command(&frame_bytes) {
                    self.handle_scalable_frame(now, scmd)?;
                    continue;
                }
            }

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
                // Nothing to do — last_activity already updated above.
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
                            if let Some(tuple) = producer.apply_receipt(&synth) {
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
                let staged_events: Vec<ConnectionEvent> =
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
                        events
                    } else {
                        Vec::new()
                    };
                for ev in staged_events {
                    self.events.push_back(ev);
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
                    let snapshots = self.in_flight_publish_snapshots.remove(&handle);
                    if let Some(slot) = self.producers.get(&handle) {
                        let mut producer = slot.state.lock();
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
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
                            pending = pending_before,
                            replayed_snapshots = snapshot_count,
                            "producer re-attach acked; replay staged and drain gate opened"
                        );
                    }
                    self.outcomes.insert(
                        PendingOpKey::Request(request_id),
                        OpOutcome::Success { request_id },
                    );
                    self.wake_for_request(request_id);
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
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::Success { request_id },
                );
                self.wake_for_request(request_id);
                if let Some(PendingRequestKind::ConsumerSubscribe { handle }) = kind {
                    self.events
                        .push_back(ConnectionEvent::SubscribeAcked { handle });
                    // ADR-0028 anti-thrash: feed the successful subscribe ack into
                    // the detector. No-op when the detector is disabled (default).
                    self.record_reattach_outcome(
                        now,
                        crate::anti_thrash::ReAttachHandle::Consumer(handle),
                        crate::anti_thrash::ReAttachOutcomeKind::ReAttachOk,
                    );
                    // Re-attach flow gate (rebuild / transient-retry paths):
                    // the broker has acked the re-subscribe — NOW the initial
                    // flow lands on a registered consumer id instead of being
                    // silently dropped mid-subscribe.
                    let flow_now = self.consumers.get(&handle).is_some_and(|slot| {
                        std::mem::take(&mut slot.state.lock().flow_on_subscribe_ack)
                    });
                    if flow_now {
                        let _ = self.initial_flow(handle);
                        tracing::debug!(
                            target: "magnetar_proto::conn",
                            handle = ?handle,
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
                    let reason = format!(
                        "broker rejected handshake (server_error={server_error_name}): {}",
                        err.message
                    );
                    tracing::warn!(
                        target: "magnetar_proto::conn",
                        state = ?self.state,
                        server_error = %server_error_name,
                        message = %err.message,
                        "captured CommandError during handshake — surfacing as handshake_failure_reason",
                    );
                    self.handshake_failure_reason = Some(reason);
                }
                let request_id = RequestId(err.request_id);
                let kind = self.pending_requests.remove(&request_id);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::Error {
                        request_id,
                        code: err.error,
                        message: err.message.clone(),
                    },
                );
                self.wake_for_request(request_id);
                // When the failing request id correlates with a pending producer-open /
                // consumer-subscribe, surface a typed failure event so event-stream waiters
                // (`EventWaitFut::ProducerReady` / `EventWaitFut::SubscribeAcked` in the
                // tokio engine, and the moonpool engine's equivalent) observe the rejection
                // instead of hanging forever. The success-side paths (`ProducerSuccess`,
                // `Success`) already push `ProducerReady` / `SubscribeAcked`; we mirror that
                // shape for failures, and drop the matching state so the dead handle is not
                // re-emitted on reconnect.
                match kind {
                    Some(PendingRequestKind::ProducerOpen { handle }) => {
                        // Pulsar wire convention: `ServiceNotReady` (6),
                        // `MetadataError` (1), `TopicNotFound` (11) are the broker's
                        // transient post-restart codes — the namespace bundle hasn't
                        // been re-acquired by this broker yet, the metadata store is
                        // still loading, or the topic load timed out. Java's
                        // `ProducerImpl.handleProducerCreationError` re-runs lookup
                        // with backoff in those cases and only fails permanently on
                        // authentication / fenced / "topic already deleted" errors.
                        // Without this classification, magnetar removed the producer
                        // state on every transient post-`docker restart` rebuild and
                        // left every subsequent `producer.send()` hanging on a
                        // "unknown producer handle".
                        if is_transient_open_error(err.error) {
                            // The attachment failed — close the drain gate so no
                            // staged send reaches the wire before the retry's
                            // `ProducerSuccess` (the broker closes the whole
                            // connection on a send to a not-ready producer).
                            if let Some(slot) = self.producers.get(&handle) {
                                slot.state.lock().broker_ready = false;
                            }
                            self.events
                                .push_back(ConnectionEvent::ProducerOpenFailedTransient {
                                    handle,
                                    code: err.error,
                                    message: err.message.clone(),
                                });
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
                    Some(PendingRequestKind::ConsumerSubscribe { handle }) => {
                        if is_transient_open_error(err.error) {
                            self.events
                                .push_back(ConnectionEvent::SubscribeFailedTransient {
                                    handle,
                                    code: err.error,
                                    message: err.message.clone(),
                                });
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
                        if let Some(PendingRequestKind::Ack { handle }) = kind {
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
                    let chain_origin = req.chain_origin;
                    // The wire-level request-id is always done at this point
                    // — the broker won't send another response for it. The
                    // *chain*'s pending_requests entry stays keyed on
                    // `chain_origin` until the terminal outcome lands; only
                    // the per-hop wire-id is dropped from pending_requests
                    // when it differs (the initial hop already shares the
                    // anchor's id).
                    if rid != chain_origin {
                        self.pending_requests.remove(&rid);
                    }
                    let (outcome, retry) = crate::lookup::translate_lookup_response(&resp, &req);
                    match retry {
                        Some(retry) => {
                            // ADR-0054 §5 single-owner rule: proto owns the
                            // redirect-chase hop log at the point of
                            // detection; the engines drain the companion
                            // `LookupResponse(Redirected)` event silently.
                            // Broker-advertised URLs are truncated per §3.
                            if let LookupOutcome::Redirected {
                                broker_service_url,
                                broker_service_url_tls,
                            } = &outcome
                            {
                                tracing::debug!(
                                    target: "magnetar_proto::conn",
                                    topic = %retry.topic,
                                    hop = crate::lookup::MAX_LOOKUP_REDIRECTS
                                        - retry.hops_remaining,
                                    hops_remaining = retry.hops_remaining,
                                    broker_service_url = broker_service_url
                                        .as_deref()
                                        .map_or("", crate::log_fields::truncate_broker_str),
                                    broker_service_url_tls = broker_service_url_tls
                                        .as_deref()
                                        .map_or("", crate::log_fields::truncate_broker_str),
                                    "lookup redirected; chasing internally",
                                );
                            }
                            // HIGH-4 (lookup multi-agent review): the
                            // intermediate `Redirected` outcome is
                            // diagnostic-only. We push it to the
                            // `ConnectionEvent` queue for tracing /
                            // observability (engines that drain the event
                            // stream can log every redirect hop) but
                            // **never** publish it to the `outcomes` slot
                            // and **never** wake the user-facing future.
                            // Only `Connect` / `Failed` reach the user.
                            self.events.push_back(ConnectionEvent::LookupResponse {
                                request_id: chain_origin,
                                result: outcome,
                            });
                            // Issue the retry frame on a fresh wire-level
                            // request-id. The retry's `chain_origin` field
                            // (set by `translate_lookup_response`) keeps
                            // the user-facing anchor stable.
                            let new_id = self.alloc_request_id();
                            if let Err(LookupSubmitError::Rejected) =
                                self.send_lookup_internal(new_id, retry)
                            {
                                // Cap-hit on retry — the frame never goes
                                // out. Deliver a synthetic Failed against
                                // the chain anchor so the user's future
                                // terminates cleanly instead of waiting
                                // on a hop that will never happen.
                                self.synthesize_lookup_failed(
                                    chain_origin,
                                    "lookup retry rejected: max pending \
                                     (ConnectionConfig::max_pending_lookups)",
                                );
                            }
                            // `LookupSubmitError::Encode` is the historic
                            // silent-drop path — the registry slot is
                            // reserved, the future stays parked until the
                            // operation timeout fires. Behaviour matches
                            // the pre-HIGH-4 fold path.
                        }
                        None => {
                            // Terminal outcome (`Connect` or `Failed`). The
                            // anchor's pending_requests entry is consumed
                            // and the user-facing future is woken with the
                            // final answer. This is the only path that
                            // ever publishes a `LookupResponse` outcome.
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
                self.events
                    .push_back(ConnectionEvent::ConsumerClosedByBroker {
                        handle,
                        assigned_broker_service_url: close.assigned_broker_service_url,
                    });
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
                self.events
                    .push_back(ConnectionEvent::ActiveConsumerChanged {
                        handle,
                        active: acc.is_active.unwrap_or(false),
                    });
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
        self.events.pop_front()
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
        self.events.remove(idx)
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
            let ping = pb::BaseCommand {
                r#type: pb::base_command::Type::Ping as i32,
                ping: Some(pb::CommandPing {}),
                ..Default::default()
            };
            let _ = self.encode_command(&ping);
            self.last_activity = Some(now);
        }

        // Tracker-driven redeliveries — both negative-ack delay and unacked-message timeout
        // produce the same CommandRedeliverUnacknowledgedMessages payload, so we collect
        // then emit through the shared helper.
        let mut redeliveries: Vec<(ConsumerHandle, Vec<MessageId>)> = Vec::new();
        let mut ack_actions: Vec<crate::trackers::AckAction> = Vec::new();
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
        }
        for (handle, ids) in redeliveries {
            self.emit_redeliver_unacked(handle, ids);
        }
        // Flush the ack-grouping tracker. The actions go through the shared dispatcher
        // which allocates a `RequestId` per coalesced `CommandAck`; the response is
        // routed back through the existing pending-requests slot, but no user future is
        // tied to it (ack_grouped_* is fire-and-forget).
        if !ack_actions.is_empty() {
            self.dispatch_ack_actions(ack_actions);
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
        }
        for (handle, seq, waker) in send_timeouts {
            let key = PendingOpKey::Send(handle, seq);
            // Pulsar's ServerError enum has no TimeoutError; use the same `-1` sentinel
            // Java surfaces as TimeoutException with a descriptive message so callers can
            // pattern-match on the error string.
            self.outcomes.insert(
                key,
                OpOutcome::SendError {
                    sequence_id: seq,
                    code: -1,
                    message: "send timeout".to_owned(),
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
                code: -1,
                message: "send timeout".to_owned(),
            });
        }
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
        request_id
    }

    /// Open a consumer. Returns the handle and emits `CommandSubscribe`. The driver receives
    /// [`ConnectionEvent::SubscribeAcked`] on success and should then call
    /// [`Self::initial_flow`] to feed the broker an initial flow.
    pub fn subscribe(&mut self, req: SubscribeRequest) -> ConsumerHandle {
        let handle = ConsumerHandle(self.next_consumer_id);
        self.next_consumer_id = self.next_consumer_id.wrapping_add(1);
        let mut state = ConsumerState::new(
            handle,
            req.topic.clone(),
            req.subscription.clone(),
            req.receiver_queue_size,
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

        let _ = self.emit_command_subscribe(handle, &req, req.start_message_id);
        handle
    }

    /// Emit a `CommandSubscribe` carrying `req`'s parameters for the consumer identified by
    /// `handle`. `resume_from` overrides `req.start_message_id` — used by
    /// [`Self::rebuild_consumers`] to point the broker at the post-ack position after a
    /// reconnect.
    fn emit_command_subscribe(
        &mut self,
        handle: ConsumerHandle,
        req: &SubscribeRequest,
        resume_from: Option<MessageId>,
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
        request_id
    }

    /// Emit the initial flow command for a consumer once it's been acked.
    pub fn initial_flow(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
        let flow_cmd = self.consumers.get(&handle)?.state.lock().initial_flow();
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

    /// Take a rolling-window stats snapshot on the consumer identified by `handle`. Runtime
    /// engines wire this to a `tokio::time::interval` ticker. Mirrors Java
    /// `ConsumerStatsRecorder`'s rolling-window rate calculation. No-op if the handle is
    /// unknown.
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
    #[must_use]
    pub fn consumer_available_permits(&self, handle: ConsumerHandle) -> u32 {
        self.consumers
            .get(&handle)
            .map_or(0, |slot| slot.state.lock().available_permits)
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
    pub fn ack(&mut self, handle: ConsumerHandle, ack: AckRequest) -> RequestId {
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
                // so any per-batch tracker entries the cumulative position covers are stale.
                // Drop them so future individual acks on the same batch don't synthesise a
                // partial bitset for state the broker has already moved past.
                if let Some(slot) = self.consumers.get(&handle) {
                    let mut consumer = slot.state.lock();
                    let covered: Vec<(u64, u64)> = ack
                        .message_ids
                        .iter()
                        .map(|id| (id.ledger_id, id.entry_id))
                        .collect();
                    for key in covered {
                        consumer.batch_ack_tracker.remove(&key);
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
        self.pending_requests
            .insert(request_id, PendingRequestKind::Ack { handle });
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
            self.dispatch_ack_actions(actions);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Individual,
                    properties: Vec::new(),
                    txn_id: None,
                },
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
            self.dispatch_ack_actions(actions);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Cumulative,
                    properties: Vec::new(),
                    txn_id: None,
                },
            );
        }
    }

    fn dispatch_ack_actions(&mut self, actions: Vec<crate::trackers::AckAction>) {
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

    /// Issue a topic lookup. The state machine handles redirects internally;
    /// the user receives **only the terminal outcome** — either
    /// `LookupOutcome::Connect` or `LookupOutcome::Failed`.
    ///
    /// HIGH-4 (lookup multi-agent review): intermediate
    /// `LookupOutcome::Redirected` outcomes are surfaced via the
    /// [`crate::event::ConnectionEvent::LookupResponse`] events queue for
    /// observability/tracing only — they **never** publish to the outcomes
    /// slot and **never** wake the user-facing future. This is what makes
    /// the redirect cap and the broker-URL passthrough end-to-end
    /// user-observable.
    ///
    /// Redirects are capped at [`crate::lookup::MAX_LOOKUP_REDIRECTS`] hops
    /// (Java parity). If [`ConnectionConfig::max_pending_lookups`] is set
    /// and the in-flight registry is already at the cap, the call surfaces
    /// synchronously as a synthetic `LookupOutcome::Failed { code: 0,
    /// message: "lookup rejected: max pending" }` against the freshly
    /// allocated request-id — the frame never touches the wire.
    pub fn lookup(&mut self, topic: &str, authoritative: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let req = LookupRequest {
            topic: topic.to_owned(),
            authoritative,
            hops_remaining: crate::lookup::MAX_LOOKUP_REDIRECTS,
            // The initial request-id IS the chain origin — every retry on
            // this lookup chain delivers its terminal outcome here.
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

    /// Close a producer.
    pub fn close_producer(&mut self, handle: ProducerHandle) -> RequestId {
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
        self.pending_requests
            .insert(request_id, PendingRequestKind::ProducerClose { handle });
        request_id
    }

    /// Close a consumer.
    pub fn close_consumer(&mut self, handle: ConsumerHandle) -> RequestId {
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
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerClose { handle });
        request_id
    }

    /// Unsubscribe — remove this consumer's subscription from the broker.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#unsubscribe`. Unlike
    /// [`close_consumer`](Self::close_consumer) which keeps the subscription
    /// cursor alive on the broker, `unsubscribe` deletes the subscription
    /// entirely — useful for tear-down + cleanup. The runtime should call
    /// `close_consumer` afterwards.
    ///
    /// `force=true` (PIP-313) drops the subscription even if other consumers
    /// are still attached.
    pub fn unsubscribe(&mut self, handle: ConsumerHandle, force: bool) -> RequestId {
        let request_id = self.alloc_request_id();
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

    /// Drain a single message from the given consumer's queue.
    pub fn pop_message(&mut self, handle: ConsumerHandle) -> Option<IncomingMessage> {
        let (msg, flow_cmd) = {
            let slot = self.consumers.get(&handle)?;
            let mut consumer = slot.state.lock();
            let msg = consumer.pop_message();
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
    // PIP-460 scalable topics (ADR-0031). Hand-encoded wire commands ride
    // the existing connection via `pb::scalable_topics::encode`; inbound
    // responses are intercepted in `handle_bytes_decode_loop` and routed
    // through `handle_scalable_frame`.
    // -------------------------------------------------------------------

    /// **Experimental** (PIP-460, ADR-0031). Issue a `CommandScalableTopicLookup`
    /// for `topic`. Returns the request id the caller correlates with the
    /// resulting [`ConnectionEvent::ScalableTopicLookupResolved`].
    #[cfg(feature = "scalable-topics")]
    pub fn send_scalable_topic_lookup(&mut self, topic: &str, authoritative: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::scalable_topics::CommandScalableTopicLookup {
            topic: topic.to_owned(),
            request_id: request_id.0,
            authoritative: Some(authoritative),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
        };
        let env = pb::scalable_topics::ScalableBaseCommand::lookup(cmd);
        let _ = pb::scalable_topics::encode(&mut self.outbound, &env);
        self.scalable_lookups.insert(request_id, topic.to_owned());
        self.pending_requests
            .insert(request_id, PendingRequestKind::ScalableTopicLookup);
        request_id
    }

    /// **Experimental** (PIP-460, ADR-0031). Open a DAG-watch session for
    /// `topic`, seeded with the lookup's `segments` snapshot and `lookup_token`.
    /// Allocates and returns a client-side watch session id; the caller MUST
    /// have an open connection to the controller broker the lookup returned.
    /// Emits the `CommandSegmentDagWatch` subscribe frame.
    #[cfg(feature = "scalable-topics")]
    pub fn open_dag_watch(
        &mut self,
        topic: &str,
        lookup_token: u64,
        segments: Vec<crate::types::SegmentDescriptor>,
    ) -> u64 {
        let watch_session_id = self.next_watch_session_id;
        self.next_watch_session_id = self.next_watch_session_id.wrapping_add(1);
        let request_id = self.alloc_request_id();
        let cmd = pb::scalable_topics::CommandSegmentDagWatch {
            topic: topic.to_owned(),
            request_id: request_id.0,
            watch_session_id,
            lookup_token,
        };
        let env = pb::scalable_topics::ScalableBaseCommand::dag_watch(cmd);
        let _ = pb::scalable_topics::encode(&mut self.outbound, &env);
        self.dag_watch_sessions.insert(
            watch_session_id,
            crate::dag_watch::DagWatchSession::new(watch_session_id, lookup_token, segments),
        );
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::DagWatch { watch_session_id },
        );
        watch_session_id
    }

    /// **Experimental** (PIP-460, ADR-0031). Close a DAG-watch session,
    /// emitting `CommandCloseSegmentDagWatch` and dropping the session state.
    #[cfg(feature = "scalable-topics")]
    pub fn close_dag_watch(&mut self, watch_session_id: u64) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::scalable_topics::CommandCloseSegmentDagWatch {
            watch_session_id,
            request_id: request_id.0,
        };
        let env = pb::scalable_topics::ScalableBaseCommand::close_dag_watch(cmd);
        let _ = pb::scalable_topics::encode(&mut self.outbound, &env);
        self.dag_watch_sessions.remove(&watch_session_id);
        self.events.push_back(ConnectionEvent::DagWatchClosed {
            watch_session_id,
            reason: Some("client-initiated close".to_owned()),
        });
        request_id
    }

    /// Snapshot the current DAG for a watch session (for the CLI `topic-info`
    /// and tests). `None` if no session with that id is open.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn dag_snapshot(
        &self,
        watch_session_id: u64,
    ) -> Option<Vec<crate::types::SegmentDescriptor>> {
        self.dag_watch_sessions
            .get(&watch_session_id)
            .map(crate::dag_watch::DagWatchSession::snapshot)
    }

    /// Try to decode the command region of a complete frame as a
    /// [`pb::scalable_topics::ScalableBaseCommand`] and return it only when
    /// the `type` discriminator is one of the PIP-460 commands (80-85). A v4
    /// frame decodes with a non-scalable `type`, so we return `None` and let
    /// the normal `decode_one` path handle it.
    #[cfg(feature = "scalable-topics")]
    fn try_decode_scalable_command(
        frame_bytes: &Bytes,
    ) -> Option<pb::scalable_topics::ScalableBaseCommand> {
        use pb::scalable_topics::base_command_type as sct;
        // Frame layout: [total_size u32][cmd_size u32][cmd bytes...]. The
        // command region begins at offset 8.
        if frame_bytes.len() < 8 {
            return None;
        }
        let cmd_size = u32::from_be_bytes([
            frame_bytes[4],
            frame_bytes[5],
            frame_bytes[6],
            frame_bytes[7],
        ]) as usize;
        let cmd_end = 8usize.checked_add(cmd_size)?;
        if frame_bytes.len() < cmd_end {
            return None;
        }
        let cmd_region = &frame_bytes[8..cmd_end];
        let scmd = <pb::scalable_topics::ScalableBaseCommand as prost::Message>::decode(cmd_region)
            .ok()?;
        match scmd.r#type {
            sct::SCALABLE_TOPIC_LOOKUP
            | sct::SCALABLE_TOPIC_LOOKUP_RESPONSE
            | sct::SEGMENT_DAG_WATCH
            | sct::SEGMENT_DAG_WATCH_RESPONSE
            | sct::SEGMENT_DAG_UPDATE
            | sct::CLOSE_SEGMENT_DAG_WATCH => Some(scmd),
            _ => None,
        }
    }

    /// Dispatch one decoded PIP-460 command frame. Mirrors the per-type arms
    /// of [`Self::handle_frame`] for the scalable command family. Only the
    /// broker→client commands carry handling here; the client never receives
    /// its own outbound lookup / subscribe / close.
    #[cfg(feature = "scalable-topics")]
    fn handle_scalable_frame(
        &mut self,
        _now: Instant,
        scmd: pb::scalable_topics::ScalableBaseCommand,
    ) -> Result<(), ProtocolError> {
        use pb::scalable_topics::scalable_lookup_response::LookupType;

        if let Some(resp) = scmd.scalable_topic_lookup_response {
            let request_id = RequestId(resp.request_id);
            self.scalable_lookups.remove(&request_id);
            self.pending_requests.remove(&request_id);
            match LookupType::from_i32(resp.response) {
                LookupType::Connect => {
                    let segments = resp
                        .segments
                        .iter()
                        .map(crate::types::SegmentDescriptor::from_pb)
                        .collect();
                    self.events
                        .push_back(ConnectionEvent::ScalableTopicLookupResolved {
                            request_id,
                            controller_broker_url: resp
                                .controller_broker_url
                                .clone()
                                .unwrap_or_default(),
                            segments,
                            lookup_token: resp.lookup_token.unwrap_or(0),
                        });
                }
                // Redirect / failure surface as a closed lookup with a
                // reason; the runtime re-resolves. (Controller-election-aware
                // redirect handling is future work per ADR-0031.)
                LookupType::Redirect | LookupType::Failed => {
                    self.events.push_back(ConnectionEvent::DagWatchClosed {
                        watch_session_id: 0,
                        reason: Some(resp.message.clone().unwrap_or_else(|| {
                            "scalable-topic lookup failed or redirected".to_owned()
                        })),
                    });
                }
            }
            return Ok(());
        }

        if let Some(resp) = scmd.segment_dag_watch_response {
            let request_id = RequestId(resp.request_id);
            self.pending_requests.remove(&request_id);
            if let Some(err) = resp.error {
                // Subscribe rejected — drop the session and surface a close.
                self.dag_watch_sessions.remove(&resp.watch_session_id);
                self.events.push_back(ConnectionEvent::DagWatchClosed {
                    watch_session_id: resp.watch_session_id,
                    reason: Some(format!(
                        "dag-watch subscribe rejected (code {err}): {}",
                        resp.message.unwrap_or_default()
                    )),
                });
            }
            // Success: the session is already installed by `open_dag_watch`.
            return Ok(());
        }

        if let Some(upd) = scmd.segment_dag_update {
            let watch_session_id = upd.watch_session_id;
            let Some(session) = self.dag_watch_sessions.get_mut(&watch_session_id) else {
                // Update for an unknown session — drop silently (stale frame
                // after a close, mirroring the lookup-registry one-shot guard).
                return Ok(());
            };
            match session.handle_update(&upd) {
                Ok(delta) => {
                    let consume_affecting = delta.is_consume_affecting();
                    let reason = delta.change_reason();
                    self.events.push_back(ConnectionEvent::SegmentDagUpdated {
                        watch_session_id,
                        delta,
                    });
                    if consume_affecting {
                        self.events
                            .push_back(ConnectionEvent::DagChangedDuringConsume {
                                watch_session_id,
                                reason,
                            });
                    }
                }
                Err(err) => {
                    // A malformed / non-monotonic update closes the session
                    // (drop-on-change). The runtime re-resolves.
                    self.dag_watch_sessions.remove(&watch_session_id);
                    self.events.push_back(ConnectionEvent::DagWatchClosed {
                        watch_session_id,
                        reason: Some(format!("dag update rejected: {err}")),
                    });
                }
            }
            return Ok(());
        }

        // Lookup / subscribe / close are client→broker only; receiving one is
        // a protocol-shape surprise but not fatal. Ignore for forward-compat.
        Ok(())
    }

    /// Re-emit `CommandProducer` for a SINGLE producer handle. Used by the supervised
    /// driver loop to retry a producer-open that the broker rejected with a transient
    /// error (`ServiceNotReady`, `MetadataError`, `TopicNotFound` — see #71). The full
    /// [`Self::rebuild_producers`] sweep would re-emit `CommandProducer` for every still-
    /// open producer; this targeted variant is cheaper and avoids stepping on producers
    /// that are already successfully reattached on this session. Bumps `epoch` so the
    /// broker associates the new attachment with a strictly newer generation. Returns
    /// the request id that the user can correlate with the next response, or `None` when
    /// the producer was closed / removed between the broker error and this retry.
    pub fn retry_producer_open(&mut self, handle: ProducerHandle) -> Option<RequestId> {
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

    /// Companion to [`Self::retry_producer_open`] for consumers. Re-emits the
    /// `CommandSubscribe` + initial `CommandFlow` for a single consumer handle, used
    /// when the broker rejected a previous `CommandSubscribe` with a transient code
    /// (`NamespaceBundleNotServed`, `ServiceNotReady`, …). The full
    /// [`Self::rebuild_consumers`] sweep is too coarse: it would re-emit every
    /// still-open consumer's `CommandSubscribe`, which would double-attach the ones
    /// that already succeeded on this session.
    pub fn retry_consumer_subscribe(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
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
        let request_id = self.emit_command_subscribe(handle, &req, resume_from);
        // Flow deferred to the subscribe ack — same gate as
        // `rebuild_consumers` (the broker drops pre-ack flow silently).
        if let Some(slot) = self.consumers.get(&handle) {
            slot.state.lock().flow_on_subscribe_ack = true;
        }
        Some(request_id)
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

    /// Ported from Java `BinaryProtoLookupService` — a `CommandLookupTopicResponse` whose
    /// `response = Redirect` must trigger a *fresh* outbound `CommandLookupTopic` with a
    /// fresh request id. Verifies that the state machine itself drives the retry (no need
    /// for the user to re-submit). HIGH-4 (lookup multi-agent review): the intermediate
    /// `Redirected` outcome must NOT publish to the outcomes slot — only the terminal
    /// outcome at the end of the chain is delivered to the user-facing future. The
    /// intermediate `Redirected` is pushed to the events queue for diagnostics only.
    #[test]
    fn lookup_redirect_response_triggers_authoritative_retry() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();

        // Issue the lookup and capture the outbound size to detect the second emission.
        let request_id = conn.lookup("persistent://public/default/foo", false);
        let outbound_after_lookup = conn.outbound_len();
        assert!(
            outbound_after_lookup > 0,
            "lookup must enqueue a CommandLookupTopic"
        );

        // Feed a Redirect response. The state machine must emit a *second* lookup frame
        // with a different request id and the `authoritative` flag forced on.
        let redirect = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: Some("pulsar://other:6650".to_owned()),
                broker_service_url_tls: None,
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

        // The state machine should have emitted a follow-up lookup. Detect it by checking
        // that the outbound buffer grew.
        assert!(
            conn.outbound_len() > outbound_after_lookup,
            "redirect must trigger a retry CommandLookupTopic (outbound={} -> {})",
            outbound_after_lookup,
            conn.outbound_len()
        );

        // HIGH-4: the intermediate `Redirected` must NOT publish to the outcomes slot —
        // only the terminal outcome (Connect / Failed) at the end of the chain does. The
        // chain anchor's pending_requests / outcomes / waker are still parked; the
        // user-facing future is correctly NOT woken on the first hop.
        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "intermediate Redirected must not publish to outcomes (HIGH-4)"
        );

        // The intermediate Redirected IS pushed to the events queue for diagnostics —
        // tracing / observability code that drains the event stream sees every hop.
        let mut saw_redirected = false;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::LookupResponse {
                request_id: rid,
                result: crate::event::LookupOutcome::Redirected { .. },
            } = ev
            {
                assert_eq!(
                    rid, request_id,
                    "diagnostic Redirected event must be keyed on the user-facing anchor"
                );
                saw_redirected = true;
            }
        }
        assert!(
            saw_redirected,
            "expected a diagnostic LookupResponse(Redirected) event on the chain anchor"
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

    /// HIGH-4 (lookup multi-agent review): a redirect chain that
    /// terminates in `Connect` must deliver the **terminal** outcome
    /// against the user-facing request-id (the `chain_origin`), not the
    /// intermediate `Redirected` outcome from the first hop. This is the
    /// behaviour that makes the broker-URL passthrough and the redirect
    /// cap end-to-end user-observable.
    #[test]
    fn lookup_redirect_chain_delivers_terminal_connect_to_origin() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();

        // Issue the user-facing lookup. The returned id is the
        // `chain_origin` — the only id the user's future will ever wake
        // against.
        let user_request_id = conn.lookup("persistent://public/default/foo", false);
        let initial_ids = drain_outbound_lookup_ids(&mut conn);
        assert_eq!(
            initial_ids,
            vec![user_request_id],
            "initial lookup must be keyed on the user-facing request-id"
        );

        // Walk two redirects, then terminate in Connect on the THIRD wire id.
        let mut current_wire_id = user_request_id;
        for hop in 0..2 {
            let redirect = pb::BaseCommand {
                r#type: pb::base_command::Type::LookupResponse as i32,
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some(format!("pulsar://hop-{hop}:6650")),
                    broker_service_url_tls: None,
                    response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                    request_id: current_wire_id.0,
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

            // Each redirect must NOT publish a terminal outcome to the
            // user-facing slot — the chain anchor must stay parked.
            assert!(
                conn.take_outcome(PendingOpKey::Request(user_request_id))
                    .is_none(),
                "hop {hop}: intermediate Redirected must not wake the user"
            );

            // The state machine must have emitted a retry frame with a NEW
            // wire request-id. Capture it for the next hop's correlator.
            let next_ids = drain_outbound_lookup_ids(&mut conn);
            assert_eq!(
                next_ids.len(),
                1,
                "hop {hop}: exactly one retry frame must be emitted"
            );
            assert_ne!(
                next_ids[0], current_wire_id,
                "hop {hop}: retry must allocate a fresh wire request-id"
            );
            assert_ne!(
                next_ids[0], user_request_id,
                "hop {hop}: retry id must differ from the chain anchor too"
            );
            current_wire_id = next_ids[0];
        }

        // Drain the diagnostic Redirected events so the queue is clean for
        // the terminal assertion below.
        while conn
            .poll_event_if(|e| matches!(e, ConnectionEvent::LookupResponse { .. }))
            .is_some()
        {}

        // Terminate the chain in Connect on the most recent wire id.
        let terminal = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: Some("pulsar://terminal:6650".to_owned()),
                broker_service_url_tls: None,
                response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
                request_id: current_wire_id.0,
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

        // The user-facing future receives the Connect outcome with the
        // terminal broker URL — NOT the first-hop redirect URL.
        match conn.take_outcome(PendingOpKey::Request(user_request_id)) {
            Some(OpOutcome::LookupResponse {
                request_id,
                outcome:
                    crate::event::LookupOutcome::Connect {
                        broker_service_url, ..
                    },
            }) => {
                assert_eq!(request_id, user_request_id);
                assert_eq!(
                    broker_service_url.as_deref(),
                    Some("pulsar://terminal:6650"),
                    "user must see the TERMINAL broker URL, not the first-hop redirect"
                );
            }
            other => panic!("expected terminal Connect outcome at the anchor, got {other:?}"),
        }
    }

    /// HIGH-4 + HIGH-2: a hostile broker that drives MAX_LOOKUP_REDIRECTS
    /// hops must surface a synthetic `Failed { code: 0, message: "lookup
    /// redirect cap exceeded …" }` to the user-facing future. Without
    /// the HIGH-4 fix the user would see the FIRST hop's Redirected
    /// outcome instead and never observe the cap.
    #[test]
    fn lookup_redirect_chain_cap_exceeded_surfaces_failed_to_origin() {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake");
        let _ = conn.poll_event();

        let user_request_id = conn.lookup("persistent://public/default/foo", false);
        let initial_ids = drain_outbound_lookup_ids(&mut conn);
        let mut current_wire_id = initial_ids[0];

        // Feed `MAX_LOOKUP_REDIRECTS + 1` redirects. The (cap+1)-th one
        // triggers the cap. The proto-level test
        // `redirect_chain_terminates_at_cap` already pins the cap
        // behaviour at the translate layer; here we confirm the
        // *connection* layer surfaces it against the user's anchor.
        for hop in 0..=crate::lookup::MAX_LOOKUP_REDIRECTS {
            let redirect = pb::BaseCommand {
                r#type: pb::base_command::Type::LookupResponse as i32,
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    broker_service_url: Some(format!("pulsar://hop-{hop}:6650")),
                    broker_service_url_tls: None,
                    response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                    request_id: current_wire_id.0,
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
            if let Some(next) = drain_outbound_lookup_ids(&mut conn).into_iter().next() {
                current_wire_id = next;
            }
        }

        // User-facing outcome: Failed with the cap diagnostic.
        match conn.take_outcome(PendingOpKey::Request(user_request_id)) {
            Some(OpOutcome::LookupResponse {
                request_id,
                outcome: crate::event::LookupOutcome::Failed { code, message },
            }) => {
                assert_eq!(request_id, user_request_id);
                assert_eq!(code, 0);
                assert!(
                    message.contains("redirect cap exceeded"),
                    "expected cap diagnostic, got: {message}"
                );
            }
            other => panic!("expected cap-exceeded Failed at the anchor, got {other:?}"),
        }
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
    /// `ServiceNotReady`/"Please redo the lookup". `ServiceNotReady` is the broker's
    /// transient post-restart code, so the connection MUST keep the producer state and
    /// emit `ProducerOpenFailedTransient` (the runtime then retries via
    /// [`Connection::retry_producer_open`]). The permanent-failure path is covered by
    /// [`command_error_on_producer_open_with_permanent_code_emits_producer_open_failed`].
    #[test]
    fn command_error_on_producer_open_emits_producer_open_failed_transient() {
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
            Some(ConnectionEvent::ProducerOpenFailedTransient {
                handle: ev_handle,
                code,
                message,
            }) => {
                assert_eq!(ev_handle, handle);
                assert_eq!(code, pb::ServerError::ServiceNotReady as i32);
                assert_eq!(message, "namespace bundle not served");
            }
            other => panic!("expected ProducerOpenFailedTransient event, got {other:?}"),
        }
        assert!(
            conn.producer(handle).is_some(),
            "producer state must be RETAINED so the runtime can retry attach"
        );
        assert!(
            !conn.has_pending_request_for_test(request_id),
            "pending request slot freed"
        );
    }

    /// Sibling of the transient test above: a hard error code
    /// (`AuthorizationError`, `ProducerFenced`, …) MUST drop the producer state and
    /// emit `ProducerOpenFailed` so the user's open future fails fast. The transient
    /// retry path only applies to the codes Java's `ProducerImpl` treats as retriable
    /// (`MetadataError`, `ServiceNotReady`, `TopicNotFound`).
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

    /// Same shape as the producer-open transient case but on the subscribe path. The
    /// transient code keeps the consumer state alive so the runtime can retry via
    /// [`Connection::retry_consumer_subscribe`].
    #[test]
    fn command_error_on_subscribe_emits_subscribe_failed_transient() {
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
            Some(ConnectionEvent::SubscribeFailedTransient {
                handle: ev_handle,
                code,
                message,
            }) => {
                assert_eq!(ev_handle, handle);
                assert_eq!(code, pb::ServerError::ServiceNotReady as i32);
                assert_eq!(message, "namespace bundle not served");
            }
            other => panic!("expected SubscribeFailedTransient event, got {other:?}"),
        }
        assert!(
            !conn.has_pending_request_for_test(request_id),
            "pending request slot freed"
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
            .retry_producer_open(producer)
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
        assert_eq!(sends[0].sequence_id, seq.0, "original sequence id preserved");
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
        let _ = conn.initial_flow(handle);
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
        let _ = conn.initial_flow(handle);
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
        let _ = conn.initial_flow(handle);
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
}

#[cfg(all(test, feature = "scalable-topics"))]
mod scalable_conn_tests {
    use super::*;
    use crate::pb::scalable_topics as st;

    fn connected_conn() -> Connection {
        let mut conn = Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        );
        conn.begin_handshake().expect("handshake");
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(crate::SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        crate::frame::encode_command(&mut buf, &cmd).expect("encode Connected");
        conn.handle_bytes(Instant::now(), &buf).expect("connected");
        // Drain the handshake `Connected` event so per-test assertions only
        // see the scalable-topic events.
        while conn.poll_event().is_some() {}
        conn
    }

    /// Layer (a) test: feed a `CommandScalableTopicLookupResponse` and assert
    /// the connection emits `ScalableTopicLookupResolved` with the segment
    /// list + controller URL + lookup token.
    #[test]
    fn conn_emits_scalable_topic_lookup_resolved() {
        let mut conn = connected_conn();
        let rid = conn.send_scalable_topic_lookup("topic://public/default/scaled", false);
        let _ = conn.poll_transmit();

        let resp = st::CommandScalableTopicLookupResponse {
            request_id: rid.0,
            response: st::scalable_lookup_response::LookupType::Connect as i32,
            controller_broker_url: Some("pulsar://controller:6650".to_owned()),
            controller_broker_url_tls: None,
            segments: vec![
                st::SegmentDescriptor {
                    segment_id: 1,
                    broker_url: "pulsar://seg1:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 0,
                    key_range_end: 32_768,
                    state: st::SegmentStatePb::Active as i32,
                },
                st::SegmentDescriptor {
                    segment_id: 2,
                    broker_url: "pulsar://seg2:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 32_768,
                    key_range_end: 65_536,
                    state: st::SegmentStatePb::Active as i32,
                },
            ],
            lookup_token: Some(42),
            error: None,
            message: None,
        };
        let mut buf = bytes::BytesMut::new();
        st::encode(&mut buf, &st::ScalableBaseCommand::lookup_response(resp))
            .expect("encode response");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle lookup response");

        let mut resolved = None;
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::ScalableTopicLookupResolved {
                request_id,
                controller_broker_url,
                segments,
                lookup_token,
            } = ev
            {
                resolved = Some((request_id, controller_broker_url, segments, lookup_token));
            }
        }
        let (request_id, url, segments, token) =
            resolved.expect("ScalableTopicLookupResolved emitted");
        assert_eq!(request_id, rid);
        assert_eq!(url, "pulsar://controller:6650");
        assert_eq!(segments.len(), 2);
        assert_eq!(token, 42);
        assert_eq!(segments[0].segment_id, crate::types::SegmentId(1));
    }

    /// Layer (a) test: open a DagWatch session, feed a `SegmentDagUpdate`
    /// carrying a split, and assert the connection emits both
    /// `SegmentDagUpdated` and `DagChangedDuringConsume { reason: Split }`.
    #[test]
    fn conn_emits_dag_changed_during_consume() {
        let mut conn = connected_conn();
        let initial = vec![crate::types::SegmentDescriptor {
            segment_id: crate::types::SegmentId(1),
            key_range: crate::types::KeyRange {
                start: 0,
                end: 65_536,
            },
            broker_url: "pulsar://seg1:6650".to_owned(),
            state: crate::types::SegmentState::Active,
        }];
        let sid = conn.open_dag_watch("topic://public/default/scaled", 42, initial);
        let _ = conn.poll_transmit();

        // Broker acks the watch subscribe.
        let watch_resp = st::CommandSegmentDagWatchResponse {
            watch_session_id: sid,
            request_id: 1,
            error: None,
            message: None,
        };
        let mut buf = bytes::BytesMut::new();
        st::encode(
            &mut buf,
            &st::ScalableBaseCommand::dag_watch_response(watch_resp),
        )
        .expect("encode watch resp");
        conn.handle_bytes(Instant::now(), &buf).expect("watch ack");

        // Broker pushes a split update.
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: sid,
            update_seq: 1,
            added: vec![
                st::SegmentDescriptor {
                    segment_id: 2,
                    broker_url: "pulsar://seg2:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 0,
                    key_range_end: 32_768,
                    state: st::SegmentStatePb::Active as i32,
                },
                st::SegmentDescriptor {
                    segment_id: 3,
                    broker_url: "pulsar://seg3:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 32_768,
                    key_range_end: 65_536,
                    state: st::SegmentStatePb::Active as i32,
                },
            ],
            removed: vec![],
            split_events: vec![st::SplitEvent {
                parent_segment_id: 1,
                child_segment_ids: vec![2, 3],
                split_at_entry: 1000,
            }],
            merge_events: vec![],
        };
        let mut buf = bytes::BytesMut::new();
        st::encode(&mut buf, &st::ScalableBaseCommand::dag_update(upd)).expect("encode update");
        conn.handle_bytes(Instant::now(), &buf).expect("update");

        let mut saw_updated = false;
        let mut saw_changed = false;
        while let Some(ev) = conn.poll_event() {
            match ev {
                ConnectionEvent::SegmentDagUpdated {
                    watch_session_id,
                    delta,
                } => {
                    assert_eq!(watch_session_id, sid);
                    assert_eq!(delta.split_events.len(), 1);
                    saw_updated = true;
                }
                ConnectionEvent::DagChangedDuringConsume {
                    watch_session_id,
                    reason,
                } => {
                    assert_eq!(watch_session_id, sid);
                    assert_eq!(reason, crate::dag_watch::DagChangeReason::Split);
                    saw_changed = true;
                }
                _ => {}
            }
        }
        assert!(saw_updated, "SegmentDagUpdated emitted");
        assert!(saw_changed, "DagChangedDuringConsume emitted on split");
        // Post-split DAG: parent gone, two children present.
        let snap = conn.dag_snapshot(sid).expect("session still open");
        assert_eq!(snap.len(), 2);
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
}
