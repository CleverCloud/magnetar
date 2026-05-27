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
use crate::event::{ConnectionEvent, IncomingMessage, TxnRoundTrip};
use crate::frame::{Frame, decode_one, encode_command, encode_payload, encode_payload_head};
use crate::lookup::{LookupRegistry, LookupRequest, PartitionedMetadataRequest};
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
    /// Outbound bytes buffer drained by [`Self::poll_transmit`].
    outbound: BytesMut,
    /// Wave-1.1 staging slot for [`Self::poll_transmit_vectored`].
    /// Holds the most recently drained outbound `Bytes` so the
    /// `Transmit::Contiguous(&slice)` return borrows against an owned
    /// buffer the [`Connection`] keeps alive. Replaced on every
    /// `poll_transmit_vectored` call; the borrow checker prevents
    /// concurrent re-entry. `None` before the first vectored drain.
    pending_vectored_drain: Option<Bytes>,
    /// Wave-1.2 producer-batch segment buffer (ADR-0039). Drained by
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

#[derive(Debug, Clone, Copy)]
enum PendingRequestKind {
    Lookup,
    PartitionedMetadata,
    ProducerOpen { handle: ProducerHandle },
    ConsumerSubscribe { handle: ConsumerHandle },
    ConsumerSeek { handle: ConsumerHandle },
    ConsumerUnsubscribe { handle: ConsumerHandle },
    ConsumerGetLastMessageId { handle: ConsumerHandle },
    Ack { handle: ConsumerHandle },
    ProducerClose { handle: ProducerHandle },
    ConsumerClose { handle: ConsumerHandle },
    TopicWatcher { watcher_id: u64 },
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
        Self {
            config,
            state: HandshakeState::Uninitialized,
            broker_max_message_size: None,
            broker_protocol_version: 0,
            feature_flags: pb::FeatureFlags::default(),
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
            lookup: LookupRegistry::default(),
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
        }
    }

    /// Returns the current handshake state.
    pub fn state(&self) -> HandshakeState {
        self.state
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
        self.state = HandshakeState::Failed;
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

        // Drop any remaining (orphaned) wakers — every legitimate one was either
        // dispatched above or belongs to an op the runtime will re-register after the
        // reconnect.
        let leftover_keys: Vec<PendingOpKey> = self.wakers.keys().copied().collect();
        for key in leftover_keys {
            if let Some(w) = self.wakers.remove(&key) {
                self.outcomes.insert(key, OpOutcome::SessionLost { key });
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
        self.lookup = LookupRegistry::default();
        self.topic_watchers = TopicWatcherRegistry::default();

        // (5) Back to Uninitialized so begin_handshake on the freshly-handshaked socket
        // succeeds.
        self.state = HandshakeState::Uninitialized;
        self.broker_max_message_size = None;
        self.broker_protocol_version = 0;
        self.feature_flags = pb::FeatureFlags::default();
        self.last_activity = None;
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
    /// Snapshotted publishes (see `in_flight_publish_snapshots`) are replayed onto
    /// `producer.outbound` in their original FIFO order with their original sequence ids, then
    /// drained onto the connection's outbound buffer. Each replayed [`crate::producer::OpSend`]
    /// goes back into the producer's `pending` queue verbatim — its `waker` field is `None`
    /// (cleared by [`Self::reset`]) so the user-facing send future re-registers on its next
    /// poll and the eventual `CommandSendReceipt` resolves the future normally.
    /// Mirrors Java `ProducerImpl#resendMessages`.
    ///
    /// Producers explicitly closed via [`Self::close_producer`] (or by the broker via
    /// `CommandCloseProducer`) are skipped — their `closed` flag is honoured. Any snapshot
    /// for a now-closed producer is discarded along with the rest of its state.
    pub fn rebuild_producers(&mut self) -> Vec<RequestId> {
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
            // Replay any snapshotted in-flight publishes onto this producer's outbound queue
            // and reinstall them in `pending`. The wire-frame data was captured at
            // emit time so the replay is byte-for-byte identical to the original publish
            // (apart from the freshly-bumped `CommandProducer.epoch` that the broker now
            // associates with this producer).
            if let Some(snapshots) = self.in_flight_publish_snapshots.remove(&handle)
                && let Some(slot) = self.producers.get(&handle)
            {
                slot.state.lock().replay_snapshots(snapshots);
            }
        }
        // Drop any snapshots that belong to producers we did NOT rebuild (e.g. ones closed
        // between reset and rebuild). Their `OpSend`s never reach a future — the user-facing
        // close path is responsible for surfacing the disposition (`Closed` error).
        self.in_flight_publish_snapshots
            .retain(|h, _| live_handles.contains(h));
        // Drain the freshly-replayed outbound frames onto the wire — same path the regular
        // `Connection::send` uses after a `queue_send`.
        self.drain_producer_outbound();
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
            // Re-issue the initial flow now so the broker starts dispatching as soon as it
            // acks the subscribe. `initial_flow` quietly tolerates an unknown handle, but the
            // consumer must already be in `self.consumers` (it is — we filtered above), so
            // the flow command goes onto the wire alongside the subscribe.
            self.initial_flow(handle);
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
        self.state = HandshakeState::ConnectSent;
        Ok(())
    }

    /// Feed inbound bytes to the state machine — **owned-chunk** entry
    /// point (ADR-0039 wave 3 — read-path ownership pass-through).
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
            match decode_one(&mut frame_bytes) {
                Ok(frame) => {
                    self.handle_frame(now, frame)?;
                }
                Err(crate::frame::FrameError::ChecksumMismatch { computed, expected }) => {
                    // CRC mismatch — drop the corrupt frame, emit the
                    // observation event, and keep decoding.
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
                self.state = HandshakeState::Connected;
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
                self.state = HandshakeState::AuthChallenging;
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
                            // byte-identical to v0.1.0.
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
                    if let Some(slot) = self.producers.get(&handle) {
                        let mut producer = slot.state.lock();
                        producer.name = Some(ok.producer_name.clone());
                        producer.last_sequence_id_published = ok.last_sequence_id.unwrap_or(-1);
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
                    let (outcome, retry) = crate::lookup::translate_lookup_response(&resp, &req);
                    if let Some(retry) = retry {
                        let new_id = self.alloc_request_id();
                        let _ = self.send_lookup_internal(new_id, retry);
                    }
                    self.pending_requests.remove(&rid);
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        OpOutcome::LookupResponse {
                            request_id: rid,
                            outcome: outcome.clone(),
                        },
                    );
                    self.wake_for_request(rid);
                    self.events.push_back(ConnectionEvent::LookupResponse {
                        request_id: rid,
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
                if self.lookup.take_partition(rid).is_some() {
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
    /// (ADR-0039 waves 1.0 / 1.1).
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
    /// descriptor (ADR-0039 wave 2 — runtime adoption).
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
    pub fn poll_timeout(&self) -> Option<Instant> {
        let mut next = self
            .last_activity
            .map(|t| t + self.config.keepalive_interval);
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
        // Keepalive.
        let due = match self.last_activity {
            Some(last) if now >= last + self.config.keepalive_interval => true,
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
                    slot.state.lock().register_waker(seq, waker);
                    return;
                }
            }
            PendingOpKey::Request(_) => {}
        }
        self.wakers.insert(key, waker);
    }

    /// Consume the outcome of a pending op, if one is ready.
    pub fn take_outcome(&mut self, key: PendingOpKey) -> Option<OpOutcome> {
        self.outcomes.remove(&key)
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
        // Pull every queued frame from every producer and emit it into the connection's
        // outbound byte buffer.
        let handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in handles {
            // SAFETY (lock-ordering): the global Connection mutex is held by the
            // caller (Connection's `&mut self`); we take the per-slot mutex
            // BELOW it, never above. See ADR-0038.
            loop {
                let frame = self
                    .producers
                    .get(&handle)
                    .and_then(|slot| slot.state.lock().next_outbound_frame());
                let Some(frame) = frame else { break };
                let _ = encode_payload(
                    &mut self.outbound,
                    &frame.command,
                    &frame.metadata,
                    &frame.payload,
                );
            }
        }
    }

    /// Wave-1.2 (ADR-0039) — drain producer frames into the
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
        let handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in handles {
            loop {
                let frame = self
                    .producers
                    .get(&handle)
                    .and_then(|slot| slot.state.lock().next_outbound_frame());
                let Some(frame) = frame else { break };
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

    /// Issue a topic lookup. The state machine handles redirects internally; the user receives
    /// either a `Connect` or `Failed` outcome.
    pub fn lookup(&mut self, topic: &str, authoritative: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let req = LookupRequest {
            topic: topic.to_owned(),
            authoritative,
        };
        let _ = self.send_lookup_internal(request_id, req);
        request_id
    }

    fn send_lookup_internal(
        &mut self,
        request_id: RequestId,
        req: LookupRequest,
    ) -> Result<(), ProtocolError> {
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
        self.encode_command(&base)?;
        self.lookup.insert_lookup(request_id, req);
        self.pending_requests
            .insert(request_id, PendingRequestKind::Lookup);
        Ok(())
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
    pub fn get_partitioned_topic_metadata(&mut self, topic: &str) -> RequestId {
        let request_id = self.alloc_request_id();
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
        self.lookup.insert_partition(
            request_id,
            PartitionedMetadataRequest {
                topic: topic.to_owned(),
            },
        );
        self.pending_requests
            .insert(request_id, PendingRequestKind::PartitionedMetadata);
        request_id
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
        self.state = HandshakeState::Closing;
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
            self.state = HandshakeState::Connected;
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
        // Pending `OpSend`s from the transient window have already had their wire frames
        // written to the socket — but the broker dropped them because the producer wasn't
        // attached (Pulsar silently discards `CommandSend` for an unknown `producer_id`).
        // After `CommandProducer` lands the new attachment, replay each pending op's
        // wire frame so the broker re-publishes and the user's `SendFut` finally observes
        // a `CommandSendReceipt`. The frames go onto outbound AFTER `CommandProducer`, so
        // the broker processes the attach first and the sends second — same ordering Java's
        // `ProducerImpl#resendMessages` enforces after a reattach. Mirrors the snapshot
        // replay in `rebuild_producers` (Stage 3 at-least-once parity), but targeted at a
        // single producer's already-in-`pending` ops rather than the reset-time snapshot.
        if let Some(slot) = self.producers.get(&handle) {
            slot.state.lock().replay_pending_outbound();
        }
        self.drain_producer_outbound();
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
        let _ = self.initial_flow(handle);
        self.drain_producer_outbound();
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
        // ADR-0039 wave 3: when the proto's inbound buffer is empty,
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
    fn poll_transmit_vectored_emits_segments_when_outbound_empty() {
        // ADR-0039 wave 1.2: when `outbound_segments` is non-empty and
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
        // ADR-0039 wave 1.2 wire-order invariant: when both
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
        // ADR-0039 wave 1.1: the new `Transmit<'_>` entry point must
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

    /// Ported from Java `BinaryProtoLookupService` — a `CommandLookupTopicResponse` whose
    /// `response = Redirect` must trigger a *fresh* outbound `CommandLookupTopic` with a
    /// fresh request id. Verifies that the state machine itself drives the retry (no need
    /// for the user to re-submit). The retry counter (Java `maxLookupRedirects`) lives at
    /// the runtime layer; here we only pin that one redirect produces one retry frame.
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

        // The user-visible outcome is `LookupResponse::Redirected` for observability.
        match conn.take_outcome(PendingOpKey::Request(request_id)) {
            Some(OpOutcome::LookupResponse {
                outcome: crate::event::LookupOutcome::Redirected { .. },
                ..
            }) => {}
            other => panic!("expected Redirected outcome, got {other:?}"),
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

        // A CommandFlow must follow the subscribe so the broker resumes dispatching as soon
        // as it acks.
        let flow_cmd = cmds
            .iter()
            .filter(|c| c.r#type == pb::base_command::Type::Flow as i32)
            .find_map(|c| c.flow.as_ref())
            .expect("CommandFlow re-emitted alongside subscribe");
        assert_eq!(flow_cmd.consumer_id, c_handle.0);
        assert_eq!(flow_cmd.message_permits, 128);

        // The returned RequestId must match the one stamped on the subscribe frame.
        assert_eq!(request_ids[0].0, subscribe_cmd.request_id);
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
        conn.rebuild_producers();
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

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-pending".to_owned(),
            ..Default::default()
        });
        // Discard initial `CommandProducer` frame.
        let _ = drain_outbound_commands(&mut conn);

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

        conn.rebuild_producers();

        // The snapshot is consumed; pending now holds the three replayed OpSends in
        // original order.
        assert_eq!(conn.in_flight_publish_snapshot_len(producer), 0);
        assert_eq!(conn.producer_pending_count(producer), 3);

        // The outbound buffer now carries one `CommandProducer` (the rebuild) followed by
        // three `CommandSend` frames in the original `[0, 1, 2]` sequence-id order.
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

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-receipt".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);

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
        conn.rebuild_producers();
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

        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-order".to_owned(),
            ..Default::default()
        });
        let _ = drain_outbound_commands(&mut conn);

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
        conn.rebuild_producers();

        // The post-rebuild outbound buffer carries the CommandProducer first, then the
        // three replayed CommandSend frames in FIFO order. Decode payloads to verify.
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
        // Default subscribe (None) MUST omit field 14 entirely so the wire bytes match v0.1.0
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
}
