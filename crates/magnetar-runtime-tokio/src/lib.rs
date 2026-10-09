// SPDX-License-Identifier: Apache-2.0

//! Tokio engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine over a tokio TCP stream,
//! optionally wrapped with `tokio-rustls`. One driver task per connection, no channels.
//!
//! # Quickstart
//!
//! ```no_run
//! use magnetar_proto::{ConnectionConfig, CreateProducerRequest};
//! use magnetar_proto::producer::OutgoingMessage;
//! use magnetar_runtime_tokio::Client;
//!
//! # async fn run() -> Result<(), magnetar_runtime_tokio::ClientError> {
//! let client = Client::connect("pulsar://localhost:6650", ConnectionConfig::default()).await?;
//!
//! let producer = client.open_producer(CreateProducerRequest {
//!     topic: "persistent://public/default/example".to_owned(),
//!     ..Default::default()
//! }).await?;
//!
//! let mut msg = OutgoingMessage {
//!     payload: bytes::Bytes::from_static(b"hello"),
//!     metadata: Default::default(),
//!     uncompressed_size: 5,
//!     num_messages: 1,
//!     txn_id: None,
//!     source_message_id: None,
//! };
//! msg.metadata.producer_name = "demo".to_owned();
//! let _id = producer.send(msg).await?;
//!
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # No channels
//!
//! This crate does not use any flavour of channel (mpsc / broadcast / watch / oneshot). The
//! pattern is documented in [GUIDELINES.md] §"No-channels rule" and atomised in
//! [ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md):
//!
//! - User-facing futures lock `Arc<parking_lot::Mutex<magnetar_proto::Connection>>` directly.
//! - Driver wake-ups travel through a single-cell [`tokio::sync::Notify`].
//! - Future completion uses [`core::task::Waker`] slabs inside the sans-io state machine,
//!   registered via [`magnetar_proto::Connection::register_waker`] and dispatched when the matching
//!   [`magnetar_proto::OpOutcome`] lands.
//!
//! See also [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! (sans-io split) and [ADR-0011](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md)
//! (clock injection on state-machine entries).
//!
//! [GUIDELINES.md]: https://github.com/CleverCloud/magnetar/blob/main/GUIDELINES.md

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The driver state machine is naturally branchy; pedantic lints fight the readability of
    // an event-pump loop. We tighten these later once the engine has stabilised.
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

pub mod auth_file;
pub mod auto_cluster_failover;
mod client;
/// Pulsar payload compression / decompression — a re-export of [`magnetar_proto::compress`].
///
/// The codecs moved into the sans-io core with ADR-0112: a batched entry has to be
/// decompressed as ONE body before `ConsumerState::deliver` can split it, and
/// `ProducerState::flush_batch` compresses the concatenated body it builds, so both sides of
/// the batch layout now need the codecs below the engine. This module keeps the
/// `magnetar_runtime_tokio::compress::{compress, decompress, kind_from_pb, CompressionError}`
/// path working for existing callers.
pub mod compress {
    pub use magnetar_proto::compress::*;
}
mod consumer;
pub mod crypto;
pub mod dns;
mod driver;
mod error;
mod log_fields;
mod pool;
mod producer;
pub mod tls_crypto;
pub mod tls_insecure;
pub mod tls_no_hostname;
mod transport;
mod url_parse;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::auth_file::file_token_auth;
pub use crate::auto_cluster_failover::{AutoClusterFailover, TokioHealthProbe};
pub use crate::client::Client;
pub use crate::compress::CompressionError;
pub use crate::consumer::{Consumer, ReceiveFut};
pub use crate::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
pub use crate::dns::{DnsResolveFuture, DnsResolver, TokioDnsResolver, arc_dns_resolver};
pub use crate::driver::DriverHandle;
pub use crate::error::ClientError;
pub use crate::producer::{Producer, SendFut};
pub use crate::tls_insecure::insecure_tls_config;
pub use crate::tls_no_hostname::tls_config_no_hostname;
pub use crate::transport::default_tls_config;
#[cfg(feature = "scalable-topics")]
pub use crate::url_parse::is_scalable_topic_url;
pub use crate::url_parse::{ParsedUrl, Scheme};

/// Shared connection state — the lock-protected sans-io state machine + a single-cell driver
/// wake-up.
///
/// Cheap to share via `Arc`. The mutex is `parking_lot::Mutex` (not async), held only for the
/// duration of a sans-io call (no `.await` inside the critical section).
///
/// # Lock-ordering invariant (ADR-0038)
///
/// `ConnectionShared.inner` guards connection-wide state (frame buffers,
/// handshake, pending requests, the events / outcomes / wakers slabs, the
/// handle registry). Per-handle hot state lives behind its own
/// `parking_lot::Mutex` on [`magnetar_proto::ProducerSlot`] /
/// [`magnetar_proto::ConsumerSlot`]. Acquisition order is strictly **global
/// (`inner`) → per-slot (`slot.state`), never the reverse** — a holder of
/// `slot.state.lock()` MUST release the slot lock before taking `inner.lock()`.
/// The producer-send hot path skips the global lock entirely via
/// [`magnetar_proto::ProducerSlot::queue_send`]; the driver merges
/// per-slot staged frames into the connection-wide buffer through
/// `Connection::poll_transmit` (which calls `drain_producer_outbound`
/// internally). See [ADR-0038](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0038-split-connection-mutex.md).
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex. See the
    /// type-level docs above for the lock-ordering invariant against the
    /// per-slot mutexes.
    pub inner: Mutex<magnetar_proto::Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel.
    pub driver_waker: Notify,
    /// Dedicated wakeup for futures that drain handle-correlated events.
    ///
    /// This is separate from [`Self::driver_waker`] so event waiters cannot
    /// consume outbound-work permits intended for the driver. The `Arc`
    /// permits those futures to retain an owned, pre-armed notification across
    /// polls, closing the registration gap around `notify_waiters()`.
    pub(crate) event_waker: Arc<Notify>,
    /// Pulsed when an opening producer/consumer is cancelled.
    ///
    /// Detached retry legs enroll before checking handle liveness, then stop
    /// their sleep or intermediate lookup when the caller's deadline/drop
    /// removes the handle.
    pub(crate) operation_cancel_notify: Notify,
    /// Optional auth provider that the driver consults when the broker emits
    /// [`CommandAuthChallenge`](magnetar_proto::pb::CommandAuthChallenge).
    /// `None` means no in-band token refresh — the connection will drop if the
    /// broker challenges. PIP-30 / PIP-292.
    pub auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    /// PIP-145 topic-list-watcher deltas. The driver pushes
    /// [`magnetar_proto::ConnectionEvent::TopicListChanged`] events here as the broker
    /// emits them; surface them via [`Client::next_topic_list_change`].
    pub topic_list_changes: Mutex<std::collections::VecDeque<TopicListChange>>,
    /// Wakeup for `next_topic_list_change` futures. Notified after every push to
    /// `topic_list_changes`.
    pub topic_list_notify: Notify,
    /// PIP-33 replicated-subscription marker observations. The driver drains
    /// [`magnetar_proto::ConnectionEvent::ReplicatedSubscriptionMarkerObserved`]
    /// events here so they do not accumulate in the proto event queue.
    /// Surface via [`Client::next_replicated_subscription_marker`] /
    /// [`Client::poll_replicated_subscription_marker`]. See ADR-0034.
    pub replicated_subscription_markers:
        Mutex<std::collections::VecDeque<ObservedReplicatedSubscriptionMarker>>,
    /// Wakeup for `next_replicated_subscription_marker` futures.
    pub replicated_subscription_marker_notify: Notify,
    /// Set by the auto-reconnect supervisor between [`magnetar_proto::Connection::reset`] and
    /// the new socket's handshake. When `true`, the driver loop runs
    /// [`magnetar_proto::Connection::rebuild_producers`] +
    /// [`magnetar_proto::Connection::rebuild_consumers`] the first time it observes the new
    /// session transitioning to [`magnetar_proto::HandshakeState::Connected`], then clears
    /// the flag so the rebuild fires exactly once per reconnect. Stage 3 of the supervisor
    /// work: transparent producer / consumer replay on session loss.
    pub pending_rebuild: AtomicBool,
    /// Set to `true` the moment this connection reaches a GENUINELY-terminal
    /// state with NO driver left to recover it: the plain (non-supervised)
    /// driver's terminal exit, or the supervisor give-up after exhausting its
    /// reconnect-attempt budget (both call sites pair this with
    /// [`magnetar_proto::Connection::fail_all_pending`]). ADR-0059.
    ///
    /// This is the load-bearing "no driver will recover this" signal the
    /// synchronous fast-fail guards read at the request-issue / subscribe /
    /// lookup entry points. It is DISTINCT from
    /// [`magnetar_proto::Connection::is_closed`]: a SUPERVISED connection is
    /// transiently `Failed` between `mark_disconnected()` and the supervisor's
    /// `reset()` while it WILL recover, so `is_closed()` alone cannot tell a
    /// recoverable-`Failed` apart from a terminal-`Failed`. An entry-point
    /// guard fast-fails only when `is_closed()` AND `no_driver` are BOTH true,
    /// so a recoverable supervised connection in its transient `Failed` window
    /// is never `PeerClosed` (transparent reconnect is preserved).
    ///
    /// Mirrored 1:1 on the moonpool engine's `ConnectionShared` (ADR-0024).
    /// `AtomicBool` (not a channel) is the right primitive for this one-way
    /// latch ([ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)).
    pub no_driver: AtomicBool,
    /// The client-wide publish memory budget (Java `ClientBuilder#memoryLimit`,
    /// issue #867, [ADR-0111](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0111-share-one-memory-limit-controller-per-client.md)).
    ///
    /// [`Client`] builds one controller from its [`magnetar_proto::ConnectionConfig`] and
    /// shares it with every connection it opens, so all of them draw on the same bytes.
    /// [`crate::Producer::send`] reserves each payload against it before handing the
    /// payload to the sans-io state machine; the reservation travels inside the publish's
    /// [`magnetar_proto::producer::OpSend`] and is released when that op leaves the
    /// client. A connection constructed directly ([`Self::new`] / [`Self::with_auth`])
    /// gets a private controller built from its own config.
    pub memory_limit: Arc<magnetar_proto::MemoryLimitController>,
    /// Set to `true` after the first successful TC-partition lookup. Pulsar brokers do not
    /// load the `__transaction_coordinator_assign-partition-N` topic until something forces
    /// the namespace bundle onto them; the first `CommandLookupTopic` for the TC partition
    /// is that trigger. Without this bootstrap, the first `CommandNewTxn` lands on a broker
    /// whose `TransactionMetadataStoreService.stores.get(tcId)` returns `null` and the broker
    /// replies `TransactionCoordinatorNotFound` (mapped to `TxnError::NotFound`). The Java
    /// client side-steps the issue by eagerly opening one
    /// `TransactionMetaStoreHandler` per TC partition during
    /// `PulsarClientImpl.initTransactionCoordinatorClient()` — the handler itself does the
    /// lookup. We mirror that lazily: the first `Client::new_txn` looks up the TC partition,
    /// flips this flag, and subsequent calls skip the bootstrap. Persists across reconnects
    /// (broker keeps the TC store loaded on disk).
    pub txn_bootstrapped: AtomicBool,
    /// PIP-460 (ADR-0093) scalable-topic events the driver drained off the
    /// proto queue (`ScalableTopicLookupResolved`, `SegmentDagUpdated`,
    /// `DagChangedDuringConsume`, `DagWatchClosed`). Surface via
    /// [`Client::next_scalable_event`]. Not a channel — a `VecDeque` behind
    /// the same `parking_lot::Mutex` + `Notify` wake pattern as the PIP-145
    /// topic-list deltas.
    #[cfg(feature = "scalable-topics")]
    pub scalable_events: Mutex<std::collections::VecDeque<crate::ScalableEvent>>,
    /// Wakeup for `next_scalable_event` futures. Notified after every push to
    /// `scalable_events`.
    #[cfg(feature = "scalable-topics")]
    pub scalable_notify: Notify,
}

/// PIP-145 topic-list-watcher delta surfaced from the driver to the user-facing
/// [`Client`]. Mirrors `ConnectionEvent::TopicListChanged` with owned vectors so callers
/// don't pay for borrows across the await boundary.
#[derive(Debug, Clone)]
pub struct TopicListChange {
    /// Topics that newly match the pattern.
    pub added: Vec<String>,
    /// Topics that no longer match the pattern.
    pub removed: Vec<String>,
}

/// PIP-33: a replicated-subscription marker observation surfaced by the driver.
/// Owned snapshot of `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` so callers
/// can hold it across `.await` boundaries.
#[derive(Debug, Clone)]
pub struct ObservedReplicatedSubscriptionMarker {
    /// Consumer the marker arrived on.
    pub handle: magnetar_proto::ConsumerHandle,
    /// Decoded marker payload.
    pub marker: magnetar_proto::ReplicatedSubscriptionMarker,
}

impl std::fmt::Debug for ConnectionShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionShared")
            .field("inner", &"<Connection>")
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish_non_exhaustive()
    }
}

/// Outcome of [`ConnectionShared::await_reconnect_or_terminal`], the
/// wake-or-terminal park the engine-side lookup-retry loop consults after an
/// in-flight lookup was severed by a supervised reconnect (ADR-0060 /
/// follow-ups ADR-0060). Mirrored 1:1 on the moonpool engine (ADR-0024).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupReissueReadiness {
    /// The connection is back in `Connected` on a fresh session — re-issue the
    /// lookup.
    Reconnected,
    /// The connection is terminal (`is_closed()`) AND `no_driver` is latched —
    /// no driver will recover it. Short-circuit to `PeerClosed` (composes with
    /// the terminal fast-fail).
    Terminal,
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    pub fn new(config: magnetar_proto::ConnectionConfig) -> Arc<Self> {
        Self::with_auth(config, None)
    }

    /// Latch the [`Self::no_driver`] signal: this connection has reached a
    /// genuinely-terminal state and no driver task will recover it. Set by the
    /// plain driver's terminal-exit path and the supervisor give-up path,
    /// alongside [`magnetar_proto::Connection::fail_all_pending`]. ADR-0059.
    pub fn mark_no_driver(&self) {
        self.no_driver.store(true, Ordering::SeqCst);
    }

    /// `true` once [`Self::mark_no_driver`] has latched — the connection has
    /// reached a genuinely-terminal state and no driver task will recover it
    /// (plain-driver terminal exit, or supervisor give-up after exhausting its
    /// attempt budget). Read by the receive future (issue #299) to surface
    /// `Err` after a supervised give-up: at that point the connection is still
    /// `HandshakeState::Failed` with a supervisor configured, so
    /// [`magnetar_proto::Connection::consumer_handle_is_terminal`] alone cannot
    /// tell it apart from a recoverable mid-reconnect `Failed` window — the
    /// engine-side `no_driver` latch is the discriminator.
    #[must_use]
    pub fn is_no_driver(&self) -> bool {
        self.no_driver.load(Ordering::SeqCst)
    }

    /// Fast-fail guard for the request-issue / subscribe / lookup entry points
    /// (ADR-0059). Returns `Err(ClientError::PeerClosed)`
    /// when the connection is terminal AND no driver will recover it — i.e.
    /// `is_closed()` AND [`Self::no_driver`] are BOTH set. Returns `Ok(())`
    /// otherwise, INCLUDING the transient `Failed` window of a SUPERVISED
    /// connection mid-reconnect (where `no_driver` is still `false`), so
    /// transparent reconnect is never regressed.
    ///
    /// Gating on `no_driver` alone (without `is_closed()`) would be unsound on
    /// a freshly-constructed connection whose driver has not yet started;
    /// gating on `is_closed()` alone would `PeerClosed` a recoverable
    /// supervised connection. Both conditions together pin exactly the
    /// "doomed new op" case.
    pub fn fail_if_no_driver(&self) -> Result<(), ClientError> {
        if self.no_driver.load(Ordering::SeqCst) && self.inner.lock().is_closed() {
            return Err(ClientError::PeerClosed);
        }
        Ok(())
    }

    /// Park until this connection is live again on a fresh session, or has gone
    /// genuinely terminal — whichever happens first. Used by the engine-side
    /// lookup-retry-on-`SessionLost` loop (ADR-0060): when an
    /// in-flight `CommandLookupTopic` is severed by a supervised reconnect
    /// ([`magnetar_proto::Connection::reset`] publishes
    /// [`magnetar_proto::OpOutcome::SessionLost`] on its request-id), the
    /// caller waits here before re-issuing the lookup against the new session.
    ///
    /// Returns:
    /// * [`LookupReissueReadiness::Reconnected`] once the state machine is back in
    ///   [`magnetar_proto::HandshakeState::Connected`] — the caller may re-issue the lookup.
    /// * [`LookupReissueReadiness::Terminal`] once the connection `is_closed()` AND
    ///   [`Self::no_driver`] is latched — no driver will recover it, so the caller short-circuits
    ///   to [`ClientError::PeerClosed`]. This composes with the terminal fast-fail: a terminal
    ///   `SessionLost` surfaces a clean `PeerClosed` rather than re-hanging or spinning to the
    ///   re-issue bound.
    ///
    /// Park-on-readiness, NOT a timer: we wait on
    /// [`Self::driver_waker`], which both engines pulse via
    /// `notify_waiters()` on every state transition (post-`CommandConnected`
    /// included). A bare wake that leaves the connection still not-connected and
    /// not-terminal loops back to park again — it never proceeds to re-issue. The
    /// `Notified` future is created and `enable()`d *before* the state re-check so
    /// a transition racing between the check and the await is not lost (no
    /// timer / no host-clock read — ADR-0011).
    pub async fn await_reconnect_or_terminal(&self) -> LookupReissueReadiness {
        loop {
            // Arm the wakeup BEFORE inspecting state so a transition that lands
            // between the check and the await is captured by this `Notified`.
            let notified = self.driver_waker.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let conn = self.inner.lock();
                if conn.is_connected() {
                    return LookupReissueReadiness::Reconnected;
                }
                // Terminal AND no driver will recover it — compose with the terminal fast-fail.
                if conn.is_closed() && self.no_driver.load(Ordering::SeqCst) {
                    return LookupReissueReadiness::Terminal;
                }
            }

            // Neither live nor terminal yet — park until the next driver pulse,
            // then re-check. A spurious wake just re-loops; it does NOT proceed.
            notified.await;
        }
    }

    /// Construct with an auth provider for in-band challenge refresh.
    pub fn with_auth(
        config: magnetar_proto::ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        let memory_limit = magnetar_proto::MemoryLimitController::from_config(&config);
        Self::with_auth_and_memory_limit(config, auth_provider, memory_limit)
    }

    /// Construct a connection that draws on an existing client-wide publish memory
    /// budget (ADR-0111). `config.memory_limit_bytes` / `config.memory_limit_policy` are
    /// not consulted: `memory_limit` already carries the client's limit and policy.
    pub fn with_auth_and_memory_limit(
        config: magnetar_proto::ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
        memory_limit: Arc<magnetar_proto::MemoryLimitController>,
    ) -> Arc<Self> {
        // ADR-0028: opt-in anti-thrash detector. When the supervisor config
        // declares a threshold, mirror it onto the sans-io detector so the
        // engine driver can feed re-attach outcomes into it.
        let anti_thrash_threshold = config
            .supervisor
            .as_ref()
            .and_then(|s| s.anti_thrash_threshold);
        let anti_thrash_cooldown = config.supervisor.as_ref().map_or_else(
            || std::time::Duration::from_secs(30),
            |s| s.max_backoff_after_thrash,
        );
        let mut conn =
            magnetar_proto::Connection::new(config, Arc::new(std::time::SystemTime::now));
        conn.set_anti_thrash(anti_thrash_threshold, anti_thrash_cooldown);
        // Construction postcondition: a freshly-built connection has not begun
        // the handshake, so it can be neither `Connected` nor terminal. This
        // runs before any socket I/O, so it cannot fire on broker / wire input;
        // it would only trip if `Connection::new` ever stopped starting in
        // `HandshakeState::Uninitialized`. Mirrored 1:1 in the moonpool engine's
        // `ConnectionShared::with_auth_and_wall_clock_base` (ADR-0038 symmetry).
        debug_assert!(
            !conn.is_connected() && !conn.is_closed(),
            "freshly-constructed ConnectionShared must start in a non-connected, \
             non-terminal handshake state",
        );
        Arc::new(Self {
            inner: Mutex::new(conn),
            driver_waker: Notify::new(),
            event_waker: Arc::new(Notify::new()),
            operation_cancel_notify: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
            replicated_subscription_markers: Mutex::new(std::collections::VecDeque::new()),
            replicated_subscription_marker_notify: Notify::new(),
            pending_rebuild: AtomicBool::new(false),
            no_driver: AtomicBool::new(false),
            memory_limit,
            txn_bootstrapped: AtomicBool::new(false),
            #[cfg(feature = "scalable-topics")]
            scalable_events: Mutex::new(std::collections::VecDeque::new()),
            #[cfg(feature = "scalable-topics")]
            scalable_notify: Notify::new(),
        })
    }
}

/// PIP-460 (ADR-0093) resolved scalable-topic lookup. Returned by
/// [`Client::scalable_topic_lookup`]. **Experimental.**
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone)]
pub struct ScalableLookup {
    /// Client-allocated session id. The session stays open and keeps receiving
    /// layout updates until it is closed; pass this to
    /// `close_scalable_topic_session`.
    pub session_id: u64,
    /// Canonical `topic://...` identity the broker resolved the request to.
    pub resolved_topic_name: Option<String>,
    /// Controller broker serving this topic's layout, when advertised.
    pub controller_broker_url: Option<String>,
    /// Initial DAG snapshot for the topic.
    pub segments: Vec<magnetar_proto::SegmentDescriptor>,
    /// Layout epoch the snapshot was stamped with.
    pub epoch: u64,
}

/// PIP-460 (ADR-0093) scalable-topic event surfaced from the driver to the
/// user-facing [`Client`]. Owned snapshot of the relevant
/// [`magnetar_proto::ConnectionEvent`] variants so callers can hold them
/// across `.await` boundaries. **Experimental.**
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone)]
pub enum ScalableEvent {
    /// A scalable-topic session resolved: its first layout landed.
    LookupResolved {
        /// Client-allocated session id of the originating lookup.
        session_id: u64,
        /// Canonical `topic://...` identity the broker resolved the request to.
        resolved_topic_name: Option<String>,
        /// Controller broker serving this topic's layout, when advertised.
        controller_broker_url: Option<String>,
        /// Initial DAG snapshot for the topic.
        segments: Vec<magnetar_proto::SegmentDescriptor>,
        /// Layout epoch the snapshot was stamped with.
        epoch: u64,
    },
    /// An open session applied a subsequent layout.
    DagUpdated {
        /// Session id the update belongs to.
        session_id: u64,
        /// The applied delta.
        delta: magnetar_proto::DagDelta,
    },
    /// The segment DAG changed under a live consumer (drop-on-change).
    DagChangedDuringConsume {
        /// Session id whose DAG changed.
        session_id: u64,
        /// Why the DAG changed.
        reason: magnetar_proto::DagChangeReason,
    },
    /// The scalable-topic session closed.
    DagWatchClosed {
        /// Session id that closed.
        session_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
    /// A scalable consumer's registration resolved with its initial share.
    ConsumerAssigned {
        /// Consumer id that registered.
        consumer_id: u64,
        /// The `segment://` topics this consumer owns.
        assignment: magnetar_proto::ConsumerAssignment,
    },
    /// The controller leader rebalanced a registered consumer's share.
    AssignmentChanged {
        /// Consumer id whose share changed.
        consumer_id: u64,
        /// What to attach to and detach from.
        delta: magnetar_proto::AssignmentDelta,
    },
    /// A scalable consumer's registration was rejected.
    ConsumerRejected {
        /// Consumer id whose registration failed.
        consumer_id: u64,
        /// Why the broker rejected it.
        reason: String,
    },
    /// A namespace-level scalable-topics watch delivered a snapshot or a diff.
    TopicsChanged {
        /// Watch id the update belongs to.
        watch_id: u64,
        /// The snapshot or diff the broker sent.
        change: magnetar_proto::TopicsChange,
    },
    /// A namespace-level scalable-topics watch ended.
    TopicsWatchClosed {
        /// Watch id that closed.
        watch_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
    /// The metadata-driven transaction-coordinator assignment set changed.
    TcAssignmentsChanged {
        /// Watch id the update belongs to.
        watch_id: u64,
        /// Number of transaction-coordinator partitions.
        parallelism: u32,
        /// Which broker serves each coordinator.
        assignments: Vec<magnetar_proto::TcAssignment>,
    },
    /// A transaction-coordinator discovery watch ended.
    TcAssignmentsWatchClosed {
        /// Watch id that closed.
        watch_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;

    use super::{ConnectionShared, TopicListChange};

    #[test]
    fn shared_state_can_be_constructed() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        let _g = s.inner.lock();
        // Topic-list buffer starts empty.
        assert!(s.topic_list_changes.lock().is_empty());
    }

    /// ADR-0059 regression: `fail_if_no_driver()` must NOT
    /// fast-fail a connection that is `is_closed()` (here: `Failed`) while a
    /// supervisor is still able to recover it — i.e. while `no_driver` is unset.
    /// This pins the exact window a SUPERVISED connection lives in between
    /// `mark_disconnected()` (→ `Failed`, so `is_closed()` is true) and the
    /// supervisor's `reset()` (→ `Uninitialized`): an op issued there must reach
    /// the live driver and recover, NOT be wrongly `PeerClosed`d (which would
    /// regress transparent reconnect, ADR-0038). Gating on `is_closed()` alone —
    /// the naive guard — would fail this test. 1:1 twin of the moonpool engine.
    #[test]
    fn fail_if_no_driver_does_not_fire_on_recoverable_failed_window() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        // Drive the connection to `Failed` (a transient drop), exactly as a
        // supervised driver would on `PeerClosed` before its next `reset()`.
        s.inner.lock().mark_disconnected();
        assert!(
            s.inner.lock().is_closed(),
            "mark_disconnected must put the connection in a terminal handshake state",
        );
        assert!(
            !s.no_driver.load(super::Ordering::SeqCst),
            "no_driver must still be UNSET in the recoverable-Failed window",
        );
        // The guard must return Ok — the supervised driver is still alive and
        // will recover this connection. A `PeerClosed` here is the regression.
        assert!(
            s.fail_if_no_driver().is_ok(),
            "fail_if_no_driver must NOT fire while the connection is recoverable \
             (is_closed but no_driver unset) — regressing this breaks transparent reconnect",
        );
    }

    /// ADR-0059: `fail_if_no_driver()` DOES fast-fail with
    /// `PeerClosed` once BOTH conditions hold — `is_closed()` AND the `no_driver`
    /// latch (set by the plain driver's terminal exit / supervisor give-up). 1:1
    /// twin of the moonpool engine.
    #[test]
    fn fail_if_no_driver_fires_when_closed_and_no_driver_latched() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        s.inner.lock().mark_disconnected();
        // The terminal-exit / give-up paths latch this alongside
        // `fail_all_pending`.
        s.mark_no_driver();
        assert!(s.inner.lock().is_closed());
        assert!(s.no_driver.load(super::Ordering::SeqCst));
        assert!(
            matches!(s.fail_if_no_driver(), Err(super::ClientError::PeerClosed)),
            "fail_if_no_driver must return PeerClosed once the connection is terminal \
             AND no driver is left to recover it",
        );
    }

    /// ADR-0059: the `no_driver` latch is unsound as a sole gate — on a
    /// freshly-constructed connection whose driver has not started,
    /// `fail_if_no_driver` must return Ok because the connection is not yet
    /// `is_closed()`. Pins the second half of the two-condition gate. 1:1 twin
    /// of the moonpool engine.
    #[test]
    fn fail_if_no_driver_does_not_fire_before_any_terminal_state() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        assert!(
            !s.inner.lock().is_closed(),
            "a fresh connection is not terminal",
        );
        assert!(
            s.fail_if_no_driver().is_ok(),
            "fail_if_no_driver must not fire on a non-terminal connection",
        );
    }

    #[test]
    fn topic_list_changes_buffer_round_trip() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec!["a".to_owned()],
            removed: vec![],
        });
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec![],
            removed: vec!["b".to_owned()],
        });
        let first = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(first.added, vec!["a".to_owned()]);
        let second = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(second.removed, vec!["b".to_owned()]);
        assert!(s.topic_list_changes.lock().is_empty());
    }

    #[test]
    fn memory_limit_zero_disables_enforcement() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        assert_eq!(s.memory_limit.limit_bytes(), 0);
        let unlimited = s
            .memory_limit
            .try_reserve(u64::MAX)
            .expect("an unlimited budget admits anything");
        assert_eq!(unlimited.bytes(), 0, "nothing is counted without a limit");
        assert_eq!(s.memory_limit.used_bytes(), 0);
    }

    #[test]
    fn memory_limit_reserve_and_release_round_trip() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);

        let first = s.memory_limit.try_reserve(400).expect("fits");
        let _second = s.memory_limit.try_reserve(400).expect("fits");
        assert_eq!(s.memory_limit.used_bytes(), 800);

        // Overflow: 800 + 300 > 1024.
        let refused = s.memory_limit.try_reserve(300).expect_err("over the limit");
        assert_eq!(
            (refused.current, refused.limit, refused.requested),
            (800, 1024, 300)
        );

        // Dropping a reservation makes room.
        drop(first);
        assert_eq!(s.memory_limit.used_bytes(), 400);
        let _third = s.memory_limit.try_reserve(300).expect("fits after release");
    }

    #[test]
    fn memory_limit_is_shared_by_connections_built_with_it() {
        let budget = magnetar_proto::MemoryLimitController::new(
            1000,
            magnetar_proto::MemoryLimitPolicy::FailImmediately,
        );
        let first = ConnectionShared::with_auth_and_memory_limit(
            ConnectionConfig::default(),
            None,
            budget.clone(),
        );
        let second = ConnectionShared::with_auth_and_memory_limit(
            ConnectionConfig::default(),
            None,
            budget.clone(),
        );
        let _held = first.memory_limit.try_reserve(600).expect("fits");
        let refused = second
            .memory_limit
            .try_reserve(600)
            .expect_err("both connections draw on the one client-wide budget");
        assert_eq!(refused.current, 600);
        // A directly constructed connection keeps its own budget.
        let standalone = ConnectionShared::new(ConnectionConfig {
            memory_limit_bytes: 1000,
            ..ConnectionConfig::default()
        });
        let _own = standalone
            .memory_limit
            .try_reserve(600)
            .expect("private budget");
    }

    // Cheap counter-Waker so we don't pull in `futures-task` for the test.
    // Mirrors the pattern used elsewhere in the workspace; counts how many
    // times `wake()` was invoked.
    struct CountingWaker {
        count: std::sync::atomic::AtomicUsize,
    }

    impl CountingWaker {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                count: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        fn count(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl std::task::Wake for CountingWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Memory-limit fast path: with budget available, `poll_reserve` must
    /// take the reservation without parking a waiter. Built through
    /// `ConnectionShared::with_auth_and_memory_limit`, the constructor the
    /// pool path uses to share the client-wide budget (ADR-0111). Mirrors
    /// the moonpool engine's unit test of the same name (ADR-0024 parity).
    #[test]
    fn poll_reserve_succeeds_without_parking_when_budget_available() {
        let budget = magnetar_proto::MemoryLimitController::new(
            1024,
            magnetar_proto::MemoryLimitPolicy::ProducerBlock,
        );
        let s = ConnectionShared::with_auth_and_memory_limit(
            ConnectionConfig::default(),
            None,
            budget.clone(),
        );
        assert!(
            std::sync::Arc::ptr_eq(&s.memory_limit, &budget),
            "the given budget is shared"
        );
        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());

        let mut waiter = None;
        let std::task::Poll::Ready(reservation) =
            s.memory_limit.poll_reserve(512, &mut waiter, &waker)
        else {
            panic!("budget available: must not park");
        };
        assert_eq!(reservation.bytes(), 512);
        assert_eq!(
            format!("{reservation:?}"),
            "MemoryReservation { bytes: 512, .. }",
            "Debug shows the bytes, not the shared controller"
        );
        assert_eq!(budget.used_bytes(), 512);
        assert_eq!(waiter, None);
        assert_eq!(budget.parked_waiters(), 0);
        assert_eq!(cw.count(), 0);
        drop(reservation);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn poll_reserve_parks_until_a_release() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        let full = s.memory_limit.try_reserve(1024).expect("initial reserve");

        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());
        let mut waiter = None;
        assert!(
            s.memory_limit
                .poll_reserve(1, &mut waiter, &waker)
                .is_pending()
        );
        let parked = waiter.expect("parking registers a waiter");
        assert_eq!(s.memory_limit.parked_waiters(), 1);
        assert_eq!(cw.count(), 0);

        // Releasing wakes parked callers and drains the registration.
        drop(full);
        assert_eq!(s.memory_limit.used_bytes(), 0);
        assert_eq!(cw.count(), 1);
        assert_eq!(s.memory_limit.parked_waiters(), 0);
        // The registration was drained — the caller's cancel must be a no-op.
        s.memory_limit.cancel_waiter(parked);
        assert!(
            s.memory_limit
                .poll_reserve(1, &mut waiter, &waker)
                .is_ready()
        );
        assert_eq!(waiter, None);
    }

    #[test]
    fn cancel_waiter_clears_registration() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 100,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        let full = s.memory_limit.try_reserve(100).expect("initial reserve");

        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());
        let mut waiter = None;
        assert!(
            s.memory_limit
                .poll_reserve(1, &mut waiter, &waker)
                .is_pending()
        );
        assert_eq!(s.memory_limit.parked_waiters(), 1);

        // Cancel: simulates the future being dropped before release.
        s.memory_limit.cancel_waiter(waiter.expect("parked"));
        assert_eq!(s.memory_limit.parked_waiters(), 0);

        // Release after cancel must not wake the dropped waiter.
        drop(full);
        assert_eq!(cw.count(), 0);
    }

    #[test]
    fn release_wakes_all_parked_waiters() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 100,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        let full = s.memory_limit.try_reserve(100).expect("initial reserve");

        let cw1 = CountingWaker::new();
        let cw2 = CountingWaker::new();
        let w1 = std::task::Waker::from(cw1.clone());
        let w2 = std::task::Waker::from(cw2.clone());
        let (mut k1, mut k2) = (None, None);
        assert!(s.memory_limit.poll_reserve(1, &mut k1, &w1).is_pending());
        assert!(s.memory_limit.poll_reserve(1, &mut k2, &w2).is_pending());
        assert_ne!(k1, k2, "every park gets its own waiter id");
        assert_eq!(s.memory_limit.parked_waiters(), 2);

        drop(full);
        assert_eq!(cw1.count(), 1);
        assert_eq!(cw2.count(), 1);
        assert_eq!(s.memory_limit.parked_waiters(), 0);
    }

    /// Lost-wakeup check across threads: a release racing a parking
    /// `poll_reserve` either lands before the attempt (which then succeeds)
    /// or after the registration (which it then wakes). Whichever way the
    /// race falls, a `Pending` outcome is always paired with a wake.
    /// Mirrors the moonpool engine's twin 1:1 (ADR-0024 parity).
    #[test]
    fn concurrent_release_never_strands_a_parked_reservation() {
        use std::time::{Duration, Instant};

        let cfg = ConnectionConfig {
            memory_limit_bytes: 16,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());

        // The deadline ENDS the loop rather than failing it: the contract
        // holds on every iteration, so the count only bounds the runtime.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut iters = 0usize;
        while Instant::now() <= deadline && iters < 2_000 {
            iters += 1;
            let full = shared
                .memory_limit
                .try_reserve(16)
                .expect("seed budget at limit");
            let wakes_before = cw.count();
            let releaser = std::thread::spawn(move || {
                std::thread::yield_now();
                drop(full);
            });
            let mut waiter = None;
            let outcome = shared.memory_limit.poll_reserve(2, &mut waiter, &waker);
            releaser.join().expect("releaser thread");
            match outcome {
                std::task::Poll::Ready(reservation) => drop(reservation),
                std::task::Poll::Pending => {
                    assert!(
                        cw.count() > wakes_before,
                        "a parked reservation must be woken by the release that follows it"
                    );
                    if let Some(id) = waiter {
                        shared.memory_limit.cancel_waiter(id);
                    }
                }
            }
            assert_eq!(shared.memory_limit.used_bytes(), 0);
        }
        assert!(
            iters >= 100,
            "expected ≥100 race iterations within 5s, got {iters}"
        );
    }
}
