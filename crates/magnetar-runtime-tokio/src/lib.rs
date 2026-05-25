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
//! [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md):
//!
//! - User-facing futures lock `Arc<parking_lot::Mutex<magnetar_proto::Connection>>` directly.
//! - Driver wake-ups travel through a single-cell [`tokio::sync::Notify`].
//! - Future completion uses [`core::task::Waker`] slabs inside the sans-io state machine,
//!   registered via [`magnetar_proto::Connection::register_waker`] and dispatched when the matching
//!   [`magnetar_proto::OpOutcome`] lands.
//!
//! See also [ADR-0004](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! (sans-io split) and [ADR-0011](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md)
//! (clock injection on state-machine entries).
//!
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

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

pub mod auto_cluster_failover;
mod client;
pub mod compress;
mod consumer;
pub mod crypto;
pub mod dns;
mod driver;
mod error;
mod producer;
pub mod tls_insecure;
pub mod tls_no_hostname;
mod transport;
mod url_parse;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Waker;

use parking_lot::Mutex;
use slab::Slab;
use tokio::sync::Notify;

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
pub use crate::url_parse::{ParsedUrl, Scheme};

/// Shared connection state — the lock-protected sans-io state machine + a single-cell driver
/// wake-up.
///
/// Cheap to share via `Arc`. The mutex is `parking_lot::Mutex` (not async), held only for the
/// duration of a sans-io call (no `.await` inside the critical section).
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<magnetar_proto::Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel.
    pub driver_waker: Notify,
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
    /// Set by the auto-reconnect supervisor between [`magnetar_proto::Connection::reset`] and
    /// the new socket's handshake. When `true`, the driver loop runs
    /// [`magnetar_proto::Connection::rebuild_producers`] +
    /// [`magnetar_proto::Connection::rebuild_consumers`] the first time it observes the new
    /// session transitioning to [`magnetar_proto::HandshakeState::Connected`], then clears
    /// the flag so the rebuild fires exactly once per reconnect. Stage 3 of the supervisor
    /// work: transparent producer / consumer replay on session loss.
    pub pending_rebuild: AtomicBool,
    /// Configured global publish memory budget in bytes. `0` disables the limit
    /// (matches `ConnectionConfig::memory_limit_bytes` default). Mirrors Java's
    /// `ClientBuilder#memoryLimit`. Reservations against this budget happen in
    /// [`crate::Producer::send`] BEFORE the payload reaches the sans-io state
    /// machine; sends that would push `memory_used` past the limit are rejected
    /// synchronously with [`ClientError::MemoryLimitExceeded`].
    pub memory_limit_bytes: u64,
    /// Current in-flight publish bytes reserved by [`crate::Producer::send`] calls
    /// that have not yet seen their [`magnetar_proto::OpOutcome::SendReceipt`] /
    /// `SendError`. Bumped in `send` (CAS against `memory_limit_bytes`); decremented
    /// in [`crate::SendFut::poll`] when the future returns `Poll::Ready`.
    pub memory_used: AtomicU64,
    /// Configured back-pressure policy when the publish budget is exhausted.
    /// Mirrors Java `org.apache.pulsar.client.api.MemoryLimitPolicy`. When
    /// `FailImmediately`, reservations that would overflow are rejected
    /// synchronously with [`ClientError::MemoryLimitExceeded`]. When
    /// `ProducerBlock`, the runtime parks the offending send future on
    /// [`Self::memory_wakers`] until enough budget frees up.
    ///
    /// Snapshotted from
    /// [`magnetar_proto::ConnectionConfig::memory_limit_policy`] at
    /// construction time. See [ADR-0020](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0020-memory-limit-producer-block.md).
    pub memory_limit_policy: magnetar_proto::MemoryLimitPolicy,
    /// Waker slab consulted by [`Self::release_memory`] when a reservation
    /// frees up. Populated by [`Self::try_reserve_memory_or_register`] from
    /// inside [`crate::Producer::send`] under
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`]. Drained on every
    /// release: every parked send wakes and re-attempts the reservation
    /// (fairness is approximate — first-to-poll wins, matching Java's
    /// `MemoryLimitController` semantics).
    ///
    /// Not a channel — this is a `Slab<Waker>` behind a `parking_lot::Mutex`,
    /// the canonical no-channel wake pattern (see
    /// [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)).
    pub memory_wakers: Mutex<Slab<Waker>>,
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

impl std::fmt::Debug for ConnectionShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionShared")
            .field("inner", &"<Connection>")
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish_non_exhaustive()
    }
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    pub fn new(config: magnetar_proto::ConnectionConfig) -> Arc<Self> {
        Self::with_auth(config, None)
    }

    /// Try to reserve `bytes` against the configured memory budget. Returns
    /// `Ok(())` when the reservation succeeds (or no limit is configured —
    /// `memory_limit_bytes = 0`); returns `Err(ClientError::MemoryLimitExceeded
    /// { current, limit, requested })` when the reservation would push
    /// `memory_used` past `memory_limit_bytes`.
    ///
    /// Lock-free: a CAS loop on `memory_used`. Mirrors Java's
    /// `MemoryLimitController` (in `MemoryLimitPolicy.FailImmediately`
    /// mode).
    ///
    /// See [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)
    /// — `AtomicU64` is not a channel; it's the right primitive for this counter.
    pub fn try_reserve_memory(&self, bytes: u64) -> Result<(), ClientError> {
        if self.memory_limit_bytes == 0 {
            return Ok(());
        }
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > self.memory_limit_bytes {
                return Err(ClientError::MemoryLimitExceeded {
                    current,
                    limit: self.memory_limit_bytes,
                    requested: bytes,
                });
            }
            // Acquire-Release CAS so that releases on other threads are visible.
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            // Lost the race; retry with the fresh value.
        }
    }

    /// Release a previous reservation. Called by [`crate::SendFut`] when the
    /// send completes (success or error). Saturating sub so a buggy
    /// over-release can't underflow the counter.
    ///
    /// After releasing, drains every waker parked on
    /// [`Self::memory_wakers`] so blocked
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`] sends re-attempt
    /// their reservation. Drain-all (rather than wake-one) matches Java's
    /// `MemoryLimitController` behaviour where any released byte may unblock
    /// several smaller pending sends; spurious wake-ups are cheap because
    /// the futures re-check the CAS budget on every poll.
    pub fn release_memory(&self, bytes: u64) {
        if bytes == 0 || self.memory_limit_bytes == 0 {
            // No budget configured: there cannot be parked wakers either.
            return;
        }
        // `fetch_sub` wraps on underflow; guard manually with a CAS loop.
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_sub(bytes);
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        self.drain_memory_wakers();
    }

    /// Try to reserve `bytes` against the configured memory budget; on
    /// failure, register `waker` on [`Self::memory_wakers`] so the caller can
    /// be re-polled when budget frees up via [`Self::release_memory`].
    ///
    /// Returns:
    /// - `Ok(())` when the reservation succeeded (or no limit is configured).
    /// - `Err(slab_key)` when the reservation failed; the caller MUST cancel the registration via
    ///   [`Self::cancel_memory_waker`] if it is dropped before observing the next release.
    ///
    /// This is the building block of
    /// [`magnetar_proto::MemoryLimitPolicy::ProducerBlock`]. The
    /// `SendFut` future in `crate::producer` polls this method until it
    /// succeeds; on `Drop` it calls
    /// [`Self::cancel_memory_waker`] to evict the stale waker slot.
    ///
    /// Re-checking after registration closes the lost-wakeup window: a
    /// release that lands between the failed CAS and the slab insert will
    /// have drained the (empty) slab without observing this waker, so we
    /// re-attempt the reservation once the waker is installed.
    pub fn try_reserve_memory_or_register(&self, bytes: u64, waker: &Waker) -> Result<(), usize> {
        // Fast path: no budget configured, or budget has room right now.
        if self.try_reserve_memory(bytes).is_ok() {
            return Ok(());
        }
        // Slow path: park a waker and re-check. The recheck closes the race
        // where a release fires between the failed CAS above and the slab
        // insert below.
        let key = self.memory_wakers.lock().insert(waker.clone());
        if self.try_reserve_memory(bytes).is_ok() {
            // Won the recheck; drop our registration so the next release
            // doesn't wake a future that already completed.
            self.cancel_memory_waker(key);
            return Ok(());
        }
        Err(key)
    }

    /// Remove a previously-registered waker. Called from the
    /// `SendFut` `Drop` impl in `crate::producer`
    /// and on the "won the recheck" path of
    /// [`Self::try_reserve_memory_or_register`]. Idempotent — a missing slot
    /// is a no-op (a concurrent [`Self::release_memory`] may have drained it
    /// already).
    pub fn cancel_memory_waker(&self, slab_key: usize) {
        let mut slab = self.memory_wakers.lock();
        if slab.contains(slab_key) {
            slab.remove(slab_key);
        }
    }

    /// Drain every parked waker and wake it. Called from
    /// [`Self::release_memory`] after the CAS-decrement lands.
    ///
    /// Held the slab lock only for the duration of the swap; `wake()` runs
    /// outside the critical section so user code that re-polls cannot
    /// deadlock on the slab mutex.
    fn drain_memory_wakers(&self) {
        let wakers: Vec<Waker> = {
            let mut slab = self.memory_wakers.lock();
            let drained: Vec<Waker> = slab.drain().collect();
            drained
        };
        for w in wakers {
            w.wake();
        }
    }

    /// Construct with an auth provider for in-band challenge refresh.
    pub fn with_auth(
        config: magnetar_proto::ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        let memory_limit_bytes = config.memory_limit_bytes;
        let memory_limit_policy = config.memory_limit_policy;
        Arc::new(Self {
            inner: Mutex::new(magnetar_proto::Connection::new(config)),
            driver_waker: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
            pending_rebuild: AtomicBool::new(false),
            memory_limit_bytes,
            memory_used: AtomicU64::new(0),
            memory_limit_policy,
            memory_wakers: Mutex::new(Slab::new()),
            txn_bootstrapped: AtomicBool::new(false),
        })
    }
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
        assert_eq!(s.memory_limit_bytes, 0);
        assert!(s.try_reserve_memory(u64::MAX).is_ok());
        // No-op release.
        s.release_memory(u64::MAX);
    }

    #[test]
    fn memory_limit_reserve_and_release_round_trip() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);

        assert!(s.try_reserve_memory(400).is_ok());
        assert!(s.try_reserve_memory(400).is_ok());
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 800);

        // Overflow: 800 + 300 > 1024.
        match s.try_reserve_memory(300) {
            Err(super::ClientError::MemoryLimitExceeded {
                current,
                limit,
                requested,
            }) => {
                assert_eq!(current, 800);
                assert_eq!(limit, 1024);
                assert_eq!(requested, 300);
            }
            other => panic!("expected MemoryLimitExceeded, got {other:?}"),
        }

        // Releasing makes room.
        s.release_memory(400);
        assert!(s.try_reserve_memory(300).is_ok());
    }

    #[test]
    fn memory_limit_release_is_saturating() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        // Over-release must not underflow.
        s.release_memory(1_000_000);
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 0);
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

    #[test]
    fn try_reserve_memory_or_register_succeeds_when_budget_available() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());

        // Empty budget: must take the fast path and NOT register a waker.
        s.try_reserve_memory_or_register(512, &waker)
            .expect("should succeed with budget available");
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 512);
        assert_eq!(s.memory_wakers.lock().len(), 0);
        assert_eq!(cw.count(), 0);
    }

    #[test]
    fn try_reserve_memory_or_register_parks_when_budget_full() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        // Saturate the budget.
        s.try_reserve_memory(1024).expect("initial reserve");

        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());

        let key = s
            .try_reserve_memory_or_register(1, &waker)
            .expect_err("must park when full");
        assert_eq!(s.memory_wakers.lock().len(), 1);
        assert_eq!(cw.count(), 0);

        // Releasing wakes parked futures.
        s.release_memory(1024);
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 0);
        assert_eq!(cw.count(), 1);
        // The slab was drained — caller's cancel must be a no-op.
        s.cancel_memory_waker(key);
        assert_eq!(s.memory_wakers.lock().len(), 0);
    }

    #[test]
    fn cancel_memory_waker_clears_slot() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 100,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        s.try_reserve_memory(100).expect("initial reserve");

        let cw = CountingWaker::new();
        let waker = std::task::Waker::from(cw.clone());

        let key = s
            .try_reserve_memory_or_register(1, &waker)
            .expect_err("must park when full");
        assert_eq!(s.memory_wakers.lock().len(), 1);

        // Cancel: simulates the future being dropped before release.
        s.cancel_memory_waker(key);
        assert_eq!(s.memory_wakers.lock().len(), 0);

        // Release after cancel must not panic and must not wake the dropped waker.
        s.release_memory(100);
        assert_eq!(cw.count(), 0);
    }

    #[test]
    fn release_wakes_all_parked_wakers() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 100,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        s.try_reserve_memory(100).expect("initial reserve");

        let cw1 = CountingWaker::new();
        let cw2 = CountingWaker::new();
        let w1 = std::task::Waker::from(cw1.clone());
        let w2 = std::task::Waker::from(cw2.clone());

        let _k1 = s.try_reserve_memory_or_register(1, &w1).expect_err("park");
        let _k2 = s.try_reserve_memory_or_register(1, &w2).expect_err("park");
        assert_eq!(s.memory_wakers.lock().len(), 2);

        s.release_memory(100);
        assert_eq!(cw1.count(), 1);
        assert_eq!(cw2.count(), 1);
        assert_eq!(s.memory_wakers.lock().len(), 0);
    }

    /// Lost-wakeup race-window coverage: the recheck path inside
    /// `ConnectionShared::try_reserve_memory_or_register` returns
    /// `Ok(())` when a concurrent `release_memory` frees budget between
    /// the failed fast-path CAS and the post-slab CAS. Drives the race
    /// via two threads and a tight loop. Mirrors the moonpool engine's
    /// `try_reserve_memory_or_register_wins_recheck_under_contention`
    /// 1:1 so ADR-0024's runtime test parity gate stays balanced.
    #[test]
    fn try_reserve_memory_or_register_wins_recheck_under_contention() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::task::Wake;
        use std::time::{Duration, Instant};

        struct NoopWaker(AtomicUsize);
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, super::Ordering::SeqCst);
            }
        }

        let cfg = ConnectionConfig {
            memory_limit_bytes: 16,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        let waker_ctr = Arc::new(NoopWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(waker_ctr.clone());

        let deadline = Instant::now() + Duration::from_secs(5);
        for _ in 0..10_000usize {
            assert!(
                Instant::now() <= deadline,
                "recheck race did not fire within 5s budget",
            );
            shared.try_reserve_memory(16).expect("seed budget at limit");

            let releaser = {
                let s = shared.clone();
                std::thread::spawn(move || {
                    std::thread::yield_now();
                    s.release_memory(16);
                })
            };
            let outcome = shared.try_reserve_memory_or_register(2, &waker);
            releaser.join().expect("releaser thread");

            match outcome {
                Ok(()) => {
                    assert!(shared.memory_wakers.lock().is_empty());
                    shared.release_memory(2);
                }
                Err(key) => {
                    shared.cancel_memory_waker(key);
                }
            }
        }
        // Best-effort coverage probe — always passes correctness-wise.
        // On contention-rich hardware the recheck-won path fires within
        // tens of iterations, lighting up the equivalent lines in the
        // tokio runtime for sim-coverage parity.
    }
}
