// SPDX-License-Identifier: Apache-2.0

//! Client-wide publish memory budget — Java `MemoryLimitController`
//! (`ClientBuilder#memoryLimit(long, MemoryLimitPolicy)`), issue #867,
//! [ADR-0111](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0111-share-one-memory-limit-controller-per-client.md).
//!
//! # One budget per client
//!
//! A runtime `Client` builds exactly one [`MemoryLimitController`] from its
//! [`ConnectionConfig`] and shares it (`Arc`) with the
//! bootstrap connection and every pooled connection it opens later — proxy
//! pool entries, `connections_per_broker` siblings and replacement
//! connections alike — so the configured bytes bound the whole client, not
//! each physical connection. A connection built without a client keeps a
//! private controller, which is the same budget scoped to that one
//! connection. Two separately constructed clients never share a budget.
//!
//! # What is counted
//!
//! The runtime reserves the length of the payload it hands to the producer
//! state machine — after its own compression and encryption — before the
//! payload enters a [`ProducerState`](crate::producer::ProducerState). Wire
//! framing, message metadata, batch container overhead, consumer receive
//! queues and everything else are not counted: this bounds pending publish
//! payloads, it is not a process memory (RSS) limit.
//!
//! # Reservation lifetime
//!
//! A successful reservation is a [`MemoryReservation`], which releases its
//! bytes exactly once, when dropped. The producer state machine moves it into
//! the publish's [`OpSend`](crate::producer::OpSend), so the bytes stay held
//! for exactly as long as the client retains the publish: they are released
//! when the op leaves the pending queue for good (send receipt, send error,
//! send timeout, producer close, terminal connection failure, non-replayable
//! batch reset), never when the caller's send future completes or is dropped,
//! and a reconnect that moves the op into its replay snapshot and back neither
//! releases nor re-charges it. A reservation that never reaches an op — a
//! synchronous enqueue rejection — is dropped by whoever still holds it.
//!
//! # Policies
//!
//! [`MemoryLimitPolicy::FailImmediately`](crate::MemoryLimitPolicy) callers use
//! [`MemoryLimitController::try_reserve`] and surface its
//! [`MemoryLimitExceeded`]. [`MemoryLimitPolicy::ProducerBlock`](crate::MemoryLimitPolicy)
//! callers park through [`MemoryLimitController::poll_reserve`], and every
//! release anywhere in the client wakes every parked caller on every
//! connection. The check is strict: a reservation is refused when
//! `current + requested > limit`. A limit of `0` disables accounting
//! entirely.
//!
//! # Locking
//!
//! The counter is a lock-free `AtomicU64`. Parked wakers live behind one
//! `parking_lot::Mutex` that is a LEAF lock: no other lock is ever acquired
//! while it is held, and wakers are always woken after it is released. It may
//! itself be acquired while the caller holds the connection mutex — an
//! [`OpSend`](crate::producer::OpSend) is dropped under it — but the producer
//! state machine hands every removed op back to its caller instead of dropping
//! it under a per-slot mutex, so no waker runs under a slot lock (ADR-0038).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Poll, Waker};

use crate::conn::{ConnectionConfig, MemoryLimitPolicy};

/// A reservation the configured budget cannot hold right now. Mirrors the
/// fields of Java's `MemoryBufferIsFullError` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("memory limit exceeded: current={current}B + requested={requested}B > limit={limit}B")]
pub struct MemoryLimitExceeded {
    /// Bytes reserved client-wide when the request was refused.
    pub current: u64,
    /// The configured client-wide limit.
    pub limit: u64,
    /// Bytes the caller asked to reserve.
    pub requested: u64,
}

/// Identity of one parked [`MemoryLimitController::poll_reserve`] caller.
///
/// Allocated from a monotonically increasing counter and never reused, so a
/// stale id can only ever name its own registration: cancelling it after a
/// release already drained it is a no-op instead of evicting a different
/// caller that happened to reuse the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryWaiterId(u64);

/// Parked `ProducerBlock` callers, keyed by their [`MemoryWaiterId`]. The
/// `BTreeMap` keeps wake order equal to registration order, which keeps the
/// moonpool simulation reproducible per seed.
#[derive(Debug, Default)]
struct Waiters {
    next_id: u64,
    parked: BTreeMap<u64, Waker>,
}

/// The client-wide publish memory budget. See the [module docs](self).
#[derive(Debug)]
pub struct MemoryLimitController {
    limit: u64,
    policy: MemoryLimitPolicy,
    used: AtomicU64,
    waiters: parking_lot::Mutex<Waiters>,
}

impl MemoryLimitController {
    /// A controller enforcing `limit_bytes` (`0` = unlimited) under `policy`.
    #[must_use]
    pub fn new(limit_bytes: u64, policy: MemoryLimitPolicy) -> Arc<Self> {
        Arc::new(Self {
            limit: limit_bytes,
            policy,
            used: AtomicU64::new(0),
            waiters: parking_lot::Mutex::new(Waiters::default()),
        })
    }

    /// A controller for `config.memory_limit_bytes` under
    /// `config.memory_limit_policy`.
    #[must_use]
    pub fn from_config(config: &ConnectionConfig) -> Arc<Self> {
        Self::new(config.memory_limit_bytes, config.memory_limit_policy)
    }

    /// The configured limit in bytes; `0` means unlimited.
    #[must_use]
    pub fn limit_bytes(&self) -> u64 {
        self.limit
    }

    /// What a caller does when a reservation does not fit.
    #[must_use]
    pub fn policy(&self) -> MemoryLimitPolicy {
        self.policy
    }

    /// Bytes currently reserved across the whole client.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Number of `ProducerBlock` callers currently parked.
    #[must_use]
    pub fn parked_waiters(&self) -> usize {
        self.waiters.lock().parked.len()
    }

    /// Reserve `bytes`, or refuse with the budget as it stood.
    ///
    /// Always succeeds with an empty reservation when the limit is `0`.
    ///
    /// # Errors
    ///
    /// [`MemoryLimitExceeded`] when `current + bytes > limit` (or the sum
    /// overflows `u64`). Nothing is reserved in that case.
    pub fn try_reserve(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<MemoryReservation, MemoryLimitExceeded> {
        if self.limit == 0 {
            return Ok(MemoryReservation::default());
        }
        let limit = self.limit;
        // Compare-and-swap loop: a lost race re-reads the counter and retries.
        loop {
            let current = self.used.load(Ordering::Acquire);
            let Some(next) = current.checked_add(bytes).filter(|next| *next <= limit) else {
                return Err(MemoryLimitExceeded {
                    current,
                    limit,
                    requested: bytes,
                });
            };
            if self
                .used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(MemoryReservation {
                    controller: Some(Arc::clone(self)),
                    bytes,
                });
            }
        }
    }

    /// `ProducerBlock` reservation: reserve `bytes`, or park `waker` until a
    /// release anywhere in the client makes room.
    ///
    /// `waiter` is the caller's registration slot: `None` before the first
    /// park. On `Pending` it names the caller's live registration — a fresh
    /// id when the previous one was drained by a release, the same id (with
    /// its waker refreshed) when the caller is re-polled without having been
    /// woken. On `Ready` it is reset to `None` and the registration removed.
    /// A caller dropped while parked must pass its id to
    /// [`Self::cancel_waiter`].
    ///
    /// The attempt and the registration happen under the waiter lock, which
    /// every release takes after decrementing the counter: a release either
    /// lands before the attempt (which then sees the freed bytes) or after
    /// the registration (which it then wakes). There is no window in which a
    /// release can be missed.
    pub fn poll_reserve(
        self: &Arc<Self>,
        bytes: u64,
        waiter: &mut Option<MemoryWaiterId>,
        waker: &Waker,
    ) -> Poll<MemoryReservation> {
        let mut waiters = self.waiters.lock();
        let (outcome, stale) = match self.try_reserve(bytes) {
            Ok(reservation) => {
                let stale = waiter.take().and_then(|id| waiters.parked.remove(&id.0));
                (Poll::Ready(reservation), stale)
            }
            Err(_) => (Poll::Pending, Self::park(&mut waiters, waiter, waker)),
        };
        // Leaf lock: release it before a replaced waker is dropped, so no
        // waker code ever runs under it.
        drop(waiters);
        drop(stale);
        outcome
    }

    /// Register `waker` under `waiter`, refreshing the caller's live
    /// registration when it still has one. Returns the waker it replaced.
    fn park(
        waiters: &mut Waiters,
        waiter: &mut Option<MemoryWaiterId>,
        waker: &Waker,
    ) -> Option<Waker> {
        if let Some(id) = *waiter
            && let Some(parked) = waiters.parked.get_mut(&id.0)
        {
            return Some(std::mem::replace(parked, waker.clone()));
        }
        let id = waiters.next_id;
        // A `u64` counter bumped once per park cannot wrap within the life of
        // a process, so ids are never reused.
        waiters.next_id = id.wrapping_add(1);
        // A fresh id is never already present, so `insert` replaces (and
        // drops) no waker under the leaf lock.
        waiters.parked.insert(id, waker.clone());
        *waiter = Some(MemoryWaiterId(id));
        None
    }

    /// Remove a parked caller's registration. A no-op when a release already
    /// drained it.
    pub fn cancel_waiter(&self, waiter: MemoryWaiterId) {
        let removed = self.waiters.lock().parked.remove(&waiter.0);
        // Dropped after the leaf lock, like every other waker.
        drop(removed);
    }

    /// Give `bytes` back and wake every parked caller. Called only by
    /// [`MemoryReservation`]'s `Drop`, exactly once per reservation and with
    /// exactly the bytes that reservation added, so the counter always holds
    /// at least `bytes` here and the subtraction cannot wrap.
    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
        // Take the parked set under the leaf lock, wake after releasing it:
        // a woken caller re-polls straight into `poll_reserve`, which takes
        // the same lock.
        let parked = std::mem::take(&mut self.waiters.lock().parked);
        for waker in parked.into_values() {
            waker.wake();
        }
    }
}

/// Bytes held against a [`MemoryLimitController`], released exactly once when
/// this value is dropped. `Default` is the empty reservation an unlimited
/// controller hands out; dropping it does nothing.
#[derive(Default)]
pub struct MemoryReservation {
    controller: Option<Arc<MemoryLimitController>>,
    bytes: u64,
}

impl std::fmt::Debug for MemoryReservation {
    /// Prints only the reserved bytes: every `OpSend` carries one, and the
    /// shared controller (with its parked wakers) is not the op's state.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl MemoryReservation {
    /// Bytes this reservation holds against the budget (`0` when the budget is
    /// unlimited).
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if let Some(controller) = self.controller.take() {
            controller.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Poll, Wake, Waker};

    use super::{MemoryLimitController, MemoryLimitExceeded};
    use crate::conn::{ConnectionConfig, MemoryLimitPolicy};

    /// Counts wakes and appends its tag to a shared log, so tests can assert
    /// both "was woken" and the wake order.
    struct TaggedWaker {
        tag: usize,
        wakes: AtomicUsize,
        log: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    impl Wake for TaggedWaker {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.log.lock().push(self.tag);
        }
    }

    fn tagged(tag: usize, log: &Arc<parking_lot::Mutex<Vec<usize>>>) -> (Arc<TaggedWaker>, Waker) {
        let inner = Arc::new(TaggedWaker {
            tag,
            wakes: AtomicUsize::new(0),
            log: log.clone(),
        });
        let waker = Waker::from(inner.clone());
        (inner, waker)
    }

    fn blocking(limit: u64) -> Arc<MemoryLimitController> {
        MemoryLimitController::new(limit, MemoryLimitPolicy::ProducerBlock)
    }

    #[test]
    fn from_config_carries_the_limit_and_policy() {
        let config = ConnectionConfig {
            memory_limit_bytes: 4096,
            memory_limit_policy: MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let controller = MemoryLimitController::from_config(&config);
        assert_eq!(controller.limit_bytes(), 4096);
        assert_eq!(controller.policy(), MemoryLimitPolicy::ProducerBlock);
        assert_eq!(controller.used_bytes(), 0);
        assert_eq!(controller.parked_waiters(), 0);
    }

    #[test]
    fn rejection_is_strict_and_reports_the_aggregate() {
        let controller = MemoryLimitController::new(100, MemoryLimitPolicy::FailImmediately);
        let first = controller.try_reserve(60).expect("60 of 100");
        assert_eq!(first.bytes(), 60);
        let refused = controller.try_reserve(41).expect_err("60 + 41 > 100");
        assert_eq!(
            refused,
            MemoryLimitExceeded {
                current: 60,
                limit: 100,
                requested: 41,
            }
        );
        assert_eq!(
            refused.to_string(),
            "memory limit exceeded: current=60B + requested=41B > limit=100B"
        );
        // Exactly reaching the limit is allowed: only `next > limit` refuses.
        let _second = controller.try_reserve(40).expect("60 + 40 == 100");
        assert_eq!(controller.used_bytes(), 100);
        // A refused attempt reserves nothing.
        assert!(controller.try_reserve(1).is_err());
        assert_eq!(controller.used_bytes(), 100);
    }

    #[test]
    fn zero_limit_is_unlimited_and_counts_nothing() {
        let controller = MemoryLimitController::new(0, MemoryLimitPolicy::ProducerBlock);
        let reservation = controller.try_reserve(u64::MAX).expect("unlimited");
        assert_eq!(reservation.bytes(), 0);
        assert_eq!(controller.used_bytes(), 0);
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (_w, waker) = tagged(0, &log);
        let mut waiter = None;
        assert!(
            controller
                .poll_reserve(u64::MAX, &mut waiter, &waker)
                .is_ready()
        );
        assert_eq!(waiter, None);
        drop(reservation);
        assert_eq!(controller.used_bytes(), 0);
    }

    #[test]
    fn overflowing_sum_is_refused_not_wrapped() {
        let controller = MemoryLimitController::new(u64::MAX, MemoryLimitPolicy::FailImmediately);
        let _held = controller.try_reserve(u64::MAX - 1).expect("fits");
        let refused = controller.try_reserve(2).expect_err("u64 overflow");
        assert_eq!(refused.current, u64::MAX - 1);
        assert_eq!(controller.used_bytes(), u64::MAX - 1);
    }

    #[test]
    fn each_reservation_is_released_exactly_once_on_drop() {
        let controller = MemoryLimitController::new(100, MemoryLimitPolicy::FailImmediately);
        let a = controller.try_reserve(30).expect("fits");
        let b = controller.try_reserve(20).expect("fits");
        assert_eq!(controller.used_bytes(), 50);
        drop(a);
        assert_eq!(controller.used_bytes(), 20);
        drop(b);
        assert_eq!(controller.used_bytes(), 0);
    }

    #[test]
    fn release_wakes_every_parked_caller_in_registration_order() {
        let controller = blocking(10);
        let full = controller.try_reserve(10).expect("fill");
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let wakers: Vec<_> = (0..3).map(|tag| tagged(tag, &log)).collect();
        let mut waiters = [None, None, None];
        for ((_, waker), waiter) in wakers.iter().zip(waiters.iter_mut()) {
            assert!(controller.poll_reserve(5, waiter, waker).is_pending());
        }
        assert_eq!(controller.parked_waiters(), 3);
        drop(full);
        assert_eq!(*log.lock(), vec![0, 1, 2], "registration order");
        assert_eq!(controller.parked_waiters(), 0);
        for (inner, _) in &wakers {
            assert_eq!(inner.wakes.load(Ordering::SeqCst), 1);
        }
    }

    /// The pre-ADR-0111 runtime slab freed its keys on drain, so a woken
    /// caller that failed again re-registered under the SAME key and then
    /// cancelled its "prior" key — its own fresh registration — and parked
    /// forever. Waiter ids are never reused: a re-park gets a new id and a
    /// stale cancel is a no-op.
    #[test]
    fn re_park_after_a_partial_release_keeps_a_live_registration() {
        let controller = blocking(1000);
        let a1 = controller.try_reserve(600).expect("fits");
        let a2 = controller.try_reserve(400).expect("fits");
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (b_count, b) = tagged(0, &log);
        let (c_count, c) = tagged(1, &log);

        let mut b_waiter = None;
        assert!(controller.poll_reserve(500, &mut b_waiter, &b).is_pending());
        let first_id = b_waiter.expect("parked");

        // 400 B back: B is woken, but 600 + 500 still does not fit.
        drop(a2);
        assert_eq!(b_count.wakes.load(Ordering::SeqCst), 1);
        assert!(controller.poll_reserve(500, &mut b_waiter, &b).is_pending());
        let second_id = b_waiter.expect("re-parked");
        assert_ne!(
            first_id, second_id,
            "a drained id is never handed out again"
        );

        // Another caller parks; a stale cancel of B's first id evicts nobody.
        let mut c_waiter = None;
        assert!(controller.poll_reserve(500, &mut c_waiter, &c).is_pending());
        controller.cancel_waiter(first_id);
        assert_eq!(controller.parked_waiters(), 2);

        // The next release reaches both.
        drop(a1);
        assert_eq!(b_count.wakes.load(Ordering::SeqCst), 2);
        assert_eq!(c_count.wakes.load(Ordering::SeqCst), 1);
        let Poll::Ready(b_reserved) = controller.poll_reserve(500, &mut b_waiter, &b) else {
            panic!("B fits after the full release");
        };
        assert_eq!(b_waiter, None);
        let Poll::Ready(c_reserved) = controller.poll_reserve(500, &mut c_waiter, &c) else {
            panic!("C fits next to B");
        };
        assert_eq!(controller.used_bytes(), 1000);
        drop((b_reserved, c_reserved));
        assert_eq!(controller.used_bytes(), 0);
    }

    #[test]
    fn re_poll_without_a_wake_refreshes_the_registration() {
        let controller = blocking(10);
        let full = controller.try_reserve(10).expect("fill");
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (old_count, old) = tagged(0, &log);
        let (new_count, new) = tagged(1, &log);
        let mut waiter = None;
        assert!(controller.poll_reserve(1, &mut waiter, &old).is_pending());
        let id = waiter;
        assert!(controller.poll_reserve(1, &mut waiter, &new).is_pending());
        assert_eq!(waiter, id, "same registration, refreshed in place");
        assert_eq!(controller.parked_waiters(), 1);
        drop(full);
        assert_eq!(old_count.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(new_count.wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancel_waiter_removes_only_its_own_registration() {
        let controller = blocking(10);
        let full = controller.try_reserve(10).expect("fill");
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (gone_count, gone) = tagged(0, &log);
        let (kept_count, kept) = tagged(1, &log);
        let (mut gone_id, mut kept_id) = (None, None);
        assert!(controller.poll_reserve(1, &mut gone_id, &gone).is_pending());
        assert!(controller.poll_reserve(1, &mut kept_id, &kept).is_pending());
        controller.cancel_waiter(gone_id.expect("parked"));
        controller.cancel_waiter(gone_id.expect("parked"));
        assert_eq!(controller.parked_waiters(), 1);
        drop(full);
        assert_eq!(gone_count.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(kept_count.wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_reservation_dropped_without_parked_callers_wakes_nobody() {
        let controller = blocking(10);
        let held = controller.try_reserve(4).expect("fits");
        drop(held);
        assert_eq!(controller.used_bytes(), 0);
        assert_eq!(controller.parked_waiters(), 0);
    }
}
