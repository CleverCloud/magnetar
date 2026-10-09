// SPDX-License-Identifier: Apache-2.0

//! Mirrors `magnetar-runtime-moonpool/tests/memory_limit_race_stress.rs`
//! 1:1 on the tokio engine. Same race scenarios, same assertions: both
//! engines drive the one `magnetar_proto::MemoryLimitController` (issue
//! #867, ADR-0111) through `ConnectionShared::memory_limit`. Keeping the
//! test 1:1 satisfies ADR-0024's runtime test parity gate.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Wake};
use std::thread;

use magnetar_proto::{ConnectionConfig, MemoryLimitPolicy};
use magnetar_runtime_tokio::ConnectionShared;

/// Counting waker for tests — increments on every wake call so we can
/// confirm parked reservations actually receive wakeups under contention.
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn shared(limit: u64) -> Arc<ConnectionShared> {
    let cfg = ConnectionConfig {
        memory_limit_bytes: limit,
        memory_limit_policy: MemoryLimitPolicy::ProducerBlock,
        ..ConnectionConfig::default()
    };
    ConnectionShared::new(cfg)
}

/// Spin N parking reservers against M releasers, all racing the same
/// budget. A reserver that parks must have been woken by the time every
/// releaser has finished (no lost wakeup), and the bookkeeping must balance
/// back to zero once every reservation is dropped (no leak, no double
/// release).
#[test]
fn memory_limit_reserve_and_release_race_balances_to_zero() {
    const ITERS: usize = 200;
    const RESERVERS: usize = 4;
    const RELEASERS: usize = 4;
    const LIMIT: u64 = 1024;
    const PAYLOAD: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        // Saturate the budget in PAYLOAD-sized chunks, one per releaser.
        let chunks: Vec<_> = (0..RELEASERS)
            .map(|_| {
                shared
                    .memory_limit
                    .try_reserve(PAYLOAD)
                    .expect("initial saturation must succeed")
            })
            .collect();
        assert_eq!(shared.memory_limit.used_bytes(), LIMIT);

        let mut reservers = Vec::with_capacity(RESERVERS);
        for _ in 0..RESERVERS {
            let s = Arc::clone(&shared);
            reservers.push(thread::spawn(move || {
                let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
                let waker = std::task::Waker::from(counter.clone());
                let mut waiter = None;
                match s.memory_limit.poll_reserve(PAYLOAD, &mut waiter, &waker) {
                    Poll::Ready(reservation) => {
                        drop(reservation);
                        None
                    }
                    Poll::Pending => Some((counter, waiter)),
                }
            }));
        }
        let mut releasers = Vec::with_capacity(RELEASERS);
        for chunk in chunks {
            let s = Arc::clone(&shared);
            releasers.push(thread::spawn(move || {
                drop(chunk);
                // Re-take and give back a chunk to keep pressure high.
                drop(s.memory_limit.try_reserve(PAYLOAD));
            }));
        }
        let parked: Vec<_> = reservers
            .into_iter()
            .map(|h| h.join().expect("reserver thread panicked"))
            .collect();
        for h in releasers {
            h.join().expect("releaser thread panicked");
        }

        for (counter, waiter) in parked.into_iter().flatten() {
            assert!(
                counter.0.load(Ordering::SeqCst) >= 1,
                "a reserver that parked must be woken by a later release",
            );
            if let Some(id) = waiter {
                shared.memory_limit.cancel_waiter(id);
            }
        }
        assert_eq!(
            shared.memory_limit.used_bytes(),
            0,
            "budget bookkeeping must balance back to zero after each iteration",
        );
        assert_eq!(shared.memory_limit.parked_waiters(), 0);
    }
}

/// `cancel_waiter` is idempotent and owner-safe: cancelling a registration
/// that a concurrent release already drained, or cancelling it twice, never
/// disturbs any other registration.
#[test]
fn cancel_waiter_is_idempotent_under_contention() {
    const ITERS: usize = 100;
    const LIMIT: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        let full = shared.memory_limit.try_reserve(LIMIT).expect("saturate");

        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter);
        let mut waiter = None;
        assert!(
            shared
                .memory_limit
                .poll_reserve(128, &mut waiter, &waker)
                .is_pending(),
            "must park with the budget full",
        );
        let id = waiter.expect("parked");

        let s = Arc::clone(&shared);
        let release_handle = thread::spawn(move || drop(full));
        let cancel_handle = thread::spawn(move || s.memory_limit.cancel_waiter(id));
        release_handle.join().unwrap();
        cancel_handle.join().unwrap();

        // Cancel again — must be a no-op.
        shared.memory_limit.cancel_waiter(id);
        assert_eq!(shared.memory_limit.parked_waiters(), 0);

        // A fresh reservation succeeds against the now-empty budget.
        let counter2 = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker2 = std::task::Waker::from(counter2);
        let mut fresh = None;
        assert!(
            shared
                .memory_limit
                .poll_reserve(64, &mut fresh, &waker2)
                .is_ready(),
            "fresh reserve against empty budget must succeed",
        );
        assert_eq!(shared.memory_limit.used_bytes(), 0);
    }
}

/// A release drains EVERY parked reserver exactly once (Java
/// `MemoryLimitController` signals all waiters on the downward crossing).
#[test]
fn release_wakes_every_parked_reserver() {
    const RESERVERS: usize = 8;
    const LIMIT: u64 = 256;
    const PAYLOAD: u64 = 128;

    let shared = shared(LIMIT);
    let full = shared.memory_limit.try_reserve(LIMIT).expect("saturate");

    let counters: Vec<_> = (0..RESERVERS)
        .map(|_| Arc::new(CountingWaker(AtomicUsize::new(0))))
        .collect();

    let mut waiters = Vec::with_capacity(RESERVERS);
    for counter in &counters {
        let waker = std::task::Waker::from(counter.clone());
        let mut waiter = None;
        assert!(
            shared
                .memory_limit
                .poll_reserve(PAYLOAD, &mut waiter, &waker)
                .is_pending(),
            "must park because budget is saturated",
        );
        waiters.push(waiter.expect("parked"));
    }
    assert_eq!(shared.memory_limit.parked_waiters(), RESERVERS);

    drop(full);

    for counter in &counters {
        assert_eq!(
            counter.0.load(Ordering::Acquire),
            1,
            "every parked reserver is woken exactly once",
        );
    }
    assert_eq!(shared.memory_limit.parked_waiters(), 0);
    // Stale ids: cancelling them is a no-op.
    for id in waiters {
        shared.memory_limit.cancel_waiter(id);
    }
}

/// The reservation compare-and-swap never over-admits under contention:
/// many threads reserving and releasing single bytes against a tiny budget
/// retry each other's lost races, yet the reservations alive at any instant
/// never exceed the limit and the counter returns to zero.
#[test]
fn concurrent_reservations_never_exceed_the_limit() {
    const THREADS: usize = 8;
    const ITERS: usize = 20_000;
    const LIMIT: u64 = 4;

    let shared = shared(LIMIT);
    let live = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(std::sync::Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            let s = Arc::clone(&shared);
            let live = Arc::clone(&live);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                let mut admitted = 0usize;
                for _ in 0..ITERS {
                    if let Ok(reservation) = s.memory_limit.try_reserve(1) {
                        let now_live = live.fetch_add(1, Ordering::SeqCst) + 1;
                        assert!(
                            now_live as u64 <= LIMIT,
                            "{now_live} live reservations exceed the {LIMIT} B budget"
                        );
                        admitted += 1;
                        live.fetch_sub(1, Ordering::SeqCst);
                        drop(reservation);
                    }
                }
                admitted
            })
        })
        .collect();
    let admitted: usize = workers
        .into_iter()
        .map(|w| w.join().expect("worker thread panicked"))
        .sum();
    assert!(admitted > 0, "the budget admitted nothing");
    assert_eq!(shared.memory_limit.used_bytes(), 0);
}
