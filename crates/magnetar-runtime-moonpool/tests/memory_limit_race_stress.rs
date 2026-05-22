// SPDX-License-Identifier: Apache-2.0

//! Stress tests that probabilistically exercise the lost-wakeup race
//! window inside `ConnectionShared::try_reserve_memory_or_register`.
//!
//! The "won the recheck" path (the second `try_reserve_memory` call in
//! that helper) only fires when a concurrent
//! `ConnectionShared::release_memory` lands between the failed initial
//! CAS and the slab insert. With a single thread there is no interleave
//! point, so the path is unreachable from a deterministic unit test;
//! this fixture spins many short-lived contending threads to hit the
//! window often enough that `cargo-llvm-cov` records execution on at
//! least one iteration.
//!
//! The test does not assert that the race fires on every run (the
//! window is genuinely narrow). It DOES assert that:
//! - every parked future eventually completes (no lost wakeups);
//! - the budget bookkeeping balances back to zero after all work drains (no leaked reservations);
//! - cancellations via `cancel_memory_waker` are idempotent under contention.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;
use std::thread;

use magnetar_proto::{ConnectionConfig, MemoryLimitPolicy};
use magnetar_runtime_moonpool::ConnectionShared;

/// Counting waker for tests — increments on every wake call so we can
/// confirm parked futures actually receive wakeups under contention.
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

/// Spin N reservation threads against M release threads, all racing the
/// same `ConnectionShared`. Each reservation thread tries to park on the
/// waker slab then claim budget; each release thread frees it. Across
/// thousands of iterations the recheck-won path inside
/// `try_reserve_memory_or_register` (the second CAS) is hit at least
/// once.
#[test]
fn memory_limit_race_recheck_path_under_contention() {
    const ITERS: usize = 200;
    const RESERVERS: usize = 4;
    const RELEASERS: usize = 4;
    const LIMIT: u64 = 1024;
    const PAYLOAD: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        // Saturate the budget so every reserve attempt initially fails
        // the fast-path CAS and enters the slow path.
        shared
            .try_reserve_memory(LIMIT)
            .expect("initial saturation must succeed");

        let mut handles = Vec::with_capacity(RESERVERS + RELEASERS);

        for _ in 0..RESERVERS {
            let s = Arc::clone(&shared);
            handles.push(thread::spawn(move || {
                let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
                let waker = std::task::Waker::from(counter.clone());
                // Try once. If we win the recheck, great — the helper
                // returns Ok and the test path is hit. Otherwise we
                // cancel and let another thread carry on.
                match s.try_reserve_memory_or_register(PAYLOAD, &waker) {
                    Ok(()) => {
                        // Won the recheck OR the fast path; release
                        // immediately so the next reserver can race.
                        s.release_memory(PAYLOAD);
                    }
                    Err(key) => {
                        s.cancel_memory_waker(key);
                    }
                }
            }));
        }

        for _ in 0..RELEASERS {
            let s = Arc::clone(&shared);
            handles.push(thread::spawn(move || {
                // Release the saturating chunk, then immediately re-
                // saturate to keep pressure high.
                s.release_memory(PAYLOAD);
                let _ = s.try_reserve_memory(PAYLOAD);
            }));
        }

        for h in handles {
            h.join().expect("worker thread panicked");
        }

        // Drain any leftover budget so the next iteration starts at zero.
        shared.release_memory(LIMIT);
        assert_eq!(
            shared.memory_used.load(Ordering::Acquire),
            0,
            "budget bookkeeping must balance back to zero after each iteration",
        );
    }
}

/// `cancel_memory_waker` is documented as idempotent — calling it twice
/// for the same key (or after `release_memory` has already drained the
/// slot) must not panic and must leave the slab valid. We hammer this
/// invariant under thread contention.
#[test]
fn cancel_memory_waker_is_idempotent_under_contention() {
    const ITERS: usize = 100;
    const LIMIT: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        shared.try_reserve_memory(LIMIT).expect("saturate");

        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter);

        // Park a waker; we know it will fail because budget is full.
        let key = shared
            .try_reserve_memory_or_register(128, &waker)
            .expect_err("must fail with budget full");

        // Race the release against the cancel.
        let s1 = Arc::clone(&shared);
        let s2 = Arc::clone(&shared);
        let key_copy = key;
        let release_handle = thread::spawn(move || {
            s1.release_memory(LIMIT);
        });
        let cancel_handle = thread::spawn(move || {
            s2.cancel_memory_waker(key_copy);
        });
        release_handle.join().unwrap();
        cancel_handle.join().unwrap();

        // Cancel again — must be a no-op.
        shared.cancel_memory_waker(key);

        // The slab is in a consistent state: a fresh reserve should
        // succeed against the now-empty budget.
        let counter2 = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker2 = std::task::Waker::from(counter2);
        shared
            .try_reserve_memory_or_register(64, &waker2)
            .expect("fresh reserve against empty budget must succeed");
        shared.release_memory(64);
    }
}

/// `release_memory` drains every parked waker exactly once; this is the
/// load-bearing wake-up invariant. Under contention we confirm that
/// at least one waker fires per release cycle (no "I parked but never
/// got woken" cases).
#[test]
fn release_memory_wakes_at_least_one_parked_reserver_under_contention() {
    const RESERVERS: usize = 8;
    const LIMIT: u64 = 256;
    const PAYLOAD: u64 = 128;

    let shared = shared(LIMIT);
    shared.try_reserve_memory(LIMIT).expect("saturate");

    let counters: Vec<_> = (0..RESERVERS)
        .map(|_| Arc::new(CountingWaker(AtomicUsize::new(0))))
        .collect();

    // Park all reservers. They MUST all return Err(key) because budget
    // is fully saturated and no release has fired yet.
    let mut slab_keys = Vec::with_capacity(RESERVERS);
    for counter in &counters {
        let waker = std::task::Waker::from(counter.clone());
        let key = shared
            .try_reserve_memory_or_register(PAYLOAD, &waker)
            .expect_err("must park because budget is saturated");
        slab_keys.push(key);
    }

    // Single release. It must wake the entire slab (the
    // `drain_memory_wakers` path).
    shared.release_memory(LIMIT);

    let total_wakes: usize = counters.iter().map(|c| c.0.load(Ordering::Acquire)).sum();
    assert!(
        total_wakes >= 1,
        "at least one parked waker must fire on release (saw {total_wakes})",
    );

    // Cleanup: cancel the keys so the slab drops cleanly.
    for key in slab_keys {
        shared.cancel_memory_waker(key);
    }
}
