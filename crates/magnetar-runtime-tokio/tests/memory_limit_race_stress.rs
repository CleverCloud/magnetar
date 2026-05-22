// SPDX-License-Identifier: Apache-2.0

//! Mirrors `magnetar-runtime-moonpool/tests/memory_limit_race_stress.rs`
//! 1:1 on the tokio engine. Same race scenarios, same assertions: the
//! tokio `ConnectionShared` implements the identical memory-limit waker
//! slab mechanics, and the lost-wakeup race window applies equally to
//! both engines. Keeping the test 1:1 satisfies ADR-0024's runtime test
//! parity gate and confirms both engines' helpers behave the same
//! under contention.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;
use std::thread;

use magnetar_proto::{ConnectionConfig, MemoryLimitPolicy};
use magnetar_runtime_tokio::ConnectionShared;

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

#[test]
fn memory_limit_race_recheck_path_under_contention() {
    const ITERS: usize = 200;
    const RESERVERS: usize = 4;
    const RELEASERS: usize = 4;
    const LIMIT: u64 = 1024;
    const PAYLOAD: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        shared
            .try_reserve_memory(LIMIT)
            .expect("initial saturation must succeed");

        let mut handles = Vec::with_capacity(RESERVERS + RELEASERS);

        for _ in 0..RESERVERS {
            let s = Arc::clone(&shared);
            handles.push(thread::spawn(move || {
                let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
                let waker = std::task::Waker::from(counter.clone());
                match s.try_reserve_memory_or_register(PAYLOAD, &waker) {
                    Ok(()) => {
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
                s.release_memory(PAYLOAD);
                let _ = s.try_reserve_memory(PAYLOAD);
            }));
        }

        for h in handles {
            h.join().expect("worker thread panicked");
        }

        shared.release_memory(LIMIT);
        assert_eq!(
            shared.memory_used.load(Ordering::Acquire),
            0,
            "budget bookkeeping must balance back to zero after each iteration",
        );
    }
}

#[test]
fn cancel_memory_waker_is_idempotent_under_contention() {
    const ITERS: usize = 100;
    const LIMIT: u64 = 256;

    for _ in 0..ITERS {
        let shared = shared(LIMIT);
        shared.try_reserve_memory(LIMIT).expect("saturate");

        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter);

        let key = shared
            .try_reserve_memory_or_register(128, &waker)
            .expect_err("must fail with budget full");

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

        shared.cancel_memory_waker(key);

        let counter2 = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker2 = std::task::Waker::from(counter2);
        shared
            .try_reserve_memory_or_register(64, &waker2)
            .expect("fresh reserve against empty budget must succeed");
        shared.release_memory(64);
    }
}

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

    let mut slab_keys = Vec::with_capacity(RESERVERS);
    for counter in &counters {
        let waker = std::task::Waker::from(counter.clone());
        let key = shared
            .try_reserve_memory_or_register(PAYLOAD, &waker)
            .expect_err("must park because budget is saturated");
        slab_keys.push(key);
    }

    shared.release_memory(LIMIT);

    let total_wakes: usize = counters.iter().map(|c| c.0.load(Ordering::Acquire)).sum();
    assert!(
        total_wakes >= 1,
        "at least one parked waker must fire on release (saw {total_wakes})",
    );

    for key in slab_keys {
        shared.cancel_memory_waker(key);
    }
}
