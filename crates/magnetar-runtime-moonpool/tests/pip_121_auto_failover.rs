// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: PIP-121 `AutoClusterFailover` (moonpool engine variant)
//! drives the [`ServiceUrlProvider`] surface from a synthetic
//! [`magnetar_proto::HealthProbe`] whose verdict flips on every tick.
//!
//! This is the moonpool counterpart to the tokio crate's unit tests on
//! [`magnetar_runtime_tokio::auto_cluster_failover::AutoClusterFailover`].
//! Both engines should observe the same failover / failback sequence
//! given the same probe verdict stream — the differential equivalence
//! test in `magnetar-differential/tests/auto_failover_equivalence.rs`
//! pins that assertion explicitly. This file pins the moonpool-engine
//! side in isolation so a moonpool-specific regression surfaces here
//! without dragging in the tokio engine.
//!
//! Why this is Moonpool territory: the probe loop is driven by
//! [`moonpool_core::TaskProvider::spawn_task`] and
//! [`moonpool_core::TimeProvider::sleep`].
//! `SimProviders` therefore drives it on Moonpool 0.8's native deterministic executor and virtual
//! clock, while this focused policy test uses `TokioProviders` on the production-shaped path.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use magnetar_proto::{HealthProbe, ServiceUrlProvider};
use magnetar_runtime_moonpool::auto_cluster_failover::AutoClusterFailover;
use moonpool_core::TokioProviders;

const PRIMARY: &str = "pulsar://primary:6650";
const STANDBY: &str = "pulsar://standby:6650";
/// Short tick so the test runs in real time without slowing the suite.
/// This test checks failover policy output on `TokioProviders`; deterministic-executor and virtual
/// clock behavior is covered by the `SimProviders` chaos suite.
const TICK: Duration = Duration::from_millis(40);

/// Synthetic probe whose verdict for the primary URL flips through a
/// scripted sequence on every probe call. Standby URLs always answer
/// healthy. Mirrors the `Flipping` probe in the tokio crate's unit
/// tests; lifted to integration scope so the moonpool task-provider
/// path is exercised end-to-end.
#[derive(Debug)]
struct ScriptedProbe {
    /// Verdict-per-tick script for the primary URL. Indexed by
    /// [`Self::primary_calls`]; over-reads clamp to the last entry.
    primary_script: Vec<bool>,
    /// Monotonic counter — bumped on every probe of the primary URL.
    primary_calls: AtomicUsize,
    /// Number of primary probes the test has explicitly permitted.
    allowed_primary_calls: AtomicUsize,
    /// Wake the provider-spawned task when the test permits its next probe.
    primary_waker: Mutex<Option<Waker>>,
}

impl ScriptedProbe {
    fn new(primary_script: Vec<bool>) -> Self {
        Self {
            primary_script,
            primary_calls: AtomicUsize::new(0),
            allowed_primary_calls: AtomicUsize::new(0),
            primary_waker: Mutex::new(None),
        }
    }

    fn allow_next_primary_probe(&self) {
        self.allowed_primary_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(waker) = self
            .primary_waker
            .lock()
            .expect("primary probe waker mutex poisoned")
            .take()
        {
            waker.wake();
        }
    }

    fn primary_call_count(&self) -> usize {
        self.primary_calls.load(Ordering::SeqCst)
    }
}

impl HealthProbe for ScriptedProbe {
    fn poll_probe(&self, endpoint: &str, _deadline: Instant, cx: &mut Context<'_>) -> Poll<bool> {
        if endpoint.contains("primary") {
            let completed = self.primary_calls.load(Ordering::SeqCst);
            if completed >= self.allowed_primary_calls.load(Ordering::SeqCst) {
                let mut primary_waker = self
                    .primary_waker
                    .lock()
                    .expect("primary probe waker mutex poisoned");
                let completed = self.primary_calls.load(Ordering::SeqCst);
                if completed >= self.allowed_primary_calls.load(Ordering::SeqCst) {
                    *primary_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }

            let idx = self.primary_calls.fetch_add(1, Ordering::SeqCst);
            let verdict = *self
                .primary_script
                .get(idx)
                .or_else(|| self.primary_script.last())
                .unwrap_or(&true);
            Poll::Ready(verdict)
        } else {
            Poll::Ready(true)
        }
    }
}

/// Drive the moonpool `AutoClusterFailover` probe loop across a
/// scripted verdict sequence and capture the active URL after each tick.
/// Asserts the sequence matches the expected failover / failback
/// trajectory the policy contract pins (first-healthy-wins, snap-on-tick).
#[tokio::test(flavor = "current_thread")]
async fn probe_loop_flips_active_url_in_sync_with_scripted_verdicts() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let providers = TokioProviders::new();
            // Tick 1: healthy → active=0 (primary).
            // Tick 2: unhealthy → active=1 (standby).
            // Tick 3: healthy again → active=0 (failback).
            // Tick 4: unhealthy → active=1.
            // Tick 5+: stuck unhealthy → active=1.
            let probe = Arc::new(ScriptedProbe::new(vec![true, false, true, false, false]));
            let failover = AutoClusterFailover::<TokioProviders>::new(
                vec![PRIMARY.to_owned(), STANDBY.to_owned()],
                probe.clone(),
            );

            let handle = failover.start(&providers, TICK);

            // Permit exactly one fresh primary probe per step, then wait for both the probe and
            // its active-index update. The timeout bounds failure duration; elapsed wall time is
            // not part of the success condition.
            let tick =
                |expected_primary_calls: usize, expected_active: usize, label: &'static str| {
                let f = &failover;
                let probe = Arc::clone(&probe);
                async move {
                    probe.allow_next_primary_probe();
                    tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            let probe_completed =
                                probe.primary_call_count() == expected_primary_calls;
                            let active_matches = f.active_index() == expected_active;
                            if probe_completed && active_matches {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "timed out waiting for primary probe {expected_primary_calls} with active index {expected_active}; observed {} calls and active index {}",
                            probe.primary_call_count(),
                            f.active_index(),
                        )
                    });
                    tracing::debug!(label, active = f.active_index(), "tick observed");
                }
            };

            // Tick 1: primary healthy.
            tick(1, 0, "tick-1").await;
            assert_eq!(failover.active_index(), 0);
            assert_eq!(failover.get_service_url(), PRIMARY);

            // Tick 2: primary unhealthy → switch to standby.
            tick(2, 1, "tick-2").await;
            assert_eq!(failover.active_index(), 1);
            assert_eq!(failover.get_service_url(), STANDBY);

            // Tick 3: primary healthy → switch back.
            tick(3, 0, "tick-3").await;
            assert_eq!(failover.active_index(), 0);

            // Tick 4: primary unhealthy → switch to standby.
            tick(4, 1, "tick-4").await;
            assert_eq!(failover.active_index(), 1);

            // Tick 5: primary still unhealthy → stays on standby.
            tick(5, 1, "tick-5").await;
            assert_eq!(failover.active_index(), 1);

            // moonpool main's `TaskProvider::JoinHandle` is an opaque
            // `must_use` future with no `abort()`; detaching the background
            // prober is now a plain drop (the `LocalSet` tears the spawned
            // task down when this future returns).
            drop(handle);
        })
        .await;
}
