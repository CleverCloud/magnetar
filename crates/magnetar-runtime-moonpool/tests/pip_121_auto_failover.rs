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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
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
}

impl ScriptedProbe {
    fn new(primary_script: Vec<bool>) -> Self {
        Self {
            primary_script,
            primary_calls: AtomicUsize::new(0),
        }
    }
}

impl HealthProbe for ScriptedProbe {
    fn poll_probe(&self, endpoint: &str, _deadline: Instant, _cx: &mut Context<'_>) -> Poll<bool> {
        if endpoint.contains("primary") {
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

            // Inline ticking. Each step waits one full `TICK` plus slack, then yields so the
            // provider-spawned probe loop can complete its `time.sleep` and `poll_fn` await
            // points before the assertion samples the active URL.
            let tick = |label: &'static str| {
                let f = &failover;
                async move {
                    // Sleep slightly longer than one TICK so the probe
                    // loop's sleep elapses, the loop body runs, and the
                    // next sleep is entered before we snapshot. Real
                    // time (not virtual) — see the TICK const for the
                    // rationale.
                    tokio::time::sleep(TICK + Duration::from_millis(10)).await;
                    tracing::debug!(label, active = f.active_index(), "tick observed");
                }
            };

            // Tick 1: primary healthy.
            tick("tick-1").await;
            assert_eq!(failover.active_index(), 0);
            assert_eq!(failover.get_service_url(), PRIMARY);

            // Tick 2: primary unhealthy → switch to standby.
            tick("tick-2").await;
            assert_eq!(failover.active_index(), 1);
            assert_eq!(failover.get_service_url(), STANDBY);

            // Tick 3: primary healthy → switch back.
            tick("tick-3").await;
            assert_eq!(failover.active_index(), 0);

            // Tick 4: primary unhealthy → switch to standby.
            tick("tick-4").await;
            assert_eq!(failover.active_index(), 1);

            // Tick 5: primary still unhealthy → stays on standby.
            tick("tick-5").await;
            assert_eq!(failover.active_index(), 1);

            // moonpool main's `TaskProvider::JoinHandle` is an opaque
            // `must_use` future with no `abort()`; detaching the background
            // prober is now a plain drop (the `LocalSet` tears the spawned
            // task down when this future returns).
            drop(handle);
        })
        .await;
}
