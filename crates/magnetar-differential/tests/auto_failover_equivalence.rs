// SPDX-License-Identifier: Apache-2.0

//! Differential equivalence: PIP-121 [`AutoClusterFailover`] must produce
//! the **same** active-URL sequence on the tokio engine and the moonpool
//! engine when fed an identical probe-verdict script.
//!
//! Why this matters: the policy machinery is duplicated between
//! [`magnetar_runtime_tokio::auto_cluster_failover::AutoClusterFailover`]
//! and
//! [`magnetar_runtime_moonpool::auto_cluster_failover::AutoClusterFailover`].
//! Both implement the same Java [`AutoClusterFailover`] semantics (PIP-121)
//! through the sans-io [`magnetar_proto::HealthProbe`] trait. The harness
//! pins their observed sequences against each other so a future tweak
//! to either crate that diverges from the policy contract surfaces at
//! commit time (ADR-0024).
//!
//! Shape: drive both engines from a [`ScriptedProbe`] that returns the
//! same verdict for `(endpoint, tick)` regardless of which engine asks.
//! After every tick, snapshot both engines' `active_index()`. Assert the
//! two snapshot sequences match — every transition, in order.
//!
//! The probe loops run against the host wall-clock at a short tick;
//! both engines share the same [`tokio::time`] view inside the same
//! `LocalSet`. See [`TICK`] for the rationale on real-time vs virtual.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use moonpool_core::TokioProviders;

const PRIMARY: &str = "pulsar://primary:6650";
const STANDBY: &str = "pulsar://standby:6650";
/// Real-time tick — see the equivalent constants in the per-engine
/// integration tests. We intentionally avoid `tokio::time::pause` here
/// because the tokio and moonpool engines spawn their probe loops on
/// different task primitives (`tokio::spawn` vs `spawn_local`), and the
/// paused-clock + `advance` pattern wakes them with different timing
/// fidelity. Real time is honest, deterministic enough at this scale,
/// and lets the assertion focus on the policy contract rather than
/// scheduler behaviour.
const TICK: Duration = Duration::from_millis(40);

/// Probe whose primary verdict follows a scripted sequence; the standby
/// verdict is always healthy. Counts probes per *endpoint* (not
/// globally) so the two engines can independently iterate over their
/// URL lists. The script index reads modulo the script length so over
/// reads cycle rather than clamp — matters for tests that drive more
/// ticks than the script defines.
#[derive(Debug)]
struct ScriptedProbe {
    primary_script: Vec<bool>,
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
            let n = self.primary_script.len().max(1);
            let v = *self.primary_script.get(idx % n).unwrap_or(&true);
            Poll::Ready(v)
        } else {
            Poll::Ready(true)
        }
    }
}

/// Spin the tokio + moonpool failovers in lock-step against the same
/// scripted probe; assert they observe identical `active_index()`
/// trajectories. Each engine has its OWN scripted probe (so the call
/// counters don't bleed across engines), but the script vectors are
/// identical.
#[tokio::test(flavor = "current_thread")]
async fn tokio_and_moonpool_observe_same_active_index_sequence() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Verdict script — exercise primary-healthy, fail, recover,
            // re-fail, stay-failed.
            let script = vec![true, false, true, false, false];

            let tokio_probe = Arc::new(ScriptedProbe::new(script.clone()));
            let moonpool_probe = Arc::new(ScriptedProbe::new(script.clone()));

            let tokio_failover =
                magnetar_runtime_tokio::auto_cluster_failover::AutoClusterFailover::new(
                    vec![PRIMARY.to_owned(), STANDBY.to_owned()],
                    tokio_probe.clone(),
                );
            let moonpool_failover =
                magnetar_runtime_moonpool::auto_cluster_failover::AutoClusterFailover::<
                    TokioProviders,
                >::new(
                    vec![PRIMARY.to_owned(), STANDBY.to_owned()],
                    moonpool_probe.clone(),
                );

            let providers = TokioProviders::new();
            let tokio_handle = tokio_failover.start(TICK);
            let moonpool_handle = moonpool_failover.start(&providers, TICK);

            // Five ticks: snapshot both engines' active index per tick.
            let mut tokio_trace: Vec<usize> = Vec::with_capacity(5);
            let mut moonpool_trace: Vec<usize> = Vec::with_capacity(5);
            for _ in 0..5 {
                // Real-time sleep so both engines' probe loops actually
                // tick before we snapshot. See TICK const.
                tokio::time::sleep(TICK + Duration::from_millis(10)).await;
                tokio_trace.push(tokio_failover.active_index());
                moonpool_trace.push(moonpool_failover.active_index());
            }

            assert_eq!(
                tokio_trace, moonpool_trace,
                "engine active-index trajectories diverged:\n  \
                 tokio    = {tokio_trace:?}\n  moonpool = {moonpool_trace:?}",
            );

            // Sanity: the trace must reflect the script — at least one
            // failover and one failback should show up.
            assert!(
                tokio_trace.contains(&0) && tokio_trace.contains(&1),
                "trace must observe both active=0 and active=1: {tokio_trace:?}",
            );

            tokio_handle.abort();
            moonpool_handle.abort();
        })
        .await;
}
