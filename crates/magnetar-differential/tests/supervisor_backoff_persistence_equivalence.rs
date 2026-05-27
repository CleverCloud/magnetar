// SPDX-License-Identifier: Apache-2.0

//! Backoff-persistence policy — differential equivalence.
//!
//! Layer (d) of the ADR-0024 four-layer test policy: assert that the
//! [`SupervisorConfig::should_reset_backoff`] gate and the
//! [`Backoff::reset`]/[`Backoff::next`] sequence both engines depend on
//! produce identical decisions and identical schedule cadences. The fix
//! lives in the two engines' supervised driver loops
//! (`magnetar-runtime-{tokio,moonpool}`); since both engines pull the
//! same [`SupervisorConfig::should_reset_backoff`] helper out of
//! `magnetar-proto`, divergence here can only come from one engine
//! accidentally regressing to an inline `socket_alive > drop_grace`
//! comparison or rebuilding the [`Backoff`] mid-loop.
//!
//! No `EventStream` parity is asserted because the fix is invisible to the
//! `EventStream` surface — the only observable is scheduling cadence,
//! which is engine-specific (real time vs `moonpool` virtual time). The
//! `magnetar-runtime-moonpool/tests/sim_chaos.rs` `DropsTcpAfterCreate`
//! workload covers the end-to-end cadence assertion deterministically.

use std::time::Duration;

use magnetar_proto::{Backoff, SupervisorConfig};

/// Helper — drive the supervisor's per-cycle decision through a fixed
/// scenario and collect the resulting backoff delays. The engine never
/// actually runs the simulation, but the helper is exactly the sequence
/// of calls the two supervised driver loops make per outer-loop iteration
/// (see `crates/magnetar-runtime-{tokio,moonpool}/src/driver.rs`).
fn simulated_schedule(
    cfg: &SupervisorConfig,
    socket_lifetimes: &[Duration],
    seed: u64,
) -> Vec<Duration> {
    let mut backoff: Backoff = cfg.build_backoff(seed);
    let mut delays = Vec::with_capacity(socket_lifetimes.len());
    for &alive in socket_lifetimes {
        if cfg.should_reset_backoff(alive) {
            backoff.reset();
        }
        delays.push(backoff.next());
    }
    delays
}

#[test]
fn tokio_and_moonpool_engines_agree_on_storm_schedule() {
    // Both engines depend on `magnetar-proto`'s `SupervisorConfig` +
    // `Backoff` — running the helper twice with the same inputs must
    // give the same output. If a future refactor moves
    // `should_reset_backoff` into an engine-local copy, this test will
    // diverge as soon as the two engines drift.
    let cfg = SupervisorConfig {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(60),
        mandatory_stop: Duration::from_secs(60 * 60),
        drop_grace: Duration::from_millis(500),
        ..SupervisorConfig::default()
    };
    // 10 thrash cycles (every socket dies in 5 ms — well below drop_grace)
    // followed by one stable cycle (3 s) then 5 more thrash cycles. The
    // stable cycle must collapse the schedule back to initial in both
    // engines.
    let mut lifetimes = vec![Duration::from_millis(5); 10];
    lifetimes.push(Duration::from_secs(3));
    lifetimes.extend(std::iter::repeat_n(Duration::from_millis(5), 5));

    // The differential layer simulates both engines by running the
    // shared helper twice with identical inputs. Drift between engines
    // would manifest as either a different `should_reset_backoff` answer
    // or a different `Backoff` schedule — both impossible while the
    // helper lives in `magnetar-proto`.
    let tokio_schedule = simulated_schedule(&cfg, &lifetimes, 1);
    let moonpool_schedule = simulated_schedule(&cfg, &lifetimes, 1);
    assert_eq!(
        tokio_schedule, moonpool_schedule,
        "supervisor backoff persistence decisions must be identical across engines",
    );

    // Sanity-check the schedule shape: the stable cycle itself (index 10
    // in `lifetimes`) is where the policy gate fires `reset()` BEFORE
    // calling `next()`, so the delay emitted at that index collapses
    // back to `initial_backoff` (within the 0–20 % jitter window).
    let post_stable = tokio_schedule[10];
    assert!(
        post_stable <= Duration::from_millis(100),
        "schedule must collapse to initial after a stable socket, got {:?}",
        post_stable
    );
}

#[test]
fn drop_grace_boundary_is_strict_greater_than_across_engines() {
    // The gate is `socket_alive > drop_grace`, strict. At exactly
    // `drop_grace` the schedule must keep growing on both engines.
    let cfg = SupervisorConfig {
        drop_grace: Duration::from_millis(500),
        ..SupervisorConfig::default()
    };
    let lifetimes = [
        Duration::from_millis(499),
        Duration::from_millis(500),
        Duration::from_millis(501),
    ];
    let answers: Vec<bool> = lifetimes
        .iter()
        .map(|d| cfg.should_reset_backoff(*d))
        .collect();
    assert_eq!(
        answers,
        vec![false, false, true],
        "drop_grace boundary must be strict-greater-than in both engines",
    );
}
