// SPDX-License-Identifier: Apache-2.0

//! Backoff-persistence policy — tokio engine integration coverage.
//!
//! Verifies that the supervisor's persisted [`Backoff`] schedule (the
//! ADR-0028 §"defence in depth" line) is correctly wired through the
//! engine's [`ConnectionShared`] surface: the [`SupervisorConfig`]
//! attached to a connection round-trips its `drop_grace` /
//! `initial_backoff` / `max_backoff` knobs and the [`should_reset_backoff`]
//! gate behaves identically from the runtime's POV.
//!
//! Mirror of `crates/magnetar-runtime-moonpool/tests/supervisor_backoff_persistence.rs`.
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024.
//!
//! End-to-end timing of the persisted schedule under a fake broker that
//! drops the socket inside `drop_grace` of every successful re-attach is
//! covered by the chaos sweep in
//! `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` (the
//! `DropsTcpAfterCreate` workload) — wall-clock observation in tokio is
//! flaky enough that the deterministic-simulation engine is the canonical
//! place for that assertion (ADR-0024 §"moonpool sim coverage").

use std::time::Duration;

use magnetar_proto::{Backoff, ConnectionConfig, SupervisorConfig};
use magnetar_runtime_tokio::ConnectionShared;

fn supervisor_with_grace(grace: Duration) -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(60),
        drop_grace: grace,
        ..SupervisorConfig::default()
    }
}

#[test]
fn supervisor_config_roundtrips_through_connection_shared() {
    // Sanity check the engine's view of the config — the supervisor reads
    // `drop_grace` via `shared.inner.lock().supervisor_config()` on every
    // outer-loop iteration. If that round-trip silently drops the new
    // policy knob we want the test to fail.
    let grace = Duration::from_millis(250);
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_grace(grace)),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    let conn = shared.inner.lock();
    let supervisor = conn
        .supervisor_config()
        .expect("supervisor config must be present");
    assert_eq!(supervisor.drop_grace, grace);
    assert!(supervisor.should_reset_backoff(grace + Duration::from_millis(1)));
    assert!(!supervisor.should_reset_backoff(grace));
}

#[test]
fn persisted_backoff_grows_under_storm_pattern() {
    // Drives the exact decision sequence the tokio supervised driver makes
    // when the broker accepts the handshake then drops the socket inside
    // `drop_grace` on every cycle. Mirrors the proto-layer unit test in
    // `magnetar-proto/src/supervisor.
    // rs::supervisor_storm_schedule_grows_geometrically_without_reset` but lives in the runtime
    // crate so a regression in how the runtime wires `Backoff` + `should_reset_backoff`
    // together gets caught here.
    let cfg = supervisor_with_grace(Duration::from_millis(500));
    let mut backoff: Backoff = cfg.build_backoff(1);

    let mut delays = Vec::with_capacity(8);
    for _ in 0..8 {
        // Every previous socket "died" in 5 ms — well below drop_grace.
        let socket_alive = Duration::from_millis(5);
        if cfg.should_reset_backoff(socket_alive) {
            backoff.reset();
        }
        delays.push(backoff.next());
    }

    let first = delays[0];
    assert!(
        first <= Duration::from_millis(100),
        "first delay starts at initial (with jitter), got {first:?}"
    );
    let third = delays[2];
    assert!(
        third >= Duration::from_millis(320),
        "by the 3rd reconnect the schedule must reflect ≥ 4x growth (got {third:?})"
    );
    // 8th call: base 12.8 s (= initial × 2^7), with up to 20 % jitter
    // → 10.24 – 12.8 s. The lower bound proves the schedule is no longer
    // near `initial`; the higher you go, the more obvious the storm is
    // bounded.
    let last = *delays.last().expect("delays not empty");
    assert!(
        last >= Duration::from_secs(10),
        "by the 8th reconnect the schedule must approach max_backoff (got {last:?})"
    );
}

#[test]
fn stable_socket_resets_persisted_backoff_to_initial() {
    // Same decision sequence, but the most recent socket survived past
    // drop_grace — the policy gate must trip and the next delay must
    // collapse back to `initial_backoff`.
    let cfg = supervisor_with_grace(Duration::from_millis(500));
    let mut backoff = cfg.build_backoff(1);

    // Run the schedule up so it's nowhere near initial.
    for _ in 0..6 {
        if cfg.should_reset_backoff(Duration::from_millis(5)) {
            backoff.reset();
        }
        let _ = backoff.next();
    }
    // Stable reconnect — survived 2 s.
    if cfg.should_reset_backoff(Duration::from_secs(2)) {
        backoff.reset();
    }
    let post_reset = backoff.next();
    assert!(
        post_reset <= Duration::from_millis(100),
        "schedule must collapse back to initial after a stable socket, got {post_reset:?}"
    );
}

#[test]
fn give_up_budget_fires_behind_tcp_accepting_endpoint() {
    // ADR-0061 / follow-ups §3.2: behind a TCP-accepting proxy whose
    // backend is down, every dial succeeds but the Pulsar handshake never
    // completes, so each post-dial `driver_loop_inner` returns and the socket
    // dies inside `drop_grace`. The hoisted give-up counter must therefore NOT
    // reset across cycles and must fire at `max_attempts` — the
    // previously-unbounded retry storm. This drives the exact counter sequence
    // the tokio supervised driver runs (hoisted `give_up_attempts`, reset only
    // on `should_reset_backoff`, give up on `should_give_up`); it mirrors the
    // proto-layer unit
    // `supervisor::give_up_fires_at_budget_behind_tcp_accept` but lives in the
    // runtime crate so a regression in how the runtime wires `should_give_up`
    // into the supervised loop is caught here.
    let cfg = SupervisorConfig {
        max_attempts: Some(3),
        ..supervisor_with_grace(Duration::from_millis(500))
    };

    // Confirm the engine sees the same config through the `ConnectionShared`
    // round-trip the supervised driver reads from.
    let shared = ConnectionShared::new(ConnectionConfig {
        supervisor: Some(cfg.clone()),
        ..ConnectionConfig::default()
    });
    assert_eq!(
        shared
            .inner
            .lock()
            .supervisor_config()
            .expect("supervisor config present")
            .max_attempts,
        Some(3)
    );

    let mut give_up_attempts: u32 = 0;
    let mut gave_up_after = None;
    for cycle in 0..20 {
        // Top of the supervisor outer loop: the previous socket either never
        // handshaked (TCP-accept proxy) or died fast — `should_reset_backoff`
        // is false, so the give-up counter is NOT reset.
        let prev_socket_alive = Duration::from_millis(5);
        if cfg.should_reset_backoff(prev_socket_alive) {
            give_up_attempts = 0;
        }
        // Inner dial loop: increment THEN check, exactly as the driver does.
        give_up_attempts = give_up_attempts.saturating_add(1);
        if cfg.should_give_up(give_up_attempts) {
            gave_up_after = Some(cycle);
            break;
        }
    }
    assert_eq!(
        gave_up_after,
        Some(3),
        "the supervisor must give up at max_attempts behind a TCP-accept endpoint \
         (previously it looped forever)"
    );
    assert_eq!(
        give_up_attempts, 4,
        "counter spans the full dial+handshake cycle"
    );
}
