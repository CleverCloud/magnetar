// SPDX-License-Identifier: Apache-2.0

//! Backoff-persistence policy — moonpool engine integration coverage.
//!
//! Mirror of
//! `crates/magnetar-runtime-tokio/tests/supervisor_backoff_persistence.rs`.
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024.
//!
//! End-to-end timing of the persisted schedule under a fake broker that
//! drops the socket inside `drop_grace` of every successful re-attach is
//! covered by the chaos sweep in
//! `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` (the
//! `DropsTcpAfterCreate` workload). This file focuses on the runtime-side
//! wiring of [`SupervisorConfig::should_reset_backoff`] + the
//! [`ConnectionShared`] round-trip.

use std::time::Duration;

use magnetar_proto::{Backoff, ConnectionConfig, SupervisorConfig};
use magnetar_runtime_moonpool::ConnectionShared;

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
    let cfg = supervisor_with_grace(Duration::from_millis(500));
    let mut backoff: Backoff = cfg.build_backoff(1);

    let mut delays = Vec::with_capacity(8);
    for _ in 0..8 {
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
    let cfg = supervisor_with_grace(Duration::from_millis(500));
    let mut backoff = cfg.build_backoff(1);

    for _ in 0..6 {
        if cfg.should_reset_backoff(Duration::from_millis(5)) {
            backoff.reset();
        }
        let _ = backoff.next();
    }
    if cfg.should_reset_backoff(Duration::from_secs(2)) {
        backoff.reset();
    }
    let post_reset = backoff.next();
    assert!(
        post_reset <= Duration::from_millis(100),
        "schedule must collapse back to initial after a stable socket, got {post_reset:?}"
    );
}
