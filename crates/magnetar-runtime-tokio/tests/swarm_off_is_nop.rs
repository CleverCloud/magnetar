// SPDX-License-Identifier: Apache-2.0

//! ADR-0097 — the swarm campaign surface must be a NOP in the
//! production engine.
//!
//! ADR-0097 gives [`magnetar_proto::ConnectionConfig`] a `buggify`
//! engine-arming slot that only the **moonpool** engine honours. This
//! file pins the tokio half of that contract, in the
//! `buggify_off_is_nop.rs` pattern: whatever the slot carries — even a
//! deliberately armed helper — the tokio engine's connections stay
//! disarmed, and the supervisor's backoff schedule keeps its production
//! cadence. Production binaries never see synthetic faults (ADR-0048).
//!
//! Moonpool twin: `crates/magnetar-runtime-moonpool/tests/swarm_config.rs`
//! (the sim engine's per-seed configuration draw), maintaining the
//! ADR-0024 1:1 runtime test count.

#![forbid(unsafe_code)]

use std::sync::Arc;

use magnetar_proto::buggify::labels;
use magnetar_proto::{Buggify, ConnectionConfig};
use magnetar_runtime_tokio::ConnectionShared;

/// Build a config whose buggify slot is as armed as the current feature
/// axis allows: under `--features buggify` this is a genuinely armed
/// helper with an all-labels filter; without the feature the same call
/// degrades to the zero-sized disabled helper. Either way the tokio
/// engine must end up disarmed.
fn config_with_hostile_slot() -> ConnectionConfig {
    ConnectionConfig {
        buggify: Buggify::with_rng_and_filter(
            Arc::new(|| 0_u64) as Arc<dyn Fn() -> u64 + Send + Sync>,
            Arc::new(|_label: &'static str| true)
                as Arc<dyn Fn(&'static str) -> bool + Send + Sync>,
        ),
        ..ConnectionConfig::default()
    }
}

/// The tokio engine ignores the ADR-0097 engine-arming slot entirely:
/// a connection constructed from a config carrying an armed helper is
/// still disarmed, and every ADR-0048 label short-circuits to `false`.
#[tokio::test(flavor = "current_thread")]
async fn tokio_engine_ignores_config_buggify_slot() {
    let shared = ConnectionShared::new(config_with_hostile_slot());
    let conn = shared.inner.lock();
    let buggify = conn.buggify();
    assert!(
        !buggify.is_armed(),
        "tokio engine must ignore the ConnectionConfig buggify slot"
    );
    for label in [
        labels::CONNECTION_RESET_DELAY,
        labels::BATCH_CONTAINER_FLUSH_SPLIT,
        labels::HANDLE_BYTES_SHORT_READ,
        labels::RETRY_CLOCK_SKEW,
    ] {
        assert!(!buggify.should_fire(label, 1.0));
        assert_eq!(buggify.fire_count(label), 0);
    }
}

/// The default config's slot is the disarmed helper at the tokio engine
/// boundary — a config that never touches the field cannot inject
/// faults through the new slot.
#[tokio::test(flavor = "current_thread")]
async fn tokio_default_config_slot_stays_disarmed() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    assert!(!shared.inner.lock().buggify().is_armed());
}

/// Cloning a config (the supervisor re-dial path clones the config for
/// every reconnect) does not conjure an armed helper on the tokio side:
/// the clone of a default slot is still disarmed.
#[tokio::test(flavor = "current_thread")]
async fn tokio_config_clone_keeps_slot_disarmed() {
    let config = ConnectionConfig::default();
    let cloned = config.clone();
    assert!(!cloned.buggify.is_armed());
    let shared = ConnectionShared::new(cloned);
    assert!(!shared.inner.lock().buggify().is_armed());
}

/// The tokio supervisor's backoff schedule is untouched by the swarm
/// surface: with no `install_buggify` call anywhere in the tokio
/// driver, two backoffs built from the same `SupervisorConfig` and seed
/// produce identical delay sequences — the production cadence, no
/// `retry_clock.skew` layer.
#[tokio::test(flavor = "current_thread")]
async fn tokio_backoff_schedule_unaffected_by_swarm_surface() {
    let cfg = magnetar_proto::SupervisorConfig::default();
    let mut a = cfg.build_backoff(7);
    let mut b = cfg.build_backoff(7);
    for step in 0..8 {
        let da = a.next();
        let db = b.next();
        assert_eq!(da, db, "step {step}: schedules diverged");
        assert!(
            da <= cfg.max_backoff,
            "step {step}: delay {da:?} exceeds configured max {:?} — a skew layer leaked in",
            cfg.max_backoff
        );
    }
}
