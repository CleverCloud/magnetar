// SPDX-License-Identifier: Apache-2.0

//! ADR-0048 / ADR-0024 layer 4 — tokio ↔ moonpool equivalence under
//! the default (no buggify) wiring.
//!
//! The buggify scaffolding (ADR-0048) introduces four named choice
//! points inside [`magnetar_proto::Connection`] plus an optional
//! skew layer in [`magnetar_proto::Backoff`]. Each path is gated on
//! [`magnetar_proto::Buggify`] reporting `should_fire == true`, and
//! the default helper (`Buggify::disabled`) returns `false` for every
//! label at every probability.
//!
//! This differential test pins the equivalence contract: **without
//! buggify wiring**, the tokio and moonpool engines emit identical
//! buggify state. Concretely both report:
//!
//! - `Connection::buggify().is_armed() == false`
//! - every label's `fire_count == 0`
//! - `should_fire(label, 1.0) == false` for every label
//!
//! The test does NOT drive any wire-protocol traffic — the production
//! Connection state machine reaches the four choice points only under
//! normal operation, and the existing differential traces already
//! cover that surface byte-for-byte. The contract pinned here is the
//! HELPER-level invariant: the tokio engine never auto-arms buggify,
//! and the moonpool engine ships the same default.

use magnetar_proto::ConnectionConfig;
use magnetar_proto::buggify::labels;

/// Tokio ↔ moonpool: both engines start with `Buggify::disabled`.
/// Every label collapses to `false` at every probability. The
/// fire-counter map stays empty. Pins the production contract from
/// both sides of the engine boundary.
#[test]
fn buggify_default_disabled_is_byte_identical_across_engines() {
    let tokio_shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let moonpool_shared =
        magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());

    let tokio_conn = tokio_shared.inner.lock();
    let moonpool_conn = moonpool_shared.inner.lock();

    // Both engines must agree on the armed state — both `false`.
    assert_eq!(
        tokio_conn.buggify().is_armed(),
        moonpool_conn.buggify().is_armed(),
        "engines disagree on default buggify arming"
    );
    assert!(!tokio_conn.buggify().is_armed());
    assert!(!moonpool_conn.buggify().is_armed());

    // Every label, every probability up to 1.0: the helper short-
    // circuits to `false` on both sides.
    for label in [
        labels::CONNECTION_RESET_DELAY,
        labels::BATCH_CONTAINER_FLUSH_SPLIT,
        labels::HANDLE_BYTES_SHORT_READ,
        labels::RETRY_CLOCK_SKEW,
    ] {
        for p in [0.05_f64, 0.5, 0.95, 1.0] {
            let tokio_fire = tokio_conn.buggify().should_fire(label, p);
            let moonpool_fire = moonpool_conn.buggify().should_fire(label, p);
            assert_eq!(
                tokio_fire, moonpool_fire,
                "engines diverged on {label}@p={p}: tokio={tokio_fire} moonpool={moonpool_fire}"
            );
            assert!(!tokio_fire);
        }
        // Fire counters stay at 0 on both sides — no firings means
        // the counter map is unmodified.
        assert_eq!(tokio_conn.buggify().fire_count(label), 0);
        assert_eq!(moonpool_conn.buggify().fire_count(label), 0);
    }
}

/// ADR-0097 — an armed helper whose per-label filter rejects every
/// label is observationally identical to `Buggify::disabled` across
/// BOTH engines. The moonpool engine honours the `ConnectionConfig`
/// buggify slot (so its helper reports armed under the `buggify`
/// feature) while the tokio engine ignores the slot entirely — an
/// intended asymmetry in wiring — yet the user-visible contract is
/// byte-identical: no label ever fires, no counter ever moves, and no
/// RNG is ever consumed (the filter short-circuits before the roll).
#[test]
fn buggify_all_labels_filtered_matches_disabled_across_engines() {
    use std::sync::Arc;
    let hostile = || magnetar_proto::ConnectionConfig {
        buggify: magnetar_proto::Buggify::with_rng_and_filter(
            Arc::new(|| 0_u64) as Arc<dyn Fn() -> u64 + Send + Sync>,
            Arc::new(|_label: &'static str| false)
                as Arc<dyn Fn(&'static str) -> bool + Send + Sync>,
        ),
        ..ConnectionConfig::default()
    };
    let tokio_shared = magnetar_runtime_tokio::ConnectionShared::new(hostile());
    let moonpool_shared = magnetar_runtime_moonpool::ConnectionShared::new(hostile());

    let tokio_conn = tokio_shared.inner.lock();
    let moonpool_conn = moonpool_shared.inner.lock();

    for label in [
        labels::CONNECTION_RESET_DELAY,
        labels::BATCH_CONTAINER_FLUSH_SPLIT,
        labels::HANDLE_BYTES_SHORT_READ,
        labels::RETRY_CLOCK_SKEW,
    ] {
        for p in [0.05_f64, 0.5, 0.95, 1.0] {
            let tokio_fire = tokio_conn.buggify().should_fire(label, p);
            let moonpool_fire = moonpool_conn.buggify().should_fire(label, p);
            assert_eq!(
                tokio_fire, moonpool_fire,
                "engines diverged on filtered {label}@p={p}"
            );
            assert!(!tokio_fire, "filtered label {label} fired at p={p}");
        }
        assert_eq!(tokio_conn.buggify().fire_count(label), 0);
        assert_eq!(moonpool_conn.buggify().fire_count(label), 0);
    }
}
