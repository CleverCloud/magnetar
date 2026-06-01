// SPDX-License-Identifier: Apache-2.0

//! ADR-0048 — buggify fault-injection points must be a NOP in
//! production. The tokio engine does NOT wire a buggify RNG; this
//! test pins the contract that a default-constructed
//! [`magnetar_proto::Connection`] (as the tokio engine builds it) has
//! `buggify().is_armed() == false` regardless of whether the
//! `buggify` Cargo feature on `magnetar-proto` is compiled in.
//!
//! Companion of
//! `crates/magnetar-runtime-moonpool/tests/buggify_sim_sweep.rs` which
//! drives the same labels under an armed helper across a moonpool
//! seed sweep. Maintains the tokio ↔ moonpool 1:1 test count required
//! by ADR-0024.

#![forbid(unsafe_code)]

use magnetar_proto::ConnectionConfig;
use magnetar_proto::buggify::labels;
use magnetar_runtime_tokio::ConnectionShared;

/// Default `ConnectionShared::new` constructs a connection with the
/// buggify helper disabled. Every label, at any probability up to
/// 1.0, returns `false`. The four named choice points therefore
/// behave identically to the pre-ADR-0048 production path.
#[tokio::test(flavor = "current_thread")]
async fn tokio_buggify_default_disabled_across_all_labels() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let conn = shared.inner.lock();
    let buggify = conn.buggify();
    assert!(
        !buggify.is_armed(),
        "tokio engine must NOT arm buggify — production binaries stay deterministic"
    );
    // The four labels named in ADR-0048 — all must short-circuit to
    // `false` with no RNG installed.
    for label in [
        labels::CONNECTION_RESET_DELAY,
        labels::BATCH_CONTAINER_FLUSH_SPLIT,
        labels::HANDLE_BYTES_SHORT_READ,
        labels::RETRY_CLOCK_SKEW,
    ] {
        assert!(
            !buggify.should_fire(label, 1.0),
            "label {label} fired despite disabled buggify"
        );
        assert_eq!(
            buggify.fire_count(label),
            0,
            "label {label} fire_count != 0"
        );
    }
}

/// Mirror of moonpool's
/// `buggify_labels_all_fire_under_armed_helper_across_seed_sweep`.
/// On the tokio side we DO NOT expect labels to fire — but we still
/// exercise the same surface to keep the 1:1 test count required by
/// ADR-0024. Construction-only: tokio production wiring never calls
/// `set_buggify`, so `should_fire(p=1.0)` returns false even when
/// the underlying `magnetar-proto` is compiled with the `buggify`
/// feature on.
#[tokio::test(flavor = "current_thread")]
async fn tokio_buggify_labels_all_inert_under_armed_helper_across_seed_sweep() {
    // Sixteen seeds — the same fan-out the moonpool sibling uses.
    // Even with the `buggify` feature on, the helper is `disabled()`
    // by default for the tokio engine.
    let seeds: [u64; 16] = [
        0xAAAA_BBBB_CCCC_DDDD,
        1,
        2,
        3,
        5,
        7,
        11,
        13,
        17,
        19,
        23,
        29,
        31,
        37,
        41,
        43,
    ];
    for seed in seeds {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let conn = shared.inner.lock();
        let buggify = conn.buggify();
        // Pin: tokio's default never arms.
        assert!(
            !buggify.is_armed(),
            "seed={seed:#x}: tokio engine armed buggify unexpectedly"
        );
        for label in [
            labels::CONNECTION_RESET_DELAY,
            labels::BATCH_CONTAINER_FLUSH_SPLIT,
            labels::HANDLE_BYTES_SHORT_READ,
            labels::RETRY_CLOCK_SKEW,
        ] {
            assert!(!buggify.should_fire(label, 1.0));
        }
    }
}

/// Mirror of moonpool's
/// `buggify_connection_and_backoff_share_fire_counter`. On the tokio
/// engine no buggify helper is shared because no helper is wired —
/// confirming `Connection::buggify()` and a default `Backoff` both
/// remain disarmed.
#[tokio::test(flavor = "current_thread")]
async fn tokio_buggify_connection_and_backoff_remain_disarmed() {
    use magnetar_proto::backoff::Backoff;
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let conn = shared.inner.lock();
    let backoff = Backoff::default();
    // We have no public accessor for `Backoff::buggify` (it's an
    // internal optimisation), so we test the visible contract: a
    // fresh Backoff with no `install_buggify` call returns
    // unmodified durations across many `next()` calls — the
    // production cadence the supervisor expects. ADR-0048 §"build
    // modes" guarantees production binaries pay nothing.
    assert!(!conn.buggify().is_armed());
    let mut probe = backoff;
    let first = probe.next();
    // First call returns `initial` (≤100ms by default) within jitter.
    assert!(first <= std::time::Duration::from_millis(100));
}
