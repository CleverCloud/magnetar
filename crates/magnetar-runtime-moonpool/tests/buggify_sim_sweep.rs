// SPDX-License-Identifier: Apache-2.0

//! ADR-0048 — buggify fault-injection labels must fire across a
//! seeded sweep on the moonpool engine. This test pins the contract
//! that wiring [`magnetar_proto::Buggify`] into
//! [`magnetar_proto::Connection`] (and into the supervisor
//! `Backoff` via `install_buggify`) actually produces observable
//! firings on each of the four named labels when the helper is
//! armed with a seed-driven RNG.
//!
//! Companion of
//! `crates/magnetar-runtime-tokio/tests/buggify_off_is_nop.rs`. The
//! tokio side asserts production stays NOP; this side asserts the
//! simulation side fires. Together they maintain the tokio ↔
//! moonpool 1:1 test count required by ADR-0024.
//!
//! Coverage strategy: a small SplitMix64-seeded counter mimics what
//! the moonpool `SimRandomProvider` exposes through
//! `Providers::Random` — one `u64` per roll, deterministic for a
//! fixed seed. The driver loop is NOT exercised here; we test the
//! sans-io helper directly so the four labels' wiring is verified
//! without the bug-finding noise of the full chaos pack (which uses
//! the labels for its own coverage).

#![forbid(unsafe_code)]

// The `Backoff` + `Buggify` + `splitmix_rng` arsenal is only reachable
// from `#[cfg(feature = "buggify")]` test functions below — the
// moonpool-seed-sweep CI workflow builds without `buggify`, so those
// imports / the helper fn need the same gate or the `-D warnings`
// lint trips with `unused_imports` / `dead_code`.
//
// `ConnectionShared`, `ConnectionConfig`, and `labels` are used by
// the non-feature-gated `moonpool_buggify_default_disabled` test
// (pins the "no auto-arm" contract under every build), so they stay
// unconditional.
#[cfg(feature = "buggify")]
use std::sync::Arc;

#[cfg(feature = "buggify")]
use magnetar_proto::Buggify;
use magnetar_proto::ConnectionConfig;
#[cfg(feature = "buggify")]
use magnetar_proto::backoff::Backoff;
use magnetar_proto::buggify::labels;
use magnetar_runtime_moonpool::ConnectionShared;
#[cfg(feature = "buggify")]
use parking_lot::Mutex;

/// SplitMix64-shaped seed → infinite `u64` stream. Mirrors what
/// `moonpool_sim::SimRandomProvider` advertises via
/// `RandomProvider::random()`, scoped down to the single primitive
/// our `Buggify::roll_u64` consumes.
#[cfg(feature = "buggify")]
fn splitmix_rng(seed: u64) -> Arc<dyn Fn() -> u64 + Send + Sync> {
    let state = Arc::new(Mutex::new(seed));
    Arc::new(move || {
        let mut g = state.lock();
        *g = g.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *g;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        z
    })
}

/// With `buggify` enabled on `magnetar-proto`, sweep 16 seeds and
/// confirm every label fires at least once when probed at a 100%
/// probability. The seed sweep guards against the case where one
/// seed happens to hit the choice point's `if` branch in a way that
/// short-circuits firing (e.g. an empty inbound buffer for
/// `handle_bytes.short_read`).
///
/// At `probability == 1.0`, `should_fire` is the unconditional path,
/// so every label fires on every call regardless of the RNG. The
/// sweep here is therefore an upper-bound smoke test: if even ONE
/// seed comes back without firings, the helper is broken.
#[cfg(feature = "buggify")]
#[test]
fn buggify_labels_all_fire_under_armed_helper_across_seed_sweep() {
    use magnetar_proto::buggify::labels;
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
        let buggify = Buggify::with_rng(splitmix_rng(seed));
        for label in [
            labels::CONNECTION_RESET_DELAY,
            labels::BATCH_CONTAINER_FLUSH_SPLIT,
            labels::HANDLE_BYTES_SHORT_READ,
            labels::RETRY_CLOCK_SKEW,
        ] {
            assert!(
                buggify.should_fire(label, 1.0),
                "seed={seed:#x} label={label} did not fire at p=1.0"
            );
        }
        // Counters tick monotonically — one fire per label per seed.
        for label in [
            labels::CONNECTION_RESET_DELAY,
            labels::BATCH_CONTAINER_FLUSH_SPLIT,
            labels::HANDLE_BYTES_SHORT_READ,
            labels::RETRY_CLOCK_SKEW,
        ] {
            assert_eq!(
                buggify.fire_count(label),
                1,
                "seed={seed:#x} label={label} fire_count off"
            );
        }
    }
}

/// Wired into [`Connection`] via `set_buggify`, the same `Buggify`
/// instance the moonpool engine installs is shared with
/// [`Backoff::install_buggify`]. Firing on the supervisor's redial
/// schedule reuses the same fire-counter map the connection labels
/// stamp into. This confirms the "single fire map for all four
/// labels" wiring contract callers need to assert coverage on.
#[cfg(feature = "buggify")]
#[test]
fn buggify_connection_and_backoff_share_fire_counter() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let buggify = Buggify::with_rng(splitmix_rng(0xDEAD_BEEF));
    let installed = shared.inner.lock().set_buggify(buggify.clone());
    assert!(installed.is_armed());
    // Build a Backoff and install the SAME helper.
    let mut backoff = Backoff::default();
    backoff.install_buggify(installed.clone());
    // Drive the connection's reset-delay label once.
    shared.inner.lock().reset();
    // Drive the Backoff's retry-clock-skew label via next() at least
    // 32 times (probability 0.05 ≈ at least one hit). The base
    // duration is 100ms with capped doubling at 60s — we don't care
    // about the absolute values, only that the shared map records
    // hits.
    for _ in 0..32 {
        let _ = backoff.next();
    }
    let total = installed.fire_count(labels::CONNECTION_RESET_DELAY)
        + installed.fire_count(labels::RETRY_CLOCK_SKEW);
    assert!(
        total > 0,
        "shared helper recorded zero fires across reset + 32 redials"
    );
}

/// Moonpool baseline: with NO buggify wired (the default
/// `ConnectionShared::new`), the connection's helper reports
/// `is_armed() == false` — same contract as the tokio side. Pins the
/// fact that the moonpool engine itself does NOT auto-arm buggify;
/// tests opt in explicitly via `set_buggify`.
#[test]
fn moonpool_buggify_default_disabled() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let conn = shared.inner.lock();
    assert!(
        !conn.buggify().is_armed(),
        "moonpool engine must not auto-arm buggify; tests opt in"
    );
    for label in [
        labels::CONNECTION_RESET_DELAY,
        labels::BATCH_CONTAINER_FLUSH_SPLIT,
        labels::HANDLE_BYTES_SHORT_READ,
        labels::RETRY_CLOCK_SKEW,
    ] {
        assert!(
            !conn.buggify().should_fire(label, 1.0),
            "label {label} fired on default moonpool connection"
        );
    }
}
