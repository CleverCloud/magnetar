// SPDX-License-Identifier: Apache-2.0

//! ADR-0097 — swarm configurations must be a pure, stable function of
//! the seed.
//!
//! The moonpool sim campaign draws a [`SwarmConfig`] per iteration from
//! `current_sim_seed()`; everything the daily sweep, the per-PR
//! `seed-replay` job, and local `MOONPOOL_SEED` triage rely on
//! (ADR-0036 / ADR-0047) collapses if the draw is not deterministic per
//! seed. These tests pin the determinism contract, the campaign
//! composition (reserved inclusive slice, non-empty subsets), and the
//! recorded-baseline shape the sub-seed-pinning regression tests force.
//!
//! Tokio twin: `crates/magnetar-runtime-tokio/tests/swarm_off_is_nop.rs`
//! (the production engine must ignore the whole surface), maintaining
//! the ADR-0024 1:1 runtime test count.

#![forbid(unsafe_code)]

use std::collections::HashSet;

use magnetar_runtime_moonpool::swarm::{SwarmConfig, SwarmSlot};

/// `config(seed)` is stable across draws: two independent derivations
/// of the same seed yield the same configuration AND a byte-identical
/// canonical config line. `MOONPOOL_SEED=<s>` must reproduce the same
/// subset every time (ADR-0097 replay contract).
#[test]
fn swarm_config_from_seed_is_stable_across_draws() {
    for seed in (0_u64..256).chain([u64::MAX, 0x4242_4242_4242_4242]) {
        let first = SwarmConfig::from_seed(seed);
        let second = SwarmConfig::from_seed(seed);
        assert_eq!(first, second, "seed {seed:#x}: draw is not stable");
        assert_eq!(
            first.to_string(),
            second.to_string(),
            "seed {seed:#x}: config line is not byte-identical"
        );
        assert_eq!(
            first.seed(),
            seed,
            "config must report the seed it was derived from"
        );
    }
}

/// The local validation sweep range (`MOONPOOL_SEED` ∈ 1..=32) must
/// explore configuration space: at least 8 distinct enabled-feature
/// sets across those seeds, per the ADR-0097 success criterion. The
/// canonical config line minus its seed field is the feature-set key.
#[test]
fn swarm_config_sweep_range_yields_distinct_feature_sets() {
    let feature_set = |seed: u64| {
        let c = SwarmConfig::from_seed(seed);
        let line = c.to_string();
        line.split_once(" slot=")
            .map(|(_, rest)| rest.to_owned())
            .expect("config line carries a slot field")
    };
    let distinct: HashSet<String> = (1_u64..=32).map(feature_set).collect();
    assert!(
        distinct.len() >= 8,
        "expected >= 8 distinct feature sets across seeds 1..=32, got {}: {distinct:?}",
        distinct.len()
    );
}

/// Campaign composition: about 1 in 4 seeds lands the inclusive
/// configuration (every feature on), the rest land proper swarm draws,
/// and NO seed produces an empty subset — an all-off draw collapses to
/// the inclusive configuration (the ISSTA 2012 non-empty-subset rule).
#[test]
fn swarm_config_reserves_full_slice_and_never_draws_empty() {
    const SAMPLE: u64 = 4096;
    let mut full = 0_u64;
    let mut swarm = 0_u64;
    for seed in 0..SAMPLE {
        let c = SwarmConfig::from_seed(seed);
        let features_on = (0..4).filter(|i| c.label_enabled(*i)).count()
            + usize::from(c.client_ack())
            + usize::from(c.client_close());
        assert!(features_on >= 1, "seed {seed:#x} drew an empty subset: {c}");
        match c.slot() {
            SwarmSlot::Full => {
                assert_eq!(features_on, 6, "full slot must enable every feature: {c}");
                assert_eq!(
                    c,
                    SwarmConfig::full(seed),
                    "a full-slice draw must equal the explicit inclusive config"
                );
                full += 1;
            }
            SwarmSlot::Swarm => swarm += 1,
            SwarmSlot::Baseline => panic!("from_seed never yields the baseline slot: {c}"),
        }
    }
    // The slice rule is 1-in-4 by hash; allow generous sampling slack
    // around 25% plus the all-off collapse (1/64 of swarm draws).
    let full_pct = full * 100 / SAMPLE;
    assert!(
        (15..=40).contains(&full_pct),
        "inclusive slice off-target: {full}/{SAMPLE} ({full_pct}%)"
    );
    assert!(swarm > 0, "no swarm draw in {SAMPLE} seeds");
}

/// The baseline shape is exactly the pre-swarm workload: ack on, close
/// absent, no buggify label armed, the historical 2x receive budget,
/// and a disabled buggify helper — what the sub-seed-pinning regression
/// tests force so their recorded trajectories stay byte-identical.
#[test]
fn swarm_config_baseline_matches_pre_swarm_shape() {
    let c = SwarmConfig::baseline(0xDEAD_BEEF);
    assert_eq!(c.slot(), SwarmSlot::Baseline);
    assert!(c.client_ack(), "baseline keeps the ack leg");
    assert!(!c.client_close(), "baseline has no close op");
    assert!(!c.any_label_enabled(), "baseline arms no buggify label");
    assert_eq!(c.receive_budget_multiplier(), 2);
    let helper = c.build_buggify(std::sync::Arc::new(|| 0_u64));
    assert!(
        !helper.is_armed(),
        "baseline must install the disabled helper so no choice point rolls"
    );
}
