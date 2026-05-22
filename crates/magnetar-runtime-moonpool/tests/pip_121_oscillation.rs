// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: PIP-121 cluster failover oscillates between primary and
//! secondary. A health probe declares the primary unhealthy → the failover
//! provider swaps to secondary; primary recovers → it swaps back. The
//! [`ServiceUrlProvider`] surface the supervised driver loop consults must
//! reflect every swap, in order, so the next reconnect attempt dials the
//! freshly-active broker.
//!
//! Why this is moonpool territory: `testcontainers` can spin up two
//! brokers and kill the primary, but it cannot deterministically schedule
//! the "primary recovers → swap back" leg of the cycle. A health probe
//! driven by virtual instants does. The moonpool engine's
//! `connect_plain_supervised` accepts a
//! `Arc<dyn ServiceUrlProvider>` exactly so a synthetic probe can drive
//! the URL slot from outside the runtime.
//!
//! `AutoClusterFailover` is now ALSO ported to the moonpool engine
//! ([`magnetar_runtime_moonpool::auto_cluster_failover::AutoClusterFailover`])
//! — its probe-loop dynamics are pinned by
//! `crates/magnetar-runtime-moonpool/tests/pip_121_auto_failover.rs`
//! and `crates/magnetar-differential/tests/auto_failover_equivalence.rs`.
//! This file remains focused on the [`ControlledClusterFailover`] slot
//! semantics: a test-owned probe loop drives URL swaps, and the URL slot
//! is what the supervised driver dials on every reconnect attempt.
//!
//! ## Shape
//!
//! 1. Construct a [`ControlledClusterFailover`] seeded with `primary`.
//! 2. Run a "probe" that toggles the URL on each tick: `primary → secondary → primary → secondary →
//!    primary`.
//! 3. After each toggle, snapshot the provider via [`ServiceUrlProvider::get_service_url`] and
//!    confirm we see every swap in order.
//! 4. Pin the invariant that the slot is *consistent* under concurrent reads (the supervisor calls
//!    `get_service_url` once per reconnect attempt — multiple in-flight `Arc` clones must see the
//!    same value after the swap).

use std::sync::Arc;

use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider};

const PRIMARY: &str = "pulsar://primary:6650";
const SECONDARY: &str = "pulsar://secondary:6650";

#[test]
fn controlled_failover_reflects_oscillating_health_probe() {
    let failover = ControlledClusterFailover::new(PRIMARY);
    let provider: Arc<dyn ServiceUrlProvider> = Arc::new(failover.clone());

    // Snapshot at every probe tick. The supervised driver loop polls
    // `get_service_url()` once per reconnect attempt, so this mirrors the
    // production sequence the loop would observe across an oscillating
    // health probe.
    let mut observed: Vec<String> = Vec::with_capacity(5);
    observed.push(provider.get_service_url());

    // Tick 1: primary failed → swap to secondary.
    failover.set_url(SECONDARY);
    observed.push(provider.get_service_url());

    // Tick 2: primary recovered → swap back.
    failover.set_url(PRIMARY);
    observed.push(provider.get_service_url());

    // Tick 3: primary failed again.
    failover.set_url(SECONDARY);
    observed.push(provider.get_service_url());

    // Tick 4: primary recovered.
    failover.set_url(PRIMARY);
    observed.push(provider.get_service_url());

    assert_eq!(
        observed,
        vec![
            PRIMARY.to_owned(),
            SECONDARY.to_owned(),
            PRIMARY.to_owned(),
            SECONDARY.to_owned(),
            PRIMARY.to_owned(),
        ],
        "every health-probe tick must be reflected in the next get_service_url() read",
    );
}

#[test]
fn controlled_failover_shares_slot_across_arc_clones() {
    // The supervised driver loop holds one `Arc<dyn ServiceUrlProvider>`;
    // user code may hold others (e.g. a control-plane sidecar that drives
    // `set_url` from a different task). The slot must be shared, so a
    // swap by any holder is visible to every other holder.
    let failover = ControlledClusterFailover::new(PRIMARY);
    let probe: Arc<dyn ServiceUrlProvider> = Arc::new(failover.clone());
    let supervisor: Arc<dyn ServiceUrlProvider> = Arc::new(failover.clone());

    assert_eq!(probe.get_service_url(), PRIMARY);
    assert_eq!(supervisor.get_service_url(), PRIMARY);

    // The probe handle swaps; the supervisor handle observes the swap on
    // its next read.
    failover.set_url(SECONDARY);
    assert_eq!(probe.get_service_url(), SECONDARY);
    assert_eq!(supervisor.get_service_url(), SECONDARY);

    // And back again.
    failover.set_url(PRIMARY);
    assert_eq!(probe.get_service_url(), PRIMARY);
    assert_eq!(supervisor.get_service_url(), PRIMARY);
}

#[test]
fn rapid_oscillation_preserves_final_value() {
    // 1000 ticks toggling primary/secondary. The supervised reconnect path
    // dials whatever the slot held at its read-time; the slot is a
    // `parking_lot::Mutex<String>` so reads and writes serialise. The
    // contract: after N writes, the slot holds the Nth value (no
    // tearing, no last-write-wins ambiguity).
    let failover = ControlledClusterFailover::new(PRIMARY);
    let mut expected = PRIMARY;
    for i in 0..1_000 {
        let target = if i % 2 == 0 { SECONDARY } else { PRIMARY };
        failover.set_url(target);
        expected = target;
    }
    assert_eq!(failover.current_url(), expected);
    assert_eq!(failover.get_service_url(), expected);
}
