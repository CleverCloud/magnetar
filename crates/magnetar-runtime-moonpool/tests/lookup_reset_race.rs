// SPDX-License-Identifier: Apache-2.0

//! Moonpool mirror of
//! `magnetar-runtime-tokio/tests/lookup_reset_race.rs`.
//!
//! Pins the ADR-0024 cross-runtime parity contract for the lookup
//! multi-agent review HIGH-3 fix: every supervised-reset boundary
//! surfaces `OpOutcome::SessionLost` on every in-flight lookup +
//! partitioned-metadata request synchronously, regardless of which
//! engine drives the connection. The moonpool engine's virtual clock
//! lets us assert the "no 30-second wait" property structurally —
//! no host-clock `Instant::now()` is consulted; `reset()` runs and
//! the outcome is observable immediately, **at the same synthetic
//! tick**.
//!
//! Strategy (per moonpool-side `tests/common/mod.rs` doc): drive
//! [`ConnectionShared`] directly, no driver task, no TCP listener,
//! all timestamps synthetic.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::Instant;

use magnetar_proto::{OpOutcome, PendingOpKey};

mod common;
use common::handshake_complete_shared;

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWake>, Waker) {
    let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker: Waker = Arc::clone(&inner).into();
    (inner, waker)
}

/// Lookup multi-agent review HIGH-3, moonpool side: an in-flight
/// lookup + partitioned-metadata pair must both observe
/// [`OpOutcome::SessionLost`] on the supervised-reset boundary, at
/// the **same** synthetic instant the reset fires.
///
/// Determinism property: the virtual clock never advances between
/// the `reset()` call and the `take_outcome()` calls — if the proto
/// layer dropped the outcome publish, the test would fail
/// deterministically across every seed. This is the lever ADR-0024's
/// moonpool seed sweep pulls.
#[test]
fn reset_surfaces_session_lost_on_in_flight_lookup_and_partition() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let (lookup_rid, partition_rid, lookup_counter, partition_counter) = {
        let mut conn = shared.inner.lock();
        let lookup_rid = conn.lookup("persistent://public/default/foo", false);
        let partition_rid = conn.get_partitioned_topic_metadata("persistent://public/default/bar");
        let (lookup_counter, lookup_waker) = counting_waker();
        let (partition_counter, partition_waker) = counting_waker();
        conn.register_waker(PendingOpKey::Request(lookup_rid), lookup_waker);
        conn.register_waker(PendingOpKey::Request(partition_rid), partition_waker);
        (lookup_rid, partition_rid, lookup_counter, partition_counter)
    };

    shared.inner.lock().reset();

    assert_eq!(
        lookup_counter.0.load(Ordering::SeqCst),
        1,
        "lookup waker must fire exactly once on reset"
    );
    assert_eq!(
        partition_counter.0.load(Ordering::SeqCst),
        1,
        "partitioned-metadata waker must fire exactly once on reset"
    );

    {
        let mut conn = shared.inner.lock();
        let lookup_key = PendingOpKey::Request(lookup_rid);
        let partition_key = PendingOpKey::Request(partition_rid);
        match conn.take_outcome(lookup_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, lookup_key),
            other => panic!("expected SessionLost on lookup rid, got {other:?}"),
        }
        match conn.take_outcome(partition_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, partition_key),
            other => panic!("expected SessionLost on partitioned-metadata rid, got {other:?}"),
        }
    }
}

/// Companion to the test above: even when the user's future never
/// registered a waker before `reset` (e.g. the executor never got to
/// `poll` the future before the supervisor kicked in), the outcome
/// MUST still be installed so the eventual `poll` sees `SessionLost`
/// instead of registering a fresh waker that will never fire.
/// Pairs 1:1 with the tokio sibling for ADR-0024 strict-parity.
#[test]
fn reset_publishes_outcome_even_when_no_waker_was_registered() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let lookup_rid = {
        let mut conn = shared.inner.lock();
        conn.lookup("persistent://public/default/no-waker", false)
    };

    shared.inner.lock().reset();

    let key = PendingOpKey::Request(lookup_rid);
    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
        other => panic!("expected SessionLost outcome, got {other:?}"),
    }
}
