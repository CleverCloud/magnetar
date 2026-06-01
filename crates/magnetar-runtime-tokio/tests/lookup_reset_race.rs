// SPDX-License-Identifier: Apache-2.0

//! Integration test for the lookup multi-agent review HIGH-3 finding:
//! a lookup or partitioned-metadata request that is in flight when
//! `Connection::reset` fires (e.g. supervised reconnect) MUST surface
//! `OpOutcome::SessionLost` to the user's future immediately. Without
//! the explicit drain in `reset`, the future stays parked on its
//! per-request waker until the runtime's `operation_timeout` (default
//! 30s) fires.
//!
//! Strategy (mirrors `reconnect_with_inflight.rs`): drive the tokio
//! engine's [`ConnectionShared`] directly — no TCP listener, no driver
//! task — and assert the outcome lands within the `take_outcome` slot
//! immediately after `reset`. That's the engine-surface contract the
//! production driver depends on; the wire-level smoke test for the
//! same race lives in `crates/magnetar/tests/e2e_lookup_reset_race.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::Instant;

use magnetar_proto::{ConnectionConfig, OpOutcome, PendingOpKey};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

fn handshake_complete(at: Instant) -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }
    shared
}

/// Lock-free counter waker — fires `wake_count.fetch_add(1)` each time
/// the proto layer wakes the future, so we can assert the synchronous
/// wake-up contract on `reset`.
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

/// Lookup multi-agent review HIGH-3: an in-flight lookup +
/// partitioned-metadata pair must both observe
/// [`OpOutcome::SessionLost`] on the supervised-reset boundary,
/// **synchronously**. The runtime's `RequestFut` would
/// otherwise sit on its waker until the engine's `operation_timeout`
/// fires (the production default is 30s); the fix in
/// `magnetar-proto`'s `Connection::reset` publishes the outcome +
/// wakes the waker before the registry is cleared, so the future's
/// next `take_outcome` poll resolves immediately.
#[test]
fn reset_surfaces_session_lost_on_in_flight_lookup_and_partition() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);

    // Issue two in-flight registry requests. The runtime's
    // `lookup_topic` / `get_partitioned_topic_metadata` calls allocate
    // exactly these keys against `pending_requests` + the lookup
    // registry; we register a waker the same way the runtime's
    // `RequestFut::poll` does.
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

    // Trigger the supervised-reconnect boundary.
    shared.inner.lock().reset();

    // (1) Both wakers fired exactly once — the runtime's executor will
    // schedule the futures' next poll, which sees `SessionLost`.
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

    // (2) `OpOutcome::SessionLost` is published synchronously — there
    // is no 30-second wait for `operation_timeout` to fire.
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
/// registered a waker before `reset` (e.g. the future was constructed
/// but the runtime never got to `poll` it before the supervisor
/// kicked in), the outcome MUST still be installed so the eventual
/// `poll` sees `SessionLost` rather than parking on a freshly-
/// registered waker. Mirrors the same invariant on the moonpool side.
#[test]
fn reset_publishes_outcome_even_when_no_waker_was_registered() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);

    let lookup_rid = {
        let mut conn = shared.inner.lock();
        conn.lookup("persistent://public/default/no-waker", false)
    };

    // No `register_waker` call — simulates the runtime constructing
    // the future but not having polled it yet.
    shared.inner.lock().reset();

    // The outcome MUST be present so the eventual poll sees it on its
    // first `take_outcome` call rather than registering a waker that
    // will never fire.
    let key = PendingOpKey::Request(lookup_rid);
    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
        other => panic!("expected SessionLost outcome, got {other:?}"),
    }
}
