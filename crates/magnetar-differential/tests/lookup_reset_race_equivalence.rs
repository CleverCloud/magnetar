// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d) differential equivalence for the lookup
//! multi-agent review HIGH-3 fix: tokio and moonpool engines MUST
//! surface byte-identical [`OpOutcome::SessionLost`] outcomes (and
//! identical wake counts) when `Connection::reset` is called with a
//! lookup + partitioned-metadata pair in flight.
//!
//! The trace/Op harness in [`magnetar_differential`] does not (yet)
//! expose `reset` as an [`Op`](magnetar_differential::Op) variant —
//! supervised reconnect is an engine-internal action, not a user
//! operation — so this test drives both engines'
//! [`ConnectionShared`] wrappers directly and compares the resulting
//! outcome stream. Same shape as
//! [`crates/magnetar-runtime-tokio/tests/lookup_reset_race.rs`] +
//! [`crates/magnetar-runtime-moonpool/tests/lookup_reset_race.rs`],
//! but the assertion is the equivalence claim (tokio outcomes ==
//! moonpool outcomes), not the per-engine `SessionLost` invariant.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, OpOutcome, PendingOpKey, RequestId, encode_command, pb};

/// Build a synthetic `CommandConnected` frame matching both engines'
/// handshake expectations. Mirrors the per-engine helpers in
/// `tests/common/mod.rs` on each side; duplicated here to keep the
/// differential test self-contained (the harness has no shared test
/// helper between the two engine crates).
fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-differential".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

/// Counting waker that tracks per-engine wake invocations. The
/// equivalence claim asserts both engines fire each waker the same
/// number of times across the reset boundary.
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

/// Per-engine outcome capture: the engine-visible state after `reset`
/// fires on an in-flight lookup + partitioned-metadata pair.
///
/// The tuple is the harness's "event stream" for this scenario:
/// (waker fires on lookup, waker fires on partition, lookup outcome
/// shape, partition outcome shape). Both engines MUST agree on every
/// component — that's the ADR-0024 layer-(d) parity claim.
#[derive(Debug, PartialEq, Eq)]
struct ResetSnapshot {
    lookup_wake_count: usize,
    partition_wake_count: usize,
    lookup_outcome_is_session_lost: bool,
    partition_outcome_is_session_lost: bool,
    lookup_outcome_key: PendingOpKey,
    partition_outcome_key: PendingOpKey,
}

fn run_tokio_scenario() -> ResetSnapshot {
    use magnetar_runtime_tokio::ConnectionShared;

    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }

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

    let mut conn = shared.inner.lock();
    let lookup_key = PendingOpKey::Request(lookup_rid);
    let partition_key = PendingOpKey::Request(partition_rid);
    let lookup_outcome = conn.take_outcome(lookup_key);
    let partition_outcome = conn.take_outcome(partition_key);
    drop(conn);

    let (lookup_session_lost, lookup_key_observed) = match lookup_outcome {
        Some(OpOutcome::SessionLost { key }) => (true, key),
        _ => (false, PendingOpKey::Request(RequestId(u64::MAX))),
    };
    let (partition_session_lost, partition_key_observed) = match partition_outcome {
        Some(OpOutcome::SessionLost { key }) => (true, key),
        _ => (false, PendingOpKey::Request(RequestId(u64::MAX))),
    };

    ResetSnapshot {
        lookup_wake_count: lookup_counter.0.load(Ordering::SeqCst),
        partition_wake_count: partition_counter.0.load(Ordering::SeqCst),
        lookup_outcome_is_session_lost: lookup_session_lost,
        partition_outcome_is_session_lost: partition_session_lost,
        lookup_outcome_key: lookup_key_observed,
        partition_outcome_key: partition_key_observed,
    }
}

fn run_moonpool_scenario() -> ResetSnapshot {
    use magnetar_runtime_moonpool::ConnectionShared;

    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }

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

    let mut conn = shared.inner.lock();
    let lookup_key = PendingOpKey::Request(lookup_rid);
    let partition_key = PendingOpKey::Request(partition_rid);
    let lookup_outcome = conn.take_outcome(lookup_key);
    let partition_outcome = conn.take_outcome(partition_key);
    drop(conn);

    let (lookup_session_lost, lookup_key_observed) = match lookup_outcome {
        Some(OpOutcome::SessionLost { key }) => (true, key),
        _ => (false, PendingOpKey::Request(RequestId(u64::MAX))),
    };
    let (partition_session_lost, partition_key_observed) = match partition_outcome {
        Some(OpOutcome::SessionLost { key }) => (true, key),
        _ => (false, PendingOpKey::Request(RequestId(u64::MAX))),
    };

    ResetSnapshot {
        lookup_wake_count: lookup_counter.0.load(Ordering::SeqCst),
        partition_wake_count: partition_counter.0.load(Ordering::SeqCst),
        lookup_outcome_is_session_lost: lookup_session_lost,
        partition_outcome_is_session_lost: partition_session_lost,
        lookup_outcome_key: lookup_key_observed,
        partition_outcome_key: partition_key_observed,
    }
}

/// Tokio + moonpool engines surface byte-identical outcome shapes
/// when `reset` fires on an in-flight lookup + partitioned-metadata
/// pair. The ADR-0024 layer-(d) equivalence claim for the lookup
/// multi-agent review HIGH-3 fix: a divergence between the two
/// engines here would mean one of them re-introduced the
/// "lookup parked until `operation_timeout`" race.
#[test]
fn reset_on_in_flight_lookup_pair_yields_identical_outcomes_across_engines() {
    let tokio_snapshot = run_tokio_scenario();
    let moonpool_snapshot = run_moonpool_scenario();

    assert_eq!(
        tokio_snapshot, moonpool_snapshot,
        "engine outcome snapshots diverged on the lookup/reset race scenario",
    );

    // Pin the absolute shape too — without this, both engines could
    // agree on "nothing happens", which is exactly the regression
    // HIGH-3 calls out.
    assert!(
        tokio_snapshot.lookup_outcome_is_session_lost,
        "both engines must publish SessionLost on the lookup rid"
    );
    assert!(
        tokio_snapshot.partition_outcome_is_session_lost,
        "both engines must publish SessionLost on the partitioned-metadata rid"
    );
    assert_eq!(
        tokio_snapshot.lookup_wake_count, 1,
        "both engines must wake the lookup waker exactly once"
    );
    assert_eq!(
        tokio_snapshot.partition_wake_count, 1,
        "both engines must wake the partitioned-metadata waker exactly once"
    );
}

/// ADR-0024 layer (d) for ADR-0060 / follow-ups §4.1: tokio and moonpool must
/// make the SAME engine-side decision after an in-flight lookup is severed by a
/// supervised reconnect (`OpOutcome::SessionLost`). The decision is owned by
/// `ConnectionShared::await_reconnect_or_terminal`, which returns
/// `Reconnected` (re-issue the lookup) once the connection is live again, or
/// `Terminal` (→ `PeerClosed`) once it `is_closed()` AND `no_driver` is
/// latched. A divergence here would mean one engine re-issues where the other
/// gives up — exactly the asymmetry ADR-0060's `ENGINE SYMMETRY HAZARD` calls out.
///
/// We capture the readiness decision for BOTH terminal states (transient
/// reconnect → `Reconnected`; supervisor-gave-up → `Terminal`) on each engine
/// and assert the pair is identical. The states are pre-set so the readiness
/// future returns on its first loop iteration — no driver pump is needed, which
/// keeps the equivalence claim deterministic.
#[derive(Debug, PartialEq, Eq)]
struct ReadinessSnapshot {
    /// Decision when the connection has re-handshaked back to `Connected` after
    /// the severing reset — must be `Reconnected` on both engines.
    after_reconnect_is_reconnected: bool,
    /// Decision when the connection is terminal (`Failed`) with `no_driver`
    /// latched — must be `Terminal` on both engines.
    after_terminal_is_terminal: bool,
}

async fn run_tokio_readiness_scenario() -> ReadinessSnapshot {
    use magnetar_runtime_tokio::{ConnectionShared, LookupReissueReadiness};

    // (a) Severed-then-reconnected: handshake → in-flight lookup → reset
    // (SessionLost) → re-handshake → Connected. Readiness == Reconnected.
    let reconnected = {
        let t0 = Instant::now();
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("connected");
            let _ = conn.poll_event();
            let _rid = conn.lookup("persistent://public/default/foo", false);
            conn.reset();
            // Supervisor re-handshakes the fresh socket.
            conn.begin_handshake().expect("re-handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("reconnected");
            let _ = conn.poll_event();
        }
        matches!(
            shared.await_reconnect_or_terminal().await,
            LookupReissueReadiness::Reconnected
        )
    };

    // (b) Severed-then-terminal: handshake → reset (SessionLost) → mark
    // disconnected (Failed = is_closed()) → latch no_driver (supervisor
    // gave up). Readiness == Terminal.
    let terminal = {
        let t0 = Instant::now();
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("connected");
            let _ = conn.poll_event();
            let _rid = conn.lookup("persistent://public/default/foo", false);
            conn.reset();
            conn.mark_disconnected();
        }
        shared.mark_no_driver();
        matches!(
            shared.await_reconnect_or_terminal().await,
            LookupReissueReadiness::Terminal
        )
    };

    ReadinessSnapshot {
        after_reconnect_is_reconnected: reconnected,
        after_terminal_is_terminal: terminal,
    }
}

async fn run_moonpool_readiness_scenario() -> ReadinessSnapshot {
    use magnetar_runtime_moonpool::{ConnectionShared, LookupReissueReadiness};

    let reconnected = {
        let t0 = Instant::now();
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("connected");
            let _ = conn.poll_event();
            let _rid = conn.lookup("persistent://public/default/foo", false);
            conn.reset();
            conn.begin_handshake().expect("re-handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("reconnected");
            let _ = conn.poll_event();
        }
        matches!(
            shared.await_reconnect_or_terminal().await,
            LookupReissueReadiness::Reconnected
        )
    };

    let terminal = {
        let t0 = Instant::now();
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            conn.handle_bytes(t0, &handshake_response_bytes())
                .expect("connected");
            let _ = conn.poll_event();
            let _rid = conn.lookup("persistent://public/default/foo", false);
            conn.reset();
            conn.mark_disconnected();
        }
        shared.mark_no_driver();
        matches!(
            shared.await_reconnect_or_terminal().await,
            LookupReissueReadiness::Terminal
        )
    };

    ReadinessSnapshot {
        after_reconnect_is_reconnected: reconnected,
        after_terminal_is_terminal: terminal,
    }
}

#[tokio::test]
async fn lookup_reissue_readiness_is_identical_across_engines() {
    let tokio_snapshot = run_tokio_readiness_scenario().await;
    let moonpool_snapshot = run_moonpool_readiness_scenario().await;

    assert_eq!(
        tokio_snapshot, moonpool_snapshot,
        "engine lookup-reissue readiness decisions diverged (ADR-0060)",
    );

    // Pin the absolute shape too — both engines must re-issue on a transient
    // reconnect and short-circuit to terminal once the driver is gone.
    assert!(
        tokio_snapshot.after_reconnect_is_reconnected,
        "both engines must return Reconnected once the session is live again — \
         the transparent-retry path"
    );
    assert!(
        tokio_snapshot.after_terminal_is_terminal,
        "both engines must return Terminal once is_closed() AND no_driver — the \
         PeerClosed short-circuit (composes with §5.1)"
    );
}
