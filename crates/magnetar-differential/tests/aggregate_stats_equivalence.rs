// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `ConsumerStats::fold` parity for
//! `MultiTopicsConsumer::aggregate_stats` / `PartitionedConsumer::
//! aggregate_stats` (issue #347 — `aggregate_stats` zeroed the rate,
//! latency-percentile, and `pending_batch_acks` fields).
//!
//! `ConsumerStats::fold` is an engine-agnostic pure function of
//! `(ConsumerStats, Option<Histogram<u64>>)` tuples, so the differential
//! claim is that **the same delivery/pop sequence, driven through each
//! engine's own `ConnectionShared`, yields the same per-consumer snapshot
//! and therefore the same folded aggregate** — the sans-io state machine
//! both engines wrap behind a `parking_lot::Mutex` is the only thing that
//! could diverge.
//!
//! Latency is first-class in this comparison. Before ADR-0086 it could not
//! be: `ConsumerState::pop_message` recorded `Instant::elapsed()` (real
//! wall-clock time, not the synthetic `now` this test injects), so this file
//! carried a `seed_deterministic_latency` helper that OVERWROTE each
//! consumer's `receive_latency_hist` with a synthetic distribution before
//! snapshotting. That workaround is gone: `pop_message` now takes the
//! caller's `now`, so the real production pop path yields an exactly
//! reproducible distribution on both engines and the compared percentiles
//! come from the code under test.
//!
//! Honesty note on what this file does and does not prove: the cross-engine
//! `assert_eq!` alone would have been GREEN before ADR-0086 — both engines
//! share the same proto code and so agreed on the same wrong (~0 ms) value.
//! That is exactly the "parallel tests drift in lockstep" failure mode
//! ADR-0024 warns about, so the absolute assertions at the end of the test
//! (the folded percentiles equal the scripted pop offsets) are what make
//! this a regression test rather than a tautology.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, ConsumerStats, SubscribeRequest, encode_command,
    encode_payload, pb,
};

/// Two consumers with distinct delivery volumes so the totals genuinely sum
/// (rather than trivially doubling identical numbers). The 6/4 split is also
/// the latency-cluster split: every one of c1's messages contributes a
/// low-latency sample and every one of c2's a high-latency one, so the
/// merged distribution is a literal 60/40.
const C1_MESSAGES: u64 = 6;
const C2_MESSAGES: u64 = 4;

/// Scripted pop offsets: each consumer's messages are delivered at `t0` and
/// popped at `t0 + C{1,2}_LATENCY_MS`, so `pop_message` records exactly this
/// value per message (ADR-0086 — the sample is the injected `now -
/// arrived_at`, with no host-clock read anywhere in the path).
///
/// Mirrors the clean low/high split from the proto-level
/// `consumer_stats_fold_propagates_every_field` test: c1's cluster is all
/// low-latency, c2's is all high-latency, so the merged p50 lands in c1's
/// cluster and the merged p99 lands in c2's. Both `<= 2047`, where
/// `hdrhistogram` at 3 significant figures round-trips values exactly.
const C1_LATENCY_MS: u64 = 10;
const C2_LATENCY_MS: u64 = 500;

/// A locally comparable projection of [`ConsumerStats`] — the proto type
/// itself doesn't derive `PartialEq` (f64 rate fields), so the differential
/// comparison goes through this wrapper instead of widening the proto
/// struct's public derive surface for one test.
#[derive(Debug, Clone, PartialEq)]
struct FoldedSnapshot {
    total_msgs_received: u64,
    total_bytes_received: u64,
    total_acks_sent: u64,
    total_acks_failed: u64,
    total_msgs_dead_lettered: u64,
    total_chunked_msgs_received: u64,
    receive_latency_p50_ms: u64,
    receive_latency_p99_ms: u64,
    receive_latency_max_ms: u64,
    msgs_per_sec: f64,
    bytes_per_sec: f64,
    pending_batch_acks: usize,
}

impl From<ConsumerStats> for FoldedSnapshot {
    fn from(s: ConsumerStats) -> Self {
        Self {
            total_msgs_received: s.total_msgs_received,
            total_bytes_received: s.total_bytes_received,
            total_acks_sent: s.total_acks_sent,
            total_acks_failed: s.total_acks_failed,
            total_msgs_dead_lettered: s.total_msgs_dead_lettered,
            total_chunked_msgs_received: s.total_chunked_msgs_received,
            receive_latency_p50_ms: s.receive_latency_p50_ms,
            receive_latency_p99_ms: s.receive_latency_p99_ms,
            receive_latency_max_ms: s.receive_latency_max_ms,
            msgs_per_sec: s.msgs_per_sec,
            bytes_per_sec: s.bytes_per_sec,
            pending_batch_acks: s.pending_batch_acks,
        }
    }
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
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

fn subscribe_and_ack(conn: &mut Connection, t0: Instant, topic: &str, sub: &str) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: sub.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: 100,
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(req);
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: subscribe_request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &buf).expect("Success");
    let _ = conn.poll_event();
    conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();
    handle
}

/// Deliver `count` messages at `t0` and pop them all at
/// `t0 + latency_ms` — so every recorded `receive_latency_hist` sample is
/// exactly `latency_ms`, on both engines, on every run.
fn deliver_and_pop(
    conn: &mut Connection,
    t0: Instant,
    handle: ConsumerHandle,
    count: u64,
    ledger: u64,
    latency_ms: u64,
) {
    for entry in 0..count {
        let msg_cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: handle.0,
                message_id: pb::MessageIdData {
                    ledger_id: ledger,
                    entry_id: entry,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                },
                redelivery_count: Some(0),
                ack_set: vec![],
                consumer_epoch: None,
            }),
            ..Default::default()
        };
        let metadata = pb::MessageMetadata {
            producer_name: "magnetar-test-prod".to_owned(),
            sequence_id: entry,
            publish_time: 0,
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_payload(
            &mut frame,
            &msg_cmd,
            &metadata,
            format!("m{entry}").as_bytes(),
        )
        .expect("encode message frame");
        conn.handle_bytes(t0, &frame).expect("deliver message");
        while conn.poll_event().is_some() {}
    }
    for _ in 0..count {
        conn.pop_message(handle, t0 + Duration::from_millis(latency_ms))
            .expect("queued message");
    }
}

/// Drive the full two-consumer delivery/pop/rate-window sequence over one
/// engine's locked `Connection`, returning the folded aggregate
/// `ConsumerStats` both engines must agree on.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> FoldedSnapshot {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let c1 = subscribe_and_ack(
        conn,
        t0,
        "persistent://public/default/aggregate-stats-equiv-1",
        "sub-aggregate-stats-equiv-1",
    );
    let c2 = subscribe_and_ack(
        conn,
        t0,
        "persistent://public/default/aggregate-stats-equiv-2",
        "sub-aggregate-stats-equiv-2",
    );

    // Baseline rate-window snapshot for both consumers (first call only
    // seeds — rates stay 0.0 until the second call).
    conn.consumer_record_rate_window(c1, t0);
    conn.consumer_record_rate_window(c2, t0);

    deliver_and_pop(conn, t0, c1, C1_MESSAGES, 11, C1_LATENCY_MS);
    deliver_and_pop(conn, t0, c2, C2_MESSAGES, 22, C2_LATENCY_MS);

    // Second rate-window snapshot one synthetic second later — deterministic
    // per-second rates from the delta, no wall-clock dependency.
    let t1 = t0 + Duration::from_secs(1);
    conn.consumer_record_rate_window(c1, t1);
    conn.consumer_record_rate_window(c2, t1);

    let snapshot = |conn: &Connection, handle: ConsumerHandle| {
        let slot = conn.consumer(handle).expect("consumer slot registered");
        let state = slot.state.lock();
        (state.stats(), state.receive_latency_histogram())
    };
    let children = vec![snapshot(conn, c1), snapshot(conn, c2)];

    ConsumerStats::fold(children).into()
}

#[test]
fn aggregate_stats_fold_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_result = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    let moonpool_result = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: both engines fold the same
    // two-consumer sequence to the identical aggregate `ConsumerStats`.
    assert_eq!(
        tokio_result, moonpool_result,
        "tokio and moonpool engines diverged on ConsumerStats::fold"
    );

    // And the fold is the CORRECT aggregate, not just cross-engine-agreeing
    // on a shared bug: totals sum, rates sum, max is exact, and the merged
    // percentiles land in the expected low/high cluster per the 60/40
    // split (see `consumer_stats_fold_propagates_every_field` in
    // `magnetar-proto` for the same reasoning against hand-picked values).
    //
    // These absolute latency assertions are the load-bearing ones for
    // ADR-0086: they read percentiles produced by the REAL `pop_message`
    // path, so they fail (0 vs 500) if the state machine ever goes back to
    // reading the host clock. The cross-engine `assert_eq!` above cannot
    // catch that regression on its own — both engines would regress
    // together.
    assert_eq!(tokio_result.total_msgs_received, C1_MESSAGES + C2_MESSAGES);
    assert_eq!(
        tokio_result.pending_batch_acks, 0,
        "no PIP-54 batch entries in this unbatched delivery sequence"
    );
    assert!(
        tokio_result.msgs_per_sec > 0.0,
        "the second rate-window snapshot must yield a positive rate"
    );
    assert_eq!(
        tokio_result.receive_latency_max_ms, C2_LATENCY_MS,
        "max is the exact max across children"
    );
    assert_eq!(
        tokio_result.receive_latency_p50_ms, C1_LATENCY_MS,
        "merged p50 must land in the 60-sample low cluster"
    );
    assert_eq!(
        tokio_result.receive_latency_p99_ms, C2_LATENCY_MS,
        "merged p99 must land in the 40-sample high cluster (real histogram \
         merge, not per-child field arithmetic)"
    );
}
