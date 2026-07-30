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
//! Latency wrinkle: both `ConsumerState::pop_message` and `ProducerState::
//! apply_receipt` record latency via `Instant::elapsed()` (real wall-clock
//! time, not the synthetic `now` this test injects at delivery) — a
//! pre-existing gap in the ADR-0011 sans-io clock-injection discipline that
//! is out of scope for issue #347 to fix. Driving `pop_message` for real
//! keeps queue-draining and permit accounting realistic, but this test then
//! OVERWRITES each consumer's `receive_latency_hist` with a fully synthetic,
//! deterministic distribution (identical on both engines) before taking the
//! stats snapshot — mirroring the established convention of reaching
//! directly into slot state for a deterministic fixture (see
//! `receiver_queue_policy_equivalence.rs`'s direct `available_permits`
//! poke) — so every compared field, including the latency percentiles, is
//! exactly reproducible across runs and across engines.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, ConsumerStats, SubscribeRequest, encode_command,
    encode_payload, pb,
};

/// Two consumers with distinct delivery volumes so the totals genuinely sum
/// (rather than trivially doubling identical numbers).
const C1_MESSAGES: u64 = 5;
const C2_MESSAGES: u64 = 3;

/// Synthetic, deterministic receive-latency samples seeded directly onto
/// each consumer after the real pop sequence — see the module doc for why.
/// Mirrors the clean 60/40 low/high split from the proto-level
/// `consumer_stats_fold_propagates_every_field` test: c1's cluster is all
/// low-latency, c2's is all high-latency, so the merged p50 lands in c1's
/// cluster and the merged p99 lands in c2's.
const C1_LATENCY_MS: u64 = 10;
const C1_SAMPLES: usize = 60;
const C2_LATENCY_MS: u64 = 500;
const C2_SAMPLES: usize = 40;

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

fn deliver_and_pop(
    conn: &mut Connection,
    t0: Instant,
    handle: ConsumerHandle,
    count: u64,
    ledger: u64,
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
        conn.pop_message(handle).expect("queued message");
    }
}

/// Overwrite `handle`'s live receive-latency histogram with a fully
/// synthetic, deterministic distribution — see the module doc for why the
/// real `pop_message`-recorded samples (wall-clock, non-reproducible) are
/// discarded here.
fn seed_deterministic_latency(
    conn: &mut Connection,
    handle: ConsumerHandle,
    value_ms: u64,
    count: usize,
) {
    let slot = conn.consumer(handle).expect("consumer slot registered");
    let mut state = slot.state.lock();
    let mut hist = hdrhistogram::Histogram::<u64>::new(3).expect("histogram");
    for _ in 0..count {
        hist.saturating_record(value_ms);
    }
    state.receive_latency_hist = Some(hist);
}

/// Drive the full two-consumer delivery/pop/rate-window/latency-seed
/// sequence over one engine's locked `Connection`, returning the folded
/// aggregate `ConsumerStats` both engines must agree on.
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

    deliver_and_pop(conn, t0, c1, C1_MESSAGES, 11);
    deliver_and_pop(conn, t0, c2, C2_MESSAGES, 22);

    // Second rate-window snapshot one synthetic second later — deterministic
    // per-second rates from the delta, no wall-clock dependency.
    let t1 = t0 + Duration::from_secs(1);
    conn.consumer_record_rate_window(c1, t1);
    conn.consumer_record_rate_window(c2, t1);

    seed_deterministic_latency(conn, c1, C1_LATENCY_MS, C1_SAMPLES);
    seed_deterministic_latency(conn, c2, C2_LATENCY_MS, C2_SAMPLES);

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
