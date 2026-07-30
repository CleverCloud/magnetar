// SPDX-License-Identifier: Apache-2.0

//! ADR-0089 layer (d): tokio ↔ moonpool parity for the connection-driven
//! rolling-rate sweep, and — the reason the sweep exists — for what the
//! wrapper `aggregate_stats()` folds now carry.
//!
//! `docs/follow-ups.md` §2 opened on the observation that
//! `PartitionedProducer::aggregate_stats` / `MultiTopicsConsumer::
//! aggregate_stats` / `PatternConsumer::aggregate_stats` sum **zeros** in the
//! rate fields, because `record_rate_window` was reachable only from a caller
//! and nothing in either engine ever called it. This file is the paired proof
//! that arming `ConnectionConfig::stats_interval` closes that at the leaf: two
//! consumers and two producers with deliberately different volumes are driven
//! through each engine's own `ConnectionShared`, ticked exclusively by
//! `Connection::handle_timeout`, and folded with the same
//! `ConsumerStats::fold` / `ProducerStats::fold` the three wrappers use.
//!
//! Note what is NOT here: any wrapper-level fan-out. Java's wrappers have none
//! either — `PartitionedProducerImpl.getStats()` resets and folds children,
//! while each leaf recorder self-ticks on `pulsarClient.timer()` — so one clock
//! ticking every slot is both the parity-correct shape and what makes the
//! folded sum well-defined. A caller who ticked three children of four would
//! get an authoritative-looking total that means nothing.
//!
//! Honesty note on what this file proves, in the shape ADR-0024 asks for: the
//! cross-engine `assert_eq!` alone would be GREEN with the sweep deleted
//! entirely — both engines share the same proto code and would agree on the
//! same wrong `0.0`. That is the "parallel tests drift in lockstep" failure
//! mode, so the absolute assertions at the end (the folded rates equal the
//! scripted per-second deltas) are what make this a regression test rather
//! than a tautology.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, ConsumerStats, CreateProducerRequest,
    ProducerHandle, ProducerStats, SubscribeRequest, encode_command, encode_payload, pb,
};

/// One synthetic second per window, so a delta of N reads as exactly N/sec and
/// the folded sums are exact integers in f64.
const INTERVAL: Duration = Duration::from_secs(1);

/// Deliberately different per-child volumes so the fold genuinely sums rather
/// than trivially doubling one number.
const C1_MESSAGES: u64 = 6;
const C2_MESSAGES: u64 = 4;
const P1_MESSAGES: u64 = 3;
const P2_MESSAGES: u64 = 7;

/// Payload on every message, both directions.
const PAYLOAD: &[u8] = b"rate";

/// [`PAYLOAD`]'s length in the `u32` shape `OutgoingMessage` wants.
const PAYLOAD_LEN: u32 = PAYLOAD.len() as u32;

/// A locally comparable projection of the two stats structs — neither derives
/// `PartialEq` (f64 rate fields), so the differential comparison goes through
/// this wrapper rather than widening a published derive surface for one test.
#[derive(Debug, Clone, PartialEq)]
struct FoldedRates {
    consumer_msgs_per_sec: f64,
    consumer_bytes_per_sec: f64,
    consumer_total_msgs: u64,
    producer_msgs_per_sec: f64,
    producer_bytes_per_sec: f64,
    producer_total_msgs: u64,
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

fn subscribe_ready(conn: &mut Connection, at: Instant, topic: &str, sub: &str) -> ConsumerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: topic.to_owned(),
        subscription: sub.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: 100,
        ..Default::default()
    });
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(at, &buf).expect("Success");
    let _ = conn.poll_event();
    conn.initial_flow(handle, at);
    let _ = conn.poll_transmit();
    handle
}

fn producer_ready(conn: &mut Connection, at: Instant, topic: &str) -> ProducerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    });
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: format!("magnetar-test-{}", handle.0),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    conn.handle_bytes(at, &buf).expect("ProducerSuccess");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();
    handle
}

fn deliver(conn: &mut Connection, at: Instant, handle: ConsumerHandle, ledger: u64, count: u64) {
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
        encode_payload(&mut frame, &msg_cmd, &metadata, PAYLOAD).expect("encode message frame");
        conn.handle_bytes(at, &frame).expect("deliver message");
        while conn.poll_event().is_some() {}
    }
}

fn publish(conn: &mut Connection, at: Instant, handle: ProducerHandle, count: u64) {
    for seq in 0..count {
        conn.send(
            handle,
            OutgoingMessage {
                payload: bytes::Bytes::from_static(PAYLOAD),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: PAYLOAD_LEN,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            seq,
            at,
        )
        .expect("queue send");
    }
    let _ = conn.poll_transmit();
}

/// Drive the full four-slot seed → traffic → sweep sequence over one engine's
/// locked `Connection`, returning the folded aggregates both engines must
/// agree on.
///
/// Every tick here is `handle_timeout`. No caller calls `record_rate_window`,
/// no wrapper fans anything out, and no engine-side task exists — which is
/// exactly the claim under test.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> FoldedRates {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let c1 = subscribe_ready(
        conn,
        t0,
        "persistent://public/default/stats-interval-equiv-1",
        "sub-stats-interval-equiv-1",
    );
    let c2 = subscribe_ready(
        conn,
        t0,
        "persistent://public/default/stats-interval-equiv-2",
        "sub-stats-interval-equiv-2",
    );
    let p1 = producer_ready(
        conn,
        t0,
        "persistent://public/default/stats-interval-equiv-1",
    );
    let p2 = producer_ready(
        conn,
        t0,
        "persistent://public/default/stats-interval-equiv-2",
    );

    // Every slot — including the two producers, the half a consumer-only fan-out
    // would have missed — was seeded at creation, anchored to the `t0` handshake
    // instant. This tick lands inside that first window and must be a no-op.
    conn.handle_timeout(t0);

    deliver(conn, t0, c1, 11, C1_MESSAGES);
    deliver(conn, t0, c2, 22, C2_MESSAGES);
    publish(conn, t0, p1, P1_MESSAGES);
    publish(conn, t0, p2, P2_MESSAGES);

    // One synthetic second later the sweep publishes every slot's rate.
    conn.handle_timeout(t0 + INTERVAL);

    let consumer_children = [c1, c2]
        .into_iter()
        .map(|h| {
            let slot = conn.consumer(h).expect("consumer slot registered");
            let state = slot.state.lock();
            (state.stats(), state.receive_latency_histogram())
        })
        .collect::<Vec<_>>();
    let producer_children = [p1, p2]
        .into_iter()
        .map(|h| {
            let slot = conn.producer(h).expect("producer slot registered");
            let state = slot.state.lock();
            (state.stats(), state.send_latency_histogram())
        })
        .collect::<Vec<_>>();

    let consumer = ConsumerStats::fold(consumer_children);
    let producer = ProducerStats::fold(producer_children);
    FoldedRates {
        consumer_msgs_per_sec: consumer.msgs_per_sec,
        consumer_bytes_per_sec: consumer.bytes_per_sec,
        consumer_total_msgs: consumer.total_msgs_received,
        producer_msgs_per_sec: producer.msgs_per_sec,
        producer_bytes_per_sec: producer.bytes_per_sec,
        producer_total_msgs: producer.total_msgs_sent,
    }
}

#[test]
fn stats_interval_sweep_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_result = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig {
            stats_interval: Some(INTERVAL),
            ..ConnectionConfig::default()
        });
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    let moonpool_result = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig {
            stats_interval: Some(INTERVAL),
            ..ConnectionConfig::default()
        });
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: the sweep is a pure function of the
    // injected `now` and the per-slot counters, so both engines fold the same
    // sequence to identical aggregates.
    assert_eq!(
        tokio_result, moonpool_result,
        "tokio and moonpool engines diverged on the stats_interval rate sweep"
    );

    // …and the aggregates are CORRECT, not just cross-engine-agreeing on a
    // shared zero. These are the assertions that fail if the sweep is removed,
    // never reaches producers, ticks only one child, or divides by the wrong
    // window — i.e. the ones that actually close `docs/follow-ups.md` §2.
    #[allow(
        clippy::cast_precision_loss,
        reason = "single-digit per-child test constants"
    )]
    let want_consumer_msgs = (C1_MESSAGES + C2_MESSAGES) as f64;
    #[allow(clippy::cast_precision_loss, reason = "same as above")]
    let want_consumer_bytes = ((C1_MESSAGES + C2_MESSAGES) * u64::from(PAYLOAD_LEN)) as f64;
    #[allow(clippy::cast_precision_loss, reason = "same as above")]
    let want_producer_msgs = (P1_MESSAGES + P2_MESSAGES) as f64;
    #[allow(clippy::cast_precision_loss, reason = "same as above")]
    let want_producer_bytes = ((P1_MESSAGES + P2_MESSAGES) * u64::from(PAYLOAD_LEN)) as f64;

    for (label, got, want) in [
        (
            "folded consumer msgs_per_sec",
            tokio_result.consumer_msgs_per_sec,
            want_consumer_msgs,
        ),
        (
            "folded consumer bytes_per_sec",
            tokio_result.consumer_bytes_per_sec,
            want_consumer_bytes,
        ),
        (
            "folded producer msgs_per_sec",
            tokio_result.producer_msgs_per_sec,
            want_producer_msgs,
        ),
        (
            "folded producer bytes_per_sec",
            tokio_result.producer_bytes_per_sec,
            want_producer_bytes,
        ),
    ] {
        assert!(
            (got - want).abs() < 1e-9,
            "{label}: the fold must sum REAL per-child rates — this is the field \
             that summed zeros before ADR-0089. Expected {want}, got {got}"
        );
    }

    // Sensitivity backstop: the per-child volumes really were asymmetric, so
    // neither sum above could have come from one child alone.
    assert_eq!(
        tokio_result.consumer_total_msgs,
        C1_MESSAGES + C2_MESSAGES,
        "both consumers must have contributed"
    );
    assert_eq!(
        tokio_result.producer_total_msgs,
        P1_MESSAGES + P2_MESSAGES,
        "both producers must have contributed"
    );
}
