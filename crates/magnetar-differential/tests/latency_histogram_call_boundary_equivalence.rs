// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d) for ADR-0086: driven through each engine's own
//! `ConnectionShared` with an identical injected-`now` script, the tokio and
//! moonpool receive- and send-latency histograms must be byte-identical.
//!
//! Since ADR-0086 the latency samples are a pure function of the instants the
//! engine hands in — `now - arrived_at` for the consumer, `now -
//! enqueued_at` for the producer — so "same script in, same histogram out"
//! is a real cross-engine claim. Before the fix both engines read the host
//! clock inside the shared state machine and produced ~0 ms garbage that
//! happened to match, which is why the absolute assertions below carry the
//! regression weight and the cross-engine `assert_eq!` alone does not (see
//! the same honesty note in `aggregate_stats_equivalence.rs`).

use std::time::{Duration, Instant};

use bytes::BytesMut;
use hdrhistogram::Histogram;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, SubscribeRequest,
    encode_command, encode_payload, pb,
};

/// Messages delivered/popped and sends receipted per engine.
const N: u64 = 4;

/// Scripted offsets from the shared base instant.
const ARRIVE_AT_MS: u64 = 100;
const POP_AT_MS: u64 = 350;
const ENQUEUE_AT_MS: u64 = 100;
const RECEIPT_AT_MS: u64 = 600;

/// Expected samples. Both `<= 2047`, where `hdrhistogram` at 3 significant
/// figures round-trips values exactly. Deliberately different from each other
/// so a consumer/producer mix-up cannot pass.
const EXPECTED_DWELL_MS: u64 = POP_AT_MS - ARRIVE_AT_MS;
const EXPECTED_RTT_MS: u64 = RECEIPT_AT_MS - ENQUEUE_AT_MS;

/// Every recorded `(value, count)` pair of one histogram.
type RecordedSamples = Vec<(u64, u64)>;

/// The pair of histograms one engine produces from the shared script.
type LatencySnapshot = (RecordedSamples, RecordedSamples);

/// Compact, fully-informative projection of a histogram: every recorded
/// `(value, count)` pair. Keeps a failure message to one readable line — the
/// raw `Debug` impl prints all 2048 sub-buckets.
fn recorded(hist: &Histogram<u64>) -> RecordedSamples {
    hist.iter_recorded()
        .map(|it| (it.value_iterated_to(), it.count_at_value()))
        .collect()
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

fn subscribe_and_ack(conn: &mut Connection, at: Instant) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: "persistent://public/default/latency-call-boundary-equiv".to_owned(),
        subscription: "sub-latency-call-boundary-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: 100,
        ..Default::default()
    };
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(req);
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

fn deliver_message(conn: &mut Connection, at: Instant, handle: ConsumerHandle, entry: u64) {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 9,
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
    conn.handle_bytes(at, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
}

/// Run the identical consumer + producer latency script over one engine's
/// locked `Connection`, returning `(receive_hist, send_hist)`.
fn lock_and_run(conn: &mut Connection, base: Instant) -> LatencySnapshot {
    let arrive = base + Duration::from_millis(ARRIVE_AT_MS);
    let pop_at = base + Duration::from_millis(POP_AT_MS);
    let enqueue_at = base + Duration::from_millis(ENQUEUE_AT_MS);
    let receipt_at = base + Duration::from_millis(RECEIPT_AT_MS);

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(base, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    // Consumer leg: deliver at `arrive`, pop at `pop_at`.
    let handle = subscribe_and_ack(conn, arrive);
    for entry in 0..N {
        deliver_message(conn, arrive, handle, entry);
    }
    for _ in 0..N {
        conn.pop_message(handle, pop_at).expect("queued message");
    }

    // Producer leg: enqueue at `enqueue_at`, receipt at `receipt_at`.
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/latency-call-boundary-equiv-producer".to_owned(),
        ..Default::default()
    });
    for entry in 0..N {
        let seq = conn
            .send(
                producer,
                magnetar_proto::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                enqueue_at,
            )
            .expect("send queues");

        let receipt = pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: producer.0,
                sequence_id: seq.0,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: entry,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &receipt).expect("encode CommandSendReceipt");
        conn.handle_bytes(receipt_at, &buf).expect("apply receipt");
        while conn.poll_event().is_some() {}
    }

    let receive_hist = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    let send_hist = conn
        .producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram()
        .expect("send_latency_hist initialised");

    (recorded(&receive_hist), recorded(&send_hist))
}

#[test]
fn latency_histograms_agree_across_engines_under_identical_now_script() {
    // ONE base for both engines — the injected script is byte-identical.
    let base = Instant::now();

    let tokio_result = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, base)
    };

    let moonpool_result = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, base)
    };

    // The differential equivalence claim.
    assert_eq!(
        tokio_result.0, moonpool_result.0,
        "tokio and moonpool diverged on receive_latency_hist"
    );
    assert_eq!(
        tokio_result.1, moonpool_result.1,
        "tokio and moonpool diverged on send_latency_hist"
    );

    // Non-vacuity: the engines agree on the CORRECT value, not on a shared
    // ~0 ms bug. These are the assertions that fail if the state machine goes
    // back to reading the host clock.
    assert_eq!(
        tokio_result.0,
        vec![(EXPECTED_DWELL_MS, N)],
        "receive samples must be the injected `now - arrived_at`"
    );
    assert_eq!(
        tokio_result.1,
        vec![(EXPECTED_RTT_MS, N)],
        "send samples must be the injected `now - enqueued_at`"
    );
}
