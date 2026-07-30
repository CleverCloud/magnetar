// SPDX-License-Identifier: Apache-2.0

//! Tokio sibling of the moonpool `latency_histogram_injected_clock` test
//! (ADR-0024 1:1 runtime-test-parity). ADR-0011 / ADR-0086, ADR-0024 layer
//! (b): the receive- and send-latency histograms are a pure function of the
//! `now` handed to `Connection::pop_message` / `handle_bytes`, never of the
//! host clock read inside the state machine.
//!
//! This engine has no clock provider — it snapshots `std::time::Instant::
//! now()` at the call boundary — so these tests are not about simulation
//! determinism. They are still substantive rather than parity filler: once
//! `pop_message` consumes the caller's `now`, a `ConnectionShared` driven
//! from a fixed synthetic script is exactly reproducible, so run-to-run
//! equality under real host-clock motion is a genuine assertion. Before
//! ADR-0086 all three failed here for the same reason they failed on
//! moonpool.
//!
//! Shape borrowed from `poll_transmit_vectored_parity.rs` (two
//! `ConnectionShared`s driven identically, compare the observable).

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use hdrhistogram::Histogram;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, SubscribeRequest,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Messages delivered/popped per run.
const N: u64 = 4;

/// Scripted instants, all derived from ONE base captured before either run.
const ARRIVE_AT_MS: u64 = 100;
const POP_AT_MS: u64 = 350;

/// The value every sample must carry. `<= 2047` keeps `hdrhistogram`
/// sigfig-3 quantisation exact.
const EXPECTED_DWELL_MS: u64 = POP_AT_MS - ARRIVE_AT_MS;

/// Producer leg: enqueue at `ENQUEUE_AT_MS`, receipt at `RECEIPT_AT_MS`.
const ENQUEUE_AT_MS: u64 = 100;
const RECEIPT_AT_MS: u64 = 350;
const EXPECTED_RTT_MS: u64 = RECEIPT_AT_MS - ENQUEUE_AT_MS;

/// Real host-clock motion injected BETWEEN the two runs. Pre-fix this landed
/// straight in run B's histogram; post-fix it is invisible.
const HOST_DRIFT: Duration = Duration::from_millis(120);

/// Compact, fully-informative projection of a histogram: every recorded
/// `(value, count)` pair. Comparing these instead of the `Histogram` structs
/// directly keeps a failure message to one readable line — the raw `Debug`
/// impl prints all 2048 sub-buckets.
fn recorded(hist: &Histogram<u64>) -> Vec<(u64, u64)> {
    hist.iter_recorded()
        .map(|it| (it.value_iterated_to(), it.count_at_value()))
        .collect()
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

fn subscribe_and_ack(conn: &mut Connection, at: Instant) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: "persistent://public/default/latency-hist-injected-clock".to_owned(),
        subscription: "magnetar-test-latency-hist-injected-clock".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N as usize,
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

/// Drive the deliver → pop script once. `pop_message` is handed the scripted
/// instant the tokio `ReceiveFut::poll` would have snapshotted at the call
/// boundary.
fn run_receive_script(base: Instant) -> Histogram<u64> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let arrive = base + Duration::from_millis(ARRIVE_AT_MS);
    let pop_at = base + Duration::from_millis(POP_AT_MS);

    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(arrive, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let handle = subscribe_and_ack(&mut conn, arrive);
    for entry in 0..N {
        deliver_message(&mut conn, arrive, handle, entry);
    }
    for _ in 0..N {
        conn.pop_message(handle, pop_at).expect("queued message");
    }

    conn.consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised")
}

/// Producer leg: the receipt clock reaches `apply_receipt` through
/// `handle_bytes(now, …)` → `handle_frame(now, …)`.
fn run_send_script(base: Instant) -> Histogram<u64> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let enqueue_at = base + Duration::from_millis(ENQUEUE_AT_MS);
    let receipt_at = base + Duration::from_millis(RECEIPT_AT_MS);

    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(enqueue_at, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/latency-hist-injected-clock-producer".to_owned(),
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

    conn.producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram()
        .expect("send_latency_hist initialised")
}

#[test]
fn tokio_receive_latency_histogram_reflects_the_call_boundary_now() {
    let base = Instant::now();

    let first = run_receive_script(base);
    std::thread::sleep(HOST_DRIFT);
    let second = run_receive_script(base);

    assert_eq!(
        recorded(&first),
        recorded(&second),
        "receive_latency_hist moved with the HOST clock across two identical runs — \
         the Instant::elapsed() leak is back"
    );
    assert_eq!(first.len(), N, "one sample per popped message");
    assert_eq!(
        first.max(),
        EXPECTED_DWELL_MS,
        "sample must be the `now` passed to pop_message minus `arrived_at`"
    );
    assert_eq!(first.value_at_quantile(0.50), EXPECTED_DWELL_MS);
}

#[test]
fn tokio_send_latency_histogram_reflects_the_call_boundary_now() {
    let base = Instant::now();

    let first = run_send_script(base);
    std::thread::sleep(HOST_DRIFT);
    let second = run_send_script(base);

    assert_eq!(
        recorded(&first),
        recorded(&second),
        "send_latency_hist moved with the HOST clock across two identical runs — \
         the Instant::elapsed() leak is back"
    );
    assert_eq!(first.len(), N, "one sample per applied CommandSendReceipt");
    assert_eq!(
        first.max(),
        EXPECTED_RTT_MS,
        "sample must be the `now` carried by handle_bytes minus `enqueued_at`"
    );
    assert_eq!(first.value_at_quantile(0.50), EXPECTED_RTT_MS);
}

/// A host sleep between delivery and pop must not change the recorded value:
/// the sample is `now - arrived_at` for the `now` the caller supplied, and
/// both are scripted here. Pre-fix this recorded the sleep duration instead.
#[test]
fn tokio_latency_histograms_do_not_move_with_host_wall_clock() {
    let base = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(base, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let handle = subscribe_and_ack(&mut conn, base);
    deliver_message(&mut conn, base, handle, 0);

    // The host clock genuinely advances; the scripted instants do not.
    std::thread::sleep(HOST_DRIFT);
    conn.pop_message(handle, base).expect("queued message");

    let hist = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    assert_eq!(hist.len(), 1);
    assert_eq!(
        hist.max(),
        0,
        "delivery and pop share one instant, so the sample is 0 ms — the host sleep \
         between them must not appear"
    );
}
