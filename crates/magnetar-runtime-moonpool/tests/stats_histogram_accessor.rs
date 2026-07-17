// SPDX-License-Identifier: Apache-2.0

//! Moonpool sibling of the tokio `stats_histogram_accessor` test (ADR-0024
//! 1:1 runtime-test-parity). Issue #347 (`aggregate_stats` zeroes fields),
//! ADR-0024 layer (c): drive a consumer and a producer through this
//! engine's `ConnectionShared` and assert the raw-histogram accessors
//! backing `ConsumerApi::receive_latency_histogram` / `ProducerApi::
//! send_latency_histogram` (`ConsumerState::receive_latency_histogram` /
//! `ProducerState::send_latency_histogram`) return the recorded sample
//! counts.
//!
//! Mirrors the existing `cumulative_batch_ack_prune.rs` convention: this
//! layer drives the sans-io `Connection` directly through the engine's
//! `ConnectionShared` wrapper (proving the wiring works through the real
//! per-slot mutex, not just the bare proto struct) rather than constructing
//! a full `Consumer`/`Producer` handle over a live driver loop — those
//! wrapper methods are a one-line `self.slot.state.lock().<accessor>()`
//! delegation with no engine-specific logic to exercise.
//!
//! Note: both `ConsumerState::pop_message` and `ProducerState::
//! apply_receipt` record latency via `Instant::elapsed()` — real wall-clock
//! time, not the synthetic `now` these tests inject at delivery — so this
//! test asserts sample **counts**, never specific millisecond values.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, SubscribeRequest,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Broker pushes this many messages; each `pop_message` call must stamp
/// exactly one `receive_latency_hist` sample.
const N: usize = 5;

/// Producer sends this many messages; each applied `CommandSendReceipt`
/// must stamp exactly one `send_latency_hist` sample.
const SENDS: u64 = 3;

fn deliver_message(conn: &mut Connection, t0: Instant, handle: ConsumerHandle, entry: u64) {
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
    conn.handle_bytes(t0, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
}

#[test]
fn consumer_receive_latency_histogram_reflects_popped_messages() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/stats-hist-accessor".to_owned(),
        subscription: "magnetar-test-stats-hist-accessor".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N,
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

    conn.initial_flow(handle);
    let _ = conn.poll_transmit();

    // No samples before any pop.
    let empty = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram();
    assert!(
        empty.is_none_or(|h| h.is_empty()),
        "no receive_latency_hist samples before any pop_message"
    );

    for entry in 0..N as u64 {
        deliver_message(&mut conn, t0, handle, entry);
    }
    for _ in 0..N {
        conn.pop_message(handle).expect("queued message");
    }

    let hist = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    assert_eq!(
        hist.len(),
        N as u64,
        "one receive_latency_hist sample per popped message"
    );
}

#[test]
fn producer_send_latency_histogram_reflects_receipts() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/stats-hist-accessor-producer".to_owned(),
        ..Default::default()
    });

    // No samples before any receipt.
    let empty = conn
        .producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram();
    assert!(
        empty.is_none_or(|h| h.is_empty()),
        "no send_latency_hist samples before any CommandSendReceipt"
    );

    for entry in 0..SENDS {
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
                t0,
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
        conn.handle_bytes(t0, &buf).expect("apply receipt");
        while conn.poll_event().is_some() {}
    }

    let hist = conn
        .producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram()
        .expect("send_latency_hist initialised");
    assert_eq!(
        hist.len(),
        SENDS,
        "one send_latency_hist sample per applied CommandSendReceipt"
    );
}
