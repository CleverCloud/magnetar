// SPDX-License-Identifier: Apache-2.0

//! Issue #423: `SendReceipt` / `SendError` are per-message observational
//! events, and nothing in the production runtime drains `Connection::events`.
//! Until they were routed through the cap, a steady producer queued one event
//! per acknowledged message for the whole life of the connection — enough to
//! OOM-kill a gateway every 24-48h on that alone.
//!
//! The cap must hold WITHOUT costing a single send completion: the outcome
//! reaches the caller through the operation waker, never through this queue.

mod common;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::conn::EVENT_QUEUE_OBSERVATIONAL_CAP;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, encode_command, pb};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Enough receipts to cross the cap with room to spare.
const SENDS: u64 = EVENT_QUEUE_OBSERVATIONAL_CAP as u64 + 100;
const SEND_RTT_MS: u64 = 5;

#[test]
fn producer_receipt_events_are_capped_without_losing_completions() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-event-cap".to_owned(),
        ..Default::default()
    });

    // Never call `poll_event` — exactly what the production runtime does.
    for entry in 0..SENDS {
        let seq = conn
            .send(
                producer,
                OutgoingMessage {
                    payload: Bytes::from_static(b"hi"),
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
        conn.handle_bytes(t0 + Duration::from_millis(SEND_RTT_MS), &buf)
            .expect("apply receipt");
    }

    assert!(
        conn.dropped_observational_events() >= 100,
        "receipts past the cap must be dropped, got {}",
        conn.dropped_observational_events()
    );

    let dropped_after_receipts = conn.dropped_observational_events();

    // The producer side has a second per-message event: a broker-side send
    // failure. Past the cap it must be dropped just the same — and, like the
    // receipt, its outcome is recorded before the event is ever queued.
    let seq = conn
        .send(
            producer,
            OutgoingMessage {
                payload: Bytes::from_static(b"hi"),
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

    let send_error = pb::BaseCommand {
        r#type: pb::base_command::Type::SendError as i32,
        send_error: Some(pb::CommandSendError {
            producer_id: producer.0,
            sequence_id: seq.0,
            error: pb::ServerError::PersistenceError as i32,
            message: "broker refused the write".to_owned(),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &send_error).expect("encode CommandSendError");
    conn.handle_bytes(t0 + Duration::from_millis(SEND_RTT_MS), &buf)
        .expect("apply send error");

    assert_eq!(
        conn.dropped_observational_events(),
        dropped_after_receipts + 1,
        "a SendError past the cap must be dropped like a receipt"
    );

    // …and every receipt still reached the producer state machine: one
    // send-latency sample per receipt, cap or no cap.
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
        "the cap must drop events, never send completions"
    );
}
