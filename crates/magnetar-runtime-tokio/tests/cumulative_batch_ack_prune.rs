// SPDX-License-Identifier: Apache-2.0

//! Tokio sibling of the moonpool `cumulative_batch_ack_prune` test (ADR-0024 1:1
//! runtime-test-parity). A cumulative ack must prune every PIP-54
//! `batch_ack_tracker` entry it covers — not just the entry of the acked id
//! itself — so a consumer that only ever acks cumulatively keeps the tracker
//! bounded by the un-acked window (issue #326).
//!
//! Before the fix the cumulative branch removed only the exact
//! `(ledger_id, entry_id)` key of the supplied id; every entry below the
//! cumulative position leaked until reconnect. A production watermark acker on
//! a batched topic (one cumulative ack per N messages, never an individual ack)
//! grew one `BatchAckEntry` per batched broker entry forever — the fleet ran out of memory
//! at ~24 GiB every 4-6 h.
//!
//! ## Shape
//!
//! 1. Handshake + plain durable subscribe (no ack timeout, no nack delay).
//! 2. Deliver a stream of synthetic BATCHED broker entries (`num_messages_in_batch = 3`, packed
//!    `SingleMessageMetadata` body) — each stamps one tracker entry.
//! 3. Every `ACK_EVERY` entries, send ONE cumulative ack at the consume front and never an
//!    individual ack.
//! 4. The `pending_batch_acks` gauge must fill to the watermark window between acks and read ZERO
//!    after every cumulative ack.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, ConsumerHandle, MessageId, OpOutcome, PendingOpKey,
    SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Batched entries delivered over the run.
const ENTRIES: u64 = 100;
/// Sub-messages packed into each batched entry (`num_messages_in_batch`).
const BATCH_SIZE: i32 = 3;
/// Watermark cadence: one cumulative ack per this many entries.
const ACK_EVERY: u64 = 10;

/// Deliver one synthetic BATCH of [`BATCH_SIZE`] messages on `(ledger, entry)`:
/// `num_messages_in_batch` metadata plus a `(u32 single_size)(SingleMessageMetadata)
/// (payload)` packed body, matching the wire format `ConsumerState::deliver`
/// explodes — each batched entry stamps exactly one `batch_ack_tracker` entry.
fn deliver_batch(
    conn: &mut Connection,
    t0: Instant,
    handle: ConsumerHandle,
    ledger: u64,
    entry: u64,
) {
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
        num_messages_in_batch: Some(BATCH_SIZE),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for idx in 0..BATCH_SIZE {
        let payload = format!("cumulative-prune-{entry}-{idx}").into_bytes();
        let sm = pb::SingleMessageMetadata {
            payload_size: payload.len() as i32,
            ..Default::default()
        };
        let sm_len = prost::Message::encoded_len(&sm);
        body.extend_from_slice(&(sm_len as u32).to_be_bytes());
        prost::Message::encode(&sm, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(&payload);
    }
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, &body).expect("encode batch frame");
    conn.handle_bytes(t0, &frame).expect("deliver batch");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

#[test]
fn cumulative_only_acking_keeps_batch_ack_tracker_bounded() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    // Handshake at synthetic t0.
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    // Plain durable subscription — no ack timeout, no nack delay: the workload
    // under test acks exclusively via cumulative watermarks.
    let req = SubscribeRequest {
        topic: "persistent://public/default/cumulative-prune".to_owned(),
        subscription: "magnetar-test-cumulative-prune".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
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

    let gauge = |conn: &Connection| {
        conn.consumer_stats(handle)
            .expect("consumer stats")
            .pending_batch_acks
    };

    for entry in 0..ENTRIES {
        deliver_batch(&mut conn, t0, handle, 12, entry);
        // Sensitivity: between watermarks the tracker really fills — one entry
        // per batched delivery — so a regression cannot pass by the gauge
        // reading 0 throughout.
        assert_eq!(
            gauge(&conn) as u64,
            entry % ACK_EVERY + 1,
            "each batched delivery must stamp exactly one tracker entry (entry {entry})"
        );
        if (entry + 1) % ACK_EVERY == 0 {
            let _ = conn.ack(
                handle,
                AckRequest {
                    message_ids: vec![MessageId {
                        ledger_id: 12,
                        entry_id: entry,
                        partition: -1,
                        batch_index: -1,
                        batch_size: 0,
                        #[cfg(feature = "scalable-topics")]
                        segment_id: None,
                    }],
                    ack_type: pb::command_ack::AckType::Cumulative,
                    properties: Vec::new(),
                    txn_id: None,
                },
                t0,
            );
            let _ = conn.poll_transmit();
            // The #326 bound: a cumulative ack at the consume front empties the
            // tracker. Before the fix the post-ack gauge read 9, 19, 29, … —
            // one leaked entry per batched entry consumed, forever.
            assert_eq!(
                gauge(&conn),
                0,
                "a cumulative ack at the consume front must prune every tracker \
                 entry it covers, not just the exact acked key (issue #326, \
                 watermark at entry {entry})"
            );
        }
    }
}

#[test]
fn individual_batch_ack_after_reset_keeps_siblings_unacked() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/batch-ack-reset".to_owned(),
        subscription: "magnetar-test-batch-ack-reset".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    deliver_batch(&mut conn, t0, handle, 11, 7);
    assert_eq!(
        conn.consumer_stats(handle)
            .expect("consumer stats")
            .pending_batch_acks,
        1
    );

    conn.reset();
    let _ = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 11,
                entry_id: 7,
                partition: -1,
                batch_index: 1,
                batch_size: BATCH_SIZE,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        t0,
    );
    let mut wire = conn.poll_transmit();
    let frame = decode_one(&mut wire).expect("CommandAck frame");
    let ack = frame.command.ack.expect("CommandAck");
    assert_eq!(ack.message_id[0].ack_set, vec![0b101]);

    let invalid_request = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 11,
                entry_id: 7,
                partition: -1,
                batch_index: BATCH_SIZE,
                batch_size: BATCH_SIZE,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        t0,
    );
    assert!(conn.poll_transmit().is_empty());
    assert!(matches!(
        conn.take_outcome(PendingOpKey::Request(invalid_request)),
        Some(OpOutcome::Error {
            code: -1,
            ref message,
            ..
        }) if message == "invalid batched message id"
    ));
}
