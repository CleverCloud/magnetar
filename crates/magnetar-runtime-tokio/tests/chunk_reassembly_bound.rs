// SPDX-License-Identifier: Apache-2.0

// A single readable step-by-step `fn` keeps the synthetic frame sequence the
// test pins legible; splitting into sub-helpers would obscure it.
#![allow(clippy::too_many_lines)]

//! Tokio sibling of the moonpool `chunk_reassembly_bound` test (ADR-0024 1:1
//! runtime-test-parity). With `max_pending_chunked_message = 2`, a THIRD
//! distinct never-completing chunked message must EVICT the oldest incomplete
//! buffer — bounding `magnetar_proto::ConsumerState::chunk_reassembly` against a
//! hostile/buggy broker streaming distinct-UUID first chunks that never finish.
//!
//! Observable at the engine seam: after the eviction, completing the EVICTED
//! message (its second chunk) surfaces NO `Message` (the buffer is gone, so its
//! non-first chunk is a dropped straggler), while completing a RETAINED message
//! surfaces exactly one reassembled `Message`.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// A `CommandMessage` whose `MessageIdData` carries `entry_id` so each chunk has
/// its own broker id.
fn message_cmd(consumer_id: u64, entry_id: u64) -> pb::CommandMessage {
    pb::CommandMessage {
        consumer_id,
        message_id: pb::MessageIdData {
            ledger_id: 1,
            entry_id,
            ..Default::default()
        },
        redelivery_count: Some(0),
        ack_set: vec![],
        consumer_epoch: None,
    }
}

/// Chunk metadata for chunk `chunk_id` of a `total`-chunk message `uuid`.
fn chunk_meta(uuid: &str, total: i32, chunk_id: i32) -> pb::MessageMetadata {
    pb::MessageMetadata {
        producer_name: "p".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000,
        uuid: Some(uuid.to_owned()),
        num_chunks_from_msg: Some(total),
        chunk_id: Some(chunk_id),
        total_chunk_msg_size: Some(0),
        ..Default::default()
    }
}

#[test]
fn third_incomplete_chunked_message_evicts_oldest_at_cap_two() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }

    // Subscribe with the chunk-reassembly cap pinned to 2.
    let req = SubscribeRequest {
        topic: "persistent://public/default/chunk-bound".to_owned(),
        subscription: "magnetar-test-chunk-bound".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        max_pending_chunked_message: 2,
        ..Default::default()
    };
    let (handle, subscribe_request_id): (ConsumerHandle, u64) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(req);
        (handle, request_id)
    };
    {
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
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &buf).expect("Success");
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    // Deliver the FIRST chunk of three distinct 2-chunk messages (u-0, u-1,
    // u-2). The third pushes the map past the cap of 2 → u-0 is evicted.
    let deliver_chunk = |uuid: &str, entry: u64, total: i32, chunk_id: i32, body: &'static [u8]| {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(message_cmd(handle.0, entry)),
            ..Default::default()
        };
        let meta = chunk_meta(uuid, total, chunk_id);
        let mut frame = BytesMut::new();
        encode_payload(&mut frame, &cmd, &meta, body).expect("encode chunk frame");
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver chunk");
        let mut got_message = false;
        while let Some(ev) = conn.poll_event() {
            if matches!(ev, magnetar_proto::ConnectionEvent::Message { .. }) {
                got_message = true;
            }
        }
        let _ = conn.poll_transmit();
        got_message
    };

    assert!(!deliver_chunk("u-0", 0, 2, 0, b"a0"));
    assert!(!deliver_chunk("u-1", 1, 2, 0, b"b0"));
    assert!(!deliver_chunk("u-2", 2, 2, 0, b"c0")); // evicts u-0

    // Completing the EVICTED message (u-0) with its second chunk surfaces NO
    // Message — the buffer is gone, so its non-first chunk is a dropped
    // straggler.
    assert!(
        !deliver_chunk("u-0", 10, 2, 1, b"a1"),
        "the evicted oldest message must not reassemble — its buffer was dropped"
    );

    // Completing a RETAINED message (u-2) with its second chunk surfaces exactly
    // one reassembled Message.
    assert!(
        deliver_chunk("u-2", 12, 2, 1, b"c1"),
        "a retained message must still reassemble after eviction of the oldest"
    );
}
