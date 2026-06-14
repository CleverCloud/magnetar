// SPDX-License-Identifier: Apache-2.0

// A single readable step-by-step `fn` keeps the synthetic frame sequence the
// test pins legible; splitting into sub-helpers would obscure it.
#![allow(clippy::too_many_lines)]

//! Chaos scenario: a message that is BOTH negatively acknowledged AND tracked by
//! the `ack_timeout` (unacked-message) tracker must be redelivered EXACTLY ONCE.
//!
//! `negative_ack` adds the id to the nack tracker; without also dropping it from
//! the unacked tracker, [`magnetar_proto::Connection::handle_timeout`] redelivers
//! it twice — once from the nack tracker, once from the ack-timeout sweep —
//! corrupting at-least-once-without-duplication.
//!
//! Moonpool territory: the same virtual-clock rationale as
//! [`virtual_clock_ack_timeout`](crate::common) — `testcontainers` cannot drive a
//! fast deterministic nack-delay + ack-timeout boundary; only synthetic
//! [`Instant`]s can pin it.
//!
//! ## Shape
//!
//! 1. Subscribe a consumer with `ack_timeout = 10s` AND `negative_ack_redelivery_delay = 2s`.
//! 2. Feed a synthetic broker `CommandMessage` at virtual t0 — the unacked tracker records it.
//! 3. `negative_ack` the message at t0 — it joins the nack tracker; the fix also drops it from the
//!    unacked tracker.
//! 4. Tick at `t0 + 11s` (past BOTH deadlines). The proto layer must emit EXACTLY ONE
//!    [`pb::CommandRedeliverUnacknowledgedMessages`] frame, not two.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, MessageId, SubscribeRequest, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const NACK_DELAY: Duration = Duration::from_secs(2);

#[test]
fn nack_drops_id_from_unacked_tracker_so_redelivery_fires_once() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    // Handshake at virtual t0.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }

    // Subscribe with BOTH the ack-timeout and the nack-redelivery-delay knobs set.
    let req = SubscribeRequest {
        topic: "persistent://public/default/nack-unacked".to_owned(),
        subscription: "magnetar-test-nack-unacked".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        ack_timeout: Some(ACK_TIMEOUT),
        negative_ack_redelivery_delay: Some(NACK_DELAY),
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
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
        let _ = conn.poll_event();
    }

    // Deliver a synthetic message; the unacked tracker arms its ack-timeout deadline.
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id: 3,
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
        sequence_id: 1,
        publish_time: 0,
        num_messages_in_batch: Some(1),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, b"nack-unacked-payload")
        .expect("encode message frame");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
        // Drain the Message event(s) and the initial flow bytes.
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    // Negatively acknowledge the delivered id. The single-message delivery path
    // normalises a non-batched id to `batch_index = -1, batch_size = 0`, so that is
    // the key the unacked tracker holds and the id a user would nack.
    let nacked_id = MessageId {
        ledger_id: 7,
        entry_id: 3,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    {
        let mut conn = shared.inner.lock();
        conn.negative_ack(handle, vec![nacked_id], t0);
    }

    // Tick past BOTH the nack delay (t0 + 2s) and the ack timeout (t0 + 10s) in one
    // sweep. Exactly ONE redelivery frame must be queued: the nack tracker's. The
    // ack-timeout sweep must add none because the fix removed the id.
    let redeliver_frames = {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + Duration::from_secs(11));
        let mut src = conn.poll_transmit();
        let mut count = 0;
        while !src.is_empty() {
            let decoded = magnetar_proto::decode_one(&mut src).expect("decode outbound frame");
            if decoded.command.r#type
                == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32
            {
                count += 1;
            }
        }
        count
    };
    assert_eq!(
        redeliver_frames, 1,
        "a nacked + ack-timeout-tracked message must be redelivered exactly once, not \
         twice (nack tracker + ack-timeout sweep)"
    );
}
