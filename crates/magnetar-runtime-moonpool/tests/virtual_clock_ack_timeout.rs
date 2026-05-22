// SPDX-License-Identifier: Apache-2.0

// Chaos scenarios value a single readable step-by-step `fn`. Splitting
// these into sub-helpers would obscure the synthetic frame sequence the
// test pins. We accept the line count.
#![allow(clippy::too_many_lines)]

//! Chaos scenario: `ack_timeout` on a consumer's
//! [`UnackedMessageTracker`](magnetar_proto::trackers::unacked) must fire at
//! the configured virtual deadline — not the host wall-clock.
//!
//! Why this is moonpool territory: same as the send-timeout sibling
//! ([`virtual_clock_send_timeout`](crate::common)) — `testcontainers`
//! cannot drive a fast deterministic deadline; only synthetic [`Instant`]s
//! can pin the boundary condition.
//!
//! ## Shape
//!
//! 1. Subscribe a consumer with `ack_timeout = 10s`.
//! 2. Feed a synthetic broker `CommandMessage` + payload back to the state machine at virtual t0.
//!    The unacked-tracker records it.
//! 3. Tick at `t0 + 9.9s` — no redelivery yet, tracker still holds the id.
//! 4. Tick at `t0 + 10.1s` — the proto layer emits a [`pb::CommandRedeliverUnacknowledgedMessages`]
//!    frame on the outbound queue, addressed to the consumer + carrying the timed-out message id.

mod common;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::{ConnectionConfig, SubscribeRequest, encode_command, encode_payload, pb};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

const ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn ack_timeout_fires_at_virtual_deadline() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    // Drive the handshake at virtual t0.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        let connected = handshake_response_bytes();
        conn.handle_bytes(t0, &connected).expect("Connected");
        let _ = conn.poll_event();
    }

    // Open a subscription with the ack-timeout knob set. Drain the
    // outbound `CommandSubscribe` and the broker's `CommandSuccess` so the
    // consumer is past the open round-trip.
    let req = SubscribeRequest {
        topic: "persistent://public/default/ack-timeout".to_owned(),
        subscription: "magnetar-test-ack-timeout".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        ack_timeout: Some(ACK_TIMEOUT),
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(req);
        (handle, request_id)
    };
    {
        // Ack the subscribe.
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

    // Feed a synthetic incoming message. The broker frame is
    // `CommandMessage` followed by a `MessageMetadata`-prefixed payload.
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
        ..Default::default()
    };
    let payload = b"unacked-payload";
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode message frame");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
    }
    // The state machine emits a `ConnectionEvent::Message` and drains the
    // consumer queue inline during `handle_bytes`. The unacked-tracker is
    // populated at `deliver` time (not on pop), so by the time we reach
    // here the timer is already armed for the synthetic message id.
    let mut saw_msg = false;
    {
        let mut conn = shared.inner.lock();
        while let Some(evt) = conn.poll_event() {
            if let magnetar_proto::ConnectionEvent::Message { message, .. } = evt {
                assert_eq!(message.payload, Bytes::from_static(payload));
                saw_msg = true;
            }
        }
    }
    assert!(
        saw_msg,
        "expected a Message event for the delivered payload"
    );

    // Drain the outbound bytes the consumer state machine queued (initial
    // `CommandFlow`, etc) so we can later observe the redeliver-unacked
    // frame in isolation.
    {
        let mut conn = shared.inner.lock();
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
    }

    // Tick before the deadline. Tracker still holds the id; no redeliver
    // frame on the wire.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + Duration::from_millis(9_900));
        let mut tx_buf: Vec<u8> = Vec::new();
        let n = conn.poll_transmit(&mut tx_buf);
        assert_eq!(
            n, 0,
            "no redeliver-unacked frame should be queued before the virtual deadline"
        );
    }

    // Tick after the deadline. The state machine emits a
    // `CommandRedeliverUnacknowledgedMessages` frame with the timed-out id.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + Duration::from_millis(10_500));
        let mut tx_buf: Vec<u8> = Vec::new();
        let n = conn.poll_transmit(&mut tx_buf);
        assert!(
            n > 0,
            "ack-timeout sweep at virtual deadline must queue a redeliver-unacked frame"
        );
        // Decode the queued frame to confirm it is the expected
        // `CommandRedeliverUnacknowledgedMessages` for the right consumer.
        let mut src: Bytes = Bytes::copy_from_slice(&tx_buf[..n]);
        let frame = magnetar_proto::decode_one(&mut src).expect("decode redeliver frame");
        assert_eq!(
            frame.command.r#type,
            pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
            "expected RedeliverUnacknowledgedMessages, got {:?}",
            frame.command.r#type,
        );
        let redeliver = frame
            .command
            .redeliver_unacknowledged_messages
            .expect("redeliver body present");
        assert_eq!(redeliver.consumer_id, handle.0);
        assert_eq!(
            redeliver.message_ids.len(),
            1,
            "expected one timed-out message id, got {:?}",
            redeliver.message_ids,
        );
        assert_eq!(redeliver.message_ids[0].ledger_id, 7);
        assert_eq!(redeliver.message_ids[0].entry_id, 3);
    }
}
