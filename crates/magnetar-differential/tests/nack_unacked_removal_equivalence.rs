// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for the
//! double-redelivery fix. A message that is BOTH negatively acknowledged AND
//! tracked by the `ack_timeout` (unacked-message) tracker must be redelivered
//! EXACTLY ONCE — and the two engines must agree on that outcome bit-for-bit.
//!
//! Why this drives the engines' `ConnectionShared` directly instead of the
//! `ScriptedBroker` runner: the bug is a virtual-clock + tracker interaction
//! (`negative_ack` then `handle_timeout` past both deadlines). The scripted-broker
//! runner cannot pin a fast deterministic nack-delay + ack-timeout boundary
//! through real wall-clock without flaking. Both engines wrap the SAME
//! `magnetar_proto::Connection` behind a `parking_lot::Mutex`, so feeding both an
//! identical synthetic frame sequence and comparing the emitted
//! `RedeliverUnacknowledgedMessages` frames is exactly the `EventStream`-parity
//! claim the harness exists to make — just at the proto seam the two engines
//! share, with a synthetic [`Instant`] both can pin.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, MessageId, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const NACK_DELAY: Duration = Duration::from_secs(2);

/// One observed redelivery: the consumer it targeted plus the `(ledger, entry,
/// batch_index)` triples it carried. This is the user-visible `EventStream` slice the
/// two engines must agree on for the nack + ack-timeout scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Redelivery {
    consumer_id: u64,
    ids: Vec<(u64, u64, i32)>,
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

/// Drive the shared nack + ack-timeout scenario over one engine's locked
/// `Connection` and collect the redelivery frames it queues. Engine-agnostic: the
/// caller hands in a `&mut Connection` obtained from either engine's
/// `ConnectionShared`, so the two runs are byte-for-byte comparable.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    // Handshake.
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    // Subscribe with BOTH knobs.
    let req = SubscribeRequest {
        topic: "persistent://public/default/nack-unacked-equiv".to_owned(),
        subscription: "sub-nack-unacked-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        ack_timeout: Some(ACK_TIMEOUT),
        negative_ack_redelivery_delay: Some(NACK_DELAY),
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(req);

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

    // Deliver one message; arm its ack-timeout deadline.
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
    conn.handle_bytes(t0, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();

    // Nack the delivered (normalised) id.
    let nacked_id = MessageId {
        ledger_id: 7,
        entry_id: 3,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    conn.negative_ack(handle, vec![nacked_id], t0);

    // Tick past both deadlines, then collect redelivery frames.
    conn.handle_timeout(t0 + Duration::from_secs(11));
    let mut src = conn.poll_transmit();
    let mut out = Vec::new();
    while !src.is_empty() {
        let decoded = decode_one(&mut src).expect("decode outbound frame");
        if decoded.command.r#type == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32
        {
            let body = decoded
                .command
                .redeliver_unacknowledged_messages
                .expect("redeliver body");
            out.push(Redelivery {
                consumer_id: body.consumer_id,
                ids: body
                    .message_ids
                    .iter()
                    .map(|m| (m.ledger_id, m.entry_id, m.batch_index.unwrap_or(-1)))
                    .collect(),
            });
        }
    }
    out
}

#[test]
fn nack_unacked_single_redelivery_event_streams_agree() {
    let t0 = Instant::now();

    // Tokio engine.
    let tokio_stream = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // Moonpool engine.
    let moonpool_stream = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: both engines redeliver the nacked +
    // ack-timeout-tracked message exactly once, with identical frame shape.
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the nack + ack-timeout redelivery stream"
    );
    assert_eq!(
        tokio_stream.len(),
        1,
        "exactly one redelivery is expected on both engines, got {tokio_stream:?}"
    );
}
