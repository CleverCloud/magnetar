// SPDX-License-Identifier: Apache-2.0

//! Reconnects never resume from a locally submitted ack watermark (#398, #403).

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, ConnectionConfig, MessageId, SubscribeRequest, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

#[test]
fn reattach_uses_only_authoritative_start_positions() {
    assert_eq!(reattach_start_message_id(true), None);
    assert_eq!(
        reattach_start_message_id(false),
        Some(original_start().to_pb())
    );
}

fn reattach_start_message_id(durable: bool) -> Option<magnetar_proto::pb::MessageIdData> {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/durable-resume".to_owned(),
        subscription: "magnetar-test-durable-resume".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        durable,
        start_message_id: Some(original_start()),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    let mut success = BytesMut::new();
    encode_command(
        &mut success,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode subscribe success");
    conn.handle_bytes(t0, &success).expect("subscribe success");
    let _ = conn.poll_event();

    let local_high = MessageId {
        ledger_id: 9,
        entry_id: 9,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    let _ = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![local_high],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        t0,
    );
    let _ = conn.poll_transmit();

    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("reconnected");
    let _ = conn.poll_transmit();
    assert_eq!(conn.rebuild_consumers().len(), 1);
    let mut wire = conn.poll_transmit();
    let frame = decode_one(&mut wire).expect("reattach CommandSubscribe");
    let subscribe = frame.command.subscribe.expect("CommandSubscribe");
    subscribe.start_message_id
}

fn original_start() -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id: 2,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}
