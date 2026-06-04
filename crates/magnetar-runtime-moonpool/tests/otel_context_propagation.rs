// SPDX-License-Identifier: Apache-2.0

//! ADR-0053 — OpenTelemetry context propagation wire-level tests (moonpool engine).
//!
//! Mirror of `magnetar-runtime-tokio/tests/otel_context_propagation.rs`.
//! Drives `magnetar_proto::Connection` directly with synthetic broker frames
//! to verify that message properties (including W3C `traceparent`/`tracestate`)
//! survive the send/receive path at the wire level.
//!
//! Parity required by ADR-0024 — count of tests must match the tokio
//! side 1:1 (`cargo xtask check-runtime-test-parity`).
//!
//! The companion layers are:
//! - `crates/magnetar-proto/src/conn.rs` (property round-trip unit test)
//! - `crates/magnetar-runtime-tokio/tests/otel_context_propagation.rs`
//! - `crates/magnetar-differential/tests/otel_context_propagation_equivalence.rs`
//! - `crates/magnetar/tests/e2e_otel_context_propagation.rs`

#![allow(clippy::expect_used)]

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, CreateProducerRequest,
    ProducerHandle, RequestId, SubscribeRequest, encode_command, encode_payload, pb,
};

// ---------------------------------------------------------------------------
// helpers — identical to other integration tests in this crate
// ---------------------------------------------------------------------------

fn connected_frame() -> BytesMut {
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

fn handshake_complete(at: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    let frame = connected_frame();
    conn.handle_bytes(at, &frame).expect("connected");
    while let Some(_e) = conn.poll_event() {}
    conn
}

fn open_producer(conn: &mut Connection, topic: &str, at: Instant) -> ProducerHandle {
    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    };
    let handle = conn.create_producer(req);
    let request_id = next_unacked_request_id(conn);
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id: request_id.0,
            producer_name: format!("magnetar-test-{}", handle.0),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
    while let Some(_e) = conn.poll_event() {}
    let _ = conn.poll_transmit();
    handle
}

fn open_consumer(
    conn: &mut Connection,
    topic: &str,
    subscription: &str,
    at: Instant,
) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: subscription.to_owned(),
        receiver_queue_size: 16,
        durable: true,
        ..Default::default()
    };
    let request_id = next_unacked_request_id(conn);
    let handle = conn.subscribe(req);
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: request_id.0,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(at, &buf).expect("apply CommandSuccess");
    while let Some(_e) = conn.poll_event() {}
    let _ = conn.poll_transmit();
    handle
}

fn next_unacked_request_id(conn: &mut Connection) -> RequestId {
    RequestId(conn.peek_next_request_id_for_test())
}

fn pop_last_command_send(conn: &mut Connection) -> (pb::CommandSend, pb::MessageMetadata) {
    let mut cursor = conn.poll_transmit();
    let mut last: Option<(pb::CommandSend, pb::MessageMetadata)> = None;
    while !cursor.is_empty() {
        let frame = magnetar_proto::decode_one(&mut cursor).expect("decode wire frame");
        if frame.command.r#type == pb::base_command::Type::Send as i32 {
            if let Some(send) = frame.command.send.clone() {
                let meta = frame
                    .payload
                    .as_ref()
                    .map(|p| p.metadata.clone())
                    .unwrap_or_default();
                last = Some((send, meta));
            }
        }
    }
    last.expect("at least one CommandSend")
}

fn message_frame_with_properties(
    consumer: ConsumerHandle,
    ledger: u64,
    entry: u64,
    payload: &[u8],
    properties: Vec<pb::KeyValue>,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: consumer.0,
            message_id: pb::MessageIdData {
                ledger_id: ledger,
                entry_id: entry,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: vec![],
                batch_size: Some(0),
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "otel-test-producer".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000,
        properties,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_payload(&mut buf, &cmd, &meta, payload).expect("encode payload frame");
    buf
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

/// W3C `traceparent` / `tracestate` properties set on an `OutgoingMessage`
/// appear in the wire frame's `MessageMetadata.properties`.
#[test]
fn send_with_otel_properties_appears_in_wire_frame() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_producer(&mut conn, "persistent://public/default/otel-wire-t", at);

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";
    let tracestate = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE";

    let mut metadata = pb::MessageMetadata::default();
    metadata.properties.push(pb::KeyValue {
        key: "traceparent".to_owned(),
        value: traceparent.to_owned(),
    });
    metadata.properties.push(pb::KeyValue {
        key: "tracestate".to_owned(),
        value: tracestate.to_owned(),
    });

    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"hello-otel"),
        metadata,
        uncompressed_size: 10,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");

    let (_send, meta) = pop_last_command_send(&mut conn);
    let tp = meta
        .properties
        .iter()
        .find(|kv| kv.key == "traceparent")
        .expect("traceparent in wire metadata");
    assert_eq!(tp.value, traceparent);

    let ts = meta
        .properties
        .iter()
        .find(|kv| kv.key == "tracestate")
        .expect("tracestate in wire metadata");
    assert_eq!(ts.value, tracestate);
}

/// Inbound message with `traceparent` / `tracestate` properties emits them
/// in the `IncomingMessage` event's metadata.
#[test]
fn receive_with_otel_properties_preserves_them() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let consumer = open_consumer(
        &mut conn,
        "persistent://public/default/otel-rx-t",
        "otel-sub",
        at,
    );

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";
    let tracestate = "rojo=00f067aa0ba902b7";

    let frame = message_frame_with_properties(
        consumer,
        1,
        0,
        b"hello-consumer",
        vec![
            pb::KeyValue {
                key: "traceparent".to_owned(),
                value: traceparent.to_owned(),
            },
            pb::KeyValue {
                key: "tracestate".to_owned(),
                value: tracestate.to_owned(),
            },
        ],
    );
    conn.handle_bytes(at, &frame).expect("handle message");

    let mut found = false;
    while let Some(event) = conn.poll_event() {
        if let ConnectionEvent::Message { message: msg, .. } = event {
            let tp = msg
                .metadata
                .properties
                .iter()
                .find(|kv| kv.key == "traceparent")
                .expect("traceparent preserved");
            assert_eq!(tp.value, traceparent);

            let ts = msg
                .metadata
                .properties
                .iter()
                .find(|kv| kv.key == "tracestate")
                .expect("tracestate preserved");
            assert_eq!(ts.value, tracestate);
            found = true;
        }
    }
    assert!(found, "expected an IncomingMessage event");
}

/// Message with no `OTel` properties sends cleanly — no properties leak in.
#[test]
fn send_without_otel_properties_is_clean() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_producer(&mut conn, "persistent://public/default/otel-none-t", at);

    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"no-otel"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 7,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");

    let (_send, meta) = pop_last_command_send(&mut conn);
    assert!(
        !meta.properties.iter().any(|kv| kv.key == "traceparent"),
        "no traceparent without OTel"
    );
}
