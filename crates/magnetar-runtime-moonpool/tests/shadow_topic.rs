// SPDX-License-Identifier: Apache-2.0

//! PIP-180 / ADR-0033 shadow-topic integration — moonpool engine.
//!
//! Mirror of `magnetar-runtime-tokio/tests/shadow_topic.rs`. Drives
//! `magnetar_proto::Connection` directly with synthetic broker frames so
//! the same wire trace exercises both engines. The moonpool engine's
//! public `Producer::send_with_source_message_id` and
//! `Consumer::set_shadow_source` are thin delegates over the sans-io
//! methods these tests touch — no real I/O, no provider plumbing
//! required.
//!
//! Parity required by ADR-0024 — count of tests must match the tokio
//! side 1:1 (`cargo xtask check-runtime-test-parity`).

#![allow(clippy::expect_used)]

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, CreateProducerRequest,
    MessageId, ProducerHandle, RequestId, ShadowTopicMetadata, SubscribeRequest, encode_command,
    encode_payload, pb,
};

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
    let mut conn = Connection::new(ConnectionConfig::default());
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
    // Ack the producer-open round trip so the producer is in the "ready" state.
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
    // Drain the producer's outbound CommandProducer so it doesn't bleed into
    // later wire captures.
    let mut sink = Vec::new();
    let _ = conn.poll_transmit(&mut sink);
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
    let mut sink = Vec::new();
    let _ = conn.poll_transmit(&mut sink);
    handle
}

/// Peek the next request id the state machine WILL allocate, without
/// consuming it — used so the test can pre-build the broker's response.
fn next_unacked_request_id(conn: &mut Connection) -> RequestId {
    RequestId(conn.peek_next_request_id_for_test())
}

/// Synthetic `CommandMessage` frame carrying `replicated_from` set to a
/// source cluster (Pulsar broker behaviour on shadow-presented entries).
fn shadow_message_frame(
    consumer: ConsumerHandle,
    ledger: u64,
    entry: u64,
    payload: &[u8],
    replicated_from: Option<&str>,
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
    let mut meta = pb::MessageMetadata {
        producer_name: "source-producer".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000,
        ..Default::default()
    };
    meta.replicated_from = replicated_from.map(str::to_owned);
    let mut buf = BytesMut::new();
    encode_payload(&mut buf, &cmd, &meta, payload).expect("encode payload frame");
    buf
}

/// Pull the most recently emitted `CommandSend` (and its surrounding
/// `MessageMetadata`) from the connection's outbound queue. Returns the
/// pair so tests can assert on `CommandSend.message_id`.
fn pop_last_command_send(conn: &mut Connection) -> (pb::CommandSend, pb::MessageMetadata) {
    let mut sink = Vec::new();
    let _ = conn.poll_transmit(&mut sink);
    let mut cursor = Bytes::from(sink);
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
    last.expect("at least one CommandSend in outbound")
}

// ------------------------- 8 PIP-180 tests -------------------------

#[tokio::test(flavor = "current_thread")]
async fn producer_send_with_source_id_emits_field() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_producer(&mut conn, "persistent://public/default/shadow-t", at);
    let source_id = MessageId {
        ledger_id: 99,
        entry_id: 42,
        partition: 0,
        batch_index: -1,
        batch_size: 0,
    };
    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"replicated"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 10,
        num_messages: 1,
        txn_id: None,
        source_message_id: Some(source_id),
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");
    let (send, _meta) = pop_last_command_send(&mut conn);
    let pb_mid = send.message_id.expect("CommandSend.message_id populated");
    assert_eq!(MessageId::from_pb(&pb_mid), source_id);
}

#[tokio::test(flavor = "current_thread")]
async fn producer_send_normal_does_not_emit_field() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_producer(&mut conn, "persistent://public/default/t", at);
    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"regular"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 7,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");
    let (send, _meta) = pop_last_command_send(&mut conn);
    assert!(
        send.message_id.is_none(),
        "regular send must leave CommandSend.message_id absent"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_observes_shadow_from_variant() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_consumer(
        &mut conn,
        "persistent://public/default/shadow-t",
        "sub-shadow",
        at,
    );
    // Tell the sans-io state this consumer is shadow-attached.
    conn.consumer_mut(handle)
        .expect("consumer alive")
        .set_shadow_metadata(ShadowTopicMetadata {
            source_topic: "persistent://public/default/source-t".to_owned(),
        });
    let frame = shadow_message_frame(handle, 7, 42, b"payload", Some("dc-east"));
    conn.handle_bytes(at, &frame).expect("apply CommandMessage");
    let mut got_shadow_event = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::MessageReceivedFromShadow {
            handle: h,
            source_topic,
            source_message_id,
            shadow_message_id,
            ..
        } = ev
        {
            assert_eq!(h, handle);
            assert_eq!(source_topic, "persistent://public/default/source-t");
            assert_eq!(source_message_id, shadow_message_id);
            got_shadow_event = true;
            break;
        }
    }
    assert!(
        got_shadow_event,
        "expected MessageReceivedFromShadow event on shadow consumer"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_message_id_equals_source_message_id() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_consumer(&mut conn, "persistent://public/default/shadow-t", "sub", at);
    conn.consumer_mut(handle)
        .unwrap()
        .set_shadow_metadata(ShadowTopicMetadata {
            source_topic: "persistent://public/default/source-t".to_owned(),
        });
    let frame = shadow_message_frame(handle, 42, 7, b"x", Some("src-cluster"));
    conn.handle_bytes(at, &frame).unwrap();
    let mut matched = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::MessageReceivedFromShadow {
            source_message_id,
            shadow_message_id,
            ..
        } = ev
        {
            // PIP-180 structural-equality contract — same (ledger, entry, ...).
            assert_eq!(source_message_id, shadow_message_id);
            assert_eq!(source_message_id.ledger_id, 42);
            assert_eq!(source_message_id.entry_id, 7);
            matched = true;
            break;
        }
    }
    assert!(matched, "expected the shadow event with matching ids");
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_pre_populates_shadow_metadata() {
    // PIP-180 hint flow — subscribe + admin REST lookup => set_shadow_metadata.
    // Here we simulate the runtime's behaviour by calling the sans-io setter
    // directly; the high-level Client::subscribe wires this from
    // magnetar-admin's `get_shadow_source` lookup.
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_consumer(&mut conn, "persistent://public/default/shadow-t", "sub", at);
    assert!(conn.consumer(handle).unwrap().shadow_metadata.is_none());
    conn.consumer_mut(handle)
        .unwrap()
        .set_shadow_metadata(ShadowTopicMetadata {
            source_topic: "persistent://public/default/source-t".to_owned(),
        });
    let meta = conn
        .consumer(handle)
        .unwrap()
        .shadow_metadata
        .as_ref()
        .expect("metadata installed");
    assert_eq!(meta.source_topic, "persistent://public/default/source-t");
}

#[tokio::test(flavor = "current_thread")]
async fn producer_send_with_source_id_bypasses_batching() {
    // Replicator-style sends are non-batched (mirrors Java
    // org.apache.pulsar.broker.service.persistent.Replicator). Even with
    // batching ENABLED on the producer, `source_message_id: Some` routes
    // the send through `emit_single` and produces one frame per send.
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_producer(&mut conn, "persistent://public/default/shadow-t", at);
    // Force batching on by editing the producer state directly (the
    // `CreateProducerRequest` default disables it; we toggle here to make
    // the bypass explicit).
    if let Some(p) = conn.producer_mut(handle) {
        p.batching_enabled = true;
        p.max_messages_in_batch = 100;
        p.max_batch_size_bytes = 1_000_000;
    }
    let source_id = MessageId {
        ledger_id: 1,
        entry_id: 1,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"data"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 4,
        num_messages: 1,
        txn_id: None,
        source_message_id: Some(source_id),
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");
    let (send, _) = pop_last_command_send(&mut conn);
    assert!(
        send.message_id.is_some(),
        "shadow send must emit immediately"
    );
    // Batch container must be empty — replicator path skipped it.
    assert_eq!(conn.producer_batch_len(handle), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn producer_chunked_send_with_source_id_propagates_to_every_chunk() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    // Configure a tiny max_message_size + chunking to force the chunked
    // path. The state-machine config exposes both knobs.
    let handle = open_producer(&mut conn, "persistent://public/default/shadow-big", at);
    if let Some(p) = conn.producer_mut(handle) {
        p.max_message_size = 8;
        p.chunking_enabled = true;
    }
    let source_id = MessageId {
        ledger_id: 7,
        entry_id: 11,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    let msg = magnetar_proto::producer::OutgoingMessage {
        payload: Bytes::from_static(b"123456789012345678"), // 18 bytes > 8 → 3 chunks
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 18,
        num_messages: 1,
        txn_id: None,
        source_message_id: Some(source_id),
    };
    conn.send(handle, msg, 1_700_000_000, at).expect("send ok");
    // Drain ALL outbound and count CommandSend frames carrying the
    // source-id field. Every chunk frame must carry it.
    let mut sink = Vec::new();
    let _ = conn.poll_transmit(&mut sink);
    let mut cursor = Bytes::from(sink);
    let mut chunk_count = 0u32;
    let mut stamped_count = 0u32;
    while !cursor.is_empty() {
        let frame = magnetar_proto::decode_one(&mut cursor).expect("decode");
        if frame.command.r#type == pb::base_command::Type::Send as i32 {
            let send = frame.command.send.as_ref().unwrap();
            if send.is_chunk == Some(true) {
                chunk_count += 1;
                if send
                    .message_id
                    .as_ref()
                    .is_some_and(|m| MessageId::from_pb(m) == source_id)
                {
                    stamped_count += 1;
                }
            }
        }
    }
    assert!(chunk_count >= 2, "expected at least 2 chunks");
    assert_eq!(
        chunk_count, stamped_count,
        "every chunk must carry the source message id"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_classified_by_replicated_from_only_when_shadow_attached() {
    // Mirror of the unit test consumer_emits_message_received_for_non_shadow:
    // a non-shadow consumer never escalates a replicated_from message to the
    // shadow event variant.
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let handle = open_consumer(
        &mut conn,
        "persistent://public/default/regular-t",
        "sub",
        at,
    );
    // DO NOT install shadow metadata. Feed a message with replicated_from set.
    let frame = shadow_message_frame(handle, 7, 42, b"x", Some("dc-west"));
    conn.handle_bytes(at, &frame).unwrap();
    let mut saw_regular_message = false;
    while let Some(ev) = conn.poll_event() {
        if matches!(ev, ConnectionEvent::Message { .. }) {
            saw_regular_message = true;
        }
        assert!(
            !matches!(ev, ConnectionEvent::MessageReceivedFromShadow { .. }),
            "non-shadow consumer must NOT emit MessageReceivedFromShadow"
        );
    }
    assert!(saw_regular_message, "expected a regular Message event");
}
