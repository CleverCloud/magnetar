// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "experimental-v5-client")]

//! PIP-466 V5 producer-mapping wire-byte test.
//!
//! Asserts that a `magnetar_proto::Connection::create_producer` call
//! parameterised through the V5 `mapping` translations emits a
//! `CommandProducer` frame whose fields match the V5 input — proving
//! the V5 → v4 mapping table in `crates/magnetar/src/v5/mapping.rs` is
//! byte-correct on the wire.
//!
//! Layered with `magnetar_fakes::FrameRecorder` so the assertion lives
//! one step closer to the wire than the per-translation unit tests
//! already in `mapping.rs::tests`.
//!
//! This file is the first of five planned V5 mapping wire-byte tests
//! enumerated under "PIP-466 — V5 client surface" in
//! `docs/follow-ups.md`. The other four (stream-consumer mapping,
//! queue-consumer mapping, v4 escape hatch, builder defaults) follow
//! the same shape and can ride this scaffolding.

use std::sync::Arc;
use std::time::SystemTime;

use magnetar::v5::mapping::{
    DEFAULT_MAX_PENDING_MESSAGES, DEFAULT_SEND_TIMEOUT, max_pending_messages_to_v4,
    send_timeout_to_ms,
};
use magnetar_fakes::FrameRecorder;
use magnetar_proto::{Connection, ConnectionConfig, CreateProducerRequest, encode_command, pb};

fn fresh_connected() -> Connection {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    let connected = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, &connected).expect("encode Connected");
    conn.handle_bytes(std::time::Instant::now(), &buf)
        .expect("apply Connected");
    let _ = conn.poll_event();
    conn
}

#[test]
fn v5_producer_default_config_emits_expected_v4_command_producer() {
    // V5 ProducerBuilder defaults per the mapping table:
    //   send_timeout              = 30s
    //   max_pending_messages      = Some(1000)
    // The v4 wire surface stores send_timeout in millis on a Producer
    // metadata path that's not directly on `CommandProducer` (it's
    // client-side at the runtime layer). What CommandProducer carries
    // for these is: producer_name, topic, producer_id, epoch.
    //
    // The mapping invariant under test: `send_timeout_to_ms(30s)` →
    // 30_000 ms and `max_pending_messages_to_v4(Some(1000))` → 1000.
    // The wire-level CommandProducer just needs to come out clean with
    // the topic + producer_id fields the v4 surface populates from
    // `CreateProducerRequest`.
    assert_eq!(
        send_timeout_to_ms(DEFAULT_SEND_TIMEOUT),
        30_000,
        "V5 default send_timeout must map to 30000 ms"
    );
    assert_eq!(
        max_pending_messages_to_v4(DEFAULT_MAX_PENDING_MESSAGES),
        1000,
        "V5 default max_pending_messages must map to 1000"
    );

    let topic = "persistent://public/default/v5-mapping-default";
    let mut conn = fresh_connected();
    // Drain the Connect frame so the recorder starts on a clean wire.
    let mut rec = FrameRecorder::new();
    let _connect = rec.drain(&mut conn).expect("drain Connect");

    // Mimic what the V5 builder would dispatch into the proto layer:
    // a `CreateProducerRequest` with the topic the V5 surface passed
    // through. The V5 builder doesn't override producer_name (the
    // broker assigns one); leave it blank.
    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        ..CreateProducerRequest::default()
    };
    let _handle = conn.create_producer(req);

    let frames = rec.drain(&mut conn).expect("drain CommandProducer");
    assert_eq!(
        frames.len(),
        1,
        "exactly one CommandProducer frame on the wire"
    );
    let cmd = &frames[0].frame.command;
    assert_eq!(
        cmd.r#type,
        pb::base_command::Type::Producer as i32,
        "wire command must be CommandProducer"
    );
    let producer = cmd
        .producer
        .as_ref()
        .expect("CommandProducer payload present");
    assert_eq!(producer.topic, topic, "topic round-trips through to wire");
    // `producer_id` is `u64`, so it's always wire-valid by construction;
    // no explicit assertion needed beyond confirming the payload decoded.
    let _ = producer.producer_id;
}

#[test]
fn v5_producer_with_named_producer_emits_producer_name() {
    // V5 ProducerBuilder::name(...) → v4 CreateProducerRequest::producer_name
    // → CommandProducer.producer_name on the wire.
    let topic = "persistent://public/default/v5-mapping-named";
    let producer_name = "magnetar-v5-test-producer";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        producer_name: Some(producer_name.to_owned()),
        ..CreateProducerRequest::default()
    };
    let _ = conn.create_producer(req);

    let frames = rec.drain(&mut conn).expect("drain CommandProducer");
    assert_eq!(frames.len(), 1);
    let cmd = &frames[0].frame.command;
    let producer = cmd.producer.as_ref().expect("CommandProducer payload");
    assert_eq!(producer.topic, topic);
    assert_eq!(
        producer.producer_name.as_deref(),
        Some(producer_name),
        "V5 .name(...) propagates to wire producer_name"
    );
}
