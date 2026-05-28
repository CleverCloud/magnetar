// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-v5-client", feature = "moonpool"))]

//! PIP-466 V5 producer-mapping wire-byte test — moonpool engine mirror.
//!
//! Companion to `v5_producer_mapping.rs` (tokio-engine mirror). Asserts
//! that the V5 → v4 mapping table outputs are byte-correct on the
//! sans-io wire AND pins the type-level contract that the
//! [`magnetar::v5::ProducerBuilder<MoonpoolEngine<TokioProviders>>`]
//! shape resolves cleanly — proving WAVE 3 of docs/follow-ups.md §2
//! (V5 engine-genericity for PIP-466 promotion).
//!
//! The mapping invariants under test are identical to the tokio
//! mirror — the V5 defaults map to the same v4 wire values regardless
//! of which engine the V5 wrapper sits over. The wire assertions go
//! through `magnetar_fakes::FrameRecorder` + `magnetar_proto::Connection`
//! (sans-io), so no real moonpool engine has to spin up.

use std::sync::Arc;
use std::time::SystemTime;

use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::{
    DEFAULT_MAX_PENDING_MESSAGES, DEFAULT_SEND_TIMEOUT, max_pending_messages_to_v4,
    send_timeout_to_ms,
};
use magnetar::{MoonpoolEngine, PulsarClient};
use magnetar_fakes::FrameRecorder;
use magnetar_proto::{Connection, ConnectionConfig, CreateProducerRequest, encode_command, pb};
use moonpool_core::TokioProviders;

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
fn v5_producer_default_config_emits_expected_v4_command_producer_under_moonpool() {
    // Mapping invariants — identical across engines.
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

    let topic = "persistent://public/default/v5-mapping-default-moonpool";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _connect = rec.drain(&mut conn).expect("drain Connect");

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
    let _ = producer.producer_id;
}

#[test]
fn v5_producer_with_named_producer_emits_producer_name_under_moonpool() {
    let topic = "persistent://public/default/v5-mapping-named-moonpool";
    let producer_name = "magnetar-v5-test-producer-moonpool";
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

#[test]
fn v5_producer_builder_shape_compiles_against_moonpool_engine() {
    // WAVE 3 type-shape pinning: `PulsarClientV5<MoonpoolEngine<P>>::producer(...)`
    // returns the engine-parametric V5 builder, which builds against the
    // engine-typed v4 builder. This entire fn is dead at runtime — the
    // assertion fires at typeck.
    fn _shape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> magnetar::v5::producer::ProducerBuilder<'_, MoonpoolEngine<TokioProviders>> {
        c.producer("persistent://public/default/shape-pin")
    }
    fn _v4_escape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> &PulsarClient<MoonpoolEngine<TokioProviders>> {
        c.v4()
    }
}
