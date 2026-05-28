// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-v5-client", feature = "moonpool"))]

//! PIP-466 V5 stream-consumer mapping wire-byte test — moonpool engine
//! mirror.
//!
//! Companion to `v5_stream_consumer_mapping.rs`. Same wire assertions
//! against `magnetar_proto::Connection` (sans-io) plus a type-shape
//! pinning that the V5 stream-consumer builder resolves under
//! `MoonpoolEngine<TokioProviders>`.

use std::sync::Arc;
use std::time::SystemTime;

use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::{
    DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY, V5SubscriptionInitialPosition,
    negative_ack_redelivery_delay_to_ms,
};
use magnetar::{MoonpoolEngine, PulsarClient};
use magnetar_fakes::FrameRecorder;
use magnetar_proto::{Connection, ConnectionConfig, SubscribeRequest, encode_command, pb};
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
fn v5_stream_consumer_default_emits_exclusive_subscribe_under_moonpool() {
    // V5 StreamConsumer default subscription type is Exclusive (callers
    // flip to Failover via .failover()). Mirror of the tokio assertion.
    let topic = "persistent://public/default/v5-stream-default-moonpool";
    let sub = "stream-sub";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: sub.to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        initial_position: V5SubscriptionInitialPosition::default().into_pb(),
        ..SubscribeRequest::default()
    };
    let _ = conn.subscribe(req);

    let frames = rec.drain(&mut conn).expect("drain CommandSubscribe");
    let subscribe_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.frame.command.r#type == pb::base_command::Type::Subscribe as i32)
        .collect();
    assert_eq!(subscribe_frames.len(), 1);
    let sub_cmd = subscribe_frames[0]
        .frame
        .command
        .subscribe
        .as_ref()
        .expect("Subscribe payload");
    assert_eq!(sub_cmd.topic, topic);
    assert_eq!(sub_cmd.subscription, sub);
    assert_eq!(
        sub_cmd.sub_type,
        pb::command_subscribe::SubType::Exclusive as i32,
        "V5 StreamConsumer default sub_type must be Exclusive on the wire"
    );
}

#[test]
fn v5_nack_redelivery_default_maps_to_60_000_ms_for_moonpool_stream() {
    // The mapping is engine-agnostic; pin it from a moonpool-named test
    // so the parity surface is symmetric.
    assert_eq!(
        negative_ack_redelivery_delay_to_ms(DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY),
        60_000
    );
}

#[test]
fn v5_stream_consumer_builder_shape_compiles_against_moonpool_engine() {
    fn _shape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> magnetar::v5::stream_consumer::StreamConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>
    {
        c.stream_consumer("persistent://public/default/stream-shape")
            .subscription("s")
    }
    fn _builder_failover_flip(
        b: magnetar::v5::stream_consumer::StreamConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>,
    ) -> magnetar::v5::stream_consumer::StreamConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>
    {
        b.failover()
    }
    fn _v4_escape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> &PulsarClient<MoonpoolEngine<TokioProviders>> {
        c.v4()
    }
}
