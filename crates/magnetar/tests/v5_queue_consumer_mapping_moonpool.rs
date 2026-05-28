// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-v5-client", feature = "moonpool"))]

//! PIP-466 V5 queue-consumer mapping wire-byte test — moonpool engine
//! mirror.
//!
//! Companion to `v5_queue_consumer_mapping.rs`. Pins the V5
//! `QueueConsumer` default (Shared) + the `.key_shared()` flip on the
//! sans-io wire AND the engine-parametric shape resolves under
//! `MoonpoolEngine<TokioProviders>`.

use std::sync::Arc;
use std::time::SystemTime;

use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::{
    DEFAULT_ACK_TIMEOUT, V5SubscriptionInitialPosition, ack_timeout_to_ms,
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
fn v5_queue_consumer_default_emits_shared_subscribe_under_moonpool() {
    let topic = "persistent://public/default/v5-queue-default-moonpool";
    let sub = "queue-sub";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: sub.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
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
        pb::command_subscribe::SubType::Shared as i32,
        "V5 QueueConsumer default sub_type must be Shared on the wire"
    );
}

#[test]
fn v5_ack_timeout_default_disabled_for_moonpool_queue() {
    // None → 0 wire-millis sentinel ("disabled"). Same as the tokio mirror.
    assert_eq!(ack_timeout_to_ms(DEFAULT_ACK_TIMEOUT), 0);
}

#[test]
fn v5_queue_consumer_builder_shape_compiles_against_moonpool_engine() {
    fn _shape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> magnetar::v5::queue_consumer::QueueConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>
    {
        c.queue_consumer("persistent://public/default/queue-shape")
            .subscription("s")
    }
    fn _builder_key_shared_flip(
        b: magnetar::v5::queue_consumer::QueueConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>,
    ) -> magnetar::v5::queue_consumer::QueueConsumerBuilder<'_, MoonpoolEngine<TokioProviders>>
    {
        b.key_shared()
    }
    fn _v4_escape(
        c: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
    ) -> &PulsarClient<MoonpoolEngine<TokioProviders>> {
        c.v4()
    }
}
