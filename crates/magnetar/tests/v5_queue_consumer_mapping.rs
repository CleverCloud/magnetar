// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "experimental-v5-client")]

//! PIP-466 V5 queue-consumer-mapping wire-byte test.
//!
//! Queue consumers default to `SubType::Shared` (mirroring Java
//! `QueueConsumerBuilder`); the V5 builder also exposes a
//! `key_shared()` constructor that flips to `SubType::KeyShared` and
//! attaches a `KeySharedMeta`. Asserts those translations show up on
//! the `CommandSubscribe` wire frame as expected.
//!
//! Part of the 5-test PIP-466 mapping suite. Companion to
//! `v5_producer_mapping.rs` and `v5_stream_consumer_mapping.rs`.

use std::sync::Arc;
use std::time::SystemTime;

use magnetar_fakes::FrameRecorder;
use magnetar_proto::{
    Connection, ConnectionConfig, KeySharedConfig, SubscribeRequest, encode_command, pb,
};

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
fn v5_queue_consumer_default_emits_shared_subscribe() {
    // V5 QueueConsumerBuilder default: sub_type=Shared.
    let topic = "persistent://public/default/v5-queue-default";
    let subscription = "v5-queue-sub";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: subscription.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        ..SubscribeRequest::default()
    };
    let _ = conn.subscribe(req);

    let frames = rec.drain(&mut conn).expect("drain CommandSubscribe");
    assert_eq!(frames.len(), 1);
    let subscribe = frames[0]
        .frame
        .command
        .subscribe
        .as_ref()
        .expect("CommandSubscribe payload");
    assert_eq!(subscribe.topic, topic);
    assert_eq!(subscribe.subscription, subscription);
    assert_eq!(
        subscribe.sub_type,
        pb::command_subscribe::SubType::Shared as i32,
        "queue-consumer default sub_type is Shared"
    );
    assert!(
        subscribe.key_shared_meta.is_none(),
        "non-key_shared sub_type must NOT carry KeySharedMeta"
    );
}

#[test]
fn v5_queue_consumer_key_shared_emits_key_shared_meta() {
    // V5 QueueConsumerBuilder::key_shared() → SubType::KeyShared with
    // a default KeySharedConfig (auto-split mode, no sticky ranges,
    // strict ordering).
    let topic = "persistent://public/default/v5-queue-key-shared";
    let subscription = "v5-queue-key-shared-sub";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let key_shared = KeySharedConfig::default();
    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: subscription.to_owned(),
        sub_type: pb::command_subscribe::SubType::KeyShared,
        key_shared: Some(key_shared),
        ..SubscribeRequest::default()
    };
    let _ = conn.subscribe(req);

    let frames = rec.drain(&mut conn).expect("drain CommandSubscribe");
    assert_eq!(frames.len(), 1);
    let subscribe = frames[0]
        .frame
        .command
        .subscribe
        .as_ref()
        .expect("CommandSubscribe payload");
    assert_eq!(
        subscribe.sub_type,
        pb::command_subscribe::SubType::KeyShared as i32,
        "V5 .key_shared() flips wire sub_type to KeyShared"
    );
    let meta = subscribe
        .key_shared_meta
        .as_ref()
        .expect("KeySharedMeta must be present for sub_type=KeyShared");
    // Default KeySharedConfig: auto-split mode (0), no sticky ranges,
    // strict ordering (allow_out_of_order_delivery=false).
    assert_eq!(
        meta.key_shared_mode, 0,
        "default key_shared_mode is AutoSplit (wire enum value 0)"
    );
    assert!(
        meta.hash_ranges.is_empty(),
        "default KeySharedConfig carries no sticky hash ranges"
    );
    assert_eq!(
        meta.allow_out_of_order_delivery,
        Some(false),
        "default KeySharedConfig is strict-ordered"
    );
}

#[test]
fn v5_queue_consumer_consumer_name_propagates() {
    // V5 QueueConsumerBuilder::consumer_name(...) → wire
    // CommandSubscribe.consumer_name.
    let topic = "persistent://public/default/v5-queue-named";
    let subscription = "v5-queue-named-sub";
    let consumer_name = "magnetar-v5-queue-test";
    let mut conn = fresh_connected();
    let mut rec = FrameRecorder::new();
    let _ = rec.drain(&mut conn).expect("drain Connect");

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: subscription.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        consumer_name: Some(consumer_name.to_owned()),
        ..SubscribeRequest::default()
    };
    let _ = conn.subscribe(req);

    let frames = rec.drain(&mut conn).expect("drain CommandSubscribe");
    let subscribe = frames[0]
        .frame
        .command
        .subscribe
        .as_ref()
        .expect("CommandSubscribe payload");
    assert_eq!(
        subscribe.consumer_name.as_deref(),
        Some(consumer_name),
        "V5 .consumer_name(...) propagates to wire consumer_name"
    );
}
