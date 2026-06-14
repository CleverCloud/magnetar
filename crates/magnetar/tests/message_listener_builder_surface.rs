// SPDX-License-Identifier: Apache-2.0

//! Builder-surface completeness pin for the consumer push-delivery listener
//! (`ConsumerBuilder::message_listener` / `TypedConsumerBuilder::message_listener`,
//! ADR-0064).
//!
//! A partial wiring would let `message_listener(...)` silently drop on one
//! builder while the Java-parity matrix claims parity. This test drives the two
//! supported builder surfaces and asserts:
//!
//! 1. base `ConsumerBuilder` — `message_listener(...)` flips the listener slot (read via the
//!    `#[doc(hidden)]` `has_listener_for_test()` seam); a default builder has no listener.
//! 2. `TypedConsumerBuilder` — same, with a typed callback.
//! 3. `subscribe_with_listener()` on a builder with **no** listener fails fast with
//!    `PulsarError::Config` (the missing-listener guard) — proving the push path refuses to
//!    silently no-op.
//!
//! Uses the same idle TCP fake-broker pattern as
//! `chunk_bound_builder_surface.rs`: a real `PulsarClient` is needed to
//! instantiate the builders, but no subscribe reaches the wire for the
//! field-level assertions. The guard assertions DO reach the wire (subscribe
//! must succeed before the listener check), so the fake broker answers a
//! subscribe.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{IncomingMessage, MessageListener, PulsarClient, PulsarError, TypedMessageListener};
use magnetar_proto::schema::StringSchema;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Spawn a TCP fake-broker that answers `CommandConnect` -> `Connected` and
/// `CommandSubscribe` -> `Success`, then idles (never pushes a message).
async fn spawn_fake_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut read_buf = BytesMut::with_capacity(8 * 1024);
                let mut out_buf = BytesMut::with_capacity(8 * 1024);
                let mut probe = [0u8; 8 * 1024];
                loop {
                    loop {
                        let mut framed = Bytes::copy_from_slice(&read_buf);
                        let before = framed.len();
                        let frame = match decode_one(&mut framed) {
                            Ok(f) => f,
                            Err(FrameError::Incomplete { .. }) => break,
                            Err(_) => return,
                        };
                        let consumed = before - framed.len();
                        let _ = read_buf.split_to(consumed);
                        let ty = frame.command.r#type;
                        if ty == pb::base_command::Type::Connect as i32 {
                            let connected = pb::BaseCommand {
                                r#type: pb::base_command::Type::Connected as i32,
                                connected: Some(pb::CommandConnected {
                                    server_version: "magnetar-listener-fake".to_owned(),
                                    protocol_version: Some(21),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            };
                            out_buf.clear();
                            encode_command(&mut out_buf, &connected).expect("encode connected");
                            if stream.write_all(&out_buf).await.is_err() {
                                return;
                            }
                        } else if ty == pb::base_command::Type::Subscribe as i32 {
                            let rid = frame.command.subscribe.as_ref().map_or(0, |s| s.request_id);
                            let success = pb::BaseCommand {
                                r#type: pb::base_command::Type::Success as i32,
                                success: Some(pb::CommandSuccess {
                                    request_id: rid,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            };
                            out_buf.clear();
                            encode_command(&mut out_buf, &success).expect("encode success");
                            if stream.write_all(&out_buf).await.is_err() {
                                return;
                            }
                        }
                        // Flow / other commands: swallow, stay idle.
                    }
                    match stream.read(&mut probe).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => read_buf.extend_from_slice(&probe[..n]),
                    }
                }
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_listener_wired_on_both_single_topic_builders() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // 1. Base ConsumerBuilder — listener slot flips.
    let noop: MessageListener = Arc::new(|_m: &IncomingMessage| {});
    let base = client
        .consumer("persistent://public/default/ml-base")
        .subscription("g")
        .subscription_type(SubType::Exclusive)
        .message_listener(noop);
    assert!(
        base.has_listener_for_test(),
        "base ConsumerBuilder::message_listener must set the listener slot"
    );

    // Default builder (no setter) has no listener.
    assert!(
        !client
            .consumer("persistent://public/default/ml-base-default")
            .subscription("g")
            .has_listener_for_test(),
        "a ConsumerBuilder without message_listener() has no listener"
    );

    // 2. TypedConsumerBuilder — typed listener slot flips.
    let typed_noop: TypedMessageListener<StringSchema> = Arc::new(|_m| {});
    let typed = client
        .typed_consumer(
            "persistent://public/default/ml-typed",
            Arc::new(StringSchema::new()),
        )
        .subscription("g")
        .message_listener(typed_noop);
    assert!(
        typed.has_listener_for_test(),
        "TypedConsumerBuilder::message_listener must set the listener slot"
    );

    // 3. subscribe_with_listener() with no listener fails fast (guard).
    let err = client
        .consumer("persistent://public/default/ml-nolistener")
        .subscription("g")
        .subscription_type(SubType::Exclusive)
        .subscribe_with_listener()
        .await
        .expect_err("subscribe_with_listener with no listener must error");
    assert!(
        matches!(err, PulsarError::Config(_)),
        "missing listener must be a Config error, got: {err:?}"
    );
}
