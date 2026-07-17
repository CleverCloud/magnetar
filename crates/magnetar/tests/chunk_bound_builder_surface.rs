// SPDX-License-Identifier: Apache-2.0

//! Builder-surface completeness pin for the bounded chunk-reassembly knobs
//! (`max_pending_chunked_message`, `auto_ack_oldest_chunked_message_on_queue_full`,
//! `expire_time_of_incomplete_chunked_message`).
//!
//! A partial wiring would leave some consumer constructors unbounded while the
//! Java-parity matrix claims parity. This test drives EACH of the five consumer
//! builder surfaces and asserts the knobs are threaded into the request /
//! template that ultimately seeds `ConsumerState`:
//!
//! 1. base `ConsumerBuilder` — read the resolved `SubscribeRequest` via the `#[doc(hidden)]`
//!    `request_snapshot()` seam.
//! 2. `TypedConsumerBuilder` — delegates to the base builder; read its own fields via the
//!    `chunk_knobs_for_test()` seam.
//! 3. `MultiTopicsConsumerBuilder` — propagates via `ConsumerTemplate`.
//! 4. `PatternConsumerBuilder` — propagates via `ConsumerTemplate`.
//! 5. `PartitionedConsumerBuilder` — delegates to the multi-topics builder.
//!
//! Uses the same idle TCP fake-broker as `builder_encryption_guardrail.rs`: a
//! real `PulsarClient` is needed to instantiate the builders, but no subscribe
//! ever reaches the wire — the assertions read the builder state directly.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar_proto::schema::StringSchema;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CAP: usize = 3;
const EXPIRE: Duration = Duration::from_secs(45);

/// Spawn a TCP fake-broker that answers a single `CommandConnect` with
/// `CommandConnected` and then idles. No subscribe reaches the wire — the test
/// reads builder state directly.
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
                        if frame.command.r#type == pb::base_command::Type::Connect as i32 {
                            let connected = pb::BaseCommand {
                                r#type: pb::base_command::Type::Connected as i32,
                                connected: Some(pb::CommandConnected {
                                    server_version: "magnetar-chunk-bound-fake".to_owned(),
                                    protocol_version: Some(21),
                                    max_message_size: Some(5 * 1024 * 1024),
                                    feature_flags: Some(pb::FeatureFlags::default()),
                                }),
                                ..Default::default()
                            };
                            encode_command(&mut out_buf, &connected)
                                .expect("encode CommandConnected");
                        }
                    }
                    if !out_buf.is_empty() {
                        if stream.write_all(&out_buf).await.is_err() {
                            return;
                        }
                        if stream.flush().await.is_err() {
                            return;
                        }
                        out_buf.clear();
                    }
                    match stream.read_buf(&mut read_buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_five_consumer_builders_thread_chunk_bounds() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // 1. Base ConsumerBuilder — read the resolved SubscribeRequest.
    let base = client
        .consumer("persistent://public/default/chunk-bound-base")
        .subscription("g")
        .subscription_type(SubType::Exclusive)
        .max_pending_chunked_message(CAP)
        .auto_ack_oldest_chunked_message_on_queue_full(true)
        .expire_time_of_incomplete_chunked_message(EXPIRE);
    let req = base.request_snapshot();
    assert_eq!(req.max_pending_chunked_message, CAP);
    assert!(req.auto_ack_oldest_chunked_message_on_queue_full);
    assert_eq!(req.expire_time_of_incomplete_chunked_message, Some(EXPIRE));

    // The default request (no setters) must carry the Java-matching defaults.
    let default_req = client
        .consumer("persistent://public/default/chunk-bound-default")
        .subscription("g")
        .request_snapshot()
        .clone();
    assert_eq!(default_req.max_pending_chunked_message, 10);
    assert!(!default_req.auto_ack_oldest_chunked_message_on_queue_full);
    assert_eq!(
        default_req.expire_time_of_incomplete_chunked_message,
        Some(Duration::from_mins(1))
    );

    // 2. TypedConsumerBuilder.
    let typed = client
        .typed_consumer(
            "persistent://public/default/chunk-bound-typed",
            Arc::new(StringSchema::new()),
        )
        .subscription("g")
        .max_pending_chunked_message(CAP)
        .auto_ack_oldest_chunked_message_on_queue_full(true)
        .expire_time_of_incomplete_chunked_message(EXPIRE);
    assert_eq!(
        typed.chunk_knobs_for_test(),
        (Some(CAP), Some(true), Some(EXPIRE))
    );

    // 3. MultiTopicsConsumerBuilder.
    let multi = client
        .multi_topics_consumer()
        .subscription("g")
        .max_pending_chunked_message(CAP)
        .auto_ack_oldest_chunked_message_on_queue_full(true)
        .expire_time_of_incomplete_chunked_message(EXPIRE);
    assert_eq!(
        multi.chunk_knobs_for_test(),
        (Some(CAP), Some(true), Some(EXPIRE))
    );

    // 4. PatternConsumerBuilder.
    let pattern = client
        .pattern_consumer()
        .subscription("g")
        .max_pending_chunked_message(CAP)
        .auto_ack_oldest_chunked_message_on_queue_full(true)
        .expire_time_of_incomplete_chunked_message(EXPIRE);
    assert_eq!(
        pattern.chunk_knobs_for_test(),
        (Some(CAP), Some(true), Some(EXPIRE))
    );

    // 5. PartitionedConsumerBuilder.
    let partitioned = client
        .partitioned_consumer("persistent://public/default/chunk-bound-part")
        .subscription("g")
        .max_pending_chunked_message(CAP)
        .auto_ack_oldest_chunked_message_on_queue_full(true)
        .expire_time_of_incomplete_chunked_message(EXPIRE);
    assert_eq!(
        partitioned.chunk_knobs_for_test(),
        (Some(CAP), Some(true), Some(EXPIRE))
    );

    client.close().await;
}
