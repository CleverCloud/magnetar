// SPDX-License-Identifier: Apache-2.0

//! Builder-surface completeness pin for the consumer-name knob on the
//! multi-child consumer builders.
//!
//! Issue #300: `PartitionedConsumerBuilder` exposed no way to set the consumer
//! name, so every per-partition child subscribed with `consumer_name: None` and
//! broker `topics stats` showed an empty `consumerName`. The fix threads a
//! `name()` setter through the multi-topics / pattern / partitioned builders and
//! propagates it verbatim (no per-partition suffix — same name on every child,
//! matching the Java client) via `ConsumerTemplate`.
//!
//! This test drives each builder surface and asserts the name reaches the field
//! the `ConsumerTemplate` is built from. It uses the same idle TCP fake-broker as
//! `chunk_bound_builder_surface.rs`: a real `PulsarClient` is needed to
//! instantiate the builders, but no subscribe ever reaches the wire — the
//! assertions read builder state directly via the `#[doc(hidden)]`
//! `consumer_name_for_test()` seam.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::PulsarClient;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const NAME: &str = "magnetar-instance-7";

/// Spawn a TCP fake-broker that answers a single `CommandConnect` with
/// `CommandConnected` and then idles. No subscribe reaches the wire.
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
                                    server_version: "magnetar-consumer-name-fake".to_owned(),
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
async fn multi_child_consumer_builders_thread_consumer_name() {
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

    // MultiTopicsConsumerBuilder.
    let multi = client.multi_topics_consumer().subscription("g").name(NAME);
    assert_eq!(multi.consumer_name_for_test(), Some(NAME));

    // PatternConsumerBuilder.
    let pattern = client.pattern_consumer().subscription("g").name(NAME);
    assert_eq!(pattern.consumer_name_for_test(), Some(NAME));

    // PartitionedConsumerBuilder — the issue-#300 surface.
    let partitioned = client
        .partitioned_consumer("persistent://public/default/consumer-name-part")
        .subscription("g")
        .name(NAME);
    assert_eq!(partitioned.consumer_name_for_test(), Some(NAME));

    // Default: no name set → broker assigns one (None on every surface).
    assert_eq!(
        client
            .multi_topics_consumer()
            .subscription("g")
            .consumer_name_for_test(),
        None
    );
    assert_eq!(
        client
            .partitioned_consumer("persistent://public/default/consumer-name-default")
            .subscription("g")
            .consumer_name_for_test(),
        None
    );

    client.close().await;
}
