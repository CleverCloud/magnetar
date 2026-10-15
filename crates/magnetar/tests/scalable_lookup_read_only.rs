// SPDX-License-Identifier: Apache-2.0

//! The façade's read-only scalable lookup never asks the broker to create a
//! topic (ADR-0093 amendment 2026-10-12).
//!
//! `PulsarClient::lookup_scalable_topic` backs `magnetarctl topic-info`. A Pulsar
//! 5.0.0 broker auto-creates a missing `topic://` topic for a lookup that leaves
//! `create_if_missing` at its default (`true`), so a diagnostic lookup of a
//! mistyped name would create a topic. The façade therefore sends `false`.
//!
//! The opposite fork — `scalable_stream_consumer`, which opens a consumer and so
//! sends `true` — is exercised by the aggregate consumer suites. This test pins
//! the read-only façade lookup and its explicit `false` request.
//!
//! Like `consumer_name_builder_surface.rs`, it needs a real `PulsarClient` and so
//! speaks to a TCP fake broker. This one advertises `supports_scalable_topics`,
//! records the `create_if_missing` field of every lookup it receives, and, like a
//! Pulsar 5.0.0 broker, answers a lookup of an absent topic with not-found unless
//! the lookup asked for creation.

#![cfg(all(feature = "tokio", feature = "scalable-topics"))]
#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::PulsarClient;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The `create_if_missing` field of every lookup the fake broker received.
#[derive(Clone, Default)]
struct Recorded(std::sync::Arc<std::sync::Mutex<Vec<Option<bool>>>>);

impl Recorded {
    fn push(&self, value: Option<bool>) {
        self.0.lock().expect("recorder lock").push(value);
    }

    fn snapshot(&self) -> Vec<Option<bool>> {
        self.0.lock().expect("recorder lock").clone()
    }
}

fn layout_reply(session_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch: 1,
                segments: vec![pb::SegmentInfoProto {
                    segment_id: 1,
                    hash_start: 0,
                    hash_end: 65_535,
                    state: pb::SegmentState::Active as i32,
                    parent_ids: Vec::new(),
                    child_ids: Vec::new(),
                    created_at_epoch: 0,
                    sealed_at_epoch: None,
                    created_at_ms: 0,
                    sealed_at_ms: None,
                    legacy_topic_name: None,
                    entry_bucket_splits: Vec::new(),
                }],
                segment_brokers: Vec::new(),
                controller_broker_url: None,
                controller_broker_url_tls: None,
            }),
            error: None,
            message: None,
            resolved_topic_name: None,
        }),
        ..Default::default()
    }
}

fn not_found_reply(session_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: None,
            error: Some(pb::ServerError::TopicNotFound as i32),
            message: Some("fake: Scalable topic not found".to_owned()),
            resolved_topic_name: None,
        }),
        ..Default::default()
    }
}

/// Spawn the fake broker and return its address and the recorded lookup flags.
async fn spawn_fake_broker() -> (String, Recorded) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let recorded = Recorded::default();
    let seen = recorded.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let seen = seen.clone();
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
                        let reply = if frame.command.r#type
                            == pb::base_command::Type::Connect as i32
                        {
                            Some(pb::BaseCommand {
                                r#type: pb::base_command::Type::Connected as i32,
                                connected: Some(pb::CommandConnected {
                                    server_version: "magnetar-scalable-lookup-fake".to_owned(),
                                    protocol_version: Some(21),
                                    max_message_size: Some(5 * 1024 * 1024),
                                    feature_flags: Some(pb::FeatureFlags {
                                        supports_scalable_topics: Some(true),
                                        ..pb::FeatureFlags::default()
                                    }),
                                }),
                                ..Default::default()
                            })
                        } else if let Some(lookup) = frame.command.scalable_topic_lookup {
                            seen.push(lookup.create_if_missing);
                            if lookup.topic.ends_with("-absent") && !lookup.create_if_missing() {
                                Some(not_found_reply(lookup.session_id))
                            } else {
                                Some(layout_reply(lookup.session_id))
                            }
                        } else {
                            None
                        };
                        if let Some(reply) = reply {
                            encode_command(&mut out_buf, &reply).expect("encode reply");
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
    (addr, recorded)
}

/// `lookup_scalable_topic` writes `create_if_missing = false`, and an absent
/// topic is therefore reported as not found rather than created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_scalable_topic_is_read_only() {
    let (addr, recorded) = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let existing = tokio::time::timeout(
        Duration::from_secs(5),
        client.lookup_scalable_topic("topic://public/default/exists"),
    )
    .await
    .expect("lookup did not hang")
    .expect("an existing topic resolves");
    assert_eq!(existing.epoch, 1);

    let absent = tokio::time::timeout(
        Duration::from_secs(5),
        client.lookup_scalable_topic("topic://public/default/never-created-absent"),
    )
    .await
    .expect("lookup did not hang")
    .expect_err("a read-only lookup of an absent topic is not-found, not a create");
    assert!(
        absent.to_string().contains("Scalable topic not found"),
        "the broker's reason reaches the caller: {absent}"
    );

    assert_eq!(
        recorded.snapshot(),
        vec![Some(false), Some(false)],
        "every façade lookup states create_if_missing = false on the wire"
    );
}
