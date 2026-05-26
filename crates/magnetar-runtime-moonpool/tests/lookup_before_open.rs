// SPDX-License-Identifier: Apache-2.0

//! Integration test for ADR-0019 Java parity item
//! "Moonpool engine: lookup before producer/consumer open".
//!
//! The moonpool engine's [`Client::open_producer`] and [`Client::subscribe`]
//! must issue a `CommandLookupTopic` round-trip BEFORE the
//! `CommandProducer` / `CommandSubscribe` frame goes out — Pulsar's broker
//! refuses producer / subscribe on a topic whose namespace bundle has not
//! been activated by a prior lookup (`ServerError::ServiceNotReady`,
//! message "not served by this instance, please redo the lookup"). The
//! tokio engine already does this; see
//! `magnetar-runtime-tokio/src/client.rs` `lookup_topic` step.
//!
//! Strategy: stand up a tiny in-process TCP broker stub that records the
//! order of every `BaseCommand` it sees, drive the moonpool engine through
//! `open_producer` and `subscribe`, then assert the recorded sequence
//! starts with `Connect, Lookup, Producer, …, Lookup, Subscribe, …`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Type-tag log entry; we don't need the full frame for the assertion,
/// just the wire-protocol command kind in the order it arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedKind(i32);

impl RecordedKind {
    fn kind(self) -> pb::base_command::Type {
        pb::base_command::Type::try_from(self.0).expect("known kind")
    }
}

/// Recording broker stub. Speaks the bare minimum of the Pulsar binary
/// protocol the moonpool engine needs to drive `open_producer` and
/// `subscribe` to completion, and appends a [`RecordedKind`] to the
/// shared log for every inbound frame.
async fn spawn_recording_broker() -> (String, Arc<Mutex<Vec<RecordedKind>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let log: Arc<Mutex<Vec<RecordedKind>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        let mut out_buf = BytesMut::with_capacity(8 * 1024);
        loop {
            // Decode every complete frame currently in the buffer.
            loop {
                let mut framed = read_buf.clone().freeze();
                let before = framed.len();
                let frame = match decode_one(&mut framed) {
                    Ok(f) => f,
                    Err(FrameError::Incomplete { .. }) => break,
                    Err(_) => return,
                };
                let consumed = before - framed.len();
                let _ = read_buf.split_to(consumed);
                log_clone.lock().push(RecordedKind(frame.command.r#type));
                handle_frame(&frame, &mut out_buf);
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
    (addr.to_string(), log)
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-lookup-test".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "lookup-test".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: s.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        _ => {}
    }
}

/// `Client::open_producer` must send a `CommandLookupTopic` and observe
/// `CommandLookupTopicResponse` before emitting `CommandProducer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_producer_issues_lookup_first() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_recording_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            tokio::time::timeout(
                Duration::from_secs(3),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/lookup-before-open-producer".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            let seen = log.lock().clone();
            let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
            client.close().await;

            // Confirm Connect → Lookup → Producer order. The state machine may
            // also have emitted a Ping; tolerate it but require the Lookup to
            // strictly precede the Producer.
            let lookup_idx = kinds
                .iter()
                .position(|k| *k == pb::base_command::Type::Lookup)
                .expect("expected CommandLookupTopic to be sent");
            let producer_idx = kinds
                .iter()
                .position(|k| *k == pb::base_command::Type::Producer)
                .expect("expected CommandProducer to be sent");
            assert!(
                lookup_idx < producer_idx,
                "expected Lookup ({lookup_idx}) to precede Producer ({producer_idx}) in {kinds:?}",
            );
        })
        .await;
}

/// `Client::subscribe` must send a `CommandLookupTopic` and observe
/// `CommandLookupTopicResponse` before emitting `CommandSubscribe`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_issues_lookup_first() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_recording_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(SubscribeRequest {
                    topic: "persistent://public/default/lookup-before-open-consumer".to_owned(),
                    subscription: "lookup-test-sub".to_owned(),
                    receiver_queue_size: 16,
                    durable: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("subscribe did not time out")
            .expect("subscribe ok");

            let seen = log.lock().clone();
            let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
            client.close().await;

            let lookup_idx = kinds
                .iter()
                .position(|k| *k == pb::base_command::Type::Lookup)
                .expect("expected CommandLookupTopic to be sent");
            let subscribe_idx = kinds
                .iter()
                .position(|k| *k == pb::base_command::Type::Subscribe)
                .expect("expected CommandSubscribe to be sent");
            assert!(
                lookup_idx < subscribe_idx,
                "expected Lookup ({lookup_idx}) to precede Subscribe ({subscribe_idx}) in {kinds:?}",
            );
        })
        .await;
}
