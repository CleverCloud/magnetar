// SPDX-License-Identifier: Apache-2.0

//! Integration test mirror of
//! `magnetar-runtime-moonpool/tests/lookup_before_open.rs`.
//!
//! The tokio engine has long issued a `CommandLookupTopic` before every
//! `open_producer` / `subscribe` (see
//! `magnetar-runtime-tokio/src/client.rs::lookup_topic`), but that
//! invariant had no engine-level integration test asserting frame order.
//! ADR-0024's cross-runtime parity rule landed a sibling test on the
//! moonpool side; the tokio side gets the same coverage here.
//!
//! Strategy: stand up a tiny in-process TCP broker stub that records the
//! order of every `BaseCommand` it sees, drive the tokio engine through
//! `open_producer` and `subscribe`, then assert the recorded sequence
//! has `Lookup` strictly before `Producer` / `Subscribe`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedKind(i32);

impl RecordedKind {
    fn kind(self) -> pb::base_command::Type {
        pb::base_command::Type::try_from(self.0).expect("known kind")
    }
}

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
    (format!("pulsar://{addr}"), log)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_producer_issues_lookup_first() {
    let (url, log) = spawn_recording_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    tokio::time::timeout(
        Duration::from_secs(3),
        client.open_producer_with(
            CreateProducerRequest {
                topic: "persistent://public/default/lookup-before-open-producer".to_owned(),
                ..Default::default()
            },
            None,
        ),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    let seen = log.lock().clone();
    let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_issues_lookup_first() {
    let (url, log) = spawn_recording_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe_with(
            SubscribeRequest {
                topic: "persistent://public/default/lookup-before-open-consumer".to_owned(),
                subscription: "lookup-test-sub".to_owned(),
                receiver_queue_size: 16,
                durable: true,
                ..Default::default()
            },
            None,
        ),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let seen = log.lock().clone();
    let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

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
}
