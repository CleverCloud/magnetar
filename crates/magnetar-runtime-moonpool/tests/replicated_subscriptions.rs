// SPDX-License-Identifier: Apache-2.0

//! PIP-33 (replicated subscriptions, ADR-0034) — moonpool engine integration
//! tests. 1:1 mirror of
//! `crates/magnetar-runtime-tokio/tests/replicated_subscriptions.rs`. The five
//! tests carry identical names so `cargo xtask check-runtime-test-parity` (ADR-0024)
//! stays green.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, ReplicatedSubscriptionMarkerKind, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Default, Clone)]
struct BrokerScript {
    actions: Vec<Action>,
}

#[derive(Clone, Copy, Debug)]
enum Action {
    RegularMessage,
    Marker(i32),
}

#[derive(Default)]
struct BrokerLog {
    seen: Vec<i32>,
    captured_subscribe: Option<pb::CommandSubscribe>,
    next_entry_id: u64,
}

async fn spawn_broker(script: BrokerScript) -> (String, Arc<Mutex<BrokerLog>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let log: Arc<Mutex<BrokerLog>> = Arc::new(Mutex::new(BrokerLog::default()));
    let log_clone = log.clone();
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut read_buf = BytesMut::with_capacity(64 * 1024);
        let mut out_buf = BytesMut::with_capacity(64 * 1024);
        let mut script_consumed = false;
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
                log_clone.lock().seen.push(frame.command.r#type);
                handle_frame(&frame, &mut out_buf, &log_clone);
                if !script_consumed && frame.command.r#type == pb::base_command::Type::Flow as i32 {
                    let consumer_id = frame.command.flow.as_ref().map_or(0, |f| f.consumer_id);
                    dispatch_script(&script, consumer_id, &log_clone, &mut out_buf);
                    script_consumed = true;
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
    (addr.to_string(), log)
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, log: &Arc<Mutex<BrokerLog>>) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-pip-33-test".to_owned(),
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
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                log.lock().captured_subscribe = Some(s.clone());
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

fn dispatch_script(
    script: &BrokerScript,
    consumer_id: u64,
    log: &Arc<Mutex<BrokerLog>>,
    out: &mut BytesMut,
) {
    for action in &script.actions {
        let entry_id = {
            let mut g = log.lock();
            g.next_entry_id += 1;
            g.next_entry_id
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id,
                message_id: pb::MessageIdData {
                    ledger_id: 1,
                    entry_id,
                    partition: None,
                    batch_index: None,
                    ack_set: Vec::new(),
                    batch_size: None,
                    first_chunk_message_id: None,
                },
                redelivery_count: Some(0),
                ack_set: Vec::new(),
                consumer_epoch: None,
            }),
            ..Default::default()
        };
        match action {
            Action::RegularMessage => {
                let meta = pb::MessageMetadata {
                    producer_name: "scripted".to_owned(),
                    sequence_id: entry_id,
                    publish_time: 1_700_000_000_000,
                    num_messages_in_batch: Some(1),
                    ..Default::default()
                };
                let _ = encode_payload(out, &cmd, &meta, b"user-payload");
            }
            Action::Marker(kind) => {
                let meta = pb::MessageMetadata {
                    producer_name: "broker-marker".to_owned(),
                    sequence_id: 0,
                    publish_time: 1_700_000_000_000,
                    marker_type: Some(*kind),
                    ..Default::default()
                };
                let payload = encode_marker_payload(*kind);
                let _ = encode_payload(out, &cmd, &meta, &payload);
            }
        }
    }
}

fn encode_marker_payload(kind: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    match kind {
        10 => {
            let m = pb::ReplicatedSubscriptionsSnapshotRequest {
                snapshot_id: format!("req-{kind}"),
                source_cluster: Some("cluster-a".to_owned()),
            };
            prost::Message::encode(&m, &mut buf).expect("encode");
        }
        11 => {
            let m = pb::ReplicatedSubscriptionsSnapshotResponse {
                snapshot_id: format!("resp-{kind}"),
                cluster: Some(pb::ClusterMessageId {
                    cluster: "cluster-b".to_owned(),
                    message_id: pb::MarkersMessageIdData {
                        ledger_id: 1,
                        entry_id: 1,
                    },
                }),
            };
            prost::Message::encode(&m, &mut buf).expect("encode");
        }
        12 => {
            let m = pb::ReplicatedSubscriptionsSnapshot {
                snapshot_id: format!("snap-{kind}"),
                local_message_id: Some(pb::MarkersMessageIdData {
                    ledger_id: 1,
                    entry_id: 1,
                }),
                clusters: vec![pb::ClusterMessageId {
                    cluster: "cluster-b".to_owned(),
                    message_id: pb::MarkersMessageIdData {
                        ledger_id: 1,
                        entry_id: 1,
                    },
                }],
            };
            prost::Message::encode(&m, &mut buf).expect("encode");
        }
        13 => {
            let m = pb::ReplicatedSubscriptionsUpdate {
                subscription_name: "sub-pip-33".to_owned(),
                clusters: vec![pb::ClusterMessageId {
                    cluster: "cluster-b".to_owned(),
                    message_id: pb::MarkersMessageIdData {
                        ledger_id: 1,
                        entry_id: 1,
                    },
                }],
            };
            prost::Message::encode(&m, &mut buf).expect("encode");
        }
        _ => {}
    }
    buf
}

fn subscribe_request(topic: &str, replicate: Option<bool>) -> SubscribeRequest {
    SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "sub-pip-33".to_owned(),
        receiver_queue_size: 32,
        durable: true,
        replicate_subscription_state: replicate,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_replicate_subscription_state_true_emits_field() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (addr, log) = spawn_broker(BrokerScript::default()).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let _consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request(
                    "persistent://public/default/replicated-true",
                    Some(true),
                )),
            )
            .await
            .expect("subscribe did not time out")
            .expect("subscribe ok");

            tokio::time::sleep(Duration::from_millis(50)).await;
            let captured = log.lock().captured_subscribe.clone().expect("subscribe");
            assert_eq!(captured.replicate_subscription_state, Some(true));
            client.close().await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_replicate_subscription_state_default_false() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (addr, log) = spawn_broker(BrokerScript::default()).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let _consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request(
                    "persistent://public/default/replicated-default",
                    None,
                )),
            )
            .await
            .expect("subscribe did not time out")
            .expect("subscribe ok");

            tokio::time::sleep(Duration::from_millis(50)).await;
            let captured = log.lock().captured_subscribe.clone().expect("subscribe");
            assert_eq!(captured.replicate_subscription_state, None);
            client.close().await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_skips_replicated_marker_against_scripted_broker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut actions = Vec::new();
            for _ in 0..5 {
                actions.push(Action::RegularMessage);
            }
            actions.push(Action::Marker(12));
            for _ in 0..5 {
                actions.push(Action::RegularMessage);
            }
            actions.push(Action::Marker(13));
            let (addr, _log) = spawn_broker(BrokerScript { actions }).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect")
            .expect("connect ok");
            let consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request(
                    "persistent://public/default/filter",
                    Some(true),
                )),
            )
            .await
            .expect("subscribe")
            .expect("subscribe ok");

            for _ in 0..10 {
                let msg = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
                    .await
                    .expect("receive did not time out")
                    .expect("receive ok");
                assert_eq!(msg.payload.as_ref(), b"user-payload");
            }
            let trailing =
                tokio::time::timeout(Duration::from_millis(200), consumer.receive()).await;
            assert!(trailing.is_err(), "no further user message expected");
            client.close().await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_emits_marker_observation_in_order() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut actions = Vec::new();
            for _ in 0..3 {
                actions.push(Action::RegularMessage);
            }
            actions.push(Action::Marker(12));
            for _ in 0..3 {
                actions.push(Action::RegularMessage);
            }
            actions.push(Action::Marker(13));
            let (addr, _log) = spawn_broker(BrokerScript { actions }).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect")
            .expect("connect ok");
            let consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request(
                    "persistent://public/default/observe",
                    Some(true),
                )),
            )
            .await
            .expect("subscribe")
            .expect("subscribe ok");

            for _ in 0..6 {
                let _msg = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
                    .await
                    .expect("receive ok")
                    .expect("msg");
            }

            let first = tokio::time::timeout(
                Duration::from_secs(10),
                client.next_replicated_subscription_marker(),
            )
            .await
            .expect("first marker timeout")
            .expect("first marker some");
            assert_eq!(
                first.marker.kind,
                ReplicatedSubscriptionMarkerKind::Snapshot
            );
            let second = tokio::time::timeout(
                Duration::from_secs(10),
                client.next_replicated_subscription_marker(),
            )
            .await
            .expect("second marker timeout")
            .expect("second marker some");
            assert_eq!(second.marker.kind, ReplicatedSubscriptionMarkerKind::Update);
            client.close().await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_filters_all_four_marker_kinds() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let actions = vec![
                Action::Marker(10),
                Action::RegularMessage,
                Action::Marker(11),
                Action::RegularMessage,
                Action::Marker(12),
                Action::RegularMessage,
                Action::Marker(13),
                Action::RegularMessage,
            ];
            let (addr, _log) = spawn_broker(BrokerScript { actions }).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect")
            .expect("connect ok");
            let consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request(
                    "persistent://public/default/all-kinds",
                    Some(true),
                )),
            )
            .await
            .expect("subscribe")
            .expect("subscribe ok");

            for _ in 0..4 {
                let _msg = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
                    .await
                    .expect("receive ok")
                    .expect("msg");
            }

            let mut kinds = Vec::new();
            for _ in 0..4 {
                let obs = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.next_replicated_subscription_marker(),
                )
                .await
                .expect("marker timeout")
                .expect("marker some");
                kinds.push(obs.marker.kind);
            }
            assert_eq!(
                kinds,
                vec![
                    ReplicatedSubscriptionMarkerKind::SnapshotRequest,
                    ReplicatedSubscriptionMarkerKind::SnapshotResponse,
                    ReplicatedSubscriptionMarkerKind::Snapshot,
                    ReplicatedSubscriptionMarkerKind::Update,
                ]
            );
            client.close().await;
        })
        .await;
}
