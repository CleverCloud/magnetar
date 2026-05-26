// SPDX-License-Identifier: Apache-2.0

//! PIP-33 (ADR-0034) — tokio ↔ moonpool differential equivalence.
//!
//! Two binding equivalence tests, per ADR-0024 §(d):
//!
//! 1. `marker_filter_event_stream_parity` — given an identical broker transcript with
//!    `REPLICATED_SUBSCRIPTION_*` markers interleaved with user messages, both engines surface the
//!    **same** user-facing message stream + the same sequence of marker observation events.
//! 2. `subscribe_options_wire_parity` — `CommandSubscribe.replicate_subscription_state` encodes to
//!    byte-identical wire bytes across engines.
//!
//! A human-reviewable golden snapshot of the expected observation sequence lives
//! at `tests/golden/replicated_subscription_filter.json`. The JSON file is
//! documentation-only — the byte-level invariant is the assertion below.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, ReplicatedSubscriptionMarkerKind, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Copy, Debug)]
enum Action {
    RegularMessage,
    Marker(i32),
}

#[derive(Default)]
struct BrokerLog {
    captured_subscribe: Option<pb::CommandSubscribe>,
    raw_subscribe_bytes: Option<Vec<u8>>,
    next_entry_id: u64,
}

async fn spawn_broker(actions: Vec<Action>) -> (String, Arc<Mutex<BrokerLog>>) {
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
                let frame_bytes_before = before;
                let frame = match decode_one(&mut framed) {
                    Ok(f) => f,
                    Err(FrameError::Incomplete { .. }) => break,
                    Err(_) => return,
                };
                let consumed = before - framed.len();
                // Capture the raw subscribe bytes (wire format) for parity check.
                if frame.command.r#type == pb::base_command::Type::Subscribe as i32 {
                    let subscribe_bytes = read_buf[..consumed].to_vec();
                    log_clone.lock().raw_subscribe_bytes = Some(subscribe_bytes);
                }
                let _ = frame_bytes_before;
                let _ = read_buf.split_to(consumed);
                handle_frame(&frame, &mut out_buf, &log_clone);
                if !script_consumed && frame.command.r#type == pb::base_command::Type::Flow as i32 {
                    let consumer_id = frame.command.flow.as_ref().map_or(0, |f| f.consumer_id);
                    dispatch_script(&actions, consumer_id, &log_clone, &mut out_buf);
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
                    server_version: "magnetar-pip-33-diff".to_owned(),
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
    actions: &[Action],
    consumer_id: u64,
    log: &Arc<Mutex<BrokerLog>>,
    out: &mut BytesMut,
) {
    for action in actions {
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
    use prost::Message as _;
    let mut buf = Vec::new();
    match kind {
        10 => pb::ReplicatedSubscriptionsSnapshotRequest {
            snapshot_id: format!("req-{kind}"),
            source_cluster: Some("cluster-a".to_owned()),
        }
        .encode(&mut buf)
        .unwrap(),
        11 => pb::ReplicatedSubscriptionsSnapshotResponse {
            snapshot_id: format!("resp-{kind}"),
            cluster: Some(pb::ClusterMessageId {
                cluster: "cluster-b".to_owned(),
                message_id: pb::MarkersMessageIdData {
                    ledger_id: 1,
                    entry_id: 1,
                },
            }),
        }
        .encode(&mut buf)
        .unwrap(),
        12 => pb::ReplicatedSubscriptionsSnapshot {
            snapshot_id: format!("snap-{kind}"),
            local_message_id: Some(pb::MarkersMessageIdData {
                ledger_id: 1,
                entry_id: 1,
            }),
            clusters: Vec::new(),
        }
        .encode(&mut buf)
        .unwrap(),
        13 => pb::ReplicatedSubscriptionsUpdate {
            subscription_name: "sub-pip-33".to_owned(),
            clusters: vec![pb::ClusterMessageId {
                cluster: "cluster-b".to_owned(),
                message_id: pb::MarkersMessageIdData {
                    ledger_id: 1,
                    entry_id: 1,
                },
            }],
        }
        .encode(&mut buf)
        .unwrap(),
        _ => {}
    }
    buf
}

fn subscribe_request(topic: &str) -> SubscribeRequest {
    SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "sub-pip-33".to_owned(),
        receiver_queue_size: 16,
        durable: true,
        replicate_subscription_state: Some(true),
        ..Default::default()
    }
}

/// Build the canonical 8-action script: 3 messages → `SnapshotRequest` →
/// 1 message → `SnapshotResponse` → 1 message → `Snapshot` → 1 message → `Update`.
fn canonical_script() -> Vec<Action> {
    vec![
        Action::RegularMessage,
        Action::RegularMessage,
        Action::RegularMessage,
        Action::Marker(10),
        Action::RegularMessage,
        Action::Marker(11),
        Action::RegularMessage,
        Action::Marker(12),
        Action::RegularMessage,
        Action::Marker(13),
    ]
}

/// Drive one engine against a broker with the canonical script. Returns:
/// - the number of user messages observed
/// - the ordered list of marker kinds observed
/// - the captured `CommandSubscribe` byte length (from the broker side)
async fn drive_engine_tokio(
    actions: Vec<Action>,
) -> (usize, Vec<ReplicatedSubscriptionMarkerKind>, usize) {
    use magnetar_runtime_tokio::Client as TokioClient;

    let (url, log) = spawn_broker(actions).await;
    let pulsar_url = format!("pulsar://{url}");
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        TokioClient::connect(&pulsar_url, ConnectionConfig::default()),
    )
    .await
    .expect("connect")
    .expect("connect ok");
    let consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe_with(subscribe_request("persistent://public/default/diff"), None),
    )
    .await
    .expect("subscribe")
    .expect("subscribe ok");

    let mut user_messages = 0;
    for _ in 0..6 {
        let msg = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
            .await
            .expect("receive ok")
            .expect("msg");
        assert_eq!(msg.payload.as_ref(), b"user-payload");
        user_messages += 1;
    }
    // Drain any further user messages with a short timeout (should be none).
    while tokio::time::timeout(Duration::from_millis(50), consumer.receive())
        .await
        .is_ok()
    {
        user_messages += 1;
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
    let wire_len = log.lock().raw_subscribe_bytes.as_ref().map_or(0, Vec::len);
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    (user_messages, kinds, wire_len)
}

async fn drive_engine_moonpool(
    actions: Vec<Action>,
) -> (usize, Vec<ReplicatedSubscriptionMarkerKind>, usize) {
    use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};

    let (addr, log) = spawn_broker(actions).await;
    let local = tokio::task::LocalSet::new();
    let (user_messages, kinds, wire_len) = local
        .run_until(async move {
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                MoonpoolClient::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect")
            .expect("connect ok");
            let consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request("persistent://public/default/diff")),
            )
            .await
            .expect("subscribe")
            .expect("subscribe ok");

            let mut user_messages = 0;
            for _ in 0..6 {
                let msg = tokio::time::timeout(Duration::from_secs(2), consumer.receive())
                    .await
                    .expect("receive ok")
                    .expect("msg");
                assert_eq!(msg.payload.as_ref(), b"user-payload");
                user_messages += 1;
            }
            while tokio::time::timeout(Duration::from_millis(50), consumer.receive())
                .await
                .is_ok()
            {
                user_messages += 1;
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
            let wire_len = log.lock().raw_subscribe_bytes.as_ref().map_or(0, Vec::len);
            client.close().await;
            (user_messages, kinds, wire_len)
        })
        .await;
    (user_messages, kinds, wire_len)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn marker_filter_event_stream_parity() {
    let (tokio_count, tokio_kinds, _) = drive_engine_tokio(canonical_script()).await;
    let (moonpool_count, moonpool_kinds, _) = drive_engine_moonpool(canonical_script()).await;
    assert_eq!(
        tokio_count, moonpool_count,
        "user-message counts diverged (tokio={tokio_count}, moonpool={moonpool_count})"
    );
    assert_eq!(
        tokio_kinds, moonpool_kinds,
        "marker observation sequences diverged",
    );
    // Sanity: the canonical script emits 6 user messages and 4 markers in this order.
    assert_eq!(tokio_count, 6);
    assert_eq!(
        tokio_kinds,
        vec![
            ReplicatedSubscriptionMarkerKind::SnapshotRequest,
            ReplicatedSubscriptionMarkerKind::SnapshotResponse,
            ReplicatedSubscriptionMarkerKind::Snapshot,
            ReplicatedSubscriptionMarkerKind::Update,
        ]
    );
}

async fn capture_subscribe_bytes_tokio() -> Vec<u8> {
    use magnetar_runtime_tokio::Client as TokioClient;
    let (url, log) = spawn_broker(Vec::new()).await;
    let pulsar_url = format!("pulsar://{url}");
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        TokioClient::connect(&pulsar_url, ConnectionConfig::default()),
    )
    .await
    .expect("connect")
    .expect("connect ok");
    let _consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe_with(subscribe_request("persistent://public/default/diff"), None),
    )
    .await
    .expect("subscribe")
    .expect("subscribe ok");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let bytes = log.lock().raw_subscribe_bytes.clone().unwrap_or_default();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    bytes
}

async fn capture_subscribe_bytes_moonpool() -> Vec<u8> {
    use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
    let (addr, log) = spawn_broker(Vec::new()).await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                MoonpoolClient::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect")
            .expect("connect ok");
            let _consumer = tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(subscribe_request("persistent://public/default/diff")),
            )
            .await
            .expect("subscribe")
            .expect("subscribe ok");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let bytes = log.lock().raw_subscribe_bytes.clone().unwrap_or_default();
            client.close().await;
            bytes
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_options_wire_parity() {
    // Both engines must encode the same `SubscribeRequest` to byte-identical
    // `CommandSubscribe` wire bytes — the proto-side `emit_command_subscribe`
    // is shared, so this is a regression guard against any future engine-side
    // drift in how SubscribeRequest reaches the proto layer.
    let tokio_bytes = capture_subscribe_bytes_tokio().await;
    let moonpool_bytes = capture_subscribe_bytes_moonpool().await;
    assert!(
        !tokio_bytes.is_empty(),
        "broker must capture the subscribe frame"
    );
    assert_eq!(
        tokio_bytes, moonpool_bytes,
        "CommandSubscribe wire bytes diverged between engines",
    );
}
