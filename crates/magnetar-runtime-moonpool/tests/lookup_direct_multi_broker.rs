// SPDX-License-Identifier: Apache-2.0

//! Moonpool sibling for the tokio engine's
//! `tests/lookup_direct_multi_broker.rs` — ADR-0039 §"Multi-broker DIRECT
//! routing (2026-06-01)" / HIGH-1 from the lookup multi-agent review.
//!
//! The moonpool engine **does not yet have a per-broker connection pool**
//! (the moonpool flavour of `ProxyConnectionPool` is tracked as the in-flight
//! follow-up in `docs/follow-ups.md §3`). The proto-level routing decision
//! is still mirrored faithfully — `lookup_topic_target` captures the
//! resolved broker URL in `LookupTarget::Direct { broker_url: Some(_) }`,
//! which the tokio engine routes through its pool. On moonpool the
//! synchronous `resolve_target` falls back to the bootstrap connection
//! (same as the pre-amendment behaviour), so this test asserts:
//!
//! 1. The proto-level routing decision is captured (the LOOKUP response surfaces a
//!    `broker_service_url` on the wire and the runtime consumes it without rejecting).
//! 2. The producer / subscribe still complete end-to-end on the bootstrap connection (degenerate
//!    multi-broker DIRECT case: one broker that names itself).
//!
//! Multi-broker DIRECT routing on moonpool (the not-yet-bootstrap case)
//! comes online once the moonpool `ProxyConnectionPool` lands; ADR-0024
//! parity is preserved via the 1:1 file mapping with the tokio test plus
//! the differential equivalence assertion in
//! `crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs`.

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

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    connect_proxy_to_broker_url: Option<String>,
    frames: Vec<i32>,
}

async fn spawn_broker(advertise_self: bool) -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<SessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    let host_port = addr.to_string();
    let advertised_url = if advertise_self {
        Some(format!("pulsar://{addr}"))
    } else {
        None
    };
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let session_idx = {
                let mut s = sessions_for_task.lock();
                s.push(SessionRecord::default());
                s.len() - 1
            };
            let sessions = sessions_for_task.clone();
            let advertised_url = advertised_url.clone();
            tokio::spawn(async move {
                let _ =
                    handle_session(stream, &sessions, session_idx, advertised_url.as_deref()).await;
            });
        }
    });
    (host_port, sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    advertised_url: Option<&str>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);

            let kind = frame.command.r#type;
            let typed = pb::base_command::Type::try_from(kind).ok();
            if matches!(typed, Some(pb::base_command::Type::Connect)) {
                if let Some(c) = &frame.command.connect {
                    sessions.lock()[session_idx]
                        .connect_proxy_to_broker_url
                        .clone_from(&c.proxy_to_broker_url);
                }
            } else {
                sessions.lock()[session_idx].frames.push(kind);
            }

            handle_frame(&frame, &mut out_buf, advertised_url);
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, advertised_url: Option<&str>) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-moonpool-direct-multi-broker-test".to_owned(),
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
                // The crux of the moonpool mirror: the broker DOES advertise a
                // `broker_service_url` (it points at itself for the moonpool case)
                // so `lookup_topic_target` consumes the DIRECT-with-broker-URL
                // shape rather than the DIRECT-with-no-URL shape.
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: advertised_url.map(ToOwned::to_owned),
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
                        producer_name: "moonpool-direct-multi-broker-test".to_owned(),
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

/// The lookup advertises the bootstrap broker as the DIRECT target. The
/// moonpool engine routes through the bootstrap connection (the
/// pool-bypass fast path). Asserts the producer completes end-to-end on
/// exactly one TCP session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_producer_routes_to_resolved_broker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker(true).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect ok")
            .expect("connect ok");

            tokio::time::timeout(
                Duration::from_secs(3),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/moonpool-direct-multi-broker-producer"
                        .to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer ok")
            .expect("open_producer ok");

            let snap = sessions.lock().clone();
            client.close().await;

            // Bootstrap-equality fast path: exactly one session. The DIRECT
            // broker_url was captured but routed to the bootstrap.
            assert_eq!(
                snap.len(),
                1,
                "expected exactly one session (bootstrap reused), got {} sessions",
                snap.len(),
            );
            assert!(
                snap[0].connect_proxy_to_broker_url.is_none(),
                "bootstrap CONNECT must NOT set proxy_to_broker_url",
            );
            let kinds: Vec<_> = snap[0]
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            assert!(
                kinds.contains(&pb::base_command::Type::Lookup),
                "bootstrap session must have seen the LOOKUP, got {kinds:?}",
            );
            assert!(
                kinds.contains(&pb::base_command::Type::Producer),
                "bootstrap session must have seen the PRODUCER, got {kinds:?}",
            );
        })
        .await;
}

/// Sibling of `open_producer_routes_to_resolved_broker` for `subscribe`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_routes_to_resolved_broker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker(true).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect ok")
            .expect("connect ok");

            tokio::time::timeout(
                Duration::from_secs(3),
                client.subscribe(SubscribeRequest {
                    topic: "persistent://public/default/moonpool-direct-multi-broker-consumer"
                        .to_owned(),
                    subscription: "moonpool-direct-multi-broker-sub".to_owned(),
                    receiver_queue_size: 16,
                    durable: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("subscribe ok")
            .expect("subscribe ok");

            let snap = sessions.lock().clone();
            client.close().await;

            assert_eq!(snap.len(), 1);
            assert!(snap[0].connect_proxy_to_broker_url.is_none());
            let kinds: Vec<_> = snap[0]
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            assert!(
                kinds.contains(&pb::base_command::Type::Subscribe),
                "bootstrap session must have seen SUBSCRIBE, got {kinds:?}",
            );
        })
        .await;
}

/// A second producer to the same topic uses the same bootstrap
/// connection (no pool entry on moonpool today). Both PRODUCER frames
/// arrive on the one session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_producer_to_same_broker_reuses_pool_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker(true).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect ok")
            .expect("connect ok");

            let _p1 = tokio::time::timeout(
                Duration::from_secs(3),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/moonpool-direct-reuse-a".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("p1 ok")
            .expect("p1 ok");

            let _p2 = tokio::time::timeout(
                Duration::from_secs(3),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/moonpool-direct-reuse-b".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("p2 ok")
            .expect("p2 ok");

            let snap = sessions.lock().clone();
            client.close().await;

            assert_eq!(
                snap.len(),
                1,
                "must reuse the bootstrap on moonpool; got {} sessions",
                snap.len()
            );
            let producer_count = snap[0]
                .frames
                .iter()
                .filter(|k| **k == pb::base_command::Type::Producer as i32)
                .count();
            assert_eq!(
                producer_count, 2,
                "both producers must have ridden on the bootstrap"
            );
        })
        .await;
}

/// Degenerate case: the broker advertises **no** `broker_service_url` on
/// the lookup response. The runtime falls back to
/// `LookupTarget::Direct { broker_url: None }` and uses the bootstrap.
/// Asserts the runtime tolerates the missing field exactly the same way
/// it did pre-amendment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_resolving_to_bootstrap_broker_reuses_bootstrap_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker(false).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect ok")
            .expect("connect ok");

            tokio::time::timeout(
                Duration::from_secs(3),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/moonpool-direct-bootstrap-equality"
                        .to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer ok")
            .expect("open_producer ok");

            let snap = sessions.lock().clone();
            client.close().await;

            assert_eq!(snap.len(), 1);
            let kinds: Vec<_> = snap[0]
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            assert!(
                kinds.contains(&pb::base_command::Type::Lookup),
                "bootstrap session must have seen LOOKUP, got {kinds:?}",
            );
            assert!(
                kinds.contains(&pb::base_command::Type::Producer),
                "bootstrap session must have seen PRODUCER, got {kinds:?}",
            );
        })
        .await;
}
