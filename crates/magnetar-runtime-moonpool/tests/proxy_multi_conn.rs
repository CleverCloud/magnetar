// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for the Apache Pulsar Proxy connection model
//! on the moonpool engine (ADR-0039 / issue #15) — 1:1 mirror of
//! `crates/magnetar-runtime-tokio/tests/proxy_multi_conn.rs` per ADR-0024.
//!
//! Wires an in-process scripted broker that emulates the proxy wire
//! contract:
//!
//! 1. The **bootstrap** connection (the one [`Client::connect_plain_supervised`] opens) MUST arrive
//!    with `CommandConnect.proxy_to_broker_url = None`. The fake accepts it, then on
//!    `CommandLookupTopic` answers with `proxy_through_service_url = true` plus a synthetic
//!    `broker_service_url` advertising the backend broker.
//! 2. The runtime must then open a **second** TCP connection with
//!    `CommandConnect.proxy_to_broker_url = Some(<host:port of the advertised broker>)`. The
//!    runtime strips the `pulsar://` scheme before stuffing the value into `CommandConnect`,
//!    matching the Java reference client and pulsar-rs.
//!
//! The test asserts:
//! - Exactly **two** TCP sessions are accepted (bootstrap + per-broker entry).
//! - The bootstrap session's `CommandConnect` carries no `proxy_to_broker_url`.
//! - The per-broker session's `CommandConnect` carries the `host:port` of the broker URL the proxy
//!   advertised in the lookup response — no `pulsar://` scheme prefix.
//! - The producer / subscribe open completes end-to-end through the pinned pool entry.
//! - A **second** open against the same broker URL reuses the existing pool entry — no third TCP
//!   session.

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

/// Per-session log: records the `proxy_to_broker_url` we saw on `CommandConnect`
/// and the kinds of every subsequent frame, in arrival order.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    /// `Some(url)` when `CommandConnect.proxy_to_broker_url = Some(url)`, `None` when
    /// the field was absent. Captures the bootstrap-vs-pinned distinction.
    connect_proxy_to_broker_url: Option<String>,
    /// All non-CONNECT frames the session received, in arrival order.
    frames: Vec<i32>,
}

/// Synthetic broker URL the fake proxy advertises in lookup responses. The
/// host portion is meaningless — the client never dials it; the dial target
/// stays the proxy address.
const ADVERTISED_BROKER_URL: &str = "pulsar://broker-a.proxy.internal:6650";

/// `host:port` form of [`ADVERTISED_BROKER_URL`]. This is what the runtime
/// must put in `CommandConnect.proxy_to_broker_url` after stripping the
/// `pulsar://` scheme (parity with Java + pulsar-rs; ADR-0039).
const ADVERTISED_BROKER_HOST_PORT: &str = "broker-a.proxy.internal:6650";

/// Spawn a fake Apache Pulsar Proxy on `127.0.0.1:0`. Returns the bound
/// `host:port` (moonpool's address form — no `pulsar://` scheme) and the
/// per-session record log.
async fn spawn_proxy() -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<SessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
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
            tokio::spawn(async move {
                let _ = handle_session(stream, &sessions, session_idx).await;
            });
        }
    });
    (addr.to_string(), sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
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

            handle_frame(&frame, &mut out_buf, session_idx);
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

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, session_idx: usize) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-proxy-test-moonpool".to_owned(),
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
                // The proxy contract: on the bootstrap session (idx==0)
                // advertise `proxy_through_service_url = true`. On pinned
                // sessions (which shouldn't be issuing lookups in this test,
                // but we tolerate them) echo `proxy_through = false` to avoid
                // a redirect loop.
                let proxy_through = session_idx == 0;
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: Some(ADVERTISED_BROKER_URL.to_owned()),
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(proxy_through),
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
                        producer_name: "proxy-moonpool".to_owned(),
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

/// Build a `ConnectionConfig` with the supervisor wired in — the moonpool
/// engine builds the proxy pool only on `connect_plain_supervised`, which
/// requires a non-`None` `supervisor` field on the config.
fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig::default()),
        ..ConnectionConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_producer_through_proxy_opens_second_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_proxy().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain_supervised(
                    &engine,
                    &host_port,
                    supervised_config(),
                    None,
                    None,
                ),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let _producer = tokio::time::timeout(
                Duration::from_secs(5),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/proxy-moonpool-producer".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(client);

            assert!(
                snapshot.len() >= 2,
                "expected at least 2 TCP sessions (bootstrap + pinned), got {snapshot:?}",
            );

            let bootstrap = &snapshot[0];
            let pinned = &snapshot[1];

            assert!(
                bootstrap.connect_proxy_to_broker_url.is_none(),
                "bootstrap CONNECT must NOT set proxy_to_broker_url, got {:?}",
                bootstrap.connect_proxy_to_broker_url
            );

            assert_eq!(
                pinned.connect_proxy_to_broker_url.as_deref(),
                Some(ADVERTISED_BROKER_HOST_PORT),
                "pinned CONNECT must set proxy_to_broker_url to the host:port form of the \
                 advertised broker URL (no scheme prefix)"
            );

            let bootstrap_kinds: Vec<_> = bootstrap
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            let pinned_kinds: Vec<_> = pinned
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            assert!(
                bootstrap_kinds.contains(&pb::base_command::Type::Lookup),
                "bootstrap session must have seen the lookup, got {bootstrap_kinds:?}"
            );
            assert!(
                pinned_kinds.contains(&pb::base_command::Type::Producer),
                "pinned session must have seen the producer create, got {pinned_kinds:?}"
            );
            assert!(
                !bootstrap_kinds.contains(&pb::base_command::Type::Producer),
                "bootstrap session must NOT have seen CommandProducer (it must have ridden \
                 on the pinned session)"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_through_proxy_opens_second_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_proxy().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain_supervised(
                    &engine,
                    &host_port,
                    supervised_config(),
                    None,
                    None,
                ),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let _consumer = tokio::time::timeout(
                Duration::from_secs(5),
                client.subscribe(SubscribeRequest {
                    topic: "persistent://public/default/proxy-moonpool-consumer".to_owned(),
                    subscription: "proxy-moonpool-sub".to_owned(),
                    receiver_queue_size: 16,
                    durable: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("subscribe did not time out")
            .expect("subscribe ok");

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(client);

            assert!(snapshot.len() >= 2);
            let pinned = &snapshot[1];
            assert_eq!(
                pinned.connect_proxy_to_broker_url.as_deref(),
                Some(ADVERTISED_BROKER_HOST_PORT),
                "pinned CONNECT must set proxy_to_broker_url to host:port (no scheme)"
            );
            let pinned_kinds: Vec<_> = pinned
                .frames
                .iter()
                .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                .collect();
            assert!(
                pinned_kinds.contains(&pb::base_command::Type::Subscribe),
                "pinned session must have seen the subscribe, got {pinned_kinds:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_producer_to_same_broker_reuses_pool_entry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_proxy().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain_supervised(
                    &engine,
                    &host_port,
                    supervised_config(),
                    None,
                    None,
                ),
            )
            .await
            .expect("connect ok")
            .expect("connect ok");

            let _p1 = tokio::time::timeout(
                Duration::from_secs(5),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/proxy-moonpool-pool-reuse-a".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("p1 ok")
            .expect("p1 ok");

            let _p2 = tokio::time::timeout(
                Duration::from_secs(5),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/proxy-moonpool-pool-reuse-b".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("p2 ok")
            .expect("p2 ok");

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(client);

            assert_eq!(
                snapshot.len(),
                2,
                "second producer must reuse the existing pinned pool entry — got {} sessions",
                snapshot.len()
            );

            let pinned = &snapshot[1];
            let producer_count = pinned
                .frames
                .iter()
                .filter(|k| **k == pb::base_command::Type::Producer as i32)
                .count();
            assert_eq!(
                producer_count, 2,
                "pinned session must have served both producers; saw {producer_count}"
            );
        })
        .await;
}
