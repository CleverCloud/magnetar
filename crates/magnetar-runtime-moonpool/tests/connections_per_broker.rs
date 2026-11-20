// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for `connections_per_broker` (Java
//! `ClientBuilder#connectionsPerBroker`, ADR-0073, issue #314) on the moonpool
//! engine — 1:1 mirror of
//! `crates/magnetar-runtime-tokio/tests/connections_per_broker.rs` per ADR-0024.
//!
//! Wires an in-process scripted broker whose `CommandLookupTopic` answer is the
//! single-broker / bootstrap shape — `broker_service_url = None`,
//! `proxy_through_service_url = false` — so every producer / consumer rides the
//! **bootstrap broker**. Each accepted TCP session is one connection the client
//! opened, so the count of distinct sessions is the realized fan-out.
//!
//! The tests assert:
//! - `connections_per_broker(3)` spreads three producers over **three** distinct connections
//!   (bootstrap + two siblings), round-robin.
//! - `connections_per_broker(1)` (the default) keeps all producers on the **single** bootstrap
//!   connection.
//! - `connections_per_broker(2)` spreads consumers too, not just producers.
//! - Sibling CONNECTs replicate the bootstrap CONNECT (no `proxy_to_broker_url`).
//! - A dropped sibling re-dials the `ServiceUrlProvider` URL, as the bootstrap does (ADR-0039).

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ControlledClusterFailover, CreateProducerRequest, FrameError,
    SubscribeRequest, SupervisorConfig, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::AbortHandle;
mod common;
use common::HANG_GUARD;

/// Per-session log: the `proxy_to_broker_url` seen on `CommandConnect` plus the
/// kinds of every subsequent frame, in arrival order.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    connect_proxy_to_broker_url: Option<String>,
    frames: Vec<i32>,
}

/// Spawn a fake single-broker Pulsar on `127.0.0.1:0`. Returns the bound
/// `host:port` (moonpool's address form — no `pulsar://` scheme) and the
/// per-session record log.
async fn spawn_broker() -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let (host_port, sessions, _session_tasks) = spawn_abortable_broker().await;
    (host_port, sessions)
}

/// [`spawn_broker`] that also hands back one [`AbortHandle`] per accepted session, in accept
/// order: aborting a session's task drops its socket, which is how a test makes the broker drop
/// a single connection while the listener keeps accepting.
async fn spawn_abortable_broker() -> (
    String,
    Arc<Mutex<Vec<SessionRecord>>>,
    Arc<Mutex<Vec<AbortHandle>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<SessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let session_tasks: Arc<Mutex<Vec<AbortHandle>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    let session_tasks_for_task = session_tasks.clone();
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
            let session = tokio::spawn(async move {
                let _ = handle_session(stream, &sessions, session_idx).await;
            });
            session_tasks_for_task.lock().push(session.abort_handle());
        }
    });
    (addr.to_string(), sessions, session_tasks)
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

            handle_frame(&frame, &mut out_buf);
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

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-cpb-test-moonpool".to_owned(),
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
                // Single-broker / bootstrap shape: no advertised broker URL, no
                // proxy. Every producer/consumer rides the bootstrap broker.
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
                        producer_name: p
                            .producer_name
                            .clone()
                            .filter(|n| !n.is_empty())
                            .unwrap_or_else(|| format!("cpb-{}", p.producer_id)),
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

/// Build a `ConnectionConfig` with the supervisor wired in — the moonpool engine
/// builds the proxy pool only on `connect_plain_supervised`, which requires a
/// non-`None` `supervisor` field on the config.
fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig::default()),
        ..ConnectionConfig::default()
    }
}

/// Count the distinct sessions that served at least one frame of `kind`.
fn sessions_serving(snapshot: &[SessionRecord], kind: pb::base_command::Type) -> usize {
    snapshot
        .iter()
        .filter(|s| s.frames.contains(&(kind as i32)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_per_broker_fans_producers_across_connections() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                HANG_GUARD,
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
            .expect("connect ok")
            .with_connections_per_broker(3);

            let mut producers = Vec::new();
            for i in 0..3 {
                let p = tokio::time::timeout(
                    HANG_GUARD,
                    client.open_producer(CreateProducerRequest {
                        topic: format!("persistent://public/default/cpb-moonpool-fanout-{i}"),
                        ..Default::default()
                    }),
                )
                .await
                .expect("open_producer did not time out")
                .expect("open_producer ok");
                producers.push(p);
            }

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(producers);
            drop(client);

            assert_eq!(
                snapshot.len(),
                3,
                "connections_per_broker(3) must open exactly 3 connections (bootstrap + 2 \
                 siblings), got {} — {snapshot:?}",
                snapshot.len()
            );
            assert_eq!(
                sessions_serving(&snapshot, pb::base_command::Type::Producer),
                3,
                "each of the 3 producers must land on a distinct connection: {snapshot:?}"
            );
            for (idx, s) in snapshot.iter().enumerate() {
                assert!(
                    s.connect_proxy_to_broker_url.is_none(),
                    "session {idx} CONNECT must carry no proxy_to_broker_url (direct bootstrap \
                     broker), got {:?}",
                    s.connect_proxy_to_broker_url
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_per_broker_one_keeps_single_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                HANG_GUARD,
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
            .expect("connect ok")
            .with_connections_per_broker(1);

            let mut producers = Vec::new();
            for i in 0..3 {
                let p = tokio::time::timeout(
                    HANG_GUARD,
                    client.open_producer(CreateProducerRequest {
                        topic: format!("persistent://public/default/cpb-moonpool-single-{i}"),
                        ..Default::default()
                    }),
                )
                .await
                .expect("open_producer did not time out")
                .expect("open_producer ok");
                producers.push(p);
            }

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(producers);
            drop(client);

            assert_eq!(
                snapshot.len(),
                1,
                "connections_per_broker(1) must keep all producers on the single bootstrap \
                 connection, got {} — {snapshot:?}",
                snapshot.len()
            );
            let producer_frames = snapshot[0]
                .frames
                .iter()
                .filter(|k| **k == pb::base_command::Type::Producer as i32)
                .count();
            assert_eq!(
                producer_frames, 3,
                "the single connection must have served all three producers; saw {producer_frames}"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_per_broker_fans_consumers_across_connections() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) = spawn_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let client = tokio::time::timeout(
                HANG_GUARD,
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
            .expect("connect ok")
            .with_connections_per_broker(2);

            let mut consumers = Vec::new();
            for i in 0..2 {
                let c = tokio::time::timeout(
                    HANG_GUARD,
                    client.subscribe(SubscribeRequest {
                        topic: format!("persistent://public/default/cpb-moonpool-consumer-{i}"),
                        subscription: format!("cpb-moonpool-sub-{i}"),
                        receiver_queue_size: 16,
                        durable: true,
                        ..Default::default()
                    }),
                )
                .await
                .expect("subscribe did not time out")
                .expect("subscribe ok");
                consumers.push(c);
            }

            let snapshot = sessions.lock().clone();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(consumers);
            drop(client);

            assert_eq!(
                snapshot.len(),
                2,
                "connections_per_broker(2) must spread the two consumers over 2 connections, \
                 got {} — {snapshot:?}",
                snapshot.len()
            );
            assert_eq!(
                sessions_serving(&snapshot, pb::base_command::Type::Subscribe),
                2,
                "each consumer must land on a distinct connection: {snapshot:?}"
            );
        })
        .await;
}

/// A `connections_per_broker` sibling is a replica of the bootstrap connection, so — like the
/// bootstrap — it follows the PIP-121 `ServiceUrlProvider` when its socket drops (ADR-0039,
/// ADR-0073). Only an entry pinned to another broker (multi-broker DIRECT) stops consulting the
/// provider; a sibling carries the same `proxy_to_broker_url = None` as such an entry, so this
/// pins that the distinction is the dialled address and not that field.
///
/// Broker A is the bootstrap and serves the sibling; the provider is then switched to broker C and
/// A drops the sibling's session. The supervisor's redial must be accepted by C.
///
/// Mirrors the tokio integration test of the same name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_bootstrap_sibling_follows_the_service_url_provider() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_a, sessions_a, session_tasks_a) = spawn_abortable_broker().await;
            let (host_c, sessions_c, _session_tasks_c) = spawn_abortable_broker().await;

            let provider = Arc::new(ControlledClusterFailover::new(format!("pulsar://{host_a}")));
            let cfg = ConnectionConfig {
                supervisor: Some(SupervisorConfig {
                    initial_backoff: Duration::from_millis(5),
                    max_backoff: Duration::from_millis(40),
                    ..SupervisorConfig::default()
                }),
                ..ConnectionConfig::default()
            };
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain_supervised(
                    &engine,
                    &host_a,
                    cfg,
                    Some(provider.clone()),
                    None,
                ),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok")
            .with_connections_per_broker(2);

            // Two producers round-robin over index 0 (the bootstrap) and index 1 (the pool
            // sibling).
            let mut producers = Vec::new();
            for i in 0..2 {
                let p = tokio::time::timeout(
                    HANG_GUARD,
                    client.open_producer(CreateProducerRequest {
                        topic: format!(
                            "persistent://public/default/cpb-moonpool-sibling-redial-{i}"
                        ),
                        ..Default::default()
                    }),
                )
                .await
                .expect("open_producer did not time out")
                .expect("open_producer ok");
                producers.push(p);
            }

            // Precondition: the bootstrap (session 0) and the sibling (session 1) are both on A.
            assert_eq!(
                sessions_a.lock().len(),
                2,
                "A serves the bootstrap and the sibling"
            );
            assert_eq!(sessions_c.lock().len(), 0, "C has not been dialled yet");

            // The cluster fails over to C, then A drops the sibling's connection. The bootstrap
            // session stays up, so only the sibling's supervisor redials.
            provider.set_url(format!("pulsar://{host_c}"));
            session_tasks_a.lock()[1].abort();

            // Wait, bounded, for the sibling's redial to be accepted by either broker.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while sessions_a.lock().len() + sessions_c.lock().len() < 3
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let accepted_by_a = sessions_a.lock().len();
            let accepted_by_c = sessions_c.lock().len();
            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(producers);
            drop(client);

            assert_eq!(
                (accepted_by_a, accepted_by_c),
                (2, 1),
                "a bootstrap sibling must re-dial the provider's current service URL (C) after \
                 its socket drops (sessions accepted: A={accepted_by_a}, C={accepted_by_c})",
            );
        })
        .await;
}
