// SPDX-License-Identifier: Apache-2.0

//! Tokio ↔ moonpool differential equivalence for `connections_per_broker`
//! (Java `ClientBuilder#connectionsPerBroker`, ADR-0073, issue #314). Layer (d)
//! of the ADR-0024 four-layer test policy.
//!
//! The single-producer `Trace`/runner model can't observe the fan-out (one
//! producer always takes round-robin slot 0), so this test drives a small
//! fake single-broker directly against BOTH engines: a client with
//! `connections_per_broker(3)` opens three producers, and we assert each engine
//! realizes the **same** connection layout — three distinct connections, each
//! serving exactly one producer. A regression that fanned out on one engine but
//! not the other (or picked indices differently) would diverge the per-engine
//! connection counts.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const HANG_GUARD: Duration = Duration::from_secs(60);
const FANOUT: usize = 3;

/// Per-session frame-kind log. One session == one TCP connection the client
/// opened, so `summary()`'s vector length is the realized connection count.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    frames: Vec<i32>,
}

/// Spawn a fake single-broker Pulsar. Returns the bound `host:port` and the
/// per-session record log.
async fn spawn_broker() -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
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
            if !matches!(
                pb::base_command::Type::try_from(kind).ok(),
                Some(pb::base_command::Type::Connect)
            ) {
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
                    server_version: "magnetar-cpb-equiv".to_owned(),
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
        _ => {}
    }
}

/// The realized connection layout for one engine run: the number of distinct
/// `CommandProducer` frames served by each session, sorted descending. With
/// `connections_per_broker(3)` and three producers round-robined, both engines
/// must produce `[1, 1, 1]`.
fn producers_per_session(snapshot: &[SessionRecord]) -> Vec<usize> {
    let mut counts: Vec<usize> = snapshot
        .iter()
        .map(|s| {
            s.frames
                .iter()
                .filter(|k| **k == pb::base_command::Type::Producer as i32)
                .count()
        })
        .filter(|c| *c > 0)
        .collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    counts
}

/// Open `FANOUT` producers through the tokio engine with
/// `connections_per_broker(FANOUT)`; return the producers-per-session layout.
async fn tokio_layout() -> Vec<usize> {
    use magnetar_runtime_tokio::Client;

    let (host_port, sessions) = spawn_broker().await;
    let url = format!("pulsar://{host_port}");
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect ok")
    .with_connections_per_broker(FANOUT);

    let mut producers = Vec::new();
    for i in 0..FANOUT {
        let p = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(CreateProducerRequest {
                topic: format!("persistent://public/default/cpb-equiv-tokio-{i}"),
                ..Default::default()
            }),
        )
        .await
        .expect("tokio open_producer did not time out")
        .expect("tokio open_producer ok");
        producers.push(p);
    }

    let layout = producers_per_session(&sessions.lock());
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(producers);
    drop(client);
    layout
}

/// Open `FANOUT` producers through the moonpool engine with
/// `connections_per_broker(FANOUT)`; return the producers-per-session layout.
async fn moonpool_layout() -> Vec<usize> {
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let (host_port, sessions) = spawn_broker().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &host_port, config, None, None),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect ok")
    .with_connections_per_broker(FANOUT);

    let mut producers = Vec::new();
    for i in 0..FANOUT {
        let p = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(CreateProducerRequest {
                topic: format!("persistent://public/default/cpb-equiv-moonpool-{i}"),
                ..Default::default()
            }),
        )
        .await
        .expect("moonpool open_producer did not time out")
        .expect("moonpool open_producer ok");
        producers.push(p);
    }

    let layout = producers_per_session(&sessions.lock());
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(producers);
    drop(client);
    layout
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connections_per_broker_fanout_is_engine_equivalent() {
    let tokio_layout = tokio_layout().await;

    // The moonpool engine's pool dial hoists onto `spawn_local`, so its leg must
    // run inside a `LocalSet`.
    let local = tokio::task::LocalSet::new();
    let moonpool_layout = local.run_until(moonpool_layout()).await;

    assert_eq!(
        tokio_layout,
        vec![1, 1, 1],
        "tokio: connections_per_broker(3) must spread 3 producers over 3 connections"
    );
    assert_eq!(
        tokio_layout, moonpool_layout,
        "tokio and moonpool must realize the same connections_per_broker fan-out layout: \
         tokio={tokio_layout:?} moonpool={moonpool_layout:?}"
    );
}
