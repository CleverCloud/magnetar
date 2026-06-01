// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 / ADR-0024 layer (d) — the tokio engine and the moonpool engine
//! MUST observe equivalent end-to-end behaviour when a `CommandLookupTopic`
//! answer carries `proxy_through_service_url = true`: both must open a
//! pinned second connection back to the proxy address with
//! `CommandConnect.proxy_to_broker_url = host:port`, then route
//! `CommandProducer` + `CommandSend` onto that pinned connection.
//!
//! Shape: a single in-process proxy fake (per-test) speaks the proxy wire
//! contract. Both engines run against the **same** proxy fake, in sequence,
//! and we assert each engine independently observes the same
//! (bootstrap + pinned) session sequence with matching CONNECT flags.
//!
//! Why no shared `ScriptedBroker`: the proxy contract is fundamentally
//! different from the single-broker contract — every lookup answers
//! `proxy_through_service_url = true` on the bootstrap session, and only
//! the pinned session serves data ops. Wedging that into the
//! `magnetar_differential::broker::ScriptedBroker` would have rippled
//! through every other differential test; keeping the proxy fake local to
//! this file is a cleaner ADR-0024 §"layer d" implementation.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const ADVERTISED_BROKER_URL: &str = "pulsar://broker-a.proxy.diff.internal:6650";
const ADVERTISED_BROKER_HOST_PORT: &str = "broker-a.proxy.diff.internal:6650";

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    connect_proxy_to_broker_url: Option<String>,
    frames: Vec<i32>,
}

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
            respond(&frame, &mut out_buf, session_idx);
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

fn respond(frame: &magnetar_proto::Frame, out: &mut BytesMut, session_idx: usize) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "diff-proxy".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
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
                        producer_name: "diff-proxy".to_owned(),
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

/// `Trace`-style observation snapshot we compare across engines.
#[derive(Debug, PartialEq, Eq)]
struct ProxyObservation {
    /// Number of distinct TCP sessions accepted by the proxy fake.
    session_count: usize,
    /// `connect_proxy_to_broker_url` field on the bootstrap session
    /// (session 0). `None` is the expected shape.
    bootstrap_connect_proxy_to_broker_url: Option<String>,
    /// `connect_proxy_to_broker_url` field on the pinned session
    /// (session 1). Must be `Some("host:port")`.
    pinned_connect_proxy_to_broker_url: Option<String>,
    /// Whether the bootstrap session saw a `CommandLookupTopic`.
    bootstrap_saw_lookup: bool,
    /// Whether the pinned session saw a `CommandProducer`.
    pinned_saw_producer: bool,
    /// Whether the bootstrap session was kept free of `CommandProducer` —
    /// the ADR-0039 contract: producer ops MUST ride on the pinned entry.
    bootstrap_free_of_producer: bool,
}

fn observation_from(snapshot: &[SessionRecord]) -> ProxyObservation {
    let session_count = snapshot.len();
    let bootstrap = snapshot.first().cloned().unwrap_or_default();
    let pinned = snapshot.get(1).cloned().unwrap_or_default();
    let bootstrap_kinds: Vec<i32> = bootstrap.frames.clone();
    let pinned_kinds: Vec<i32> = pinned.frames.clone();
    let lookup_kind = pb::base_command::Type::Lookup as i32;
    let producer_kind = pb::base_command::Type::Producer as i32;
    ProxyObservation {
        session_count,
        bootstrap_connect_proxy_to_broker_url: bootstrap.connect_proxy_to_broker_url,
        pinned_connect_proxy_to_broker_url: pinned.connect_proxy_to_broker_url,
        bootstrap_saw_lookup: bootstrap_kinds.contains(&lookup_kind),
        pinned_saw_producer: pinned_kinds.contains(&producer_kind),
        bootstrap_free_of_producer: !bootstrap_kinds.contains(&producer_kind),
    }
}

async fn run_tokio(url: &str) -> ProxyObservation {
    use magnetar_runtime_tokio::Client;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(url, ConnectionConfig::default()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect ok");

    let _producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/proxy-routing-equiv-tokio".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("tokio open_producer did not time out")
    .expect("tokio open_producer ok");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    // `ProxyObservation` is filled in by the caller from the per-engine
    // `sessions` snapshot.
    ProxyObservation {
        session_count: 0,
        bootstrap_connect_proxy_to_broker_url: None,
        pinned_connect_proxy_to_broker_url: None,
        bootstrap_saw_lookup: false,
        pinned_saw_producer: false,
        bootstrap_free_of_producer: false,
    }
}

async fn run_moonpool(host_port: &str) -> ProxyObservation {
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, host_port, cfg, None, None),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect ok");

    let _producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/proxy-routing-equiv-moonpool".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("moonpool open_producer did not time out")
    .expect("moonpool open_producer ok");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    ProxyObservation {
        session_count: 0,
        bootstrap_connect_proxy_to_broker_url: None,
        pinned_connect_proxy_to_broker_url: None,
        bootstrap_saw_lookup: false,
        pinned_saw_producer: false,
        bootstrap_free_of_producer: false,
    }
}

/// Both engines, when handed an `open_producer` on a proxy-routed topic,
/// must produce equivalent observations: same session count (bootstrap +
/// pinned), same CONNECT flags, same per-session command kinds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_producer_through_proxy_observation_parity() {
    // Each engine gets its own proxy fake so the sessions vectors stay
    // independent — running both against the *same* fake would race the
    // bootstrap-session index between engines.
    let (tokio_host_port, tokio_sessions) = spawn_proxy().await;
    let tokio_url = format!("pulsar://{tokio_host_port}");
    let _ = run_tokio(&tokio_url).await;
    let tokio_snap = tokio_sessions.lock().clone();
    let tokio_obs = observation_from(&tokio_snap);

    let (moonpool_host_port, moonpool_sessions) = spawn_proxy().await;
    let _ = run_moonpool(&moonpool_host_port).await;
    let moonpool_snap = moonpool_sessions.lock().clone();
    let moonpool_obs = observation_from(&moonpool_snap);

    // First, sanity-check the absolute shape of each engine's observation
    // — proves both engines actually did the proxy dance (not that they
    // both silently failed in the same way).
    assert_eq!(
        tokio_obs.session_count, 2,
        "tokio: expected bootstrap + pinned, got {tokio_obs:?}"
    );
    assert!(
        tokio_obs.bootstrap_connect_proxy_to_broker_url.is_none(),
        "tokio bootstrap CONNECT must NOT set proxy_to_broker_url"
    );
    assert_eq!(
        tokio_obs.pinned_connect_proxy_to_broker_url.as_deref(),
        Some(ADVERTISED_BROKER_HOST_PORT)
    );
    assert!(tokio_obs.bootstrap_saw_lookup);
    assert!(tokio_obs.pinned_saw_producer);
    assert!(tokio_obs.bootstrap_free_of_producer);

    assert_eq!(
        moonpool_obs.session_count, 2,
        "moonpool: expected bootstrap + pinned, got {moonpool_obs:?}"
    );
    assert!(moonpool_obs.bootstrap_connect_proxy_to_broker_url.is_none());
    assert_eq!(
        moonpool_obs.pinned_connect_proxy_to_broker_url.as_deref(),
        Some(ADVERTISED_BROKER_HOST_PORT)
    );
    assert!(moonpool_obs.bootstrap_saw_lookup);
    assert!(moonpool_obs.pinned_saw_producer);
    assert!(moonpool_obs.bootstrap_free_of_producer);

    // Then, the equivalence assertion proper.
    assert_eq!(
        tokio_obs, moonpool_obs,
        "engine observations diverged for the proxy-routing scenario"
    );
}
