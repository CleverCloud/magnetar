// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for multi-broker DIRECT routing — ADR-0039
//! §"Multi-broker DIRECT routing (2026-06-01)" / HIGH-1 from the lookup
//! multi-agent review.
//!
//! Scenario:
//!
//! 1. **Broker A** is the *bootstrap* broker. The client connects to it first; `CommandLookupTopic`
//!    arrives here.
//! 2. **Broker B** is a separate broker (different `host:port`). It answers `CommandProducer` /
//!    `CommandSubscribe`.
//! 3. Broker A answers every lookup with `LookupOutcome::Connect { broker_service_url = Some(B),
//!    proxy_through_service_url = false }`.
//! 4. The runtime is expected to open a **second** TCP connection — to broker B — and route the
//!    producer / subscribe frames there. The second connection's
//!    `CommandConnect.proxy_to_broker_url` must be `None` (we are dialling B directly, not through
//!    a proxy).
//!
//! The test asserts:
//! - Exactly **two** distinct backends accepted connections (A + B).
//! - The bootstrap session on A saw the LOOKUP and *did not* see the `CommandProducer` /
//!   `CommandSubscribe`.
//! - The pinned session on B saw the data frame.
//! - Its `CommandConnect` carries no `proxy_to_broker_url` (it is **not** a proxy-routed
//!   connection, despite riding through the same `ProxyConnectionPool`).
//! - A second producer to the same topic reuses the existing B-pinned pool entry (no third TCP
//!   session).
//!
//! Sibling moonpool simulation coverage lives in
//! `crates/magnetar-runtime-moonpool/tests/lookup_direct_multi_broker.rs`
//! (ADR-0024 1:1 parity).

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

/// Per-session log: records the `proxy_to_broker_url` seen on `CommandConnect`
/// and the kinds of every subsequent frame, in arrival order.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    connect_proxy_to_broker_url: Option<String>,
    frames: Vec<i32>,
}

#[derive(Clone)]
struct BrokerRole {
    /// `Some(url)` when this broker should redirect lookups to the *other*
    /// broker via `LookupOutcome::Connect { broker_service_url = url,
    /// proxy_through_service_url = false }`. `None` means this broker
    /// answers lookups by claiming itself (which makes the runtime reuse
    /// the bootstrap connection — useful as the data plane on broker B).
    redirect_to: Option<String>,
}

/// Spawn an in-process broker on `127.0.0.1:0`. Returns the bound URL and
/// the per-session record log.
async fn spawn_broker(role: BrokerRole) -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<SessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    let url = format!("pulsar://{addr}");
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
            let role = role.clone();
            tokio::spawn(async move {
                let _ = handle_session(stream, &sessions, session_idx, &role).await;
            });
        }
    });
    (url, sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    role: &BrokerRole,
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

            handle_frame(&frame, &mut out_buf, role);
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

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, role: &BrokerRole) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-direct-multi-broker-test".to_owned(),
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
                // The crux of the test: the bootstrap broker redirects lookups
                // to the other broker via DIRECT routing (proxy_through =
                // false). Without the ADR-0039 §2026-06-01 amendment the
                // runtime would have ignored this `broker_service_url` and
                // tried to open the producer on the bootstrap connection —
                // the original HIGH-1 bug.
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: role.redirect_to.clone(),
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
                        producer_name: "direct-multi-broker-test".to_owned(),
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
async fn open_producer_routes_to_resolved_broker() {
    // 1. Bring up B first — A needs B's URL to redirect.
    let (url_b, sessions_b) = spawn_broker(BrokerRole { redirect_to: None }).await;
    // 2. Bring up A and tell it to redirect every lookup to B.
    let (url_a, sessions_a) = spawn_broker(BrokerRole {
        redirect_to: Some(url_b.clone()),
    })
    .await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("connect ok")
    .expect("connect ok");

    let _producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/direct-multi-broker-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer ok")
    .expect("open_producer ok");

    let snap_a = sessions_a.lock().clone();
    let snap_b = sessions_b.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // Bootstrap (A) was hit exactly once: the bootstrap connection.
    assert_eq!(
        snap_a.len(),
        1,
        "bootstrap broker A must have served exactly one connection (the bootstrap), got {} \
         sessions",
        snap_a.len()
    );

    // Resolved broker (B) was hit exactly once: the pinned pool entry for B.
    assert_eq!(
        snap_b.len(),
        1,
        "resolved broker B must have served exactly one connection (the pinned pool entry), got \
         {} sessions",
        snap_b.len()
    );

    let bootstrap = &snap_a[0];
    let pinned = &snap_b[0];

    // Bootstrap CONNECT carries no `proxy_to_broker_url` (it is the
    // connect-time URL — never proxy-routed).
    assert!(
        bootstrap.connect_proxy_to_broker_url.is_none(),
        "bootstrap CONNECT must NOT set proxy_to_broker_url, got {:?}",
        bootstrap.connect_proxy_to_broker_url,
    );

    // Pinned CONNECT also carries no `proxy_to_broker_url` — multi-broker
    // DIRECT routing dials the broker directly (no proxy in the middle).
    // This is the crucial bit that distinguishes the DIRECT pool path
    // from the PROXY pool path (where the pinned CONNECT *does* set
    // `proxy_to_broker_url = Some(broker_host_port)`).
    assert!(
        pinned.connect_proxy_to_broker_url.is_none(),
        "pinned CONNECT to a directly-dialled broker must NOT set proxy_to_broker_url, got {:?}",
        pinned.connect_proxy_to_broker_url,
    );

    // Bootstrap session saw the LOOKUP; pinned session saw the PRODUCER.
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
        "bootstrap session must have seen the LOOKUP, got {bootstrap_kinds:?}",
    );
    assert!(
        pinned_kinds.contains(&pb::base_command::Type::Producer),
        "pinned session must have seen the PRODUCER, got {pinned_kinds:?}",
    );
    assert!(
        !bootstrap_kinds.contains(&pb::base_command::Type::Producer),
        "bootstrap session must NOT have seen PRODUCER (multi-broker DIRECT routing must have \
         landed it on the pinned session)",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_routes_to_resolved_broker() {
    let (url_b, sessions_b) = spawn_broker(BrokerRole { redirect_to: None }).await;
    let (url_a, _sessions_a) = spawn_broker(BrokerRole {
        redirect_to: Some(url_b.clone()),
    })
    .await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("connect ok")
    .expect("connect ok");

    let _consumer = tokio::time::timeout(
        Duration::from_secs(5),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/direct-multi-broker-consumer".to_owned(),
            subscription: "direct-multi-broker-sub".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe ok")
    .expect("subscribe ok");

    let snap_b = sessions_b.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    assert_eq!(
        snap_b.len(),
        1,
        "resolved broker B must have served the SUBSCRIBE on a single pinned session, got {} \
         sessions",
        snap_b.len()
    );
    let pinned = &snap_b[0];
    assert!(
        pinned.connect_proxy_to_broker_url.is_none(),
        "pinned CONNECT (DIRECT route) must NOT set proxy_to_broker_url"
    );
    let kinds: Vec<_> = pinned
        .frames
        .iter()
        .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
        .collect();
    assert!(
        kinds.contains(&pb::base_command::Type::Subscribe),
        "pinned session must have seen SUBSCRIBE, got {kinds:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_producer_to_same_broker_reuses_pool_entry() {
    let (url_b, sessions_b) = spawn_broker(BrokerRole { redirect_to: None }).await;
    let (url_a, _sessions_a) = spawn_broker(BrokerRole {
        redirect_to: Some(url_b.clone()),
    })
    .await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("connect ok")
    .expect("connect ok");

    let _p1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/direct-multi-broker-reuse-a".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("p1 ok")
    .expect("p1 ok");

    let _p2 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/direct-multi-broker-reuse-b".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("p2 ok")
    .expect("p2 ok");

    let snap_b = sessions_b.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // Both producers landed on the SAME pinned pool entry — exactly one
    // session served them.
    assert_eq!(
        snap_b.len(),
        1,
        "second producer must reuse the existing pinned pool entry on broker B; got {} sessions",
        snap_b.len(),
    );

    let producer_count = snap_b[0]
        .frames
        .iter()
        .filter(|k| **k == pb::base_command::Type::Producer as i32)
        .count();
    assert_eq!(
        producer_count, 2,
        "pinned session must have served both producers; saw {producer_count}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_resolving_to_bootstrap_broker_reuses_bootstrap_connection() {
    // When the lookup advertises the *bootstrap* broker as the resolved
    // target, the bootstrap-equality fast path must reuse the existing
    // bootstrap connection — no new TCP session opened. Mirrors the Java
    // pool's identity check.

    // A single broker that redirects lookups to itself.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ok");
    let addr = listener.local_addr().expect("addr ok");
    let url = format!("pulsar://{addr}");
    let sessions: Arc<Mutex<Vec<SessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    let redirect_to = url.clone();
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
            let role = BrokerRole {
                redirect_to: Some(redirect_to.clone()),
            };
            tokio::spawn(async move {
                let _ = handle_session(stream, &sessions, session_idx, &role).await;
            });
        }
    });

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect ok")
    .expect("connect ok");

    let _producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/direct-bootstrap-equality".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer ok")
    .expect("open_producer ok");

    let snap = sessions.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // Bootstrap-equality fast path: exactly ONE session — the bootstrap.
    assert_eq!(
        snap.len(),
        1,
        "lookup resolving to the bootstrap broker must reuse the bootstrap connection (no new \
         pool entry), got {} sessions",
        snap.len(),
    );

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
        "bootstrap session must have seen PRODUCER (reused, not pooled), got {kinds:?}",
    );
}
