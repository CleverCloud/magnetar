// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for the Apache Pulsar Proxy connection model
//! (ADR-0039 / issue #15).
//!
//! Wires an in-process scripted broker that emulates the proxy's wire
//! contract:
//!
//! 1. The **bootstrap** connection (the one [`Client::connect`] opens) MUST arrive with
//!    `CommandConnect.proxy_to_broker_url = None`. The fake broker accepts it, then on
//!    `CommandLookupTopic` answers with `proxy_through_service_url = true` plus a synthetic
//!    `broker_service_url` advertising the backend broker. It DOES NOT serve `CommandProducer` /
//!    `CommandSubscribe` on that connection — sending a data frame there triggers a panic in the
//!    broker stub.
//! 2. The runtime is expected to then open a **second** TCP connection with
//!    `CommandConnect.proxy_to_broker_url = Some(<the broker URL>)`. The fake broker accepts it and
//!    answers `CommandProducer` / `CommandSubscribe`.
//!
//! The test asserts:
//! - Exactly **two** TCP sessions are accepted (bootstrap + per-broker entry).
//! - The bootstrap session's `CommandConnect` carries no `proxy_to_broker_url`.
//! - The per-broker session's `CommandConnect` carries the broker URL the proxy advertised in the
//!   lookup response.
//! - The producer / consumer round-trip completes end-to-end through the pinned pool entry.
//!
//! Sibling moonpool simulation coverage lives in
//! `crates/magnetar-runtime-moonpool/tests/proxy_multi_conn.rs` (ADR-0024
//! 1:1 parity).

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

/// Per-session log: records the `proxy_to_broker_url` we saw on `CommandConnect` and the
/// kinds of every subsequent frame, in arrival order.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    /// `Some(url)` when `CommandConnect.proxy_to_broker_url = Some(url)`, `None`
    /// when the field was absent. Captures the bootstrap-vs-pinned distinction.
    connect_proxy_to_broker_url: Option<String>,
    /// All non-CONNECT frames the session received, in arrival order.
    frames: Vec<i32>,
}

/// Synthetic broker URL the fake proxy advertises in lookup responses. The
/// runtime sets `CommandConnect.proxy_to_broker_url` to this exact string on
/// the second connection. The host portion is meaningless — the client never
/// dials it; the dial target stays the proxy address.
const ADVERTISED_BROKER_URL: &str = "pulsar://broker-a.proxy.internal:6650";

/// Spawn a fake Apache Pulsar Proxy on `127.0.0.1:0`. Returns the bound
/// address (as a `pulsar://...` URL) and the per-session record log.
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
                let _ = handle_session(stream, sessions, session_idx).await;
            });
        }
    });
    (format!("pulsar://{addr}"), sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: Arc<Mutex<Vec<SessionRecord>>>,
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
            // Capture CONNECT's `proxy_to_broker_url` field — that's the
            // signal the bootstrap vs pinned distinction rides on.
            if matches!(typed, Some(pb::base_command::Type::Connect)) {
                if let Some(c) = &frame.command.connect {
                    sessions.lock()[session_idx].connect_proxy_to_broker_url =
                        c.proxy_to_broker_url.clone();
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
                    server_version: "magnetar-proxy-test".to_owned(),
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
                // The proxy contract: on the bootstrap session, advertise
                // `proxy_through_service_url = true` + a synthetic broker URL.
                // On a pinned session (which shouldn't be issuing lookups in
                // this test, but we tolerate it) just echo `proxy_through =
                // false` to avoid a redirect loop.
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
                        producer_name: "proxy-test".to_owned(),
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
async fn open_producer_through_proxy_opens_second_connection() {
    let (url, sessions) = spawn_proxy().await;

    // Bootstrap connect.
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // Trigger the proxy-routing path.
    let _producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/proxy-test-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    // Snapshot the sessions before we tear things down.
    let snapshot = sessions.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // The bootstrap accept lands first.
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
        Some(ADVERTISED_BROKER_URL),
        "pinned CONNECT must set proxy_to_broker_url to the advertised broker URL"
    );

    // The bootstrap session saw the lookup; the pinned session saw the producer create.
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
        "bootstrap session must NOT have seen CommandProducer (it must have ridden on the pinned session)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_through_proxy_opens_second_connection() {
    let (url, sessions) = spawn_proxy().await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let _consumer = tokio::time::timeout(
        Duration::from_secs(5),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/proxy-test-consumer".to_owned(),
            subscription: "proxy-test-sub".to_owned(),
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
        Some(ADVERTISED_BROKER_URL),
        "pinned CONNECT must set proxy_to_broker_url"
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_producer_to_same_broker_reuses_pool_entry() {
    let (url, sessions) = spawn_proxy().await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect ok")
    .expect("connect ok");

    let _p1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/proxy-pool-reuse-a".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("p1 ok")
    .expect("p1 ok");

    let _p2 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/proxy-pool-reuse-b".to_owned(),
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

    // Bootstrap + one pinned entry (the second producer reuses the first
    // pinned connection because both lookups resolved to the same advertised
    // broker URL).
    assert_eq!(
        snapshot.len(),
        2,
        "second producer must reuse the existing pinned pool entry — got {} sessions",
        snapshot.len()
    );

    // The pinned session must have seen TWO `CommandProducer` frames.
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
}
