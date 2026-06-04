// SPDX-License-Identifier: Apache-2.0

//! Lifecycle coverage for the ADR-0039 per-broker connection pool on the
//! tokio engine — the 1:1 mirror of
//! `magnetar-runtime-moonpool/tests/pool_lifecycle.rs` (ADR-0024
//! `check-runtime-test-parity`: two `#[tokio::test]` functions here mirror the
//! moonpool file's two `#[test]` functions).
//!
//! ## What this pins
//!
//! A `proxy_through_service_url = true` lookup must open a *pooled* per-broker
//! connection that is established and reused, and engine `close()` must tear
//! that pooled dial down without panic:
//!
//! 1. **Pooled connection established + used.** `Client::connect` opens the bootstrap connection;
//!    the fake proxy answers `CommandLookupTopic` with `proxy_through_service_url = true` plus a
//!    synthetic `broker_service_url`. The runtime then opens a *second* TCP connection whose
//!    `CommandConnect.proxy_to_broker_url` is the advertised broker's `host:port` (scheme stripped,
//!    ADR-0039), and the producer create rides that pinned pool entry. The producer open only
//!    resolves if the pinned connection handshaked and round-tripped `CommandProducer`.
//! 2. **Reuse.** A second producer to the same advertised broker URL reuses the existing pinned
//!    pool entry — no third TCP session.
//! 3. **Clean teardown of pooled dials.** `Client::close()` drains the pool
//!    (`ProxyConnectionPool::close`), closing every entry's connection and joining its supervised
//!    driver. The test asserts `close()` returns within a bound — a wedged pool teardown (e.g. a
//!    driver join that never resolves) would blow the timeout.
//!
//! Sibling moonpool simulation coverage lives in
//! `crates/magnetar-runtime-moonpool/tests/pool_lifecycle.rs`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Synthetic broker URL the fake proxy advertises in lookup responses. The
/// host is meaningless — the client never dials it; the pinned pool entry
/// stays on the proxy address and rides `proxy_to_broker_url` to reach it.
const ADVERTISED_BROKER_URL: &str = "pulsar://broker-pool-lifecycle.proxy.internal:6650";

/// `host:port` form of [`ADVERTISED_BROKER_URL`] — the value the runtime must
/// stuff into `CommandConnect.proxy_to_broker_url` after stripping the
/// `pulsar://` scheme (parity with Java + pulsar-rs; ADR-0039).
const ADVERTISED_BROKER_HOST_PORT: &str = "broker-pool-lifecycle.proxy.internal:6650";

/// Per-session log: the `proxy_to_broker_url` seen on `CommandConnect` and the
/// kinds of every subsequent frame, in arrival order. Captures the
/// bootstrap-vs-pinned distinction.
#[derive(Debug, Default, Clone)]
struct SessionRecord {
    /// `Some(url)` when `CommandConnect.proxy_to_broker_url = Some(url)`, `None`
    /// when the field was absent.
    connect_proxy_to_broker_url: Option<String>,
    /// All non-CONNECT frame kinds the session received, in arrival order.
    frames: Vec<i32>,
}

/// Spawn a fake Apache Pulsar Proxy on `127.0.0.1:0`. Returns the bound address
/// (as a `pulsar://...` URL) and the per-session record log.
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
    (format!("pulsar://{addr}"), sessions)
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
            if matches!(
                pb::base_command::Type::try_from(kind).ok(),
                Some(pb::base_command::Type::Connect)
            ) {
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
                    server_version: "magnetar-pool-lifecycle".to_owned(),
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
                // Only the bootstrap session (idx 0) advertises
                // proxy_through=true; pinned sessions echo false to avoid a
                // redirect loop (they shouldn't issue lookups in this test).
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
                        producer_name: "pool-lifecycle".to_owned(),
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

/// Mirror of `moonpool_pooled_proxy_connection_opens_and_tears_down_clean_smoke`.
/// A `proxy_through_service_url = true` lookup opens a pooled per-broker
/// connection; the producer rides it; then `Client::close()` tears the pool
/// down cleanly (returns within a bound — a wedged teardown would time out).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_pooled_proxy_connection_opens_and_tears_down_clean() {
    let (url, sessions) = spawn_proxy().await;

    // Bootstrap connect.
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // Open a producer through the proxy → forces the pinned pool dial.
    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/pool-lifecycle-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    // Snapshot the pool-open shape before teardown.
    let snapshot = sessions.lock().clone();
    assert!(
        snapshot.len() >= 2,
        "proxy_through lookup must open a SECOND pooled connection (bootstrap + pinned), \
         got {snapshot:?}",
    );
    let bootstrap = &snapshot[0];
    let pinned = &snapshot[1];
    assert!(
        bootstrap.connect_proxy_to_broker_url.is_none(),
        "bootstrap CONNECT must NOT carry proxy_to_broker_url, got {:?}",
        bootstrap.connect_proxy_to_broker_url
    );
    assert_eq!(
        pinned.connect_proxy_to_broker_url.as_deref(),
        Some(ADVERTISED_BROKER_HOST_PORT),
        "pinned pool CONNECT must carry proxy_to_broker_url = host:port (no scheme)"
    );
    assert!(
        pinned
            .frames
            .contains(&(pb::base_command::Type::Producer as i32)),
        "pooled producer open must ride the pinned connection; pinned frames {:?}",
        pinned.frames
    );

    // Clean teardown: `close(self)` drains the proxy pool (ADR-0039), closing
    // the pinned connection and joining its supervised driver. It must return
    // within a bound — a wedged pool teardown (driver join that never resolves)
    // would blow this timeout. We drop the producer first so the only thing
    // keeping the pinned connection alive is the pool itself.
    drop(producer);
    tokio::time::timeout(Duration::from_secs(5), client.close())
        .await
        .expect("Client::close must drain the proxy pool and return (no wedged teardown)");
}

/// Mirror of
/// `moonpool_pooled_proxy_connection_opens_and_tears_down_clean_sweep_8_seeds`'s
/// reuse + clean-teardown angle. A second producer to the same advertised
/// broker URL reuses the existing pinned pool entry (no third TCP session),
/// then `Client::close()` tears the single pinned entry down cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_pooled_proxy_connection_reused_then_torn_down() {
    let (url, sessions) = spawn_proxy().await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let p1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/pool-lifecycle-reuse-a".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("p1 did not time out")
    .expect("p1 ok");

    let p2 = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/pool-lifecycle-reuse-b".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("p2 did not time out")
    .expect("p2 ok");

    let snapshot = sessions.lock().clone();
    // Bootstrap + exactly one pinned entry — the second producer reused the
    // first pinned connection because both lookups resolved to the same
    // advertised broker URL.
    assert_eq!(
        snapshot.len(),
        2,
        "second producer must REUSE the existing pinned pool entry — got {} sessions: {snapshot:?}",
        snapshot.len(),
    );
    let pinned = &snapshot[1];
    let producer_count = pinned
        .frames
        .iter()
        .filter(|k| **k == pb::base_command::Type::Producer as i32)
        .count();
    assert_eq!(
        producer_count, 2,
        "the single pinned pool connection must have served BOTH producers; saw {producer_count}",
    );

    // Clean teardown of the reused pool entry.
    drop(p1);
    drop(p2);
    tokio::time::timeout(Duration::from_secs(5), client.close())
        .await
        .expect("Client::close must drain the reused proxy pool and return (no wedged teardown)");
}
