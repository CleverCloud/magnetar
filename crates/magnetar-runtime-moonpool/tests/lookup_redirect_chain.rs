// SPDX-License-Identifier: Apache-2.0

//! Moonpool sibling of `magnetar-runtime-tokio/tests/lookup_redirect_chain.rs`
//! — HIGH-4 (lookup multi-agent review): the engine must observe the
//! *terminal* outcome of a redirect chain, not the first hop's intermediate
//! `Redirected`.
//!
//! Scenario:
//!
//! 1. The client dials a single broker. The broker answers the first two LOOKUPs with
//!    `LookupType::Redirect` (with a redirect URL on each hop), and the third LOOKUP with
//!    `LookupType::Connect`.
//! 2. The runtime's `open_producer` MUST complete — the engine must see the terminal Connect and
//!    proceed with the producer round-trip. Before the HIGH-4 fix the engine would have seen the
//!    first-hop `LookupOutcome::Redirected` (proto layer published it on the user's request-id) and
//!    either folded it into a no-op (`broker_url = None`) — silently bypassing the broker URL — or
//!    returned it as the raw `LookupOutcome` (regressively surfacing an intermediate outcome to
//!    user code).
//!
//! The moonpool engine does not yet have multi-broker DIRECT routing (see
//! `lookup_direct_multi_broker.rs`), so we keep the terminal LOOKUP claiming the bootstrap
//! itself. The crux of the test is the **redirect chain settling at all**, not the
//! follow-up dial — that's what the differential equivalence test covers cross-engine.
//!
//! ADR-0024 1:1 parity with `magnetar-runtime-tokio/tests/lookup_redirect_chain.rs`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    frames: Vec<i32>,
    lookup_request_ids: Vec<u64>,
}

async fn spawn_chain_broker(
    redirects_before_connect: u8,
    redirect_url: String,
) -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let host_port = addr.to_string();
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
            let redirect_url = redirect_url.clone();
            tokio::spawn(async move {
                let _ = handle_session(
                    stream,
                    &sessions,
                    session_idx,
                    redirects_before_connect,
                    &redirect_url,
                )
                .await;
            });
        }
    });
    (host_port, sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    redirects_before_connect: u8,
    redirect_url: &str,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut redirects_left = redirects_before_connect;
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

            sessions.lock()[session_idx]
                .frames
                .push(frame.command.r#type);
            handle_frame(
                &frame,
                &mut out_buf,
                sessions,
                session_idx,
                &mut redirects_left,
                redirect_url,
            );
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

fn handle_frame(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    redirects_left: &mut u8,
    redirect_url: &str,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-redirect-chain-test".to_owned(),
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
                sessions.lock()[session_idx]
                    .lookup_request_ids
                    .push(l.request_id);
                let response_kind = if *redirects_left > 0 {
                    *redirects_left -= 1;
                    pb::command_lookup_topic_response::LookupType::Redirect
                } else {
                    pb::command_lookup_topic_response::LookupType::Connect
                };
                let broker_service_url =
                    if response_kind == pb::command_lookup_topic_response::LookupType::Connect {
                        // Terminal Connect: no broker URL → bootstrap routing.
                        // Moonpool falls back to the bootstrap connection
                        // (no per-broker pool yet for DIRECT).
                        None
                    } else {
                        Some(redirect_url.to_owned())
                    };
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url,
                        broker_service_url_tls: None,
                        response: Some(response_kind as i32),
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
                        producer_name: "redirect-chain-test".to_owned(),
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

/// A two-hop redirect chain terminating in Connect must unblock
/// `open_producer` and produce a working producer. Before HIGH-4 the
/// engine saw the first-hop `LookupOutcome::Redirected` on the user-facing
/// request-id; on moonpool it surfaced raw `Redirected` to the caller —
/// the producer never opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_resolves_to_terminal_broker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, sessions) =
                spawn_chain_broker(2, "pulsar://redirect-intermediate:6650".to_owned()).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let _producer = tokio::time::timeout(
                Duration::from_secs(5),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/redirect-chain-producer".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            let snap = sessions.lock().clone();
            client.close().await;

            let session = snap.first().expect("session exists");
            // Three LOOKUPs (Redirect, Redirect, Connect), each with a
            // distinct wire request-id. Same invariant as the tokio test.
            assert_eq!(
                session.lookup_request_ids.len(),
                3,
                "expected 3 LOOKUP frames (Redirect, Redirect, Connect), got {:?}",
                session.lookup_request_ids
            );
            let mut sorted_ids = session.lookup_request_ids.clone();
            sorted_ids.sort_unstable();
            sorted_ids.dedup();
            assert_eq!(
                sorted_ids.len(),
                3,
                "every redirect hop must allocate a fresh wire request-id, got {:?}",
                session.lookup_request_ids
            );

            // The producer round-trip landed on this session — proving the
            // engine got the terminal Connect outcome (not a Redirected
            // fold-into-error). On moonpool today the terminal Connect with
            // `broker_url = None` reuses the bootstrap, which is THIS
            // session, so we should observe a CommandProducer here.
            let saw_producer = session
                .frames
                .contains(&(pb::base_command::Type::Producer as i32));
            assert!(
                saw_producer,
                "broker must have received CommandProducer after the redirect chain settled; \
                 frames were {:?}",
                session.frames
            );
        })
        .await;
}

/// A redirect chain that exceeds [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`]
/// must surface a `ClientError::Broker` carrying the cap diagnostic to the
/// user — proving F1's redirect cap is end-to-end user-observable on
/// moonpool. ADR-0024 1:1 parity with the tokio engine's identically-named
/// test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_cap_surfaces_to_user() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, _sessions) =
                spawn_chain_broker(u8::MAX, "pulsar://hostile-redirect:6650".to_owned()).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let err = tokio::time::timeout(
                Duration::from_secs(5),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/redirect-chain-cap-producer".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect_err("open_producer must fail when the redirect chain exceeds the cap");
            client.close().await;

            let msg = format!("{err}");
            assert!(
                msg.contains("redirect cap exceeded"),
                "expected the cap diagnostic to be surfaced to the user, got: {msg}"
            );
        })
        .await;
}
