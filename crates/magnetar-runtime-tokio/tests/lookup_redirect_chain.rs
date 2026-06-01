// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for HIGH-4 (lookup multi-agent review) — the engine
//! must observe the *terminal* outcome of a redirect chain, not the first
//! hop's intermediate `Redirected`.
//!
//! Scenario:
//!
//! 1. The client dials a single broker. The broker answers the first two LOOKUPs with
//!    `LookupType::Redirect` (advertising a redirect URL on each hop), and the third LOOKUP with
//!    `LookupType::Connect { broker_service_url = TERMINAL, proxy_through_service_url = false }`.
//! 2. The runtime's `open_producer` MUST see the TERMINAL broker URL — not one of the intermediate
//!    redirect URLs. Before the HIGH-4 fix the proto layer surfaced the first-hop
//!    `LookupOutcome::Redirected` on the user-facing request-id, which the engine then folded into
//!    `LookupTarget::Direct { broker_url: None }` (the bootstrap fallback). After the fix the
//!    engine observes the terminal `Connect` and routes accordingly.
//!
//! Sibling moonpool simulation coverage lives in
//! `crates/magnetar-runtime-moonpool/tests/lookup_redirect_chain.rs`
//! (ADR-0024 1:1 parity). The differential equivalence test lives in
//! `crates/magnetar-differential/tests/lookup_redirect_chain_equivalence.rs`.

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

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    /// Frames seen, in the order they arrived (kind only).
    frames: Vec<i32>,
    /// Wire-level request-ids of every `CommandLookupTopic` seen on this
    /// session — used to confirm the state machine allocates a fresh id
    /// per redirect hop.
    lookup_request_ids: Vec<u64>,
}

/// Spawn the chain broker. It answers the first `redirects_before_connect`
/// LOOKUPs with `LookupType::Redirect` (advertising `redirect_url` on each)
/// and then answers the next LOOKUP with `LookupType::Connect` resolving to
/// `terminal_url`. Subsequent LOOKUPs (e.g. ones triggered by reconnects)
/// keep answering Connect. The broker also serves the producer round-trip.
async fn spawn_chain_broker(
    redirects_before_connect: u8,
    redirect_url: String,
    terminal_url: String,
) -> (String, Arc<Mutex<Vec<SessionRecord>>>) {
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
            let redirect_url = redirect_url.clone();
            let terminal_url = terminal_url.clone();
            tokio::spawn(async move {
                let _ = handle_session(
                    stream,
                    &sessions,
                    session_idx,
                    redirects_before_connect,
                    &redirect_url,
                    &terminal_url,
                )
                .await;
            });
        }
    });
    (url, sessions)
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    redirects_before_connect: u8,
    redirect_url: &str,
    terminal_url: &str,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    // Per-session redirect budget; counts DOWN.
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
                terminal_url,
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
    terminal_url: &str,
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
                // While we still have redirect budget, answer Redirect.
                // Once exhausted, answer with the terminal Connect.
                let (response_kind, broker_url) = if *redirects_left > 0 {
                    *redirects_left -= 1;
                    (
                        pb::command_lookup_topic_response::LookupType::Redirect,
                        redirect_url.to_owned(),
                    )
                } else {
                    (
                        pb::command_lookup_topic_response::LookupType::Connect,
                        terminal_url.to_owned(),
                    )
                };
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: Some(broker_url),
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

/// A two-hop redirect chain terminating in Connect resolves to the
/// TERMINAL broker's URL — proving the engine observes the chain's tail,
/// not the first-hop Redirected. Without the HIGH-4 fix the engine
/// folded the first hop into `Direct { broker_url: None }` and routed
/// the producer onto the bootstrap connection (the original
/// engine-asymmetry bug).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_resolves_to_terminal_broker() {
    // 1. Bring up the *terminal* broker. We point the redirect chain at it.
    let (url_terminal, sessions_terminal) =
        spawn_chain_broker(0, String::new(), String::new()).await;
    // 2. Bring up the bootstrap broker — does two redirects, then Connect to terminal.
    let (url_bootstrap, sessions_bootstrap) = spawn_chain_broker(
        2,
        "pulsar://redirect-intermediate:6650".to_owned(),
        url_terminal.clone(),
    )
    .await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url_bootstrap, ConnectionConfig::default()),
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

    let snap_bootstrap = sessions_bootstrap.lock().clone();
    let snap_terminal = sessions_terminal.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // The bootstrap broker saw THREE LOOKUPs (two redirected, one Connect).
    // Each redirect hop must allocate a fresh wire request-id, so the three
    // ids are all distinct — the state machine doesn't reuse the user-facing
    // anchor for the on-wire correlator.
    let bootstrap_session = snap_bootstrap.first().expect("bootstrap session exists");
    assert_eq!(
        bootstrap_session.lookup_request_ids.len(),
        3,
        "expected 3 LOOKUP frames (Redirect, Redirect, Connect) on the bootstrap, got {:?}",
        bootstrap_session.lookup_request_ids
    );
    let mut sorted_ids = bootstrap_session.lookup_request_ids.clone();
    sorted_ids.sort_unstable();
    sorted_ids.dedup();
    assert_eq!(
        sorted_ids.len(),
        3,
        "every redirect hop must allocate a fresh wire request-id, got {:?}",
        bootstrap_session.lookup_request_ids
    );

    // The TERMINAL broker received the CommandProducer — proving the engine
    // routed the data ops to the chain's tail, not to the bootstrap or to
    // an intermediate redirect URL. The whole point of HIGH-4 + HIGH-1.
    assert_eq!(
        snap_terminal.len(),
        1,
        "terminal broker must have served exactly one connection (the pinned pool entry), got {} \
         sessions",
        snap_terminal.len()
    );
    let terminal_session = &snap_terminal[0];
    let saw_producer = terminal_session
        .frames
        .contains(&(pb::base_command::Type::Producer as i32));
    assert!(
        saw_producer,
        "terminal broker must have received CommandProducer, but session frames were {:?}",
        terminal_session.frames
    );
    // And it must NOT have shown up on the bootstrap (the regressive
    // engine-fold bug would have sent CommandProducer here).
    let bootstrap_saw_producer = bootstrap_session
        .frames
        .contains(&(pb::base_command::Type::Producer as i32));
    assert!(
        !bootstrap_saw_producer,
        "bootstrap broker must NOT have received CommandProducer (engine fold bug); \
         session frames were {:?}",
        bootstrap_session.frames
    );
}

/// A redirect chain that exceeds [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`]
/// must surface as a [`magnetar_runtime_tokio::ClientError::Broker`] with
/// the cap diagnostic — proving F1's redirect cap is end-to-end
/// user-observable. Before the HIGH-4 fix the engine would have folded the
/// first hop into `Direct { broker_url: None }`, hidden the cap behind a
/// warning log, and routed onto the bootstrap connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_cap_surfaces_to_user() {
    // Bootstrap broker answers every LOOKUP with a Redirect — never resolves.
    let redirect_url = "pulsar://hostile-redirect:6650".to_owned();
    let (url_bootstrap, _sessions_bootstrap) =
        spawn_chain_broker(u8::MAX, redirect_url, String::new()).await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url_bootstrap, ConnectionConfig::default()),
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

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    let msg = format!("{err}");
    assert!(
        msg.contains("redirect cap exceeded"),
        "expected the cap diagnostic to be surfaced to the user, got: {msg}"
    );
}
