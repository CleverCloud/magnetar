// SPDX-License-Identifier: Apache-2.0

//! Integration coverage for ADR-0039 redirect-target dialing — when a broker
//! answers `CommandLookupTopic` with `LookupType::Redirect`, the engine dials
//! the **redirect target broker** and re-issues the lookup *there*, instead of
//! re-asking the same (non-owner) broker on the bootstrap socket.
//!
//! Scenario (genuine two-broker topology):
//!
//! 1. Broker A (the bootstrap) answers the first LOOKUP with `LookupType::Redirect`, advertising
//!    broker B's **real** `host:port` as the redirect target.
//! 2. Broker B answers the re-issued LOOKUP with `LookupType::Connect` resolving to itself, then
//!    serves the producer round-trip.
//! 3. The engine MUST dial broker B (a NEW physical connection), re-issue the LOOKUP there, and
//!    route the `CommandProducer` onto B's connection — NOT onto A. Before this change the proto
//!    layer re-encoded the LOOKUP on A's socket (re-asking the non-owner), which on a real
//!    multi-broker cluster loops to the redirect cap and fails.
//!
//! Sibling moonpool simulation coverage lives in
//! `crates/magnetar-runtime-moonpool/tests/lookup_redirect_chain.rs`
//! (ADR-0024 1:1 parity). The differential equivalence test lives in
//! `crates/magnetar-differential/tests/lookup_redirect_chain_equivalence.rs`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]
// Two-broker topology: `_a` / `_b` suffixes (broker A vs broker B) are the
// clearest naming for the redirect source and target.
#![allow(clippy::similar_names)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, OperationRetryConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
mod common;
use common::HANG_GUARD;

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    /// Frames seen, in the order they arrived (kind only).
    frames: Vec<i32>,
    /// Wire-level request-ids of every `CommandLookupTopic` seen on this
    /// session — used to confirm the state machine allocates a fresh id
    /// per redirect hop.
    lookup_request_ids: Vec<u64>,
}

/// How a broker answers each `CommandLookupTopic` it receives.
#[derive(Clone)]
enum LookupBehaviour {
    /// Answer `Redirect` (advertising `redirect_url`) for the first `count`
    /// lookups, then resolve to self.
    RedirectTo { redirect_url: String, count: u8 },
    /// Reject the first lookup with a retryable broker error, then redirect
    /// the retry to another physical broker.
    RetryableThenRedirect { redirect_url: String },
    /// Always answer `Connect` resolving to self (no advertised URL → the
    /// engine routes onto this connection).
    ConnectToSelf,
}

/// Bind a broker listener and return its bound URL plus a oneshot-free started
/// listener so the caller can build a self-referential redirect behaviour.
async fn bind_broker() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    (format!("pulsar://{addr}"), listener)
}

/// Drive an already-bound broker listener with the given behaviour.
fn serve_broker(
    listener: TcpListener,
    behaviour: LookupBehaviour,
) -> Arc<Mutex<Vec<SessionRecord>>> {
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
            let behaviour = behaviour.clone();
            tokio::spawn(async move {
                let _ = handle_session(stream, &sessions, session_idx, behaviour).await;
            });
        }
    });
    sessions
}

async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<SessionRecord>>>,
    session_idx: usize,
    behaviour: LookupBehaviour,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    // Per-session redirect budget; counts DOWN (only meaningful for RedirectTo).
    let mut redirects_left = match &behaviour {
        LookupBehaviour::RedirectTo { count, .. } => *count,
        LookupBehaviour::RetryableThenRedirect { .. } | LookupBehaviour::ConnectToSelf => 0,
    };
    let mut retryable_lookup_pending =
        matches!(behaviour, LookupBehaviour::RetryableThenRedirect { .. });
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
                &behaviour,
                &mut redirects_left,
                &mut retryable_lookup_pending,
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
    behaviour: &LookupBehaviour,
    redirects_left: &mut u8,
    retryable_lookup_pending: &mut bool,
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
                if *retryable_lookup_pending {
                    *retryable_lookup_pending = false;
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::LookupResponse as i32,
                        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                            broker_service_url: None,
                            broker_service_url_tls: None,
                            response: Some(
                                pb::command_lookup_topic_response::LookupType::Failed as i32,
                            ),
                            request_id: l.request_id,
                            authoritative: Some(true),
                            error: Some(pb::ServerError::ServiceNotReady as i32),
                            message: Some("owner is moving; retry lookup".to_owned()),
                            proxy_through_service_url: Some(false),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(out, &cmd);
                    return;
                }
                let (response_kind, broker_url) = match behaviour {
                    LookupBehaviour::RedirectTo { redirect_url, .. } if *redirects_left > 0 => {
                        *redirects_left -= 1;
                        (
                            pb::command_lookup_topic_response::LookupType::Redirect,
                            Some(redirect_url.to_owned()),
                        )
                    }
                    LookupBehaviour::RetryableThenRedirect { redirect_url } => (
                        pb::command_lookup_topic_response::LookupType::Redirect,
                        Some(redirect_url.to_owned()),
                    ),
                    // Redirect budget exhausted → resolve to self.
                    LookupBehaviour::RedirectTo { .. } | LookupBehaviour::ConnectToSelf => {
                        (pb::command_lookup_topic_response::LookupType::Connect, None)
                    }
                };
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: broker_url,
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

/// A redirect from broker A to broker B (a DIFFERENT physical address) must
/// make the engine DIAL broker B and re-issue the LOOKUP there, then route the
/// producer onto B. This is the core ADR-0039 redirect-dial behaviour: the
/// re-lookup lands on B's connection (a real dial to B), not on A.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_dials_target_broker_and_re_lookups_there() {
    // Broker B: answers LOOKUP with Connect-to-self, serves the producer.
    let (url_b, listener_b) = bind_broker().await;
    let sessions_b = serve_broker(listener_b, LookupBehaviour::ConnectToSelf);
    // Broker A (bootstrap): redirects the first LOOKUP to B's real address.
    let (url_a, listener_a) = bind_broker().await;
    let sessions_a = serve_broker(
        listener_a,
        LookupBehaviour::RedirectTo {
            redirect_url: url_b.clone(),
            count: 1,
        },
    );

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/redirect-dial-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    let snap_a = sessions_a.lock().clone();
    let snap_b = sessions_b.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    // Broker A saw exactly ONE LOOKUP (the first hop) and answered Redirect.
    let session_a = snap_a.first().expect("broker A session exists");
    assert_eq!(
        session_a.lookup_request_ids.len(),
        1,
        "broker A must see exactly one LOOKUP (the redirect hop), got {:?}",
        session_a.lookup_request_ids
    );
    // Broker A must NOT have served the producer — the re-lookup + producer
    // landed on B (this is the regression the dial fixes; the old self-chase
    // would have re-asked A and served the producer on A).
    assert!(
        !session_a
            .frames
            .contains(&(pb::base_command::Type::Producer as i32)),
        "broker A must NOT receive CommandProducer (self-chase regression); frames {:?}",
        session_a.frames
    );

    // Broker B received its own connection: a CONNECT, the re-issued LOOKUP,
    // and the CommandProducer. This proves the engine DIALED B and re-issued
    // the lookup there.
    assert_eq!(
        snap_b.len(),
        1,
        "broker B must have served exactly one connection (the dialed pool entry), got {}",
        snap_b.len()
    );
    let session_b = &snap_b[0];
    assert_eq!(
        session_b.lookup_request_ids.len(),
        1,
        "broker B must see the re-issued LOOKUP, got {:?}",
        session_b.lookup_request_ids
    );
    assert!(
        session_b
            .frames
            .contains(&(pb::base_command::Type::Connect as i32)),
        "broker B must have received a CommandConnect (a real dial to B); frames {:?}",
        session_b.frames
    );
    assert!(
        session_b
            .frames
            .contains(&(pb::base_command::Type::Producer as i32)),
        "broker B must have received CommandProducer (data ops routed to the redirect target); \
         frames {:?}",
        session_b.frames
    );

    // A retryable lookup error recovered before a redirect must remain the
    // operation-wide diagnostic if the target handshake later reaches the
    // deadline. Cancelling that unresolved pool build must abort its driver;
    // the silent target therefore observes EOF.
    let (silent_url, silent_listener) = bind_broker().await;
    let silent_accepted = Arc::new(tokio::sync::Notify::new());
    let silent_accepted_task = Arc::clone(&silent_accepted);
    let silent_eof = Arc::new(tokio::sync::Notify::new());
    let silent_eof_task = Arc::clone(&silent_eof);
    tokio::spawn(async move {
        let Ok((mut stream, _peer)) = silent_listener.accept().await else {
            return;
        };
        silent_accepted_task.notify_one();
        let mut buf = [0_u8; 1024];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    silent_eof_task.notify_one();
                    return;
                }
                Ok(_) => {}
            }
        }
    });
    let (retry_url, retry_listener) = bind_broker().await;
    let _retry_sessions = serve_broker(
        retry_listener,
        LookupBehaviour::RetryableThenRedirect {
            redirect_url: silent_url,
        },
    );
    let client = Client::connect(
        &retry_url,
        ConnectionConfig {
            operation_timeout: Duration::from_millis(500),
            ..ConnectionConfig::default()
        },
    )
    .await
    .expect("retry bootstrap connect")
    .with_operation_retry(OperationRetryConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        max_retries: Some(2),
    });
    let error = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/retry-redirect-deadline".to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("silent redirect target must reach the operation deadline");
    assert!(
        matches!(error, magnetar_runtime_tokio::ClientError::Broker { code, ref message }
            if code == pb::ServerError::ServiceNotReady as i32
                && message == "owner is moving; retry lookup"),
        "later target timeout must preserve the earlier broker error, got {error:?}"
    );
    tokio::time::timeout(Duration::from_secs(1), silent_accepted.notified())
        .await
        .expect("Tokio regression must reach the silent redirect target");
    client.close().await;
    tokio::time::timeout(Duration::from_secs(1), silent_eof.notified())
        .await
        .expect("cancelled Tokio pool dial must close the silent target socket");
}

/// A redirect chain that exceeds [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`]
/// must surface as a [`magnetar_runtime_tokio::ClientError::Broker`] with the
/// cap diagnostic — proving the redirect cap is end-to-end user-observable even
/// across the engine-driven dial loop. Broker A redirects every LOOKUP to ITS
/// OWN address, so each hop dials back to the bootstrap (bootstrap-equality
/// reuse) and the cap trips within the bound rather than looping forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_cap_surfaces_to_user() {
    // Broker A redirects every LOOKUP to its own address. Bind first so the
    // behaviour can advertise A's real URL as the redirect target.
    let (url_a, listener_a) = bind_broker().await;
    let _sessions_a = serve_broker(
        listener_a,
        LookupBehaviour::RedirectTo {
            redirect_url: url_a.clone(),
            count: u8::MAX,
        },
    );

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let err = tokio::time::timeout(
        HANG_GUARD,
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
