// SPDX-License-Identifier: Apache-2.0

//! Moonpool sibling of `magnetar-runtime-tokio/tests/lookup_redirect_chain.rs`
//! — ADR-0039 redirect-target dialing: when a broker answers
//! `CommandLookupTopic` with `LookupType::Redirect`, the engine dials the
//! **redirect target broker** and re-issues the lookup *there*, instead of
//! re-asking the same (non-owner) broker on the bootstrap socket.
//!
//! Scenario (genuine two-broker topology):
//!
//! 1. Broker A (the bootstrap) redirects the first LOOKUP to broker B's real `host:port`.
//! 2. Broker B answers the re-issued LOOKUP with `Connect`-to-self, then serves the producer.
//! 3. The moonpool engine MUST dial broker B (a NEW connection via the per-broker pool), re-issue
//!    the LOOKUP there, and route the `CommandProducer` onto B — NOT onto A. The moonpool
//!    `ProxyConnectionPool` dial path (`crate::pool::get_or_open` → `spawn_task`) is what makes
//!    this work, so this also exercises the §3 moonpool-pool parity ADR-0039 flagged as follow-up.
//!
//! ADR-0024 1:1 parity with `magnetar-runtime-tokio/tests/lookup_redirect_chain.rs`.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]
// Two-broker topology: `_a` / `_b` suffixes (broker A vs broker B) are the
// clearest naming for the redirect source and target.
#![allow(clippy::similar_names)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The per-broker redirect dial requires the proxy pool, which is only built
/// on a supervised client.
fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    }
}

#[derive(Debug, Default, Clone)]
struct SessionRecord {
    frames: Vec<i32>,
    lookup_request_ids: Vec<u64>,
}

/// How a broker answers each `CommandLookupTopic` it receives.
#[derive(Clone)]
enum LookupBehaviour {
    /// Answer `Redirect` (advertising `redirect_url`) for the first `count`
    /// lookups, then resolve to self.
    RedirectTo { redirect_url: String, count: u8 },
    /// Always answer `Connect` resolving to self (no advertised URL → the
    /// engine routes onto this connection via `landed_on`).
    ConnectToSelf,
}

/// Bind a broker listener and return its bound `host:port` plus the listener so
/// the caller can build a self-referential redirect behaviour.
async fn bind_broker() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    (addr.to_string(), listener)
}

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
    let mut redirects_left = match &behaviour {
        LookupBehaviour::RedirectTo { count, .. } => *count,
        LookupBehaviour::ConnectToSelf => 0,
    };
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
                let (response_kind, broker_url) = match behaviour {
                    LookupBehaviour::RedirectTo { redirect_url, .. } if *redirects_left > 0 => {
                        *redirects_left -= 1;
                        (
                            pb::command_lookup_topic_response::LookupType::Redirect,
                            Some(redirect_url.to_owned()),
                        )
                    }
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
/// make the moonpool engine DIAL broker B (via the per-broker pool) and
/// re-issue the LOOKUP there, then route the producer onto B. This is the
/// moonpool sibling of the tokio dial test and exercises the §3 moonpool-pool
/// dial path ADR-0039 flagged as follow-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_dials_target_broker_and_re_lookups_there() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Broker B: Connect-to-self, serves the producer.
            let (hostport_b, listener_b) = bind_broker().await;
            let sessions_b = serve_broker(listener_b, LookupBehaviour::ConnectToSelf);
            // Broker A (bootstrap): redirects the first LOOKUP to B's real address.
            let (hostport_a, listener_a) = bind_broker().await;
            let sessions_a = serve_broker(
                listener_a,
                LookupBehaviour::RedirectTo {
                    // moonpool's lookup target is normalised to host:port; advertise B's
                    // bare authority so `direct_broker_authority` round-trips it.
                    redirect_url: format!("pulsar://{hostport_b}"),
                    count: 1,
                },
            );

            let engine = MoonpoolEngine::new(TokioProviders::new());
            // A redirect dial requires the per-broker pool, which is only built on a
            // supervised client (`connect_plain_supervised`).
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain_supervised(
                    &engine,
                    &hostport_a,
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
                    topic: "persistent://public/default/redirect-dial-producer".to_owned(),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            let snap_a = sessions_a.lock().clone();
            let snap_b = sessions_b.lock().clone();
            client.close().await;

            // Broker A saw exactly one LOOKUP (the redirect hop) and no producer.
            let session_a = snap_a.first().expect("broker A session exists");
            assert_eq!(
                session_a.lookup_request_ids.len(),
                1,
                "broker A must see exactly one LOOKUP (the redirect hop), got {:?}",
                session_a.lookup_request_ids
            );
            assert!(
                !session_a
                    .frames
                    .contains(&(pb::base_command::Type::Producer as i32)),
                "broker A must NOT receive CommandProducer (self-chase regression); frames {:?}",
                session_a.frames
            );

            // Broker B got its own dialed connection: CONNECT + re-issued LOOKUP + Producer.
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
                "broker B must have received CommandProducer (data ops routed to the redirect \
                 target); frames {:?}",
                session_b.frames
            );
        })
        .await;
}

/// A redirect chain that exceeds [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`]
/// must surface a `ClientError::Broker` carrying the cap diagnostic to the
/// user — proving the redirect cap is end-to-end user-observable on moonpool
/// across the engine-driven dial loop. Broker A redirects every LOOKUP to its
/// OWN address, so each hop dials back to the bootstrap (bootstrap-equality
/// reuse) and the cap trips within the bound. ADR-0024 1:1 parity with the
/// tokio engine's identically-named test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_redirect_chain_cap_surfaces_to_user() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (hostport_a, listener_a) = bind_broker().await;
            let _sessions_a = serve_broker(
                listener_a,
                LookupBehaviour::RedirectTo {
                    redirect_url: format!("pulsar://{hostport_a}"),
                    count: u8::MAX,
                },
            );

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain_supervised(
                    &engine,
                    &hostport_a,
                    supervised_config(),
                    None,
                    None,
                ),
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
