// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 redirect-target dialing — tokio ↔ moonpool engine equivalence on
//! the LOOKUP redirect-chain path.
//!
//! Per ADR-0024 four-layer test rule. A `Redirect` resolves to a driveable
//! `LookupOutcome::Redirected` carrying the redirect target + remaining hop
//! budget; the engine dials the target broker and re-issues the lookup there
//! (the proto layer no longer chases on the bootstrap socket). Both engines
//! must observe the same outcomes on the same request-ids when driven through
//! an identical redirect chain.
//!
//! Two complementary layers are asserted here:
//!
//! - **Proto-level chain walk** (in-memory `Connection`, no second physical broker): drive the
//!   redirect chain the engine way — each hop re-issues via `Connection::lookup_redirect` on a
//!   fresh request-id (simulating the dial) and threads the carried hop budget — and assert both
//!   engines produce bit-identical snapshots:
//!   1. The terminal outcome on the LAST hop's request-id is `Connect` (terminal), carrying the
//!      chain's *tail* URL, not an intermediate redirect URL. (PRESERVED tail-URL assertion.)
//!   2. A redirect-cap-exhausted chain surfaces the same synthetic `Failed { code: 0, message: "…
//!      redirect cap exceeded …" }` on both engines. (PRESERVED cap-exhaustion assertion.)
//! - **Engine-level two-broker dial** (real `Client`s over TCP): broker A redirects to broker B at
//!   a DIFFERENT physical address; both engines must DIAL broker B and land the re-lookup +
//!   producer there, not on A. (ADDED two-broker dial-to-B assertion.)

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]
// `DialObservation`'s `broker_*` field prefix and the `_a` / `_b` two-broker
// suffixes are the clearest naming for the redirect source/target topology.
#![allow(clippy::struct_field_names, clippy::similar_names)]

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, LookupOutcome, OpOutcome, RequestId, encode_command, pb,
};

#[derive(Debug, PartialEq, Eq, Clone)]
enum LookupSnapshot {
    Connect {
        broker_service_url: Option<String>,
        proxy_through_service_url: bool,
    },
    Failed {
        code: i32,
        message: String,
    },
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff-chain".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn lookup_redirect_bytes(request_id: u64, broker_url: &str) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: Some(broker_url.to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode redirect");
    buf
}

fn lookup_connect_bytes(request_id: u64, broker_url: &str) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: Some(broker_url.to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode terminal Connect");
    buf
}

trait SharedConn: Send + Sync {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection>;
}

struct TokioShared(Arc<magnetar_runtime_tokio::ConnectionShared>);
struct MoonpoolShared(Arc<magnetar_runtime_moonpool::ConnectionShared>);

impl SharedConn for TokioShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.0.inner.lock()
    }
}
impl SharedConn for MoonpoolShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.0.inner.lock()
    }
}

/// Decode every complete `CommandLookupTopic` currently sitting in the
/// proto's outbound buffer and return the latest wire-level request id —
/// i.e. the id the broker correlates its next response against. Each
/// redirect hop allocates a fresh wire id, so we re-read this after every
/// hop's `handle_bytes`.
fn drain_latest_lookup_wire_id(conn: &mut Connection) -> Option<RequestId> {
    let bytes = conn.poll_transmit();
    let mut cursor: bytes::Bytes = bytes;
    let mut latest = None;
    while !cursor.is_empty() {
        let frame = magnetar_proto::decode_one(&mut cursor).expect("decode outbound");
        if let Ok(pb::base_command::Type::Lookup) =
            pb::base_command::Type::try_from(frame.command.r#type)
        {
            if let Some(l) = frame.command.lookup_topic {
                latest = Some(RequestId(l.request_id));
            }
        }
    }
    latest
}

/// Drive an engine's [`Connection`] through a redirect chain **the engine
/// way**: handshake, a user LOOKUP, then `redirects` redirect responses where
/// each `Redirected` outcome is consumed and the lookup re-issued via
/// [`magnetar_proto::Connection::lookup_redirect`] on a fresh request-id
/// (simulating the per-broker dial), threading the carried hop budget. The
/// chain terminates in a `Connect` carrying `terminal_url`.
///
/// Returns the snapshot the engine would feed into its `LookupTarget`
/// decision: `Connect` carrying the chain's tail URL for a happy chain. The
/// terminal outcome is read from the LAST hop's request-id (each hop runs on
/// its own connection / request-id now — there is no cross-hop anchor).
fn drive_redirect_chain<F>(
    make_shared: F,
    redirects: u8,
    redirect_url: &str,
    terminal_url: &str,
) -> LookupSnapshot
where
    F: FnOnce(ConnectionConfig) -> Arc<dyn SharedConn>,
{
    let shared = make_shared(ConnectionConfig::default());
    let start = Instant::now();

    {
        let mut conn = shared.lock();
        conn.begin_handshake().expect("begin_handshake");
        let _ = conn.poll_transmit();
        conn.handle_bytes(start, &handshake_response_bytes())
            .expect("handshake");
    }

    // Issue the initial user LOOKUP.
    let mut current = {
        let mut conn = shared.lock();
        let rid = conn.lookup("persistent://public/default/diff-chain-topic", false);
        let _ = drain_latest_lookup_wire_id(&mut conn);
        rid
    };

    // Walk `redirects` redirect responses, re-issuing each via `lookup_redirect`.
    for _ in 0..redirects {
        {
            let mut conn = shared.lock();
            conn.handle_bytes(start, &lookup_redirect_bytes(current.0, redirect_url))
                .expect("redirect response");
        }
        let hops = {
            let mut conn = shared.lock();
            let _ = drain_latest_lookup_wire_id(&mut conn);
            match conn.take_outcome(magnetar_proto::PendingOpKey::Request(current)) {
                Some(OpOutcome::LookupResponse {
                    outcome: LookupOutcome::Redirected { hops_remaining, .. },
                    ..
                }) => hops_remaining,
                other => panic!("expected driveable Redirected, got {other:?}"),
            }
        };
        let mut conn = shared.lock();
        current = conn.lookup_redirect("persistent://public/default/diff-chain-topic", true, hops);
        let _ = drain_latest_lookup_wire_id(&mut conn);
    }

    // Terminate the chain with a Connect on the latest request-id.
    {
        let mut conn = shared.lock();
        conn.handle_bytes(start, &lookup_connect_bytes(current.0, terminal_url))
            .expect("terminal Connect");
    }

    let mut conn = shared.lock();
    let outcome = conn
        .take_outcome(magnetar_proto::PendingOpKey::Request(current))
        .expect("terminal outcome present at the last hop's request-id");
    match outcome {
        OpOutcome::LookupResponse {
            outcome:
                LookupOutcome::Connect {
                    broker_service_url,
                    proxy_through_service_url,
                    ..
                },
            ..
        } => LookupSnapshot::Connect {
            broker_service_url,
            proxy_through_service_url,
        },
        OpOutcome::LookupResponse {
            outcome: LookupOutcome::Failed { code, message },
            ..
        } => LookupSnapshot::Failed { code, message },
        other => panic!("expected terminal Connect or Failed, got {other:?}"),
    }
}

/// Drive a hostile redirect chain that pushes the engine-driven dial loop past
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops. The outcome on the
/// final hop must be a synthetic `Failed` carrying the cap diagnostic —
/// proving the redirect cap is end-to-end user-observable on both engines.
/// Each hop is re-issued via `lookup_redirect` (simulating the dial), threading
/// the carried budget, so the proto floor trips within the bound.
fn drive_cap_exhausted_chain<F>(make_shared: F) -> LookupSnapshot
where
    F: FnOnce(ConnectionConfig) -> Arc<dyn SharedConn>,
{
    let shared = make_shared(ConnectionConfig::default());
    let start = Instant::now();

    {
        let mut conn = shared.lock();
        conn.begin_handshake().expect("begin_handshake");
        let _ = conn.poll_transmit();
        conn.handle_bytes(start, &handshake_response_bytes())
            .expect("handshake");
    }

    let mut current = {
        let mut conn = shared.lock();
        let rid = conn.lookup("persistent://public/default/diff-chain-cap", false);
        let _ = drain_latest_lookup_wire_id(&mut conn);
        rid
    };

    // Feed up to MAX_LOOKUP_REDIRECTS + 1 redirects — the proto floor trips and
    // surfaces the synthetic Failed once the threaded budget reaches zero.
    for _ in 0..=magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS {
        {
            let mut conn = shared.lock();
            conn.handle_bytes(
                start,
                &lookup_redirect_bytes(current.0, "pulsar://hostile-redirect:6650"),
            )
            .expect("redirect response");
        }
        let next = {
            let mut conn = shared.lock();
            let _ = drain_latest_lookup_wire_id(&mut conn);
            match conn.take_outcome(magnetar_proto::PendingOpKey::Request(current)) {
                Some(OpOutcome::LookupResponse {
                    outcome: LookupOutcome::Redirected { hops_remaining, .. },
                    ..
                }) => Some(hops_remaining),
                Some(OpOutcome::LookupResponse {
                    outcome: LookupOutcome::Failed { code, message },
                    ..
                }) => return LookupSnapshot::Failed { code, message },
                other => panic!("unexpected outcome during cap walk: {other:?}"),
            }
        };
        let hops = next.expect("redirect must carry a budget until the floor trips");
        let mut conn = shared.lock();
        current = conn.lookup_redirect("persistent://public/default/diff-chain-cap", true, hops);
        let _ = drain_latest_lookup_wire_id(&mut conn);
    }

    panic!("cap-exhausted chain did not surface a synthetic Failed within the bound");
}

/// HIGH-4 + HIGH-1: both engines must surface the *terminal* broker URL
/// from a redirect chain, not the first-hop intermediate. Before the fix
/// the proto layer published a `LookupOutcome::Redirected` outcome on the
/// user-facing request-id, the tokio engine folded it into
/// `Direct { broker_url: None }` (silent), the moonpool engine surfaced it
/// raw (ADR-0024 parity violation). After the fix the user gets the
/// terminal Connect with the chain's tail URL — identical on both engines.
#[test]
fn tokio_and_moonpool_observe_the_same_terminal_outcome_after_redirect_chain() {
    let redirect_url = "pulsar://redirect-intermediate.example:6650";
    let terminal_url = "pulsar://terminal.example:6650";

    let tokio_snap = drive_redirect_chain(
        |cfg| {
            Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
                cfg,
            )))
        },
        2,
        redirect_url,
        terminal_url,
    );
    let moonpool_snap = drive_redirect_chain(
        |cfg| {
            Arc::new(MoonpoolShared(
                magnetar_runtime_moonpool::ConnectionShared::new(cfg),
            ))
        },
        2,
        redirect_url,
        terminal_url,
    );

    assert_eq!(
        tokio_snap, moonpool_snap,
        "tokio and moonpool engines surfaced different terminal outcomes on the same chain:\n\
         tokio    = {tokio_snap:?}\n\
         moonpool = {moonpool_snap:?}",
    );
    match &tokio_snap {
        LookupSnapshot::Connect {
            broker_service_url,
            proxy_through_service_url,
        } => {
            assert_eq!(
                broker_service_url.as_deref(),
                Some(terminal_url),
                "the user must see the TERMINAL broker URL, not the first-hop redirect"
            );
            assert!(
                !proxy_through_service_url,
                "DIRECT path implies proxy_through_service_url = false"
            );
        }
        failed @ LookupSnapshot::Failed { .. } => {
            panic!("expected terminal Connect, got {failed:?}")
        }
    }
}

/// HIGH-4 + HIGH-2: a cap-exhausted redirect chain must surface the same
/// synthetic Failed outcome on both engines — the cap diagnostic message
/// is part of the public contract because runtime callers grep it to map
/// the error to a retry decision.
#[test]
fn tokio_and_moonpool_observe_the_same_cap_exceeded_failed() {
    let tokio_snap = drive_cap_exhausted_chain(|cfg| {
        Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
            cfg,
        )))
    });
    let moonpool_snap = drive_cap_exhausted_chain(|cfg| {
        Arc::new(MoonpoolShared(
            magnetar_runtime_moonpool::ConnectionShared::new(cfg),
        ))
    });

    assert_eq!(
        tokio_snap, moonpool_snap,
        "tokio and moonpool diverged on the cap-exhausted chain outcome:\n\
         tokio    = {tokio_snap:?}\n\
         moonpool = {moonpool_snap:?}",
    );
    match &tokio_snap {
        LookupSnapshot::Failed { code, message } => {
            assert_eq!(*code, 0, "synthetic cap-exceeded Failed uses code 0");
            assert!(
                message.contains("redirect cap exceeded"),
                "expected the cap diagnostic, got: {message}"
            );
        }
        connect @ LookupSnapshot::Connect { .. } => {
            panic!("expected synthetic Failed at the cap, got {connect:?}")
        }
    }
}

// ---------------------------------------------------------------------------
// Engine-level two-broker dial parity (ADDED).
//
// Broker A redirects the LOOKUP to broker B at a DIFFERENT physical address.
// Both engines must DIAL broker B and land the re-lookup + producer there, not
// on A. This is the parity the proto-level walk above cannot exercise (it has
// only one in-memory connection). Run via real `Client`s over TCP.
// ---------------------------------------------------------------------------

use magnetar_differential::HANG_GUARD;
use magnetar_proto::{CreateProducerRequest, FrameError, SupervisorConfig, decode_one};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Default, Clone)]
struct DialSession {
    frames: Vec<i32>,
}

#[derive(Clone)]
enum DialBehaviour {
    RedirectOnceTo(String),
    ConnectToSelf,
}

/// What each engine observed on broker B: whether B was dialed (received a
/// CONNECT) and served the producer, and that A never saw the producer.
#[derive(Debug, PartialEq, Eq)]
struct DialObservation {
    broker_b_session_count: usize,
    broker_b_dialed: bool,
    broker_b_saw_producer: bool,
    broker_a_free_of_producer: bool,
}

async fn spawn_dial_broker(behaviour: DialBehaviour) -> (String, Arc<Mutex<Vec<DialSession>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<DialSession>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let idx = {
                let mut s = sessions_for_task.lock();
                s.push(DialSession::default());
                s.len() - 1
            };
            let sessions = sessions_for_task.clone();
            let behaviour = behaviour.clone();
            tokio::spawn(async move {
                let _ = drive_dial_session(stream, &sessions, idx, behaviour).await;
            });
        }
    });
    (addr.to_string(), sessions)
}

async fn drive_dial_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<DialSession>>>,
    idx: usize,
    behaviour: DialBehaviour,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut redirected = false;
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
            sessions.lock()[idx].frames.push(frame.command.r#type);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "diff-redirect-dial".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out_buf, &cmd);
                }
                pb::base_command::Type::Ping => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Pong as i32,
                        pong: Some(pb::CommandPong {}),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out_buf, &cmd);
                }
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        let (response_kind, broker_url) = match &behaviour {
                            DialBehaviour::RedirectOnceTo(url) if !redirected => {
                                redirected = true;
                                (
                                    pb::command_lookup_topic_response::LookupType::Redirect,
                                    Some(url.clone()),
                                )
                            }
                            _ => (pb::command_lookup_topic_response::LookupType::Connect, None),
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
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::ProducerSuccess as i32,
                            producer_success: Some(pb::CommandProducerSuccess {
                                request_id: p.request_id,
                                producer_name: "diff-redirect-dial".to_owned(),
                                last_sequence_id: Some(-1),
                                schema_version: None,
                                topic_epoch: Some(0),
                                producer_ready: Some(true),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                _ => {}
            }
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

fn observe(
    sessions_a: &Arc<Mutex<Vec<DialSession>>>,
    sessions_b: &Arc<Mutex<Vec<DialSession>>>,
) -> DialObservation {
    let producer = pb::base_command::Type::Producer as i32;
    let connect = pb::base_command::Type::Connect as i32;
    let snap_a = sessions_a.lock().clone();
    let snap_b = sessions_b.lock().clone();
    let a_free = snap_a.iter().all(|s| !s.frames.contains(&producer));
    let b0 = snap_b.first().cloned().unwrap_or_default();
    DialObservation {
        broker_b_session_count: snap_b.len(),
        broker_b_dialed: b0.frames.contains(&connect),
        broker_b_saw_producer: b0.frames.contains(&producer),
        broker_a_free_of_producer: a_free,
    }
}

async fn run_tokio_dial() -> DialObservation {
    use magnetar_runtime_tokio::Client;
    // Broker B: Connect-to-self. Broker A: redirect once to B's real address.
    let (addr_b, sessions_b) = spawn_dial_broker(DialBehaviour::ConnectToSelf).await;
    let (addr_a, sessions_a) =
        spawn_dial_broker(DialBehaviour::RedirectOnceTo(format!("pulsar://{addr_b}"))).await;
    let url_a = format!("pulsar://{addr_a}");
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url_a, ConnectionConfig::default()),
    )
    .await
    .expect("tokio connect")
    .expect("tokio connect ok");
    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/diff-redirect-dial-tokio".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("tokio open_producer")
    .expect("tokio open_producer ok");
    let obs = observe(&sessions_a, &sessions_b);
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    obs
}

async fn run_moonpool_dial() -> DialObservation {
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
    let (addr_b, sessions_b) = spawn_dial_broker(DialBehaviour::ConnectToSelf).await;
    let (addr_a, sessions_a) =
        spawn_dial_broker(DialBehaviour::RedirectOnceTo(format!("pulsar://{addr_b}"))).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr_a, cfg, None, None),
    )
    .await
    .expect("moonpool connect")
    .expect("moonpool connect ok");
    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/diff-redirect-dial-moonpool".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("moonpool open_producer")
    .expect("moonpool open_producer ok");
    let obs = observe(&sessions_a, &sessions_b);
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    obs
}

/// Both engines, handed a redirect from broker A to broker B, must DIAL B and
/// land the re-lookup + producer there — identical observation on B, with A
/// never serving the producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokio_and_moonpool_both_dial_the_redirect_target() {
    let tokio_obs = run_tokio_dial().await;
    let moonpool_obs = run_moonpool_dial().await;

    assert_eq!(
        tokio_obs, moonpool_obs,
        "tokio and moonpool diverged on the redirect-dial observation:\n\
         tokio    = {tokio_obs:?}\n\
         moonpool = {moonpool_obs:?}",
    );
    // And the shared contract both must satisfy: B was dialed + served the
    // producer; A never served the producer.
    assert_eq!(tokio_obs.broker_b_session_count, 1);
    assert!(tokio_obs.broker_b_dialed, "broker B must receive a CONNECT");
    assert!(
        tokio_obs.broker_b_saw_producer,
        "broker B must serve the producer (data ops routed to the redirect target)"
    );
    assert!(
        tokio_obs.broker_a_free_of_producer,
        "broker A must NOT serve the producer (self-chase regression)"
    );
}
