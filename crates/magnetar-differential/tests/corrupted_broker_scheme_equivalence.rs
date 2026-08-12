// SPDX-License-Identifier: Apache-2.0

//! Layer (d) of the ADR-0024 obligation for the corrupted-broker-scheme
//! hardening, in two parts: the PROXY-path half (issue #364,
//! `magnetar_runtime_moonpool::client::proxy_broker_authority` /
//! `direct_broker_authority` changed from `fn(&str) -> String` to
//! `fn(&str) -> Result<String, ClientError>`) and the DIRECT-path half
//! (`magnetar_runtime_tokio::client::parse_direct_broker_url`, tracked in
//! `docs/follow-ups.md` until this changeset closed it — see `git log` for
//! the implementation reference; the tracker renumbers on every close, so
//! no section number is cited here).
//! Both concern a corrupted / unrecognised URL scheme — a single-bit
//! corruption of the `pulsar` scheme word, `"ptlsar://..."`, the shape
//! moonpool-sim's bit-flip chaos actually produced.
//!
//! The file holds two tests at two different altitudes; read the scope
//! notes below before treating either as proof of something it is not.
//!
//! # 1. Proto-layer decode equivalence — [`tokio_and_moonpool_decode_the_same_corrupted_scheme_lookup_response`]
//!
//! **Honest scope note: this one is NOT a regression proof.** It asserts a
//! proto-level invariant that is unaffected by, and true both before AND
//! after, either client-layer fix — reverting either `client.rs` change
//! would NOT turn it red, because it never calls the client helpers at
//! all; it only drives `magnetar_proto::Connection` directly. What it
//! proves is narrower but still load-bearing: both engines' proto layers
//! decode the identical corrupted wire bytes identically, so whatever each
//! engine's `Client` layer subsequently does with that value is reacting
//! to the same decoded input, not a decode-level divergence. The
//! `magnetar-proto` side of the same invariant is pinned by
//! `crates/magnetar-proto/src/lookup.rs`'s
//! `connect_outcome_surfaces_a_corrupted_scheme_verbatim`.
//!
//! # 2. Client-layer rejection equivalence — [`tokio_and_moonpool_reject_the_same_corrupted_direct_broker_url`]
//!
//! This one IS a regression proof, and it is the reason the file grew a
//! client-level test after the `lookup_direct_multi_broker_equivalence.rs`
//! precedent had settled on proto-only assertions: on the DIRECT path the
//! two engines used to genuinely **disagree**, so there was a real
//! cross-engine divergence to pin. Moonpool's `direct_broker_authority`
//! rejected `"ptlsar://…"`; tokio's `parse_direct_broker_url` fell through
//! to a bare-`host:port` fallback that prefixed a SECOND scheme onto a
//! string already carrying one (`"pulsar://ptlsar://…"`), which
//! `url::Url::parse` accepts — silently yielding host `"ptlsar"` with the
//! WRONG default port, which the runtime then dialled. Verified red
//! pre-fix: the tokio half failed with
//! `Io(… "failed to lookup address information: Name or service not
//! known")`, i.e. the fabricated target really was dialled.
//!
//! Both engines are driven over real host sockets against one shared
//! in-process fake broker (moonpool via `TokioProviders`), so this asserts
//! equivalence of the actual `Client` surface, not of a proto snapshot.
//!
//! ADR-0103 assigns this execution to the isolated Tokio evidence domain.
//! That domain runs Tokio's unit/integration tests plus the differential suite
//! but reports only `magnetar-runtime-tokio` adapter source; its profiles can
//! never satisfy the separate Moonpool/shared report.
//!
//! Reported and failed: the gate landed advisory under ADR-0090, but
//! ADR-0092 flipped `SIM_COVERAGE_ENFORCES_UNCOVERED` to `true` and put the
//! check on every pull request, so an uncovered added line now prints with a
//! count and fails. The record-less-crate case above failed even while the
//! verdict was advisory.
//!
//! # Still deliberately NOT asserted
//!
//! The **PROXY** path remains an intentional cross-engine split, and no
//! test here claims otherwise: moonpool's `proxy_broker_authority` rejects
//! a corrupted scheme, while tokio's `preferred_broker_url` forwards the
//! raw string unchanged with a warning, relying on the downstream Pulsar
//! Proxy's `validateBrokerTarget()` to reject it. See
//! `crates/magnetar-runtime-moonpool/src/client.rs`'s
//! `proxy_broker_authority` doc comment for that verdict. The moonpool
//! PROXY-path red/green proof lives in
//! `crates/magnetar-runtime-moonpool/tests/proxy_multi_conn.rs`'s
//! `open_producer_through_proxy_rejects_corrupted_broker_scheme`.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, FrameError, LookupOutcome, OpOutcome,
    SupervisorConfig, decode_one, encode_command, pb,
};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A single-bit corruption of the `pulsar` scheme word, matching the
/// shape moonpool-sim's bit-flip chaos actually produced for issue #364.
const CORRUPTED_SCHEME_BROKER_URL: &str = "ptlsar://broker-sim.proxy.internal:6650";

#[derive(Debug, PartialEq, Eq, Clone)]
struct LookupSnapshot {
    /// `broker_service_url` from the response, verbatim — both engines must
    /// decode the identical corrupted string at the proto layer; only the
    /// CLIENT layer's post-processing of it diverges (moonpool only).
    broker_service_url: Option<String>,
    proxy_through_service_url: bool,
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff-corrupted-scheme".to_owned(),
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

fn proxy_lookup_response_bytes(request_id: u64, broker_url: &str) -> BytesMut {
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
            proxy_through_service_url: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandLookupTopicResponse");
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

/// Drive an engine's [`Connection`] through a handshake + a single
/// PROXY-routed LOOKUP round-trip carrying [`CORRUPTED_SCHEME_BROKER_URL`].
/// Returns the [`LookupSnapshot`] the proto layer surfaced — this is the
/// value BOTH engines' `Client` layers receive before any
/// engine-specific post-processing (the point where moonpool's and
/// tokio's handling diverges).
fn drive_proxy_lookup_with_corrupted_scheme<F>(make_shared: F) -> LookupSnapshot
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

    let request_id = {
        let mut conn = shared.lock();
        conn.lookup("persistent://public/default/diff-corrupted-scheme", false)
    };
    {
        let mut conn = shared.lock();
        let _ = conn.poll_transmit();
        conn.handle_bytes(
            start,
            &proxy_lookup_response_bytes(request_id.0, CORRUPTED_SCHEME_BROKER_URL),
        )
        .expect("lookup response");
    }

    let mut conn = shared.lock();
    while conn.poll_event().is_some() {}
    let outcome = conn
        .take_outcome(magnetar_proto::PendingOpKey::Request(request_id))
        .expect("lookup outcome present");
    match outcome {
        OpOutcome::LookupResponse {
            outcome:
                LookupOutcome::Connect {
                    broker_service_url,
                    proxy_through_service_url,
                    ..
                },
            ..
        } => LookupSnapshot {
            broker_service_url,
            proxy_through_service_url,
        },
        other => panic!("expected LookupResponse -> Connect, got {other:?}"),
    }
}

/// Both engines must agree on the raw `broker_service_url` the proto layer
/// surfaces even when it carries a corrupted scheme — the wire bytes are
/// identical on both engines and unaffected by moonpool's client-layer
/// hardening. This is the proto-level invariant load-bearing for issue
/// #364: whatever each engine's `Client` layer subsequently does with this
/// value (moonpool: reject; tokio: forward raw), they must be reacting to
/// the SAME decoded input, not a decode-level divergence.
#[test]
fn tokio_and_moonpool_decode_the_same_corrupted_scheme_lookup_response() {
    let tokio_snap = drive_proxy_lookup_with_corrupted_scheme(|cfg| {
        Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
            cfg,
        )))
    });
    let moonpool_snap = drive_proxy_lookup_with_corrupted_scheme(|cfg| {
        Arc::new(MoonpoolShared(
            magnetar_runtime_moonpool::ConnectionShared::new(cfg),
        ))
    });

    assert_eq!(
        tokio_snap, moonpool_snap,
        "tokio and moonpool engines decoded the corrupted-scheme lookup response \
         differently:\ntokio    = {tokio_snap:?}\nmoonpool = {moonpool_snap:?}",
    );
    assert_eq!(
        tokio_snap.broker_service_url.as_deref(),
        Some(CORRUPTED_SCHEME_BROKER_URL),
        "the corrupted scheme must be surfaced verbatim by the proto layer on both engines — \
         truncation only ever happened downstream, in moonpool's client-layer \
         proxy_broker_authority, which this differential test does not exercise (that's the \
         private-fn unit tests' job; see client.rs)"
    );
    assert!(
        tokio_snap.proxy_through_service_url,
        "PROXY routing must decode identically on both engines regardless of scheme corruption"
    );
}

// ---------------------------------------------------------------------------
// Client-layer rejection equivalence (DIRECT path).
// ---------------------------------------------------------------------------

/// How an engine's `Client` reacted to the corrupted DIRECT-path broker URL.
///
/// The distinction that matters is *where* the failure came from, not merely
/// that one occurred: both engines fail either way, so a bare `is_err()`
/// comparison would have called the pre-fix divergence "equivalent".
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum RejectionShape {
    /// The engine refused to derive a dial target from the corrupted scheme —
    /// `ClientError::Other` on both engines, raised before any socket to the
    /// fabricated host is opened.
    SchemeRejected,
    /// The engine derived *something* from the corrupted scheme and tried to
    /// dial it, failing at the transport. This is what tokio did before the
    /// `parse_direct_broker_url` fix. The two engines spell this arm
    /// differently — tokio has `ClientError::Io`, moonpool routes socket
    /// failures through `ClientError::Engine(EngineError)` — so the mapping is
    /// per-engine and the comparison happens on this normalised shape.
    TransportFailure,
    /// Neither — recorded verbatim so a mismatch reports what actually
    /// happened instead of collapsing to "not equal".
    Unexpected(&'static str),
}

/// Per-session log for the shared fake broker.
#[derive(Debug, Default, Clone)]
struct DirectSessionRecord {
    connect_proxy_to_broker_url: Option<String>,
    frames: Vec<i32>,
}

/// Spawn an in-process broker on `127.0.0.1:0` that answers every lookup with
/// `LookupOutcome::Connect { broker_service_url = CORRUPTED_SCHEME_BROKER_URL,
/// proxy_through_service_url = false }` — the DIRECT-routing shape both
/// engines feed through their broker-URL helper.
///
/// Returns `(host:port, pulsar:// url, session log)`. The two address forms
/// exist because the engines take different bootstrap shapes: tokio's
/// `Client::connect` wants the full URL, moonpool's
/// `Client::connect_plain_supervised` wants the bare authority.
async fn spawn_corrupted_direct_broker() -> (String, String, Arc<Mutex<Vec<DirectSessionRecord>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions: Arc<Mutex<Vec<DirectSessionRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let session_idx = {
                let mut s = sessions_for_task.lock();
                s.push(DirectSessionRecord::default());
                s.len() - 1
            };
            let sessions = sessions_for_task.clone();
            tokio::spawn(async move {
                let _ = handle_direct_session(stream, &sessions, session_idx).await;
            });
        }
    });
    (addr.to_string(), format!("pulsar://{addr}"), sessions)
}

async fn handle_direct_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<Mutex<Vec<DirectSessionRecord>>>,
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

            handle_direct_frame(&frame, &mut out_buf);
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

fn handle_direct_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-diff-corrupted-direct".to_owned(),
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
                        broker_service_url: Some(CORRUPTED_SCHEME_BROKER_URL.to_owned()),
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        // DIRECT routing — the path `parse_direct_broker_url` /
                        // `direct_broker_authority` serve.
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        // A producer must never reach us — if the corrupted target were
        // dialled it would go to the fabricated host, not here. Answering
        // anyway keeps the fake honest if the contract ever regresses, so the
        // frame-kind assertion is what catches it rather than a hang.
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "diff-corrupted-direct".to_owned(),
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

/// The supervisor must be wired so each engine builds its `ProxyConnectionPool`
/// — without a pool there is no per-broker dial path and the DIRECT branch
/// degrades to "reuse the bootstrap", never reaching the helper under test.
fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        operation_timeout: Duration::from_secs(10),
        ..ConnectionConfig::default()
    }
}

const DIFF_TOPIC: &str = "persistent://public/default/diff-corrupted-direct-scheme";

/// Drive `magnetar_runtime_tokio::Client` against the corrupted-scheme broker.
async fn tokio_direct_rejection() -> (RejectionShape, Vec<DirectSessionRecord>) {
    let (_host_port, url, sessions) = spawn_corrupted_direct_broker().await;

    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_tokio::Client::connect(&url, supervised_config()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect ok");

    let open_result = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: DIFF_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("tokio open_producer did not time out");

    let snapshot = sessions.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    let shape = match open_result {
        Ok(_) => RejectionShape::Unexpected("open_producer succeeded"),
        Err(magnetar_runtime_tokio::ClientError::Other(_)) => RejectionShape::SchemeRejected,
        Err(magnetar_runtime_tokio::ClientError::Io(_)) => RejectionShape::TransportFailure,
        Err(_) => RejectionShape::Unexpected("other ClientError variant"),
    };
    (shape, snapshot)
}

/// Drive `magnetar_runtime_moonpool::Client` against its own instance of the
/// same corrupted-scheme broker. `TokioProviders` runs the moonpool engine
/// over real host sockets, so both engines face identical wire conditions.
async fn moonpool_direct_rejection() -> (RejectionShape, Vec<DirectSessionRecord>) {
    let (host_port, _url, sessions) = spawn_corrupted_direct_broker().await;

    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_moonpool::Client::connect_plain_supervised(
            &engine,
            &host_port,
            supervised_config(),
            None,
            None,
        ),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect ok");

    let open_result = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: DIFF_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("moonpool open_producer did not time out");

    let snapshot = sessions.lock().clone();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    let shape = match open_result {
        Ok(_) => RejectionShape::Unexpected("open_producer succeeded"),
        Err(magnetar_runtime_moonpool::ClientError::Other(_)) => RejectionShape::SchemeRejected,
        Err(magnetar_runtime_moonpool::ClientError::Engine(_)) => RejectionShape::TransportFailure,
        Err(_) => RejectionShape::Unexpected("other ClientError variant"),
    };
    (shape, snapshot)
}

/// Both engines' `Client` layers must reject a corrupted-scheme DIRECT broker
/// URL the same way: refuse to derive a dial target, before any socket to the
/// fabricated host is opened.
///
/// This is the cross-engine parity assertion the DIRECT path could not carry
/// until `magnetar_runtime_tokio::client::parse_direct_broker_url` was fixed:
/// moonpool answered `SchemeRejected`, tokio answered `TransportFailure` (it
/// had derived host `"ptlsar"` with the wrong default port 6650 and dialled
/// it).
/// The session log pins the same conclusion structurally — one session, the
/// bootstrap, carrying the LOOKUP and no `CommandProducer`.
///
/// Runs in a `LocalSet`: `TokioProviders` is not `Send`, so the moonpool half
/// must stay on one thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokio_and_moonpool_reject_the_same_corrupted_direct_broker_url() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (tokio_shape, tokio_sessions) = tokio_direct_rejection().await;
            let (moonpool_shape, moonpool_sessions) = moonpool_direct_rejection().await;

            assert_eq!(
                tokio_shape, moonpool_shape,
                "the engines disagree on how to handle a corrupted-scheme DIRECT broker URL:\n\
                 tokio    = {tokio_shape:?}\n\
                 moonpool = {moonpool_shape:?}",
            );
            assert_eq!(
                tokio_shape,
                RejectionShape::SchemeRejected,
                "both engines must refuse to derive a dial target from '{CORRUPTED_SCHEME_BROKER_URL}' \
                 rather than dialling whatever they can salvage from it",
            );

            for (engine, snapshot) in [
                ("tokio", &tokio_sessions),
                ("moonpool", &moonpool_sessions),
            ] {
                assert_eq!(
                    snapshot.len(),
                    1,
                    "{engine}: only the bootstrap session may be opened, got {snapshot:?}",
                );
                let kinds: Vec<_> = snapshot[0]
                    .frames
                    .iter()
                    .filter_map(|k| pb::base_command::Type::try_from(*k).ok())
                    .collect();
                assert!(
                    kinds.contains(&pb::base_command::Type::Lookup),
                    "{engine}: bootstrap session must have seen the LOOKUP, got {kinds:?}",
                );
                assert!(
                    !kinds.contains(&pb::base_command::Type::Producer),
                    "{engine}: the PRODUCER must never be sent — the lookup target could not be \
                     resolved, got {kinds:?}",
                );
            }
        })
        .await;
}
