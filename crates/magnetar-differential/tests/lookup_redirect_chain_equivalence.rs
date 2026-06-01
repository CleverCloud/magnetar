// SPDX-License-Identifier: Apache-2.0

//! HIGH-4 (lookup multi-agent review) — tokio ↔ moonpool engine
//! event-stream equivalence on the LOOKUP redirect-chain path.
//!
//! Per ADR-0024 four-layer test rule. The HIGH-4 fix moved the
//! redirect-chain handling so that only the *terminal* outcome
//! (`Connect` / `Failed`) is delivered to the user-facing future —
//! intermediate `Redirected` outcomes ride the events queue for
//! diagnostics only. Both engines must observe the same terminal
//! outcome on the same `chain_origin` request-id when fed an identical
//! redirect chain.
//!
//! This test feeds the same scripted lookup-response bytes (two
//! Redirects then a Connect) into both engines'
//! [`magnetar_proto::Connection`] surface and asserts:
//!
//! 1. The user-facing outcome on the *original* request-id is `Connect` (terminal), not
//!    `Redirected` (intermediate).
//! 2. The broker URL surfaced on that terminal outcome is the *chain's tail*, not one of the
//!    intermediate redirect URLs.
//! 3. Both engines decode the chain to bit-identical `LookupSnapshot` values.
//! 4. A redirect-cap-exhausted chain surfaces the same synthetic `Failed { code: 0, message: "…
//!    redirect cap exceeded …" }` on both engines.

#![forbid(unsafe_code)]

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

/// Drive an engine's [`Connection`] through:
/// - the handshake
/// - a single user-facing LOOKUP (anchored on `chain_origin`)
/// - `redirects` redirect responses (each on the latest wire-level id)
/// - either a terminal Connect (when `terminate_in_connect = true`) or another redirect that pushes
///   the chain past the cap.
///
/// Returns the user-facing snapshot the engine would feed into
/// `Client::lookup_topic` — Connect for a happy chain, Failed for the
/// cap-exhausted chain.
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

    // Issue the LOOKUP — capture the user-facing anchor.
    let chain_origin = {
        let mut conn = shared.lock();
        conn.lookup("persistent://public/default/diff-chain-topic", false)
    };
    let mut current_wire_id = chain_origin;
    {
        let mut conn = shared.lock();
        if let Some(latest) = drain_latest_lookup_wire_id(&mut conn) {
            current_wire_id = latest;
        }
    }

    // Walk `redirects` redirect responses.
    for _ in 0..redirects {
        {
            let mut conn = shared.lock();
            conn.handle_bytes(
                start,
                &lookup_redirect_bytes(current_wire_id.0, redirect_url),
            )
            .expect("redirect response");
        }
        let mut conn = shared.lock();
        if let Some(latest) = drain_latest_lookup_wire_id(&mut conn) {
            current_wire_id = latest;
        }
    }

    // Terminate the chain with a Connect on the latest wire id, then
    // pull the user-facing outcome from the anchor.
    {
        let mut conn = shared.lock();
        conn.handle_bytes(
            start,
            &lookup_connect_bytes(current_wire_id.0, terminal_url),
        )
        .expect("terminal Connect");
    }

    let mut conn = shared.lock();
    let outcome = conn
        .take_outcome(magnetar_proto::PendingOpKey::Request(chain_origin))
        .expect("terminal outcome present at the chain anchor");
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

/// Drive a hostile redirect chain that pushes the state machine past
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops. The user-facing
/// outcome must be a synthetic `Failed` carrying the cap diagnostic —
/// proving F1's cap is end-to-end user-observable on both engines.
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

    let chain_origin = {
        let mut conn = shared.lock();
        conn.lookup("persistent://public/default/diff-chain-cap", false)
    };
    let mut current_wire_id = chain_origin;
    {
        let mut conn = shared.lock();
        if let Some(latest) = drain_latest_lookup_wire_id(&mut conn) {
            current_wire_id = latest;
        }
    }

    // Feed MAX_LOOKUP_REDIRECTS + 1 redirects — the last one triggers the cap.
    for _ in 0..=magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS {
        {
            let mut conn = shared.lock();
            conn.handle_bytes(
                start,
                &lookup_redirect_bytes(current_wire_id.0, "pulsar://hostile-redirect:6650"),
            )
            .expect("redirect response");
        }
        let mut conn = shared.lock();
        if let Some(latest) = drain_latest_lookup_wire_id(&mut conn) {
            current_wire_id = latest;
        }
    }

    let mut conn = shared.lock();
    let outcome = conn
        .take_outcome(magnetar_proto::PendingOpKey::Request(chain_origin))
        .expect("cap-exhausted Failed at the chain anchor");
    match outcome {
        OpOutcome::LookupResponse {
            outcome: LookupOutcome::Failed { code, message },
            ..
        } => LookupSnapshot::Failed { code, message },
        other => panic!("expected synthetic Failed at the cap, got {other:?}"),
    }
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
