// SPDX-License-Identifier: Apache-2.0

//! Handshake-error capture — tokio engine, real loopback broker.
//!
//! Mirror of the moonpool deterministic-simulation fixture
//! `crates/magnetar-runtime-moonpool/tests/handshake_error_capture.
//! rs::connect_plain_surfaces_handshake_failure_reason_from_broker_command_error`.
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024.
//!
//! Pins the new `magnetar_proto::Connection::handshake_failure_reason`
//! enrichment: when a broker rejects `CommandConnect` (or
//! `CommandAuthChallenge`) with a `CommandError` and then tears the socket
//! down, the user-facing connect future must surface
//! `ClientError::Other("handshake failed: …")` carrying the broker's
//! `ServerError` name + verbatim message — instead of the opaque
//! `"handshake failed"` string the previous code produced for any
//! mid-handshake drop.
//!
//! Wall-clock timing of the drop is intentionally NOT asserted (it is
//! flaky over loopback); the error envelope is the authoritative proof
//! the proto-layer capture survived the supervisor's failure path.

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Broker-side message — a SHORT message must round-trip verbatim into the
/// engine-surfaced `ClientError::Other("handshake failed: …")` payload.
const BROKER_MESSAGE: &str = "token expired";

/// Proto-side ceiling for broker-supplied strings (ADR-0054 §3 / ADR-0062).
/// Kept in sync with `magnetar_proto::log_fields::MAX_BROKER_STR` (a private
/// const); a broker message above this is truncated at the capture site.
const MAX_BROKER_STR: usize = 256;

/// Spawn a fake broker on `127.0.0.1:0` that reads the inbound
/// `CommandConnect`, replies with `CommandError(AuthenticationError,
/// <message>)`, and drops the socket. Returns the `pulsar://...` URL.
async fn spawn_reject_handshake_broker(message: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let Ok((stream, _peer)) = listener.accept().await else {
            return;
        };
        let _ = handle_reject_handshake_session(stream, message).await;
    });
    format!("pulsar://{addr}")
}

/// Per-session script: read until we see a `CommandConnect`, then send
/// back a `CommandError(AuthenticationError, <message>)` and drop
/// the socket. Mirrors the moonpool `handle_reject_handshake_session`
/// helper.
async fn handle_reject_handshake_session(
    mut stream: tokio::net::TcpStream,
    message: String,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut saw_connect = false;
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
            if pb::base_command::Type::try_from(frame.command.r#type)
                == Ok(pb::base_command::Type::Connect)
            {
                saw_connect = true;
            }
        }

        if saw_connect {
            // request_id = 0 — the broker does not correlate
            // mid-handshake CONNECT failures with any pending request,
            // and the proto layer is expected to capture the message
            // regardless.
            let err = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: 0,
                    error: pb::ServerError::AuthenticationError as i32,
                    message: message.clone(),
                }),
                ..Default::default()
            };
            let mut out = BytesMut::new();
            let _ = encode_command(&mut out, &err);
            stream.write_all(&out).await?;
            stream.flush().await?;
            // Drop the socket — return from the task, dropping `stream`.
            return Ok(());
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

/// When the broker rejects the handshake with `CommandError` mid-CONNECT
/// and tears the socket down, the tokio engine's connect future must
/// surface a `ClientError::Other` whose message contains the
/// `"handshake failed:"` envelope prefix, the `ServerError` variant
/// name, AND the verbatim broker message. Without the proto-layer
/// capture, the supervisor's drop-driven failure path would have
/// produced the legacy opaque `"handshake failed"` string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_surfaces_handshake_failure_reason_from_broker_command_error() {
    let url = spawn_reject_handshake_broker(BROKER_MESSAGE.to_owned()).await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out");

    let err = result.expect_err(
        "Client::connect must fail when the broker rejects CommandConnect with CommandError",
    );
    let msg = match err {
        ClientError::Other(m) => m,
        other => panic!("expected ClientError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("handshake failed:"),
        "engine error must carry the enriched \"handshake failed: …\" envelope (got: {msg})",
    );
    assert!(
        msg.contains("AuthenticationError"),
        "engine error must mention the broker's ServerError variant (got: {msg})",
    );
    // A SHORT broker message is below the budget, so it round-trips verbatim —
    // the bound is a ceiling, not a fixed-width truncation (ADR-0062).
    assert!(
        msg.contains(BROKER_MESSAGE),
        "engine error must carry the verbatim broker message \
         \"{BROKER_MESSAGE}\" (got: {msg})",
    );
}

/// ADR-0062: a hostile broker returning an arbitrarily long mid-handshake
/// `CommandError.message` must NOT inflate the surfaced engine error
/// unboundedly. The proto capture site truncates the broker text to
/// `MAX_BROKER_STR` bytes at a char boundary, and every downstream sink —
/// here the tokio `ClientError::Other("handshake failed: …")` payload —
/// inherits the bound. A bounded prefix of the message still round-trips so
/// the operator keeps a useful (just truncated) explanation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_bounds_oversized_broker_handshake_message() {
    // 'é' is 2 bytes; 400 of them = 800 bytes, with the 256-byte cut falling
    // mid-char so the boundary back-off is exercised end-to-end.
    let oversized = "é".repeat(400);
    let url = spawn_reject_handshake_broker(oversized.clone()).await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out");

    let msg = match result.expect_err("connect must fail on broker CommandError") {
        ClientError::Other(m) => m,
        other => panic!("expected ClientError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("handshake failed:"),
        "engine error must carry the enriched envelope (got len {}): {msg}",
        msg.len(),
    );
    // The surfaced message is "handshake failed: broker rejected handshake
    // (server_error=AuthenticationError): <bounded>". Only the broker text is
    // attacker-controlled; the fixed envelope prefixes are small and constant,
    // so the whole message must stay within budget + a small fixed envelope.
    let envelope_budget = MAX_BROKER_STR + 128;
    assert!(
        msg.len() <= envelope_budget,
        "oversized broker handshake message must be bounded \
         (msg len {} > budget {envelope_budget}): {msg}",
        msg.len(),
    );
    // A bounded char-boundary prefix of the broker message still round-trips.
    let bounded_prefix: String = oversized.chars().take(64).collect();
    assert!(
        msg.contains(&bounded_prefix),
        "a bounded prefix of the broker message must still surface (got: {msg})",
    );
}
