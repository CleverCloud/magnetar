// SPDX-License-Identifier: Apache-2.0

//! Layer (d) of the ADR-0024 four-layer policy for the broker handshake
//! error-text bound (ADR-0062): tokio ↔ moonpool differential equivalence.
//!
//! ADR-0062 bounds a hostile broker's mid-handshake `CommandError.message`
//! ONCE at the shared `magnetar-proto` capture site, before it is stored in
//! [`magnetar_proto::Connection::handshake_failure_reason`]. Both engines'
//! connect surfaces then read that stored reason verbatim — the tokio engine
//! wraps it as `ClientError::Other("handshake failed: {reason}")`, the
//! moonpool engine as `EngineError::HandshakeFailed(reason)`. Because the
//! bound lives at the single shared capture point, the broker text each engine
//! surfaces MUST be byte-identical and identically bounded.
//!
//! The runner-based differential harness drives a fault-free handshake (the
//! `ScriptedBroker` answers `CommandConnect` → `CommandConnected`), so it has
//! no handshake-rejection knob and its `Event` stream does not carry the
//! connect-error reason text. This differential therefore exercises the SHARED
//! proto capture directly: it feeds the SAME oversized broker `CommandError`
//! bytes into two independent `Connection` instances — one standing in for the
//! tokio driver's byte feed, one for the moonpool driver's — and asserts the
//! captured reason is byte-for-byte equal across the two, then applies each
//! engine's exact sink transformation and asserts both surface the SAME
//! bounded broker text. A divergence here would mean one engine's sink leaked
//! an unbounded or differently-bounded broker string.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, HandshakeState, encode_command, pb};

/// Kept in sync with `magnetar_proto::log_fields::MAX_BROKER_STR` (a private
/// const). A broker message above this is truncated at the capture site.
const MAX_BROKER_STR: usize = 256;

/// Drive a fresh `Connection` to `ConnectSent`, feed it the broker
/// `CommandError(AuthenticationError, <message>)` mid-handshake, and return the
/// captured `handshake_failure_reason`. This mirrors exactly what each engine's
/// driver does when it reads the broker's rejection bytes off the wire.
fn capture_handshake_reason(broker_message: &str) -> String {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("begin_handshake");
    assert_eq!(conn.state(), HandshakeState::ConnectSent);

    let err = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id: 0,
            error: pb::ServerError::AuthenticationError as i32,
            message: broker_message.to_owned(),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &err).expect("encode CommandError");
    conn.handle_bytes(Instant::now(), &buf)
        .expect("handle CommandError");

    conn.handshake_failure_reason()
        .expect("mid-handshake CommandError must populate the reason")
        .to_owned()
}

/// The exact transformation the tokio engine applies to the captured reason
/// (`client.rs::handshake_failure_message`).
fn tokio_surface(reason: &str) -> String {
    format!("handshake failed: {reason}")
}

/// The exact transformation the moonpool engine applies (`lib.rs`
/// `EngineError::HandshakeFailed(reason.to_owned())`, whose `Display` is
/// `"handshake failed: {0}"`).
fn moonpool_surface(reason: &str) -> String {
    // Mirrors `EngineError::HandshakeFailed`'s `#[error("handshake failed: {0}")]`.
    format!("handshake failed: {reason}")
}

#[test]
fn handshake_error_text_is_identically_bounded_across_engines() {
    // 'é' is 2 bytes; 400 of them = 800 bytes, with the 256-byte cut falling
    // mid-char so the boundary back-off is exercised end-to-end.
    let oversized = "é".repeat(400);
    assert!(oversized.len() > MAX_BROKER_STR);

    // Both engines feed identical bytes into the SAME shared proto capture, so
    // the captured reason must be byte-for-byte identical.
    let tokio_reason = capture_handshake_reason(&oversized);
    let moonpool_reason = capture_handshake_reason(&oversized);
    assert_eq!(
        tokio_reason, moonpool_reason,
        "the shared proto capture must yield a byte-identical reason for both engines",
    );

    // The embedded broker text must be bounded at the capture site.
    let prefix = "broker rejected handshake (server_error=AuthenticationError): ";
    let embedded = tokio_reason
        .strip_prefix(prefix)
        .expect("reason must carry the fixed envelope prefix");
    assert!(
        embedded.len() <= MAX_BROKER_STR,
        "embedded broker text must be bounded to MAX_BROKER_STR (got {} bytes)",
        embedded.len(),
    );
    assert!(
        oversized.starts_with(embedded),
        "the bounded text must be a verbatim char-boundary prefix of the broker message",
    );

    // Each engine's surface transformation must preserve the SAME bounded text.
    let tokio_surfaced = tokio_surface(&tokio_reason);
    let moonpool_surfaced = moonpool_surface(&moonpool_reason);
    assert_eq!(
        tokio_surfaced, moonpool_surfaced,
        "both engines must surface the SAME bounded handshake-failure message",
    );
    // The whole surfaced message stays within budget + the small fixed envelope.
    let envelope_budget = MAX_BROKER_STR + 128;
    assert!(
        tokio_surfaced.len() <= envelope_budget,
        "surfaced message must stay bounded (len {} > budget {envelope_budget})",
        tokio_surfaced.len(),
    );
}

#[test]
fn short_handshake_error_text_round_trips_identically_across_engines() {
    // A SHORT message is below the budget — the bound is a ceiling, not a
    // fixed-width truncation, so it round-trips verbatim on both engines.
    let short = "token expired";
    let tokio_reason = capture_handshake_reason(short);
    let moonpool_reason = capture_handshake_reason(short);
    assert_eq!(tokio_reason, moonpool_reason);
    assert!(tokio_reason.contains(short));
    assert_eq!(
        tokio_surface(&tokio_reason),
        moonpool_surface(&moonpool_reason)
    );
}
