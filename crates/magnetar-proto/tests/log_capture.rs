// SPDX-License-Identifier: Apache-2.0

//! ADR-0054 proto log-emission unit tests (ADR-0024 layer a).
//!
//! `magnetar-proto` owns the point-of-detection logs (ADR-0054 §5
//! single-owner rule): the CRC32C `ChecksumMismatch` `error!` in the decode
//! loop, the redirect-chase hop `debug!`, and the handshake state-transition
//! `debug!`s. These tests drive the sans-io [`Connection`] directly and
//! capture everything it emits through a thread-local subscriber
//! (`tracing::subscriber::with_default` — proto is synchronous, so every
//! event fires on the test thread and no global subscriber is needed).
//!
//! Covered:
//!
//! 1. a CRC32C-corrupted frame fires the `error!` with `computed` / `expected` fields and the
//!    connection survives the drop;
//! 2. a broker `Redirect` lookup response fires the redirect-chase `debug!` with hop count + broker
//!    URLs;
//! 3. no log emitted across a full handshake — including an `AUTH_CHALLENGE` round-trip — contains
//!    the sentinel secrets placed in `CommandConnect.auth_data`, the challenge bytes, or the
//!    refreshed response bytes (ADR-0054 §3 no-secrets rule at the proto layer).

#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::conn::{Connection, ConnectionConfig};
use magnetar_proto::{encode_command, encode_payload, pb};

/// Sentinel placed in `CommandConnect.auth_data`. Must never be logged.
const TOKEN_SENTINEL: &str = "SENTINEL-PROTO-TOKEN-DO-NOT-LOG-4c1d";

/// Sentinel the fake broker puts on `CommandAuthChallenge.challenge.auth_data`.
/// Must never be logged.
const CHALLENGE_SENTINEL: &str = "SENTINEL-PROTO-CHALLENGE-BYTES-a0e2";

/// Sentinel submitted as the refreshed `CommandAuthResponse` bytes.
/// Must never be logged.
const RESPONSE_SENTINEL: &str = "SENTINEL-PROTO-REFRESHED-BYTES-d99f";

/// Shared in-memory sink for the capturing fmt subscriber.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("capture sink poisoned")).into_owned()
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("capture sink poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `f` with a TRACE-level capturing subscriber installed as the
/// thread-local default; return everything it logged plus `f`'s output.
///
/// Every [`Connection`] is constructed and driven **inside** the closure:
/// tracing caches per-callsite interest globally, and a callsite first hit
/// while zero dispatchers are registered (e.g. a helper running before
/// `with_default` on a parallel test thread) can race the interest-cache
/// rebuild and stick at `Interest::never`, silently emptying another
/// test's capture. Keeping all proto-driving code under a registered
/// dispatcher removes that cross-test hazard.
fn capture_logs<R>(f: impl FnOnce() -> R) -> (String, R) {
    let writer = CaptureWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(writer.clone())
        .with_ansi(false)
        .finish();
    let out = tracing::subscriber::with_default(subscriber, f);
    (writer.contents(), out)
}

/// `CommandConnected` reply frame completing the handshake.
fn connected_frame() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-log-capture-test".to_owned(),
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

/// Build a fully handshaked connection (drains the Connected event).
fn connected_conn(config: ConnectionConfig) -> Connection {
    let mut conn = Connection::new(config, Arc::new(std::time::SystemTime::now));
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(Instant::now(), &connected_frame())
        .expect("handle CommandConnected");
    while conn.poll_event().is_some() {}
    conn
}

/// CRC32C-corrupted payload frame — same construction as the proto unit
/// test `frame::tests::detects_crc32c_mismatch`: encode a SEND-shaped
/// payload frame, then flip the last payload byte to invalidate the CRC.
fn corrupted_payload_frame() -> Bytes {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Send as i32,
        send: Some(pb::CommandSend {
            producer_id: 1,
            sequence_id: 1,
            num_messages: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "p".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000_000,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_payload(&mut buf, &cmd, &meta, b"corrupt-me").expect("encode_payload");
    let last = buf.len() - 1;
    buf[last] ^= 0xff;
    buf.freeze()
}

/// ADR-0054 §5: the `ChecksumMismatch` `error!` fires at the decode-loop
/// detection point with plain-integer `computed` / `expected` fields, and
/// the connection survives the drop (CRC32C verify-or-drop, workspace
/// invariant 4).
#[test]
fn checksum_mismatch_logs_error_at_detection_point() {
    let (logs, conn) = capture_logs(|| {
        let mut conn = connected_conn(ConnectionConfig::default());
        conn.handle_bytes(Instant::now(), &corrupted_payload_frame())
            .expect("corrupt frame is dropped, not fatal");
        conn
    });

    assert!(
        logs.contains("CRC32C checksum mismatch; corrupt frame dropped"),
        "checksum error! must fire at the proto detection point; got:\n{logs}"
    );
    assert!(
        logs.contains("ERROR"),
        "checksum mismatch must log at error! level; got:\n{logs}"
    );
    assert!(
        logs.contains("computed=") && logs.contains("expected="),
        "checksum error! must carry structured computed/expected fields; got:\n{logs}"
    );
    assert!(
        conn.is_connected(),
        "connection must survive the corrupt-frame drop"
    );
}

/// ADR-0054 §5: each internally-chased lookup redirect hop logs a `debug!`
/// with the hop count and the broker-advertised URLs.
#[test]
fn redirect_hop_logs_debug_with_hop_and_urls() {
    let (logs, ()) = capture_logs(|| {
        let mut conn = connected_conn(ConnectionConfig::default());
        let request_id = conn.lookup("persistent://public/default/log-capture", false);

        let redirect = pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: Some("pulsar://other:6650".to_owned()),
                broker_service_url_tls: Some("pulsar+ssl://other:6651".to_owned()),
                response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
                request_id: request_id.0,
                authoritative: Some(true),
                error: None,
                message: None,
                proxy_through_service_url: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &redirect).expect("encode redirect");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle redirect");
    });

    assert!(
        logs.contains("lookup redirected; chasing internally"),
        "redirect-chase hop debug! must fire at the proto detection point; got:\n{logs}"
    );
    assert!(
        logs.contains("hop=1") && logs.contains("hops_remaining=4"),
        "first hop must log hop=1 hops_remaining=4 (MAX_LOOKUP_REDIRECTS=5); got:\n{logs}"
    );
    // `&str` fields render Debug-quoted in the default fmt layout, so the
    // field name and the value are asserted separately.
    assert!(
        logs.contains("broker_service_url=")
            && logs.contains("pulsar://other:6650")
            && logs.contains("broker_service_url_tls=")
            && logs.contains("pulsar+ssl://other:6651"),
        "redirect hop must log both broker-advertised URLs; got:\n{logs}"
    );
    assert!(
        logs.contains("topic=persistent://public/default/log-capture"),
        "redirect hop must carry the topic field (Display sigil, unquoted); got:\n{logs}"
    );
}

/// ADR-0054 §3 at the proto layer: a full handshake — `CommandConnect`
/// (sentinel `auth_data`) → `CommandAuthChallenge` (sentinel challenge
/// bytes) → `CommandAuthResponse` (sentinel refreshed bytes) →
/// `CommandConnected` — must never leak any sentinel into the logs, at any
/// level, while the state-transition `debug!`s do fire (guards against a
/// vacuously-empty capture).
#[test]
fn handshake_logs_never_contain_auth_data() {
    let (logs, ()) = capture_logs(|| {
        let config = ConnectionConfig {
            auth_method_name: "token".to_owned(),
            auth_data: Some(Bytes::from(TOKEN_SENTINEL)),
            ..ConnectionConfig::default()
        };
        let mut conn = Connection::new(config, Arc::new(std::time::SystemTime::now));

        let challenge = pb::BaseCommand {
            r#type: pb::base_command::Type::AuthChallenge as i32,
            auth_challenge: Some(pb::CommandAuthChallenge {
                challenge: Some(pb::AuthData {
                    auth_method_name: Some("token".to_owned()),
                    auth_data: Some(Bytes::from(CHALLENGE_SENTINEL)),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut challenge_buf = BytesMut::new();
        encode_command(&mut challenge_buf, &challenge).expect("encode CommandAuthChallenge");

        conn.begin_handshake().expect("begin_handshake");
        let _ = conn.poll_transmit();
        conn.handle_bytes(Instant::now(), &challenge_buf)
            .expect("handle CommandAuthChallenge");
        while conn.poll_event().is_some() {}
        conn.submit_auth_response(Bytes::from(RESPONSE_SENTINEL), Some("token".to_owned()));
        let _ = conn.poll_transmit();
        conn.handle_bytes(Instant::now(), &connected_frame())
            .expect("handle CommandConnected");
        while conn.poll_event().is_some() {}
    });

    // The capture is live: every handshake state edge logged its transition.
    assert!(
        logs.contains("handshake state transition"),
        "state-transition debug!s must fire during the handshake; got:\n{logs}"
    );
    for state in ["ConnectSent", "AuthChallenging", "Connected"] {
        assert!(
            logs.contains(state),
            "expected the {state} transition in the captured logs; got:\n{logs}"
        );
    }
    // §3 no-secrets rule: none of the sentinels may appear, at any level.
    for sentinel in [TOKEN_SENTINEL, CHALLENGE_SENTINEL, RESPONSE_SENTINEL] {
        assert!(
            !logs.contains(sentinel),
            "auth secret sentinel {sentinel} leaked into proto logs:\n{logs}"
        );
    }
}
