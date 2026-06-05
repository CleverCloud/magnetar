// SPDX-License-Identifier: Apache-2.0

//! ADR-0054 secret-scan capture test — tokio engine, real loopback broker.
//!
//! Drives the full engine driver through an in-band `AUTH_CHALLENGE`
//! round-trip (`CommandConnect` → `CommandAuthChallenge` →
//! `CommandAuthResponse` → `CommandConnected`) against a scripted loopback
//! broker (harness pattern: `tests/handshake_error_capture.rs`), with a
//! **global** TRACE-level capturing subscriber installed, and asserts the
//! ADR-0054 no-secrets rule on everything the process logged:
//!
//! 1. the `CommandConnect.auth_data` sentinel token never appears,
//! 2. the broker's challenge sentinel bytes never appear,
//! 3. `"BEGIN PRIVATE KEY"` / `"client_secret"` never appear (defence in depth),
//! 4. a provider whose error `Display` embeds a sentinel does NOT leak it through the auth-path
//!    `warn!` (which logs `auth_method` + a stable error class only),
//! 5. the `auth_method` and `host`/`port` lifecycle fields ARE present.
//!
//! # Why this file is its own integration-test binary with ONE test fn
//!
//! The capturing subscriber must be **global** (`set_global_default` via
//! `fmt().init()`): the engine driver runs on other tokio worker threads,
//! so a thread-local `set_default` guard would miss every driver-side
//! event. A global subscriber can be installed exactly once per process,
//! so this file holds a single test fn and shares the binary with no other
//! test (mirrored 1:1 by
//! `crates/magnetar-runtime-moonpool/tests/logging_no_secrets.rs` for the
//! ADR-0024 runtime-test-parity count).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::auth::{AuthError, AuthProvider, TokenAuth};
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Sentinel token carried in `CommandConnect.auth_data` and returned by the
/// `TokenAuth` provider on the challenge refresh. Must never be logged.
const TOKEN_SENTINEL: &str = "SENTINEL-TOKEN-DO-NOT-LOG-7f3a";

/// Sentinel challenge bytes the scripted broker puts on
/// `CommandAuthChallenge.challenge.auth_data`. Must never be logged.
const CHALLENGE_SENTINEL: &str = "SENTINEL-CHALLENGE-BYTES-91bc";

/// Sentinel embedded in the leaky provider's error `Display`. The auth-path
/// `warn!` logs `auth_method` + error class only, so this must never reach
/// the captured logs.
const ERROR_SENTINEL: &str = "SENTINEL-PROVIDER-ERROR-55ee";

/// Shared in-memory sink for the global fmt subscriber.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
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

/// Auth provider whose challenge-refresh error `Display` embeds a sentinel —
/// models a third-party provider stringifying credentials into its errors.
#[derive(Debug)]
struct LeakyErrorProvider;

impl AuthProvider for LeakyErrorProvider {
    fn method(&self) -> &str {
        "leaky"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        Ok(Bytes::from_static(b"leaky-initial-credential"))
    }

    fn respond_to_challenge(&self, _challenge: &[u8]) -> Result<Bytes, AuthError> {
        Err(AuthError::Invalid(format!(
            "refresh rejected by IDP: {ERROR_SENTINEL}"
        )))
    }
}

/// Scripted broker: `CommandConnect` → `CommandAuthChallenge` (sentinel
/// bytes) → [`CommandAuthResponse` → `CommandConnected`] → read until EOF.
/// When the client never answers the challenge (the leaky-provider leg),
/// the read loop simply observes the client-side teardown.
async fn run_auth_challenge_broker(listener: TcpListener) {
    let Ok((mut stream, _peer)) = listener.accept().await else {
        return;
    };
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_challenge = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => {
                    let challenge = pb::BaseCommand {
                        r#type: pb::base_command::Type::AuthChallenge as i32,
                        auth_challenge: Some(pb::CommandAuthChallenge {
                            server_version: Some("logging-no-secrets-broker/0".to_owned()),
                            challenge: Some(pb::AuthData {
                                auth_method_name: Some("token".to_owned()),
                                auth_data: Some(Bytes::from_static(CHALLENGE_SENTINEL.as_bytes())),
                            }),
                            protocol_version: Some(21),
                        }),
                        ..Default::default()
                    };
                    let mut out = BytesMut::new();
                    let _ = encode_command(&mut out, &challenge);
                    if stream.write_all(&out).await.is_err() {
                        return;
                    }
                    let _ = stream.flush().await;
                    sent_challenge = true;
                }
                Ok(pb::base_command::Type::AuthResponse) if sent_challenge => {
                    let connected = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "logging-no-secrets-broker/0".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let mut out = BytesMut::new();
                    let _ = encode_command(&mut out, &connected);
                    if stream.write_all(&out).await.is_err() {
                        return;
                    }
                    let _ = stream.flush().await;
                }
                _ => {}
            }
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Spawn the scripted broker on a fresh loopback port; returns its URL.
async fn spawn_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(run_auth_challenge_broker(listener));
    format!("pulsar://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_never_contain_auth_secrets() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    // ── Leg 1: happy AUTH_CHALLENGE round-trip with a sentinel token ──
    let url = spawn_broker().await;
    let config = ConnectionConfig {
        auth_method_name: "token".to_owned(),
        auth_data: Some(Bytes::from_static(TOKEN_SENTINEL.as_bytes())),
        ..ConnectionConfig::default()
    };
    let provider: Arc<dyn AuthProvider> = Arc::new(TokenAuth::from_string(TOKEN_SENTINEL));
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_auth(&url, config, Some(provider)),
    )
    .await
    .expect("connect did not time out")
    .expect("connect through AUTH_CHALLENGE must succeed");
    client.close().await;

    // ── Leg 2: provider whose error `Display` embeds a sentinel ──
    // The driver's auth-path `warn!` must log `auth_method` + error class
    // only. The connect future itself never resolves (the driver dies on
    // the failed refresh and no supervisor is configured), so the leg is
    // bounded by a timeout and the captured logs are the assertion target.
    let url = spawn_broker().await;
    let config = ConnectionConfig {
        auth_method_name: "leaky".to_owned(),
        auth_data: Some(Bytes::from_static(b"leaky-initial-credential")),
        ..ConnectionConfig::default()
    };
    let provider: Arc<dyn AuthProvider> = Arc::new(LeakyErrorProvider);
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        Client::connect_auth(&url, config, Some(provider)),
    )
    .await;

    // ── Assertions on everything the process logged ──
    let captured = sink.contents();
    assert!(
        !captured.is_empty(),
        "the capturing subscriber must have seen events",
    );
    for secret in [
        TOKEN_SENTINEL,
        CHALLENGE_SENTINEL,
        ERROR_SENTINEL,
        "BEGIN PRIVATE KEY",
        "client_secret",
    ] {
        assert!(
            !captured.contains(secret),
            "captured logs must never contain the secret {secret:?}:\n{captured}",
        );
    }
    // Lifecycle fields present: auth_method + host/port on the
    // "connection established" record, and the auth-path error class.
    for needle in [
        "connection established",
        "auth_method",
        "token",
        "127.0.0.1",
        "auth challenge received",
        "error_class",
        "auth_refresh_failed",
        "leaky",
    ] {
        assert!(
            captured.contains(needle),
            "captured logs must contain {needle:?}:\n{captured}",
        );
    }
}
