// SPDX-License-Identifier: Apache-2.0

//! ADR-0054 secret-scan capture test — moonpool engine, real loopback broker.
//!
//! 1:1 twin of `crates/magnetar-runtime-tokio/tests/logging_no_secrets.rs`
//! (ADR-0024 runtime-test-parity). Drives the moonpool engine through a
//! scripted `CommandConnect` (sentinel token in `auth_data`) →
//! `CommandAuthChallenge` (sentinel challenge bytes) → `CommandConnected`
//! exchange with a **global** TRACE-level capturing subscriber installed,
//! then asserts the ADR-0054 no-secrets rule on everything the process
//! logged: the token sentinel, the challenge sentinel, `"BEGIN PRIVATE
//! KEY"`, and `"client_secret"` never appear, while the `auth_method` and
//! `host`/`port` lifecycle fields DO appear.
//!
//! # Why `TokioProviders` and not `SimProviders`
//!
//! `SimulationBuilder::run()` unconditionally installs the
//! `SimulationLayer` as the **thread-local** default subscriber for the
//! whole run (`moonpool-sim` `runner/builder.rs`), which shadows any
//! global capturing subscriber for every event emitted inside the
//! simulation — the capture sink would stay empty. Driving the engine over
//! `TokioProviders` against a real loopback `TcpListener` (the
//! `tests/lookup_before_open.rs` harness pattern) exercises the same
//! moonpool engine code paths (`handshake_plain`, driver event pump) with
//! no subscriber shadowing.
//!
//! # Why the challenge leg asserts the *no-provider* warn
//!
//! The moonpool engine has no connect entry that accepts an
//! `AuthProvider` (`make_shared_with_providers` hardcodes `None` — a
//! pre-existing engine API gap; the tokio twin covers the
//! provider-refresh and provider-error-`Display` legs). The broker's
//! challenge therefore lands on the driver's "no `AuthProvider` configured"
//! `warn!`, which must carry the broker-requested `auth_method` and must
//! NOT carry the challenge bytes.
//!
//! # Why this file is its own integration-test binary with ONE test fn
//!
//! The capturing subscriber must be **global** (`fmt().init()`): the
//! engine driver runs on detached tasks, so a thread-local `set_default`
//! guard could miss driver-side events. A global subscriber can be
//! installed exactly once per process, so this file holds a single test fn
//! and shares the binary with no other test.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Sentinel token carried in `CommandConnect.auth_data`. Must never be logged.
const TOKEN_SENTINEL: &str = "SENTINEL-TOKEN-DO-NOT-LOG-7f3a";

/// Sentinel challenge bytes the scripted broker puts on
/// `CommandAuthChallenge.challenge.auth_data`. Must never be logged.
const CHALLENGE_SENTINEL: &str = "SENTINEL-CHALLENGE-BYTES-91bc";

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

/// Scripted broker: `CommandConnect` → `CommandAuthChallenge` (sentinel
/// bytes) → short pause (lets the engine driver spawn and park on read) →
/// `CommandConnected` → read until EOF. The delayed `Connected` frame is
/// what pumps the driver's event loop so the queued `AuthChallenge` event
/// reaches `handle_pending_events` (and its no-provider `warn!`).
async fn run_auth_challenge_broker(listener: TcpListener) {
    let Ok((mut stream, _peer)) = listener.accept().await else {
        return;
    };
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
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
            if pb::base_command::Type::try_from(frame.command.r#type)
                == Ok(pb::base_command::Type::Connect)
            {
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

                // Give `handshake_plain` time to observe `AuthChallenging`,
                // return, and hand the socket to the driver loop.
                tokio::time::sleep(Duration::from_millis(50)).await;

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
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn logs_never_contain_auth_secrets() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
            let addr = listener.local_addr().expect("local_addr");
            tokio::task::spawn_local(run_auth_challenge_broker(listener));

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let config = ConnectionConfig {
                auth_method_name: "token".to_owned(),
                auth_data: Some(Bytes::from_static(TOKEN_SENTINEL.as_bytes())),
                ..ConnectionConfig::default()
            };
            // `connect_plain` returns once the broker has answered the
            // CONNECT (here: with the AUTH_CHALLENGE) — the "connection
            // established" lifecycle record fires inside `handshake_plain`.
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr.to_string(), config),
            )
            .await
            .expect("connect did not time out")
            .expect("connect through the scripted handshake must succeed");

            // Wait for the broker's delayed `Connected` frame to pump the
            // driver event loop: the queued `AuthChallenge` event reaches
            // `handle_pending_events`, whose no-provider `warn!` carries the
            // broker-requested `auth_method`.
            tokio::time::sleep(Duration::from_millis(200)).await;
            client.close().await;
        })
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
        "BEGIN PRIVATE KEY",
        "client_secret",
    ] {
        assert!(
            !captured.contains(secret),
            "captured logs must never contain the secret {secret:?}:\n{captured}",
        );
    }
    // Lifecycle fields present: auth_method + host/port on the
    // "connection established" record, plus the no-provider challenge warn
    // carrying the broker-requested method.
    for needle in [
        "connection established",
        "auth_method",
        "token",
        "127.0.0.1",
        "no AuthProvider configured",
    ] {
        assert!(
            captured.contains(needle),
            "captured logs must contain {needle:?}:\n{captured}",
        );
    }
}
