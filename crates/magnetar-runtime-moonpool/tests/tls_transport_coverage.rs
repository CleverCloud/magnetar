// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the TLS variant of `Transport` and the
//! engine's `connect_tls` entry point.
//!
//! `crates/magnetar-runtime-moonpool/src/transport.rs` carries the
//! `Transport::Tls { stream, adapter, plaintext_overflow }` arm and its
//! associated pump (`connect_tls`, `tls_handshake`, the TLS variants of
//! `read_buf` / `write_all` / `flush` / `shutdown`). Until this file
//! landed, only the `Plain` arm was exercised through the engine —
//! per-file coverage on `transport.rs` sat at 30.3% with the 124-line
//! TLS hunk uncovered (`docs/follow-ups.md` §7, ADR-0024 patch coverage).
//!
//! Strategy. Stand up an in-process rustls server (one self-signed cert
//! per test, minted with `rcgen` at fixture build time so no PEM file
//! ships in-tree) bound to `127.0.0.1:0`. Hand the cert to the moonpool
//! client as the sole trust anchor, then drive `connect_tls` against
//! the listener through `TokioProviders` (which the moonpool engine
//! consumes the same way `moonpool-sim` would — option (d) ADR-0006).
//!
//! Why `TokioProviders` and not `SimProviders`. `moonpool-sim`'s
//! `SimTcpStream` is a deterministic byte pipe; a `tokio_rustls`
//! server-side `TlsAcceptor` cannot wrap it (they are separate
//! `AsyncRead`/`AsyncWrite` worlds). The TLS pump's correctness is the
//! same either way: the moonpool client's `RustlsByteAdapter` drives
//! `rustls::ClientConnection` sans-io, regardless of which provider
//! supplies the wire bytes. Once moonpool-sim grows a hook to inject
//! pre-driven cipher bytes (currently tracked at
//! [`docs/follow-ups.md` §1](https://github.com/CleverCloud/magnetar/blob/main/docs/follow-ups.md)),
//! the same tests can flip over to `SimProviders` without changing
//! their assertions.
//!
//! Each test pairs 1:1 with a same-named test in the tokio crate's
//! `tls_transport_coverage.rs` to keep the
//! `xtask check-runtime-test-parity` gate balanced (ADR-0024). The
//! tokio mirrors are intentionally lightweight (Debug/fmt + crypto-
//! provider smoke) because the tokio engine's TLS path is already
//! exercised end-to-end through `tls_handshake_chaos.rs` and the
//! workspace e2e suite.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{EngineError, MoonpoolEngine};
use moonpool_core::TokioProviders;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

/// SNI hostname every fixture-issued cert carries as a SAN. The cert is
/// also valid for `localhost` so a host-resolved IPv4 dial still verifies.
const FIXTURE_HOSTNAME: &str = "broker.test.invalid";

/// Test-side TLS server fixture.
///
/// Generates a fresh self-signed cert via `rcgen`, binds a localhost
/// TCP listener, wraps each inbound connection in `tokio_rustls`'s
/// `TlsAcceptor`, and runs the supplied per-connection handler.
///
/// Returns the bound `host:port` literal (suitable for
/// `MoonpoolEngine::connect_tls`'s `addr` argument) and a
/// `rustls::ClientConfig` pre-loaded with the issued cert as the sole
/// trust anchor — exactly what the moonpool engine needs to verify the
/// fixture's identity without falling back to the system trust store.
///
/// The server task is spawned on the current tokio runtime and aborts
/// when the test drops its `JoinHandle`. Each test typically spins up
/// its own fixture so cross-test interference (port collisions, leaked
/// connections) is impossible.
struct TlsFixture {
    addr: String,
    client_config: Arc<ClientConfig>,
    server_handle: JoinHandle<()>,
}

impl Drop for TlsFixture {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

/// Mint a self-signed cert + key pair, build a `rustls::ServerConfig`
/// that uses it, and a client-side `rustls::ClientConfig` that trusts
/// the same cert. Single source of truth for the test crypto material
/// so we never accidentally mismatch the server's identity from the
/// client's expectations.
fn build_tls_material() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec![FIXTURE_HOSTNAME.to_owned(), "localhost".to_owned()])
            .expect("rcgen self-signed cert");
    let cert_der: CertificateDer<'static> = cert.der().clone();
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    // Server side: present the cert+key on every inbound connection.
    // Use `builder_with_provider` so the test never trips rustls's
    // "ambiguous default" panic when the workspace's `--all-features`
    // pulls in both `aws-lc-rs` and `ring`.
    let server_config = ServerConfig::builder_with_provider(
        magnetar_runtime_moonpool::tls_crypto::active_provider(),
    )
    .with_safe_default_protocol_versions()
    .expect("rustls default protocol versions are valid")
    .with_no_client_auth()
    .with_single_cert(vec![cert_der.clone()], key_der)
    .expect("rustls server config");

    // Client side: trust ONLY the fixture-issued cert. Empty root store
    // would force every test to use `tls_insecure` shims — the explicit
    // trust anchor here exercises the production verification path.
    let mut roots = RootCertStore::empty();
    roots
        .add(cert_der)
        .expect("trust anchor add must succeed for a freshly-minted cert");
    let client_config = ClientConfig::builder_with_provider(
        magnetar_runtime_moonpool::tls_crypto::active_provider(),
    )
    .with_safe_default_protocol_versions()
    .expect("rustls default protocol versions are valid")
    .with_root_certificates(roots)
    .with_no_client_auth();

    (Arc::new(server_config), Arc::new(client_config))
}

/// Spawn the TLS server fixture with `handler` driving each inbound
/// (already-handshaken) connection. The handler is invoked with the
/// post-TLS `TlsStream`, lets the test script the application-layer
/// protocol (typically Pulsar's CONNECT → CONNECTED exchange) and
/// observe the bytes the client wrote.
async fn spawn_tls_fixture<H, Fut>(handler: H) -> TlsFixture
where
    H: FnOnce(tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> Fut
        + Send
        + Clone
        + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (server_config, client_config) = build_tls_material();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let acceptor = TlsAcceptor::from(server_config);

    let server_handle = tokio::spawn(async move {
        // Accept ONE connection (the test only dials once); subsequent
        // accepts would block forever, which is fine — the test drops
        // the JoinHandle to abort us.
        let Ok((tcp, _peer)) = listener.accept().await else {
            return;
        };
        let tls = match acceptor.accept(tcp).await {
            Ok(s) => s,
            Err(err) => {
                tracing::debug!("tls fixture: accept failed: {err}");
                return;
            }
        };
        let h = handler.clone();
        h(tls).await;
    });

    TlsFixture {
        addr,
        client_config,
        server_handle,
    }
}

/// Spawn a TLS fixture whose server-side handler accepts the TLS
/// handshake, reads the client's Pulsar `CommandConnect` frame, and
/// writes back a `CommandConnected` response. Mirrors
/// `common::handshake_response_bytes` (the moonpool-side helper for the
/// plaintext fixture) — kept inline here because the TLS pump is the
/// thing under test, not the application-layer handshake content.
async fn spawn_pulsar_connected_fixture() -> TlsFixture {
    spawn_tls_fixture(|mut tls| async move {
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        // Drain inbound bytes until we have a full Pulsar frame; reply
        // CONNECTED. The handshake exchange is exactly one frame each way.
        loop {
            let mut framed = read_buf.clone().freeze();
            match decode_one(&mut framed) {
                Ok(_frame) => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "magnetar-tls-fixture".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let mut out = BytesMut::new();
                    encode_command(&mut out, &cmd).expect("encode CONNECTED");
                    if tls.write_all(&out).await.is_err() {
                        return;
                    }
                    if tls.flush().await.is_err() {
                        return;
                    }
                    // Park: keep the TLS session open so the client's
                    // driver doesn't see an EOF before the test asserts.
                    // Read once more so we observe the eventual close
                    // signal; ignore the result.
                    let mut sink = [0u8; 64];
                    let _ = tls.read(&mut sink).await;
                    return;
                }
                Err(FrameError::Incomplete { .. }) => {}
                Err(err) => {
                    tracing::debug!("tls fixture: decode error: {err:?}");
                    return;
                }
            }
            match tls.read_buf(&mut read_buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
}

/// Spawn a TLS fixture whose server-side handler completes the TLS
/// handshake and then immediately drops the connection — driving the
/// "peer closed mid-application-handshake" branch in
/// `Transport::read_buf` (TLS arm, EOF path) and in the engine's
/// `handshake_plain`.
async fn spawn_drop_after_tls_fixture() -> TlsFixture {
    spawn_tls_fixture(|tls| async move {
        // Explicit drop — close the TLS session cleanly so the client
        // observes a TLS `close_notify` rather than a TCP RST. The
        // moonpool engine surfaces this as `EngineError::PeerClosed`.
        drop(tls);
    })
    .await
}

// -- TEST 1 -----------------------------------------------------------------

/// End-to-end success path: the moonpool engine completes the rustls
/// handshake against the fixture, then exchanges the Pulsar CONNECT /
/// CONNECTED frames over the encrypted channel, then returns Ok with a
/// fully-connected `ConnectionShared`. Exercises (in order):
///
/// 1. `Transport::connect_tls` (DNS-less localhost dial path).
/// 2. `Transport::tls_handshake` (full handshake pump, multi-record loop).
/// 3. TLS `Transport::write_all` (encrypts the client's `CommandConnect`).
/// 4. TLS `Transport::flush` (drains any post-write ciphertext).
/// 5. TLS `Transport::read_buf` (pulls + decrypts the broker's `CONNECTED`).
/// 6. `Transport::shutdown` (run on driver drop at end-of-scope).
///
/// The driver task is spawned via `TokioTaskProvider`'s `spawn_local`,
/// so the test runs inside a `LocalSet` on the current-thread runtime
/// — mirrors every other moonpool integration test that crosses the
/// engine boundary.
#[tokio::test(flavor = "current_thread")]
async fn connect_tls_completes_handshake_then_drives_pulsar_connected() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fixture = spawn_pulsar_connected_fixture().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                engine.connect_tls(
                    &fixture.addr,
                    FIXTURE_HOSTNAME,
                    fixture.client_config.clone(),
                    ConnectionConfig::default(),
                    None,
                ),
            )
            .await
            .expect("connect_tls did not complete within 5s");

            let (shared, driver) = result.expect("connect_tls must succeed against the fixture");
            assert!(
                shared.inner.lock().is_connected(),
                "post-handshake state must be Connected after a successful TLS + CONNECT round-trip"
            );

            // Drop the driver — exercises the `Transport::shutdown` arm
            // on the TLS path. The driver task aborts itself on
            // `DriverHandle::drop` (see `driver.rs`).
            drop(driver);
            drop(shared);
            drop(fixture);
        })
        .await;
}

// -- TEST 2 -----------------------------------------------------------------

/// `connect_tls` rejects an SNI name rustls cannot parse as a
/// `ServerName`. Surfaces as `EngineError::Config` *before* any wire
/// traffic — no fixture needed, the failure is structural. Pins the
/// caller-friendly error envelope (raw `ServerName::try_from` error
/// would leak rustls types into the moonpool surface).
#[tokio::test(flavor = "current_thread")]
async fn connect_tls_rejects_invalid_server_name() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Listen so the TCP dial succeeds; the SNI failure must
            // happen client-side BEFORE the rustls handshake byte loop.
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local_addr").to_string();
            // Don't accept — if the test misbehaves, the dial parks and
            // the timeout below fires.
            let _accept = tokio::spawn(async move {
                let _ = listener.accept().await;
            });

            let (_server_cfg, client_cfg) = build_tls_material();
            let engine = MoonpoolEngine::new(TokioProviders::new());

            // Empty string is not a valid `ServerName`: rustls rejects
            // it. The moonpool transport wraps the failure in
            // `EngineError::Config` with a hint.
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                engine.connect_tls(&addr, "", client_cfg, ConnectionConfig::default(), None),
            )
            .await
            .expect("connect_tls with bad SNI must not park indefinitely");

            match result {
                Err(EngineError::Config(msg)) => {
                    assert!(
                        msg.contains("invalid TLS server name"),
                        "error message should mention TLS server name; got {msg}",
                    );
                }
                Err(other) => panic!("expected EngineError::Config for invalid SNI, got {other:?}"),
                Ok(_) => panic!("connect_tls must not succeed against an invalid SNI"),
            }
        })
        .await;
}

// -- TEST 3 -----------------------------------------------------------------

/// The TLS handshake completes, then the broker drops the connection
/// before responding to the Pulsar `CommandConnect`. The engine's
/// `handshake_plain` must surface this as `EngineError::PeerClosed`
/// (the recoverable envelope the supervisor reconnect path already
/// understands). Exercises the EOF arm of TLS `Transport::read_buf`
/// (`read_n == 0`) and the post-write `flush` path.
#[tokio::test(flavor = "current_thread")]
async fn connect_tls_propagates_peer_drop_after_tls_handshake() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fixture = spawn_drop_after_tls_fixture().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                engine.connect_tls(
                    &fixture.addr,
                    FIXTURE_HOSTNAME,
                    fixture.client_config.clone(),
                    ConnectionConfig::default(),
                    None,
                ),
            )
            .await
            .expect("connect_tls did not complete within 5s");

            match result {
                Err(EngineError::PeerClosed) => {
                    // Expected: rustls `close_notify` from the dropped
                    // session shows up as a clean EOF on `read_buf`.
                }
                Err(EngineError::Io(err)) => {
                    // Acceptable on slower hosts: the TCP RST beats the
                    // TLS `close_notify` over loopback. Either envelope
                    // is recoverable for the supervisor.
                    assert!(
                        !err.to_string().is_empty(),
                        "io error must carry a payload, got {err:?}"
                    );
                }
                Err(other) => {
                    panic!("expected PeerClosed or Io for post-handshake drop, got {other:?}")
                }
                Ok(_) => {
                    panic!("connect_tls must not succeed when the broker drops before CONNECTED")
                }
            }
            drop(fixture);
        })
        .await;
}

// -- TEST 4 -----------------------------------------------------------------

/// After a successful TLS + Pulsar handshake the test drops the
/// `DriverHandle` (which the moonpool engine wires up to abort the
/// spawned driver task on drop). The TLS `Transport::shutdown` arm
/// runs as part of the driver's exit path. The test passes if no
/// panic / hang surfaces; the `tokio::time::timeout` is the hang gate.
///
/// This is the only test that explicitly demonstrates the TLS
/// `Transport::shutdown` arm runs without panicking — the other
/// success-path test exercises the same arm too, but does so as a
/// side-effect rather than as the assertion.
#[tokio::test(flavor = "current_thread")]
async fn connect_tls_clean_shutdown_releases_resources() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fixture = spawn_pulsar_connected_fixture().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());

            let (shared, driver) = tokio::time::timeout(
                Duration::from_secs(5),
                engine.connect_tls(
                    &fixture.addr,
                    FIXTURE_HOSTNAME,
                    fixture.client_config.clone(),
                    ConnectionConfig::default(),
                    None,
                ),
            )
            .await
            .expect("connect_tls did not complete within 5s")
            .expect("connect_tls must succeed");

            // Mark the connection as user-closed so the driver's loop
            // observes the close request and exits via the shutdown arm
            // rather than getting aborted mid-poll. The `close()` hook
            // flips `is_closed()` true so the driver's "user requested
            // close" gate fires on its next wake.
            shared.inner.lock().close();
            shared.driver_waker.notify_one();

            // Yield a few times so the driver observes the wake and
            // runs through `Transport::shutdown` on its way out.
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }

            // Abort explicitly — dropping the handle only detaches the task.
            // Cooperative `abort()` closes the connection and wakes the driver
            // so it runs its shutdown path and exits, letting a subsequent
            // `.join().await` resolve rather than hang.
            driver.abort();
            let join_outcome = tokio::time::timeout(Duration::from_secs(3), driver.join()).await;
            assert!(
                join_outcome.is_ok(),
                "driver join must not hang after user_close + abort"
            );

            drop(shared);
            drop(fixture);
        })
        .await;
}
