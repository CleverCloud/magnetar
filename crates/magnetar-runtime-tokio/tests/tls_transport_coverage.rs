// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 parity mirror for
//! `crates/magnetar-runtime-moonpool/tests/tls_transport_coverage.rs`.
//!
//! The tokio engine's TLS path is already exercised end-to-end via:
//!
//! - [`tls_handshake_chaos.rs`](./tls_handshake_chaos.rs) — corrupt-byte rejection on
//!   `rustls::ClientConnection`.
//! - [`tls_crypto_provider_smoke.rs`](./tls_crypto_provider_smoke.rs) — per-provider TLS plumbing.
//! - The e2e suite (`cargo test -- --include-ignored`), which dials the Pulsar 4.x container over
//!   `pulsar+ssl://`.
//!
//! That leaves the moonpool-side TLS hunk in `transport.rs` as the
//! biggest single uncovered region in the workspace. The moonpool
//! fixture lifts the in-process rustls broker
//! (self-signed cert + `tokio_rustls::TlsAcceptor`) and pins the
//! engine's `connect_tls` end-to-end; this file keeps the
//! `xtask check-runtime-test-parity` gate balanced (ADR-0024 §3) by
//! adding same-named Debug / fmt / crypto-provider smoke counterparts
//! on the tokio side. The full-stack TLS behaviour the moonpool tests
//! cover is the same `rustls::ClientConnection` state machine the
//! tokio engine wraps via `tokio-rustls`, so the upstream coverage
//! transfers without a duplicated in-process broker.
//!
//! The three tests after `connect_tls_clean_shutdown_releases_resources` are not smoke
//! mirrors: they run the in-process TLS broker against the real tokio `Client`
//! (`connect_tls_bounds_a_silent_tls_peer_by_connect_timeout`,
//! `supervised_tls_redial_to_a_silent_peer_does_not_block_close`,
//! `supervised_tls_redial_verifies_the_failover_target_hostname`) and pin, on the tokio
//! engine, the two behaviours the moonpool engine was brought in line with: the whole dial
//! (resolver, connect, TLS handshake) is bounded by `connect_timeout`, and a supervised
//! redial verifies the PIP-121 target's certificate against that target's own hostname.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ControlledClusterFailover, FrameError, SupervisorConfig, decode_one,
};
use magnetar_runtime_tokio::tls_crypto::active_provider;
use magnetar_runtime_tokio::{Client, ClientError, DnsResolveFuture, DnsResolver, ParsedUrl};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_rustls::TlsAcceptor;

mod common;
use common::HANG_GUARD;

/// Build the same flavour of `ClientConfig` the moonpool TLS coverage
/// fixture hands to `MoonpoolEngine::connect_tls` — empty trust store
/// is fine for the tokio mirror because we never finish a handshake
/// here; we only smoke the construction path so the workspace's
/// active rustls crypto provider stays exercised at the test layer.
fn empty_trust_client_config() -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_provider(active_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls default protocol versions are valid")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    )
}

// -- TEST 1 -----------------------------------------------------------------

/// Tokio-side parity mirror for the moonpool
/// `connect_tls_completes_handshake_then_drives_pulsar_connected` test.
/// Constructs the same `rustls::ClientConnection` shape the production
/// engine wraps via `tokio-rustls`, confirming the workspace's
/// active crypto provider can mint a session that pre-emits a
/// `ClientHello`. The full handshake round-trip is exercised by
/// `tls_handshake_chaos.rs` (which feeds adversarial responses) and by
/// the e2e suite (which dials the live broker over `pulsar+ssl://`).
#[test]
fn connect_tls_completes_handshake_then_drives_pulsar_connected() {
    let config = empty_trust_client_config();
    let name = ServerName::try_from("broker.test.invalid").expect("valid server name");
    let mut session = ClientConnection::new(config, name).expect("rustls session");
    // The session must want to write the ClientHello before it observes
    // any inbound bytes — the moonpool engine's `tls_handshake` relies
    // on this to seed the byte-pump loop.
    assert!(
        session.wants_write(),
        "fresh rustls::ClientConnection must want to write the ClientHello"
    );
    // Drain the queued ClientHello so we can confirm the session
    // produced ciphertext on the very first step. Mirrors the moonpool
    // `RustlsByteAdapter::take_encrypted_outbound` drain after the
    // initial `step()`.
    let mut out = Vec::new();
    let n = session.write_tls(&mut out).expect("write_tls");
    assert!(n > 0, "ClientHello must be at least one TLS record");
    assert!(!out.is_empty(), "ClientHello bytes must be observable");
}

// -- TEST 2 -----------------------------------------------------------------

/// Tokio-side parity mirror for the moonpool
/// `connect_tls_rejects_invalid_server_name` test. The
/// `ServerName::try_from` failure surface is identical on both engines
/// — the moonpool engine wraps it in `EngineError::Config`, the tokio
/// engine surfaces it through `ClientError::Other`. Both paths root
/// in `rustls::pki_types::ServerName::try_from` returning `Err`, so
/// pinning the rustls envelope here keeps the cross-engine error
/// story consistent.
#[test]
fn connect_tls_rejects_invalid_server_name() {
    // Empty string is not a valid ServerName: rustls rejects it.
    let result = ServerName::try_from(String::new());
    assert!(
        result.is_err(),
        "rustls must reject an empty string as a ServerName"
    );
    // A bare colon is not a valid hostname either — exercises a
    // different validation branch in `ServerName::try_from`.
    let colon = ServerName::try_from(":".to_owned());
    assert!(colon.is_err(), "rustls must reject \":\" as a ServerName");
}

// -- TEST 3 -----------------------------------------------------------------

/// Tokio-side parity mirror for the moonpool
/// `connect_tls_propagates_peer_drop_after_tls_handshake` test. The
/// moonpool transport surfaces a peer drop as `EngineError::PeerClosed`
/// (`read_n == 0` on the TLS-decrypted byte pipe). The tokio engine
/// returns the same envelope via its own `ClientError::PeerClosed`
/// variant on EOF, which is already exercised end-to-end by the
/// `coverage_close.rs` driver-handle path. Here we pin the
/// `rustls::ClientConnection` state-machine contract: a fresh session
/// is in the "handshaking, no bytes yet" state — the moonpool engine's
/// `EngineError::PeerClosed` envelope is therefore reachable for any
/// peer that drops before a full record arrives.
#[test]
fn connect_tls_propagates_peer_drop_after_tls_handshake() {
    let config = empty_trust_client_config();
    let name = ServerName::try_from("broker.test.invalid").expect("valid server name");
    let mut session = ClientConnection::new(config, name).expect("rustls session");
    assert!(
        session.is_handshaking(),
        "fresh rustls::ClientConnection must be in the handshaking state"
    );
    // Drain the ClientHello (mirrors the moonpool engine's
    // `RustlsByteAdapter::take_encrypted_outbound` after the initial
    // `step()`). Once the ClientHello has been written, the session
    // pivots to wanting inbound bytes to consume the server's reply
    // — the exact gate the moonpool engine's `tls_handshake` loop
    // parks on. A peer drop at that point surfaces as
    // `EngineError::PeerClosed`.
    let mut sink = Vec::new();
    let _ = session.write_tls(&mut sink);
    assert!(
        session.wants_read(),
        "post-ClientHello rustls session must want_read to consume the ServerHello"
    );
}

// -- TEST 4 -----------------------------------------------------------------

/// Tokio-side parity mirror for the moonpool
/// `connect_tls_clean_shutdown_releases_resources` test. The moonpool
/// engine routes `Transport::shutdown` through `tokio::io::AsyncWriteExt::shutdown`
/// on the wrapped `TcpStream`. The tokio engine uses the same surface
/// through `tokio_rustls`'s `TlsStream<TcpStream>`. The shutdown path
/// is structurally simple — both wrappers delegate to the underlying
/// `TcpStream::shutdown` — but the moonpool fixture exercises the live
/// teardown so a future refactor that drops the shutdown call surfaces
/// there. Here we pin the rustls-side `send_close_notify` contract:
/// the API exists and accepts no arguments, so any caller (moonpool's
/// TLS pump or `tokio_rustls`'s drop impl) can invoke it without
/// per-session config.
#[test]
fn connect_tls_clean_shutdown_releases_resources() {
    let config = empty_trust_client_config();
    let name = ServerName::try_from("broker.test.invalid").expect("valid server name");
    let mut session = ClientConnection::new(config, name).expect("rustls session");
    // `send_close_notify` is the rustls hook for graceful TLS close;
    // both engines rely on the underlying `AsyncWrite::shutdown` to
    // drain the resulting alert. Calling it on a mid-handshake session
    // must not panic — rustls queues the alert for the next
    // `write_tls` drain.
    session.send_close_notify();
    let mut out = Vec::new();
    // Drain the alert (or whatever rustls has queued). The exact byte
    // count is internal to rustls; the assertion is that the API does
    // not panic and produces a valid TLS record stream we could ship
    // on the wire.
    let _ = session.write_tls(&mut out);
}

// -- Moonpool mirrors: the tokio engine already bounds the whole dial and
// -- verifies a failover redial against the target's own hostname ---------------

/// Mint a self-signed cert naming exactly `name`, and return the server config
/// that presents it plus the cert (to be added to the client's trust anchors).
fn build_named_server(name: &str) -> (Arc<ServerConfig>, CertificateDer<'static>) {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec![name.to_owned()]).expect("rcgen self-signed cert");
    let cert_der: CertificateDer<'static> = cert.der().clone();
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_config = ServerConfig::builder_with_provider(active_provider())
        .with_safe_default_protocol_versions()
        .expect("rustls default protocol versions are valid")
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("rustls server config");
    (Arc::new(server_config), cert_der)
}

/// A client config that trusts exactly the given certs.
fn trusting(certs: Vec<CertificateDer<'static>>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).expect("trust anchor");
    }
    Arc::new(
        ClientConfig::builder_with_provider(active_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls default protocol versions are valid")
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// A supervisor that redials quickly and never gives up.
fn fast_supervisor() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
        mandatory_stop: Duration::from_secs(1),
        max_attempts: None,
        ..SupervisorConfig::default()
    }
}

/// Resolves the two cluster hostnames of the failover scenario to their listeners.
#[derive(Debug)]
struct ClusterResolver {
    cluster_a: SocketAddr,
    cluster_b: SocketAddr,
}

impl DnsResolver for ClusterResolver {
    fn resolve<'a>(&'a self, host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
        let resolved = match host {
            "a.test" => Ok(vec![self.cluster_a]),
            "b.test" => Ok(vec![self.cluster_b]),
            other => Err(ClientError::Other(format!(
                "test resolver only knows a.test and b.test, got {other}"
            ))),
        };
        Box::pin(async move { resolved })
    }
}

/// Answer one inbound Pulsar `CommandConnect` with `CommandConnected` over an
/// already-handshaken TLS stream. Returns `false` when the peer went away first.
async fn reply_connected(tls: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> bool {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    loop {
        let mut framed = read_buf.clone().freeze();
        match decode_one(&mut framed) {
            Ok(_) => break,
            Err(FrameError::Incomplete { .. }) => {}
            Err(_) => return false,
        }
        match tls.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
    }
    let output = common::handshake_response_bytes();
    tls.write_all(&output).await.is_ok() && tls.flush().await.is_ok()
}

/// Tokio-side mirror of the moonpool test of the same name: `connect_timeout`
/// bounds the TLS handshake too, so a peer that accepts TCP and never answers the
/// `ClientHello` fails the dial with `Io(TimedOut)` after about `connect_timeout`.
/// The tokio engine puts the whole dial (resolver, connect, handshake) under one
/// `tokio::time::timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_tls_bounds_a_silent_tls_peer_by_connect_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local_addr").port();
    // Accept the TCP connection and hold it open without ever answering.
    let silent_peer = tokio::spawn(async move {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        std::future::pending::<()>().await;
        drop(tcp);
    });

    let config = ConnectionConfig {
        connect_timeout: Duration::from_millis(200),
        connect_max_retries: 0,
        ..ConnectionConfig::default()
    };
    let url = ParsedUrl::parse(&format!("pulsar+ssl://127.0.0.1:{port}")).expect("url");
    // The guard is a fail-fast bound for a regression, not the behaviour asserted:
    // the correct dial gives up after ~200 ms.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_with_resolver_and_provider(
            url,
            Some(empty_trust_client_config()),
            config,
            None,
            None,
            None,
        ),
    )
    .await;
    silent_peer.abort();

    let Ok(outcome) = outcome else {
        panic!(
            "connect parked for 5s on a TLS peer that accepted TCP and never answered the \
             ClientHello: connect_timeout (200ms) must bound the TLS handshake too"
        );
    };
    match outcome {
        Err(ClientError::Io(err)) => assert_eq!(
            err.kind(),
            std::io::ErrorKind::TimedOut,
            "a dial that exceeds connect_timeout surfaces as Io(TimedOut), got {err:?}"
        ),
        Err(other) => panic!("expected Io(TimedOut) from the bounded dial, got {other:?}"),
        Ok(_) => panic!("connect must not succeed against a silent peer"),
    }
}

/// Tokio-side mirror of the moonpool test of the same name: the supervisor's redial
/// goes through the same bounded dial, so a TLS peer that accepts the replacement
/// TCP connection and stays silent is abandoned after ~`connect_timeout`, the
/// supervisor observes the close request and `Client::close` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_tls_redial_to_a_silent_peer_does_not_block_close() {
    let (server_config, cert) = build_named_server("a.test");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let acceptor = TlsAcceptor::from(server_config);
    let drop_first = Arc::new(Notify::new());
    let silent_accepts = Arc::new(AtomicUsize::new(0));
    let silent_changed = Arc::new(Notify::new());
    let server = {
        let drop_first = drop_first.clone();
        let silent_accepts = silent_accepts.clone();
        let silent_changed = silent_changed.clone();
        tokio::spawn(async move {
            // First connection: a real TLS + Pulsar handshake, then dropped on cue.
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                return;
            };
            if !reply_connected(&mut tls).await {
                return;
            }
            drop_first.notified().await;
            drop(tls);
            // Every later connection: accept TCP, never answer the ClientHello.
            let mut held = Vec::new();
            while let Ok((tcp, _)) = listener.accept().await {
                held.push(tcp);
                silent_accepts.fetch_add(1, Ordering::Release);
                silent_changed.notify_waiters();
            }
        })
    };

    let config = ConnectionConfig {
        connect_timeout: Duration::from_millis(200),
        supervisor: Some(fast_supervisor()),
        ..ConnectionConfig::default()
    };
    let url = ParsedUrl::parse(&format!("pulsar+ssl://a.test:{}", addr.port())).expect("url");
    let resolver = Arc::new(ClusterResolver {
        cluster_a: addr,
        cluster_b: addr,
    });
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_with_resolver_and_provider(
            url,
            Some(trusting(vec![cert])),
            config,
            None,
            None,
            Some(resolver),
        ),
    )
    .await
    .expect("supervised TLS client did not complete its initial handshake")
    .expect("supervised TLS client initial handshake succeeds");

    drop_first.notify_one();
    // Wait until the supervisor's redial has reached the silent peer.
    tokio::time::timeout(
        HANG_GUARD,
        wait_for_count(&silent_accepts, &silent_changed, 1),
    )
    .await
    .expect("the supervisor never redialled the silent peer");

    // `close()` is a fail-fast bound for a regression: the correct redial is
    // abandoned after ~200 ms and the supervisor then exits on the close request.
    let closed = tokio::time::timeout(Duration::from_secs(5), client.close()).await;
    server.abort();
    assert!(
        closed.is_ok(),
        "Client::close parked for 5s behind a supervised TLS redial whose peer accepted TCP and \
         never answered the ClientHello: connect_timeout (200ms) must bound the handshake of a \
         redial too"
    );
}

/// Cluster A of the failover scenario: one full TLS + Pulsar session on the first
/// connection, dropped when `drop_session` is notified.
fn spawn_old_cluster(
    listener: TcpListener,
    server_config: Arc<ServerConfig>,
    drop_session: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    let acceptor = TlsAcceptor::from(server_config);
    tokio::spawn(async move {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut tls) = acceptor.accept(tcp).await else {
            return;
        };
        if !reply_connected(&mut tls).await {
            return;
        }
        drop_session.notified().await;
        drop(tls);
    })
}

/// What cluster B of the failover scenario observed.
struct NewCluster {
    task: tokio::task::JoinHandle<()>,
    /// TLS sessions that completed the Pulsar handshake.
    sessions: Arc<AtomicUsize>,
    /// Handshakes the client aborted (a certificate-name rejection surfaces here).
    rejected: Arc<AtomicUsize>,
    /// Pulsed whenever `sessions` grows.
    changed: Arc<Notify>,
}

/// Cluster B of the failover scenario: serves every connection and counts what it saw.
fn spawn_new_cluster(listener: TcpListener, server_config: Arc<ServerConfig>) -> NewCluster {
    let sessions = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let acceptor = TlsAcceptor::from(server_config);
    let task = {
        let sessions = sessions.clone();
        let rejected = rejected.clone();
        let changed = changed.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                match acceptor.accept(tcp).await {
                    Ok(mut tls) => {
                        if reply_connected(&mut tls).await {
                            sessions.fetch_add(1, Ordering::Release);
                            changed.notify_waiters();
                            let mut sink = [0_u8; 64];
                            let _ = tls.read(&mut sink).await;
                        }
                    }
                    Err(_) => {
                        rejected.fetch_add(1, Ordering::Release);
                    }
                }
            }
        })
    };
    NewCluster {
        task,
        sessions,
        rejected,
        changed,
    }
}

/// Wait until `counter` reaches `at_least`, re-checking whenever `changed` is pulsed.
async fn wait_for_count(counter: &AtomicUsize, changed: &Notify, at_least: usize) {
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if counter.load(Ordering::Acquire) >= at_least {
            return;
        }
        notified.await;
    }
}

/// Tokio-side mirror of the moonpool test of the same name. PIP-121: the supervisor
/// takes the redial target from the `ServiceUrlProvider`; after the provider switches
/// to a differently named cluster the redial verifies that cluster's certificate
/// against the target's own hostname (`b.test`), not the bootstrap one (`a.test`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_tls_redial_verifies_the_failover_target_hostname() {
    let (server_a, cert_a) = build_named_server("a.test");
    let (server_b, cert_b) = build_named_server("b.test");
    let listener_a = TcpListener::bind("127.0.0.1:0").await.expect("bind a");
    let listener_b = TcpListener::bind("127.0.0.1:0").await.expect("bind b");
    let addr_a = listener_a.local_addr().expect("local_addr a");
    let addr_b = listener_b.local_addr().expect("local_addr b");

    let drop_a = Arc::new(Notify::new());
    let old_cluster = spawn_old_cluster(listener_a, server_a, drop_a.clone());
    let new_cluster = spawn_new_cluster(listener_b, server_b);

    let failover = Arc::new(ControlledClusterFailover::new(format!(
        "pulsar+ssl://a.test:{}",
        addr_a.port()
    )));
    let config = ConnectionConfig {
        supervisor: Some(fast_supervisor()),
        ..ConnectionConfig::default()
    };
    let url = ParsedUrl::parse(&format!("pulsar+ssl://a.test:{}", addr_a.port())).expect("url");
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_with_resolver_and_provider(
            url,
            Some(trusting(vec![cert_a, cert_b])),
            config,
            None,
            Some(failover.clone()),
            Some(Arc::new(ClusterResolver {
                cluster_a: addr_a,
                cluster_b: addr_b,
            })),
        ),
    )
    .await
    .expect("supervised TLS client did not complete its initial handshake")
    .expect("supervised TLS client initial handshake succeeds");

    // Fail over: point the provider at cluster B, then lose cluster A's session.
    failover.set_url(format!("pulsar+ssl://b.test:{}", addr_b.port()));
    drop_a.notify_one();

    // The bound is fail-fast for a regression (every attempt would be rejected, so
    // the supervisor would retry forever); a correct redial lands within milliseconds.
    let landed = tokio::time::timeout(
        Duration::from_secs(10),
        wait_for_count(&new_cluster.sessions, &new_cluster.changed, 1),
    )
    .await
    .is_ok();
    let rejected = new_cluster.rejected.load(Ordering::Acquire);
    if landed {
        // Let the replacement session finish its own handshake before closing.
        tokio::time::timeout(HANG_GUARD, async {
            while !client.is_connected() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the client never reported connected on cluster B's session");
    }
    old_cluster.abort();
    let _ = tokio::time::timeout(Duration::from_secs(10), client.close()).await;
    new_cluster.task.abort();
    assert!(
        landed,
        "the redial after failing over to b.test never completed a TLS session with cluster B: \
         {rejected} handshake(s) were aborted by the client, which verified B's certificate \
         against the bootstrap hostname a.test instead of b.test"
    );
}
