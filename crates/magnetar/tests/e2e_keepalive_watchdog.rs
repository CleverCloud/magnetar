// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the progress-based keepalive watchdog
//! (ADR-0058), layer (e) of the ADR-0024 four-layer policy.
//!
//! Pins the production contract that a connection whose peer goes **silent
//! mid-session** — answering nothing, not even keepalive pings — is detected
//! and failed by the client-side watchdog (after two missed keepalive
//! intervals) rather than wedging forever, so the supervised driver redials
//! and the user-facing producer keeps working.
//!
//! ## How "silent peer" is simulated against a real broker
//!
//! A real Pulsar broker always answers `PING` with `PONG`, so the wedge is
//! unreachable by talking to the broker directly. We interpose a loopback
//! **black-hole gate** between the client and the broker:
//!
//! 1. Normal phase — the gate splices client↔broker bytes, so the handshake, producer-open, and a
//!    sanity round-trip all complete.
//! 2. Black-hole phase — for [`BLACKHOLE_WINDOW`] the gate stops forwarding in BOTH directions on
//!    the live connection (bytes are read and dropped). The TCP socket stays *open* — this is a
//!    half-open / black-holed peer, exactly the desync class that the pre-ADR-0058 watchdog wedged
//!    on. The client's keepalive ping is sent but never answered.
//! 3. Recovery — with the watchdog fix the client fails the connection after two missed keepalive
//!    intervals and the supervisor redials; the gate accepts the fresh connection and proxies it
//!    normally again, so a post-black-hole `send()` succeeds.
//!
//! The client runs with a short [`KEEPALIVE`] interval and the
//! auto-reconnect supervisor enabled. Before ADR-0058 the chatty/half-open
//! black-hole reset the keepalive baseline (or simply re-pinged forever), so
//! the post-black-hole `send()` would hang until the test budget expired —
//! the regression this test guards.
//!
//! Pairs with the proto unit tests, the runtime keepalive_watchdog
//! integration tests, and the differential equivalence test (ADR-0024).
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`, no
//! feature gate). Requires Docker + a reachable `apachepulsar/pulsar` image.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Short keepalive so two missed intervals (the watchdog escalation
/// threshold) elapse quickly inside the test budget.
const KEEPALIVE: Duration = Duration::from_secs(1);

/// How long the gate black-holes the live connection — comfortably more than
/// two keepalive intervals, so the watchdog must escalate during the window.
const BLACKHOLE_WINDOW: Duration = Duration::from_secs(4);

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

async fn start_pulsar()
-> Result<(String, u16, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    Ok((host, binary_port, container))
}

/// Generous reconnect budget — once the watchdog fails the black-holed
/// connection, the supervisor must redial and re-handshake against the gate.
fn supervisor_for_e2e() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(2),
        mandatory_stop: Duration::from_secs(120),
        max_attempts: None,
        ..SupervisorConfig::default()
    }
}

/// Copy bytes from `src` to `dst` while `!black_hole`; once `black_hole`
/// flips true, keep *reading* `src` (so the kernel buffer never fills and the
/// socket stays open) but **drop** everything — nothing reaches `dst`. Returns
/// when either side closes.
async fn splice_with_blackhole<R, W>(mut src: R, mut dst: W, black_hole: Arc<AtomicBool>)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if black_hole.load(Ordering::SeqCst) {
            // Black-hole: discard the bytes. The peer's keepalive ping is
            // read and dropped — never answered.
            continue;
        }
        if dst.write_all(&buf[..n]).await.is_err() {
            break;
        }
        if dst.flush().await.is_err() {
            break;
        }
    }
}

/// Spawn a gate that proxies client↔broker. The returned `host:port` is what
/// the client dials. The shared `black_hole` flag, when set, makes every live
/// connection through the gate stop forwarding (in both directions) while
/// keeping the socket open.
async fn spawn_blackhole_gate(
    broker_host: String,
    broker_port: u16,
    black_hole: Arc<AtomicBool>,
) -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            let host = broker_host.clone();
            let bh = black_hole.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                let c2b = splice_with_blackhole(ri, wo, bh.clone());
                let b2c = splice_with_blackhole(ro, wi, bh.clone());
                tokio::join!(c2b, b2c);
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

/// Establish a connection through the gate, sanity round-trip, then black-hole
/// the live connection long enough that the keepalive watchdog must escalate.
/// After the black-hole window the supervisor redials through the (now
/// healthy) gate and a fresh `send()` succeeds — proving the watchdog failed
/// the wedged connection instead of pinging it forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_keepalive_watchdog_recovers_from_silent_peer() -> Result<(), Box<dyn std::error::Error>>
{
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    let black_hole = Arc::new(AtomicBool::new(false));
    let gate_host_port = spawn_blackhole_gate(broker_host, broker_port, black_hole.clone()).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .keepalive(KEEPALIVE)
        .enable_reconnect(supervisor_for_e2e())
        .operation_timeout(Duration::from_secs(60))
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-keepalive-watchdog";
    let producer = client.producer(topic).create().await?;

    // Sanity round-trip before the black-hole so we know the session is live.
    producer
        .send(OutgoingMessage::with_payload(b"before-blackhole".to_vec()).into())
        .await?;

    // Black-hole the live connection. The keepalive ping the client sends will
    // be read by the gate and dropped — never answered. After two missed
    // intervals the watchdog must fail the connection (ADR-0058).
    tracing::info!("entering black-hole window");
    black_hole.store(true, Ordering::SeqCst);
    tokio::time::sleep(BLACKHOLE_WINDOW).await;
    // Heal the gate: the next supervised redial proxies normally again.
    tracing::info!("leaving black-hole window; gate healthy again");
    black_hole.store(false, Ordering::SeqCst);

    // A post-black-hole send must succeed: the watchdog failed the wedged
    // connection, the supervisor reconnected through the healed gate, and the
    // producer was transparently rebuilt. Bounded retries so a regression
    // (watchdog never fired → connection wedged) fails fast instead of hanging.
    let payload = b"after-blackhole".to_vec();
    let mut attempts = 0u32;
    let send_outcome: Result<(), Box<dyn std::error::Error>> = loop {
        attempts += 1;
        if attempts > 30 {
            break Err("post-black-hole send never completed — keepalive watchdog \
                       did not fail the wedged connection (ADR-0058 regression)"
                .into());
        }
        match tokio::time::timeout(
            Duration::from_secs(10),
            producer.send(OutgoingMessage::with_payload(payload.clone()).into()),
        )
        .await
        {
            Ok(Ok(_message_id)) => break Ok(()),
            Ok(Err(e)) => {
                tracing::info!(?e, attempts, "post-black-hole send retry");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_elapsed) => {
                tracing::info!(attempts, "post-black-hole send attempt timed out; retrying");
            }
        }
    };
    send_outcome?;

    producer.close().await?;
    client.close().await;
    Ok(())
}
