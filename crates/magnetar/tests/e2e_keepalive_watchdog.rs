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
//! Pairs with the proto unit tests, the runtime `keepalive_watchdog`
//! integration tests, and the differential equivalence test (ADR-0024).
//!
//! ## Issue #370 / ADR-0083 — the write-deadline sibling scenario
//!
//! [`e2e_write_deadline_recovers_from_stalled_peer`] below is the e2e layer
//! for ADR-0083, a DIFFERENT failure shape than the black-hole scenario
//! above: `keepalive_watchdog` covers a peer that goes silent on READS
//! (never answers `PING`); the write-deadline scenario covers a peer that
//! accepts the connection and then stops draining our WRITES — exactly the
//! one-directional stall `keepalive_interval` cannot detect (it only
//! measures read-side liveness). It uses its own `spawn_stall_once_gate` /
//! `splice_with_stall` pair rather than the black-hole gate above: a
//! black-hole gate keeps READING the client's bytes and only drops them
//! (so the client's own socket write never backs up — it wouldn't
//! reproduce this bug at all); the stall gate genuinely stops reading from
//! the client-facing socket, so the client's own kernel send buffer fills.
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`, no
//! feature gate). Requires Docker + a reachable `apachepulsar/pulsar` image.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
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
        mandatory_stop: Duration::from_mins(2),
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
        .operation_timeout(Duration::from_mins(1))
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

/// Payload comfortably larger than the loopback TCP window the gate has
/// already been granted by the time it stops draining the client→broker
/// direction — window growth requires the receiver to actually read, which
/// never happens once the stall arms. Kept under any realistic
/// `max_message_size` so it is one frame, no chunking involved.
const STALLED_WRITE_PAYLOAD_BYTES: usize = 4 * 1024 * 1024 - 4096;

/// Bytes forwarded client→broker before the stall arms — generous headroom
/// over the CONNECT + PRODUCER open frames so both are safely through
/// before the gate stops reading.
const ARM_STALL_AFTER_BYTES: u64 = 4096;

/// Copies `src` → `dst` until EOF/error, OR — once `stall` is armed for
/// this connection AND `forwarded` crosses [`ARM_STALL_AFTER_BYTES`] —
/// parks forever without ever reading `src` again. The socket is neither
/// closed nor drained further: this is the genuine one-directional stall
/// issue #370 is about, DIFFERENT from `splice_with_blackhole` above
/// (which keeps reading and only drops bytes, so the peer's own socket
/// write never backs up and would not reproduce this bug at all).
async fn splice_with_stall<R, W>(mut src: R, mut dst: W, stall_this_connection: bool)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    let mut forwarded: u64 = 0;
    loop {
        if stall_this_connection && forwarded >= ARM_STALL_AFTER_BYTES {
            std::future::pending::<()>().await;
        }
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        forwarded += u64::try_from(n).unwrap_or(u64::MAX);
        if dst.write_all(&buf[..n]).await.is_err() {
            break;
        }
        if dst.flush().await.is_err() {
            break;
        }
    }
}

/// Spawn a gate that proxies client↔broker. The FIRST accepted connection
/// stalls its client→broker direction once [`ARM_STALL_AFTER_BYTES`] have
/// been forwarded (letting CONNECT + PRODUCER-open through first); every
/// LATER accepted connection (the supervisor's post-deadline redial)
/// proxies normally in both directions, so the replayed send can complete.
async fn spawn_stall_once_gate(
    broker_host: String,
    broker_port: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let accept_count = Arc::new(AtomicU32::new(0));

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            let is_first = accept_count.fetch_add(1, Ordering::SeqCst) == 0;
            let host = broker_host.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                let c2b = splice_with_stall(ri, wo, is_first);
                let b2c = splice_with_stall(ro, wi, false);
                tokio::join!(c2b, b2c);
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

/// Issue #370 / ADR-0083: a broker-side peer that accepts the connection
/// then stops draining the client's socket must not be able to wedge the
/// connection forever. A short `operation_timeout` bounds the write; once
/// it fires the connection is marked disconnected and the supervisor
/// redials through the gate's second (healthy) accepted connection, so a
/// send that started before the stall still eventually completes, and a
/// send issued after recovery succeeds normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_write_deadline_recovers_from_stalled_peer() -> Result<(), Box<dyn std::error::Error>> {
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    let gate_host_port = spawn_stall_once_gate(broker_host, broker_port).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .enable_reconnect(supervisor_for_e2e())
        // Short so the write-deadline expiry is reached quickly inside the
        // test budget; still comfortably longer than a normal round trip
        // against the local gate.
        .operation_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-write-deadline";
    let producer = client.producer(topic).create().await?;

    // Sanity round-trip before the stall so we know the session is live.
    producer
        .send(OutgoingMessage::with_payload(b"before-stall".to_vec()).into())
        .await?;

    // Oversized publish: parks mid-write once the gate's first connection
    // stops draining. Bounded by an outer harness timeout well past the
    // 3s operation_timeout + redial + replay budget, so a regression back
    // to "never resolves" fails loudly instead of hanging the suite.
    tracing::info!("issuing the stalled write");
    let big_payload = vec![0xABu8; STALLED_WRITE_PAYLOAD_BYTES];
    let stalled_outcome = tokio::time::timeout(
        Duration::from_secs(25),
        producer.send(OutgoingMessage::with_payload(big_payload).into()),
    )
    .await;
    match stalled_outcome {
        Ok(Ok(_message_id)) => {
            tracing::info!("stalled send recovered via write-deadline + redial + replay");
        }
        Ok(Err(e)) => {
            return Err(format!(
                "stalled send resolved with a terminal error instead of \
                 redial + replay (issue #370 regression): {e:?}"
            )
            .into());
        }
        Err(_elapsed) => {
            return Err(
                "stalled send never completed within the 25s harness budget — \
                         the driver write deadline did not fire (issue #370 regression)"
                    .into(),
            );
        }
    }

    // A send issued AFTER recovery, on the now-healthy redialled
    // connection, must also succeed normally.
    producer
        .send(OutgoingMessage::with_payload(b"after-stall".to_vec()).into())
        .await?;

    producer.close().await?;
    client.close().await;
    Ok(())
}
