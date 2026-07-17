// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the producer `send_timeout` (ADR-0072), layer (e)
//! of the ADR-0024 five-layer policy.
//!
//! Pins the production contract that an in-flight `send()` whose
//! `CommandSendReceipt` never comes back fails with the synthetic timeout
//! sentinel (`code = -1`, message "send timeout") instead of hanging forever —
//! the user-visible behavior the Java-parity 30s default (ADR-0072) restores,
//! and the root-cause fix for moonpool seed `0x4402f874c43758d1` (a chaos
//! bit-flip corrupted a receipt's `sequence_id`, the receipt carries no CRC32C
//! so it was delivered-but-wrong, the matching send was dropped, and the
//! `send()` future hung forever because the old default was `None`).
//!
//! ## How "lost receipt" is simulated against a real broker
//!
//! A real Pulsar broker always answers `CommandSend` with a
//! `CommandSendReceipt`, so the lost-receipt path is unreachable by talking to
//! the broker directly. We interpose a loopback **black-hole gate** between the
//! client and the broker (same technique as `e2e_keepalive_watchdog.rs`):
//!
//! 1. Normal phase — the gate splices client↔broker bytes, so the handshake, producer-open, and a
//!    sanity round-trip all complete.
//! 2. Black-hole phase — the gate stops forwarding in BOTH directions on the live connection (bytes
//!    are read and dropped, the socket stays open). A `CommandSend` issued now reaches the gate but
//!    its `CommandSendReceipt` never returns to the client.
//! 3. The client's per-producer `send_timeout` fires after the configured window and resolves the
//!    `send()` future with the `code = -1, "send timeout"` sentinel.
//!
//! Reconnect is DISABLED on this client so the timeout fires cleanly: with the
//! auto-reconnect supervisor on, a redial would transparently replay the
//! in-flight send and mask the timeout. The keepalive watchdog recovery path is
//! covered separately by `e2e_keepalive_watchdog.rs`.
//!
//! Pairs with the proto unit tests (`conn.rs`), the runtime
//! `virtual_clock_driver_loop` integration tests, and the differential
//! `send_timeout_default_equivalence` test (ADR-0024).
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`, no
//! feature gate). Requires Docker + a reachable `apachepulsar/pulsar` image.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Short, explicit per-producer send timeout so the deadline fires quickly
/// inside the test budget (the 30s Java-parity default is correct for
/// production but too slow for a test).
const SEND_TIMEOUT: Duration = Duration::from_secs(3);

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
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    Ok((host, binary_port, container))
}

/// Copy bytes from `src` to `dst` while `!black_hole`; once `black_hole` flips
/// true, keep *reading* `src` (so the kernel buffer never fills and the socket
/// stays open) but **drop** everything — nothing reaches `dst`. Returns when
/// either side closes.
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
            // Black-hole: discard the bytes. A CommandSend issued during the
            // window reaches the gate but its receipt is never forwarded back.
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
/// the live connection and issue a send whose receipt can never return. The
/// per-producer `send_timeout` must fire and resolve the send with the
/// `code = -1, "send timeout"` sentinel — not hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_timeout_fires_when_receipt_lost() -> Result<(), Box<dyn std::error::Error>> {
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    let black_hole = Arc::new(AtomicBool::new(false));
    let gate_host_port = spawn_blackhole_gate(broker_host, broker_port, black_hole.clone()).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    // Reconnect intentionally DISABLED — a redial would replay the in-flight
    // send and mask the timeout we are asserting.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .operation_timeout(Duration::from_mins(1))
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-send-timeout";
    let producer = client
        .producer(topic)
        .send_timeout(SEND_TIMEOUT)
        .create()
        .await?;

    // Sanity round-trip before the black-hole so we know the session is live
    // and a healthy send resolves Ok well within the timeout window.
    producer
        .send(OutgoingMessage::with_payload(b"before-blackhole".to_vec()).into())
        .await?;

    // Black-hole the live connection: the CommandSend below reaches the gate
    // but its CommandSendReceipt is dropped and never returns to the client.
    tracing::info!("entering black-hole window");
    black_hole.store(true, Ordering::SeqCst);

    // The send must fail with the timeout sentinel PROMPTLY (within the 3s
    // send_timeout plus margin), not hang. The outer tokio timeout is the
    // no-hang guard — a regression (default `None` / sweep not firing) would
    // trip it instead of the inner send_timeout.
    let send_result = tokio::time::timeout(
        Duration::from_secs(30),
        producer.send(OutgoingMessage::with_payload(b"receipt-will-be-lost".to_vec()).into()),
    )
    .await
    .expect("the send must resolve via send_timeout, not hang past the no-hang guard");

    match send_result {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("timeout"),
                "the lost-receipt send must fail with a TIMEOUT error, got: {e}"
            );
        }
        Ok(message_id) => {
            panic!("send returned Ok({message_id:?}) despite its receipt being black-holed")
        }
    }

    // Heal the gate and tear down cleanly.
    black_hole.store(false, Ordering::SeqCst);
    let _ = producer.close().await;
    client.close().await;
    Ok(())
}
