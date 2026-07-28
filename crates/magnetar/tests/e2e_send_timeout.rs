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

// ---------------------------------------------------------------------------
// Issue #369 — send_timeout must surface for a publish RELOCATED across a
// supervised reconnect, instead of hanging for the whole reconnect budget.
//
// The scenario above disables reconnect on purpose ("a redial would
// transparently replay the in-flight send and mask the timeout") — that
// comment marks exactly the gap this second test closes: with the
// auto-reconnect supervisor ENABLED, the in-flight send survives the drop by
// being relocated into `Connection::in_flight_publish_snapshots`
// (`Connection::reset()`), and MUST still resolve via `send_timeout` well
// before the supervisor's much longer reconnect budget gives up.
// ---------------------------------------------------------------------------

/// Proxy client<->broker bytes for ONE connection, watching the
/// client-to-broker direction for a `CommandSend` frame. Every frame BEFORE
/// the send is forwarded untouched; the `CommandSend` frame itself (and
/// everything behind it) is dropped WITHOUT forwarding and the connection is
/// torn down immediately — the broker never sees the publish, so a
/// `CommandSendReceipt` is categorically impossible and cannot race the
/// timeout under test. Flips `send_seen` right before dropping so the
/// accept loop knows every connection from here on is a supervisor redial.
///
/// Applied to EVERY connection the gate proxies (not just "the first") — a
/// Pulsar client may legitimately open more than one physical connection to
/// the same broker address before the producer's operational connection
/// carries any `CommandSend` (e.g. a separate bootstrap/lookup leg), and
/// hard-coding "only the first connection is real" black-holed those
/// legitimate legs and made `open_producer` hang on `operation_timeout`
/// instead of exercising the scenario under test.
async fn proxy_until_send_then_drop(
    inbound: TcpStream,
    outbound: TcpStream,
    send_seen: Arc<AtomicBool>,
) {
    let (mut ri, mut wi) = inbound.into_split();
    let (mut ro, mut wo) = outbound.into_split();

    let c2b = async move {
        let mut pending = bytes::BytesMut::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match ri.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            pending.extend_from_slice(&buf[..n]);
            loop {
                let before_len = pending.len();
                let mut probe = pending.clone().freeze();
                let frame = match magnetar_proto::decode_one(&mut probe) {
                    Ok(f) => f,
                    Err(magnetar_proto::FrameError::Incomplete { .. }) => break,
                    Err(_) => return,
                };
                let consumed = before_len - probe.len();
                let is_send =
                    frame.command.r#type == magnetar_proto::pb::base_command::Type::Send as i32;
                let frame_bytes = pending.split_to(consumed);
                if is_send {
                    // Mid-publish drop: never forward the Send frame, close now.
                    send_seen.store(true, Ordering::SeqCst);
                    return;
                }
                if wo.write_all(&frame_bytes).await.is_err() {
                    return;
                }
                if wo.flush().await.is_err() {
                    return;
                }
            }
        }
    };
    let b2c = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match ro.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if wi.write_all(&buf[..n]).await.is_err() {
                return;
            }
            if wi.flush().await.is_err() {
                return;
            }
        }
    };
    tokio::select! {
        () = c2b => {}
        () = b2c => {}
    }
    // Whichever direction finished first, dropping the other future here
    // closes its half of both sockets — the connection is fully torn down.
}

/// Gate for issue #369: every inbound connection proxies transparently to
/// the real broker (via [`proxy_until_send_then_drop`]) UNTIL the first
/// `CommandSend` frame is observed on ANY connection — at that point the
/// carrying connection drops (the broker never sees the publish) and the
/// shared `send_seen` flag flips. Every connection accepted AFTER that
/// point is a supervisor redial: accepted but never proxied to the broker
/// at all (a loopback black hole), so the handshake never completes and the
/// supervisor stays "trying" for as long as its (deliberately generous)
/// attempt budget allows, while the relocated send's `send_timeout` is what
/// this test expects to fire first.
async fn spawn_drop_on_send_then_blackhole_gate(
    broker_host: String,
    broker_port: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let send_seen = Arc::new(AtomicBool::new(false));

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            if send_seen.load(Ordering::SeqCst) {
                tokio::spawn(async move {
                    let _inbound = inbound;
                    std::future::pending::<()>().await;
                });
                continue;
            }
            let host = broker_host.clone();
            let flag = send_seen.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                proxy_until_send_then_drop(inbound, outbound, flag).await;
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

fn generous_supervisor() -> magnetar_proto::SupervisorConfig {
    magnetar_proto::SupervisorConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        // Comfortably longer than SEND_TIMEOUT — the supervisor must still
        // be trying (not have given up) when the send-timeout sweep fires,
        // proving the fix (not a give-up / fail_all_pending path) resolved
        // the send.
        mandatory_stop: Duration::from_mins(2),
        max_attempts: Some(10_000),
        anti_thrash_threshold: None,
        drop_grace: Duration::from_millis(500),
        max_backoff_after_thrash: Duration::from_millis(200),
    }
}

/// Issue #369 e2e acceptance test: with the auto-reconnect supervisor
/// ENABLED, a publish relocated by `Connection::reset()` across a supervised
/// reconnect surfaces its configured `send_timeout` error, instead of
/// parking for the supervisor's entire (much longer) reconnect budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_timeout_fires_for_publish_relocated_across_supervised_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    let gate_host_port = spawn_drop_on_send_then_blackhole_gate(broker_host, broker_port).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    // Reconnect intentionally ENABLED — this is the gap the scenario above
    // does NOT cover.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .operation_timeout(Duration::from_mins(1))
        .enable_reconnect(generous_supervisor())
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-send-timeout-reconnect";
    let producer = client
        .producer(topic)
        .send_timeout(SEND_TIMEOUT)
        .create()
        .await?;

    // No sanity send before this one: the gate drops the connection on the
    // FIRST `CommandSend` frame it ever observes (see
    // `spawn_drop_on_send_then_blackhole_gate`), so an earlier sanity
    // publish would itself trigger the drop instead of this one.
    // `open_producer`'s already-successful `CommandProducerSuccess`
    // round-trip is sufficient proof the connection is live and healthy.
    let send_started = std::time::Instant::now();
    // The gate drops the connection the instant it sees this CommandSend,
    // then black-holes every subsequent redial. The send must still resolve
    // via `send_timeout` well before the supervisor's 120s / 10_000-attempt
    // budget could plausibly be exhausted. The outer tokio timeout is a
    // generous no-hang guard, not the assertion itself.
    let send_result = tokio::time::timeout(
        Duration::from_secs(30),
        producer.send(OutgoingMessage::with_payload(b"relocated-across-reconnect".to_vec()).into()),
    )
    .await
    .expect("the relocated send must resolve well before the supervisor's reconnect budget");
    let elapsed = send_started.elapsed();

    match send_result {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("timeout"),
                "the relocated send must fail with a TIMEOUT error, got: {e}"
            );
        }
        Ok(message_id) => {
            panic!(
                "send returned Ok({message_id:?}) despite the connection being dropped mid-publish"
            )
        }
    }
    assert!(
        elapsed < Duration::from_secs(20),
        "send-timeout must fire on roughly the {SEND_TIMEOUT:?} deadline, not the \
         supervisor's reconnect budget (elapsed={elapsed:?})"
    );

    client.close().await;
    Ok(())
}
