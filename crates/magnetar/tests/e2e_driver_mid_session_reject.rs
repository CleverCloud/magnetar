// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the driver re-entrant-mutex deadlock fix
//! (ADR-0038), layer (e) of the ADR-0024 four-layer policy.
//!
//! Pins the production contract that a client whose live broker connection
//! receives an **unexpected / malformed frame mid-session** rides it out:
//! the driver surfaces the framing reject, the supervisor re-dials and
//! replays, and the client stays usable — instead of the driver task
//! self-deadlocking on the re-entrant `shared.inner` `parking_lot::Mutex`
//! and wedging every future behind it.
//!
//! This is the e2e analogue of:
//!
//! - the proto unit test (`handle_bytes_owned` rejects a `total_size=0` frame),
//! - the tokio + moonpool runtime tests (driver terminates with the reject, no deadlock), and
//! - the differential test (both engines reject identically via shared proto).
//!
//! ## How "a malformed mid-session frame" is forced against a real broker
//!
//! A real Pulsar broker only ever speaks valid protocol, so we interpose a
//! local **poison gate** between client and broker. The gate splices bytes
//! both ways for every connection (so the handshake, lookups, and a full
//! produce/consume round-trip all work), but on a one-shot
//! [`tokio::sync::Notify`] signal it injects a single malformed frame — a
//! 4-byte big-endian `total_size = 0` prefix — toward the client on the
//! currently-live connection. The client's `peek_full_frame_len` rejects
//! it with `BadLength(0)`, driving the driver's error arm (the deadlock
//! site). With auto-reconnect enabled (`enable_reconnect`, the production
//! config), the supervisor re-dials and the client completes a second
//! round-trip — the proof the driver terminated the reject instead of
//! self-deadlocking.
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`,
//! no feature gate). Requires Docker + a reachable `apachepulsar/pulsar`
//! image.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

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

/// Start a Pulsar 4.x standalone container; return (`broker_host`,
/// `broker_port`, `container_handle`). Dropping the guard stops the broker.
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

/// Splice one client↔broker connection. `client → broker` is a plain copy.
/// `broker → client` forwards normally but, on the first `poison` signal
/// won globally (guarded by `already_poisoned`), injects one malformed
/// frame (`total_size = 0`) toward the client before resuming the forward.
async fn splice_with_poison(
    inbound: TcpStream,
    outbound: TcpStream,
    poison: Arc<Notify>,
    already_poisoned: Arc<AtomicBool>,
) {
    let (mut ri, mut wi) = inbound.into_split();
    let (mut ro, mut wo) = outbound.into_split();

    let c2b = async move {
        let _ = tokio::io::copy(&mut ri, &mut wo).await;
    };

    let b2c = async move {
        let mut buf = vec![0u8; 16 * 1024];
        let notified = poison.notified();
        tokio::pin!(notified);
        let mut fired = false;
        loop {
            tokio::select! {
                () = &mut notified, if !fired => {
                    fired = true;
                    // One-shot across the whole gate: only the first
                    // connection to win the signal injects the bad frame.
                    if !already_poisoned.swap(true, Ordering::SeqCst) {
                        if wi.write_all(&[0u8; 4]).await.is_err() {
                            break;
                        }
                        let _ = wi.flush().await;
                    }
                }
                r = ro.read(&mut buf) => {
                    match r {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if wi.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            let _ = wi.flush().await;
                        }
                    }
                }
            }
        }
    };

    tokio::join!(c2b, b2c);
}

/// Bind a loopback gate that splices every connection through to the real
/// broker and injects one malformed frame on the `poison` signal. Returns
/// the gate `host:port` the client should dial.
async fn spawn_poison_gate(
    broker_host: String,
    broker_port: u16,
    poison: Arc<Notify>,
) -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let already_poisoned = Arc::new(AtomicBool::new(false));

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            let host = broker_host.clone();
            let poison = poison.clone();
            let already_poisoned = already_poisoned.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                splice_with_poison(inbound, outbound, poison, already_poisoned).await;
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

/// One full produce → consume round-trip on a fresh topic/subscription.
/// Each call uses a distinct `tag` so the two round-trips in the test are
/// independent (no cursor reuse across them).
async fn round_trip(client: &PulsarClient, tag: &str) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("persistent://public/default/magnetar-e2e-mid-session-reject-{tag}");
    let payload = format!("payload-{tag}").into_bytes();

    let producer = client.producer(topic.clone()).create().await?;
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription(format!("sub-{tag}"))
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = consumer.receive().await?;
    assert_eq!(msg.payload.to_vec(), payload);
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    Ok(())
}

/// Build a client through the poison gate, prove it healthy with a baseline
/// round-trip, inject a malformed frame mid-session on the now-idle control
/// connection, and assert a second round-trip still succeeds — the
/// production proof that the driver surfaced the reject and the supervisor
/// reconnected rather than self-deadlocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_client_survives_malformed_mid_session_frame() -> Result<(), Box<dyn std::error::Error>>
{
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    let poison = Arc::new(Notify::new());
    let gate_host_port = spawn_poison_gate(broker_host, broker_port, poison.clone()).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    // `enable_reconnect` wires the supervisor so a mid-session drop is
    // auto-recovered (the production auto-reconnect config). Without it the
    // driver still surfaces the reject cleanly (the deadlock fix), but
    // nothing re-dials — recovery is exactly what the supervisor provides.
    let client = tokio::time::timeout(
        Duration::from_secs(40),
        PulsarClient::builder()
            .service_url(service_url)
            .enable_reconnect(SupervisorConfig::default())
            .build(),
    )
    .await
    .expect("client build must not exceed the test guard")
    .expect("client must connect through the gate");

    // Baseline: prove the gated client is fully healthy before any fault.
    tokio::time::timeout(Duration::from_secs(40), round_trip(&client, "baseline"))
        .await
        .expect("baseline round-trip must not hang")
        .expect("baseline round-trip must succeed");

    // The producer/consumer are closed, so the client is now idle on the
    // gated control connection. Inject the malformed frame mid-session: the
    // driver must reject it (`BadLength`), disconnect, and let the
    // supervisor re-dial — NOT self-deadlock. A pre-fix driver would wedge
    // the read loop here, and the recovery round-trip below would hang
    // until the test guard fires.
    poison.notify_one();

    // The reject resets the bootstrap connection; the supervisor re-dials.
    // A request that was already in flight at reset surfaces `SessionLost`
    // (correct: in-flight ops fail on reset, the caller retries), so the
    // *first* post-poison round-trip may race the reset and fail fast.
    // Retry until the reconnected session serves it — the claim under test
    // is that the client RECOVERS (the driver terminated the reject instead
    // of self-deadlocking), not that the very first request survives the
    // reset. Each attempt is timeout-guarded so a *deadlocked* driver (the
    // pre-fix bug) still surfaces here: every attempt would hang, the outer
    // deadline would fire, and the test fails loudly.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    let mut last_err: Option<String> = None;
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "client did not recover from the mid-session reject in time \
             (driver deadlock?); last error: {last_err:?}",
        );
        match tokio::time::timeout(Duration::from_secs(15), round_trip(&client, "recovered")).await
        {
            Ok(Ok(())) => break,
            Ok(Err(e)) => {
                // Transient (e.g. SessionLost on the in-flight request) —
                // let the reconnect settle and retry.
                last_err = Some(format!("{e}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(_elapsed) => {
                // A single attempt hung — the pre-fix deadlock signature.
                // Record and let the outer deadline gate the failure.
                last_err = Some("round-trip attempt hung (possible driver deadlock)".to_owned());
            }
        }
    }

    client.close().await;
    Ok(())
}
