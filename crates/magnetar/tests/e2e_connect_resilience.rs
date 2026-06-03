// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the dual-cap initial-dial retry (ADR-0052),
//! layer (e) of the ADR-0024 four-layer policy.
//!
//! Pins the production contract that a client started **before its broker
//! is reachable** rides out the early connection failures and connects
//! once the broker comes up — instead of failing fast on the first
//! refusal. This is the e2e analogue of:
//!
//! - the moonpool resilience sweep (`connect_resilience.rs`: a connect-hang is recovered or
//!   bounded),
//! - the tokio retry-then-resolve integration test, and
//! - the proto unit test pinning the `connect_max_retries = 8` / `operation_timeout = 30 s`
//!   dual-cap defaults.
//!
//! ## How "broker not yet reachable" is simulated against a real broker
//!
//! testcontainers only hands back the broker's mapped host port once the
//! container is up, so we cannot point the client at a real-but-down
//! broker directly. Instead we reserve a local loopback **gate port** and
//! point the client at it, but bring the gate up only **after** a delay:
//!
//! - For the first [`GATE_REFUSAL_WINDOW`] **nothing is listening on the gate port**, so the
//!   client's dial gets `ConnectionRefused` — a clean transient `Io` error, the exact "broker not
//!   reachable yet" condition.
//! - After the window we bind the gate and transparently splice client↔broker bytes for every
//!   connection (the initial dial, and any lookup re-dial the client makes against the same gate
//!   `host:port`), so the handshake and a full produce/consume round-trip complete.
//!
//! The client uses the default [`magnetar_proto::ConnectionConfig`]
//! dual cap (`connect_timeout=10s`, `connect_max_retries=8`,
//! `operation_timeout=30s`), so it re-dials across the refusal window and
//! succeeds once the gate binds. No retry config is set on the builder —
//! the defaults are the whole point.
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`,
//! no feature gate). Requires Docker + a reachable
//! `apachepulsar/pulsar` image.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::{TcpListener, TcpStream};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// How long the gate port stays unbound (dials get `ConnectionRefused`)
/// before the gate binds and starts proxying to the real broker. Spans
/// several of the client's 50 ms-doubling connect-retry backoff steps,
/// well inside the 8-retry / 30 s dual cap.
const GATE_REFUSAL_WINDOW: Duration = Duration::from_secs(2);

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

/// Reserve a loopback gate port (bind then drop — the port stays free on
/// loopback for the brief window before we re-bind it), and after
/// [`GATE_REFUSAL_WINDOW`] bind it for real and splice client↔broker.
///
/// Returns the gate `host:port` the client should dial. During the
/// refusal window the port is unbound, so the client's dial gets a clean
/// `ConnectionRefused` and re-dials under the dual cap.
async fn spawn_connect_gate(
    broker_host: String,
    broker_port: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    // Reserve an ephemeral port, then release it so the gate port is
    // *closed* (ConnectionRefused) until the delayed bind below.
    let probe = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = probe.local_addr()?;
    drop(probe);

    tokio::spawn(async move {
        // Refusal window: leave the gate port unbound so dials are refused.
        tokio::time::sleep(GATE_REFUSAL_WINDOW).await;

        // Bind the gate and splice every connection (the initial dial plus
        // any lookup re-dial against the same gate host:port) through to
        // the real broker.
        let Ok(listener) = TcpListener::bind(gate_addr).await else {
            return;
        };
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            let host = broker_host.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                let (mut ri, mut wi) = inbound.into_split();
                let (mut ro, mut wo) = outbound.into_split();
                let c2b = tokio::io::copy(&mut ri, &mut wo);
                let b2c = tokio::io::copy(&mut ro, &mut wi);
                let _ = tokio::join!(c2b, b2c);
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_client_retries_until_broker_reachable() -> Result<(), Box<dyn std::error::Error>> {
    let (broker_host, broker_port, _container) = start_pulsar().await?;

    // Point the client at the gate, which refuses for the first 2 s.
    let gate_host_port = spawn_connect_gate(broker_host, broker_port).await?;
    let service_url = format!("pulsar://{gate_host_port}");

    // The connect is issued NOW — while the gate is still refusing. With
    // the default dual-cap retry the build()/connect rides out the
    // refusal window and succeeds once the gate opens. The 30 s outer
    // bound is the test guard; the dual cap (8 retries / 30 s) is what
    // actually carries the connect through.
    let client = tokio::time::timeout(
        Duration::from_secs(40),
        PulsarClient::builder().service_url(service_url).build(),
    )
    .await
    .expect("client build must not exceed the test guard")
    .expect("client must connect once the gate opens (dual-cap retry path)");

    // Prove the connection is actually usable end-to-end, not just TCP-up.
    let topic = "persistent://public/default/magnetar-e2e-connect-resilience";
    let producer = client.producer(topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"after-retry".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-connect-resilience")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = consumer.receive().await?;
    assert_eq!(msg.payload.to_vec(), b"after-retry".to_vec());
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    Ok(())
}
