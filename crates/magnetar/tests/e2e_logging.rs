// SPDX-License-Identifier: Apache-2.0

//! ADR-0054 end-to-end logging capture against a real Apache Pulsar 4.x
//! standalone broker spun up via `testcontainers-rs` (ADR-0024 e2e layer
//! for the logging changeset).
//!
//! Runs as a regular test under `cargo test` (ADR-0046 — no `#[ignore]`,
//! no feature gate). Requires Docker on the host. Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_logging -- --nocapture
//! ```
//!
//! Captures everything the magnetar workspace crates log during a normal
//! connect + produce + consume round-trip and asserts:
//!
//! 1. the ADR-0054 lifecycle `info!` records appear — "connection established", "producer created",
//!    "consumer subscribed";
//! 2. the no-secrets rule holds end-to-end: the [`TokenAuth`] sentinel carried in
//!    `CommandConnect.auth_data` never appears in any captured record, nor do the defence-in-depth
//!    needles `"BEGIN PRIVATE KEY"` / `"client_secret"`.
//!
//! # Auth posture
//!
//! The standalone broker runs **unauthenticated** (no `AuthenticationProvider`
//! configured) and happily accepts a token-method `CommandConnect` — the
//! bearer bytes are stored on the session without validation (same posture
//! as `tests/e2e_oauth2.rs`). That is exactly what this test needs: a real
//! token sentinel flows through the engine's auth plumbing and onto the
//! wire, and the capture proves it never reaches the logs. Enabling
//! broker-side token auth would require minting a JWT against a configured
//! secret inside the container — extra moving parts with zero additional
//! log-capture coverage.
//!
//! # Why this file is its own integration-test binary with ONE test fn
//!
//! The capturing subscriber must be **global** (`fmt().init()`): the engine
//! driver runs on other tokio worker threads, so a thread-local
//! `set_default` guard would miss every driver-side event. A global
//! subscriber can be installed exactly once per process, so this file holds
//! a single test fn and shares the binary with no other test.
//!
//! The capture is scoped to the magnetar workspace crates (TRACE) with a
//! `warn` default for everything else: the ADR-0054 no-secrets rule governs
//! *our* logs, and capturing `bollard`/`hyper` Docker-API traffic at TRACE
//! would only bloat the sink without adding coverage.

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::auth::TokenAuth;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{AuthProvider, OutgoingMessage, PulsarClient};
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Sentinel token carried in `CommandConnect.auth_data` (and re-served by
/// the provider on any challenge refresh). Must never be logged.
const TOKEN_SENTINEL: &str = "SENTINEL-E2E-TOKEN-DO-NOT-LOG-9c4f";

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

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

/// Start an unauthenticated Pulsar 4.x standalone container and return
/// (`service_url`, `container_handle`). Mirrors `tests/e2e_pulsar.rs`,
/// minus the env-filter `init_tracing` — this binary installs its own
/// global capturing subscriber before the container starts.
async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_logging_lifecycle_records_present_and_secret_free()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "warn,magnetar=trace,magnetar_proto=trace,magnetar_runtime_tokio=trace,\
             magnetar_runtime_moonpool=trace,magnetar_admin=trace,magnetar_auth_oauth2=trace,\
             magnetar_auth_athenz=trace,magnetar_auth_sasl=trace",
        ))
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let (service_url, _container) = start_pulsar().await?;

    // Token-method CONNECT with a sentinel bearer: the sentinel flows
    // through `ClientBuilder::auth` → `provider.initial()` →
    // `CommandConnect.auth_data` (see `client_builder.rs::build`), so the
    // no-secrets assertion below covers the real end-to-end auth plumbing.
    let provider: Arc<dyn AuthProvider> = Arc::new(TokenAuth::from_string(TOKEN_SENTINEL));
    let client = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-logging";
    let producer = client.producer(topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"logging-e2e".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-logging")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(msg.payload.as_ref(), b"logging-e2e");
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    // ── Assertions on everything the magnetar crates logged ──
    let captured = sink.contents();
    assert!(
        !captured.is_empty(),
        "the capturing subscriber must have seen events",
    );
    // ADR-0054 lifecycle records, against the real broker.
    for needle in [
        "connection established",
        "producer created",
        "consumer subscribed",
    ] {
        assert!(
            captured.contains(needle),
            "captured logs must contain the lifecycle record {needle:?}:\n{captured}",
        );
    }
    // ADR-0054 no-secrets rule, end-to-end.
    for secret in [TOKEN_SENTINEL, "BEGIN PRIVATE KEY", "client_secret"] {
        assert!(
            !captured.contains(secret),
            "captured logs must never contain the secret {secret:?}:\n{captured}",
        );
    }
    Ok(())
}
