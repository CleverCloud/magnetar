// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for the pluggable DNS resolver
//! (`ClientBuilder::dns_resolver` — ADR-0015).
//!
//! Spins up a real Pulsar 4.x standalone broker via `testcontainers-rs`,
//! wires a recording resolver that captures every `(host, port)` lookup
//! and delegates to tokio's built-in resolver, and confirms:
//!
//! 1. the resolver is invoked at least once for the broker host on initial connect, and
//! 2. a producer/consumer round-trip still works through the recorded resolutions.
//!
//! Mirrors Java's `ClientBuilder#dnsResolver(...)` integration coverage.
//!
//! Gated behind the `e2e` feature flag. Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_dns_resolver -- --nocapture
//! ```
//!
//! Requires Docker on the host. CI runs this only in the dedicated `e2e`
//! workflow.

#![cfg(feature = "e2e")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::runtime_tokio::{ClientError, DnsResolveFuture, DnsResolver};
use magnetar::{OutgoingMessage, PulsarClient};
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
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

async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Created namespace public/default"))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

/// Recording resolver — captures every `(host, port)` pair handed to it and
/// delegates the actual lookup to tokio's `lookup_host`. We re-implement
/// the delegation inline rather than wrapping `TokioDnsResolver` so the
/// recording layer can stay free of `Arc<dyn DnsResolver>` indirection.
#[derive(Debug, Default)]
struct RecordingResolver {
    /// Every `host:port` string passed to `resolve` is appended here.
    calls: Mutex<Vec<String>>,
}

impl RecordingResolver {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl DnsResolver for RecordingResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> DnsResolveFuture<'a> {
        let target = format!("{host}:{port}");
        self.calls.lock().push(target.clone());
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&target)
                .await
                .map_err(|e| {
                    ClientError::Other(format!("recording dns lookup_host({target}): {e}"))
                })?
                .collect();
            Ok(addrs)
        })
    }
}

/// Wire a custom recording resolver into the client, confirm it is invoked at
/// least once with the broker host, and round-trip a single message through
/// the resulting connection.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_custom_dns_resolver_invoked_and_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let resolver = Arc::new(RecordingResolver::new());

    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .dns_resolver(resolver.clone() as Arc<dyn DnsResolver>)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-dns-resolver";

    let producer = client.producer(topic).create().await?;
    let payload = b"dns-resolver-roundtrip".to_vec();
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-dns-resolver")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive()).await??;
    let received = msg.payload.to_vec();
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payload);

    let calls = resolver.snapshot();
    assert!(
        !calls.is_empty(),
        "custom DnsResolver must be invoked at least once on initial connect"
    );
    // Extract the host portion from the broker service URL ("pulsar://<host>:<port>")
    // and assert at least one recorded lookup targeted it. Testcontainers usually
    // returns `127.0.0.1` as the host, but we don't hard-code that — we match on
    // whatever the container reports.
    let expected_host = service_url
        .trim_start_matches("pulsar://")
        .split(':')
        .next()
        .expect("service_url has host part")
        .to_owned();
    assert!(
        calls
            .iter()
            .any(|c| c.starts_with(&format!("{expected_host}:"))),
        "expected at least one resolution for broker host `{expected_host}`, got {calls:?}",
    );
    Ok(())
}
