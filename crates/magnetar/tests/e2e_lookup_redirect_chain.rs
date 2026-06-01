// SPDX-License-Identifier: Apache-2.0

//! E2E regression: HIGH-4 (lookup multi-agent review) — a normal
//! (non-redirecting) LOOKUP against a real Apache Pulsar 4.x standalone
//! broker still resolves cleanly. The HIGH-4 fix changes how the proto
//! state machine delivers redirect-chain outcomes (only terminal Connect /
//! Failed reach the user-facing future); this test pins the no-op happy
//! path so the change doesn't regress single-broker LOOKUP semantics.
//!
//! ADR-0024 layer 5 of the HIGH-4 fix. The other layers (proto unit,
//! tokio integration, moonpool integration, differential equivalence)
//! live alongside this file in their respective crates — search for
//! `lookup_redirect_chain` to find the siblings.
//!
//! Strategy: stand up a standalone Pulsar 4.x container, open a producer
//! (which issues a LOOKUP before the producer round-trip — see
//! `magnetar-runtime-tokio/src/client.rs::lookup_topic`). The single
//! broker resolves the LOOKUP to itself in one round-trip (no redirects),
//! so the chain-handling code path collapses to the trivial case. If
//! HIGH-4 regressed the terminal-outcome delivery, this would either hang
//! on `wait_producer_ready` or surface a state-machine bug error from
//! `lookup_topic`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker
//! on the host with `apachepulsar/pulsar:4.0.4` reachable.

use std::time::Duration;

use magnetar::PulsarClient;
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

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_single_broker_lookup_still_resolves_after_high4_fix()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Opening a producer issues a LOOKUP under the hood (single-broker
    // resolves immediately, no redirects). If the HIGH-4 terminal-outcome
    // delivery regressed, `producer().create()` would either hang on the
    // LOOKUP (no terminal outcome ever delivered to the anchor) or
    // surface the "BUG: intermediate Redirected leaked …" error from the
    // engines' exhaustive match. Either way this assertion catches it.
    let topic = "persistent://public/default/magnetar-e2e-lookup-redirect-chain";
    let producer = tokio::time::timeout(Duration::from_secs(15), client.producer(topic).create())
        .await
        .expect("producer().create() must not hang on single-broker LOOKUP")?;
    producer.close().await?;
    Ok(())
}
