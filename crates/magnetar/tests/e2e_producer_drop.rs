// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the last-clone producer drop guard (issue #241).
//!
//! Production repro (otelgw): an LRU cache evicted producers by dropping
//! them. Magnetar producers had no `Drop` impl, so the broker kept the
//! `(topic, producer_name)` registration alive on the shared TCP
//! connection, and every recreation with the same user-provided name
//! (the hostname) failed forever with:
//!
//! ```text
//! NamingException: Producer with name 'X' is already connected to topic 'T'
//! ```
//!
//! With the drop guard, releasing the last clone pushes a best-effort
//! `CommandCloseProducer`; the close and the follow-up open ride the
//! same connection in order, so the recreate observes the freed name.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_producer_drop -- --nocapture
//! ```
//!
//! Requires Docker on the host. See `e2e_pulsar.rs` for the broker
//! container plumbing; this file uses the same image/wait strategy via
//! a local helper.

use std::time::Duration;

use magnetar::PulsarClient;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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

/// Start a Pulsar 4.x standalone container and return (`service_url`, `admin_url`,
/// `container_handle`).
async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
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
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// The production repro: drop a NAMED producer without close, then
/// recreate one with the same name on the same topic. Before the drop
/// guard, the second create failed forever with `NamingException`
/// (broker error code 16); with the guard, the best-effort
/// `CloseProducer` and the follow-up open ride the same connection in
/// order, so the recreate succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_dropped_named_producer_allows_same_name_recreate()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-drop");
    const PRODUCER_NAME: &str = "drop-guard-hostname";

    let producer = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    producer.send_bytes(b"before-drop".to_vec()).await?;

    // The otelgw LRU-eviction path: every reference released, no close().
    drop(producer);

    // Same name, same topic — the broker must have freed the
    // registration. The drop guard's CloseProducer precedes this open on
    // the same connection (FIFO), so no retry loop is needed.
    let recreated = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    recreated.send_bytes(b"after-drop".to_vec()).await?;
    recreated.close().await?;
    client.close().await;
    Ok(())
}

/// Baseline interaction: an explicit `close().await` followed by the
/// last-clone drop must not break the connection or the name — the
/// guard skips its duplicate close (slot already closed) and a
/// same-name recreate still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_explicit_close_then_drop_allows_same_name_recreate()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-close-drop");
    const PRODUCER_NAME: &str = "close-then-drop-hostname";

    let producer = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    producer.send_bytes(b"payload".to_vec()).await?;
    let clone = producer.clone();
    clone.close().await?;
    drop(producer); // guard must skip — slot already closed

    let recreated = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    recreated.send_bytes(b"after-close-drop".to_vec()).await?;
    recreated.close().await?;
    client.close().await;
    Ok(())
}
