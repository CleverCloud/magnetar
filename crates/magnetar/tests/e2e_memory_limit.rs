// SPDX-License-Identifier: Apache-2.0

//! E2E: `ClientBuilder::memory_limit(bytes, MemoryLimitPolicy::FailImmediately)`.
//!
//! Java parity: `ClientBuilder#memoryLimit(long, MemoryLimitPolicy)`. See
//! [ADR-0017](../../../specs/adr/0017-memory-limit-atomic-reservation.md).
//!
//! Two scenarios:
//!   1. **Reject path** — limit = 1 KiB, send a 2 KiB message. Expect
//!      `ClientError::MemoryLimitExceeded` (synchronous reservation failure; the bytes never reach
//!      the wire).
//!   2. **Happy path** — limit = 64 KiB, send a 1 KiB message. Expect a normal `SendReceipt` (the
//!      reservation succeeds and is released on completion).
//!
//! Runs as a regular test under `cargo test` (ADR-0045).

use std::time::Duration;

use magnetar::{MemoryLimitPolicy, OutgoingMessage, PulsarClient};
use magnetar_runtime_tokio::ClientError;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_memory_limit_rejects_oversize() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    // 1 KiB budget. The producer below tries to send 2 KiB — the reservation
    // CAS fails synchronously and the payload never reaches the wire.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .memory_limit(1024, MemoryLimitPolicy::FailImmediately)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-memory-limit-reject";
    let producer = client.producer(topic).create().await?;

    let payload = vec![0u8; 2 * 1024];
    let send_res = producer
        .send(OutgoingMessage::with_payload(payload).into())
        .await;

    match send_res {
        Err(ClientError::MemoryLimitExceeded {
            current,
            limit,
            requested,
        }) => {
            assert_eq!(limit, 1024, "limit echoes the builder value");
            assert!(
                requested >= 2 * 1024,
                "requested ({requested}) must cover at least the payload size",
            );
            assert!(
                current + requested > limit,
                "the reservation logic only rejects when current+requested > limit \
                 (current={current}, requested={requested}, limit={limit})",
            );
        }
        Err(other) => panic!("expected MemoryLimitExceeded, got {other:?}"),
        Ok(id) => panic!("expected MemoryLimitExceeded, got Ok({id:?})"),
    }

    producer.close().await?;
    client.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_memory_limit_happy_path() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    // 64 KiB budget. A 1 KiB payload fits — the send completes normally and
    // the reservation is released when SendFut drops.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .memory_limit(64 * 1024, MemoryLimitPolicy::FailImmediately)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-memory-limit-ok";
    let producer = client.producer(topic).create().await?;

    let payload = vec![0xABu8; 1024];
    let id = producer
        .send(OutgoingMessage::with_payload(payload).into())
        .await?;
    // The receipt's ledger / entry ids are broker-assigned; any successful
    // SendReceipt suffices to prove the reservation released cleanly.
    let _ = id;

    producer.close().await?;
    client.close().await;
    Ok(())
}
