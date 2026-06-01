// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the producer batching and chunking surfaces, modelled
//! after Apache Pulsar's `BatchMessageTest`, `ConsumerBatchReceiveTest` and
//! `MessageChunkingTest`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_batch_chunk -- --nocapture
//! ```
//!
//! Requires Docker on the host. See `e2e_pulsar.rs` for the broker container
//! plumbing; this file uses the same image/wait strategy via a local helper.
//!
//! PIP-37 (Large Message Size) requires producer chunking + batching disabled
//! (chunks-never-batched). The chunked round-trip below mirrors that constraint.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
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

/// Producer with `batching(max_msgs=5, max_bytes=1 MiB)` and a generous delay
/// (1 minute) so the batch can only flush on the message-count cap. Sends 5
/// messages and verifies the consumer receives all 5 in order. Mirrors Java
/// `BatchMessageTest` (`batchingMaxMessages` triggering a flush).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_batching_flushes_on_max_msgs() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-batch-maxmsgs");

    let producer = client
        .producer(&topic)
        .batching(5, 1_000_000)
        .batching_max_publish_delay(Duration::from_secs(60))
        .create()
        .await?;
    let payloads: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("batch-msg-{i}").into_bytes())
        .collect();
    // Sequential await would never fill the batch (each send would wait on a
    // receipt that arrives only after a flush). Mirror Java
    // `BatchMessageTest`'s "fire all sendAsync, then join" pattern: enqueue
    // every message before awaiting any, so the 5th send fills the batch and
    // the broker emits one batched receipt that resolves all five futures.
    let send_futures: Vec<_> = payloads
        .iter()
        .map(|p| producer.send(OutgoingMessage::with_payload(p.clone()).into()))
        .collect();
    for fut in send_futures {
        fut.await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-batch-maxmsgs")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payloads);
    Ok(())
}

/// Consumer `receive_batch_with_bytes_cap(count=5, bytes=1 MiB)` mirrors Java's
/// `BatchReceivePolicy`: the call returns at most 5 messages even when 10 are
/// available, and a second call drains the remainder. Modelled after
/// `ConsumerBatchReceiveTest`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_consumer_batch_receive() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-batchrecv");

    let producer = client.producer(&topic).create().await?;
    let payloads: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("recv-msg-{i:02}").into_bytes())
        .collect();
    for p in &payloads {
        producer
            .send(OutgoingMessage::with_payload(p.clone()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-batchrecv")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received: Vec<Vec<u8>> = Vec::with_capacity(payloads.len());
    while received.len() < payloads.len() {
        let batch = consumer
            .receive_batch_with_bytes_cap(5, 1_000_000, Duration::from_secs(10))
            .await?;
        assert!(
            batch.len() <= 5,
            "batch should respect count cap of 5, got {}",
            batch.len()
        );
        assert!(!batch.is_empty(), "batch receive timed out before drain");
        for msg in batch {
            received.push(msg.payload.to_vec());
            consumer.ack(msg.message_id).await?;
        }
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payloads);
    Ok(())
}

/// Producer with `chunking(true)` + batching disabled splits an oversize payload
/// (~6 MiB, above the default 5 MiB `max_message_size`) into chunks; the
/// consumer reassembles them into a single `IncomingMessage` whose payload
/// matches the original length. Mirrors PIP-37 / Java `MessageChunkingTest`.
///
/// Only the length is asserted — per-byte comparison would dominate test wall
/// time without adding signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_chunked_message_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-chunk");

    // Chunks-never-batched: disable batching explicitly even though it's the
    // default — makes the constraint visible at the call site.
    let producer = client
        .producer(&topic)
        .chunking(true)
        .batching(0, 0)
        .create()
        .await?;

    // ~6 MiB payload, comfortably above the broker's default 5 MiB max message
    // size, so the producer must emit at least two chunks.
    let payload_size: usize = 6 * 1024 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-chunk")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let msg = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        msg.payload.len(),
        payload_size,
        "reassembled chunked payload length mismatch"
    );
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    Ok(())
}
