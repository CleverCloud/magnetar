// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for ADR-0040 vectored producer writes against a real
//! Apache Pulsar 4.x standalone broker.
//!
//! The moonpool / tokio engines now dispatch each queued producer publish as
//! a `magnetar_proto::TransmitOwned::Vectored` — a `[frame-head, payload]`
//! segment pair flushed via a real `write_vectored` rather than coalescing
//! into one buffer first (ADR-0040 wave 2). The wire bytes are byte-identical
//! to the old contiguous path, so the *only* thing an e2e can meaningfully
//! assert is the **broker-visible round-trip semantics** — not the syscall
//! shape (which is invisible above the socket).
//!
//! This test publishes payloads large enough that the head/payload split is
//! non-trivial (the payload segment dwarfs the frame head, so a vectored
//! flush genuinely spans two sizable `IoSlice`s) and confirms every byte
//! survives the round trip in order. A multi-message batch on a single
//! producer exercises the driver merging several per-slot staged sends into
//! one vectored transmit.
//!
//! Runs as a regular test under `cargo test` (ADR-0045). Requires
//! Docker on the host.

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

/// Start a Pulsar 4.x standalone container and return (`service_url`,
/// `container_handle`). The container is held by the returned guard; dropping
/// it stops the broker.
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

/// Publish a batch of large payloads through one producer (each publish is a
/// `[head, payload]` vectored transmit) and assert the broker delivers every
/// byte back, in order. The payloads carry a 64 KiB body so the payload
/// `IoSlice` genuinely dominates the frame-head `IoSlice` — a coalescing
/// regression would still pass, but the round-trip equality is the contract
/// the vectored path must preserve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_vectored_large_payload_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-vectored";

    // 64 KiB payloads, distinct per message so reassembly order is checkable.
    let payload_len = 64 * 1024;
    let count = 8usize;
    let expected: Vec<Vec<u8>> = (0..count)
        .map(|i| {
            // Fill byte encodes the message index → any cross-segment or
            // cross-message corruption shows up immediately.
            vec![u8::try_from(i).unwrap_or(0); payload_len]
        })
        .collect();

    let producer = client.producer(topic).create().await?;
    for body in &expected {
        producer
            .send(OutgoingMessage::with_payload(body.clone()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-vectored")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received: Vec<Vec<u8>> = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive()).await??;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received.len(),
        count,
        "every vectored publish must round-trip"
    );
    for (i, (got, want)) in received.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            got.len(),
            want.len(),
            "message {i}: payload length mismatch"
        );
        assert_eq!(
            got, want,
            "message {i}: payload bytes corrupted across the vectored flush"
        );
    }
    Ok(())
}
