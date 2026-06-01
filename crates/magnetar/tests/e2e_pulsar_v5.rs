// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the PIP-466 V5 client surface against a real
//! Apache Pulsar 4.x standalone broker spun up via `testcontainers-rs`.
//!
//! Gated behind the `experimental-v5-client` feature.
//! flags. Run with:
//!
//! ```sh
//! cargo test --features experimental-v5-client \
//!   -p magnetar --test e2e_pulsar_v5 -- --nocapture
//! ```
//!
//! Requires Docker on the host.
//!
//! Mirrors `e2e_pulsar.rs`'s setup pattern (same `apachepulsar/pulsar:4.0.4`
//! image, same `start_pulsar` shape) but drives the V5 surface
//! (`PulsarClientV5`, `v5::producer::ProducerBuilder`,
//! `v5::stream_consumer::StreamConsumerBuilder`).

#![cfg(feature = "experimental-v5-client")]

use std::time::Duration;

use bytes::Bytes;
use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::V5SubscriptionInitialPosition;
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
async fn e2e_v5_produce_consume_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let v4 = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let client = PulsarClientV5::from_v4(v4);
    let topic = "persistent://public/default/magnetar-v5-e2e-roundtrip";

    // V5 producer with explicit V5-typed config.
    let producer = client
        .producer(topic)
        .send_timeout(Duration::from_secs(30))
        .max_pending_messages(Some(1000))
        .create()
        .await?;
    let payloads: &[&[u8]] = &[b"v5-hello", b"v5-pulsar", b"v5-4.0"];
    for p in payloads {
        producer.send(Bytes::copy_from_slice(p)).await?;
    }

    // V5 stream consumer (Exclusive default). `initial_position(Earliest)`
    // is load-bearing here: V5 mirrors Java's `Latest` default, but this
    // test produces BEFORE subscribing, so a Latest consumer would never
    // see the three payloads and `consumer.receive().await` would hang
    // forever.
    let consumer = client
        .stream_consumer(topic)
        .subscription("magnetar-v5-e2e")
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .ack_timeout(Some(Duration::from_secs(30)))
        .negative_ack_redelivery_delay(Duration::from_secs(60))
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.id).await?;
    }
    client.into_v4().close().await;

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "V5 round-trip preserves payload order on a single Exclusive subscription"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_v5_v4_escape_hatch_shares_state() -> Result<(), Box<dyn std::error::Error>> {
    // Cross-surface invariant from ADR-0032: `PulsarClientV5::v4()`
    // returns a borrowed reference to the SAME engine state. We
    // publish via the V5 producer and consume via the v4 escape-hatch
    // consumer on the same client — proves no double-init or state
    // divergence between the two surfaces.
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let v4 = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let client = PulsarClientV5::from_v4(v4);
    let topic = "persistent://public/default/magnetar-v5-escape-hatch";

    let producer = client.producer(topic).create().await?;
    let payload = b"escape-hatch-payload";
    producer.send(Bytes::copy_from_slice(payload)).await?;

    // Subscribe via the v4 escape hatch — same engine state, no
    // re-handshake, no second connection.
    let consumer = client
        .v4()
        .consumer(topic)
        .subscription("magnetar-v5-escape-hatch")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = consumer.receive().await?;
    assert_eq!(
        &msg.payload[..],
        payload,
        "v4 escape-hatch consumer sees V5-produced bytes verbatim"
    );
    // v4 IncomingMessage uses `message_id` (the field name on the
    // v4 surface; the V5 wrapper renamed it to `id` for ergonomics).
    consumer.ack(msg.message_id).await?;
    client.into_v4().close().await;
    Ok(())
}
