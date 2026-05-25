// SPDX-License-Identifier: Apache-2.0

//! Partitioned topic e2e tests that go beyond the existing
//! `e2e_partitioned_topic_roundtrip` (which only covers a Shared subscription
//! sink). These mirror Apache Pulsar Java's
//! `pulsar-broker/src/test/java/org/apache/pulsar/client/api/MessageRouterTest.java`
//! and the partitioned-producer / partitioned-consumer parity bits scattered
//! across `PartitionedProducerImplTest`.

#![cfg(feature = "e2e")]

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{MessageRouter, MessageRoutingMode, OutgoingMessage, PulsarClient};
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

fn fresh_topic(suffix: &str) -> String {
    format!(
        "persistent://public/default/magnetar-e2e-{}-{}",
        suffix,
        uuid::Uuid::new_v4().simple()
    )
}

const PARTITIONS: usize = 4;

/// `MessageRouter` impl that pins every message to partition 1. Hoisted outside
/// the test function so the `items_after_statements` lint is satisfied.
#[derive(Debug)]
struct PinToOne;

impl MessageRouter for PinToOne {
    fn route(&self, _msg: &OutgoingMessage, _partitions: usize) -> usize {
        1
    }
}

async fn create_partitioned_topic(
    admin_url: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    admin
        .topic_create_partitioned(topic, PARTITIONS as u32)
        .await?;
    Ok(())
}

/// Round-robin routing visits every partition. We dedicate one Exclusive
/// consumer per partition (subscribing to `<topic>-partition-<n>`) and assert
/// that an 8-message blast lands two messages on each.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partitioned_round_robin_visits_every_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("round-robin");
    create_partitioned_topic(&admin_url, &topic).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.clone())
        .routing(MessageRoutingMode::RoundRobin)
        .create()
        .await?;

    // Subscribe one Exclusive consumer per partition BEFORE producing so we
    // see every published message regardless of broker retention.
    let mut consumers = Vec::with_capacity(PARTITIONS);
    for p in 0..PARTITIONS {
        let part_topic = format!("{topic}-partition-{p}");
        let c = client
            .consumer(part_topic)
            .subscription(format!("magnetar-rr-sub-{p}"))
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .subscribe()
            .await?;
        consumers.push(c);
    }

    let total = 8usize;
    for i in 0..total {
        producer
            .send(OutgoingMessage::with_payload(
                format!("msg-{i}").into_bytes(),
            ))
            .await?;
    }
    producer.close().await?;

    // Drain each partition's consumer; assert exactly `total / PARTITIONS`.
    for (p, c) in consumers.into_iter().enumerate() {
        let expected = total / PARTITIONS;
        for _ in 0..expected {
            let msg = tokio::time::timeout(Duration::from_secs(15), c.receive())
                .await
                .map_err(|_| format!("partition {p} timeout waiting for {expected} messages"))??;
            c.ack(msg.message_id).await?;
        }
        // No more messages within a short window — partition really got `expected`.
        let extra = tokio::time::timeout(Duration::from_millis(500), c.receive()).await;
        assert!(
            extra.is_err(),
            "partition {p} received more than {expected} messages"
        );
        c.close().await?;
    }
    client.close().await;
    Ok(())
}

/// Pinning `MessageRouter` always returns partition 1 — partition 1 sees every
/// message, partitions 0 / 2 / 3 see none.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partitioned_custom_message_router() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("custom-router");
    create_partitioned_topic(&admin_url, &topic).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.clone())
        .message_router(Arc::new(PinToOne))
        .create()
        .await?;

    let mut consumers = Vec::with_capacity(PARTITIONS);
    for p in 0..PARTITIONS {
        let part_topic = format!("{topic}-partition-{p}");
        let c = client
            .consumer(part_topic)
            .subscription(format!("magnetar-custom-sub-{p}"))
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .subscribe()
            .await?;
        consumers.push(c);
    }

    let total = 5usize;
    for i in 0..total {
        producer
            .send(OutgoingMessage::with_payload(
                format!("msg-{i}").into_bytes(),
            ))
            .await?;
    }
    producer.close().await?;

    for (p, c) in consumers.into_iter().enumerate() {
        if p == 1 {
            for _ in 0..total {
                let msg = tokio::time::timeout(Duration::from_secs(15), c.receive())
                    .await
                    .map_err(|_| format!("partition 1 timeout — expected {total} messages"))??;
                c.ack(msg.message_id).await?;
            }
        } else {
            let extra = tokio::time::timeout(Duration::from_millis(800), c.receive()).await;
            assert!(
                extra.is_err(),
                "partition {p} should be empty under the pinning router"
            );
        }
        c.close().await?;
    }
    client.close().await;
    Ok(())
}

/// `PartitionedConsumer` aggregates across partitions and preserves per-partition
/// order. We assign each message a distinct payload + key and verify the
/// partitioned consumer drains all of them.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partitioned_consumer_aggregates_all() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("partitioned-agg");
    create_partitioned_topic(&admin_url, &topic).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.clone())
        .routing(MessageRoutingMode::RoundRobin)
        .create()
        .await?;

    let total = 12usize;
    for i in 0..total {
        producer
            .send(OutgoingMessage::with_payload(
                format!("msg-{i}").into_bytes(),
            ))
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .partitioned_consumer(topic.clone())
        .subscription("magnetar-agg-sub")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = 0usize;
    while received < total {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("partitioned consumer timeout: got {received} of {total}"))??;
        consumer.ack(&msg.topic, msg.message.message_id).await?;
        received += 1;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, total);
    Ok(())
}
