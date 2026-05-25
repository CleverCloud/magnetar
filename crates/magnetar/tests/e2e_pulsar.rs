// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests against a real Apache Pulsar 4.x standalone broker
//! spun up via `testcontainers-rs`.
//!
//! Gated behind the `e2e` feature flag. Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_pulsar -- --nocapture
//! ```
//!
//! Requires Docker on the host. CI runs these only in the `e2e` workflow
//! (`workflow_dispatch` + `release/*` branches) so unrelated PRs don't pay
//! the multi-minute container startup cost.
//!
//! ## Image
//!
//! Uses `apachepulsar/pulsar:4.0.4` (Pulsar 4.0 LTS, our minimum supported
//! broker version per `ask-magnetar-decisions.md`). Override with
//! `MAGNETAR_PULSAR_IMAGE` env var if you need a different tag locally.

#![cfg(feature = "e2e")]

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
///
/// The container is held by the returned guard; dropping it stops the broker.
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

#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_produce_consume_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-roundtrip";

    let producer = client.producer(topic).create().await?;
    let payloads: &[&[u8]] = &[b"hello", b"pulsar", b"4.0"];
    for p in payloads {
        producer
            .send(OutgoingMessage::with_payload(p.to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e")
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

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>()
    );
    Ok(())
}

#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partitioned_topic_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;

    // Create the partitioned topic via the admin API.
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let topic = "persistent://public/default/magnetar-e2e-partitioned";
    admin.topic_create_partitioned(topic, 4).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let producer = client.producer(topic).create().await?;

    let n = 16usize;
    for i in 0..n {
        producer
            .send(
                OutgoingMessage::with_payload(format!("msg-{i}").into_bytes())
                    .key(format!("key-{}", i % 4))
                    .into(),
            )
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-partitioned")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = 0usize;
    while received < n {
        let msg = consumer.receive().await?;
        received += 1;
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, n);
    Ok(())
}

#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_pattern_consumer_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Pre-publish a sentinel to two topics that share a prefix so the pattern matches.
    let topic_a = "persistent://public/default/magnetar-e2e-pattern-aa";
    let topic_b = "persistent://public/default/magnetar-e2e-pattern-bb";
    let unrelated = "persistent://public/default/magnetar-e2e-other-cc";
    for topic in [topic_a, topic_b, unrelated] {
        let producer = client.producer(topic).create().await?;
        producer
            .send(OutgoingMessage::with_payload(topic.as_bytes().to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Subscribe via the regex pattern. The broker filters server-side, so we trust the snapshot.
    let pattern = client
        .pattern_consumer()
        .namespace("public/default")
        .pattern("persistent://public/default/magnetar-e2e-pattern-.*")
        .subscription("magnetar-e2e-pattern")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut topics = pattern.topics();
    topics.sort();
    let mut expected = vec![topic_a.to_owned(), topic_b.to_owned()];
    expected.sort();
    assert_eq!(
        topics, expected,
        "pattern snapshot must match both prefix topics only"
    );

    // Drain one message from each subscribed topic — the pre-published sentinels.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(10), pattern.receive()).await??;
        seen.insert(msg.topic.clone());
        pattern.ack(&msg.topic, msg.message.message_id).await?;
    }
    pattern.close().await?;
    client.close().await;

    let mut got: Vec<String> = seen.into_iter().collect();
    got.sort();
    assert_eq!(got, expected);
    Ok(())
}

#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_key_shared_dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-key-shared";

    let producer = client.producer(topic).create().await?;
    let consumer_a = client
        .consumer(topic)
        .subscription("magnetar-e2e-ks")
        .subscription_type(SubType::KeyShared)
        .name("consumer-a")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(topic)
        .subscription("magnetar-e2e-ks")
        .subscription_type(SubType::KeyShared)
        .name("consumer-b")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let keys: &[&str] = &["alpha", "beta", "gamma", "delta"];
    let per_key = 4usize;
    let total = keys.len() * per_key;
    for (i, k) in (0..total).zip(keys.iter().cycle()) {
        producer
            .send(
                OutgoingMessage::with_payload(format!("{k}#{i}").into_bytes())
                    .key((*k).to_owned())
                    .into(),
            )
            .await?;
    }
    producer.close().await?;

    // Drain both consumers concurrently using `tokio::join!` (NOT a channel).
    let consume_loop = |consumer: magnetar::runtime_tokio::Consumer| async move {
        let mut keys_seen = std::collections::HashSet::new();
        loop {
            let timeout = tokio::time::timeout(Duration::from_secs(5), consumer.receive()).await;
            let Ok(Ok(msg)) = timeout else {
                break (consumer, keys_seen);
            };
            if let Some(key) = msg.metadata.partition_key.as_deref() {
                keys_seen.insert(key.to_owned());
            }
            consumer.ack(msg.message_id).await.ok();
        }
    };

    let (a_done, b_done) = tokio::join!(consume_loop(consumer_a), consume_loop(consumer_b));
    a_done.0.close().await?;
    b_done.0.close().await?;
    client.close().await;

    // Sticky key-shared dispatch should make each key go to exactly one consumer.
    let a_keys = a_done.1;
    let b_keys = b_done.1;
    let intersection: std::collections::HashSet<_> = a_keys.intersection(&b_keys).collect();
    assert!(
        intersection.is_empty(),
        "key-shared dispatch should partition keys: A={a_keys:?} B={b_keys:?}",
    );
    Ok(())
}
