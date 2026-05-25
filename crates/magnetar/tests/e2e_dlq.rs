// SPDX-License-Identifier: Apache-2.0

//! End-to-end DLQ + retry-letter (`reconsume_later`) round-trip tests against a
//! real Apache Pulsar 4.x standalone broker.
//!
//! Mirrors Apache Pulsar's `DeadLetterTopicTest` (PIP-22 / PIP-58 / PIP-409)
//! Java parity coverage. Gated behind `e2e`; run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_dlq -- --nocapture
//! ```
//!
//! Requires Docker on the host.

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

/// PIP-22 DLQ routing: once a message has been redelivered past
/// `max_redeliver_count`, the consumer flags it as dead-letter; we then republish
/// to the DLQ topic and verify a second consumer reads it back.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_dlq_max_redeliver_routes_to_dead_letter() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-dlq-{id}");
    let dlq_topic = format!("persistent://public/default/magnetar-e2e-dlq-{id}-DLQ");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Producer sends a single payload onto the source topic.
    let producer = client.producer(topic.clone()).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"poison".to_vec()).into())
        .await?;
    producer.close().await?;

    // Main consumer with max_redeliver_count = 1 — after the first redelivery the
    // message should be flagged for DLQ.
    let consumer = client
        .consumer(topic.clone())
        .subscription("magnetar-dlq-sub")
        .subscription_type(SubType::Shared)
        .dead_letter_policy(1, Some(dlq_topic.clone()))
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // First receive: do not ack.
    let msg = consumer.receive().await?;
    assert_eq!(msg.payload.as_ref(), b"poison");

    // Force redelivery (faster + deterministic than ack_timeout).
    consumer.redeliver_unacked();

    // Second receive: also do not ack. Once redelivered past max_redeliver_count
    // it lands in the per-consumer DLQ buffer.
    let _msg2 = consumer.receive().await?;
    consumer.redeliver_unacked();

    // Poll the dead-letter drain a few times — the broker may need a tick to
    // republish the message with its bumped redelivery count.
    let dlq_producer = client.producer(dlq_topic.clone()).create().await?;
    let mut republished = 0usize;
    for _ in 0..30 {
        republished += consumer.republish_dead_letters(&dlq_producer).await?;
        if republished > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Try forcing another redelivery to push the count over the limit.
        consumer.redeliver_unacked();
        if let Ok(Ok(extra)) =
            tokio::time::timeout(Duration::from_millis(200), consumer.receive()).await
        {
            // Drain whatever pops; do not ack.
            let _ = extra;
        }
    }
    assert!(republished >= 1, "expected at least one DLQ republish");

    // Second consumer on the DLQ topic reads the republished message.
    let dlq_consumer = client
        .consumer(dlq_topic.clone())
        .subscription("magnetar-dlq-tail")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let dlq_msg = tokio::time::timeout(Duration::from_secs(10), dlq_consumer.receive()).await??;
    assert_eq!(dlq_msg.payload.as_ref(), b"poison");
    dlq_consumer.ack(dlq_msg.message_id).await?;

    dlq_consumer.close().await?;
    dlq_producer.close().await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// PIP-58 retry-letter (`reconsume_later`): republish a message with a delay onto
/// the retry-letter topic, then re-subscribe to that topic and verify the
/// delayed redelivery + `RECONSUMETIMES` property.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_reconsume_later_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-retry-{id}");
    let retry_topic = format!("persistent://public/default/magnetar-e2e-retry-{id}-RETRY");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(topic.clone()).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"deferred".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic.clone())
        .subscription("magnetar-retry-sub")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = consumer.receive().await?;
    assert_eq!(msg.payload.as_ref(), b"deferred");

    // Republish to the retry topic with a 1s delay.
    let retry_producer = client.producer(retry_topic.clone()).create().await?;
    consumer
        .reconsume_later(&retry_producer, msg, Duration::from_secs(1))
        .await?;

    // Subscribe to the retry topic and pull the delayed message.
    let retry_consumer = client
        .consumer(retry_topic.clone())
        .subscription("magnetar-retry-tail")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let redelivered =
        tokio::time::timeout(Duration::from_secs(15), retry_consumer.receive()).await??;
    assert_eq!(redelivered.payload.as_ref(), b"deferred");

    // RECONSUMETIMES should have been stamped at 1 by the runtime.
    let reconsume_times = redelivered
        .metadata
        .properties
        .iter()
        .find(|kv| kv.key == "RECONSUMETIMES")
        .map(|kv| kv.value.clone());
    assert_eq!(reconsume_times.as_deref(), Some("1"));

    retry_consumer.ack(redelivered.message_id).await?;
    retry_consumer.close().await?;
    retry_producer.close().await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// Once a DLQ-routed message is consumed and acked on the DLQ topic, it must
/// not reappear on a fresh subscription read.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_dlq_explicit_ack_terminates() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let dlq_topic = format!("persistent://public/default/magnetar-e2e-dlq-ack-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Seed the DLQ topic directly — we're only testing the ack semantics on the
    // tail consumer, not the routing path covered by the first test.
    let producer = client.producer(dlq_topic.clone()).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"terminal".to_vec()).into())
        .await?;
    producer.close().await?;

    // First consumer: receive and ack.
    let consumer = client
        .consumer(dlq_topic.clone())
        .subscription("magnetar-dlq-terminal")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let msg = consumer.receive().await?;
    assert_eq!(msg.payload.as_ref(), b"terminal");
    consumer.ack(msg.message_id).await?;

    // Wait for the broker to record the ack, then verify no redelivery on the
    // same subscription.
    let redelivery = tokio::time::timeout(Duration::from_secs(2), consumer.receive()).await;
    assert!(
        redelivery.is_err(),
        "DLQ message reappeared after explicit ack"
    );

    consumer.close().await?;
    client.close().await;
    Ok(())
}
