// SPDX-License-Identifier: Apache-2.0

//! End-to-end DLQ + retry-letter (`reconsume_later`) round-trip tests against a
//! real Apache Pulsar 4.x standalone broker.
//!
//! Mirrors Apache Pulsar's `DeadLetterTopicTest` (PIP-22 / PIP-58 / PIP-409)
//! Java parity coverage. Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_dlq -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{MessageRoutingMode, OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_partitioned_consumer_aggregate_republishes_every_child_dead_letter()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-partitioned-dlq-{id}");
    let dlq_topic = format!("{topic}-DLQ");
    let subscription = format!("magnetar-partitioned-dlq-{id}");

    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    admin.topic_create_partitioned(&topic, 2).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let producer = client
        .partitioned_producer(&topic)
        .routing(MessageRoutingMode::RoundRobin)
        .create()
        .await?;
    let expected = [
        (
            "poison-0",
            "key-0",
            b"order-0".as_slice(),
            1_700_000_001_000,
            "value-0",
        ),
        (
            "poison-1",
            "key-1",
            b"order-1".as_slice(),
            1_700_000_001_001,
            "value-1",
        ),
    ];
    for (payload, key, ordering_key, event_time, property) in expected {
        producer
            .send(
                OutgoingMessage::with_payload(payload.as_bytes().to_vec())
                    .key(key)
                    .ordering_key(ordering_key.to_vec())
                    .event_time_ms(event_time)
                    .property("custom", property),
            )
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .partitioned_consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .dead_letter_policy(1, Some(dlq_topic.clone()))
        .subscribe()
        .await?;

    let mut source_correlation = std::collections::BTreeMap::new();
    for delivery_round in 0..2 {
        for index in 0..expected.len() {
            let received = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
                .await
                .map_err(|_| {
                    format!("timed out receiving poison message {index} in round {delivery_round}")
                })??;
            let payload = String::from_utf8(received.message.payload.to_vec())?;
            if delivery_round == 0 {
                source_correlation.insert(
                    payload,
                    (
                        received.topic.clone(),
                        received.message.message_id.to_string(),
                    ),
                );
            }
        }
        consumer.redeliver_unacked();
    }
    let source_topics: std::collections::BTreeSet<_> = source_correlation
        .values()
        .map(|(source_topic, _)| source_topic.as_str())
        .collect();
    assert_eq!(
        source_topics.len(),
        2,
        "round-robin poison messages must exercise both real partition children"
    );

    let dlq_producer = client.producer(&dlq_topic).create().await?;
    let classified = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let classified = consumer.aggregate_stats().total_msgs_dead_lettered;
            if classified >= expected.len() as u64 {
                break classified;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| "timed out waiting for both partition children to classify dead letters")?;
    assert_eq!(
        classified,
        expected.len() as u64,
        "partition children classified an unexpected number of source originals"
    );
    assert_eq!(
        consumer.republish_dead_letters(&dlq_producer).await?,
        expected.len(),
        "one aggregate drain must republish every partition child's dead letter"
    );
    assert_eq!(
        consumer.republish_dead_letters(&dlq_producer).await?,
        0,
        "a second aggregate drain must be empty"
    );

    let dlq_consumer = client
        .consumer(&dlq_topic)
        .subscription(format!("magnetar-partitioned-dlq-tail-{id}"))
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut replacements = std::collections::BTreeMap::new();
    for index in 0..expected.len() {
        let message = tokio::time::timeout(Duration::from_secs(15), dlq_consumer.receive())
            .await
            .map_err(|_| format!("timed out receiving DLQ replacement {index}"))??;
        let payload = String::from_utf8(message.payload.to_vec())?;
        let property = |key: &str| {
            message
                .metadata
                .properties
                .iter()
                .find(|property| property.key == key)
                .map(|property| property.value.clone())
        };
        assert!(
            replacements
                .insert(
                    payload,
                    (
                        message.metadata.partition_key.clone(),
                        message.metadata.ordering_key.clone(),
                        message.metadata.event_time,
                        property("custom"),
                        property("REAL_TOPIC"),
                        property("ORIGINAL_MESSAGE_ID"),
                    ),
                )
                .is_none(),
            "duplicate DLQ replacement payload"
        );
        dlq_consumer.ack(message.message_id).await?;
    }
    let extra_replacement =
        tokio::time::timeout(Duration::from_secs(2), dlq_consumer.receive()).await;
    assert!(
        extra_replacement.is_err(),
        "unexpected additional DLQ replacement: {extra_replacement:?}"
    );
    for (payload, key, ordering_key, event_time, custom) in expected {
        let replacement = replacements
            .get(payload)
            .ok_or_else(|| format!("missing DLQ replacement for {payload}"))?;
        assert_eq!(replacement.0.as_deref(), Some(key));
        assert_eq!(replacement.1.as_deref(), Some(ordering_key));
        assert_eq!(replacement.2, Some(event_time));
        assert_eq!(replacement.3.as_deref(), Some(custom));
        let (source_topic, source_message_id) = source_correlation
            .get(payload)
            .ok_or_else(|| format!("missing source correlation for {payload}"))?;
        assert_eq!(replacement.4.as_deref(), Some(source_topic.as_str()));
        assert_eq!(replacement.5.as_deref(), Some(source_message_id.as_str()));
    }

    dlq_consumer.close().await?;
    dlq_producer.close().await?;
    consumer.close().await?;
    let resumed = client
        .partitioned_consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let redelivery = tokio::time::timeout(Duration::from_secs(2), resumed.receive()).await;
    assert!(
        redelivery.is_err(),
        "source originals reappeared after replacement publication and aggregate ACK: {redelivery:?}"
    );
    resumed.close().await?;
    client.close().await;
    Ok(())
}
