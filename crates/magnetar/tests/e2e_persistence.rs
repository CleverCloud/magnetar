// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests covering the `persistent://` vs `non-persistent://` topic
//! distinction against a real Pulsar 4.x standalone broker.
//!
//! Ports the behavioural intent of Apache Pulsar's
//! `org.apache.pulsar.client.api.NonPersistentTopicTest`:
//!
//! * `persistent://` topics survive across producer/consumer disconnects via the managed-ledger
//!   storage layer.
//! * `non-persistent://` topics dispatch directly from the broker without ever touching
//!   `BookKeeper`. They have no backlog: if no consumer is subscribed at produce time, the message
//!   is dropped.
//!
//! Magnetar's wire protocol treats both schemes as opaque topic strings — the
//! client never has to special-case them. These tests pin that contract end-to-end
//! against the real broker rather than re-asserting it in unit tests.
//!
//! Gated behind the `e2e` feature flag. Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_persistence -- --nocapture --ignored
//! ```

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

/// Round-trip with the explicit `persistent://` prefix. Same path as the default
/// `e2e_produce_consume_roundtrip` test, but pinned to the prefix so a future
/// regression in topic-string handling (e.g. stripping the scheme) is caught.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_persistent_topic_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-persistent-{}",
        uuid::Uuid::new_v4().simple()
    );

    let producer = client.producer(&topic).create().await?;
    let payloads: &[&[u8]] = &[b"persistent-1", b"persistent-2", b"persistent-3"];
    for p in payloads {
        producer
            .send(OutgoingMessage::with_payload(p.to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-persistent")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "persistent topic must replay produced payloads to a later subscriber",
    );
    Ok(())
}

/// Round-trip against a `non-persistent://` topic. The broker dispatches directly
/// without writing to `BookKeeper`, so the consumer must subscribe BEFORE the
/// producer sends — otherwise the messages are dropped at the dispatcher.
///
/// Mirrors `NonPersistentTopicTest#testNonPersistentTopic` from the Java client.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_non_persistent_topic_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = format!(
        "non-persistent://public/default/magnetar-e2e-non-persistent-{}",
        uuid::Uuid::new_v4().simple()
    );

    // Subscribe FIRST so the dispatcher exists when the producer sends —
    // non-persistent topics have no backlog.
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-non-persistent")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let payloads: &[&[u8]] = &[b"np-1", b"np-2", b"np-3"];
    for p in payloads {
        producer
            .send(OutgoingMessage::with_payload(p.to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "non-persistent topic must dispatch produced payloads to an already-attached consumer",
    );
    Ok(())
}

/// Non-persistent topics have no backlog: messages sent while no consumer is
/// attached must be dropped at the broker, never delivered to a later subscriber.
///
/// This is the defining behavioural difference vs `persistent://` and is what the
/// Java `NonPersistentTopicTest` exercises through the dispatcher tests.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_non_persistent_topic_drops_when_no_consumer() -> Result<(), Box<dyn std::error::Error>>
{
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = format!(
        "non-persistent://public/default/magnetar-e2e-np-drop-{}",
        uuid::Uuid::new_v4().simple()
    );

    // Produce 5 messages with NO consumer attached. The non-persistent dispatcher
    // has nothing to fan out to, so the messages are silently dropped.
    let producer = client.producer(&topic).create().await?;
    for i in 0..5 {
        producer
            .send(OutgoingMessage::with_payload(format!("dropped-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    // Subscribe AFTER the send. A persistent topic would replay; a non-persistent
    // topic must yield nothing within a short wait window.
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-np-drop")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let timeout = Duration::from_secs(3);
    match tokio::time::timeout(timeout, consumer.receive()).await {
        Ok(Ok(msg)) => {
            panic!(
                "non-persistent topic must drop messages produced before subscription, \
                 but received payload {:?}",
                msg.payload,
            );
        }
        Ok(Err(e)) => return Err(Box::<dyn std::error::Error>::from(e)),
        Err(_) => {
            // Timeout — expected. The dispatcher had no backlog to replay.
        }
    }

    consumer.close().await?;
    client.close().await;
    Ok(())
}
