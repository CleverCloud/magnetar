// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests covering the `ProducerInterceptor` / `ConsumerInterceptor`
//! SPIs and the ack-pattern surface (individual / batch / cumulative).
//!
//! Mirrors Apache Pulsar Java's
//! `pulsar-broker/src/test/java/org/apache/pulsar/client/api/InterceptorsTest.java`
//! and the ack-list tests under `ConsumerAckListTest.java`.

#![cfg(feature = "e2e")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{
    ConsumerInterceptor, IncomingMessage, OutgoingMessage, ProducerInterceptor, PulsarClient,
    PulsarError, ack_with_interceptors, receive_with_interceptors, send_with_interceptors,
};
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

/// Counter-collecting `ProducerInterceptor`. Stamps an opaque property on every
/// `before_send` and bumps an `Arc<AtomicUsize>` on every
/// `on_send_acknowledgement` success.
#[derive(Debug)]
#[allow(clippy::struct_field_names, reason = "names mirror trait callbacks")]
struct CountingProducerInterceptor {
    before_send_calls: Arc<AtomicUsize>,
    on_ack_calls: Arc<AtomicUsize>,
}

impl ProducerInterceptor for CountingProducerInterceptor {
    fn before_send(&self, msg: &mut OutgoingMessage) {
        self.before_send_calls.fetch_add(1, Ordering::SeqCst);
        // Stamp a property so we can see the chain ran end-to-end.
        msg.properties
            .push(("magnetar.interceptor.tag".to_owned(), "stamped".to_owned()));
    }

    fn on_send_acknowledgement(
        &self,
        _msg: &OutgoingMessage,
        outcome: Result<magnetar_proto::MessageId, &PulsarError>,
    ) {
        if outcome.is_ok() {
            self.on_ack_calls.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
#[allow(clippy::struct_field_names, reason = "names mirror trait callbacks")]
struct CountingConsumerInterceptor {
    before_consume_calls: Arc<AtomicUsize>,
    on_ack_calls: Arc<AtomicUsize>,
    on_ack_cumulative_calls: Arc<AtomicUsize>,
}

impl ConsumerInterceptor for CountingConsumerInterceptor {
    fn before_consume(&self, _msg: &mut IncomingMessage) {
        self.before_consume_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn on_acknowledge(
        &self,
        _message_id: magnetar_proto::MessageId,
        outcome: Result<(), &PulsarError>,
    ) {
        if outcome.is_ok() {
            self.on_ack_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn on_acknowledge_cumulative(
        &self,
        _message_id: magnetar_proto::MessageId,
        outcome: Result<(), &PulsarError>,
    ) {
        if outcome.is_ok() {
            self.on_ack_cumulative_calls.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn fresh_topic(suffix: &str) -> String {
    format!(
        "persistent://public/default/magnetar-e2e-{}-{}",
        suffix,
        uuid::Uuid::new_v4().simple()
    )
}

/// Producer interceptor chain observes every send + ack round-trip.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_interceptor_observes_send_ack() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("producer-interceptor");

    let producer = client.producer(topic.clone()).create().await?;
    let interceptor = Arc::new(CountingProducerInterceptor {
        before_send_calls: Arc::new(AtomicUsize::new(0)),
        on_ack_calls: Arc::new(AtomicUsize::new(0)),
    });
    let chain: Vec<Arc<dyn ProducerInterceptor>> = vec![interceptor.clone()];

    let payloads: &[&[u8]] = &[b"alpha", b"bravo", b"charlie"];
    for p in payloads {
        send_with_interceptors(&producer, OutgoingMessage::with_payload(p.to_vec()), &chain)
            .await?;
    }
    producer.close().await?;
    client.close().await;

    assert_eq!(
        interceptor.before_send_calls.load(Ordering::SeqCst),
        payloads.len(),
        "before_send must fire once per send"
    );
    assert_eq!(
        interceptor.on_ack_calls.load(Ordering::SeqCst),
        payloads.len(),
        "on_send_acknowledgement must fire once per broker ack"
    );
    Ok(())
}

/// Consumer interceptor chain observes every receive + ack.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_consumer_interceptor_observes_receive_ack() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("consumer-interceptor");

    let producer = client.producer(topic.clone()).create().await?;
    for i in 0..3u32 {
        producer
            .send(OutgoingMessage::with_payload(i.to_be_bytes().to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic.clone())
        .subscription("magnetar-e2e-interceptor-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let interceptor = Arc::new(CountingConsumerInterceptor {
        before_consume_calls: Arc::new(AtomicUsize::new(0)),
        on_ack_calls: Arc::new(AtomicUsize::new(0)),
        on_ack_cumulative_calls: Arc::new(AtomicUsize::new(0)),
    });
    let chain: Vec<Arc<dyn ConsumerInterceptor>> = vec![interceptor.clone()];

    for _ in 0..3 {
        let msg = receive_with_interceptors(&consumer, &chain).await?;
        ack_with_interceptors(&consumer, msg.id, &chain).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        interceptor.before_consume_calls.load(Ordering::SeqCst),
        3,
        "before_consume must fire once per receive"
    );
    assert_eq!(
        interceptor.on_ack_calls.load(Ordering::SeqCst),
        3,
        "on_acknowledge must fire once per individual ack"
    );
    Ok(())
}

/// Batch ack of all received message ids terminates redelivery — restarting the
/// subscription must NOT replay them.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_ack_batch_terminates_redelivery() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;
    let topic = fresh_topic("ack-batch");
    let sub = "magnetar-e2e-ack-batch-sub";

    // Round 1: produce + receive + batch ack.
    let producer = client.producer(topic.clone()).create().await?;
    for i in 0..5u32 {
        producer
            .send(OutgoingMessage::with_payload(i.to_be_bytes().to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic.clone())
        .subscription(sub)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut ids = Vec::with_capacity(5);
    for _ in 0..5 {
        let msg = consumer.receive().await?;
        ids.push(msg.message_id);
    }
    consumer.ack_batch(ids).await?;
    consumer.close().await?;

    // Round 2: same subscription must see zero messages within a short window.
    let consumer2 = client
        .consumer(topic.clone())
        .subscription(sub)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let recv = tokio::time::timeout(Duration::from_secs(3), consumer2.receive()).await;
    assert!(
        recv.is_err(),
        "batch ack should terminate redelivery; got {recv:?}"
    );
    consumer2.close().await?;
    client.close().await;
    Ok(())
}

/// Cumulative ack of the last message id terminates redelivery for everything up
/// to and including that id.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_ack_cumulative_terminates_prior() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;
    let topic = fresh_topic("ack-cumulative");
    let sub = "magnetar-e2e-ack-cumulative-sub";

    let producer = client.producer(topic.clone()).create().await?;
    for i in 0..5u32 {
        producer
            .send(OutgoingMessage::with_payload(i.to_be_bytes().to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic.clone())
        .subscription(sub)
        .subscription_type(SubType::Failover)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut last_id = None;
    for _ in 0..5 {
        let msg = consumer.receive().await?;
        last_id = Some(msg.message_id);
    }
    consumer
        .ack_cumulative(last_id.expect("received 5 messages"))
        .await?;
    consumer.close().await?;

    // Restart on same subscription — cumulative ack must have advanced the
    // dispatch cursor past every message.
    let consumer2 = client
        .consumer(topic.clone())
        .subscription(sub)
        .subscription_type(SubType::Failover)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let recv = tokio::time::timeout(Duration::from_secs(3), consumer2.receive()).await;
    assert!(
        recv.is_err(),
        "cumulative ack should terminate redelivery; got {recv:?}"
    );
    consumer2.close().await?;
    client.close().await;
    Ok(())
}
