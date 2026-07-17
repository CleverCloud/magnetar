// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for consumer push delivery (`MessageListener`, ADR-0064)
//! against a real Apache Pulsar 4.x standalone broker via `testcontainers-rs`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker +
//! `apachepulsar/pulsar` reachable.
//!
//! ## What this proves end-to-end
//!
//! 1. A consumer with a registered `message_listener` receives every produced message via the
//!    callback (push delivery), in order.
//! 2. The callback acks explicitly (the poller never auto-acks); the acks land, so a second
//!    consumer on the same subscription sees **no redelivery** of the already-acked backlog.
//! 3. The listener task shuts down cleanly when its `MessageListenerHandle` is closed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{IncomingMessage, MessageListener, OutgoingMessage, PulsarClient};
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::Notify;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
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
        .with_startup_timeout(Duration::from_mins(2))
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
async fn e2e_message_listener_push_delivery_with_explicit_ack()
-> Result<(), Box<dyn std::error::Error>> {
    const N: usize = 8;

    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-listener";
    let subscription = "magnetar-e2e-listener-sub";

    // Produce N messages.
    let producer = client.producer(topic).create().await?;
    for i in 0..N {
        producer
            .send(OutgoingMessage::with_payload(format!("listener-msg-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    // Subscribe a consumer, then attach a push-delivery listener to it.
    // Clone the consumer so the callback can ack explicitly (Java parity — the
    // poller never auto-acks). The poller drives delivery over `consumer`; the
    // closure acks via `ack_consumer`.
    let consumer = client
        .consumer(topic)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let ack_consumer = consumer.clone();

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(Notify::new());

    let received_cb = received.clone();
    let count_cb = count.clone();
    let done_cb = done.clone();
    let listener: MessageListener = Arc::new(move |msg: &IncomingMessage| {
        received_cb
            .lock()
            .push(String::from_utf8_lossy(&msg.payload).into_owned());
        // Explicit ack — spawn the async ack from the sync callback, capturing
        // the dedicated ack-consumer clone.
        let ack = ack_consumer.clone();
        let id = msg.id;
        tokio::spawn(async move {
            let _ = ack.ack(id).await;
        });
        if count_cb.fetch_add(1, Ordering::SeqCst) + 1 == N {
            done_cb.notify_waiters();
        }
    });

    let handle = magnetar::spawn_message_listener(consumer, listener);

    // Await all N deliveries (bounded).
    tokio::time::timeout(Duration::from_secs(30), done.notified())
        .await
        .expect("listener delivered all messages within the deadline");

    // The listener saw every message, in order.
    let got = received.lock().clone();
    let expected: Vec<String> = (0..N).map(|i| format!("listener-msg-{i}")).collect();
    assert_eq!(got, expected, "listener observed every message in order");

    // Give the spawned acks a moment to land on the broker, then stop the poller.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(handle.is_running(), "poller is alive before close");
    handle.close().await;
    assert!(!handle.is_running(), "poller stops cleanly after close");

    // No redelivery: a fresh consumer on the SAME subscription must NOT see the
    // already-acked backlog (acks landed). Earliest position + a short receive
    // timeout — expect nothing.
    let verifier = client
        .consumer(topic)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let redelivered = verifier
        .receive_with_timeout(Duration::from_secs(5))
        .await?;
    assert!(
        redelivered.is_none(),
        "explicit acks landed — no redelivery of the acked backlog, got {redelivered:?}",
    );
    verifier.close().await?;
    client.close().await;
    Ok(())
}
