// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for **wrapper** consumer push delivery
//! (`WrapperMessageListener`, ADR-0064 wrapper extension) against a real Apache
//! Pulsar 4.x standalone broker via `testcontainers-rs`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker +
//! `apachepulsar/pulsar` reachable.
//!
//! ## What this proves end-to-end
//!
//! 1. A multi-topic consumer with a registered `message_listener` receives every produced message
//!    across all its topics via the callback (push delivery), tagged with the originating topic,
//!    with each topic's messages in produce order.
//! 2. The callback acks explicitly via the wrapper's topic-routed `ack(topic, id)` (the poller
//!    never auto-acks); the acks land, so a second multi-topic consumer on the same subscription
//!    sees no redelivery of the already-acked backlog.
//! 3. The listener task shuts down cleanly when its `MessageListenerHandle` is closed.
//! 4. **Pattern-child inheritance**: a `PatternConsumer` subscribed with a listener picks up a
//!    topic created *after* subscribe (once `update()` reconciles it in) and routes that new
//!    topic's messages through the same listener.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{IncomingMessage, OutgoingMessage, PulsarClient, WrapperMessageListener};
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::sync::Notify;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_multi_topic_message_listener_push_delivery_with_explicit_ack()
-> Result<(), Box<dyn std::error::Error>> {
    const PER_TOPIC: usize = 4;

    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = Arc::new(
        PulsarClient::builder()
            .service_url(service_url)
            .build()
            .await?,
    );
    let topic_a = "persistent://public/default/magnetar-e2e-wrap-listener-a";
    let topic_b = "persistent://public/default/magnetar-e2e-wrap-listener-b";
    let subscription = "magnetar-e2e-wrap-listener-sub";

    // Produce PER_TOPIC messages to each topic.
    for topic in [topic_a, topic_b] {
        let producer = client.producer(topic).create().await?;
        for i in 0..PER_TOPIC {
            producer
                .send(
                    OutgoingMessage::with_payload(format!("wrap-{topic}-{i}").into_bytes()).into(),
                )
                .await?;
        }
        producer.close().await?;
    }

    // Subscribe a multi-topic consumer over both topics, then attach a
    // push-delivery listener. Clone the consumer so the callback can ack
    // explicitly via the topic-routed ack (Java parity — the poller never
    // auto-acks).
    let consumer = client
        .multi_topics_consumer()
        .topic(topic_a)
        .topic(topic_b)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let ack_consumer = consumer.clone();

    // Per-topic ordered delivery log + total counter.
    let received: Arc<Mutex<BTreeMap<String, Vec<String>>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(Notify::new());
    let total = PER_TOPIC * 2;

    let received_cb = received.clone();
    let count_cb = count.clone();
    let done_cb = done.clone();
    let listener: WrapperMessageListener = Arc::new(move |topic: &str, msg: &IncomingMessage| {
        received_cb
            .lock()
            .entry(topic.to_owned())
            .or_default()
            .push(String::from_utf8_lossy(&msg.payload).into_owned());
        // Explicit topic-routed ack — spawn the async ack from the sync callback.
        let ack = ack_consumer.clone();
        let topic = topic.to_owned();
        let id = msg.id;
        tokio::spawn(async move {
            let _ = ack.ack(&topic, id).await;
        });
        if count_cb.fetch_add(1, Ordering::SeqCst) + 1 == total {
            done_cb.notify_waiters();
        }
    });

    let handle = magnetar::spawn_wrapper_message_listener(consumer, listener);

    // Await all deliveries (bounded).
    tokio::time::timeout(Duration::from_secs(30), done.notified())
        .await
        .expect("wrapper listener delivered all messages within the deadline");

    // Each topic's messages arrived in produce order.
    let got = received.lock().clone();
    for topic in [topic_a, topic_b] {
        let expected: Vec<String> = (0..PER_TOPIC)
            .map(|i| format!("wrap-{topic}-{i}"))
            .collect();
        assert_eq!(
            got.get(topic).cloned().unwrap_or_default(),
            expected,
            "wrapper listener observed {topic} in order, once each",
        );
    }

    // Let the spawned acks land, then stop the poller cleanly.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(handle.is_running(), "poller is alive before close");
    handle.close().await;
    assert!(!handle.is_running(), "poller stops cleanly after close");

    // No redelivery: a fresh multi-topic consumer on the SAME subscription must
    // NOT see the already-acked backlog (acks landed).
    let verifier = client
        .multi_topics_consumer()
        .topic(topic_a)
        .topic(topic_b)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let redelivered = tokio::time::timeout(Duration::from_secs(5), verifier.receive()).await;
    assert!(
        redelivered.is_err(),
        "explicit acks landed — no redelivery of the acked backlog, got {redelivered:?}",
    );
    verifier.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_pattern_listener_inherits_late_discovered_child()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = Arc::new(
        PulsarClient::builder()
            .service_url(service_url)
            .build()
            .await?,
    );
    let namespace = "public/default";
    let pattern = "persistent://public/default/magnetar-e2e-wrap-pat-.*";
    let topic_initial = "persistent://public/default/magnetar-e2e-wrap-pat-initial";
    let topic_late = "persistent://public/default/magnetar-e2e-wrap-pat-late";
    let subscription = "magnetar-e2e-wrap-pat-sub";

    // Create + seed the initial topic so the pattern matches at least one topic
    // at subscribe time.
    {
        let producer = client.producer(topic_initial).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"pat-initial-0".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Subscribe a pattern consumer with a listener. Keep a clone so we can drive
    // `update()` (the poller consumes the consumer it is handed). The callback
    // acks via the topic-routed ack.
    let driver = client
        .pattern_consumer()
        .namespace(namespace)
        .pattern(pattern)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let ack_consumer = driver.clone();

    let saw_late = Arc::new(Notify::new());
    let saw_late_cb = saw_late.clone();
    let received: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = received.clone();
    let listener: WrapperMessageListener = Arc::new(move |topic: &str, msg: &IncomingMessage| {
        let payload = String::from_utf8_lossy(&msg.payload).into_owned();
        received_cb.lock().push((topic.to_owned(), payload));
        let ack = ack_consumer.clone();
        let topic_owned = topic.to_owned();
        let id = msg.id;
        tokio::spawn(async move {
            let _ = ack.ack(&topic_owned, id).await;
        });
        // Fire once the late-discovered topic's message reaches the listener.
        if topic == topic_late {
            saw_late_cb.notify_waiters();
        }
    });

    // Hold a separate reconcile handle by cloning BEFORE moving the consumer into
    // the poller. Both clones share one `Arc<Inner>`, so a topic that `reconcile`
    // subscribes shows up in the consumer set the poller is draining.
    let reconcile = driver.clone();
    let handle = magnetar::spawn_wrapper_message_listener(driver, listener);

    // Now create the late topic and produce to it.
    {
        let producer = client.producer(topic_late).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"pat-late-0".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Reconcile the pattern set so the late topic gets subscribed; the running
    // poller (sharing the same `Arc<Inner>`) then sweeps it on its next
    // `receive()`. Retry the reconcile a few times — the broker's PIP-145 watch
    // delta may take a moment to arrive.
    let mut reconciled_late = false;
    for _ in 0..30 {
        let report = reconcile.update(&client).await?;
        if report.added > 0 || reconcile.topics().iter().any(|t| t == topic_late) {
            reconciled_late = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        reconciled_late,
        "pattern consumer must reconcile the late-created topic into its set, topics now: {:?}",
        reconcile.topics(),
    );

    // The late topic's message must reach the SAME listener (inheritance).
    tokio::time::timeout(Duration::from_secs(20), saw_late.notified())
        .await
        .expect("late-discovered pattern child's message must reach the inherited listener");

    let got = received.lock().clone();
    assert!(
        got.iter()
            .any(|(t, p)| t == topic_initial && p == "pat-initial-0"),
        "listener saw the initial topic's message, got {got:?}",
    );
    assert!(
        got.iter()
            .any(|(t, p)| t == topic_late && p == "pat-late-0"),
        "listener saw the late-discovered topic's message (inheritance), got {got:?}",
    );

    handle.close().await;
    assert!(
        !handle.is_running(),
        "pattern poller stops cleanly after close"
    );
    Ok(())
}
