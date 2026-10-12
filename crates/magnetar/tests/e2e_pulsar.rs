// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests against a real Apache Pulsar 4.x standalone broker
//! spun up via `testcontainers-rs`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_pulsar -- --nocapture
//! ```
//!
//! Requires Docker on the host.
//!
//! ## Image
//!
//! The broker image comes from the shared fixture in `common/pulsar.rs`
//! (ADR-0115): its `DEFAULT_TAG`, overridable with `MAGNETAR_PULSAR_IMAGE_TAG`
//! for a compatibility run.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

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

/// Issues #303 / #319 driver fairness sanity: a large burst of CONCURRENT
/// in-flight sends with non-trivial payloads against a real broker all resolve
/// with receipts. The driver loop must read back every `CommandSendReceipt`
/// promptly while the outbound `driver_waker` is hammered and the write side
/// crosses the per-iteration budget. A regression that starved receipt reads
/// would surface here as the burst-join timing out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_burst_all_receipts_resolve() -> Result<(), Box<dyn std::error::Error>> {
    const BURST: usize = 512;

    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-send-burst";

    let producer = std::sync::Arc::new(client.producer(topic).create().await?);

    // Fire the whole burst before awaiting any of it, so the driver loop is
    // driven under sustained `driver_waker` pressure while the broker streams
    // receipts back. Every `SendFut` must resolve.
    let mut tasks = Vec::with_capacity(BURST);
    for i in 0..BURST {
        let p = producer.clone();
        tasks.push(tokio::spawn(async move {
            let payload = vec![u8::try_from(i % 251).unwrap_or(0); 4 * 1024];
            p.send(OutgoingMessage::with_payload(payload).into()).await
        }));
    }

    let mut acked = 0usize;
    for (i, t) in tasks.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .map_err(|_| format!("send #{i} of {BURST} did not resolve within 30s"))?
            .map_err(|e| format!("send task #{i} panicked: {e:?}"))?;
        result.map_err(|e| format!("send #{i} of {BURST} failed: {e:?}"))?;
        acked += 1;
    }
    assert_eq!(acked, BURST, "every send in the burst must resolve");

    std::sync::Arc::try_unwrap(producer)
        .map_err(|_| "producer still shared")?
        .close()
        .await?;
    client.close().await;
    Ok(())
}

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
    let producer = client.partitioned_producer(topic).create().await?;

    let n = 16usize;
    for i in 0..n {
        producer
            .send(
                OutgoingMessage::with_payload(format!("msg-{i}").into_bytes())
                    .key(format!("key-{}", i % 4)),
            )
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .partitioned_consumer(topic)
        .subscription("magnetar-e2e-partitioned")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = 0usize;
    while received < n {
        // Timeout guard mirrors e2e_partitioned_consumer_aggregates_all: a
        // partition-discovery miss fails fast instead of hanging the harness.
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("partitioned consumer timeout: got {received} of {n}"))??;
        received += 1;
        consumer.ack(&msg.topic, msg.message.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, n);
    Ok(())
}

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
