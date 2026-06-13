// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage of the PIP-466 V5 subscription-type surface
//! against a real Apache Pulsar 4.x broker. Mirrors `e2e_sub_types.rs`
//! (the v4 baseline) but drives the V5 `stream_consumer` /
//! `queue_consumer` builders to confirm V5 → v4 `SubType` translations
//! work end-to-end.
//!
//! Gated `feature = "e2e,experimental-v5-client"`. Run with:
//!
//! ```sh
//! cargo test --features experimental-v5-client \
//!   -p magnetar --test e2e_sub_types_v5 -- --nocapture
//! ```

#![cfg(feature = "experimental-v5-client")]

use std::time::Duration;

use bytes::Bytes;
use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::V5SubscriptionInitialPosition;
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

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
async fn e2e_v5_stream_consumer_failover_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // V5 stream_consumer().failover() → wire SubType::Failover. The
    // broker must accept the Failover subscription shape and the
    // active consumer must receive every message.
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let v4 = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let client = PulsarClientV5::from_v4(v4);
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-v5-failover-{suffix}");
    let subscription = format!("magnetar-v5-failover-{suffix}");

    // Two failover consumers on the same subscription — only one
    // becomes active at a time.
    let consumer_a = client
        .stream_consumer(&topic)
        .subscription(&subscription)
        .failover()
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .stream_consumer(&topic)
        .subscription(&subscription)
        .failover()
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let producer = client.producer(&topic).create().await?;
    let n = 10usize;
    for i in 0..n {
        producer
            .send(Bytes::from(format!("msg-{i}").into_bytes()))
            .await?;
    }

    let active_is_a = tokio::select! {
        first = tokio::time::timeout(Duration::from_secs(20), consumer_a.receive()) => {
            let msg = first??;
            consumer_a.ack(msg.id).await?;
            true
        }
        first = tokio::time::timeout(Duration::from_secs(20), consumer_b.receive()) => {
            let msg = first??;
            consumer_b.ack(msg.id).await?;
            false
        }
    };

    let active = if active_is_a {
        &consumer_a
    } else {
        &consumer_b
    };
    let mut received = 1usize;
    while received < n {
        let msg = tokio::time::timeout(Duration::from_secs(20), active.receive()).await??;
        received += 1;
        active.ack(msg.id).await?;
    }
    client.into_v4().close().await;
    assert_eq!(received, n);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_v5_queue_consumer_shared_distributes_messages()
-> Result<(), Box<dyn std::error::Error>> {
    // V5 queue_consumer() default → wire SubType::Shared. Two Shared
    // consumers split the work; total received across both must
    // equal published count (no duplicates, no drops on happy path).
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let v4 = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let client = PulsarClientV5::from_v4(v4);
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-v5-shared-{suffix}");
    let subscription = format!("magnetar-v5-shared-{suffix}");

    let c1 = client
        .queue_consumer(&topic)
        .subscription(&subscription)
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    let c2 = client
        .queue_consumer(&topic)
        .subscription(&subscription)
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let n = 20usize;
    for i in 0..n {
        producer
            .send(Bytes::from(format!("msg-{i}").into_bytes()))
            .await?;
    }

    let mut got1 = 0usize;
    let mut got2 = 0usize;
    while got1 + got2 < n {
        tokio::select! {
            m = c1.receive() => {
                let msg = m?;
                got1 += 1;
                c1.ack(msg.id).await?;
            }
            m = c2.receive() => {
                let msg = m?;
                got2 += 1;
                c2.ack(msg.id).await?;
            }
            () = tokio::time::sleep(Duration::from_secs(20)) => {
                panic!(
                    "Shared dispatch timed out before delivering {n} messages (got {got1}+{got2})"
                );
            }
        }
    }
    client.into_v4().close().await;
    assert_eq!(
        got1 + got2,
        n,
        "Shared dispatch total must equal published count"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_v5_queue_consumer_key_shared_per_key_ordering()
-> Result<(), Box<dyn std::error::Error>> {
    // V5 queue_consumer().key_shared() → wire SubType::KeyShared.
    // Per-key ordering must be preserved at the receiver (the broker
    // dispatches all messages for a given key to the same active
    // consumer). Keyed publishes need the v4 escape hatch — the V5
    // `Producer::send(Bytes)` doesn't expose `.key(...)` yet (that's
    // future V5 surface lift).
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let v4 = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let client = PulsarClientV5::from_v4(v4);
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-v5-key-shared-{suffix}");
    let subscription = format!("magnetar-v5-key-shared-{suffix}");

    let c1 = client
        .queue_consumer(&topic)
        .subscription(&subscription)
        .key_shared()
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    let c2 = client
        .queue_consumer(&topic)
        .subscription(&subscription)
        .key_shared()
        .initial_position(V5SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;

    // Keyed publish via the v4 escape hatch — V5 producer.send takes
    // only Bytes today.
    let producer = client.v4().producer(&topic).create().await?;
    let keys = ["alpha", "beta", "gamma"];
    let per_key = 5;
    for k in &keys {
        for seq in 0..per_key {
            producer
                .send(
                    OutgoingMessage::with_payload(format!("{k}-{seq}").into_bytes())
                        .key(k.to_string())
                        .into(),
                )
                .await?;
        }
    }

    let total = keys.len() * per_key;
    let mut received: Vec<(String, String)> = Vec::with_capacity(total);
    while received.len() < total {
        tokio::select! {
            m = c1.receive() => {
                let msg = m?;
                let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                let key = msg.metadata.partition_key.clone().unwrap_or_default();
                received.push((key, payload));
                c1.ack(msg.id).await?;
            }
            m = c2.receive() => {
                let msg = m?;
                let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                let key = msg.metadata.partition_key.clone().unwrap_or_default();
                received.push((key, payload));
                c2.ack(msg.id).await?;
            }
            () = tokio::time::sleep(Duration::from_secs(20)) => {
                panic!(
                    "KeyShared dispatch timed out: got {} of {total}",
                    received.len()
                );
            }
        }
    }
    client.into_v4().close().await;

    // Per-key ordering: filter received to each key, payloads must be
    // strictly monotonic sequence numbers.
    for k in &keys {
        let seq: Vec<usize> = received
            .iter()
            .filter(|(key, _)| key == k)
            .map(|(_, p)| {
                p.strip_prefix(&format!("{k}-"))
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(usize::MAX)
            })
            .collect();
        assert_eq!(
            seq.len(),
            per_key,
            "key {k} delivered {} of {per_key}",
            seq.len()
        );
        let mut expected: Vec<usize> = (0..per_key).collect();
        expected.sort_unstable();
        let mut got = seq.clone();
        got.sort_unstable();
        assert_eq!(got, expected, "key {k} missing or duplicated sequence");
        // Ordering: the as-received sequence must already be
        // monotonic because KeyShared sends each key to a single
        // active consumer.
        let mono = seq.windows(2).all(|w| w[0] < w[1]);
        assert!(mono, "key {k} out-of-order: {seq:?}");
    }
    Ok(())
}
