// SPDX-License-Identifier: Apache-2.0

//! E2E: `PartitionedConsumer::seek_per_partition(F)` — per-topic seek callback.
//!
//! Java parity: `Consumer#seekAsync(Function<String, Object>)` (PIP-145 fan-out
//! over partitioned topics). See `README.md#java-client-parity-matrix` row 458.
//!
//! Scenario:
//!   1. Create a 4-partition topic.
//!   2. Produce N messages per partition with a `key-<partition>` partition key so the default
//!      routing maps each key to its own partition.
//!   3. Capture a `publish_time_ms` mid-point sentinel between halves on partition 0.
//!   4. Subscribe a fresh `PartitionedConsumer`, then call `seek_per_partition(|topic| if topic
//!      ends with "-partition-0" then PublishTimeMs(mid) else PublishTimeMs(0))`.
//!   5. Verify that partition 0 yields only the post-seek tail while the other three partitions
//!      yield their full contents.
//!
//! Gated behind the `e2e` feature flag.

#![cfg(feature = "e2e")]

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient, SeekTarget};
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
        .with_wait_for(WaitFor::message_on_stdout("messaging service is ready"))
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
#[allow(clippy::too_many_lines)]
async fn e2e_seek_per_partition_callback() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;

    // Create a 4-partition topic via the admin API.
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let topic = "persistent://public/default/magnetar-e2e-seek-per-partition";
    admin.topic_create_partitioned(topic, 4).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // First half: produce N messages per partition (keyed so each key lands on
    // a single partition under the default hash router).
    let producer = client.producer(topic).create().await?;
    let half = 5usize;
    let partitions = 4usize;
    for i in 0..half {
        for p in 0..partitions {
            producer
                .send(
                    OutgoingMessage::with_payload(format!("first-{p}-{i}").into_bytes())
                        .key(format!("key-{p}"))
                        .into(),
                )
                .await?;
        }
    }

    // Sentinel boundary between halves — every later message has
    // `publish_time >= mid_ms`. The sleep gives the broker a clear gap and
    // dodges clock-skew on slow CI hosts.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mid_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Second half.
    for i in 0..half {
        for p in 0..partitions {
            producer
                .send(
                    OutgoingMessage::with_payload(format!("second-{p}-{i}").into_bytes())
                        .key(format!("key-{p}"))
                        .into(),
                )
                .await?;
        }
    }
    producer.close().await?;

    // Subscribe a fresh PartitionedConsumer from the earliest position so the
    // pre-seek baseline reads every message.
    let consumer = client
        .partitioned_consumer(topic)
        .subscription("magnetar-e2e-seek-per-partition")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Seek per-partition: rewind partition-0 to `mid_ms`, leave the others at
    // the earliest position (epoch 0 = "start of time").
    let topic_owned = topic.to_owned();
    consumer
        .seek_per_partition(move |child_topic| {
            if child_topic == format!("{topic_owned}-partition-0") {
                SeekTarget::PublishTimeMs(mid_ms)
            } else {
                SeekTarget::PublishTimeMs(0)
            }
        })
        .await?;

    // After seek, drain. Expectation:
    //   - partition-0 yields only the `second-0-*` tail (5 msgs).
    //   - partitions 1..=3 yield both halves (10 msgs each).
    let expected_partition0 = half;
    let expected_others = 2 * half;
    let expected_total = expected_partition0 + expected_others * (partitions - 1);

    let mut per_topic: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for _ in 0..expected_total {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive()).await??;
        let payload = String::from_utf8(msg.message.payload.to_vec())?;
        per_topic
            .entry(msg.topic.clone())
            .or_default()
            .push(payload);
        consumer.ack(&msg.topic, msg.message.message_id).await?;
    }
    // Make sure no surprise straggler arrives on partition-0 from before `mid_ms`.
    let stray = tokio::time::timeout(Duration::from_millis(500), consumer.receive()).await;
    assert!(
        stray.is_err(),
        "no further messages expected after the post-seek tail drained: {stray:?}",
    );

    consumer.close().await?;
    client.close().await;

    // Per-partition assertion: partition-0 saw only `second-0-*`; the others
    // saw both halves. We compare on the payload prefix to keep ordering
    // expectations relaxed (Shared dispatch doesn't promise FIFO across keys).
    let p0_key = format!("{topic}-partition-0");
    let p0 = per_topic.get(&p0_key).cloned().unwrap_or_default();
    assert_eq!(
        p0.len(),
        expected_partition0,
        "partition-0 should only yield the post-seek tail; got {p0:?}",
    );
    assert!(
        p0.iter().all(|s| s.starts_with("second-0-")),
        "partition-0 payloads must all be from the post-seek half; got {p0:?}",
    );

    for p in 1..partitions {
        let key = format!("{topic}-partition-{p}");
        let msgs = per_topic.get(&key).cloned().unwrap_or_default();
        assert_eq!(
            msgs.len(),
            expected_others,
            "partition-{p} should yield both halves; got {msgs:?}",
        );
    }
    Ok(())
}
