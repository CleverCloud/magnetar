// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the last-clone consumer drop guard (issue #342).
//!
//! The broker's topic-stats response is the external oracle: a named consumer must disappear from
//! `subscriptions[subscription].consumers[].consumerName` after its final clone drops while the
//! client and shared connection remain alive. Reopening the same name and subscription on that
//! client, then receiving a message, proves the connection remains usable and the broker-side
//! consumer registration was released.
//! The drop path also buffers a grouped acknowledgement before releasing the final clone and
//! verifies that same-subscription recreation does not redeliver the acknowledged message.
//! The explicit-close baseline additionally proves that closing through one clone and dropping the
//! final clone keeps broker state absent and same-name recreation usable. Exact one-frame dedup is
//! asserted by the paired runtime `consumer_drop_close.rs` tests because topic stats cannot count
//! wire frames.
//!
//! Runs as a regular test under `cargo test` (ADR-0046) and requires Docker on the host.

use std::time::{Duration, Instant};

use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar_admin::{AdminClient, TopicStats};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const ADMIN_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const ADMIN_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DROP_SUBSCRIPTION: &str = "consumer-drop-sub";
const DROP_CONSUMER_NAME: &str = "consumer-drop-name";
const EXPLICIT_SUBSCRIPTION: &str = "consumer-explicit-close-sub";
const EXPLICIT_CONSUMER_NAME: &str = "consumer-explicit-close-name";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

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

async fn start_pulsar() -> TestResult<(String, String, testcontainers::ContainerAsync<GenericImage>)>
{
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

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

fn registered_consumer_names(stats: &TopicStats, subscription: &str) -> TestResult<Vec<String>> {
    let Some(subscription_stats) = stats.subscriptions.get(subscription) else {
        return Ok(Vec::new());
    };
    let consumers = subscription_stats
        .get("consumers")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "subscription `{subscription}` stats lack a `consumers` array: \
                 {subscription_stats}"
            )
        })?;
    Ok(consumers
        .iter()
        .filter_map(|consumer| {
            consumer
                .get("consumerName")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect())
}

async fn wait_for_consumer_presence(
    admin: &AdminClient,
    topic: &str,
    subscription: &str,
    consumer_name: &str,
    expected_present: bool,
) -> TestResult {
    let deadline = Instant::now() + ADMIN_POLL_TIMEOUT;
    loop {
        let stats = admin.topic_stats(topic).await?;
        let names = registered_consumer_names(&stats, subscription)?;
        let present = names.iter().any(|name| name == consumer_name);
        if present == expected_present {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "consumer `{consumer_name}` presence did not become {expected_present} within \
                 {ADMIN_POLL_TIMEOUT:?}; last registered names for subscription `{subscription}`: \
                 {names:?}"
            )
            .into());
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    }
}

async fn verify_final_clone_drop(client: &PulsarClient, admin: &AdminClient) -> TestResult {
    let drop_topic = unique_topic("magnetar-e2e-consumer-drop");
    let consumer = client
        .consumer(&drop_topic)
        .subscription(DROP_SUBSCRIPTION)
        .subscription_type(SubType::Exclusive)
        .durable(true)
        .name(DROP_CONSUMER_NAME)
        .ack_group_time(Duration::from_secs(60))
        .subscribe()
        .await?;
    wait_for_consumer_presence(
        admin,
        &drop_topic,
        DROP_SUBSCRIPTION,
        DROP_CONSUMER_NAME,
        true,
    )
    .await?;

    let producer = client.producer(&drop_topic).create().await?;
    producer
        .send_bytes(b"consumer-drop-before-close".to_vec())
        .await?;
    let message = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(message.payload.as_ref(), b"consumer-drop-before-close");
    consumer.ack_grouped(message.message_id);

    // Release the final clone without `close().await` while `client` remains alive.
    drop(consumer);
    wait_for_consumer_presence(
        admin,
        &drop_topic,
        DROP_SUBSCRIPTION,
        DROP_CONSUMER_NAME,
        false,
    )
    .await?;

    let recreated = client
        .consumer(&drop_topic)
        .subscription(DROP_SUBSCRIPTION)
        .subscription_type(SubType::Exclusive)
        .durable(true)
        .name(DROP_CONSUMER_NAME)
        .subscribe()
        .await?;
    wait_for_consumer_presence(
        admin,
        &drop_topic,
        DROP_SUBSCRIPTION,
        DROP_CONSUMER_NAME,
        true,
    )
    .await?;

    match tokio::time::timeout(Duration::from_secs(1), recreated.receive()).await {
        Err(_) => {}
        Ok(Ok(message)) => {
            return Err(format!(
                "grouped acknowledgement was lost during consumer drop; recreated consumer \
                 received payload {:?}",
                message.payload
            )
            .into());
        }
        Ok(Err(error)) => return Err(error.into()),
    }

    producer
        .send_bytes(b"consumer-drop-round-trip".to_vec())
        .await?;
    let message = tokio::time::timeout(Duration::from_secs(10), recreated.receive()).await??;
    assert_eq!(message.payload.as_ref(), b"consumer-drop-round-trip");
    recreated.ack(message.message_id).await?;
    recreated.close().await?;
    wait_for_consumer_presence(
        admin,
        &drop_topic,
        DROP_SUBSCRIPTION,
        DROP_CONSUMER_NAME,
        false,
    )
    .await?;
    producer.close().await?;
    Ok(())
}

async fn verify_explicit_close_baseline(client: &PulsarClient, admin: &AdminClient) -> TestResult {
    // Explicit-close baseline: topic stats prove broker state stays absent after the final clone
    // drops and same-name recreation remains usable. The paired runtime tests assert that this
    // sequence emits exactly one CloseConsumer frame.
    let explicit_topic = unique_topic("magnetar-e2e-consumer-explicit-close");
    let explicit_consumer = client
        .consumer(&explicit_topic)
        .subscription(EXPLICIT_SUBSCRIPTION)
        .subscription_type(SubType::Exclusive)
        .durable(true)
        .name(EXPLICIT_CONSUMER_NAME)
        .subscribe()
        .await?;
    wait_for_consumer_presence(
        admin,
        &explicit_topic,
        EXPLICIT_SUBSCRIPTION,
        EXPLICIT_CONSUMER_NAME,
        true,
    )
    .await?;

    let surviving_clone = explicit_consumer.clone();
    explicit_consumer.close().await?;
    drop(surviving_clone);
    wait_for_consumer_presence(
        admin,
        &explicit_topic,
        EXPLICIT_SUBSCRIPTION,
        EXPLICIT_CONSUMER_NAME,
        false,
    )
    .await?;

    let explicit_recreated = client
        .consumer(&explicit_topic)
        .subscription(EXPLICIT_SUBSCRIPTION)
        .subscription_type(SubType::Exclusive)
        .durable(true)
        .name(EXPLICIT_CONSUMER_NAME)
        .subscribe()
        .await?;
    wait_for_consumer_presence(
        admin,
        &explicit_topic,
        EXPLICIT_SUBSCRIPTION,
        EXPLICIT_CONSUMER_NAME,
        true,
    )
    .await?;
    explicit_recreated.close().await?;
    wait_for_consumer_presence(
        admin,
        &explicit_topic,
        EXPLICIT_SUBSCRIPTION,
        EXPLICIT_CONSUMER_NAME,
        false,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_consumer_drop_unregisters_and_explicit_close_baseline() -> TestResult {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    verify_final_clone_drop(&client, &admin).await?;
    verify_explicit_close_baseline(&client, &admin).await?;
    client.close().await;
    Ok(())
}
