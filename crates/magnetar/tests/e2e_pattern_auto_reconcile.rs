// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for [`magnetar::PatternConsumer::start_auto_reconcile`]
//! against a real Apache Pulsar 4.x standalone broker.
//!
//! Mirrors the Java client's `PatternMultiTopicsConsumerImpl#recheckTopics`
//! cycle, exercised by `pulsar-broker/src/test/java/org/apache/pulsar/
//! client/api/PatternTopicsConsumerImplTest.java` — the recheck timer notices
//! topics that were created **after** the pattern consumer was built and
//! subscribes to them.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_pattern_auto_reconcile -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::InitialPosition;
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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
        .with_wait_for(WaitFor::message_on_stdout("Created namespace public/default"))
        .with_startup_timeout(Duration::from_secs(120))
        // The test pattern uses a UUID suffix that pushes the regex past the
        // broker's default `subscriptionPatternMaxLength=50` limit. Bump it
        // via PULSAR_PREFIX_; the image's CMD is `sh` (no entrypoint that
        // wires the env-config helper), so we apply it explicitly before
        // launching the broker.
        .with_env_var("PULSAR_PREFIX_subscriptionPatternMaxLength", "200")
        .with_cmd(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ \
                 conf/standalone.conf && bin/pulsar standalone"
                .to_owned(),
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

/// Reconciler ticker rediscovers topics created *after* the [`PatternConsumer`]
/// was subscribed. We start with one matching topic, build the consumer, ask
/// the broker to create a second matching topic, publish to it, and assert the
/// ticker subscribed and delivered the message within the ticker interval.
///
/// Mirrors Java `PatternTopicsConsumerImplTest#testPatternTopicsConsumerCheck`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_pattern_auto_reconcile_picks_up_new_topic() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    // Share a UUID across the two test topics so a regex anchored at a unique
    // prefix matches exactly them and nothing else lingering on the broker.
    let suite = uuid::Uuid::new_v4().simple().to_string();
    let topic_a = format!("persistent://public/default/magnetar-e2e-patrec-{suite}-aa");
    let topic_b = format!("persistent://public/default/magnetar-e2e-patrec-{suite}-bb");
    let pattern = format!("persistent://public/default/magnetar-e2e-patrec-{suite}-.*");

    let client = Arc::new(
        PulsarClient::builder()
            .service_url(service_url)
            .build()
            .await?,
    );

    // Pre-create topic A and publish a sentinel — this guarantees the topic
    // shows up in the broker's namespace listing when the pattern consumer
    // builds its initial snapshot.
    {
        let producer = client.producer(&topic_a).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"sentinel-a".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    let pattern_consumer = client
        .pattern_consumer()
        .namespace("public/default")
        .pattern(&pattern)
        .subscription(format!("magnetar-patrec-{suite}"))
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    assert_eq!(
        pattern_consumer.len(),
        1,
        "initial snapshot must contain only topic A"
    );

    // Drain the sentinel from topic A so the post-reconcile receive only
    // surfaces traffic from topic B.
    let sentinel = tokio::time::timeout(Duration::from_secs(15), pattern_consumer.receive())
        .await
        .map_err(|_| "timed out waiting for topic A sentinel")??;
    pattern_consumer
        .ack(&sentinel.topic, sentinel.message.message_id)
        .await?;

    // Spawn the auto-reconcile loop. A short interval keeps the test snappy
    // without depending on the Java client's 60s default.
    let reconcile_interval = Duration::from_secs(2);
    let reconcile_handle =
        pattern_consumer.start_auto_reconcile(client.clone(), reconcile_interval);

    // Create topic B *after* the pattern consumer is live. We publish to it
    // unconditionally — the namespace lookup driving the reconcile is what
    // picks the topic up.
    let producer_b = client.producer(&topic_b).create().await?;
    producer_b
        .send(OutgoingMessage::with_payload(b"hello-b".to_vec()).into())
        .await?;
    producer_b.close().await?;

    // Wait for the reconciler to pick topic B up. We allow up to 6× the ticker
    // interval to absorb a slow broker namespace listing on first-touch.
    let deadline = Duration::from_secs(2) + reconcile_interval * 6;
    let wait_for_b = async {
        loop {
            if pattern_consumer.topics().iter().any(|t| t == &topic_b) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    tokio::time::timeout(deadline, wait_for_b)
        .await
        .map_err(|_| {
            format!(
                "auto-reconcile did not subscribe to topic B within {deadline:?}; \
                 topics={:?}",
                pattern_consumer.topics()
            )
        })?;

    // Now drain a single message — it MUST be from topic B (the only producer
    // we ran post-snapshot). Receive can race the reconcile briefly when the
    // ticker fires mid-publish, so we tolerate up to two messages: any extra
    // from topic A is a leftover the broker delivered late.
    let mut got_b = false;
    for _ in 0..4 {
        let msg = tokio::time::timeout(Duration::from_secs(10), pattern_consumer.receive())
            .await
            .map_err(|_| "timed out waiting for topic B message")??;
        pattern_consumer
            .ack(&msg.topic, msg.message.message_id)
            .await?;
        if msg.topic == topic_b {
            assert_eq!(msg.message.payload.as_ref(), b"hello-b");
            got_b = true;
            break;
        }
    }
    assert!(
        got_b,
        "pattern consumer never delivered a message from the freshly-discovered topic B"
    );

    reconcile_handle.abort();
    // Wait for the spawned task to drop its `Arc<PulsarClient>` before we
    // close. `JoinError::is_cancelled()` is the expected outcome here.
    let _ = reconcile_handle.await;
    pattern_consumer.close().await?;
    let client = Arc::try_unwrap(client)
        .map_err(|_| "client still has outstanding Arc handles after reconcile abort")?;
    client.close().await;
    Ok(())
}
