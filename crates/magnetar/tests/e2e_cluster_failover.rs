// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for PIP-121 controlled cluster failover.
//!
//! Verifies that a [`magnetar_proto::ControlledClusterFailover`] threaded
//! through `ClientBuilder::service_url_provider(...)` causes the
//! supervised reconnect loop to redial against the swapped URL — i.e.
//! the client can be steered from broker-A to broker-B from outside
//! without rebuilding the client.
//!
//! **Scenario** — two Pulsar 4.x standalone containers on independent
//! host-port mappings. The client starts pointed at broker-A, round-trips
//! a message, then we:
//!
//! 1. swap the failover provider's URL to broker-B,
//! 2. stop broker-A to force a disconnect (the provider is only consulted on reconnect, so an
//!    in-flight session would otherwise keep using A),
//! 3. send another message; the supervisor redials, the provider hands out broker-B's URL, and the
//!    producer rebuild lands on B.
//!
//! Routing is asserted via broker-B's admin REST: the post-swap topic
//! shows a non-zero `msg_in_counter`, while the identically-named topic
//! on broker-A (which was never written to before the swap) was deleted
//! along with broker-A on shutdown.
//!
//! ## Skipped sub-tests (deferred scope)
//!
//! * **`AutoClusterFailover` + `HealthProbe`** — the runtime engine already ships the auto variant,
//!   but exercising it end-to-end needs a probe that flips its verdict in lock-step with the live
//!   cluster state; tracking that without a channel (per ADR-0003) tangles the test plumbing more
//!   than it pays back. The controlled variant covers the underlying supervisor + provider
//!   contract.
//! * **PIP-188 `TOPIC_MIGRATED` injection** — requires either a broker admin operation that emits
//!   the frame on demand or a fake broker that synthesizes it. `magnetar-fakes` has no
//!   `CommandTopicMigrated` emitter today; the supervised-reset path is unit-tested in
//!   `magnetar-proto` instead.
//!
//! Gated behind the `e2e` feature flag. Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_cluster_failover -- --nocapture --ignored
//! ```

#![cfg(feature = "e2e")]

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::ControlledClusterFailover;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

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

/// Start a Pulsar 4.x standalone container and return
/// (`service_url`, `admin_url`, `container_handle`). Each call provisions
/// a fresh container with Docker-assigned host ports, so spinning two of
/// them in parallel yields two independent brokers.
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

/// Generous reconnect budget — broker-B is a separate container that's
/// already up before the swap, but the supervisor's first redial after
/// broker-A goes away can race against in-flight ops and TCP timeouts.
fn supervisor_for_e2e() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(5),
        mandatory_stop: Duration::from_secs(180),
        max_attempts: None,
    }
}

#[ignore = "e2e: requires Docker (two containers)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_controlled_cluster_failover_manual_swap() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url_a, _admin_url_a, container_a) = start_pulsar().await?;
    let (service_url_b, admin_url_b, _container_b) = start_pulsar().await?;
    tracing::info!(%service_url_a, %service_url_b, "two pulsar brokers up");

    let failover = ControlledClusterFailover::new(service_url_a.clone());
    let provider: Arc<dyn magnetar::proto::ServiceUrlProvider> = Arc::new(failover.clone());

    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .enable_reconnect(supervisor_for_e2e())
        .operation_timeout(Duration::from_secs(60))
        .build()
        .await?;

    let topic = format!(
        "persistent://public/default/magnetar-e2e-failover-{}",
        Uuid::new_v4()
    );
    let subscription = format!("magnetar-e2e-failover-sub-{}", Uuid::new_v4());

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await?;

    // Round-trip against broker-A so we know the session is healthy.
    producer
        .send(OutgoingMessage::with_payload(b"on-broker-a".to_vec()).into())
        .await?;
    let pre = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(pre.payload.as_ref(), b"on-broker-a");
    consumer.ack(pre.message_id).await?;

    // Flip the provider to broker-B and tear down broker-A. The provider
    // is consulted on every reconnect, so the supervisor's next redial
    // must land on broker-B.
    tracing::info!(%service_url_b, "swapping failover provider to broker-b");
    failover.set_url(service_url_b.clone());
    assert_eq!(failover.current_url(), service_url_b);

    tracing::info!("stopping broker-a to force reconnect");
    container_a.stop_with_timeout(Some(5)).await?;

    // Drain producer.send() until the supervisor has reconnected to
    // broker-B. The first attempts will likely fail with a session-lost
    // error while the supervisor is still backing off; retry until the
    // rebuild lands or the budget is exhausted.
    let payload = b"on-broker-b".to_vec();
    let mut attempts = 0u32;
    let send_outcome = loop {
        attempts += 1;
        match producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await
        {
            Ok(_message_id) => break Ok(()),
            Err(e) if attempts < 30 => {
                tracing::info!(?e, attempts, "producer send retry during failover");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => break Err(e),
        }
    };
    send_outcome?;

    // Consumer should also have been rebuilt against broker-B.
    let post = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        post.payload.as_ref(),
        payload.as_slice(),
        "consumer must receive the post-failover message via broker-b",
    );
    consumer.ack(post.message_id).await?;

    // Assert routing via broker-b's admin REST: the topic must show a
    // non-zero `msg_in_counter`. broker-a is gone, so a parallel query
    // there would fail anyway — we only check b.
    let admin_b = magnetar_admin::AdminClient::builder()
        .service_url(admin_url_b.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let stats_b = admin_b.topic_stats(&topic).await?;
    assert!(
        stats_b.msg_in_counter > 0,
        "broker-b must have received the post-swap publish; \
         msg_in_counter={} on broker-b for topic {topic}",
        stats_b.msg_in_counter,
    );

    consumer.close().await?;
    producer.close().await?;
    client.close().await;
    Ok(())
}
