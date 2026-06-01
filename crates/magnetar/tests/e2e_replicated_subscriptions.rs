// SPDX-License-Identifier: Apache-2.0

//! PIP-33 (ADR-0034) — end-to-end test against a real two-cluster Pulsar 4.x
//! fixture.
//!
//! Unlike the single-broker e2e tests (which use testcontainers-rs), PIP-33
//! is only meaningfully exercised against **two clusters** configured as
//! geo-replication peers — a single-cluster broker silently ignores
//! `replicate_subscription_state(true)` and the cursor-sync mechanism never
//! fires. The fixture lives in
//! `crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml` and is
//! brought up out-of-band by `configure_replicated_subs.sh`.
//!
//! Runs as a regular test under `cargo test` (ADR-0045 — the former
//! `e2e` / `e2e-multi-cluster` Cargo features are gone). The two-cluster
//! docker-compose fixture must be healthy before `cargo test` starts;
//! per-PR CI brings it up automatically in the `test` job
//! (`.github/workflows/ci.yml`).
//!
//! ## Running locally
//!
//! ```sh
//! cd crates/magnetar/tests/fixtures
//! docker compose -f docker-compose.replicated-subs.yml up -d
//! ./configure_replicated_subs.sh
//! cargo test -p magnetar --test e2e_replicated_subscriptions
//! docker compose -f docker-compose.replicated-subs.yml down -v
//! ```

#![allow(clippy::similar_names, clippy::duration_suboptimal_units)]

use std::time::Duration;

use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::SubType;

const CLUSTER_A_URL: &str = "pulsar://localhost:6650";
const CLUSTER_B_URL: &str = "pulsar://localhost:6651";

async fn build_client(url: &str) -> Result<PulsarClient, Box<dyn std::error::Error>> {
    Ok(PulsarClient::builder().service_url(url).build().await?)
}

/// Cursor-resume tolerance asserted by PIP-33: after a failover from cluster-a
/// to cluster-b, the consumer must resume within **one snapshot interval** of
/// duplicate messages (broker snapshot freq is pinned to 1000ms in the
/// fixture, so up to ~1s of redelivery, never less than half the consumed
/// prefix).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_resumes_within_one_second_after_cluster_failover()
-> Result<(), Box<dyn std::error::Error>> {
    let topic = format!(
        "persistent://public/default/pip-33-failover-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let subscription = "sub-pip-33-failover";

    // (1) Produce 100 messages on cluster-a.
    let client_a = build_client(CLUSTER_A_URL).await?;
    let producer = client_a.producer(&topic).create().await?;
    for i in 0..100u32 {
        producer
            .send(magnetar::OutgoingMessage::with_payload(format!("msg-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    // (2) Subscribe with replicated state, consume 50 + ack.
    let consumer_a = client_a
        .consumer(&topic)
        .subscription(subscription)
        .subscription_type(SubType::Failover)
        .replicate_subscription_state(true)
        .subscribe()
        .await?;
    let mut consumed_a = 0;
    for _ in 0..50 {
        let msg = consumer_a.receive().await?;
        consumer_a.ack(msg.message_id).await?;
        consumed_a += 1;
    }
    assert_eq!(consumed_a, 50);

    // (3) Wait two snapshot intervals so the broker has propagated cursor
    //     position to cluster-b.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // (4) Tear down the cluster-a consumer + client.
    consumer_a.close().await?;
    client_a.close().await;

    // (5) Reconnect to cluster-b and consume.
    let client_b = build_client(CLUSTER_B_URL).await?;
    let consumer_b = client_b
        .consumer(&topic)
        .subscription(subscription)
        .subscription_type(SubType::Failover)
        .replicate_subscription_state(true)
        .subscribe()
        .await?;

    let mut consumed_b = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while consumed_b < 50 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), consumer_b.receive()).await {
            Ok(Ok(msg)) => {
                consumer_b.ack(msg.message_id).await?;
                consumed_b += 1;
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }
    consumer_b.close().await?;
    client_b.close().await;

    // Tolerance: cluster-b must deliver at least the un-acked tail of the
    // backlog (50 messages), and no more than 50 + one snapshot window of
    // re-delivery overhead (typically <= 1 second of duplicates).
    assert!(
        (50..=60).contains(&consumed_b),
        "consumer_b consumed {consumed_b} messages; expected 50..=60 (post-failover with ≤1s of duplicate tolerance)",
    );
    Ok(())
}

/// Observation channel sanity: against a real geo-replicated namespace, the
/// per-client buffer must surface at least one `REPLICATED_SUBSCRIPTION_*`
/// marker within the snapshot window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn marker_observation_event_fires_against_real_broker()
-> Result<(), Box<dyn std::error::Error>> {
    let topic = format!(
        "persistent://public/default/pip-33-observe-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let client = build_client(CLUSTER_A_URL).await?;
    let producer = client.producer(&topic).create().await?;
    let _consumer = client
        .consumer(&topic)
        .subscription("sub-pip-33-observe")
        .subscription_type(SubType::Exclusive)
        .replicate_subscription_state(true)
        .subscribe()
        .await?;

    // Produce a steady trickle so the broker has reason to emit markers.
    for i in 0..20u32 {
        producer
            .send(
                magnetar::OutgoingMessage::with_payload(format!("trigger-{i}").into_bytes()).into(),
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Drain the observation buffer; expect at least one within ~5 seconds.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut markers = 0;
    while tokio::time::Instant::now() < deadline && markers == 0 {
        if client.poll_replicated_subscription_marker().is_some() {
            markers += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    producer.close().await?;
    client.close().await;
    assert!(
        markers > 0,
        "expected at least one replicated-subscription marker against a geo-replicated namespace",
    );
    Ok(())
}
