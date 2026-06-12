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
//! Runs as a regular test under `cargo test` (ADR-0046 — the former
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
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};

// Host-side ports — match `fixtures/docker-compose.replicated-subs.yml`.
// They're off the default Pulsar 6650 to avoid colliding with the
// `testcontainers`-spawned brokers that other e2e tests start (those
// brokers advertise themselves as `localhost:8080` internally, so a
// fixed `localhost:6650` mapping here would route their lookup
// responses back to the wrong broker).
const CLUSTER_A_URL: &str = "pulsar://localhost:16650";
const CLUSTER_B_URL: &str = "pulsar://localhost:16651";

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
    let topic_name = format!(
        "pip-33-failover-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let topic = format!("persistent://public/default/{topic_name}");
    let subscription = "sub-pip-33-failover";

    // (1) Subscribe with replicated state BEFORE producing: the PIP-33
    // controller snapshots the topic ONLY while new publishes arrive, and a
    // cursor UPDATE ships only when the acked position crosses a COMPLETED
    // snapshot. Subscribing first interleaves snapshot markers through the
    // message stream at positions the acks below will cross; subscribing
    // after the backlog leaves a single snapshot at the stream tail that 50
    // acks can never reach, so nothing ever materializes on cluster-b.
    // (`Earliest` is belt-and-braces — the subscription exists before the
    // first publish.) Every await is timeout-bounded so environmental broker
    // death fails the test fast instead of hanging (same hygiene as the
    // `e2e_reconnect` send loop).
    let client_a = build_client(CLUSTER_A_URL).await?;
    let producer = client_a.producer(&topic).create().await?;
    let consumer_a = client_a
        .consumer(&topic)
        .subscription(subscription)
        .subscription_type(SubType::Failover)
        .initial_position(InitialPosition::Earliest)
        .replicate_subscription_state(true)
        .subscribe()
        .await?;

    // (2) Produce 100 messages, paced so several 1s snapshot windows
    // elapse mid-stream (snapshot freq is pinned to 1000ms in the fixture).
    for i in 0..100u32 {
        tokio::time::timeout(
            Duration::from_secs(10),
            producer.send(
                magnetar::OutgoingMessage::with_payload(format!("msg-{i}").into_bytes()).into(),
            ),
        )
        .await
        .map_err(|_| format!("send of msg-{i} on cluster-a timed out after 10s"))??;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    producer.close().await?;
    // (3) Consume 50 + ack, paced: the mark-delete position crosses the
    // mid-stream snapshots one window at a time.
    let mut consumed_a = 0;
    for i in 0..50 {
        let msg = tokio::time::timeout(Duration::from_secs(10), consumer_a.receive())
            .await
            .map_err(|_| {
                format!("cluster-a receive #{i} timed out after 10s (consumed {consumed_a}/50)")
            })??;
        consumer_a.ack(msg.message_id).await?;
        consumed_a += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(consumed_a, 50);

    // (4) Wait until the replicated cursor actually MATERIALIZES on
    // cluster-b — the cross-cluster snapshot/update cycle is asynchronous,
    // so a fixed sleep is a race, not a barrier.
    let subs_url = format!(
        "http://localhost:18081/admin/v2/persistent/public/default/{topic_name}/subscriptions"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut materialized = false;
    while tokio::time::Instant::now() < deadline && !materialized {
        if let Ok(resp) = reqwest::get(&subs_url).await {
            if let Ok(subs) = resp.json::<Vec<String>>().await {
                materialized = subs.iter().any(|s| s == subscription);
            }
        }
        if !materialized {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    assert!(
        materialized,
        "PIP-33: the replicated subscription never materialized on cluster-b within 30s of \
         the acked position advancing across snapshot windows",
    );

    // (5) Tear down the cluster-a consumer + client.
    consumer_a.close().await?;
    client_a.close().await;

    // (6) Reconnect to cluster-b and consume.
    let client_b = build_client(CLUSTER_B_URL).await?;
    let consumer_b = tokio::time::timeout(
        Duration::from_secs(15),
        client_b
            .consumer(&topic)
            .subscription(subscription)
            .subscription_type(SubType::Failover)
            .replicate_subscription_state(true)
            .subscribe(),
    )
    .await
    .map_err(|_| "cluster-b subscribe timed out after 15s")??;

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

/// PIP-33 cursor-sync sanity: a subscription created with
/// `replicate_subscription_state(true)` on cluster-a, whose acked position
/// advances across broker snapshot windows, must MATERIALIZE on cluster-b —
/// the broker-side effect of the snapshot/update marker cycle.
///
/// Real brokers never dispatch `REPLICATED_SUBSCRIPTION_*` marker entries to
/// client consumers (the dispatcher filters marker entries off consumer
/// delivery; verified against Pulsar 4.x — the marker entries are present in
/// the topic ledger but a from-earliest consumer receives only the data
/// messages), so the client-side observation channel
/// (`poll_replicated_subscription_marker`) is exercisable only against the
/// scripted sim brokers — the moonpool / differential
/// `replicated_subscriptions` suites own that surface. Against a real broker
/// the observable PIP-33 contract is the remote-cluster subscription
/// appearing once the acked position crossed a snapshot, which is what this
/// test pins (PIP-33 replicates the ACKED position: a never-acked cursor
/// produces nothing for cluster-b to materialize).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_subscription_materializes_on_remote_cluster()
-> Result<(), Box<dyn std::error::Error>> {
    let topic_name = format!(
        "pip-33-observe-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let topic = format!("persistent://public/default/{topic_name}");
    let subscription = "sub-pip-33-observe";
    let client = build_client(CLUSTER_A_URL).await?;
    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription(subscription)
        .subscription_type(SubType::Exclusive)
        .replicate_subscription_state(true)
        .subscribe()
        .await?;

    // Produce a steady trickle so several 1s snapshot windows elapse
    // (broker snapshot freq is pinned to 1000ms in the fixture), acking
    // every message so the replicated cursor has progress to ship.
    for i in 0..20u32 {
        producer
            .send(
                magnetar::OutgoingMessage::with_payload(format!("trigger-{i}").into_bytes()).into(),
            )
            .await?;
        let msg = tokio::time::timeout(Duration::from_secs(5), consumer.receive()).await??;
        consumer.ack(msg.message_id).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Poll cluster-b's admin REST until the replicated subscription
    // materializes, keeping a paced ack'd trickle alive so the
    // snapshot/update cycle has traffic to ride on.
    let subs_url = format!(
        "http://localhost:18081/admin/v2/persistent/public/default/{topic_name}/subscriptions"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut materialized = false;
    let mut i = 20u32;
    while tokio::time::Instant::now() < deadline && !materialized {
        if let Ok(resp) = reqwest::get(&subs_url).await {
            if let Ok(subs) = resp.json::<Vec<String>>().await {
                materialized = subs.iter().any(|s| s == subscription);
            }
        }
        if !materialized {
            producer
                .send(
                    magnetar::OutgoingMessage::with_payload(format!("trigger-{i}").into_bytes())
                        .into(),
                )
                .await?;
            i += 1;
            if let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_secs(2), consumer.receive()).await
            {
                consumer.ack(msg.message_id).await?;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    consumer.close().await?;
    producer.close().await?;
    client.close().await;
    assert!(
        materialized,
        "PIP-33: the replicated subscription must materialize on cluster-b once the acked \
         position has crossed a snapshot window (cursor sync never reached the remote cluster)",
    );
    Ok(())
}
