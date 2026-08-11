// SPDX-License-Identifier: Apache-2.0

//! F11 e2e — partitioned-topic-metadata fast-path against a real Pulsar
//! broker.
//!
//! When the caller asks for partitioned-topic metadata on a topic name
//! that already encodes a partition index (`<base>-partition-<N>` per
//! Java `TopicName#isPartitioned`), magnetar short-circuits to
//! `partitions = 0` without issuing a `CommandPartitionedTopicMetadata`
//! frame. Mirrors the streamnative-pulsar-rs #327 service-discovery
//! fix and cuts the per-partition lookup amplification from `N+1`
//! round-trips to `1` for a partitioned topic with `N` partitions.
//!
//! Observable claim:
//!
//! 1. `partitions_for_topic(parent)` returns `Ok(4)` (the real broker knows the parent topic has 4
//!    partitions).
//! 2. `partitions_for_topic("<parent>-partition-0")` returns `Ok(0)` instantly (the fast-path
//!    resolves it without any broker round-trip — which is exactly the Java client's behaviour).
//! 3. The fast-path resolves even when the per-partition child topic has NOT been activated yet (no
//!    producer / consumer attached). A naive implementation that always issues
//!    `CommandPartitionedTopicMetadata` would have to wait for the broker to perform a fresh
//!    lookup; the fast-path elides that entirely.

use std::time::Duration;

use magnetar::PulsarClient;

mod common;

use common::pulsar::start_pulsar;

const PARTITIONS: u32 = 4;

fn fresh_topic(suffix: &str) -> String {
    format!(
        "persistent://public/default/magnetar-fast-path-{}-{}",
        suffix,
        uuid::Uuid::new_v4().simple()
    )
}

async fn create_partitioned_topic(
    admin_url: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    admin.topic_create_partitioned(topic, PARTITIONS).await?;
    Ok(())
}

/// F11: the parent (partitioned) topic resolves to `Ok(PARTITIONS)`
/// against a real broker; the per-partition child name resolves to
/// `Ok(0)` synthetically via the fast-path. Both calls go through the
/// same `PulsarClient::partitions_for_topic` surface — only the child
/// case takes the fast-path and skips the broker round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partition_fast_path_resolves_child_topic_to_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("zero");
    create_partitioned_topic(&admin_url, &topic).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Sanity: the parent topic is reported as PARTITIONS-partitioned by
    // the broker (this exercises the *slow* path — a real
    // `CommandPartitionedTopicMetadata` round-trip).
    let parent_count =
        tokio::time::timeout(Duration::from_secs(5), client.partitions_for_topic(&topic)).await??;
    assert_eq!(
        parent_count, PARTITIONS,
        "broker must report {PARTITIONS} partitions for {topic}"
    );

    // F11 fast-path: a per-partition child name resolves to 0
    // immediately. This call MUST NOT issue a wire frame — verified at
    // the proto + runtime layers in the sibling unit / integration
    // tests; here we only assert the observable e2e outcome.
    let child_topic = format!("{topic}-partition-0");
    let child_count = tokio::time::timeout(
        Duration::from_millis(500),
        client.partitions_for_topic(&child_topic),
    )
    .await
    .map_err(|_| {
        format!(
            "fast-path on {child_topic:?} did not resolve within 500ms — \
             a broker round-trip would have been required"
        )
    })??;
    assert_eq!(
        child_count, 0,
        "fast-path must report 0 partitions for {child_topic}"
    );

    // Bonus claim: the fast-path resolves for every child index of the
    // parent topic. For a 4-partition topic this turns 4 round-trips
    // into 0.
    for i in 0..PARTITIONS {
        let child = format!("{topic}-partition-{i}");
        let count = tokio::time::timeout(
            Duration::from_millis(200),
            client.partitions_for_topic(&child),
        )
        .await
        .map_err(|_| format!("fast-path did not resolve for {child:?}"))??;
        assert_eq!(count, 0, "fast-path must report 0 partitions for {child}");
    }

    client.close().await;
    Ok(())
}
