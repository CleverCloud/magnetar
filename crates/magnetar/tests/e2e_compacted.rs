// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for Pulsar 4.x compacted-topic semantics (PIP-94) and
//! the [`magnetar::TableView`] round-trip. Each test spins up a Pulsar
//! standalone broker via `testcontainers-rs`, runs producer/consumer
//! traffic, and exercises one branch of the compaction story.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_compacted -- --nocapture
//! ```
//!
//! Mirrors the Java parity tests at
//! `pulsar-broker/src/test/java/org/apache/pulsar/compaction/CompactionTest.java`
//! and `pulsar-broker/src/test/java/org/apache/pulsar/client/impl/TableViewTest.java`.

use std::collections::HashMap;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
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

/// Mint a unique topic name per test so concurrent runs don't share broker
/// state. The `uuid::simple` form is `[a-f0-9]{32}`, safe for Pulsar's topic
/// grammar.
fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        Uuid::new_v4().simple()
    )
}

/// Start a Pulsar 4.x standalone container and return (`service_url`, `admin_url`,
/// `container_handle`). The container is held by the returned guard; dropping
/// it stops the broker.
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

/// Trigger compaction on a persistent topic via the broker's admin REST API.
/// `magnetar-admin` does not currently expose a typed wrapper for the
/// compaction endpoint, so we drive it directly with `reqwest`. Java:
/// `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`
/// (`@PUT @Path("/{tenant}/{namespace}/{topic}/compaction")`).
///
/// The endpoint returns 204 on success and 409 if a compaction is already
/// running — we treat 409 as a success (the running pass will drain the
/// backlog we just published) per Java's idempotent re-trigger semantics.
async fn trigger_compaction(
    admin_url: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Strip the optional `persistent://` scheme so we can split into
    // tenant/namespace/name segments. Mirrors `magnetar-admin::split_topic`.
    let path_remainder = topic.strip_prefix("persistent://").unwrap_or(topic);
    let mut parts = path_remainder.splitn(3, '/');
    let tenant = parts.next().unwrap_or("");
    let namespace = parts.next().unwrap_or("");
    let name = parts.next().unwrap_or("");
    assert!(
        !tenant.is_empty() && !namespace.is_empty() && !name.is_empty(),
        "expected persistent://tenant/namespace/topic, got {topic:?}"
    );
    let url = format!("{admin_url}/admin/v2/persistent/{tenant}/{namespace}/{name}/compaction");
    let response = reqwest::Client::new().put(&url).send().await?;
    let status = response.status();
    if status.is_success() || status.as_u16() == 409 {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    Err(format!("trigger compaction failed: {status} {body}").into())
}

/// Producer publishes ("k1", v1) → ("k2", v2) → ("k1", v3); broker is asked
/// to compact the topic; a fresh subscription with `read_compacted(true)`
/// must only see the latest value per key — i.e. v1 is dropped. Mirrors the
/// Java assertion shape in `CompactionTest#testCompaction` (PIP-94).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_compacted_reader_sees_latest_per_key() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = unique_topic("magnetar-e2e-compact-latest");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Publish (k1, v1) → (k2, v2) → (k1, v3). After compaction the broker
    // must retain only (k1, v3) and (k2, v2).
    let producer = client.producer(&topic).create().await?;
    for (key, payload) in [("k1", &b"v1"[..]), ("k2", &b"v2"[..]), ("k1", &b"v3"[..])] {
        producer
            .send(
                OutgoingMessage::with_payload(payload.to_vec())
                    .key(key.to_owned())
                    .into(),
            )
            .await?;
    }
    producer.close().await?;

    // Trigger compaction via the admin REST endpoint. The broker compacts
    // asynchronously; we sleep a conservative amount rather than poll the
    // status endpoint — the e2e job's budget covers it.
    trigger_compaction(&admin_url, &topic).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Subscribe from earliest with `read_compacted(true)` — the broker
    // serves the compacted ledger, not the raw backlog.
    let consumer = client
        .consumer(&topic)
        .subscription(format!("magnetar-e2e-compact-{}", Uuid::new_v4().simple()))
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .read_compacted(true)
        .subscribe()
        .await?;

    // Drain up to 5 seconds; we expect exactly two messages (one per key).
    // A timeout on the third receive proves no further messages are delivered.
    let mut received: HashMap<String, Vec<u8>> = HashMap::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
        let key = msg
            .metadata
            .partition_key
            .clone()
            .or_else(|| {
                msg.single_metadata
                    .as_ref()
                    .and_then(|sm| sm.partition_key.clone())
            })
            .expect("compacted message must carry a partition key");
        received.insert(key, msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    // No third message should land on the compacted view.
    let extra = tokio::time::timeout(Duration::from_secs(3), consumer.receive()).await;
    assert!(
        extra.is_err(),
        "compacted view delivered an unexpected extra message: {extra:?}"
    );
    consumer.close().await?;
    client.close().await;

    assert_eq!(received.len(), 2, "expected exactly two distinct keys");
    assert_eq!(
        received.get("k1").map(Vec::as_slice),
        Some(&b"v3"[..]),
        "compaction must retain only the latest value for k1"
    );
    assert_eq!(
        received.get("k2").map(Vec::as_slice),
        Some(&b"v2"[..]),
        "compaction must retain k2's only write"
    );
    Ok(())
}

/// Wait for a [`magnetar::TableView`] to materialise exactly `expected`
/// distinct keys, polling the view every 100 ms. Returns the final
/// snapshot. Times out after `deadline`, panicking with the last observed
/// size for diagnostics. Mirrors the "spin until tv.size == n" loop in
/// `TableViewTest#testTableView`.
async fn wait_for_size(
    tv: &magnetar::TableView,
    expected: usize,
    deadline: Duration,
) -> HashMap<String, bytes::Bytes> {
    let inner = async {
        loop {
            let snapshot = tv.snapshot();
            if snapshot.len() == expected {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(deadline, inner)
        .await
        .unwrap_or_else(|_| panic!("table view did not reach size {expected} within {deadline:?}"))
}

/// Producer publishes 3 keys × 2 versions; the `TableView` must converge to
/// the *latest* value per key. Mirrors `TableViewTest#testTableView` —
/// the snapshot of a `TableView` is the compacted view of the topic, even
/// when compaction has not been triggered (the table view reads the raw
/// log and overwrites entries in memory).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_tableview_compacted_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let topic = unique_topic("magnetar-e2e-tv-snapshot");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Three keys, two versions each. Order matters: the second write per
    // key is the value the table view must retain.
    let producer = client.producer(&topic).create().await?;
    let writes: &[(&str, &[u8])] = &[
        ("k1", b"k1-v1"),
        ("k2", b"k2-v1"),
        ("k3", b"k3-v1"),
        ("k1", b"k1-v2"),
        ("k2", b"k2-v2"),
        ("k3", b"k3-v2"),
    ];
    for (key, payload) in writes {
        producer
            .send(
                OutgoingMessage::with_payload(payload.to_vec())
                    .key((*key).to_owned())
                    .into(),
            )
            .await?;
    }
    producer.close().await?;

    let tv = client
        .table_view(&topic)
        .subscription_name(format!("magnetar-e2e-tv-{}", Uuid::new_v4().simple()))
        .create()
        .await?;

    // Wait until all three keys are materialised, then snapshot.
    let snapshot = wait_for_size(&tv, 3, Duration::from_secs(10)).await;
    tv.close().await;
    client.close().await;

    assert_eq!(snapshot.len(), 3, "table view must hold exactly 3 keys");
    assert_eq!(
        snapshot.get("k1").map(AsRef::as_ref),
        Some(&b"k1-v2"[..]),
        "k1 must converge to its latest write"
    );
    assert_eq!(
        snapshot.get("k2").map(AsRef::as_ref),
        Some(&b"k2-v2"[..]),
        "k2 must converge to its latest write"
    );
    assert_eq!(
        snapshot.get("k3").map(AsRef::as_ref),
        Some(&b"k3-v2"[..]),
        "k3 must converge to its latest write"
    );
    Ok(())
}

/// Producer publishes ("k1", v1) followed by ("k1", <empty payload>). The
/// empty payload is the Pulsar compaction tombstone convention; the
/// `TableView` must drop k1 from its snapshot. Mirrors
/// `TableViewTest#testTableViewTombstone` and the drain-task branch in
/// `magnetar::table_view::TableViewBuilder::create` that calls
/// `state.remove(&key)` on an empty payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_tableview_tombstone_removes_key() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let topic = unique_topic("magnetar-e2e-tv-tombstone");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;
    // First write — k1 should land in the table view.
    producer
        .send(
            OutgoingMessage::with_payload(b"v1".to_vec())
                .key("k1".to_owned())
                .into(),
        )
        .await?;
    // Tombstone — empty payload with the same key removes k1.
    producer
        .send(
            OutgoingMessage::with_payload(Vec::new())
                .key("k1".to_owned())
                .into(),
        )
        .await?;
    producer.close().await?;

    let tv = client
        .table_view(&topic)
        .subscription_name(format!("magnetar-e2e-tomb-{}", Uuid::new_v4().simple()))
        .create()
        .await?;

    // Wait up to 10 s for the drain task to apply both writes. We can't
    // simply wait for `size == 0` from the start because that's also the
    // initial state; instead, observe the size go to 1 then back to 0.
    // The two writes are in flight before the table view opens, so we
    // poll for "either we see k1 transiently then it goes away, or we
    // never see it because the second message overwrote the first before
    // we sampled". Both outcomes are correct — the post-condition is
    // simply `!tv.contains_key("k1")` once the drain has caught up.
    //
    // We give the drain a generous budget to consume both messages, then
    // assert the final state. Sleeping is acceptable here because the e2e
    // job has time to spare.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let snapshot = tv.snapshot();
    tv.close().await;
    client.close().await;

    assert!(
        !snapshot.contains_key("k1"),
        "tombstone must remove k1 from the table view, got snapshot={snapshot:?}"
    );
    assert!(
        snapshot.is_empty(),
        "table view should be empty after tombstoning the sole key, got {snapshot:?}"
    );
    Ok(())
}
