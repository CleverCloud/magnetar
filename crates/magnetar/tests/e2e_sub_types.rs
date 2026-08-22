// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the non-Exclusive subscription types — `Shared`,
//! `Failover`, and `Key_Shared` — against a real Apache Pulsar 4.x standalone
//! broker spun up via `testcontainers-rs`.
//!
//! Mirrors the Java client's `SharedSubscriptionTest`, `FailoverSubscriptionTest`,
//! and `KeySharedSubscriptionTest`, but pared down to broker-observable
//! semantics (no peeking at internal dispatcher state).
//!
//! `e2e_produce_consume_roundtrip` already covers `Exclusive`; this file adds
//! the three remaining variants for Java parity.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_sub_types -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::collections::HashSet;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_admin::{AdminClient, TopicStats};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

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

/// Start a Pulsar 4.x standalone container and return (`service_url`, `admin_url`,
/// `container_handle`).
///
/// The container is held by the returned guard; dropping it stops the broker.
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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
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

/// Receiver-queue size the issue #427 Failover guard configures on both of its consumers,
/// so the grant the broker must report back is an exact number rather than the default.
const FAILOVER_RECEIVER_QUEUE_SIZE: usize = 16;

/// How long that guard waits for both consumers' initial grants to appear in the broker's
/// topic stats.
const ADMIN_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const ADMIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Settle window between "the broker reports a non-zero grant" and the reading the
/// assertion uses. A redundant second `CommandFlow` is encoded into the same connection
/// buffer under the same lock and rides the same driver flush, so it is already on the wire
/// — this only removes any dependence on where the broker happened to be in processing that
/// write.
const ADMIN_SETTLE: Duration = Duration::from_secs(1);

/// The broker's own `availablePermits` for every consumer registered on `subscription`, in
/// the order the broker lists them. Empty while the subscription is not yet in the stats
/// response.
fn broker_available_permits(stats: &TopicStats, subscription: &str) -> Vec<i64> {
    stats
        .subscriptions
        .get(subscription)
        .and_then(|sub| sub.get("consumers"))
        .and_then(serde_json::Value::as_array)
        .map(|consumers| {
            consumers
                .iter()
                .filter_map(|consumer| {
                    consumer
                        .get("availablePermits")
                        .and_then(serde_json::Value::as_i64)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Poll the broker until it reports a grant for all `expected` consumers, let the reading
/// settle, then return the settled per-consumer permits. Nothing has been published at the
/// call site, so the broker has spent none of what it was granted and its counter is the
/// grant itself.
async fn settled_broker_permits(
    admin: &AdminClient,
    topic: &str,
    subscription: &str,
    expected: usize,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + ADMIN_POLL_TIMEOUT;
    loop {
        let observation = match admin.topic_stats(topic).await {
            Ok(stats) => {
                let permits = broker_available_permits(&stats, subscription);
                if permits.len() == expected && permits.iter().all(|p| *p > 0) {
                    tokio::time::sleep(ADMIN_SETTLE).await;
                    return Ok(broker_available_permits(
                        &admin.topic_stats(topic).await?,
                        subscription,
                    ));
                }
                format!("{permits:?}")
            }
            // The topic is created by the subscribe itself, so a stats call that races
            // that creation 404s. Retry until the deadline.
            Err(error) => format!("admin error: {error}"),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "broker did not report an initial grant for all {expected} consumer(s) of \
                 `{subscription}` within {ADMIN_POLL_TIMEOUT:?}; last observation: {observation}"
            )
            .into());
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    }
}

/// Drain a [`magnetar::runtime_tokio::Consumer`] until it stays idle for
/// `idle_timeout`, returning the payloads received in arrival order. Each
/// message is acked individually so the broker advances the cursor.
async fn drain_payloads(
    consumer: magnetar::runtime_tokio::Consumer,
    idle_timeout: Duration,
) -> (magnetar::runtime_tokio::Consumer, Vec<Vec<u8>>) {
    let mut payloads = Vec::new();
    while let Ok(Ok(msg)) = tokio::time::timeout(idle_timeout, consumer.receive()).await {
        payloads.push(msg.payload.to_vec());
        let _ = consumer.ack(msg.message_id).await;
    }
    (consumer, payloads)
}

/// Drain a consumer, collecting the partition-keys it observes. Companion to
/// [`drain_payloads`] for the key-shared assertion.
async fn drain_keys(
    consumer: magnetar::runtime_tokio::Consumer,
    idle_timeout: Duration,
) -> (magnetar::runtime_tokio::Consumer, HashSet<String>) {
    let mut keys = HashSet::new();
    while let Ok(Ok(msg)) = tokio::time::timeout(idle_timeout, consumer.receive()).await {
        if let Some(key) = msg.metadata.partition_key.as_deref() {
            keys.insert(key.to_owned());
        }
        let _ = consumer.ack(msg.message_id).await;
    }
    (consumer, keys)
}

/// Two consumers on a `Shared` subscription should split the message stream
/// between them. We don't pin the exact split (broker may skew under load),
/// only that the union covers every payload exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_shared_subscription_distributes_across_consumers()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-shared-{suffix}");
    let subscription = format!("magnetar-e2e-shared-{suffix}");

    // Subscribe both consumers first so the broker dispatches across them as
    // the producer publishes (otherwise the first consumer drains everything
    // before the second one shows up).
    let consumer_a = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .name("consumer-a")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .name("consumer-b")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let total: usize = 6;
    let mut sent: Vec<Vec<u8>> = Vec::with_capacity(total);
    for i in 0..total {
        let payload = format!("shared-{i}").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
        sent.push(payload);
    }
    producer.close().await?;

    let (a_done, b_done) = tokio::join!(
        drain_payloads(consumer_a, Duration::from_secs(5)),
        drain_payloads(consumer_b, Duration::from_secs(5)),
    );
    a_done.0.close().await?;
    b_done.0.close().await?;
    client.close().await;

    let received_a = a_done.1;
    let received_b = b_done.1;

    // Total count must match — no drops, no duplicates.
    assert_eq!(
        received_a.len() + received_b.len(),
        total,
        "shared dispatch should deliver each message exactly once: a={received_a:?} b={received_b:?}"
    );

    let mut union: Vec<Vec<u8>> = received_a.into_iter().chain(received_b).collect();
    union.sort();
    let mut expected = sent;
    expected.sort();
    assert_eq!(
        union, expected,
        "shared dispatch should cover every published payload exactly once"
    );

    Ok(())
}

/// `Failover` should pin dispatch to a single active consumer. When that
/// consumer goes away, the stand-by takes over and drains the remaining
/// backlog plus any new publishes.
///
/// **Issue #307 guard**: after the stand-by is promoted, it must hold positive
/// broker-side permits (`available_permits() > 0`) — the
/// `CommandActiveConsumerChange` re-arms flow on promotion. Before the fix the
/// promoted consumer sat at zero permits and `receive()` starved forever. The
/// test asserts the positive permit count immediately after promotion (before
/// any phase-2 publish could lazily replenish) AND that it actually drains the
/// post-failover backlog.
///
/// **Issue #427 guard**: and on the way in, each consumer must hold EXACTLY the
/// receiver-queue size it configured — read from the broker's own `availablePermits`, the
/// only oracle that can tell one grant from two. `Failover` is where the double grant lived:
/// a real broker sends `CommandActiveConsumerChange` right behind the subscribe `Success`,
/// so the #307 re-arm above and the engine's own post-ack `initial_flow` both fired for one
/// attach and the broker held `2 ×` what the client accounted for.
///
/// **Election determinism**: Pulsar's `pickAndScheduleActiveConsumer` picks
/// by `(priorityLevel ASC, consumerName ASC)`. We register two consumers
/// with the same priority but distinct names — the broker is therefore
/// free to elect either depending on internal scheduling, and this is a
/// known race in 4.0 standalone. Rather than assume which one is active,
/// we detect it dynamically: drain both consumers concurrently after the
/// first batch and treat whichever one received as "active" for this run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_failover_subscription_active_only() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;

    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-failover-{suffix}");
    let subscription = format!("magnetar-e2e-failover-{suffix}");

    let consumer_a = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("consumer-a")
        .receiver_queue_size(FAILOVER_RECEIVER_QUEUE_SIZE)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("consumer-b")
        .receiver_queue_size(FAILOVER_RECEIVER_QUEUE_SIZE)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Broker takes a beat to elect the active consumer once both have
    // registered. Pulsar's `pickAndScheduleActiveConsumer` flips the active
    // flag on after `activeConsumerFailoverDelayTimeMillis` (default 1 s).
    // Sleep 3 s for slow Docker hosts.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Issue #427: before anything is published — so the broker has spent none of what it
    // was granted — its own `availablePermits` must equal the configured receiver-queue
    // size for BOTH consumers. The elected one is the interesting half: it received
    // `CommandActiveConsumerChange { is_active: true }` right behind its subscribe
    // `Success`, which is exactly the frame that used to make the sans-io #307 re-arm and
    // the engine's post-ack `initial_flow` both grant for one attach (measured
    // `2 × receiver_queue_size` on the broker against `1 ×` on the client).
    let granted = settled_broker_permits(&admin, &topic, &subscription, 2).await?;
    assert_eq!(
        granted,
        vec![i64::try_from(FAILOVER_RECEIVER_QUEUE_SIZE)?; 2],
        "each Failover consumer must hold exactly one receiver-queue grant after subscribe \
         (issue #427): configured {FAILOVER_RECEIVER_QUEUE_SIZE}, broker reports {granted:?}",
    );

    let producer = client.producer(&topic).create().await?;
    let first_batch: usize = 5;
    for i in 0..first_batch {
        producer
            .send(OutgoingMessage::with_payload(format!("phase-1-{i}").into_bytes()).into())
            .await?;
    }

    // Drain whichever consumer is active. We try receiving from both
    // concurrently with a generous per-message timeout — the `select`
    // arm that resolves first identifies the active side. The other is
    // the stand-by.
    let active_is_a = tokio::select! {
        first = tokio::time::timeout(Duration::from_secs(15), consumer_a.receive()) => {
            let msg = first.map_err(|_| "phase-1: both failover consumers timed out (no election)".to_owned())??;
            consumer_a.ack(msg.message_id).await?;
            true
        }
        first = tokio::time::timeout(Duration::from_secs(15), consumer_b.receive()) => {
            let msg = first.map_err(|_| "phase-1: both failover consumers timed out (no election)".to_owned())??;
            consumer_b.ack(msg.message_id).await?;
            false
        }
    };
    let active_name = if active_is_a {
        "consumer-a"
    } else {
        "consumer-b"
    };
    let standby_name = if active_is_a {
        "consumer-b"
    } else {
        "consumer-a"
    };
    eprintln!("phase-1: broker elected {active_name} as active");

    // Drain the remaining 4 messages from the active side.
    for i in 1..first_batch {
        let msg = if active_is_a {
            tokio::time::timeout(Duration::from_secs(15), consumer_a.receive())
                .await
                .map_err(|_| {
                    format!("phase-1: {active_name} timed out at message {i} / {first_batch}")
                })??
        } else {
            tokio::time::timeout(Duration::from_secs(15), consumer_b.receive())
                .await
                .map_err(|_| {
                    format!("phase-1: {active_name} timed out at message {i} / {first_batch}")
                })??
        };
        if active_is_a {
            consumer_a.ack(msg.message_id).await?;
        } else {
            consumer_b.ack(msg.message_id).await?;
        }
    }
    // Stand-by must be silent.
    let standby_idle = if active_is_a {
        tokio::time::timeout(Duration::from_millis(500), consumer_b.receive()).await
    } else {
        tokio::time::timeout(Duration::from_millis(500), consumer_a.receive()).await
    };
    assert!(
        standby_idle.is_err(),
        "failover stand-by ({standby_name}) should not receive any messages while {active_name} is active"
    );

    // Split into active vs stand-by. After this `Some`/`None` shape the
    // close-self moves are unambiguous to the borrow checker.
    let (mut active_opt, mut standby_opt) = if active_is_a {
        (Some(consumer_a), Some(consumer_b))
    } else {
        (Some(consumer_b), Some(consumer_a))
    };

    // Close the active consumer → broker promotes the stand-by. Failover
    // re-election delay (default 1 s) + close notification settle: sleep 5 s.
    eprintln!("phase-2: closing {active_name}, expecting {standby_name} to take over");
    active_opt
        .take()
        .expect("active was just set")
        .close()
        .await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Regression guard (issue #307): on promotion to active, the broker sends
    // `CommandActiveConsumerChange { is_active: true }` and the proto layer must
    // re-arm flow. Before #307, the promoted stand-by sat at
    // `available_permits == 0` — the broker had no permit to push the backlog
    // and `receive()` starved forever. Assert the promoted consumer now holds a
    // positive permit count, BEFORE any phase-2 publish drives a lazy
    // replenishment, so this pins the on-promotion re-flow specifically.
    let promoted_permits = standby_opt
        .as_ref()
        .expect("standby was just set")
        .available_permits();
    assert!(
        promoted_permits > 0,
        "promoted stand-by ({standby_name}) must hold positive broker permits after promotion \
         (issue #307: ActiveConsumerChange must re-arm flow), got {promoted_permits}"
    );

    let second_batch: usize = 3;
    for i in 0..second_batch {
        producer
            .send(OutgoingMessage::with_payload(format!("phase-2-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    let promoted = standby_opt.as_ref().expect("standby was just set");
    let mut received_promoted: Vec<Vec<u8>> = Vec::new();
    for i in 0..second_batch {
        let msg = tokio::time::timeout(Duration::from_secs(30), promoted.receive())
            .await
            .map_err(|_| {
                format!(
                    "phase-2: {standby_name} timed out at message {i} / {second_batch}; \
                     received so far = {received_promoted:?}"
                )
            })??;
        received_promoted.push(msg.payload.to_vec());
        promoted.ack(msg.message_id).await?;
    }
    standby_opt
        .take()
        .expect("standby was just set")
        .close()
        .await?;
    client.close().await;

    assert_eq!(
        received_promoted.len(),
        second_batch,
        "promoted stand-by ({standby_name}) must drain post-failover publishes"
    );
    Ok(())
}

/// `Key_Shared` with the default (auto-split) policy should partition the
/// key-space across consumers so each key sticks to exactly one consumer.
/// The Java baseline is `KeySharedSubscriptionTest` — we keep the assertion
/// to broker-observable semantics: disjoint key sets, full key coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_key_shared_sticks_per_key() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-keyshared-{suffix}");
    let subscription = format!("magnetar-e2e-keyshared-{suffix}");

    let consumer_a = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::KeyShared)
        .name("consumer-a")
        .key_shared_policy(magnetar::proto::KeySharedConfig::default())
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::KeyShared)
        .name("consumer-b")
        .key_shared_policy(magnetar::proto::KeySharedConfig::default())
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let keys: &[&str] = &["A", "B", "C", "D"];
    let per_key: usize = 5;
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

    let (a_done, b_done) = tokio::join!(
        drain_keys(consumer_a, Duration::from_secs(5)),
        drain_keys(consumer_b, Duration::from_secs(5)),
    );
    a_done.0.close().await?;
    b_done.0.close().await?;
    client.close().await;

    let a_keys = a_done.1;
    let b_keys = b_done.1;

    // Disjoint per Key_Shared sticky guarantee.
    let intersection: HashSet<_> = a_keys.intersection(&b_keys).collect();
    assert!(
        intersection.is_empty(),
        "Key_Shared dispatch must partition keys across consumers: a={a_keys:?} b={b_keys:?}"
    );

    // Union must cover every key the producer used.
    let mut union: Vec<String> = a_keys.union(&b_keys).cloned().collect();
    union.sort();
    let mut expected: Vec<String> = keys.iter().map(|k| (*k).to_owned()).collect();
    expected.sort();
    assert_eq!(
        union, expected,
        "every published key must reach exactly one consumer"
    );

    Ok(())
}
