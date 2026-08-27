// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for issue #436 against a real Apache Pulsar 4.x standalone broker: a
//! `Shared` subscription over BATCHED entries, with `ack_timeout` armed, whose flow control
//! wedges once a partially-acked entry is re-dispatched.
//!
//! ## The production failure
//!
//! Twelve `Shared` consumers over a twelve-partition topic whose entries pack 1024 messages
//! each. Within the hour every consumer sits at `availablePermits` `0` or negative — one
//! reached `-3535` — `msgRateOut` is `0`, and the application's own probe reports it
//! acknowledging essentially everything it receives. Raising `ack_timeout` past the run
//! length, with nothing else changed, makes the wedge disappear. One batched entry had
//! additionally pinned the subscription's mark-delete position for days, with the first
//! individually-deleted range starting exactly one entry past it.
//!
//! ## What this pins that the lower layers cannot
//!
//! The proto, both runtimes, and the differential harness pin the trajectory against
//! synthetic frames and a scripted broker (`batch_redelivery_flow_wedge.rs`,
//! `batch_redelivery_flow_equivalence.rs`). What none of them can prove is that a REAL broker
//! actually attaches `CommandMessage.ack_set` in this situation and that magnetar reads the
//! bitset it really sends — the field only appears when the broker is configured with
//! `acknowledgmentAtBatchIndexLevelEnabled=true`, which is the production configuration behind
//! issue #436 and is therefore pinned explicitly in the container environment below rather
//! than inherited from whatever the image happens to default to.
//!
//! The split is driven by a consumer restart: one consumer drains part of a batched entry,
//! acknowledges what it took, and leaves. The broker re-dispatches that entry to the
//! replacement as ONE entry carrying the positions that are still outstanding. Before
//! [ADR-0105](../../../specs/adr/0105-read-the-delivered-batch-index-ack-set.md) the
//! replacement exploded the whole entry, so it was handed messages the departed consumer had
//! already acknowledged, re-registered them in its ack-timeout tracker, and debited a permit
//! for each against a broker that charged only the still-outstanding ones.
//!
//! Three assertions carry the claim, all read from the broker rather than from the client:
//!
//! 1. **No acknowledged message is re-delivered.** Set-based on purpose — an ack-timeout sweep
//!    firing mid-run may legitimately re-deliver an OUTSTANDING message, and that must not make the
//!    test flaky; re-delivering an ACKNOWLEDGED one is the defect.
//! 2. **The subscription drains.** Every message the departed consumer left behind reaches the
//!    replacement inside the deadline. A subscription spending its delivery capacity re-consuming
//!    acknowledged history is `msgRateOut = 0` seen from the application.
//! 3. **The split entry completes.** Once the residue is acked the broker's own stats report an
//!    empty backlog and no non-contiguous deleted range — i.e. the mark-delete position advanced
//!    PAST the split entry instead of being pinned behind it — while the live consumer's
//!    `availablePermits` is still positive, so the client never stopped replenishing.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_batch_ack_timeout_shared -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::collections::BTreeSet;
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

/// JVM budget for the `pulsar standalone` container. See
/// `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

/// Sub-messages packed into each batched broker entry. Wide enough that "one entry" and "one
/// permit" are visibly different things — which is the whole premise of issue #436 — and small
/// enough to name every position in a failure message.
const BATCH_SIZE: usize = 8;

/// Batched entries published. Two is the minimum that distinguishes "the split entry
/// completed" from "the subscription happened to have nothing behind it".
const ENTRIES: usize = 2;

/// Messages the first consumer takes and acknowledges before it leaves. Inside the first
/// entry, and short of it, so that entry is genuinely SPLIT: some positions acknowledged, some
/// still outstanding.
const ACKED_PREFIX: usize = 6;

/// Receiver queue for both consumers. `maybe_flow` re-arms at `max(RQ / 2, 1)`, so a drain of
/// this width forces several flow round-trips and the permit balance actually moves.
const RECEIVER_QUEUE_SIZE: usize = 4;

/// Ack-timeout window, armed on both consumers — the configuration issue #436 reports, and the
/// one whose removal made the wedge disappear. Reachable on purpose: a sweep firing mid-run
/// re-delivers OUTSTANDING positions, which every assertion below tolerates, and would
/// re-deliver ACKNOWLEDGED ones only if the defect were back.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Budget for the replacement consumer to collect everything the departed one left behind.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-`receive()` patience while draining. Short relative to [`DRAIN_TIMEOUT`] so a stalled
/// subscription is retried rather than blocking the whole budget on one call.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the broker is given to report the drained, fully-acked end state.
const ADMIN_POLL_TIMEOUT: Duration = Duration::from_secs(30);
const ADMIN_POLL_INTERVAL: Duration = Duration::from_millis(200);

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

/// Start a Pulsar 4.x standalone container with **batch-index acknowledgement enabled** and
/// return (`service_url`, `admin_url`, `container_handle`).
///
/// `acknowledgmentAtBatchIndexLevelEnabled` is the switch that makes the broker track
/// per-position acknowledgement inside a batched entry at all, and therefore the switch that
/// makes it attach `CommandMessage.ack_set` when it re-dispatches a partially-acked one. It is
/// pinned here rather than assumed: the field this test exists to exercise is simply absent
/// when the broker runs with the default, and the test would then pass for the wrong reason.
/// The container's own `apply-config-from-env-with-prefix.py` writes it into
/// `conf/standalone.conf` before the broker boots (the `e2e_reconnect_safety.rs` pattern).
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
        .with_startup_timeout(Duration::from_mins(3))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
        .with_env_var(
            "PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled",
            "true",
        )
        .with_cmd(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ conf/standalone.conf && \
             bin/pulsar standalone"
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

/// The subscription object out of the broker's raw topic-stats JSON.
fn subscription_stats<'a>(
    stats: &'a TopicStats,
    subscription: &str,
) -> Option<&'a serde_json::Value> {
    stats.subscriptions.get(subscription)
}

/// The broker's own `availablePermits` for every consumer registered on `subscription`, in the
/// order the broker lists them. Empty while the subscription is not yet in the response.
fn broker_available_permits(stats: &TopicStats, subscription: &str) -> Vec<i64> {
    subscription_stats(stats, subscription)
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

/// An `i64` field of the subscription object, or `None` while it is absent.
fn subscription_i64(stats: &TopicStats, subscription: &str, field: &str) -> Option<i64> {
    subscription_stats(stats, subscription)
        .and_then(|sub| sub.get(field))
        .and_then(serde_json::Value::as_i64)
}

/// Publish [`ENTRIES`] batched entries of [`BATCH_SIZE`] messages each and return the payloads
/// in publish order.
///
/// Every send future is enqueued before any is awaited: awaiting sequentially would never fill
/// a batch, because each send would wait on a receipt that only arrives after a flush. The
/// generous publish delay leaves the message-count cap as the only thing that can flush, so
/// one flush is exactly one broker entry of [`BATCH_SIZE`] packed messages (the
/// `e2e_producer_batching_flushes_on_max_msgs` pattern).
async fn publish_batched_entries(
    client: &PulsarClient,
    topic: &str,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let producer = client
        .producer(topic)
        .batching(BATCH_SIZE, 1_000_000)
        .batching_max_publish_delay(Duration::from_mins(1))
        .create()
        .await?;
    let mut published = Vec::with_capacity(ENTRIES * BATCH_SIZE);
    for entry in 0..ENTRIES {
        let payloads: Vec<Vec<u8>> = (0..BATCH_SIZE)
            .map(|position| format!("batch-436-{entry}-{position}").into_bytes())
            .collect();
        let sends: Vec<_> = payloads
            .iter()
            .map(|payload| producer.send(OutgoingMessage::with_payload(payload.clone()).into()))
            .collect();
        for send in sends {
            send.await?;
        }
        published.extend(payloads);
    }
    producer.close().await?;
    Ok(published)
}

/// A `Shared` consumer on `subscription` with the ack-timeout armed.
async fn open_consumer(
    client: &PulsarClient,
    topic: &str,
    subscription: &str,
    name: &str,
) -> Result<magnetar::runtime_tokio::Consumer, Box<dyn std::error::Error>> {
    Ok(client
        .consumer(topic)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .name(name.to_owned())
        .receiver_queue_size(RECEIVER_QUEUE_SIZE)
        .ack_timeout(ACK_TIMEOUT)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?)
}

/// A `Shared` consumer restart splits a batched broker entry. The replacement must be handed
/// only the positions that entry still owes, the subscription must drain, and the split entry
/// must complete so the broker's mark-delete position moves past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// One linear timeline keeps the split-entry frames unambiguous (the e2e_reconnect_safety idiom).
#[allow(clippy::too_many_lines)]
async fn e2e_batched_entry_split_by_a_consumer_restart_completes_and_keeps_flowing()
-> Result<(), Box<dyn std::error::Error>> {
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
    let topic = format!("persistent://public/default/magnetar-e2e-batch-436-{suffix}");
    let subscription = format!("magnetar-e2e-batch-436-{suffix}");

    let published = publish_batched_entries(&client, &topic).await?;
    assert_eq!(published.len(), ENTRIES * BATCH_SIZE);

    // The consumer that splits the entry: it takes a prefix of the first batched entry,
    // acknowledges exactly what it took, and leaves. Whatever the broker had already
    // dispatched to it beyond that prefix is un-acked and becomes the subscription's problem.
    let splitter = open_consumer(&client, &topic, &subscription, "batch-436-splitter").await?;
    let mut acked: BTreeSet<Vec<u8>> = BTreeSet::new();
    for _ in 0..ACKED_PREFIX {
        let message = tokio::time::timeout(RECV_TIMEOUT, splitter.receive()).await??;
        let payload = message.payload.to_vec();
        splitter.ack(message.message_id).await?;
        acked.insert(payload);
    }
    assert_eq!(
        acked.len(),
        ACKED_PREFIX,
        "the splitter must acknowledge {ACKED_PREFIX} distinct messages before leaving; \
         got {acked:?}",
    );
    // Individual acks are not broker-confirmed on this path, so let the writes land before the
    // close that turns the rest of the entry into a redelivery.
    tokio::time::sleep(Duration::from_secs(1)).await;
    splitter.close().await?;

    // The replacement. Its PIP-54 tracker is empty, so the re-dispatched entry's `ack_set` is
    // the only thing that can tell it which positions are still outstanding — this is the
    // SEED path of ADR-0105, and the one a consumer restart always takes.
    let survivor = open_consumer(&client, &topic, &subscription, "batch-436-survivor").await?;

    let expected_residue: BTreeSet<Vec<u8>> = published
        .iter()
        .filter(|payload| !acked.contains(*payload))
        .cloned()
        .collect();
    let mut collected: BTreeSet<Vec<u8>> = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    while collected.len() < expected_residue.len() {
        let Ok(Ok(message)) = tokio::time::timeout(RECV_TIMEOUT, survivor.receive()).await else {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            continue;
        };
        let payload = message.payload.to_vec();
        // (1) The redelivery must carry only what the split entry still owes. A consumer that
        // ignores `CommandMessage.ack_set` re-explodes the whole entry and hands back the
        // positions the departed consumer acknowledged — messages nothing will ever ack again,
        // which is what makes the ack-timeout redelivery loop self-sustaining (issue #436).
        assert!(
            !acked.contains(&payload),
            "a message the departed consumer already acknowledged was re-delivered: {}. The \
             broker re-dispatches a partially-acked batched entry as ONE entry and names the \
             still-outstanding positions in CommandMessage.ack_set; delivering a cleared \
             position hands the application a duplicate, re-registers it in the ack-timeout \
             tracker, and debits a permit the broker never charged (ADR-0105)",
            String::from_utf8_lossy(&payload),
        );
        survivor.ack(message.message_id).await?;
        collected.insert(payload);
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }

    // (2) Consumption resumes: everything the departed consumer left behind — the split
    // entry's outstanding tail AND the whole entry published behind it — reaches the
    // replacement. A subscription still re-delivering acknowledged history has no delivery
    // capacity left for the backlog.
    assert_eq!(
        collected,
        expected_residue,
        "every message the departed consumer left un-acked must reach the replacement within \
         {DRAIN_TIMEOUT:?}; missing {:?}",
        expected_residue
            .difference(&collected)
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect::<Vec<_>>(),
    );

    // (3) The split entry completes, so the mark-delete position advances past it. Read from
    // the broker, which is the only oracle that can tell "acked" from "acked as far as the
    // client is concerned". `nonContiguousDeletedMessagesRanges` is the direct signature of
    // the issue #436 mark-delete symptom: an entry that never reaches fully-acked pins the
    // position while every entry after it is individually deleted around it.
    let mut observation = String::new();
    let admin_deadline = tokio::time::Instant::now() + ADMIN_POLL_TIMEOUT;
    let settled = loop {
        match admin.topic_stats(&topic).await {
            Ok(stats) => {
                let backlog = subscription_i64(&stats, &subscription, "msgBacklog");
                let ranges =
                    subscription_i64(&stats, &subscription, "nonContiguousDeletedMessagesRanges");
                if backlog == Some(0) && ranges == Some(0) {
                    break Some(stats);
                }
                observation = format!("msgBacklog={backlog:?} ranges={ranges:?}");
            }
            Err(error) => observation = format!("admin error: {error}"),
        }
        if tokio::time::Instant::now() >= admin_deadline {
            break None;
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    };
    let settled = settled.ok_or_else(|| {
        format!(
            "the subscription did not reach an empty backlog with a fully-advanced mark-delete \
             position within {ADMIN_POLL_TIMEOUT:?}; last observation: {observation}. A \
             partially-acked batched entry the client re-seeds as all-unacked can never reach \
             fully-acked, so its CommandAck carries an ack_set forever and the broker holds \
             the cursor behind it (issue #436)"
        )
    })?;

    // ...and the client never stopped replenishing while getting there. Under the defect the
    // permit mirror is debited for positions the broker never charged, so it reaches zero
    // while the broker still holds permits, the client stops sending CommandFlow, and the
    // broker's own counter drains to zero and stays there — `availablePermits` 0 with
    // `msgRateOut` 0, exactly what issue #436 reports.
    let permits = broker_available_permits(&settled, &subscription);
    assert_eq!(
        permits.len(),
        1,
        "exactly one consumer remains attached to the subscription, got {permits:?}",
    );
    assert!(
        permits.iter().all(|balance| *balance > 0),
        "the broker must still hold permits for the drained consumer — a client that debits \
         its mirror for positions the broker never charged stops replenishing while the \
         broker's own availablePermits sits at 0 (issue #436); got {permits:?}",
    );

    survivor.close().await?;
    client.close().await;
    Ok(())
}
