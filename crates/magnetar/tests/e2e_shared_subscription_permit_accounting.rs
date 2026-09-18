// SPDX-License-Identifier: Apache-2.0

//! Broker-side permit *accounting* under `Shared`-subscription churn — the
//! other half of issue #414, and the half
//! [`e2e_shared_subscription_churn.rs`](./e2e_shared_subscription_churn.rs)
//! deliberately does not cover.
//!
//! ## Why a second churn suite
//!
//! The existing suite asserts that **dispatch continues** across a mid-drain
//! consumer close. That assertion is green on a broker whose permit ledger is
//! leaking, because a leak only wedges the subscription once it has accumulated
//! past a threshold — `readMoreEntries` has fallen back to
//! `max(totalAvailablePermits, firstAvailableConsumerPermits)` since 2021, so a
//! negative *aggregate* is masked entirely. "Messages kept flowing" therefore
//! proves nothing about issue #414's root cause. This suite asserts on the
//! **numbers the broker reports about itself** instead.
//!
//! ## The upstream mechanism (apache/pulsar#26416)
//!
//! `Consumer.flowPermits` credits the per-consumer counter synchronously on the
//! IO thread, then hands the subscription-aggregate credit to the broker
//! executor. `PersistentDispatcherMultipleConsumers.internalConsumerFlow`
//! discards that deferred credit when the consumer has already left
//! `consumerSet`, while `removeConsumer` debits the aggregate by the consumer's
//! **full** per-consumer counter — discarded credit included. Merged fixes:
//! apache/pulsar#26289 (deferred-flow accounting) and apache/pulsar#26422
//! (unacked double-debit guard).
//!
//! The production signature was a **per-consumer** `ConsumerStatsImpl`
//! `availablePermits` of `-177300` that never recovered; only a superuser
//! `topics unload` cleared it.
//!
//! ## What this test observes, and where it has to read it from
//!
//! The dispatcher's `totalAvailablePermits` — the counter the root cause
//! corrupts — is **exposed by no Pulsar admin endpoint**. Measured against
//! 4.2.4 on 2026-09-18: absent from `…/stats` (`SubscriptionStatsImpl` carries
//! no permit field at all), from `/admin/v2/broker-stats/topics`, from
//! `…/internalStats`, and from `/metrics`. The broker does print it on its own
//! debug logger, so this suite raises exactly that one logger
//! (`DISPATCHER_DEBUG_LOGGER_PATCH`) and reads the aggregate out of the
//! container's log. That is the only oracle there is for it; a run that parses
//! zero observations fails as an infrastructure fault rather than passing.
//!
//! Four assertions, all on numbers the broker reported about itself:
//!
//! 1. the dispatcher's `totalAvailablePermits` is never negative — the root cause;
//! 2. every **per-consumer** `availablePermits` stays inside `[0, receiver_queue_size]` — the
//!    client only ever grants what it has consumed, so the broker's balance can neither go negative
//!    (the production wedge signature) nor exceed one queue's worth (an issue #426 double grant);
//! 3. no per-consumer and no subscription `unackedMessages` is negative, which the
//!    apache/pulsar#26422 double-debit can cause;
//! 4. the backlog reaches zero with a survivor still attached.
//!
//! The per-consumer readings are taken **settled** — after the churn, with the
//! subscription quiescent — because the production failure was a counter stuck
//! negative forever, not a transient dip while a batched entry is being charged.
//!
//! ## Measured verdicts (2026-09-18, this test binary)
//!
//! | image | broker | runs | red | min `totalAvailablePermits` |
//! | ----- | ------ | ---- | --- | --------------------------- |
//! | `4.0.4` | 4.0.4 | 3 | 3 | −10, −14, −14 |
//! | `4.2.4` (== `latest`) | 4.2.4 | 8 | 8 | −4 … −18 |
//! | `5.0.0-M2` | 5.0.0-M2 | 12 | 1 | 0 in eleven runs, −2 in one |
//!
//! The merged fixes ship in no released image: Docker Hub's `apachepulsar/pulsar`
//! stops at 4.0.13 and 4.2.4, and 4.0.14 / 4.2.5 are unpublished. `5.0.0-M2` is a
//! milestone build and the only image carrying both, so it is the only green cell
//! available at all.
//!
//! Two findings the numbers carry beyond "the fix works":
//!
//! * `5.0.0-M2` is not unconditionally clean. One run in twelve reached −2 with a single negative
//!   reading, against 22–83 negative readings in every pre-fix run. apache/pulsar#26289 and #26422
//!   shrink the leak by roughly two orders of magnitude here without closing it, which is
//!   consistent with apache/pulsar#26416 having stayed open and with PIP-491 being unmerged.
//! * The per-consumer counter stayed inside `[0, 4]` on every image at every settled reading,
//!   although the discard window was entered 2–10 times per run. The production wedge signature — a
//!   per-consumer `availablePermits` stuck at −177300 — did **not** reproduce; only the aggregate
//!   leak behind it did.
//!
//! ## The shape being reproduced
//!
//! Per round: every live consumer receives exactly `receiver_queue_size / 2`
//! messages (magnetar's `maybe_flow` threshold, `magnetar-proto`'s
//! `Consumer::flow_threshold`) and leaves them **un-acked**, so a `CommandFlow`
//! is on the wire and pending-acks exist; then all but one consumer close
//! concurrently, so each leaver's flow races its own `removeConsumer`. One
//! survivor always stays attached — the last consumer out resets the aggregate
//! and would hide the leak. Replacements re-attach and the round repeats, so a
//! small per-round delta accumulates into an unmistakable one.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_shared_subscription_permit_accounting -- --nocapture
//! ```
//!
//! `MAGNETAR_PULSAR_IMAGE_TAG` selects the broker under test, so the same
//! binary runs a compatibility matrix (`4.0.4`, `4.2.4`, `5.0.0-M2`, …).
//!
//! Requires Docker on the host.

use std::collections::HashSet;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::runtime_tokio::Consumer;
use magnetar::{MessageId, OutgoingMessage, PulsarClient};
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

/// Receiver-queue size for every consumer here. Small enough that each round's
/// partial receive crosses the half-queue flow threshold immediately, which is
/// what puts a `CommandFlow` on the wire right before the close that races it.
const RECEIVER_QUEUE_SIZE: usize = 4;

/// magnetar's own replenishment trigger — `magnetar-proto`'s
/// `Consumer::flow_threshold`, `(receiver_queue_size / 2).max(1)`. Receiving
/// exactly this many messages is the smallest pop count that emits a
/// non-initial `CommandFlow`.
const FLOW_THRESHOLD: usize = RECEIVER_QUEUE_SIZE / 2;

/// Consumers attached at the start of every round. The scale-down closes all
/// but one at once, so the subscription never reaches zero consumers — the
/// last-consumer-out path resets the aggregate and would erase whatever the
/// churn leaked. Six rather than the minimum three so each round drives five
/// leavers through the race window simultaneously, which also loads the broker
/// executor that has to drain the deferred flow credit.
const CONSUMERS: usize = 6;

/// Churn rounds. The leak accumulates per round, so repeating it turns a
/// per-round delta too small to distinguish from noise into a monotone drift.
const ROUNDS: usize = 3;

/// Messages published per round. Comfortably more than
/// `CONSUMERS * RECEIVER_QUEUE_SIZE` so a real backlog exists behind the
/// in-flight window while the churn happens.
const MESSAGES_PER_ROUND: usize = 40;

/// How long to wait for the broker to finish standalone bootstrap.
const BROKER_READY_TIMEOUT: Duration = Duration::from_mins(4);

/// How long to wait for the broker's topic stats to reach an expected shape.
const ADMIN_POLL_TIMEOUT: Duration = Duration::from_secs(30);
const ADMIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Settle window between "the stats reached the expected consumer count" and
/// the reading the assertions use, so no assertion depends on where the broker
/// happened to be in processing the churn's last frame.
const ADMIN_SETTLE: Duration = Duration::from_secs(2);

/// Per-`receive()` budget. A consumer that stays silent this long is treated as
/// drained, not as failed — the survivors share one backlog, so an empty one is
/// the normal end state.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Appends one `log4j2` logger to the image's own configuration, raising
/// `PersistentDispatcherMultipleConsumers` — and nothing else — to `debug`.
///
/// This is how the dispatcher's `totalAvailablePermits` becomes observable at
/// all: the field is exposed by no admin endpoint (see the module docs), but
/// the dispatcher prints it on every flow it processes and on every
/// `removeConsumer` debit. The entry carries its own `Console` appender with
/// `additivity: false` because the root logger's `AppenderRef` is filtered at
/// `${sys:pulsar.log.level}` (`info`), which would otherwise swallow the very
/// lines this asks for. Raising the root level instead would put the whole
/// broker, bookkeeper and zookeeper at `debug`, which is both enormous and slow
/// enough to perturb the timings this suite measures.
///
/// The block is appended at end of file: YAML comments — which is all the stock
/// configuration has after its last `Logger` entry — do not close the sequence,
/// so the new entry joins it. Verified to raise the logger on `4.2.4` and
/// `5.0.0-M2` on 2026-09-18.
const DISPATCHER_DEBUG_LOGGER_PATCH: &str = concat!(
    r#"printf "
      - name: org.apache.pulsar.broker.service.persistent."#,
    r#"PersistentDispatcherMultipleConsumers
        level: debug
"#,
    r#"        additivity: false
        AppenderRef:
          - ref: Console
""#,
    " >> /pulsar/conf/log4j2.yaml",
);

/// Marker every dispatcher log line carries, used to keep the parser off the
/// per-consumer `Consumer` lines that use the same words.
const DISPATCHER_LOGGER: &str = "PersistentDispatcherMultipleConsumers";

/// The dispatcher's own name for the discarded deferred credit
/// (`internalConsumerFlow`'s `consumerSet` miss). Counted and reported as
/// corroboration that the race window was entered at all — never asserted on,
/// because entering the window is not itself a defect.
const DISCARD_MARKER: &str = "Ignoring flow control from disconnected consumer";

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

/// Start a `pulsar standalone` container and return (`service_url`,
/// `admin_url`, `container_handle`).
///
/// Readiness is polled from the broker itself rather than matched on a startup
/// log line: `apachepulsar/pulsar:5.0.0-M2` logs `Created namespace
/// {clientAppId=null, namespace=public/default}` on **stderr**, where 4.x logs
/// `Created namespace public/default` on stdout (measured 2026-09-18). A
/// log-string wait therefore turns a perfectly healthy 5.x broker into an
/// opaque `StartupTimeout`, which would read as a red test rather than the
/// infrastructure mismatch it is. Same reasoning, and the same shape, as
/// `e2e_scalable_topic.rs`.
async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::Nothing)
        .with_startup_timeout(Duration::from_mins(3))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
        .with_cmd(vec![
            "bash".to_owned(),
            "-c".to_owned(),
            format!("{DISPATCHER_DEBUG_LOGGER_PATCH} && exec bin/pulsar standalone"),
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

/// Poll the broker until `public/default` exists, which is what standalone
/// bootstrap finishing actually means for the topics created below.
async fn await_broker_ready(admin: &AdminClient) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + BROKER_READY_TIMEOUT;
    loop {
        let observation = match admin.namespaces_list("public").await {
            Ok(namespaces) => {
                if namespaces
                    .iter()
                    .any(|namespace| namespace == "public/default")
                {
                    return Ok(());
                }
                format!("{namespaces:?}")
            }
            Err(error) => format!("admin error: {error}"),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "broker did not finish standalone bootstrap within {BROKER_READY_TIMEOUT:?}; \
                 last observation: {observation}"
            )
            .into());
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    }
}

/// The broker's own version string, so the matrix row records what actually
/// ran rather than what the tag was believed to mean. `magnetar-admin` exposes
/// no `brokers/version` verb, so this is a raw GET.
async fn broker_version(admin_url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let body = reqwest::Client::new()
        .get(format!("{admin_url}/admin/v2/brokers/version"))
        .send()
        .await?
        .text()
        .await?;
    Ok(body.trim().to_owned())
}

/// One row of `ConsumerStatsImpl` as the broker reports it. `None` means the
/// broker did not report the field at all, which the invariant treats as a
/// failure rather than as a zero.
#[derive(Debug, Clone)]
struct ConsumerRow {
    name: String,
    available_permits: Option<i64>,
    unacked_messages: Option<i64>,
}

fn subscription_object<'a>(
    stats: &'a TopicStats,
    subscription: &str,
) -> Option<&'a serde_json::Value> {
    stats.subscriptions.get(subscription)
}

/// Every consumer the broker lists on `subscription`, in the order it lists
/// them.
fn consumer_rows(stats: &TopicStats, subscription: &str) -> Vec<ConsumerRow> {
    subscription_object(stats, subscription)
        .and_then(|sub| sub.get("consumers"))
        .and_then(serde_json::Value::as_array)
        .map(|consumers| {
            consumers
                .iter()
                .map(|consumer| ConsumerRow {
                    name: consumer
                        .get("consumerName")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("<unnamed>")
                        .to_owned(),
                    available_permits: consumer
                        .get("availablePermits")
                        .and_then(serde_json::Value::as_i64),
                    unacked_messages: consumer
                        .get("unackedMessages")
                        .and_then(serde_json::Value::as_i64),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A scalar subscription-level field, or `None` when this broker version does
/// not report it.
fn subscription_i64(stats: &TopicStats, subscription: &str, field: &str) -> Option<i64> {
    subscription_object(stats, subscription)
        .and_then(|sub| sub.get(field))
        .and_then(serde_json::Value::as_i64)
}

/// Print, once per run, the subscription-level field names this broker version
/// exposes and whether any of them mentions permits. This is the evidence for
/// the claim that the dispatcher aggregate is unobservable — it is re-measured
/// on every image in the matrix instead of being assumed from one of them.
fn report_subscription_shape(stats: &TopicStats, subscription: &str) {
    let Some(serde_json::Value::Object(map)) = subscription_object(stats, subscription) else {
        eprintln!("[#414-permits] subscription `{subscription}` absent from topic stats");
        return;
    };
    let mut scalar_keys: Vec<&str> = map
        .keys()
        .filter(|key| key.as_str() != "consumers")
        .map(String::as_str)
        .collect();
    scalar_keys.sort_unstable();
    let permit_keys: Vec<&&str> = scalar_keys
        .iter()
        .filter(|key| key.to_lowercase().contains("permit"))
        .collect();
    eprintln!("[#414-permits] subscription-level fields: {scalar_keys:?}");
    eprintln!(
        "[#414-permits] subscription-level permit fields: {permit_keys:?} \
         (empty ⇒ the dispatcher's `totalAvailablePermits` is not observable through admin stats)"
    );
}

/// The invariant this suite exists to check, asserted on numbers the broker
/// reported about itself.
///
/// * `availablePermits >= 0` — the wedge signature of issue #414. The wire protocol carries only
///   monotonic client → broker permit increments, so a negative balance can only come from the
///   broker's own ledger.
/// * `availablePermits <= receiver_queue_size` — the client never grants more than one queue's
///   worth outstanding, so a larger balance is an issue #426-style double grant.
/// * `unackedMessages >= 0` — apache/pulsar#26422's double-debit.
fn assert_permit_invariant(rows: &[ConsumerRow], label: &str) {
    for row in rows {
        let name = &row.name;
        let permits = row.available_permits.unwrap_or_else(|| {
            panic!("{label}: broker reported no `availablePermits` for consumer `{name}`")
        });
        assert!(
            permits >= 0,
            "{label}: broker-side `availablePermits` for consumer `{name}` is {permits}, \
             which is negative — the issue #414 wedge signature. The client can only ever \
             send monotonic permit increments, so this balance came from the broker's own \
             ledger. Full row: {row:?}"
        );
        assert!(
            permits <= RECEIVER_QUEUE_SIZE as i64,
            "{label}: broker-side `availablePermits` for consumer `{name}` is {permits}, \
             above the configured receiver-queue size {RECEIVER_QUEUE_SIZE} — the broker holds \
             more grant than the client believes it issued (issue #426). Full row: {row:?}"
        );
        let unacked = row.unacked_messages.unwrap_or_else(|| {
            panic!("{label}: broker reported no `unackedMessages` for consumer `{name}`")
        });
        assert!(
            unacked >= 0,
            "{label}: broker-side `unackedMessages` for consumer `{name}` is {unacked}, \
             which is negative — the apache/pulsar#26422 double-debit. Full row: {row:?}"
        );
    }
}

/// Poll the broker's topic stats until it lists exactly `expected` consumers on
/// `subscription`, let the reading settle, and return the settled stats.
async fn settled_stats(
    admin: &AdminClient,
    topic: &str,
    subscription: &str,
    expected: usize,
) -> Result<TopicStats, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + ADMIN_POLL_TIMEOUT;
    loop {
        let observation = match admin.topic_stats(topic).await {
            Ok(stats) => {
                if consumer_rows(&stats, subscription).len() == expected {
                    tokio::time::sleep(ADMIN_SETTLE).await;
                    return Ok(admin.topic_stats(topic).await?);
                }
                format!("{:?}", consumer_rows(&stats, subscription))
            }
            // The topic is created by the first subscribe, so a stats call that
            // races that creation 404s. Retry until the deadline.
            Err(error) => format!("admin error: {error}"),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "broker did not list {expected} consumer(s) on `{subscription}` within \
                 {ADMIN_POLL_TIMEOUT:?}; last observation: {observation}"
            )
            .into());
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    }
}

/// Parse an `i64` off the front of `text`, stopping at the first byte that
/// cannot continue one. Used instead of a regex so the suite keeps its
/// dependency set.
fn parse_leading_i64(text: &str) -> Option<i64> {
    let mut end = 0;
    for (index, byte) in text.bytes().enumerate() {
        if byte == b'-' && index == 0 {
            end = 1;
        } else if byte.is_ascii_digit() {
            end = index + 1;
        } else {
            break;
        }
    }
    text.get(..end).and_then(|digits| digits.parse().ok())
}

/// Every `i64` that follows an occurrence of `needle` in `line`.
fn values_after(line: &str, needle: &str, out: &mut Vec<i64>) {
    let mut rest = line;
    while let Some(offset) = rest.find(needle) {
        rest = &rest[offset + needle.len()..];
        if let Some(value) = parse_leading_i64(rest) {
            out.push(value);
        }
    }
}

/// Every value of the dispatcher's `totalAvailablePermits` the broker printed,
/// plus how many times it took `internalConsumerFlow`'s discard branch.
///
/// Three spellings of the same field, because the message changed between
/// broker generations and the matrix spans both (all measured 2026-09-18):
///
/// * 4.x — `… with permits <N> after adding <M> permits`
/// * 4.x — `… New dispatcher permit count is <N>`
/// * 5.x — structured key/value, `… {…, totalAvailablePermits=<N>}`
///
/// Only lines from the dispatcher logger are considered: `Consumer` logs
/// "Added more flow control message permits" with the same vocabulary for the
/// *per-consumer* counter, which is a different number.
fn dispatcher_aggregate_permits(logs: &str) -> (Vec<i64>, usize) {
    let mut values = Vec::new();
    let mut discards = 0;
    for line in logs.lines() {
        if !line.contains(DISPATCHER_LOGGER) {
            continue;
        }
        if line.contains(DISCARD_MARKER) {
            discards += 1;
        }
        values_after(line, "totalAvailablePermits=", &mut values);
        values_after(line, "with permits ", &mut values);
        values_after(line, "New dispatcher permit count is ", &mut values);
    }
    (values, discards)
}

/// Assert the invariant issue #414's root cause breaks: the dispatcher's
/// subscription-wide permit ledger never goes negative.
///
/// Reading zero observations is its own, separately-worded failure. It means
/// the debug logger did not come up or the broker renamed the message — an
/// infrastructure fault, not a clean ledger — and a check that cannot measure
/// must never report success.
fn assert_aggregate_invariant(logs: &str) {
    let (values, discards) = dispatcher_aggregate_permits(logs);
    let minimum = values.iter().copied().min();
    let negatives = values.iter().filter(|value| **value < 0).count();
    eprintln!(
        "[#414-permits] dispatcher totalAvailablePermits: observations={} min={minimum:?} \
         negatives={negatives} discardedDeferredCredits={discards}",
        values.len(),
    );
    assert!(
        !values.is_empty(),
        "could not observe the dispatcher's `totalAvailablePermits` at all: the \
         `{DISPATCHER_LOGGER}` debug logger did not come up, or this broker renamed the \
         message the parser looks for. This is an infrastructure failure, NOT a clean \
         permit ledger — the aggregate is exposed by no admin endpoint, so the broker's \
         own log is the only oracle for it."
    );
    let minimum = minimum.unwrap_or_default();
    assert!(
        minimum >= 0,
        "the dispatcher's subscription-wide `totalAvailablePermits` reached {minimum}, which \
         is negative — issue #414's broker-side root cause (apache/pulsar#26416). The \
         deferred flow credit of a consumer that has already left `consumerSet` is discarded \
         by `internalConsumerFlow` while `removeConsumer` still debits the aggregate by that \
         consumer's full counter. Observed {negatives} negative reading(s) of {} total, with \
         {discards} discarded deferred credit(s) during the run. Fixed upstream by \
         apache/pulsar#26289 and apache/pulsar#26422.",
        values.len(),
    );
}

async fn subscribe_one(
    client: &PulsarClient,
    topic: &str,
    subscription: &str,
    name: &str,
) -> Result<Consumer, Box<dyn std::error::Error>> {
    Ok(client
        .consumer(topic)
        .subscription(subscription)
        .subscription_type(SubType::Shared)
        .name(name)
        .receiver_queue_size(RECEIVER_QUEUE_SIZE)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?)
}

/// Receive exactly `count` messages **without acking them**, so the consumer
/// holds pending acks and has just crossed `maybe_flow`'s half-queue threshold.
/// Returns the payloads and the un-acked ids.
async fn receive_without_ack(consumer: &Consumer, count: usize) -> (Vec<Vec<u8>>, Vec<MessageId>) {
    let mut payloads = Vec::with_capacity(count);
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let Ok(Ok(message)) = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive()).await
        else {
            break;
        };
        payloads.push(message.payload.to_vec());
        ids.push(message.message_id);
    }
    (payloads, ids)
}

/// Receive and ack until the consumer stays silent for `RECEIVE_TIMEOUT` or
/// `max` messages have been taken.
async fn drain_and_ack(consumer: &Consumer, max: usize) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    while payloads.len() < max {
        let Ok(Ok(message)) = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive()).await
        else {
            break;
        };
        payloads.push(message.payload.to_vec());
        let _ = consumer.ack(message.message_id).await;
    }
    payloads
}

/// Drive every leaver through the race window at once: each one crosses
/// `maybe_flow`'s half-queue threshold and then closes with **nothing awaited
/// in between**, so its `CommandFlow` and its own `CommandCloseConsumer` reach
/// the broker's IO thread back to back. That ordering is the whole point — the
/// deferred aggregate credit is only discarded when `removeConsumer` has
/// already emptied `consumerSet` by the time the broker executor drains
/// `internalConsumerFlow`. Running the leavers concurrently also loads that
/// executor, which is the other half of the window.
///
/// Returns what the leavers received, all of it left un-acked so
/// `removeConsumer` has pending acks to redeliver.
async fn flow_then_close_concurrently(
    leavers: Vec<(String, Consumer)>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut handles = Vec::with_capacity(leavers.len());
    for (name, consumer) in leavers {
        handles.push(tokio::spawn(async move {
            let (payloads, _unacked) = receive_without_ack(&consumer, FLOW_THRESHOLD).await;
            (name, payloads, consumer.close().await)
        }));
    }
    let mut received = Vec::new();
    for handle in handles {
        let (name, payloads, outcome) = handle.await.map_err(|error| error.to_string())?;
        outcome.map_err(|error| format!("closing leaver `{name}`: {error}"))?;
        received.extend(payloads);
    }
    Ok(received)
}

/// Log a settled observation and assert the invariant on it.
fn observe(stats: &TopicStats, subscription: &str, label: &str) {
    let rows = consumer_rows(stats, subscription);
    let permit_sum: i64 = rows.iter().filter_map(|row| row.available_permits).sum();
    eprintln!(
        "[#414-permits] {label}: consumers={} permits={:?} unacked={:?} sum(permits)={permit_sum} \
         subUnacked={:?} backlog={:?}",
        rows.len(),
        rows.iter()
            .map(|row| (row.name.clone(), row.available_permits))
            .collect::<Vec<_>>(),
        rows.iter()
            .map(|row| (row.name.clone(), row.unacked_messages))
            .collect::<Vec<_>>(),
        subscription_i64(stats, subscription, "unackedMessages"),
        subscription_i64(stats, subscription, "msgBacklog"),
    );
    assert_permit_invariant(&rows, label);
    if let Some(unacked) = subscription_i64(stats, subscription, "unackedMessages") {
        assert!(
            unacked >= 0,
            "{label}: subscription-level `unackedMessages` is {unacked}, which is negative \
             (apache/pulsar#26422 double-debit)"
        );
    }
}

/// One churn round: publish a backlog, put every live consumer one flow
/// round-trip in with un-acked work, close all but the survivor at once, and
/// read the broker's accounting on both sides of the drain that follows.
async fn churn_round(
    admin: &AdminClient,
    producer: &magnetar::runtime_tokio::Producer,
    topic: &str,
    subscription: &str,
    live: &mut Vec<(String, Consumer)>,
    round: usize,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), Box<dyn std::error::Error>> {
    let mut sent = Vec::with_capacity(MESSAGES_PER_ROUND);
    for index in 0..MESSAGES_PER_ROUND {
        let payload = format!("permit-r{round}-m{index}").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
        sent.push(payload);
    }

    // The churn. The survivor is never closed: the last consumer out resets the
    // dispatcher aggregate and would erase whatever this round leaked, so it
    // takes its own share of the backlog on this task while every leaver runs
    // the flow-then-close window concurrently.
    let survivor = live.remove(0);
    let leavers: Vec<(String, Consumer)> = std::mem::take(live);
    let leaver_count = leavers.len();
    let churn = tokio::spawn(flow_then_close_concurrently(leavers));
    let (mut received, survivor_unacked) = receive_without_ack(&survivor.1, FLOW_THRESHOLD).await;
    received.extend(churn.await.map_err(|error| error.to_string())??);
    live.push(survivor);

    let stats = settled_stats(admin, topic, subscription, live.len()).await?;
    if round == 0 {
        report_subscription_shape(&stats, subscription);
    }
    observe(
        &stats,
        subscription,
        &format!("round {round} settled after closing {leaver_count} of {CONSUMERS} consumers"),
    );

    // The survivor clears its own pending acks, then absorbs the round's
    // backlog plus everything the leavers left un-acked.
    for id in survivor_unacked {
        let _ = live[0].1.ack(id).await;
    }
    received.extend(drain_and_ack(&live[0].1, MESSAGES_PER_ROUND * 2).await);

    let stats = settled_stats(admin, topic, subscription, live.len()).await?;
    observe(
        &stats,
        subscription,
        &format!("round {round} settled after the survivor drained the backlog"),
    );

    Ok((sent, received))
}

/// Repeated `Shared`-subscription scale-downs must leave the broker's own
/// permit ledger intact: no per-consumer `availablePermits` outside
/// `[0, receiver_queue_size]`, no negative `unackedMessages`, and a backlog
/// that still reaches zero.
///
/// # Why this one e2e test is `#[ignore]`d
///
/// [ADR-0046](../../../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) makes the
/// e2e suite run as a regular `cargo test`, deliberately: an e2e test behind `#[ignore]` is an
/// e2e test nobody runs. This is the documented exception, and it is narrow.
///
/// Every other e2e test asserts something about **magnetar**. This one asserts
/// something about the **broker**: that `apache/pulsar#26416`'s deferred-flow
/// accounting leak is absent. The client's behaviour cannot make it pass or fail, and
/// no generally-available image carries the fix — Docker Hub's `apachepulsar/pulsar`
/// stops at 4.0.13 and 4.2.4, and the fixed 4.0.14 / 4.2.5 are unpublished. Left
/// running on the `latest` default it would therefore be **permanently red in CI on a
/// defect this repository cannot fix**, which is the failure mode the enforcement rules
/// exist to prevent: a check that is always failing stops being read.
///
/// It keeps the `latest` default rather than pinning a green image, so the day an image
/// with the fix ships as `latest` the reproduce command below goes green with no edit.
/// Remove the `#[ignore]` then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "asserts apache/pulsar#26416; no GA image carries the fix (4.0.14/4.2.5 unpublished). Reproduce: MAGNETAR_PULSAR_IMAGE_TAG=4.2.4 cargo test -p magnetar-driver --test e2e_shared_subscription_permit_accounting -- --ignored"]
async fn e2e_shared_subscription_permit_accounting_survives_repeated_churn()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, container) = start_pulsar().await?;

    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    await_broker_ready(&admin).await?;
    let version = broker_version(&admin_url)
        .await
        .unwrap_or_else(|error| format!("<unavailable: {error}>"));
    eprintln!(
        "[#414-permits] image={}:{} brokerVersion={version}",
        image_repo(),
        image_tag()
    );

    // Deliberately no `consumer_stall_timeout` / `consumer_stall_auto_recovery`:
    // an automatic re-subscribe would itself add and remove consumers, and every
    // number below is read off the broker's ledger for the consumers this test
    // attached on purpose.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-permits-{suffix}");
    let subscription = format!("magnetar-e2e-permits-{suffix}");

    let mut live: Vec<(String, Consumer)> = Vec::with_capacity(CONSUMERS);
    for index in 0..CONSUMERS {
        let name = format!("permit-r0-c{index}");
        let consumer = subscribe_one(&client, &topic, &subscription, &name).await?;
        live.push((name, consumer));
    }

    let stats = settled_stats(&admin, &topic, &subscription, CONSUMERS).await?;
    observe(&stats, &subscription, "after the initial subscribe");

    let producer = client.producer(&topic).create().await?;
    let mut sent: HashSet<Vec<u8>> = HashSet::new();
    let mut received: HashSet<Vec<u8>> = HashSet::new();

    for round in 0..ROUNDS {
        let (round_sent, round_received) =
            churn_round(&admin, &producer, &topic, &subscription, &mut live, round).await?;
        sent.extend(round_sent);
        received.extend(round_received);

        // Re-attach replacements for the next round. The survivor stays, so the
        // consumer count never touches zero across the whole run.
        if round + 1 < ROUNDS {
            for index in 1..CONSUMERS {
                let name = format!("permit-r{}-c{index}", round + 1);
                let consumer = subscribe_one(&client, &topic, &subscription, &name).await?;
                live.push((name, consumer));
            }
        }
    }

    // Final sweep so nothing is left un-acked behind the assertions below.
    for (_, consumer) in &live {
        received.extend(drain_and_ack(consumer, MESSAGES_PER_ROUND).await);
    }
    let final_stats = settled_stats(&admin, &topic, &subscription, live.len()).await?;
    observe(&final_stats, &subscription, "final settled reading");

    producer.close().await?;
    for (_, consumer) in live {
        consumer.close().await?;
    }
    client.close().await;

    assert_eq!(
        received,
        sent,
        "every published message must reach a live consumer across {ROUNDS} churn round(s); \
         received {} distinct of {} published",
        received.len(),
        sent.len(),
    );

    // A subscription whose ledger survived the churn also empties. Read from
    // the broker, not inferred from the client's own counting.
    let backlog = subscription_i64(&final_stats, &subscription, "msgBacklog");
    assert_eq!(
        backlog,
        Some(0),
        "the subscription must have drained to an empty backlog with a survivor attached; \
         broker reports {backlog:?}"
    );

    // The aggregate. Read last, from the broker's own output, because no admin
    // endpoint carries it. Both streams are concatenated: the dedicated
    // `Console` appender targets `SYSTEM_OUT`, but the stock routing appender
    // sends some versions' lines to `SYSTEM_ERR`, and which stream a line lands
    // on is not part of what this is measuring.
    let mut logs = String::from_utf8_lossy(&container.stdout_to_vec().await?).into_owned();
    logs.push_str(&String::from_utf8_lossy(&container.stderr_to_vec().await?));
    assert_aggregate_invariant(&logs);

    Ok(())
}
