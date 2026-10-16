// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 — scalable-topic end-to-end coverage against a real
//! Apache Pulsar broker.
//!
//! Runs against the shared fixture's `Broker::Scalable` image (`common/pulsar.rs`,
//! ADR-0115), Pulsar 5.0.0 GA, the release that carries PIP-460. Its `standalone.conf` sets
//! `scalableTopicsEnabled=true`; the test sets it explicitly anyway so it does not silently depend
//! on an upstream default that may move.
//!
//! Per [ADR-0046] these are ordinary tests — no `#[ignore]`, no `feature =
//! "e2e"` — gated only on `feature = "scalable-topics"` like the rest of the
//! surface. They require Docker on the host.
//!
//! # Test inventory
//!
//! 1. [`e2e_hardened_scalable_stream_consumer_contract`] — one transaction-enabled GA broker proves
//!    typed multi-segment delivery, live and restored-vector acknowledgement, broker-effective
//!    vector seek replay, transaction commit/abort, single-member split progression in Strict mode,
//!    explicit broker-managed cross-member behavior, reachable broker-authored controller and
//!    segment authorities matching the bootstrap transport, and clean controller membership leave.
//!    Because the standalone broker controls child assignment, this does not isolate client-side
//!    Strict gating or prove same-cluster multi-broker routing.
//! 2. [`e2e_scalable_topic_info_cli_round_trip`] — the `magnetarctl topic-info` view of the topic
//!    matches the layout the client library resolves, so the CLI and the driver cannot drift.
//! 3. [`e2e_scalable_topic_drops_on_broker_split`] — an admin-triggered segment split bumps the
//!    layout epoch on a live session, which surfaces the drop-on-change event with the split
//!    classified from the children's `parent_ids` (ADR-0093 §D2).
//! 4. [`e2e_scalable_topic_unsupported_on_v4_broker`] — the other half of ADR-0093 §D3 against a
//!    **real Pulsar 4.x broker**: the capability is negotiated away and the scalable path refuses
//!    rather than emitting a command the peer cannot parse. That is the guarantee that lets this
//!    surface ship without breaking existing deployments, and it is only provable against a real v4
//!    broker.
//! 5. [`e2e_scalable_topic_read_only_lookup_does_not_create_the_topic`] — `lookup_scalable_topic`
//!    is read-only: on a never-created topic it reports "not found", and the broker's admin REST
//!    still answers 404 for that topic afterwards. Pulsar 5.0.0 auto-creates the topic for a lookup
//!    that leaves `create_if_missing` unset, so this is what the explicit `false` is for.
//!
//! 6. [`e2e_scalable_stream_close_unregisters_with_a_live_pooled_connection`] — repeated same-name
//!    subscribe/close removes controller membership before the pooled client transport closes.
//! 7. [`e2e_scalable_bucket_owners_preserve_keys_and_transactional_acknowledgements`] — two members
//!    share one segment by entry bucket, preserving key ownership and order, repeated cumulative
//!    acknowledgement idempotence, and transactional commit/abort durability. Both this and clean
//!    leave reuse the same assertions through Tokio and Moonpool clients when `moonpool` is
//!    enabled.
//!
//! [ADR-0046]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::select_all;
use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::schema::BytesSchema;
use magnetar::scalable::{
    OrderingMode, PositionVector, ReceiverBudget, SegmentSource, StreamConsumer,
    StreamConsumerError, StreamConsumerEvent, StreamMessage, TransactionOutcome,
};
use testcontainers::GenericImage;
use testcontainers::core::{CmdWaitFor, ExecCommand};

mod common;

use common::pulsar::{Broker, Pulsar, init_tracing};

const TENANT_NS: &str = "public/default";
const STREAM_BUDGET_BYTES: usize = 16 * 1024 * 1024;
const SETTLED_METADATA_SLACK_BYTES: usize = 64 * 1024;
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_TIMEOUT: Duration = Duration::from_secs(30);
const NO_DELIVERY_WINDOW: Duration = Duration::from_secs(3);
const TXN_TIMEOUT: Duration = Duration::from_secs(30);

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type PulsarContainer = testcontainers::ContainerAsync<GenericImage>;
type ByteStreamConsumer<E = magnetar::TokioEngine> = StreamConsumer<BytesSchema, E>;
type ByteStreamMessage = StreamMessage<BytesSchema>;
type ReceiveFuture<'a> = Pin<
    Box<dyn Future<Output = (usize, Result<ByteStreamMessage, StreamConsumerError>)> + Send + 'a>,
>;

struct HostPortProxy {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for HostPortProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn spawn_host_port_proxy(listener: tokio::net::TcpListener, target: String) -> HostPortProxy {
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut inbound, _)) = accepted else {
                        return;
                    };
                    let target = target.clone();
                    connections.spawn(async move {
                        let mut outbound = tokio::net::TcpStream::connect(target).await?;
                        tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await?;
                        Ok::<(), std::io::Error>(())
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    match completed {
                        Some(Ok(Err(error))) => {
                            tracing::warn!(error = %error, "hardened broker proxy I/O failed");
                        }
                        Some(Err(error)) => {
                            tracing::warn!(error = %error, "hardened broker proxy task failed");
                        }
                        Some(Ok(Ok(()))) | None => {}
                    }
                }
            }
        }
    });
    HostPortProxy { task }
}

/// Locate the compiled `magnetarctl` binary.
///
/// `CARGO_BIN_EXE_magnetarctl` is only injected for tests in the `magnetarctl`
/// package, so this test — which lives with the façade's e2e suite — resolves it
/// from its own executable's directory instead (`target/<profile>/deps/<test>`
/// → `target/<profile>/magnetarctl`).
fn magnetarctl_binary() -> std::path::PathBuf {
    let mut dir = std::env::current_exe().expect("test executable path");
    dir.pop(); // deps/
    if dir.ends_with("deps") {
        dir.pop();
    }
    let exe = dir.join(format!("magnetarctl{}", std::env::consts::EXE_SUFFIX));
    assert!(
        exe.exists(),
        "magnetarctl not built at {}; run `cargo build -p magnetarctl --features scalable-topics`",
        exe.display()
    );
    exe
}

/// Bring up one unauthenticated standalone broker.
///
/// PIP-460 is configured only for [`Broker::Scalable`]; it is passed explicitly
/// rather than relying on the upstream default, so the negative (v4) case and
/// the positive case differ only in the image.
///
/// The shared fixture waits for the broker to serve `public/default` by asking
/// the broker itself (ADR-0115) and applies the `PULSAR_PREFIX_*` configuration
/// to `conf/standalone.conf` before it boots. This suite used to export these
/// two keys without ever running the helper that applies them, so neither took
/// effect.
async fn start_broker(
    broker: Broker,
) -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let mut pulsar = Pulsar::new(broker)
        .flag("--no-functions-worker")
        .flag("--no-stream-storage");
    if broker == Broker::Scalable {
        pulsar = pulsar
            // `standalone.conf` of Pulsar 5.0.0 sets this to `true`; set it
            // explicitly so the test does not depend on an upstream default.
            .config("scalableTopicsEnabled", "true")
            // PIP-483 auto split/merge also defaults on. Disable it so the only
            // layout change in these tests is the one the admin API triggers —
            // otherwise a background split could race the assertions.
            .config("scalableTopicAutoScaleEnabled", "false");
    }
    Ok(pulsar.start().await?.into_parts())
}

/// Reserve a reachable direct authority without sharing a fixed host port.
/// The common fixture owns startup, readiness, version checks and JVM limits;
/// this proxy only forwards the broker-advertised port to Docker's mapping.
async fn start_hardened_broker() -> TestResult<(String, PulsarContainer, HostPortProxy)> {
    init_tracing();
    let proxy_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let broker_port = proxy_listener.local_addr()?.port();
    let running = Pulsar::new(Broker::Scalable)
        .binary_port(broker_port)
        .config("scalableTopicsEnabled", "true")
        .config("scalableTopicAutoScaleEnabled", "false")
        .config("transactionCoordinatorEnabled", "true")
        .config("advertisedAddress", "127.0.0.1")
        .pre_start(
            "bin/pulsar initialize-transaction-coordinator-metadata \
             --cluster standalone --configuration-store rocksdb:///pulsar/data/metadata",
        )
        .flag("--no-functions-worker")
        .flag("--no-stream-storage")
        .start()
        .await?;
    let loopback = running
        .host
        .parse::<std::net::Ipv4Addr>()
        .is_ok_and(|address| address.is_loopback())
        || running.host.eq_ignore_ascii_case("localhost");
    if !loopback {
        return Err(format!(
            "direct-routing scalable e2e requires a local IPv4 Docker host, got `{}`",
            running.host
        )
        .into());
    }
    let proxy = spawn_host_port_proxy(
        proxy_listener,
        format!("{}:{}", running.host, running.binary_port),
    );
    Ok((
        format!("pulsar://127.0.0.1:{broker_port}"),
        running.container,
        proxy,
    ))
}

/// Run `pulsar-admin` inside the container, returning its exit code alongside
/// the combined output.
///
/// Separate from [`pulsar_admin`] because the readiness poll needs a non-zero
/// exit to mean "the broker is not serving yet, poll again" rather than "give
/// up". `pulsar-admin` exits 1 with `Connection refused: localhost/127.0.0.1:8080`
/// for the whole of standalone's startup, so propagating that aborts the wait on
/// its first iteration — which is how a correct exit-code check took all four
/// e2e tests red at once.
async fn pulsar_admin_raw(
    container: &testcontainers::ContainerAsync<GenericImage>,
    args: &[&str],
) -> Result<(Option<i64>, String), Box<dyn std::error::Error>> {
    let mut command = vec!["bin/pulsar-admin".to_owned()];
    command.extend(args.iter().map(|a| (*a).to_owned()));
    // `CmdWaitFor::exit()` so the exec is only read back once the process has
    // terminated. Without it `exit_code()` is free to answer `None`, which
    // testcontainers documents as "the command has not yet exited" — not as
    // success.
    let mut out = container
        .exec(ExecCommand::new(command).with_cmd_ready_condition(CmdWaitFor::exit()))
        .await?;
    let stdout = String::from_utf8_lossy(&out.stdout_to_vec().await?).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr_to_vec().await?).into_owned();
    let combined = format!("{stdout}{stderr}");
    Ok((out.exit_code().await?, combined))
}

/// Run `pulsar-admin` inside the container, asserting it exited zero.
///
/// The exit code is the check, not a substring scan of the output. An earlier
/// version sniffed for the word "error" and so silently tolerated picocli's
/// usage message when `split-segment` was called with the segment id
/// positionally instead of behind `--segment-id`: the split never happened and
/// the test spent 60 s waiting for a layout change that was never coming.
///
/// It is deliberately the *only* success check on a command. Upstream's success
/// banners — "Created scalable topic", "Split segment" — are not part of any
/// contract this repository can verify, and asserting on one is a guess about
/// another project's stdout. Callers assert the observable *effect* instead.
async fn pulsar_admin(
    container: &testcontainers::ContainerAsync<GenericImage>,
    args: &[&str],
) -> Result<String, Box<dyn std::error::Error>> {
    let (code, combined) = pulsar_admin_raw(container, args).await?;
    match code {
        Some(0) => Ok(combined),
        Some(code) => {
            Err(format!("pulsar-admin {} exited {code}:\n{combined}", args.join(" ")).into())
        }
        // `None` is "has not yet exited", never success. Treating it as success
        // would restore, in a subtler form, exactly the fail-open the exit-code
        // check exists to remove.
        None => Err(format!(
            "pulsar-admin {} did not exit; no status to check:\n{combined}",
            args.join(" ")
        )
        .into()),
    }
}

/// Render a resolved layout for a failure message.
///
/// Prints each segment's `parent_ids` / `child_ids` edges and state, because
/// those edges are exactly what the split derivation reads: a layout that
/// advanced its epoch but carries no edges tells a different story from one
/// the broker never changed at all.
fn describe_layout(epoch: u64, segments: &[magnetar::scalable::SegmentDescriptor]) -> String {
    let rendered = segments
        .iter()
        .map(|s| {
            format!(
                "{}(parents={:?}, children={:?}, state={:?})",
                s.segment_id.0,
                s.parent_ids.iter().map(|p| p.0).collect::<Vec<_>>(),
                s.child_ids.iter().map(|c| c.0).collect::<Vec<_>>(),
                s.state
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("epoch={epoch}, segments=[{rendered}]")
}

/// Create a scalable topic with `segments` initial segments.
async fn create_scalable_topic(
    container: &testcontainers::ContainerAsync<GenericImage>,
    topic: &str,
    segments: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let segments = segments.to_string();
    // A non-zero exit fails the call inside `pulsar_admin`; the topic's actual
    // existence is proven by the lookup every caller performs against it, which
    // is a stronger check than any success banner.
    pulsar_admin(
        container,
        &["scalable-topics", "create", topic, "--segments", &segments],
    )
    .await?;
    Ok(())
}

/// Publish distinct opaque payloads through Pulsar's bundled V5 client. This is the
/// real scalable producer path, not one producer opened directly per segment.
async fn publish_scalable_messages(
    container: &testcontainers::ContainerAsync<GenericImage>,
    service_url: &str,
    topic: &str,
    payloads: &[String],
) -> TestResult {
    publish_scalable_messages_with_key(container, service_url, topic, payloads, None).await
}

async fn publish_scalable_messages_with_key(
    container: &PulsarContainer,
    service_url: &str,
    topic: &str,
    payloads: &[String],
    key: Option<&str>,
) -> TestResult {
    if payloads.is_empty() || payloads.iter().any(|payload| payload.contains(',')) {
        return Err("scalable publisher requires non-empty comma-free payloads".into());
    }
    let messages = payloads.join(",");
    let mut command = vec![
        "bin/pulsar-client".to_owned(),
        "--url".to_owned(),
        service_url.to_owned(),
        "produce".to_owned(),
        "--disable-batching".to_owned(),
        "--messages".to_owned(),
        messages,
        topic.to_owned(),
    ];
    if let Some(key) = key {
        command.extend(["--key".to_owned(), key.to_owned()]);
    }
    let mut output = container
        .exec(ExecCommand::new(command).with_cmd_ready_condition(CmdWaitFor::exit()))
        .await?;
    let stdout = String::from_utf8_lossy(&output.stdout_to_vec().await?).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr_to_vec().await?).into_owned();
    match output.exit_code().await? {
        Some(0) => Ok(()),
        Some(code) => Err(format!(
            "bundled V5 pulsar-client failed to publish to {topic} with exit {code}:\n{stdout}{stderr}"
        )
        .into()),
        None => Err(format!(
            "bundled V5 pulsar-client did not exit while publishing to {topic}:\n{stdout}{stderr}"
        )
        .into()),
    }
}

fn unique_scalable_topic(label: &str, suffix: &str) -> String {
    format!("topic://{TENANT_NS}/e2e-hardened-{label}-{suffix}")
}

fn payloads(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index}"))
        .collect()
}

async fn subscribe_byte_stream<E: magnetar::Engine>(
    client: &PulsarClient<E>,
    topic: &str,
    subscription: &str,
    consumer_name: &str,
    ordering_mode: OrderingMode,
) -> TestResult<ByteStreamConsumer<E>>
where
    E::ClientState: magnetar::SegmentSubscriberApi,
{
    let budget = ReceiverBudget::bytes(STREAM_BUDGET_BYTES)?;
    Ok(client
        .scalable_stream_consumer(topic, Arc::new(BytesSchema::new()))
        .subscription(subscription)
        .consumer_name(consumer_name)
        .receiver_budget(budget)
        .ordering_mode(ordering_mode)
        .subscribe()
        .await?)
}

async fn wait_for_status(
    consumer: &ByteStreamConsumer,
    description: &str,
    mut predicate: impl FnMut(&magnetar::scalable::StreamConsumerStatus) -> bool,
) -> TestResult<magnetar::scalable::StreamConsumerStatus> {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    loop {
        let status = consumer.status();
        if predicate(&status) {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "stream consumer did not reach {description} within {STATUS_TIMEOUT:?}; \
                 last status: {status:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_status_with_events<E: magnetar::Engine>(
    consumer: &ByteStreamConsumer<E>,
    description: &str,
    mut predicate: impl FnMut(&magnetar::scalable::StreamConsumerStatus) -> bool,
) -> TestResult<magnetar::scalable::StreamConsumerStatus> {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    let mut statuses = Vec::new();
    let mut events = Vec::new();
    loop {
        let status = consumer.status();
        if statuses.last() != Some(&status) {
            if statuses.len() == 32 {
                statuses.remove(0);
            }
            statuses.push(status.clone());
        }
        if predicate(&status) {
            return Ok(status);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "stream consumer did not reach {description} within {STATUS_TIMEOUT:?}; \
                 status history: {statuses:#?}; event history: {events:#?}"
            )
            .into());
        }
        match tokio::time::timeout(
            remaining.min(Duration::from_millis(100)),
            consumer.next_event(),
        )
        .await
        {
            Ok(Ok(Some(event))) => {
                if events.len() == 32 {
                    events.remove(0);
                }
                events.push(event);
            }
            Ok(Ok(None)) => {
                if events.len() == 32 {
                    events.remove(0);
                }
                events.push(StreamConsumerEvent::Closed);
            }
            Ok(Err(error)) => {
                return Err(format!(
                    "stream consumer event stream failed while waiting for {description}: \
                     {error}; status history: {statuses:#?}; event history: {events:#?}"
                )
                .into());
            }
            Err(_) => {}
        }
    }
}

async fn wait_for_attached_flow(
    consumer: &ByteStreamConsumer,
    description: &str,
    segments: usize,
) -> TestResult<usize> {
    Ok(wait_for_status(consumer, description, |status| {
        status.assigned_segments() == segments
            && status.attached_segments() == segments
            && status.receiver_budget_used() > 0
    })
    .await?
    .receiver_budget_used())
}

async fn wait_for_settled_delivery_leases(
    consumer: &ByteStreamConsumer,
    description: &str,
    idle_budget_used: usize,
) -> TestResult {
    wait_for_status(consumer, description, |status| {
        status.receiver_budget_used()
            <= idle_budget_used.saturating_add(SETTLED_METADATA_SLACK_BYTES)
    })
    .await?;
    Ok(())
}

async fn receive_expected_messages<E: magnetar::Engine>(
    consumer: &ByteStreamConsumer<E>,
    payloads: &[String],
    context: &str,
) -> TestResult<Vec<ByteStreamMessage>> {
    let mut remaining: BTreeSet<String> = payloads.iter().cloned().collect();
    if remaining.len() != payloads.len() {
        return Err(format!("{context} payload fixture contains duplicates").into());
    }
    let mut messages = Vec::with_capacity(payloads.len());
    for _ in payloads {
        let message = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive())
            .await
            .map_err(|_| {
                format!("{context} did not receive all messages within {RECEIVE_TIMEOUT:?}")
            })??;
        if message.value().as_ref() != message.payload() {
            return Err(format!(
                "{context} typed BytesSchema value differs from its decoded payload"
            )
            .into());
        }
        let value = std::str::from_utf8(message.value().as_ref())?.to_owned();
        if !remaining.remove(&value) {
            return Err(format!(
                "{context} received unexpected or duplicate payload `{value}` from {}",
                message.source().topic()
            )
            .into());
        }
        messages.push(message);
    }
    if !remaining.is_empty() {
        return Err(format!("{context} did not receive payloads {remaining:?}").into());
    }
    Ok(messages)
}

fn assert_stream_payloads(
    messages: &[ByteStreamMessage],
    expected: &[String],
    context: &str,
) -> TestResult {
    let actual: Vec<&[u8]> = messages.iter().map(StreamMessage::payload).collect();
    let expected: Vec<&[u8]> = expected.iter().map(String::as_bytes).collect();
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}: {actual:?}").into())
    }
}

async fn receive_from_any<E: magnetar::Engine>(
    consumers: &[ByteStreamConsumer<E>],
    indexes: &[usize],
    context: &str,
) -> TestResult<(usize, ByteStreamMessage)> {
    if indexes.is_empty() || indexes.iter().any(|index| *index >= consumers.len()) {
        return Err(format!("{context} has no valid stream consumer candidates").into());
    }
    let receives: Vec<ReceiveFuture<'_>> = indexes
        .iter()
        .copied()
        .map(|index| {
            let consumer = &consumers[index];
            Box::pin(async move { (index, consumer.receive().await) }) as ReceiveFuture<'_>
        })
        .collect();
    let ((index, result), _, _) = tokio::time::timeout(RECEIVE_TIMEOUT, select_all(receives))
        .await
        .map_err(|_| format!("{context} received no message within {RECEIVE_TIMEOUT:?}"))?;
    Ok((index, result?))
}

async fn wait_for_transaction_outcome<E: magnetar::Engine>(
    consumer: &ByteStreamConsumer<E>,
    transaction: magnetar::Transaction,
    expected: TransactionOutcome,
) -> TestResult {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "transaction {} did not surface aggregate outcome {expected:?}",
                transaction.id()
            )
            .into());
        }
        match tokio::time::timeout(remaining, consumer.next_event()).await {
            Ok(Ok(Some(StreamConsumerEvent::TransactionOutcome { txn_id, outcome })))
                if txn_id == transaction.id() =>
            {
                if outcome != expected {
                    return Err(format!(
                        "transaction {txn_id} surfaced {outcome:?}, expected {expected:?}"
                    )
                    .into());
                }
                return Ok(());
            }
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => {
                return Err(format!(
                    "aggregate closed before transaction {} surfaced {expected:?}",
                    transaction.id()
                )
                .into());
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(format!(
                    "transaction {} did not surface aggregate outcome {expected:?}",
                    transaction.id()
                )
                .into());
            }
        }
    }
}

async fn scalable_consumer_count(
    container: &testcontainers::ContainerAsync<GenericImage>,
    topic: &str,
    subscription: &str,
) -> TestResult<usize> {
    let output = pulsar_admin(container, &["scalable-topics", "stats", topic]).await?;
    let start = output
        .find('{')
        .ok_or_else(|| format!("scalable stats returned no JSON object:\n{output}"))?;
    let end = output
        .rfind('}')
        .ok_or_else(|| format!("scalable stats returned incomplete JSON:\n{output}"))?;
    let stats: serde_json::Value = serde_json::from_str(&output[start..=end])?;
    let subscriptions = stats
        .get("subscriptions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("scalable stats lack `subscriptions`: {stats}"))?;
    let Some(subscription_stats) = subscriptions.get(subscription) else {
        return Ok(0);
    };
    let consumers = subscription_stats
        .get("consumers")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "scalable stats for subscription `{subscription}` lack `consumers`: \
                 {subscription_stats}"
            )
        })?;
    Ok(consumers.len())
}

async fn wait_for_consumer_count(
    container: &testcontainers::ContainerAsync<GenericImage>,
    topic: &str,
    subscription: &str,
    expected: usize,
) -> TestResult {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    loop {
        let actual = scalable_consumer_count(container, topic, subscription).await?;
        if actual == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "scalable subscription `{subscription}` on `{topic}` retained {actual} \
                 controller consumer(s), expected {expected} within {STATUS_TIMEOUT:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn force_delete_scalable_topics(
    container: &testcontainers::ContainerAsync<GenericImage>,
    topics: &[String],
) -> TestResult {
    let mut failures = Vec::new();
    for topic in topics {
        if let Err(error) =
            pulsar_admin(container, &["scalable-topics", "delete", topic, "--force"]).await
        {
            failures.push(format!("{topic}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "failed to force-delete hardened scalable topics: {}",
            failures.join(" | ")
        )
        .into())
    }
}

async fn verify_scalable_topology(
    client: &PulsarClient,
    topic: &str,
    service_url: &str,
    expected_segments: usize,
) -> TestResult {
    let lookup = client.lookup_scalable_topic(topic).await?;
    let result = (|| {
        if lookup.resolved_topic_name.as_deref() != Some(topic) {
            return Err(format!(
                "GA broker resolved `{topic}` as {:?}",
                lookup.resolved_topic_name
            )
            .into());
        }
        if lookup
            .controller_broker_url
            .as_deref()
            .is_some_and(|controller_url| controller_url != service_url)
        {
            return Err(format!(
                "published controller authority must be the reachable broker-authored direct URL \
                 `{service_url}`, got {:?}",
                lookup.controller_broker_url
            )
            .into());
        }
        if lookup.segments.len() != expected_segments {
            return Err(format!(
                "{expected_segments}-segment topic resolved {} segments",
                lookup.segments.len()
            )
            .into());
        }
        for segment in &lookup.segments {
            if segment.broker_url.as_deref() != Some(service_url) {
                return Err(format!(
                    "segment {} authority must be the broker-authored direct URL `{service_url}`, \
                     got {:?}",
                    segment.segment_id.0, segment.broker_url
                )
                .into());
            }
        }
        Ok(())
    })();
    client.close_scalable_topic_session(lookup.session_id);
    result
}

async fn exercise_delivery_and_close_cleanup(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("delivery", suffix);
    let subscription = format!("e2e-delivery-{suffix}");
    create_scalable_topic(container, &topic, 4).await?;
    created_topics.push(topic.clone());
    verify_scalable_topology(client, &topic, service_url, 4).await?;

    let consumer = subscribe_byte_stream(
        client,
        &topic,
        &subscription,
        &format!("e2e-delivery-consumer-{suffix}"),
        OrderingMode::Strict,
    )
    .await?;
    wait_for_status(&consumer, "four attached delivery segments", |status| {
        status.assigned_segments() == 4
            && status.attached_segments() == 4
            && status.receiver_budget_limit() == STREAM_BUDGET_BYTES
    })
    .await?;
    wait_for_consumer_count(container, &topic, &subscription, 1).await?;

    let expected = payloads(&format!("delivery-{suffix}"), 12);
    publish_scalable_messages(container, service_url, &topic, &expected).await?;
    let messages = receive_expected_messages(&consumer, &expected, "typed delivery phase").await?;
    let sources: BTreeSet<SegmentSource> = messages
        .iter()
        .map(|message| message.source().clone())
        .collect();
    if sources.len() < 2
        || sources
            .iter()
            .any(|source| !source.topic().starts_with("segment://"))
    {
        return Err(format!(
            "official V5 publisher must drive multiple canonical segment sources, got {sources:?}"
        )
        .into());
    }
    if consumer.status().receiver_budget_used() == 0 {
        return Err("unresolved typed deliveries consumed no aggregate budget".into());
    }
    consumer.acknowledge(&messages[0]).await?;
    let position = consumer.delivered_position();
    if position.len() != sources.len() {
        return Err(format!(
            "delivered position has {} components for {} observed sources",
            position.len(),
            sources.len()
        )
        .into());
    }
    let restored = PositionVector::from_bytes(&position.to_bytes()?)?;
    consumer.acknowledge_positions(&restored).await?;
    for (index, message) in messages.iter().enumerate() {
        match consumer.acknowledge(message).await {
            Err(StreamConsumerError::Model(
                magnetar::proto::StreamConsumerModelError::StaleDeliveryToken,
            )) => {}
            Ok(()) => {
                return Err(format!(
                    "delivery {index} remained acknowledgeable after restored-vector acknowledgement"
                )
                .into());
            }
            Err(error) => {
                return Err(format!(
                    "delivery {index} returned {error} instead of stale authority"
                )
                .into());
            }
        }
    }
    consumer.close().await?;

    // Pulsar 5 GA clean leave removes the controller member while the pooled
    // transport stays alive, matching the Java V5 consumer.
    wait_for_consumer_count(container, &topic, &subscription, 0).await
}

async fn exercise_vector_seek(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("vector-seek", suffix);
    let subscription = format!("e2e-vector-seek-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    created_topics.push(topic.clone());

    let consumer = subscribe_byte_stream(
        client,
        &topic,
        &subscription,
        &format!("e2e-vector-seek-consumer-{suffix}"),
        OrderingMode::Strict,
    )
    .await?;
    let idle_budget_used = wait_for_attached_flow(
        &consumer,
        "one attached vector-seek segment with FLOW authority",
        1,
    )
    .await?;

    let expected = payloads(&format!("vector-seek-{suffix}"), 5);
    publish_scalable_messages(container, service_url, &topic, &expected).await?;
    let initial =
        receive_expected_messages(&consumer, &expected, "pre-seek delivery phase").await?;
    let source = initial
        .first()
        .ok_or("vector-seek fixture delivered no messages")?
        .source()
        .clone();
    if initial.iter().any(|message| message.source() != &source) {
        return Err("one-segment vector-seek fixture delivered from multiple sources".into());
    }
    assert_stream_payloads(
        &initial,
        &expected,
        "one-segment delivery must preserve FIFO before seek",
    )?;

    let target = &initial[2];
    let target_id = target.message_id().ordinary_message_id();
    if target_id.batch_index != -1 {
        return Err(
            format!("vector-seek fixture expected an ordinary id, got {target_id:?}").into(),
        );
    }
    let seek_position = PositionVector::new(
        target.position().layout_epoch(),
        [(source.clone(), target_id)],
    )?;
    consumer.acknowledge_batch(&initial).await?;
    drop(initial);
    wait_for_settled_delivery_leases(
        &consumer,
        "settled pre-seek delivery leases",
        idle_budget_used,
    )
    .await?;

    consumer.seek_positions(&seek_position).await?;
    let replay_expected = &expected[2..];
    let replayed =
        receive_expected_messages(&consumer, replay_expected, "post-seek replay phase").await?;
    if replayed.iter().any(|message| message.source() != &source) {
        return Err("vector seek replay crossed its sole source".into());
    }
    assert_stream_payloads(
        &replayed,
        replay_expected,
        "vector seek must replay the inclusive source-local suffix",
    )?;

    consumer.acknowledge(&replayed[0]).await?;
    consumer
        .acknowledge_cumulative(
            replayed
                .last()
                .ok_or("vector seek replay unexpectedly returned no messages")?,
        )
        .await?;
    drop(replayed);
    wait_for_settled_delivery_leases(
        &consumer,
        "settled post-seek delivery leases",
        idle_budget_used,
    )
    .await?;
    match tokio::time::timeout(NO_DELIVERY_WINDOW, consumer.receive()).await {
        Err(_) => {}
        Ok(Ok(message)) => {
            return Err(format!(
                "vector seek replay produced an unexpected straggler {:?}",
                message.value()
            )
            .into());
        }
        Ok(Err(error)) => {
            return Err(format!("vector seek aggregate failed after replay: {error}").into());
        }
    }
    consumer.close().await?;
    Ok(())
}

async fn assert_committed_sources_drained(
    client: &PulsarClient,
    sources: &[SegmentSource],
    subscription: &str,
    suffix: &str,
) -> TestResult {
    for (index, source) in sources.iter().enumerate() {
        let direct = client
            .consumer(source.topic())
            .subscription(subscription)
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .name(format!("e2e-commit-proof-{suffix}-{index}"))
            .subscribe()
            .await?;
        let observation = tokio::time::timeout(NO_DELIVERY_WINDOW, direct.receive()).await;
        let unexpected = match observation {
            Err(_) => None,
            Ok(Ok(message)) => Some(format!(
                "committed cursor on {} redelivered payload {:?}",
                source.topic(),
                message.payload
            )),
            Ok(Err(error)) => Some(format!(
                "commit cursor proof consumer on {} failed: {error}",
                source.topic()
            )),
        };
        direct.close().await?;
        if let Some(unexpected) = unexpected {
            return Err(unexpected.into());
        }
    }
    Ok(())
}

async fn exercise_transaction_commit(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("txn-commit", suffix);
    let subscription = format!("e2e-txn-commit-{suffix}");
    create_scalable_topic(container, &topic, 3).await?;
    created_topics.push(topic.clone());
    let consumer = subscribe_byte_stream(
        client,
        &topic,
        &subscription,
        &format!("e2e-txn-commit-consumer-{suffix}"),
        OrderingMode::Strict,
    )
    .await?;
    let idle_budget_used = wait_for_attached_flow(
        &consumer,
        "three attached commit segments with FLOW authority",
        3,
    )
    .await?;

    let expected = payloads(&format!("txn-commit-{suffix}"), 9);
    publish_scalable_messages(container, service_url, &topic, &expected).await?;
    let messages =
        receive_expected_messages(&consumer, &expected, "transaction commit phase").await?;
    let position = consumer.delivered_position();
    if position.len() < 2 {
        return Err(format!(
            "transaction commit vector must span multiple segments, got {position:?}"
        )
        .into());
    }
    let sources: Vec<SegmentSource> = position.iter().map(|(source, _)| source.clone()).collect();
    let transaction = client.new_transaction(TXN_TIMEOUT).await?;
    consumer
        .acknowledge_positions_in_transaction(&position, transaction)
        .await?;
    if consumer.status().receiver_budget_used() == 0 {
        return Err("transactional admission resolved deliveries before commit".into());
    }
    let state = client.commit_transaction(transaction).await?;
    if state != magnetar::TxnState::Committed {
        return Err(
            format!("broker returned {state:?} for committed aggregate transaction").into(),
        );
    }
    wait_for_transaction_outcome(&consumer, transaction, TransactionOutcome::Committed).await?;
    wait_for_settled_delivery_leases(
        &consumer,
        "committed transactional leases resolved",
        idle_budget_used,
    )
    .await?;
    drop(messages);
    consumer.close().await?;
    assert_committed_sources_drained(client, &sources, &subscription, suffix).await
}

async fn exercise_transaction_abort(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("txn-abort", suffix);
    let subscription = format!("e2e-txn-abort-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    created_topics.push(topic.clone());
    let consumer = subscribe_byte_stream(
        client,
        &topic,
        &subscription,
        &format!("e2e-txn-abort-consumer-{suffix}"),
        OrderingMode::Strict,
    )
    .await?;
    wait_for_status(&consumer, "one attached abort segment", |status| {
        status.assigned_segments() == 1 && status.attached_segments() == 1
    })
    .await?;

    let expected = payloads(&format!("txn-abort-{suffix}"), 1);
    publish_scalable_messages(container, service_url, &topic, &expected).await?;
    let mut messages =
        receive_expected_messages(&consumer, &expected, "transaction abort phase").await?;
    let message = messages.pop().ok_or("abort message missing")?;
    let source = message.source().clone();
    let position = consumer.delivered_position();
    let transaction = client.new_transaction(TXN_TIMEOUT).await?;
    consumer
        .acknowledge_positions_in_transaction(&position, transaction)
        .await?;
    let state = client.abort_transaction(transaction).await?;
    if state != magnetar::TxnState::Aborted {
        return Err(format!("broker returned {state:?} for aborted aggregate transaction").into());
    }
    wait_for_transaction_outcome(&consumer, transaction, TransactionOutcome::Aborted).await?;
    if consumer.status().receiver_budget_used() == 0 {
        return Err("aborted transactional acknowledgement resolved its delivery lease".into());
    }
    drop(message);
    consumer.close().await?;

    let replay = client
        .consumer(source.topic())
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .name(format!("e2e-abort-replay-{suffix}"))
        .subscribe()
        .await?;
    let replayed = tokio::time::timeout(RECEIVE_TIMEOUT, replay.receive())
        .await
        .map_err(|_| "aborted aggregate transaction did not redeliver after child close")??;
    if replayed.payload.as_ref() != expected[0].as_bytes() {
        return Err(format!(
            "aborted transaction redelivered {:?}, expected `{}`",
            replayed.payload, expected[0]
        )
        .into());
    }
    replay.ack(replayed.message_id).await?;
    replay.close().await?;
    Ok(())
}

async fn split_scalable_segment(
    container: &PulsarContainer,
    topic: &str,
    segment_id: u64,
) -> TestResult {
    let segment_id = segment_id.to_string();
    pulsar_admin(
        container,
        &[
            "scalable-topics",
            "split-segment",
            topic,
            "--segment-id",
            &segment_id,
        ],
    )
    .await?;
    Ok(())
}

async fn initial_segment(client: &PulsarClient, topic: &str) -> TestResult<(u64, u64)> {
    let lookup = client.lookup_scalable_topic(topic).await?;
    let result = lookup
        .segments
        .first()
        .map(|segment| (lookup.epoch, segment.segment_id.0))
        .ok_or_else(|| format!("scalable topic `{topic}` has no initial segment").into());
    client.close_scalable_topic_session(lookup.session_id);
    result
}

async fn exercise_strict_split(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("strict-split", suffix);
    let subscription = format!("e2e-strict-split-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    created_topics.push(topic.clone());
    let (initial_epoch, parent_id) = initial_segment(client, &topic).await?;
    let consumer = subscribe_byte_stream(
        client,
        &topic,
        &subscription,
        &format!("e2e-strict-consumer-{suffix}"),
        OrderingMode::Strict,
    )
    .await?;
    wait_for_status(&consumer, "one attached strict parent", |status| {
        status.assigned_segments() == 1 && status.attached_segments() == 1
    })
    .await?;

    let parent_payloads = payloads(&format!("strict-parent-{suffix}"), 1);
    publish_scalable_messages(container, service_url, &topic, &parent_payloads).await?;
    let mut parents =
        receive_expected_messages(&consumer, &parent_payloads, "strict parent phase").await?;
    let parent = parents.pop().ok_or("strict parent message missing")?;
    if parent.source().segment_id().0 != parent_id {
        return Err("strict parent delivery came from the wrong segment".into());
    }
    split_scalable_segment(container, &topic, parent_id).await?;
    let child_payloads = payloads(&format!("strict-child-{suffix}"), 6);
    publish_scalable_messages(container, service_url, &topic, &child_payloads).await?;
    let blocked = wait_for_status(&consumer, "strict split DAG with parent only", |status| {
        status
            .layout_epoch()
            .is_some_and(|epoch| epoch > initial_epoch)
            && status.assigned_segments() == 1
    })
    .await?;
    if !blocked.ordering_unprovable().is_empty() {
        return Err(format!(
            "single-member strict ancestry became unprovable: {:?}",
            blocked.ordering_unprovable()
        )
        .into());
    }
    match tokio::time::timeout(NO_DELIVERY_WINDOW, consumer.receive()).await {
        Err(_) => {}
        Ok(Ok(message)) => {
            return Err(format!(
                "strict descendant {} surfaced before parent acknowledgement",
                message.source().topic()
            )
            .into());
        }
        Ok(Err(error)) => {
            return Err(format!(
                "strict aggregate failed while its parent barrier was held: {error}"
            )
            .into());
        }
    }
    consumer.acknowledge(&parent).await?;
    drop(parent);
    let child_ids =
        wait_for_drained_parent_assignment(client, &consumer, &topic, parent_id, initial_epoch)
            .await?;
    let children =
        receive_expected_messages(&consumer, &child_payloads, "strict descendant phase").await?;
    if children
        .iter()
        .any(|message| !child_ids.contains(&message.source().segment_id().0))
    {
        return Err(
            "strict child payload was delivered outside the exact successor assignment".into(),
        );
    }
    consumer.acknowledge_batch(&children).await?;
    consumer.close().await?;
    Ok(())
}

async fn wait_for_drained_parent_assignment(
    client: &PulsarClient,
    consumer: &ByteStreamConsumer,
    topic: &str,
    parent_id: u64,
    initial_epoch: u64,
) -> TestResult<BTreeSet<u64>> {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    let layout = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        client.lookup_scalable_topic(topic),
    )
    .await??;
    client.close_scalable_topic_session(layout.session_id);
    let child_ids: BTreeSet<u64> = layout
        .segments
        .iter()
        .filter(|segment| segment.parent_ids.iter().any(|id| id.0 == parent_id))
        .map(|segment| segment.segment_id.0)
        .collect();
    if child_ids.len() != 2 {
        return Err(format!(
            "split parent {parent_id} must have exactly two successors: {child_ids:?}"
        )
        .into());
    }
    // GA retires a sealed parent once drained, then assigns its exact successors.
    let mut assigned_successors = false;
    loop {
        let status = consumer.status();
        if assigned_successors
            && status.phase() == magnetar::proto::AggregatePhase::Open
            && status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
            && status.assigned_segments() == 2
            && status.attached_segments() == 2
            && status.draining_segments() == 0
            && status.pending_ownership().is_empty()
            && status.ordering_unprovable().is_empty()
        {
            return Ok(child_ids);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "did not reach exact attached successor assignment {child_ids:?} within \
                 {STATUS_TIMEOUT:?}: observed assignment={assigned_successors}, status={status:?}"
            )
            .into());
        }
        match tokio::time::timeout(
            remaining.min(Duration::from_millis(100)),
            consumer.next_event(),
        )
        .await
        {
            Ok(Ok(Some(StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            }))) if layout_epoch > initial_epoch
                && sources
                    .iter()
                    .map(|source| source.segment_id().0)
                    .collect::<BTreeSet<_>>()
                    == child_ids =>
            {
                assigned_successors = true;
            }
            Ok(Ok(Some(_))) | Err(_) => {}
            Ok(Ok(None)) => {
                return Err("strict consumer closed before attached successor assignment".into());
            }
            Ok(Err(error)) => return Err(error.into()),
        }
    }
}

async fn acknowledge_broker_managed_children(
    consumers: &[ByteStreamConsumer],
    parent_id: u64,
    expected: &[String],
    first_owner: usize,
    first: ByteStreamMessage,
) -> TestResult {
    let mut remaining: BTreeSet<String> = expected.iter().cloned().collect();
    let first_value = std::str::from_utf8(first.value().as_ref())?.to_owned();
    if !remaining.remove(&first_value) {
        return Err(format!(
            "broker-managed non-parent member received unexpected payload `{first_value}`"
        )
        .into());
    }
    consumers[first_owner].acknowledge(&first).await?;
    while !remaining.is_empty() {
        let (owner, message) = receive_from_any(
            consumers,
            &[0, 1, 2],
            "remaining broker-managed child delivery",
        )
        .await?;
        if message.source().segment_id().0 == parent_id {
            return Err("broker-managed child payload arrived from sealed parent".into());
        }
        let value = std::str::from_utf8(message.value().as_ref())?.to_owned();
        if !remaining.remove(&value) {
            return Err(format!(
                "broker-managed phase received unexpected or duplicate payload `{value}`"
            )
            .into());
        }
        consumers[owner].acknowledge(&message).await?;
    }
    Ok(())
}

async fn exercise_broker_managed_split(
    client: &PulsarClient,
    container: &PulsarContainer,
    service_url: &str,
    suffix: &str,
    created_topics: &mut Vec<String>,
) -> TestResult {
    let topic = unique_scalable_topic("broker-managed", suffix);
    let subscription = format!("e2e-broker-managed-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    created_topics.push(topic.clone());
    let (initial_epoch, parent_id) = initial_segment(client, &topic).await?;

    let mut consumers = Vec::with_capacity(3);
    for member in ['a', 'b', 'c'] {
        consumers.push(
            subscribe_byte_stream(
                client,
                &topic,
                &subscription,
                &format!("e2e-broker-managed-{member}-{suffix}"),
                OrderingMode::BrokerManaged,
            )
            .await?,
        );
    }
    // PIP-486 fans this one segment out by entry bucket before any split.
    for consumer in &consumers {
        wait_for_status_with_events(consumer, "broker-managed shared parent owner", |status| {
            status.assigned_segments() == 1 && status.attached_segments() == 1
        })
        .await?;
    }

    let parent_payloads = payloads(&format!("broker-parent-{suffix}"), 1);
    publish_scalable_messages(container, service_url, &topic, &parent_payloads).await?;
    let (parent_owner, parent) =
        receive_from_any(&consumers, &[0, 1, 2], "broker-managed parent phase").await?;
    if parent.payload() != parent_payloads[0].as_bytes() {
        return Err("broker-managed parent payload differs from the published message".into());
    }
    if parent.source().segment_id().0 != parent_id {
        return Err("broker-managed parent delivery came from the wrong segment".into());
    }
    split_scalable_segment(container, &topic, parent_id).await?;
    let child_payloads = payloads(&format!("broker-child-{suffix}"), 6);
    publish_scalable_messages(container, service_url, &topic, &child_payloads).await?;
    for consumer in &consumers {
        wait_for_status(consumer, "split visible before broker drain", |status| {
            status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
                && status.assigned_segments() == 1
        })
        .await?;
    }
    consumers[parent_owner]
        .acknowledge_cumulative(&parent)
        .await?;
    drop(parent);
    for consumer in &consumers {
        wait_for_status_with_events(consumer, "balanced post-drain split assignment", |status| {
            status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
                && status.assigned_segments() == 1
        })
        .await?;
    }

    let remote_owners: Vec<usize> = (0..consumers.len())
        .filter(|owner| *owner != parent_owner)
        .collect();
    let (remote_owner, remote_child) = receive_from_any(
        &consumers,
        &remote_owners,
        "broker-managed member without parent delivery history",
    )
    .await?;
    if remote_child.source().segment_id().0 == parent_id {
        return Err(format!(
            "non-parent member {remote_owner} received from the sealed parent instead of a child"
        )
        .into());
    }
    if !consumers[remote_owner]
        .status()
        .ordering_unprovable()
        .is_empty()
    {
        return Err("BrokerManaged member entered strict OrderingUnprovable state".into());
    }
    acknowledge_broker_managed_children(
        &consumers,
        parent_id,
        &child_payloads,
        remote_owner,
        remote_child,
    )
    .await?;
    for consumer in consumers {
        consumer.close().await?;
    }
    Ok(())
}

/// (1) Public hardened aggregate contract against one real GA broker.
///
/// The phases are sequential and UUID-isolated so this adds no concurrent broker
/// beyond the lightweight lookup test it replaces. Standalone cannot prove a
/// same-cluster multi-broker route. Controller and segment authorities must
/// remain reachable through the broker-advertised standalone transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_hardened_scalable_stream_consumer_contract() -> TestResult {
    let (service_url, container, _proxy) = start_hardened_broker().await?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;
    if !client.broker_supports_scalable_topics() {
        client.close().await;
        return Err(
            "GA broker with scalableTopicsEnabled=true did not advertise scalable-topic support"
                .into(),
        );
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let mut created_topics = Vec::new();
    let exercise: TestResult = async {
        exercise_delivery_and_close_cleanup(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        exercise_vector_seek(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        exercise_transaction_commit(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        exercise_transaction_abort(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        exercise_strict_split(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        exercise_broker_managed_split(
            &client,
            &container,
            &service_url,
            &suffix,
            &mut created_topics,
        )
        .await?;

        Ok(())
    }
    .await;

    // Release every pooled socket, then remove only the UUID-isolated topics
    // this test created, including when an earlier assertion failed.
    client.close().await;
    let cleanup = force_delete_scalable_topics(&container, &created_topics).await;
    match (exercise, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(format!(
            "hardened scalable contract failed: {error}; cleanup also failed: {cleanup_error}"
        )
        .into()),
    }
}

#[cfg(feature = "moonpool")]
async fn connect_moonpool(
    service_url: &str,
) -> TestResult<PulsarClient<magnetar::MoonpoolEngine<moonpool_core::TokioProviders>>> {
    use magnetar::runtime_moonpool::{Client, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let host_port = service_url
        .strip_prefix("pulsar://")
        .ok_or("broker URL lacks scheme")?;
    let runtime_client = Client::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        host_port,
        magnetar::proto::ConnectionConfig {
            operation_timeout: STATUS_TIMEOUT,
            ..Default::default()
        },
        None,
        None,
    )
    .await?;
    Ok(PulsarClient::from_runtime_client(runtime_client))
}

/// Clean leave and same-name rejoin match Java V5EntryBucketDispatchTest's
/// controller cleanup contract without closing the pooled client transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_stream_close_unregisters_with_a_live_pooled_connection() -> TestResult {
    let (service_url, container, _proxy) = start_hardened_broker().await?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;
    let result = exercise_close_contract(&client, &container).await;
    client.close().await;
    result
}

#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_moonpool_transaction_bootstraps_a_fresh_coordinator() -> TestResult {
    let (service_url, _container, _proxy) = start_hardened_broker().await?;
    let client = connect_moonpool(&service_url).await?;
    let result = async {
        for _ in 0..2 {
            let transaction = client.new_transaction(TXN_TIMEOUT).await?;
            let state = client.abort_transaction(transaction).await?;
            if state != magnetar::TxnState::Aborted {
                return Err(format!("fresh coordinator returned {state:?} on abort").into());
            }
        }
        Ok(())
    }
    .await;
    client.close().await;
    result
}

#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_stream_close_unregisters_with_a_live_pooled_connection_moonpool() -> TestResult
{
    let (service_url, container, _proxy) = start_hardened_broker().await?;
    let client = connect_moonpool(&service_url).await?;
    let result = exercise_close_contract(&client, &container).await;
    client.close().await;
    result
}

async fn exercise_close_contract<E: magnetar::Engine>(
    client: &PulsarClient<E>,
    container: &PulsarContainer,
) -> TestResult
where
    E::ClientState: magnetar::SegmentSubscriberApi,
{
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = unique_scalable_topic("clean-leave", &suffix);
    let subscription = format!("clean-leave-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    let exercise: TestResult = async {
        for _ in 0..3 {
            let consumer = subscribe_byte_stream(
                client,
                &topic,
                &subscription,
                &format!("same-name-{suffix}"),
                OrderingMode::BrokerManaged,
            )
            .await?;
            wait_for_consumer_count(container, &topic, &subscription, 1).await?;
            consumer.close().await?;
            wait_for_consumer_count(container, &topic, &subscription, 0).await?;
        }
        Ok(())
    }
    .await;
    let cleanup = force_delete_scalable_topics(container, &[topic]).await;
    match (exercise, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("clean leave failed: {error}; cleanup also failed: {cleanup_error}").into())
        }
    }
}

/// Java GA V5EntryBucketDispatchTest's one-segment/two-consumer contract:
/// whole keys retain one owner and FIFO order; cumulative acknowledgements
/// translate to individual Key_Shared acknowledgements, including transactions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_scalable_bucket_owners_preserve_keys_and_transactional_acknowledgements() -> TestResult
{
    let (service_url, container, _proxy) = start_hardened_broker().await?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;
    let result = exercise_bucket_contract(&client, &container, &service_url).await;
    client.close().await;
    result
}

#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_scalable_bucket_owners_preserve_keys_and_transactional_acknowledgements_moonpool()
-> TestResult {
    let (service_url, container, _proxy) = start_hardened_broker().await?;
    let client = connect_moonpool(&service_url).await?;
    let result = exercise_bucket_contract(&client, &container, &service_url).await;
    client.close().await;
    result
}

async fn exercise_bucket_transaction<E: magnetar::Engine>(
    client: &PulsarClient<E>,
    consumers: &[ByteStreamConsumer<E>],
    last_messages: &BTreeMap<usize, ByteStreamMessage>,
    phase: &str,
) -> TestResult
where
    E::ClientState: magnetar::SegmentSubscriberApi + magnetar::TransactionApi,
{
    let transaction = client.new_transaction(TXN_TIMEOUT).await?;
    for (owner, message) in last_messages {
        consumers[*owner]
            .acknowledge_cumulative_in_transaction(message, transaction)
            .await?;
    }
    let expected_outcome = if phase == "commit" {
        if client.commit_transaction(transaction).await? != magnetar::TxnState::Committed {
            return Err("bucket transaction did not commit".into());
        }
        TransactionOutcome::Committed
    } else {
        if client.abort_transaction(transaction).await? != magnetar::TxnState::Aborted {
            return Err("bucket transaction did not abort".into());
        }
        TransactionOutcome::Aborted
    };
    for consumer in consumers {
        wait_for_transaction_outcome(consumer, transaction, expected_outcome).await?;
    }
    Ok(())
}

async fn exercise_bucket_phase<E: magnetar::Engine>(
    client: &PulsarClient<E>,
    container: &PulsarContainer,
    service_url: &str,
    topic: &str,
    consumers: &[ByteStreamConsumer<E>],
    key_owners: &mut BTreeMap<String, usize>,
    phase: &str,
) -> TestResult<Vec<String>>
where
    E::ClientState: magnetar::SegmentSubscriberApi + magnetar::TransactionApi,
{
    let mut abort_payloads = Vec::new();
    // The Java reference uses sixteen keys and twenty messages per key.
    // Each CLI call fixes its key and publishes in send order.
    let per_key = if phase == "plain" { 20 } else { 1 };
    let mut expected = BTreeMap::new();
    for key_index in 0..16 {
        let key = format!("key-{key_index}");
        let values: Vec<String> = (0..per_key)
            .map(|sequence| format!("{phase}-{key}:{sequence}"))
            .collect();
        publish_scalable_messages_with_key(container, service_url, topic, &values, Some(&key))
            .await?;
        if phase == "abort" {
            abort_payloads.extend(values.iter().cloned());
        }
        expected.insert(key, values);
    }
    let mut actual: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut last_messages: BTreeMap<usize, ByteStreamMessage> = BTreeMap::new();
    let mut owner_counts = [0usize; 2];
    for _ in 0..16 * per_key {
        let (owner, message) = receive_from_any(consumers, &[0, 1], phase).await?;
        let value = std::str::from_utf8(message.value().as_ref())?.to_owned();
        let (key_with_phase, _) = value
            .rsplit_once(':')
            .ok_or("keyed payload lacks sequence")?;
        let key = key_with_phase
            .strip_prefix(&format!("{phase}-"))
            .ok_or("keyed payload belongs to another phase")?
            .to_owned();
        if !expected.contains_key(&key) {
            return Err(format!("unexpected key `{key}`").into());
        }
        if let Some(previous) = key_owners.insert(key.clone(), owner) {
            if previous != owner {
                return Err(format!("key `{key}` changed bucket owner").into());
            }
        }
        owner_counts[owner] += 1;
        actual.entry(key).or_default().push(value);
        if phase == "plain" {
            consumers[owner].acknowledge_cumulative(&message).await?;
            // A successful repeated cumulative ack is idempotent in Java.
            consumers[owner].acknowledge_cumulative(&message).await?;
        } else {
            last_messages.insert(owner, message);
        }
    }
    if owner_counts.contains(&0) {
        return Err(format!("both bucket owners must receive traffic: {owner_counts:?}").into());
    }
    if actual != expected {
        return Err(format!("per-key order/content mismatch in {phase}: {actual:?}").into());
    }
    if phase != "plain" {
        exercise_bucket_transaction(client, consumers, &last_messages, phase).await?;
    }
    Ok(abort_payloads)
}

async fn exercise_bucket_contract<E: magnetar::Engine>(
    client: &PulsarClient<E>,
    container: &PulsarContainer,
    service_url: &str,
) -> TestResult
where
    E::ClientState: magnetar::SegmentSubscriberApi + magnetar::TransactionApi,
{
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let topic = unique_scalable_topic("bucket-share", &suffix);
    let subscription = format!("bucket-share-{suffix}");
    create_scalable_topic(container, &topic, 1).await?;
    let exercise: TestResult = async {
        let mut consumers = Vec::with_capacity(2);
        for owner in ['a', 'b'] {
            consumers.push(
                subscribe_byte_stream(
                    client,
                    &topic,
                    &subscription,
                    &format!("bucket-{owner}-{suffix}"),
                    OrderingMode::BrokerManaged,
                )
                .await?,
            );
        }
        for consumer in &consumers {
            wait_for_status_with_events(consumer, "shared entry-bucket owner attached", |status| {
                status.assigned_segments() == 1 && status.attached_segments() == 1
            })
            .await?;
        }
        wait_for_consumer_count(container, &topic, &subscription, 2).await?;
        let mut key_owners = BTreeMap::new();
        let mut abort_payloads = Vec::new();
        for phase in ["plain", "commit", "abort"] {
            abort_payloads.extend(
                exercise_bucket_phase(
                    client,
                    container,
                    service_url,
                    &topic,
                    &consumers,
                    &mut key_owners,
                    phase,
                )
                .await?,
            );
        }
        for consumer in consumers {
            consumer.close().await?;
        }
        wait_for_consumer_count(container, &topic, &subscription, 0).await?;
        let replay = subscribe_byte_stream(
            client,
            &topic,
            &subscription,
            &format!("bucket-replay-{suffix}"),
            OrderingMode::BrokerManaged,
        )
        .await?;
        let replayed = receive_expected_messages(
            &replay,
            &abort_payloads,
            "aborted bucket transaction replay",
        )
        .await?;
        for message in &replayed {
            replay.acknowledge_cumulative(message).await?;
        }
        match tokio::time::timeout(NO_DELIVERY_WINDOW, replay.receive()).await {
            Err(_) => {}
            Ok(Ok(message)) => {
                return Err(
                    format!("acked bucket payload redelivered: {:?}", message.payload()).into(),
                );
            }
            Ok(Err(error)) => return Err(error.into()),
        }
        replay.close().await?;
        wait_for_consumer_count(container, &topic, &subscription, 0).await
    }
    .await;
    let cleanup = force_delete_scalable_topics(container, &[topic]).await;
    match (exercise, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(format!(
            "bucket contract failed: {error}; cleanup also failed: {cleanup_error}"
        )
        .into()),
    }
}

/// (2) `topic-info` CLI round-trip against a real PIP-460 broker.
///
/// The CLI and the driver resolve the same topic through the same code path, so
/// the assertion is that the printed view matches what the library reports —
/// the two cannot drift without this failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_info_cli_round_trip() {
    let (service_url, _admin_url, container) = start_broker(Broker::Scalable)
        .await
        .expect("pulsar 5.0 broker starts");

    let topic = format!("topic://{TENANT_NS}/e2e-scaled-cli");
    create_scalable_topic(&container, &topic, 3)
        .await
        .expect("scalable topic created");

    // What the library sees.
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await
        .expect("client connects");
    let lookup = client
        .lookup_scalable_topic(&topic)
        .await
        .expect("scalable lookup resolves");
    client.close_scalable_topic_session(lookup.session_id);

    // What the CLI prints.
    let output = std::process::Command::new(magnetarctl_binary())
        .args(["--service-url", &service_url, "topic-info", &topic])
        .output()
        .expect("magnetarctl runs");
    assert!(
        output.status.success(),
        "magnetarctl topic-info failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed = String::from_utf8_lossy(&output.stdout).into_owned();

    assert!(
        printed.contains(&format!("layout-epoch: {}", lookup.epoch)),
        "the CLI prints the epoch the library resolved.\n--- printed ---\n{printed}"
    );
    assert!(
        printed.contains(&format!("({} segment(s))", lookup.segments.len())),
        "the CLI prints the segment count the library resolved.\n--- printed ---\n{printed}"
    );
    for seg in &lookup.segments {
        let state = format!("{:?}", seg.state);
        let expected_row = format!(
            "{:<10} [{:>5},{:>5}] {:<10} {}",
            seg.segment_id.0,
            seg.key_range.start(),
            seg.key_range.end(),
            state,
            seg.broker_url.as_deref().unwrap_or("-"),
        );
        assert!(
            printed.lines().any(|line| line == expected_row),
            "exact inclusive row for segment {} is missing from the CLI output.\n\
             expected: {expected_row}\n--- printed ---\n{printed}",
            seg.segment_id.0,
        );
    }
}

/// HTTP status of the broker's admin REST for the scalable topic `name` of [`TENANT_NS`]:
/// `GET /admin/v2/scalable/{tenant}/{namespace}/{name}` answers 200 with the topic's metadata
/// when it exists and 404 with `Scalable topic not found` when it does not (Pulsar 5.0.0).
async fn scalable_topic_admin_status(admin_url: &str, name: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("{admin_url}/admin/v2/scalable/{TENANT_NS}/{name}"))
        .send()
        .await
        .expect("admin REST answers")
        .status()
}

/// (5) A read-only lookup never creates the topic it looks up.
///
/// A Pulsar 5.0.0 broker auto-creates a scalable topic for a lookup that leaves
/// `create_if_missing` unset (it defaults to `true`), where 5.0.0-M1 failed with "Scalable topic
/// not found". `PulsarClient::lookup_scalable_topic` sends `false` explicitly, so on a
/// never-created topic it must report "not found" and the topic must still not exist afterwards;
/// `magnetarctl topic-info` relies on exactly that. The broker's admin REST is the oracle, with a
/// positive control proving the endpoint really distinguishes the two states.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_read_only_lookup_does_not_create_the_topic() {
    let (service_url, admin_url, container) = start_broker(Broker::Scalable)
        .await
        .expect("pulsar 5.0 broker starts");

    // Positive control: the admin endpoint answers 200 for a topic that exists.
    create_scalable_topic(
        &container,
        &format!("topic://{TENANT_NS}/e2e-scaled-control"),
        1,
    )
    .await
    .expect("control topic created");
    assert_eq!(
        scalable_topic_admin_status(&admin_url, "e2e-scaled-control").await,
        reqwest::StatusCode::OK,
        "the admin endpoint reports an existing scalable topic"
    );

    let name = "e2e-scaled-never-created";
    assert_eq!(
        scalable_topic_admin_status(&admin_url, name).await,
        reqwest::StatusCode::NOT_FOUND,
        "the topic does not exist before the lookup"
    );

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await
        .expect("client connects");
    let err = client
        .lookup_scalable_topic(&format!("topic://{TENANT_NS}/{name}"))
        .await
        .expect_err("a read-only lookup of a never-created topic fails");
    let rendered = err.to_string();
    assert!(
        rendered.to_lowercase().contains("not found"),
        "the lookup reports the topic as not found, got `{rendered}`"
    );

    assert_eq!(
        scalable_topic_admin_status(&admin_url, name).await,
        reqwest::StatusCode::NOT_FOUND,
        "a read-only lookup must not auto-create the topic (create_if_missing = false); the lookup \
         failed with `{rendered}`"
    );
}

/// (3) Drop-on-DAG-change observed against a broker-driven segment split.
///
/// PIP-483 auto-scale is disabled on this broker, so the only layout change is
/// the one `pulsar-admin scalable-topics split-segment` triggers — the
/// assertion is about the client's reaction, not about a timing window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_drops_on_broker_split() {
    let (service_url, _admin_url, container) = start_broker(Broker::Scalable)
        .await
        .expect("pulsar 5.0 broker starts");

    let topic = format!("topic://{TENANT_NS}/e2e-scaled-split");
    create_scalable_topic(&container, &topic, 1)
        .await
        .expect("scalable topic created");

    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await
        .expect("client connects");

    // This test deliberately drives the raw layout session: its contract is the
    // low-level `DagChangedDuringConsume` classification, while the public
    // aggregate's split behavior is covered by the hardened test above.
    let lookup = client
        .lookup_scalable_topic(&topic)
        .await
        .expect("scalable lookup resolves");
    let epoch_before = lookup.epoch;
    let segment_to_split = lookup
        .segments
        .first()
        .expect("the initial layout has one segment")
        .segment_id
        .0;

    // Trigger the split on the broker.
    // `--segment-id` is an option, not a positional: `SplitSegmentCmd` in
    // upstream's `CmdScalableTopics.java` declares only the topic positionally.
    // The exit code is the check; the split's real proof is the layout change
    // the loop below drains for.
    pulsar_admin(
        &container,
        &[
            "scalable-topics",
            "split-segment",
            &topic,
            "--segment-id",
            &segment_to_split.to_string(),
        ],
    )
    .await
    .expect("split-segment runs");

    // The broker pushes the new layout on the still-open session. Drain until
    // the drop-on-change event lands rather than asserting on a timing window.
    //
    // Every event is recorded, because three very different failures otherwise
    // produce the same bare assertion message and none of them is reproducible
    // without a container runtime: the broker never split; it split but pushed
    // nothing on the open session; or it pushed a layout whose `parent_ids` /
    // `child_ids` edges our derivation did not classify as a split. The
    // post-loop re-lookup separates the first from the other two.
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let mut saw_split = false;
    let mut new_epoch = epoch_before;
    let mut observed: Vec<String> = Vec::new();
    while tokio::time::Instant::now() < deadline && !saw_split {
        let Ok(Some(ev)) =
            tokio::time::timeout(Duration::from_secs(5), client.next_scalable_event()).await
        else {
            continue;
        };
        match ev {
            magnetar::scalable::ScalableEvent::DagUpdated { delta, .. } => {
                new_epoch = delta.epoch;
                observed.push(format!(
                    "DagUpdated{{epoch={}, added={}, removed={}, splits={}, merges={}}}",
                    delta.epoch,
                    delta.added.len(),
                    delta.removed.len(),
                    delta.split_events.len(),
                    delta.merge_events.len()
                ));
                if !delta.split_events.is_empty() {
                    assert_eq!(
                        delta.split_events[0].parent_segment_id.0, segment_to_split,
                        "the split names the segment the admin API split"
                    );
                    saw_split = true;
                }
            }
            magnetar::scalable::ScalableEvent::DagChangedDuringConsume { reason, .. } => {
                observed.push(format!("DagChangedDuringConsume{{reason={reason:?}}}"));
                assert_eq!(
                    reason,
                    magnetar::proto::DagChangeReason::Split,
                    "a split is classified from the children's parent_ids"
                );
                saw_split = true;
            }
            other => observed.push(format!("{other:?}")),
        }
    }

    // Ground truth from the broker, independent of what it pushed.
    let broker_view = match client.lookup_scalable_topic(&topic).await {
        Ok(l) => describe_layout(l.epoch, &l.segments),
        Err(err) => format!("re-lookup failed: {err}"),
    };

    assert!(
        saw_split,
        "the broker-driven split reached the client as a drop-on-change event.\n\
         split segment id: {segment_to_split}\n\
         epoch before: {epoch_before}, last pushed epoch: {new_epoch}\n\
         events observed in 60s ({}): {}\n\
         broker layout on re-lookup: {broker_view}",
        observed.len(),
        if observed.is_empty() {
            "none".to_owned()
        } else {
            observed.join(" | ")
        }
    );
    assert!(
        new_epoch > epoch_before,
        "the layout epoch advanced: {epoch_before} -> {new_epoch}"
    );

    client.close_scalable_topic_session(lookup.session_id);
}

/// (4) **v4 compatibility** (ADR-0093 §D3) against a real Pulsar 4.x broker.
///
/// The negotiation half of the contract: a 4.x broker has no PIP-460 surface,
/// so the client must refuse locally rather than emit a command the peer cannot
/// parse. This is what lets the surface ship without breaking existing
/// deployments, and it is only provable against a real v4 broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_unsupported_on_v4_broker() {
    let (service_url, _admin_url, _container) = start_broker(Broker::V4Lts)
        .await
        .expect("pulsar 4.x broker starts");

    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await
        .expect("client connects to a v4 broker");

    assert!(
        !client.broker_supports_scalable_topics(),
        "a Pulsar 4.x broker does not advertise the PIP-460 capability"
    );

    let err = client
        .lookup_scalable_topic(&format!("topic://{TENANT_NS}/never-created"))
        .await
        .expect_err("the scalable path refuses against a v4 broker");
    let rendered = err.to_string();
    assert!(
        rendered.contains("does not support scalable topics"),
        "the refusal names the reason, got `{rendered}`"
    );

    // The connection is unharmed — a refused scalable lookup must not poison
    // the session for ordinary v4 work.
    assert!(
        client.is_connected(),
        "refusing the scalable path leaves the v4 connection usable"
    );
}
