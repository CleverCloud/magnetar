// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 — scalable-topic end-to-end coverage against a real
//! Apache Pulsar broker.
//!
//! Runs against `apachepulsar/pulsar:5.0.0-M1`, the first published release
//! carrying PIP-460. Its `ServiceConfiguration` defaults
//! `scalableTopicsEnabled` to `true`; the fixture sets it explicitly anyway so
//! the test does not silently depend on an upstream default that may move
//! before Pulsar 5.0 final.
//!
//! Per [ADR-0046] these are ordinary tests — no `#[ignore]`, no `feature =
//! "e2e"` — gated only on `feature = "scalable-topics"` like the rest of the
//! surface. They require Docker on the host.
//!
//! # Test inventory
//!
//! 1. [`e2e_hardened_scalable_stream_consumer_contract`] — one transaction-enabled M1 broker proves
//!    typed multi-segment delivery, live and restored-vector acknowledgement, broker-effective
//!    vector seek replay, transaction commit/abort, single-member split progression in Strict mode,
//!    explicit broker-managed cross-member behavior, direct-bootstrap controller fallback when M1
//!    has not published a controller URL, reachable broker-authored segment authorities matching
//!    the bootstrap transport, and the accepted logical-close membership residue. Because the
//!    standalone broker controls child assignment, this does not isolate client-side Strict gating
//!    or prove same-cluster multi-broker routing.
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
//!
//! [ADR-0046]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::collections::BTreeSet;
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
use testcontainers::core::{CmdWaitFor, ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const IMAGE_REPO: &str = "apachepulsar/pulsar";
/// The first published Pulsar release carrying PIP-460. A milestone, not GA —
/// see ADR-0093 §D1. Overridable so a later RC can be tried without an edit.
const DEFAULT_SCALABLE_IMAGE_TAG: &str = "5.0.0-M1";
/// A v4 broker, for the negotiation-refusal test. Matches the tag the rest of
/// the e2e suite pins (`CLAUDE.md` § Validation chain).
const V4_IMAGE_TAG: &str = "4.0.4";

const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container — the image default costs
/// ~2.3 GiB RSS and libtest runs e2e tests in parallel. Enforced by
/// `cargo run -p xtask -- check-e2e-container-memory`; see
/// `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

const TENANT_NS: &str = "public/default";
const STREAM_BUDGET_BYTES: usize = 16 * 1024 * 1024;
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_TIMEOUT: Duration = Duration::from_secs(30);
const NO_DELIVERY_WINDOW: Duration = Duration::from_secs(3);
const TXN_TIMEOUT: Duration = Duration::from_secs(30);

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type PulsarContainer = testcontainers::ContainerAsync<GenericImage>;
type ByteStreamConsumer = StreamConsumer<BytesSchema>;
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

fn scalable_image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG")
        .unwrap_or_else(|_| DEFAULT_SCALABLE_IMAGE_TAG.to_owned())
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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Bring up one unauthenticated standalone broker at `tag`.
///
/// `scalable_topics` selects whether PIP-460 is enabled. It is passed
/// explicitly rather than relying on the upstream default, so the negative
/// (v4) case and the positive case differ only in the image.
async fn start_broker(
    tag: &str,
    scalable_topics: bool,
) -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();

    // Readiness is polled below rather than matched on a startup log line.
    // `apachepulsar/pulsar:5.0.0-M1` does not print 4.x's "Created namespace
    // public/default", so a log-string wait times out with `StartupTimeout` and
    // says nothing about why — measured on CI run 30843979377. Waiting on the
    // container being up and then asking the broker itself keeps the fixture
    // working across broker versions and makes a genuine startup failure
    // report the broker's own logs instead of an opaque timeout.
    //
    // The two arms are spelled out rather than assembled conditionally so each
    // keeps `PULSAR_MEM` and `.start()` on one chain — `cargo run -p xtask --
    // check-e2e-container-memory` can only verify the cap when it can see both
    // in the same expression.
    let container = if scalable_topics {
        GenericImage::new(IMAGE_REPO, tag)
            .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
            .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
            .with_wait_for(WaitFor::Nothing)
            .with_startup_timeout(Duration::from_mins(3))
            .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
            // Defaults to `true` in 5.0.0-M1's ServiceConfiguration; set
            // explicitly so the test does not depend on an upstream default
            // holding through to Pulsar 5.0 final.
            .with_env_var("PULSAR_PREFIX_scalableTopicsEnabled", "true")
            // PIP-483 auto split/merge also defaults on. Disable it so the only
            // layout change in these tests is the one the admin API triggers —
            // otherwise a background split could race the assertions.
            .with_env_var("PULSAR_PREFIX_scalableTopicAutoScaleEnabled", "false")
            .with_cmd(vec![
                "bin/pulsar".to_owned(),
                "standalone".to_owned(),
                "--no-functions-worker".to_owned(),
                "--no-stream-storage".to_owned(),
            ])
            .start()
            .await?
    } else {
        GenericImage::new(IMAGE_REPO, tag)
            .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
            .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
            .with_wait_for(WaitFor::Nothing)
            .with_startup_timeout(Duration::from_mins(3))
            .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
            .with_cmd(vec![
                "bin/pulsar".to_owned(),
                "standalone".to_owned(),
                "--no-functions-worker".to_owned(),
                "--no-stream-storage".to_owned(),
            ])
            .start()
            .await?
    };

    await_broker_ready(&container).await?;

    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    Ok((
        format!("pulsar://{host}:{binary_port}"),
        format!("http://{host}:{http_port}"),
        container,
    ))
}

/// Start the positive M1 fixture with direct routing and transactions enabled.
///
/// M1 writes its advertised broker authority into every segment placement, but
/// may omit the controller URL until leader election publishes one. Reserve an ephemeral loopback
/// listener before container startup and proxy that advertised port to Docker's random host
/// mapping, so concurrent test runs cannot race for one global host port.
async fn start_hardened_broker() -> TestResult<(String, PulsarContainer, HostPortProxy)> {
    init_tracing();
    let image_tag = scalable_image_tag();
    let proxy_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let broker_port = proxy_listener.local_addr()?.port();
    let container = GenericImage::new(IMAGE_REPO, image_tag.as_str())
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_exposed_port(ContainerPort::Tcp(broker_port))
        .with_wait_for(WaitFor::Nothing)
        .with_startup_timeout(Duration::from_mins(4))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
        .with_env_var("PULSAR_PREFIX_scalableTopicsEnabled", "true")
        .with_env_var("PULSAR_PREFIX_scalableTopicAutoScaleEnabled", "false")
        .with_env_var("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
        .with_env_var("PULSAR_PREFIX_advertisedAddress", "127.0.0.1")
        .with_env_var("PULSAR_PREFIX_brokerServicePort", broker_port.to_string())
        .with_cmd(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ \
                 conf/standalone.conf && \
             bin/pulsar initialize-transaction-coordinator-metadata \
                 --cluster standalone \
                 --configuration-store rocksdb:///pulsar/data/metadata && \
             exec bin/pulsar standalone \
                 --no-functions-worker \
                 --no-stream-storage"
                .to_owned(),
        ])
        .start()
        .await?;

    await_broker_ready(&container).await?;
    let host = container.get_host().await?;
    let loopback = match &host {
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv6(_) => false,
    };
    if !loopback {
        return Err(format!(
            "hardened scalable e2e requires a local IPv4 Docker host because M1 advertises \
             pulsar://127.0.0.1:{broker_port}; testcontainers reported `{host}`"
        )
        .into());
    }
    let mapped = container.get_host_port_ipv4(broker_port).await?;
    let proxy = spawn_host_port_proxy(proxy_listener, format!("{host}:{mapped}"));
    Ok((
        format!("pulsar://127.0.0.1:{broker_port}"),
        container,
        proxy,
    ))
}

/// Poll the broker's own health endpoint until it answers, then confirm the
/// bootstrap namespace exists.
///
/// On timeout this returns the broker's last log lines, so a startup failure is
/// diagnosable from the CI log rather than surfacing as a bare timeout.
async fn await_broker_ready(
    container: &testcontainers::ContainerAsync<GenericImage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(4);
    let mut last = String::new();
    while tokio::time::Instant::now() < deadline {
        // `pulsar-admin` exits non-zero until the broker serves; the namespace
        // probe additionally proves standalone finished bootstrapping, which is
        // what the topic-create calls below depend on. A non-zero exit is the
        // normal state of this loop, not a failure, so the raw form is used and
        // the code deliberately ignored — the loop's own deadline is the failure
        // condition, and `last` carries the final output into its message.
        let (_code, out) = pulsar_admin_raw(container, &["namespaces", "list", "public"]).await?;
        last = out;
        if last.contains("public/default") {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let logs = String::from_utf8_lossy(&container.stdout_to_vec().await?).into_owned();
    let tail: String = logs.lines().rev().take(40).collect::<Vec<_>>().join("\n");
    Err(format!(
        "broker did not become ready within 4 min.\n--- last pulsar-admin output ---\n{last}\n--- broker log tail ---\n{tail}"
    )
    .into())
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

/// Publish distinct opaque payloads through M1's bundled V5 client. This is the
/// real scalable producer path, not one producer opened directly per segment.
async fn publish_scalable_messages(
    container: &testcontainers::ContainerAsync<GenericImage>,
    service_url: &str,
    topic: &str,
    payloads: &[String],
) -> TestResult {
    if payloads.is_empty() || payloads.iter().any(|payload| payload.contains(',')) {
        return Err("scalable publisher requires non-empty comma-free payloads".into());
    }
    let messages = payloads.join(",");
    let command = vec![
        "bin/pulsar-client".to_owned(),
        "--url".to_owned(),
        service_url.to_owned(),
        "produce".to_owned(),
        "--disable-batching".to_owned(),
        "--messages".to_owned(),
        messages,
        topic.to_owned(),
    ];
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

async fn subscribe_byte_stream(
    client: &PulsarClient,
    topic: &str,
    subscription: &str,
    consumer_name: &str,
    ordering_mode: OrderingMode,
) -> TestResult<ByteStreamConsumer> {
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

async fn receive_expected_messages(
    consumer: &ByteStreamConsumer,
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

async fn receive_from_any(
    consumers: &[ByteStreamConsumer],
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

async fn wait_for_transaction_outcome(
    consumer: &ByteStreamConsumer,
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
    let count = subscription_stats
        .get("consumerCount")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            format!(
                "scalable stats for subscription `{subscription}` lack `consumerCount`: \
                 {subscription_stats}"
            )
        })?;
    Ok(usize::try_from(count)?)
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

async fn verify_m1_topology(
    client: &PulsarClient,
    topic: &str,
    service_url: &str,
    expected_segments: usize,
) -> TestResult {
    let lookup = client.lookup_scalable_topic(topic).await?;
    let result = (|| {
        if lookup.resolved_topic_name.as_deref() != Some(topic) {
            return Err(
                format!("M1 resolved `{topic}` as {:?}", lookup.resolved_topic_name).into(),
            );
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

async fn exercise_delivery_and_close_residue(
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
    verify_m1_topology(client, &topic, service_url, 4).await?;

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
    wait_for_status(&consumer, "all delivery leases acknowledged", |status| {
        status.receiver_budget_used() == 0
    })
    .await?;
    consumer.close().await?;

    // M1 has no unregister command. Logical close is definitive locally but
    // leaves this durable member while the pooled connection remains alive.
    wait_for_consumer_count(container, &topic, &subscription, 1).await
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
    wait_for_status(&consumer, "one attached vector-seek segment", |status| {
        status.assigned_segments() == 1 && status.attached_segments() == 1
    })
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
    let initial_payloads: Vec<&[u8]> = initial.iter().map(StreamMessage::payload).collect();
    let expected_payloads: Vec<&[u8]> = expected.iter().map(String::as_bytes).collect();
    if initial_payloads != expected_payloads {
        return Err(format!(
            "one-segment delivery must preserve FIFO before seek: {initial_payloads:?}"
        )
        .into());
    }

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
    wait_for_status(&consumer, "settled pre-seek delivery leases", |status| {
        status.receiver_budget_used() == 0
    })
    .await?;

    consumer.seek_positions(&seek_position).await?;
    let replay_expected = &expected[2..];
    let replayed =
        receive_expected_messages(&consumer, replay_expected, "post-seek replay phase").await?;
    if replayed.iter().any(|message| message.source() != &source) {
        return Err("vector seek replay crossed its sole source".into());
    }
    let replayed_payloads: Vec<&[u8]> = replayed.iter().map(StreamMessage::payload).collect();
    let expected_replayed_payloads: Vec<&[u8]> =
        replay_expected.iter().map(String::as_bytes).collect();
    if replayed_payloads != expected_replayed_payloads {
        return Err(format!(
            "vector seek must replay the inclusive source-local suffix; got {replayed_payloads:?}"
        )
        .into());
    }

    consumer.acknowledge(&replayed[0]).await?;
    consumer
        .acknowledge_cumulative(
            replayed
                .last()
                .ok_or("vector seek replay unexpectedly returned no messages")?,
        )
        .await?;
    drop(replayed);
    wait_for_status(&consumer, "settled post-seek delivery leases", |status| {
        status.receiver_budget_used() == 0
    })
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
    wait_for_status(&consumer, "three attached commit segments", |status| {
        status.assigned_segments() == 3 && status.attached_segments() == 3
    })
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
    wait_for_status(
        &consumer,
        "committed transactional leases resolved",
        |status| status.receiver_budget_used() == 0,
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
    wait_for_status(
        &consumer,
        "sealed parent plus two strict children",
        |status| {
            status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
                && status.assigned_segments() == 3
        },
    )
    .await?;
    let children =
        receive_expected_messages(&consumer, &child_payloads, "strict descendant phase").await?;
    if children
        .iter()
        .any(|message| message.source().segment_id().0 == parent_id)
    {
        return Err("strict child payload was delivered from the sealed parent".into());
    }
    consumer.acknowledge_batch(&children).await?;
    consumer.close().await?;
    Ok(())
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
    wait_for_status(&consumers[0], "broker-managed parent owner", |status| {
        status.assigned_segments() == 1 && status.attached_segments() == 1
    })
    .await?;
    for consumer in &consumers[1..] {
        wait_for_status(consumer, "broker-managed non-parent member", |status| {
            status.assigned_segments() == 0
        })
        .await?;
    }

    let parent_payloads = payloads(&format!("broker-parent-{suffix}"), 1);
    publish_scalable_messages(container, service_url, &topic, &parent_payloads).await?;
    let mut parents = receive_expected_messages(
        &consumers[0],
        &parent_payloads,
        "broker-managed parent phase",
    )
    .await?;
    let parent = parents
        .pop()
        .ok_or("broker-managed parent message missing")?;
    if parent.source().segment_id().0 != parent_id {
        return Err("broker-managed parent delivery came from the wrong segment".into());
    }
    split_scalable_segment(container, &topic, parent_id).await?;
    let child_payloads = payloads(&format!("broker-child-{suffix}"), 6);
    publish_scalable_messages(container, service_url, &topic, &child_payloads).await?;
    for (index, consumer) in consumers.iter().enumerate() {
        let expected_segments = usize::from(index == 0);
        wait_for_status(consumer, "split visible before broker drain", |status| {
            status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
                && status.assigned_segments() == expected_segments
        })
        .await?;
    }
    consumers[0].acknowledge(&parent).await?;
    drop(parent);
    for consumer in &consumers {
        wait_for_status(consumer, "balanced post-drain split assignment", |status| {
            status
                .layout_epoch()
                .is_some_and(|epoch| epoch > initial_epoch)
                && status.assigned_segments() == 1
        })
        .await?;
    }

    let (remote_owner, remote_child) = receive_from_any(
        &consumers,
        &[1, 2],
        "broker-managed member without parent history",
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

/// (1) Public M1-hardened aggregate contract against one real broker.
///
/// The phases are sequential and UUID-isolated so this adds no concurrent broker
/// beyond the lightweight lookup test it replaces. Standalone cannot prove a
/// same-cluster multi-broker route. M1 may omit its controller URL before
/// leader election, in which case successful subscription proves reuse of the
/// already-authenticated direct bootstrap connection.
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
            "M1 with scalableTopicsEnabled=true did not advertise scalable-topic support".into(),
        );
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let mut created_topics = Vec::new();
    let exercise: TestResult = async {
        exercise_delivery_and_close_residue(
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

    // Physical close releases every pooled socket. M1 deliberately retains the
    // persisted controller registrations for its 60-second reconnect grace, so
    // cleanup uses the exact force-delete operation rather than waiting or
    // pretending disconnect is an immediate unregister.
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

/// (2) `topic-info` CLI round-trip against a real PIP-460 broker.
///
/// The CLI and the driver resolve the same topic through the same code path, so
/// the assertion is that the printed view matches what the library reports —
/// the two cannot drift without this failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_info_cli_round_trip() {
    let (service_url, _admin_url, container) = start_broker(&scalable_image_tag(), true)
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

/// (3) Drop-on-DAG-change observed against a broker-driven segment split.
///
/// PIP-483 auto-scale is disabled on this broker, so the only layout change is
/// the one `pulsar-admin scalable-topics split-segment` triggers — the
/// assertion is about the client's reaction, not about a timing window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_drops_on_broker_split() {
    let (service_url, _admin_url, container) = start_broker(&scalable_image_tag(), true)
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
    let (service_url, _admin_url, _container) = start_broker(V4_IMAGE_TAG, false)
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
