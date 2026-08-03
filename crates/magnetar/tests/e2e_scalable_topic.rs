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
//! 1. [`e2e_scalable_topic_lookup_then_consume`] — lookup resolves the broker's own segment layout,
//!    the consumer registers with the controller leader and is assigned a share of it, and every
//!    `segment://` topic it is handed belongs to the layout the lookup returned.
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

use std::time::Duration;

use magnetar::PulsarClient;
use magnetar::scalable::ScalableConsumerType;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
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
        // `brokers healthcheck` exits non-zero until the broker serves; the
        // namespace probe additionally proves standalone finished bootstrapping,
        // which is what the topic-create calls below depend on.
        last = pulsar_admin(container, &["namespaces", "list", "public"]).await?;
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

/// Run `pulsar-admin` inside the container and return its combined output.
async fn pulsar_admin(
    container: &testcontainers::ContainerAsync<GenericImage>,
    args: &[&str],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut command = vec!["bin/pulsar-admin".to_owned()];
    command.extend(args.iter().map(|a| (*a).to_owned()));
    let mut out = container.exec(ExecCommand::new(command)).await?;
    let stdout = String::from_utf8_lossy(&out.stdout_to_vec().await?).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr_to_vec().await?).into_owned();
    Ok(format!("{stdout}{stderr}"))
}

/// Create a scalable topic with `segments` initial segments.
async fn create_scalable_topic(
    container: &testcontainers::ContainerAsync<GenericImage>,
    topic: &str,
    segments: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let segments = segments.to_string();
    let out = pulsar_admin(
        container,
        &["scalable-topics", "create", topic, "--segments", &segments],
    )
    .await?;
    assert!(
        !out.to_lowercase().contains("error"),
        "pulsar-admin scalable-topics create failed: {out}"
    );
    Ok(())
}

/// (1) Lookup-then-consume happy path against a real PIP-460 broker.
///
/// Pins two things a broker-less test cannot: that the layout the client
/// resolves is the broker's own, and that registering as a consumer yields a
/// share **of that layout** rather than an unrelated set of topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scalable_topic_lookup_then_consume() {
    let (service_url, _admin_url, container) = start_broker(&scalable_image_tag(), true)
        .await
        .expect("pulsar 5.0 broker starts");

    let topic = format!("topic://{TENANT_NS}/e2e-scaled-lookup");
    create_scalable_topic(&container, &topic, 2)
        .await
        .expect("scalable topic created");

    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await
        .expect("client connects");

    assert!(
        client.broker_supports_scalable_topics(),
        "a 5.0 broker with scalableTopicsEnabled=true must advertise the capability"
    );

    let lookup = client
        .lookup_scalable_topic(&topic)
        .await
        .expect("scalable lookup resolves");

    // The broker normalises whatever form we asked for to the canonical
    // `topic://` identity.
    assert_eq!(
        lookup.resolved_topic_name.as_deref(),
        Some(topic.as_str()),
        "the broker resolves to the canonical identity"
    );
    assert_eq!(
        lookup.segments.len(),
        2,
        "the layout carries the two segments the topic was created with"
    );
    assert!(
        lookup.segments.iter().all(|s| !s.is_legacy()),
        "a real scalable topic is not the synthetic legacy layout"
    );
    assert!(
        lookup.segments.iter().all(|s| s.broker_url.is_some()),
        "every active segment carries a placement"
    );
    // The hash ranges partition the keyspace without overlap.
    let mut ranges: Vec<(u32, u32)> = lookup
        .segments
        .iter()
        .map(|s| (s.key_range.start, s.key_range.end))
        .collect();
    ranges.sort_unstable();
    for pair in ranges.windows(2) {
        assert!(
            pair[0].1 <= pair[1].0,
            "segment hash ranges must not overlap: {ranges:?}"
        );
    }

    // Registering is what grants a share of the topic.
    let assignment = client
        .scalable_topic_subscribe(
            &topic,
            "e2e-sub",
            "e2e-consumer",
            1,
            ScalableConsumerType::default(),
        )
        .await
        .expect("controller leader assigns a share");

    assert!(
        !assignment.segments.is_empty(),
        "a sole consumer is assigned at least one segment"
    );
    assert_eq!(
        assignment.layout_epoch, lookup.epoch,
        "the assignment is computed against the layout we resolved"
    );
    let layout_ids: Vec<u64> = lookup.segments.iter().map(|s| s.segment_id.0).collect();
    for seg in &assignment.segments {
        assert!(
            layout_ids.contains(&seg.segment_id.0),
            "assigned segment {} is not in the resolved layout {layout_ids:?}",
            seg.segment_id.0
        );
        assert!(
            seg.segment_topic.starts_with("segment://"),
            "the consumer is handed a segment topic, got `{}`",
            seg.segment_topic
        );
    }

    client.close_scalable_topic_session(lookup.session_id);
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
        assert!(
            printed
                .lines()
                .any(|l| l.split_whitespace().next() == Some(&seg.segment_id.0.to_string())),
            "segment {} is missing from the CLI output.\n--- printed ---\n{printed}",
            seg.segment_id.0
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

    // NOTE: `PulsarClient::scalable_stream_consumer` carries a
    // `E::ClientState: Clone` bound that `magnetar_runtime_tokio::Client` does
    // not satisfy, so it is not constructible on the default engine today —
    // tracked in `docs/follow-ups.md`. This test therefore drives the layout
    // session directly, which is the same wire path the StreamConsumer wraps.
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
    let out = pulsar_admin(
        &container,
        &[
            "scalable-topics",
            "split-segment",
            &topic,
            &segment_to_split.to_string(),
        ],
    )
    .await
    .expect("split-segment runs");
    assert!(
        !out.to_lowercase().contains("error"),
        "pulsar-admin scalable-topics split-segment failed: {out}"
    );

    // The broker pushes the new layout on the still-open session. Drain until
    // the drop-on-change event lands rather than asserting on a timing window.
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    let mut saw_split = false;
    let mut new_epoch = epoch_before;
    while tokio::time::Instant::now() < deadline && !saw_split {
        let Ok(Some(ev)) =
            tokio::time::timeout(Duration::from_secs(5), client.next_scalable_event()).await
        else {
            continue;
        };
        match ev {
            magnetar::scalable::ScalableEvent::DagUpdated { delta, .. } => {
                new_epoch = delta.epoch;
                if !delta.split_events.is_empty() {
                    assert_eq!(
                        delta.split_events[0].parent_segment_id.0, segment_to_split,
                        "the split names the segment the admin API split"
                    );
                    saw_split = true;
                }
            }
            magnetar::scalable::ScalableEvent::DagChangedDuringConsume { reason, .. } => {
                assert_eq!(
                    reason,
                    magnetar::scalable::DagChangeReason::Split,
                    "a split is classified from the children's parent_ids"
                );
                saw_split = true;
            }
            _ => {}
        }
    }

    assert!(
        saw_split,
        "the broker-driven split reached the client as a drop-on-change event"
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
