// SPDX-License-Identifier: Apache-2.0

//! Issue #451 e2e — a producer detached by `topics unload` on a connection that
//! stays up, against a real Apache Pulsar 4.x standalone broker.
//!
//! `bin/pulsar-admin topics unload <topic>-partition-1` makes the owning broker
//! remove the producer and write `CommandCloseProducer` on a connection that
//! keeps serving every other partition. A standalone broker has nowhere to
//! reassign the bundle to, so there is no TCP drop and no supervised reconnect —
//! exactly the production shape of issue #451.
//!
//! Before ADR-0106 the detached child producer stayed at `broker_ready = false`
//! for the life of the connection: `PartitionedProducer`'s round-robin router has
//! no readiness input, so every publish it routed to that partition failed
//! `code=-1 "send timeout"` after the configured `send_timeout`, forever.
//!
//! Two tests:
//!
//! 1. `e2e_producer_partition_unload_reattach` — the regression. 20 publishes across 2 partitions
//!    after the unload must ALL resolve `Ok`, and no child may report a transport disconnect.
//! 2. `e2e_producer_exclusive_unload_outcome` — observability only, for an `Exclusive`-access
//!    single-topic producer. `emit_command_producer` sends `topic_epoch: None` on the re-attach, so
//!    the broker arbitrates; parity with Java on that path is UNVERIFIED (ADR-0106 § Consequences).
//!    The outcome is printed, not asserted.
//!
//! A standalone broker runs the `ModularLoadManager`, so the close carries
//! `assigned_broker_service_url = None`. The `Some(url)` shape an
//! Extensible-Load-Manager multi-phase unload produces is covered by the proto,
//! runtime and differential layers instead.
//!
//! Runs as a regular test under `cargo test` (ADR-0046) — no `#[ignore]`, no
//! feature gate. Requires Docker on the host.

use std::time::Duration;

use magnetar::{MessageRoutingMode, OutgoingMessage, PulsarClient};
use testcontainers::core::{CmdWaitFor, ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const PARTITIONS: usize = 2;

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

/// REGRESSION (issue #451): unload ONE partition of a partitioned topic and keep
/// publishing. The round-robin router has no readiness input, so every
/// `1/PARTITIONS`-th publish lands on the detached child — before ADR-0106 each
/// of those failed `code=-1 "send timeout"` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_partition_unload_reattach() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-producer-unload-reattach";

    // `pulsar-admin` inside the container owns partitioned-topic creation here:
    // the unload below is exec'd the same way, so both go through one surface.
    container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "topics",
                "create-partitioned-topic",
                "-p",
                "2",
                topic,
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.to_owned())
        .routing(MessageRoutingMode::RoundRobin)
        .send_timeout(Duration::from_secs(10))
        .create()
        .await?;

    // Warm up so every child producer is attached before the unload.
    for i in 0..(PARTITIONS * 2) {
        producer
            .send(OutgoingMessage::with_payload(
                format!("warmup-{i}").into_bytes(),
            ))
            .await?;
    }

    // Detach the producer on ONE partition. The standalone broker reloads the
    // bundle on itself: no TCP drop, so nothing resets the connection.
    container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "topics",
                "unload",
                &format!("{topic}-partition-1"),
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    // Round-robin keeps routing half of these to the detached child.
    for i in 0..20 {
        let payload = format!("post-unload-{i}").into_bytes();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            producer.send(OutgoingMessage::with_payload(payload)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "publish {i} WEDGED after unloading {topic}-partition-1: the detached child \
                 producer was never re-attached (issue #451)"
            )
        });
        result.unwrap_or_else(|err| {
            panic!(
                "publish {i} failed after unloading {topic}-partition-1: {err:?} — before \
                 ADR-0106 this was `code=-1 send timeout`, forever (issue #451)"
            )
        });
    }

    producer.close().await?;
    drop(container);
    Ok(())
}

/// OBSERVABILITY (issue #451, ADR-0106 § Consequences): the re-attach sends
/// `topic_epoch: None`, so for an `Exclusive`-access producer the broker
/// arbitrates and may answer `ProducerFenced`, which terminalizes the slot.
/// Parity with Java on that path is UNVERIFIED, so the outcome is printed rather
/// than asserted — the assertion is only that the publish RESOLVES, either way,
/// instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_exclusive_unload_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-producer-unload-exclusive";

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .producer(topic)
        .access_mode(magnetar::proto::pb::ProducerAccessMode::Exclusive)
        .send_timeout(Duration::from_secs(10))
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(b"exclusive-warmup".to_vec()).into())
        .await?;

    container
        .exec(
            ExecCommand::new(["bin/pulsar-admin", "topics", "unload", topic])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        producer.send(OutgoingMessage::with_payload(b"exclusive-post-unload".to_vec()).into()),
    )
    .await
    .expect("an Exclusive producer's post-unload publish must RESOLVE, not hang");
    match outcome {
        Ok(id) => println!("exclusive-access re-attach after unload: published {id:?}"),
        Err(err) => println!("exclusive-access re-attach after unload: rejected {err:?}"),
    }

    drop(container);
    Ok(())
}
