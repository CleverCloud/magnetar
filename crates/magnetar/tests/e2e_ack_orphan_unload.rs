// SPDX-License-Identifier: Apache-2.0

//! Issue #346 e2e — ack orphaned by same-broker `CloseConsumer` + no
//! deadline, against a real Apache Pulsar 4.x standalone broker.
//!
//! `bin/pulsar-admin topics unload` forces the broker to tear the topic's
//! dispatcher down and re-load it on the SAME broker (a standalone broker
//! has nowhere else to reassign the bundle to) — the same-broker
//! `CommandCloseConsumer{assigned_broker_service_url: None}` root cause
//! issue #307 fixed the receiver-queue wedge for, and issue #346 fixes the
//! ack-orphaning half of.
//!
//! # Shape
//!
//! 1. Produce one message, subscribe, and `receive()` it (leaving it un-acked).
//! 2. Exec `bin/pulsar-admin topics unload <topic>` inside the container — this races the client's
//!    in-flight `ack()` against the broker tearing the consumer id down.
//! 3. `ack(msg.message_id)` MUST resolve within 15s — either `Ok(())` (the in-place re-subscribe
//!    won the race and the ack landed against the fresh consumer id) or `Err(Broker{code: -1,
//!    message: "ack orphaned by broker consumer close"})` (the close-handler sweep won the race).
//!    The **non-hang bound is the assertion** — before this fix the ack would park until the (also
//!    new) `ack_response_timeout` backstop, or forever if that knob were disabled.
//! 4. A fresh produce + `receive()` + `ack()` round-trips `Ok` afterward, proving the in-place
//!    re-subscribe left the consumer healthy.
//!
//! Runs as a regular test under `cargo test` (ADR-0046) — no `#[ignore]`,
//! no feature gate. Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::runtime_tokio::ClientError;
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{CmdWaitFor, ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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
/// `container_handle`). Mirrors `e2e_pulsar.rs::start_pulsar`.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_ack_orphan_unload() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-ack-orphan-unload";

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Produce + receive one message, leaving it un-acked.
    let producer = client.producer(topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"orphan-me".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-ack-orphan-unload")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .expect("initial receive must not hang")?;

    // Stage the ack NOW — `Consumer::ack` synchronously queues the
    // `CommandAck` and wakes the driver before returning the future, so the
    // request is genuinely in `pending_requests` (and very likely already on
    // the wire) by the time the unload below lands. Deliberately not
    // `.await`ed yet: the whole point is to race this in-flight ack against
    // the broker tearing the consumer id down, mirroring the production
    // window issue #346 fixes. Awaiting it here first would let it resolve
    // normally and never exercise the close-handler sweep at all.
    let ack_fut = consumer.ack(msg.message_id);

    // Force a same-broker bundle reassignment: the standalone broker tears
    // the topic's dispatcher down and reloads it on itself — no TCP drop,
    // exactly the #307/#346 root cause.
    container
        .exec(
            ExecCommand::new(["bin/pulsar-admin", "topics", "unload", topic])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    // The pending ack must not hang: either it landed Ok before the close
    // (unlikely given the exec above already blocked for the unload to
    // complete, but not impossible under scheduling jitter), or the orphan
    // sweep fails it fast with the -1 sentinel. Either way it must resolve
    // within a bounded window well under the 30s `ack_response_timeout`
    // default — the non-hang bound is the assertion.
    let ack_result = tokio::time::timeout(Duration::from_secs(15), ack_fut)
        .await
        .expect(
            "ack must resolve within 15s after the topic unload — orphan sweep or \
             ack_response_timeout backstop regression",
        );
    match ack_result {
        Ok(()) => {}
        Err(ClientError::Broker { code, message }) => {
            assert_eq!(code, -1, "orphaned/timed-out ack must use the -1 sentinel");
            assert!(
                message == "ack orphaned by broker consumer close" || message == "ack timeout",
                "unexpected ack error message after unload: {message:?}"
            );
        }
        Err(other) => panic!("unexpected ack error after unload: {other:?}"),
    }

    // A fresh produce + receive + ack round-trips Ok afterward — the
    // in-place re-subscribe left the consumer healthy.
    let producer2 = client.producer(topic).create().await?;
    producer2
        .send(OutgoingMessage::with_payload(b"post-unload".to_vec()).into())
        .await?;
    producer2.close().await?;

    let msg2 = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .expect("post-unload receive must not hang")?;
    consumer.ack(msg2.message_id).await?;

    drop(container);
    Ok(())
}
