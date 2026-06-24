// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for the pluggable receiver-queue policy (issue #301, PIP-74
//! `autoScaledReceiverQueueSizeEnabled` parity) against a real Apache Pulsar 4.x
//! standalone broker.
//!
//! A consumer configured with [`magnetar_proto::Auto`] drains a pre-produced
//! backlog. The sanity claim is operational, not numeric: the consumer makes
//! forward progress (drains the whole backlog) and its buffered-queue byte
//! footprint stays bounded — it never balloons to hold the entire backlog at
//! once. The auto-adjust tick rides the tokio driver's existing
//! `poll_timeout`/`handle_timeout` loop, so no manual ticking is needed here.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_receiver_queue_policy -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_proto::Auto;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
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

/// End-to-end: produce a backlog, subscribe with an `Auto` receiver-queue
/// policy, drain the whole backlog, and assert (a) every message was received
/// (forward progress) and (b) the buffered-queue depth never approached the full
/// backlog (bounded memory — the `Auto` floor + byte budget keep prefetch
/// modest while still letting the broker stream ahead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_auto_receiver_queue_drains_backlog_without_unbounded_memory()
-> Result<(), Box<dyn std::error::Error>> {
    const BACKLOG: usize = 500;
    // A small floor so the prefetch window stays well below the backlog; the
    // byte budget is generous so the floor (not the OOM cap) governs here.
    const FLOOR: usize = 20;
    const MAX_BYTES: usize = 64 * 1024 * 1024;

    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-rq-policy-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Produce the backlog FIRST so the consumer faces a full queue to drain.
    let producer = client.producer(&topic).create().await?;
    for i in 0..BACKLOG {
        let payload = format!("rq-policy-msg-{i:05}-padding-XXXXXXXXXXXXXXXXXXXXXXXX").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
    }
    producer.flush().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-rq-policy")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .receiver_queue_policy(Arc::new(Auto::new(FLOOR, MAX_BYTES)))
        // Fast adjust cadence so the ramp is observable within the test window.
        .receiver_queue_adjust_interval(Duration::from_millis(200))
        .subscribe()
        .await?;

    let mut received = 0usize;
    let mut max_observed_queue_depth = 0usize;
    while received < BACKLOG {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive())
            .await
            .expect("consumer.receive timeout")
            .expect("consumer.receive error");
        consumer.ack(msg.message_id).await.ok();
        received += 1;
        // Sample the buffered-queue depth — the prefetch the broker has pushed
        // but the user has not yet drained. The Auto floor + byte budget keep
        // this bounded; it must never approach the whole backlog.
        max_observed_queue_depth = max_observed_queue_depth.max(consumer.available_in_queue());
    }

    let current_target = consumer.current_receiver_queue_size();

    consumer.close().await?;
    client.close().await;

    // (a) Forward progress: the whole backlog drained.
    assert_eq!(
        received, BACKLOG,
        "Auto-policy consumer must drain the entire backlog"
    );
    // (b) Bounded memory: the buffered prefetch never held more than a small
    // fraction of the backlog at once. The Auto floor (20) plus bounded growth
    // keep it well under half the backlog even after ramping.
    assert!(
        max_observed_queue_depth < BACKLOG / 2,
        "Auto receiver queue must stay bounded: observed max buffered depth \
         {max_observed_queue_depth} approached the full backlog of {BACKLOG}"
    );
    // The target stayed within the sane band (floor .. a bounded multiple),
    // never running away.
    assert!(
        current_target >= FLOOR,
        "the auto-tuned target {current_target} must not drop below the floor {FLOOR}"
    );
    Ok(())
}
