// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for the issue-#326 batch-ack-tracker leak against a real
//! Apache Pulsar 4.x standalone broker.
//!
//! A consumer on a BATCHED topic that acks exclusively via cumulative watermarks
//! (never an individual ack) must keep the PIP-54 `batch_ack_tracker` bounded by
//! the un-acked window. Before the fix the cumulative branch pruned only the
//! tracker entry of the exact acked `(ledger, entry)` key; every entry below the
//! cumulative position leaked one `BatchAckEntry` until reconnect — the production
//! workload that surfaced this (a contiguous-watermark acker at 1000-message
//! cadence) drove a 12-instance fleet out of memory at ~24 GiB every 4-6 h.
//!
//! The deterministic trajectory proof lives in the proto / runtime / differential
//! layers, which pin a synthetic frame sequence; this end-to-end test pins the
//! user-visible contract against a real broker: after consuming a batched stream
//! with cumulative-only acks and acking at the consume front, the
//! `ConsumerStats::pending_batch_acks` gauge reads ZERO — not "everything below
//! the last watermark" as the bug left it.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_cumulative_batch_ack_prune -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Sub-messages packed into each batched broker entry.
const BATCH_SIZE: usize = 5;
/// Batched entries produced over the run (one flush per entry).
const ENTRIES: usize = 10;

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

/// Cumulative-only acking on a batched topic keeps the batch-ack tracker bounded:
/// after the final cumulative ack at the consume front, `pending_batch_acks` is 0.
///
/// The producer flushes exactly [`BATCH_SIZE`] messages per batched entry (count-cap
/// flush, generous publish delay — the `e2e_producer_batching_flushes_on_max_msgs`
/// pattern), so every broker entry is a real batch and every delivery stamps one
/// tracker entry. The consumer receives the whole stream without a single individual
/// ack — an intermediate cumulative watermark mid-stream mirrors the production
/// cadence — then acks cumulatively on the LAST received id. Only the final gauge is
/// asserted zero: mid-stream reads race broker prefetch (entries above the watermark
/// may already be delivered and legitimately tracked), but once every produced
/// message has been received and the consume front is acked there is nothing left to
/// track. Before the fix this final read reported every entry below the last
/// watermark (here: `ENTRIES - 1` leaked entries).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_cumulative_only_acking_keeps_batch_ack_tracker_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-cumulative-prune-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Count-cap batching: fire BATCH_SIZE sends before awaiting any so each wave
    // fills one batch and flushes as ONE batched broker entry.
    let producer = client
        .producer(topic.clone())
        .batching(BATCH_SIZE, 1_000_000)
        .batching_max_publish_delay(Duration::from_secs(60))
        .create()
        .await?;
    for entry in 0..ENTRIES {
        let send_futures: Vec<_> = (0..BATCH_SIZE)
            .map(|i| {
                let payload = format!("cumulative-prune-{entry}-{i}").into_bytes();
                producer.send(OutgoingMessage::with_payload(payload).into())
            })
            .collect();
        for fut in send_futures {
            fut.await?;
        }
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic.clone())
        .subscription("magnetar-e2e-cumulative-prune")
        .subscription_type(SubType::Failover)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let total = ENTRIES * BATCH_SIZE;
    let mut last_id = None;
    for n in 1..=total {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive())
            .await
            .unwrap_or_else(|_| panic!("message {n}/{total} must arrive within 30s"))?;
        last_id = Some(msg.message_id);
        // Production cadence: one cumulative watermark mid-stream, never an
        // individual ack. No gauge assertion here — entries above the watermark
        // may already be delivered (broker prefetch) and legitimately tracked.
        if n == total / 2 {
            consumer.ack_cumulative(msg.message_id).await?;
        }
    }

    // Sensitivity: with every message received and only one mid-stream watermark
    // sent, the tracker must still hold the entries above that watermark — a
    // regression cannot pass by the gauge reading 0 throughout.
    let before_final = consumer.stats().pending_batch_acks;
    assert!(
        before_final > 0,
        "with the full batched stream delivered and only a mid-stream watermark \
         acked, the tracker must hold the entries above the watermark"
    );

    // The #326 bound: a cumulative ack at the consume front leaves NOTHING to
    // track. Before the fix this read ENTRIES - 1 (every entry below the last
    // watermark leaked until reconnect).
    consumer
        .ack_cumulative(last_id.expect("received at least one message"))
        .await?;
    assert_eq!(
        consumer.stats().pending_batch_acks,
        0,
        "a cumulative ack at the consume front must prune every batch-ack tracker \
         entry it covers, not just the exact acked key (issue #326)"
    );

    consumer.close().await?;
    client.close().await;
    Ok(())
}
