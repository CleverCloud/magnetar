// SPDX-License-Identifier: Apache-2.0

//! End-to-end rolling per-second stats window tests against a real Apache Pulsar
//! 4.x standalone broker.
//!
//! Mirrors Java parity for `ProducerStats#getSendMsgsRate` /
//! `ProducerStats#getSendBytesRate` and `ConsumerStats#getRateMsgsReceived` /
//! `ConsumerStats#getRateBytesReceived`. The Java client wires the rolling
//! window via `ProducerStatsRecorderImpl` / `ConsumerStatsRecorderImpl` which
//! tick once a second; in magnetar the caller drives the cadence by calling
//! [`magnetar::runtime_tokio::Producer::record_rate_window`] and
//! [`magnetar::runtime_tokio::Consumer::record_rate_window`] periodically.
//!
//! This test spins a paced send loop over ~3s while ticking the windows once a
//! second, then asserts the resulting `msgs_per_sec` / `bytes_per_sec` are both
//! strictly positive — i.e. the rolling delta math actually fires end-to-end
//! against a live broker.
//!
//! Gated behind the `e2e` feature flag. Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_rolling_stats -- --nocapture
//! ```
//!
//! Requires Docker on the host. CI runs these only in the dedicated `e2e`
//! workflow (`workflow_dispatch` + `release/*` branches).

#![cfg(feature = "e2e")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
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
        .with_wait_for(WaitFor::message_on_stdout("messaging service is ready"))
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

/// Drives the rolling-window samplers for a producer + consumer on a 1-second
/// cadence (matching Java `ProducerStatsRecorderImpl`'s `statsIntervalSeconds`
/// default). Each tick takes a host-`Instant` snapshot and pushes it through
/// the public `record_rate_window` helpers.
///
/// Spinning both samplers inside the same task is the simplest way to avoid
/// channel use (per [ADR-0003](../../../specs/adr/0003-no-channels-rule.md))
/// — we just borrow `Arc<AtomicBool>` for the stop flag.
async fn drive_rate_windows(
    producer: magnetar::runtime_tokio::Producer,
    consumer: magnetar::runtime_tokio::Consumer,
    stop: Arc<AtomicBool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    // First tick fires immediately; the first call only seeds the baseline so
    // we want the seed in early.
    ticker.tick().await;
    producer.record_rate_window(Instant::now());
    consumer.record_rate_window(Instant::now());
    while !stop.load(Ordering::Acquire) {
        ticker.tick().await;
        producer.record_rate_window(Instant::now());
        consumer.record_rate_window(Instant::now());
    }
}

/// End-to-end: produce + consume at a paced cadence over ~3s with periodic
/// `record_rate_window` ticks, then assert both producer and consumer surface
/// strictly positive `msgs_per_sec` and `bytes_per_sec`.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_rolling_per_second_stats_window() -> Result<(), Box<dyn std::error::Error>> {
    // Paced send loop: 100 messages over ~3s ≈ 33 msg/s.
    const TOTAL_MSGS: usize = 100;
    const TOTAL_DURATION: Duration = Duration::from_secs(3);

    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-rolling-stats-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-rolling-stats")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await?;

    // Spawn the 1-Hz rate-window sampler. The producer/consumer handles are
    // cheap clones (`Arc<Shared>` inside), so the sampler can own its pair
    // and the main task can keep producing + consuming on the original
    // handles in parallel.
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn(drive_rate_windows(
        producer.clone(),
        consumer.clone(),
        Arc::clone(&stop),
    ));

    let per_msg_delay = TOTAL_DURATION / TOTAL_MSGS as u32;

    let consume_handle = tokio::spawn({
        let consumer = consumer.clone();
        async move {
            for _ in 0..TOTAL_MSGS {
                let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
                    .await
                    .expect("consumer.receive timeout")
                    .expect("consumer.receive error");
                consumer.ack(msg.message_id).await.ok();
            }
        }
    });

    for i in 0..TOTAL_MSGS {
        let payload = format!("rolling-stats-msg-{i:04}-padding-XXXXXXXXXXXXXXXXXXXX").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
        tokio::time::sleep(per_msg_delay).await;
    }

    // Drain the consumer side and stop the sampler.
    consume_handle.await?;
    stop.store(true, Ordering::Release);
    let _ = sampler.await;

    let producer_stats = producer.stats();
    let consumer_stats = consumer.stats();

    producer.close().await?;
    consumer.close().await?;
    client.close().await;

    assert!(
        producer_stats.msgs_per_sec > 0.0,
        "expected producer msgs_per_sec > 0, got {} (stats={producer_stats:?})",
        producer_stats.msgs_per_sec,
    );
    assert!(
        producer_stats.bytes_per_sec > 0.0,
        "expected producer bytes_per_sec > 0, got {} (stats={producer_stats:?})",
        producer_stats.bytes_per_sec,
    );
    assert!(
        consumer_stats.msgs_per_sec > 0.0,
        "expected consumer msgs_per_sec > 0, got {} (stats={consumer_stats:?})",
        consumer_stats.msgs_per_sec,
    );
    assert!(
        consumer_stats.bytes_per_sec > 0.0,
        "expected consumer bytes_per_sec > 0, got {} (stats={consumer_stats:?})",
        consumer_stats.bytes_per_sec,
    );
    Ok(())
}
