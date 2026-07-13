// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the producer batching and chunking surfaces, modelled
//! after Apache Pulsar's `BatchMessageTest`, `ConsumerBatchReceiveTest` and
//! `MessageChunkingTest`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_batch_chunk -- --nocapture
//! ```
//!
//! Requires Docker on the host. See `e2e_pulsar.rs` for the broker container
//! plumbing; this file uses the same image/wait strategy via a local helper.
//!
//! PIP-37 (Large Message Size) requires producer chunking + batching disabled
//! (chunks-never-batched). The chunked round-trip below mirrors that constraint.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future::join_all;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.2.3";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

const PARTITIONS: usize = 12;
const RECEIVER_QUEUE_SIZE: usize = 2_000;
const CHUNKED_MESSAGES: usize = 900;
const SMALL_MESSAGES: usize = 1_100;
const CHUNK_PAYLOAD_SIZE: usize = 35_840;

#[derive(Debug)]
struct PartitionReceive {
    partition: usize,
    received: usize,
}

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

#[test]
fn default_image_tag_tracks_latest_pulsar_four() {
    assert_eq!(DEFAULT_IMAGE_TAG, "4.2.3");
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
/// `container_handle`).
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

/// Start Pulsar 4.2.3 with an 8,192-byte broker message limit so each
/// 35,840-byte issue-#331 payload is split into exactly five 7,168-byte chunks.
async fn start_small_message_pulsar() -> Result<
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
        .with_env_var("PULSAR_PREFIX_maxMessageSize", "8192")
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd([
            "bash",
            "-lc",
            "bin/apply-config-from-env.py conf/standalone.conf && exec bin/pulsar standalone",
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    Ok((
        format!("pulsar://{host}:{binary_port}"),
        format!("http://{host}:{http_port}"),
        container,
    ))
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// Producer with `batching(max_msgs=5, max_bytes=1 MiB)` and a generous delay
/// (1 minute) so the batch can only flush on the message-count cap. Sends 5
/// messages and verifies the consumer receives all 5 in order. Mirrors Java
/// `BatchMessageTest` (`batchingMaxMessages` triggering a flush).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_batching_flushes_on_max_msgs() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-batch-maxmsgs");

    let producer = client
        .producer(&topic)
        .batching(5, 1_000_000)
        .batching_max_publish_delay(Duration::from_secs(60))
        .create()
        .await?;
    let payloads: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("batch-msg-{i}").into_bytes())
        .collect();
    // Sequential await would never fill the batch (each send would wait on a
    // receipt that arrives only after a flush). Mirror Java
    // `BatchMessageTest`'s "fire all sendAsync, then join" pattern: enqueue
    // every message before awaiting any, so the 5th send fills the batch and
    // the broker emits one batched receipt that resolves all five futures.
    let send_futures: Vec<_> = payloads
        .iter()
        .map(|p| producer.send(OutgoingMessage::with_payload(p.clone()).into()))
        .collect();
    for fut in send_futures {
        fut.await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-batch-maxmsgs")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payloads);
    Ok(())
}

/// Consumer `receive_batch_with_bytes_cap(count=5, bytes=1 MiB)` mirrors Java's
/// `BatchReceivePolicy`: the call returns at most 5 messages even when 10 are
/// available, and a second call drains the remainder. Modelled after
/// `ConsumerBatchReceiveTest`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_consumer_batch_receive() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-batchrecv");

    let producer = client.producer(&topic).create().await?;
    let payloads: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("recv-msg-{i:02}").into_bytes())
        .collect();
    for p in &payloads {
        producer
            .send(OutgoingMessage::with_payload(p.clone()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-batchrecv")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received: Vec<Vec<u8>> = Vec::with_capacity(payloads.len());
    while received.len() < payloads.len() {
        let batch = consumer
            .receive_batch_with_bytes_cap(5, 1_000_000, Duration::from_secs(10))
            .await?;
        assert!(
            batch.len() <= 5,
            "batch should respect count cap of 5, got {}",
            batch.len()
        );
        assert!(!batch.is_empty(), "batch receive timed out before drain");
        for msg in batch {
            received.push(msg.payload.to_vec());
            consumer.ack(msg.message_id).await?;
        }
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payloads);
    Ok(())
}

/// Producer with `chunking(true)` + batching disabled splits an oversize payload
/// (~6 MiB, above the default 5 MiB `max_message_size`) into chunks; the
/// consumer reassembles them into a single `IncomingMessage` whose payload
/// matches the original length. Mirrors PIP-37 / Java `MessageChunkingTest`.
///
/// Only the length is asserted — per-byte comparison would dominate test wall
/// time without adding signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_chunked_message_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-chunk");

    // Chunks-never-batched: disable batching explicitly even though it's the
    // default — makes the constraint visible at the call site.
    let producer = client
        .producer(&topic)
        .chunking(true)
        .batching(0, 0)
        .create()
        .await?;

    // ~6 MiB payload, comfortably above the broker's default 5 MiB max message
    // size, so the producer must emit at least two chunks.
    let payload_size: usize = 6 * 1024 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-chunk")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let msg = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        msg.payload.len(),
        payload_size,
        "reassembled chunked payload length mismatch"
    );
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    Ok(())
}

/// Bounded chunk reassembly (PIP-37 consumer-side hardening). A consumer with
/// the Java-matching bounds explicitly configured
/// (`max_pending_chunked_message`, `auto_ack_oldest_chunked_message_on_queue_full`,
/// `expire_time_of_incomplete_chunked_message`) must still reassemble a normal
/// oversized chunked message end-to-end — the bounds guard against unbounded
/// growth without breaking valid chunking. Pins the new builder knobs through
/// the real subscribe → `ConsumerState` seeding path against a live broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_bounded_chunk_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-chunk-bounded");

    let producer = client
        .producer(&topic)
        .chunking(true)
        .batching(0, 0)
        .create()
        .await?;

    // ~6 MiB payload (above the broker's default 5 MiB max message size).
    let payload_size: usize = 6 * 1024 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-chunk-bounded")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        // Java-matching consumer-side chunk bounds, explicitly set.
        .max_pending_chunked_message(10)
        .auto_ack_oldest_chunked_message_on_queue_full(false)
        .expire_time_of_incomplete_chunked_message(Duration::from_secs(60))
        .subscribe()
        .await?;

    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let msg = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        msg.payload.len(),
        payload_size,
        "a bounded consumer must still reassemble a valid chunked message"
    );
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    Ok(())
}

async fn publish_issue_331_backlog(
    service_url: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let publisher_client = PulsarClient::builder()
        .service_url(service_url.to_owned())
        .build()
        .await?;
    let partition_zero = format!("{topic}-partition-0");
    let chunked_producer = publisher_client
        .producer(&partition_zero)
        .chunking(true)
        .batching(0, 0)
        .create()
        .await?;
    let chunk_payload = vec![b'x'; CHUNK_PAYLOAD_SIZE];
    for _ in 0..CHUNKED_MESSAGES {
        chunked_producer
            .send(OutgoingMessage::with_payload(chunk_payload.clone()).into())
            .await?;
    }
    chunked_producer.close().await?;

    for partition in 1..PARTITIONS {
        let child_topic = format!("{topic}-partition-{partition}");
        let producer = publisher_client
            .producer(child_topic)
            .batching(100, 4_096)
            .batching_max_publish_delay(Duration::from_secs(60))
            .create()
            .await?;
        for wave in 0..(SMALL_MESSAGES / 100) {
            let send_futures: Vec<_> = (0..100)
                .map(|offset| {
                    let payload =
                        format!("partition-{partition}-message-{wave}-{offset}").into_bytes();
                    producer.send(OutgoingMessage::with_payload(payload).into())
                })
                .collect();
            for send in send_futures {
                send.await?;
            }
        }
        producer.close().await?;
    }
    publisher_client.close().await;
    Ok(())
}

async fn subscribe_issue_331_consumers(
    service_url: &str,
    topic: &str,
    subscription: &str,
) -> Result<(Vec<PulsarClient>, Vec<magnetar_runtime_tokio::Consumer>), Box<dyn std::error::Error>>
{
    let mut clients = Vec::with_capacity(PARTITIONS);
    let mut consumers = Vec::with_capacity(PARTITIONS);
    for partition in 0..PARTITIONS {
        let client = PulsarClient::builder()
            .service_url(service_url.to_owned())
            .build()
            .await?;
        let consumer = client
            .consumer(format!("{topic}-partition-{partition}"))
            .subscription(subscription)
            .subscription_type(SubType::Failover)
            .name(format!("issue-331-instance-{partition}"))
            .initial_position(InitialPosition::Earliest)
            .receiver_queue_size(RECEIVER_QUEUE_SIZE)
            .subscribe()
            .await?;
        clients.push(client);
        consumers.push(consumer);
    }
    Ok((clients, consumers))
}

fn expected_issue_331_messages(partition: usize) -> usize {
    if partition == 0 {
        CHUNKED_MESSAGES
    } else {
        SMALL_MESSAGES
    }
}

async fn receive_issue_331_partition(
    partition: usize,
    consumer: &magnetar_runtime_tokio::Consumer,
    received_count: &AtomicUsize,
) -> Result<PartitionReceive, String> {
    let expected = expected_issue_331_messages(partition);
    for received in 0..expected {
        let message = match tokio::time::timeout(Duration::from_secs(15), consumer.receive()).await
        {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return Err(format!(
                    "partition {partition} failed after {received}/{expected}: {error}"
                ));
            }
            Err(_) => {
                return Err(format!(
                    "partition {partition} timed out after {received}/{expected}"
                ));
            }
        };
        if partition == 0 && message.payload.len() != CHUNK_PAYLOAD_SIZE {
            return Err(format!(
                "partition 0 payload {received}/{expected} had length {}, expected {}",
                message.payload.len(),
                CHUNK_PAYLOAD_SIZE,
            ));
        }
        received_count.store(received + 1, Ordering::Relaxed);
    }
    Ok(PartitionReceive {
        partition,
        received: expected,
    })
}

async fn receive_issue_331_partitions(
    consumers: &[magnetar_runtime_tokio::Consumer],
    received_counts: &[AtomicUsize],
) -> Vec<Result<PartitionReceive, String>> {
    let receive_futures = consumers.iter().enumerate().map(|(partition, consumer)| {
        receive_issue_331_partition(partition, consumer, &received_counts[partition])
    });

    match tokio::time::timeout(Duration::from_secs(60), join_all(receive_futures)).await {
        Ok(results) => results,
        Err(_) => (0..PARTITIONS)
            .map(|partition| {
                let received = received_counts[partition].load(Ordering::Relaxed);
                let expected = expected_issue_331_messages(partition);
                Err(format!(
                    "outer receive timeout: partition {partition} reached {received}/{expected}"
                ))
            })
            .collect(),
    }
}

struct Issue331Diagnostics {
    receive_failures: Vec<String>,
    broker_subscriptions: Option<String>,
    local_stats: Vec<magnetar::proto::ConsumerStats>,
    counts: Vec<usize>,
}

async fn capture_issue_331_diagnostics(
    admin: &magnetar_admin::AdminClient,
    partition_zero: &str,
    receive_results: &[Result<PartitionReceive, String>],
    consumers: &[magnetar_runtime_tokio::Consumer],
    received_counts: &[AtomicUsize],
) -> Issue331Diagnostics {
    let receive_failures: Vec<String> = receive_results
        .iter()
        .filter_map(|result| result.as_ref().err().map(ToOwned::to_owned))
        .collect();
    let broker_subscriptions = if receive_failures.is_empty() {
        None
    } else {
        Some(match admin.topic_stats(partition_zero).await {
            Ok(stats) => serde_json::to_string_pretty(&stats.subscriptions)
                .unwrap_or_else(|error| format!("could not encode subscriptions: {error}")),
            Err(error) => format!("topic_stats failed: {error}"),
        })
    };
    let local_stats = consumers
        .iter()
        .map(magnetar_runtime_tokio::Consumer::stats)
        .collect();
    let counts = received_counts
        .iter()
        .map(|count| count.load(Ordering::Relaxed))
        .collect();
    Issue331Diagnostics {
        receive_failures,
        broker_subscriptions,
        local_stats,
        counts,
    }
}

async fn close_issue_331_consumers(
    consumers: Vec<magnetar_runtime_tokio::Consumer>,
    clients: Vec<PulsarClient>,
) -> Vec<String> {
    let mut close_errors = Vec::new();
    for (partition, consumer) in consumers.into_iter().enumerate() {
        if let Err(error) = consumer.close().await {
            close_errors.push(format!("consumer {partition}: {error}"));
        }
    }
    for client in clients {
        client.close().await;
    }
    close_errors
}

/// Issue #331: twelve direct partition consumers share one Failover
/// subscription. Partition zero carries five-chunk logical messages whose
/// accepted intermediate chunks must replenish broker flow immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_331_chunked_failover_partition_replenishes_flow()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_small_message_pulsar().await?;
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let topic = unique_topic("magnetar-e2e-331-chunk-flow");
    admin
        .topic_create_partitioned(&topic, PARTITIONS as u32)
        .await?;

    // Publish the complete backlog before subscribing, matching the reported
    // application startup against already-persisted partition data.
    publish_issue_331_backlog(&service_url, &topic).await?;

    let subscription = format!("issue-331-{}", uuid::Uuid::new_v4().simple());
    let (clients, consumers) =
        subscribe_issue_331_consumers(&service_url, &topic, &subscription).await?;
    let received_counts: Vec<AtomicUsize> = (0..PARTITIONS).map(|_| AtomicUsize::new(0)).collect();
    let receive_results = receive_issue_331_partitions(&consumers, &received_counts).await;
    let partition_zero = format!("{topic}-partition-0");
    let diagnostics = capture_issue_331_diagnostics(
        &admin,
        &partition_zero,
        &receive_results,
        &consumers,
        &received_counts,
    )
    .await;
    let close_errors = close_issue_331_consumers(consumers, clients).await;

    assert!(
        diagnostics.receive_failures.is_empty(),
        "issue #331 receive failure: {:?}; counts={:?}; partition-0 subscriptions={:#?}; \
         local_stats={:#?}; close_errors={close_errors:?}",
        diagnostics.receive_failures,
        diagnostics.counts,
        diagnostics.broker_subscriptions,
        diagnostics.local_stats,
    );
    assert!(close_errors.is_empty(), "close failures: {close_errors:?}");

    for result in receive_results {
        let result = result.expect("receive failures returned above");
        assert_eq!(
            result.received,
            expected_issue_331_messages(result.partition)
        );
    }
    assert_eq!(
        diagnostics.local_stats[0].total_chunked_msgs_received, CHUNKED_MESSAGES as u64,
        "partition zero must reassemble every five-chunk message",
    );

    Ok(())
}
