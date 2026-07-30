// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for issue #347 (`aggregate_stats` zeroes fields)
//! against a real Apache Pulsar broker. ADR-0024 layer (e).
//!
//! Two scenarios:
//!
//! 1. [`e2e_partitioned_aggregate_stats_propagates_totals_and_batch_acks`] — a 2-partition topic,
//!    partitioned producer + partitioned consumer: send a batched round, receive it, and assert
//!    `aggregate_stats()` totals equal the real send/receive counts, `pending_batch_acks` is
//!    propagated (nonzero before any ack — the issue's headline symptom), and the latency
//!    percentile ordering (`max >= p99 >= p50`) holds. The rolling `msgs_per_sec` / `bytes_per_sec`
//!    rates are NOT asserted here: `PartitionedProducer` / `PartitionedConsumer` expose no way to
//!    drive `record_rate_window` on their per-partition children through the public façade (only
//!    the concrete `magnetar_runtime_tokio::{Producer, Consumer}` types expose that method), so a
//!    real partitioned child's rate fields can never become nonzero today — see scenario 2 for the
//!    rate-propagation proof and the report's "open concerns" for this reachability gap as a
//!    follow-up candidate.
//! 2. [`e2e_aggregate_stats_fold_propagates_real_rate`] — a single (non-partitioned) producer +
//!    consumer pair, driven exactly like `e2e_rolling_stats.rs`, proves `ConsumerStats::fold` /
//!    `ProducerStats::fold` propagate a REAL nonzero rolling rate (and every other field)
//!    end-to-end against a live broker.
//! 3. [`e2e_receive_latency_reflects_real_queue_dwell`] — ADR-0086 layer (e): the injected clock
//!    the state machine now stamps latency against must be a LIVE clock, not a pinned constant.
//!    Every in-process layer of the ADR-0086 test set compares against a *scripted* delta, so a fix
//!    that pinned `now` (e.g. `pop_message(msg.arrived_at)`) would zero the histogram forever and
//!    still pass all of them. Only a real broker with a real queue dwell catches that.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker.

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::{ConsumerStats, ProducerStats};
use magnetar::{MessageRoutingMode, OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const PARTITIONS: u32 = 2;

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

fn fresh_topic(suffix: &str) -> String {
    format!(
        "persistent://public/default/magnetar-e2e-{}-{}",
        suffix,
        uuid::Uuid::new_v4().simple()
    )
}

async fn create_partitioned_topic(
    admin_url: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    admin.topic_create_partitioned(topic, PARTITIONS).await?;
    Ok(())
}

/// Scenario 1: partitioned producer + partitioned consumer, batched send.
/// Asserts `aggregate_stats()` totals, `pending_batch_acks` propagation
/// (the issue's headline symptom), and percentile ordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_partitioned_aggregate_stats_propagates_totals_and_batch_acks()
-> Result<(), Box<dyn std::error::Error>> {
    const TOTAL_MSGS: usize = 6;

    let (service_url, admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("aggregate-stats-partitioned");
    create_partitioned_topic(&admin_url, &topic).await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // A long publish delay + a high message cap means NOTHING auto-flushes
    // before the explicit `.flush()` below — every message sent in this
    // round stays buffered in its partition's batch container until then,
    // so each partition emits exactly one BATCHED broker entry (not one
    // per message) and stamps exactly one `batch_ack_tracker` entry.
    let producer = client
        .partitioned_producer(topic.clone())
        .routing(MessageRoutingMode::RoundRobin)
        .batching(50, 10 * 1024 * 1024)
        .batching_max_publish_delay(Duration::from_secs(30))
        .create()
        .await?;

    let consumer = client
        .partitioned_consumer(topic.clone())
        .subscription("magnetar-e2e-aggregate-stats-partitioned")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // `PartitionedProducer::send` is a lazy `async fn` (unlike the eager,
    // synchronously-enqueuing `magnetar_runtime_tokio::Producer::send`), so
    // awaiting each send sequentially would enqueue exactly one message,
    // then block forever on a receipt for a batch that's never big enough
    // to auto-flush and never explicitly flushed. Fire every send
    // concurrently (`join_all`) and flush once they've all had a chance to
    // enqueue, mirroring `e2e_batch_chunk.rs`'s "fire all, then join"
    // pattern adapted for a lazy per-call future.
    let sends = async {
        let futures: Vec<_> = (0..TOTAL_MSGS)
            .map(|i| {
                producer.send(OutgoingMessage::with_payload(
                    format!("agg-stats-{i}").into_bytes(),
                ))
            })
            .collect();
        join_all(futures).await
    };
    let flush_once_enqueued = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        producer.flush().await
    };
    let (send_results, flush_result) = tokio::join!(sends, flush_once_enqueued);
    flush_result?;
    for r in send_results {
        r?;
    }

    let mut received = Vec::with_capacity(TOTAL_MSGS);
    while received.len() < TOTAL_MSGS {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("timeout: got {} of {TOTAL_MSGS}", received.len()))??;
        received.push(msg);
    }

    // Snapshot BEFORE acking: every received message is still un-acked, so
    // both partitions' batch-ack tracker entries are still live. This is
    // the issue's headline symptom — pre-fix, `pending_batch_acks` read 0
    // here regardless of how many entries the children actually tracked.
    let pre_ack_stats = consumer.aggregate_stats();
    assert!(
        pre_ack_stats.pending_batch_acks > 0,
        "aggregate_stats().pending_batch_acks must propagate the children's \
         live batch-ack tracker entries, got {} (stats={pre_ack_stats:?})",
        pre_ack_stats.pending_batch_acks
    );

    for msg in &received {
        consumer.ack(&msg.topic, msg.message.message_id).await?;
    }

    let producer_stats = producer.aggregate_stats();
    let consumer_stats = consumer.aggregate_stats();

    producer.close().await?;
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        producer_stats.total_msgs_sent, TOTAL_MSGS as u64,
        "producer aggregate totals must equal the real send count"
    );
    assert_eq!(
        consumer_stats.total_msgs_received, TOTAL_MSGS as u64,
        "consumer aggregate totals must equal the real receive count"
    );
    assert_eq!(
        consumer_stats.total_acks_sent, TOTAL_MSGS as u64,
        "consumer aggregate ack total must equal the real ack count"
    );

    // Percentile ordering: wherever a percentile is nonzero, max >= p99 >=
    // p50 must hold (both engines compute these from the SAME merged
    // histogram, so this is a structural invariant, not a timing guess).
    if consumer_stats.receive_latency_max_ms > 0 {
        assert!(
            consumer_stats.receive_latency_max_ms >= consumer_stats.receive_latency_p99_ms,
            "max must be >= p99, got {consumer_stats:?}"
        );
        assert!(
            consumer_stats.receive_latency_p99_ms >= consumer_stats.receive_latency_p50_ms,
            "p99 must be >= p50, got {consumer_stats:?}"
        );
    }
    if producer_stats.send_latency_max_ms > 0 {
        assert!(
            producer_stats.send_latency_max_ms >= producer_stats.send_latency_p99_ms,
            "max must be >= p99, got {producer_stats:?}"
        );
        assert!(
            producer_stats.send_latency_p99_ms >= producer_stats.send_latency_p50_ms,
            "p99 must be >= p50, got {producer_stats:?}"
        );
    }

    Ok(())
}

/// Scenario 2: a single (non-partitioned) producer + consumer pair, ticked
/// through `record_rate_window` exactly like `e2e_rolling_stats.rs`, proves
/// `ConsumerStats::fold` / `ProducerStats::fold` propagate a REAL nonzero
/// rolling rate (and every other field) end-to-end. `fold` over a
/// single-element iterator must reproduce that element's own snapshot
/// exactly — `saturating_add`/`f64 +=`/`max` against a `default()` (zeroed)
/// accumulator is the identity operation, and the "merged" histogram is
/// just the one child's histogram, so its percentiles recompute to the same
/// values `stats()` already reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::float_cmp,
    reason = "fold over a single-element iterator is the f64 += identity \
              operation (0.0 + x == x exactly under IEEE754) against the \
              same snapshot taken moments earlier — bit-exact equality is \
              the actual invariant under test here, not an approximation"
)]
async fn e2e_aggregate_stats_fold_propagates_real_rate() -> Result<(), Box<dyn std::error::Error>> {
    const TOTAL_MSGS: usize = 20;

    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("aggregate-stats-fold-rate");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-aggregate-stats-fold-rate")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Baseline rate-window snapshot (first call only seeds — rates stay
    // 0.0 until the second call, mirroring e2e_rolling_stats.rs).
    producer.record_rate_window(Instant::now());
    consumer.record_rate_window(Instant::now());

    for i in 0..TOTAL_MSGS {
        let payload = format!("fold-rate-msg-{i:04}-padding-XXXXXXXXXXXXXXXXXXXX").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
        let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
            .await
            .expect("consumer.receive timeout")
            .expect("consumer.receive error");
        consumer.ack(msg.message_id).await.ok();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    producer.record_rate_window(Instant::now());
    consumer.record_rate_window(Instant::now());

    let producer_snapshot = producer.stats();
    let producer_hist = producer.send_latency_histogram();
    let consumer_snapshot = consumer.stats();
    let consumer_hist = consumer.receive_latency_histogram();

    let folded_producer = ProducerStats::fold([(producer_snapshot, producer_hist)]);
    let folded_consumer = ConsumerStats::fold([(consumer_snapshot, consumer_hist)]);

    producer.close().await?;
    consumer.close().await?;
    client.close().await;

    assert!(
        producer_snapshot.msgs_per_sec > 0.0,
        "expected a real nonzero producer rate to fold, got {producer_snapshot:?}"
    );
    assert!(
        consumer_snapshot.msgs_per_sec > 0.0,
        "expected a real nonzero consumer rate to fold, got {consumer_snapshot:?}"
    );

    assert_eq!(
        folded_producer.total_msgs_sent,
        producer_snapshot.total_msgs_sent
    );
    assert_eq!(
        folded_producer.total_bytes_sent,
        producer_snapshot.total_bytes_sent
    );
    assert_eq!(folded_producer.msgs_per_sec, producer_snapshot.msgs_per_sec);
    assert_eq!(
        folded_producer.bytes_per_sec,
        producer_snapshot.bytes_per_sec
    );
    assert_eq!(
        folded_producer.send_latency_max_ms,
        producer_snapshot.send_latency_max_ms
    );
    assert_eq!(
        folded_producer.send_latency_p50_ms,
        producer_snapshot.send_latency_p50_ms
    );
    assert_eq!(
        folded_producer.send_latency_p99_ms,
        producer_snapshot.send_latency_p99_ms
    );

    assert_eq!(
        folded_consumer.total_msgs_received,
        consumer_snapshot.total_msgs_received
    );
    assert_eq!(
        folded_consumer.total_bytes_received,
        consumer_snapshot.total_bytes_received
    );
    assert_eq!(folded_consumer.msgs_per_sec, consumer_snapshot.msgs_per_sec);
    assert_eq!(
        folded_consumer.bytes_per_sec,
        consumer_snapshot.bytes_per_sec
    );
    assert_eq!(
        folded_consumer.receive_latency_max_ms,
        consumer_snapshot.receive_latency_max_ms
    );
    assert_eq!(
        folded_consumer.receive_latency_p50_ms,
        consumer_snapshot.receive_latency_p50_ms
    );
    assert_eq!(
        folded_consumer.receive_latency_p99_ms,
        consumer_snapshot.receive_latency_p99_ms
    );

    Ok(())
}

/// Scenario 3 (ADR-0086, ADR-0024 layer (e)): the receive-latency histogram must reflect REAL
/// queue-dwell time against a live broker.
///
/// Every in-process layer of the ADR-0086 test set compares the recorded sample against a
/// *scripted* delta, so none of them can catch a fix that pins `now` to a constant — e.g.
/// `pop_message(msg.arrived_at)`, or an engine snapshotting `now` once at subscribe time. Both
/// silently zero the histogram forever. Publishing, letting the messages sit unread in the
/// receiver queue, then draining is the only place that shows up.
///
/// This test is a characterization test, not a regression test: the fix is deliberately
/// production-neutral on tokio (which passes a host `Instant::now()` at the call boundary either
/// way), so it passed before ADR-0086 too. It is red against the plausible WRONG fixes named
/// above, which is why it earns its container.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_receive_latency_reflects_real_queue_dwell() -> Result<(), Box<dyn std::error::Error>> {
    const TOTAL_MSGS: usize = 8;
    /// How long the messages sit in the receiver queue before anyone calls `receive()`.
    const DWELL: Duration = Duration::from_millis(1_500);
    /// Slack below `DWELL` so broker push latency and scheduling jitter cannot flake the test;
    /// a pinned/zeroed clock reports 0 and misses this by three orders of magnitude.
    const DWELL_SLACK_MS: u64 = 200;
    /// Generous upper bound: catches a `u64::MAX` saturation and a ms/µs/ns unit confusion
    /// without asserting anything about host scheduling speed.
    const SANE_UPPER_MS: u64 = 60_000;

    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let topic = fresh_topic("receive-latency-dwell");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-receive-latency-dwell")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    for i in 0..TOTAL_MSGS {
        let payload = format!("dwell-msg-{i:04}").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
    }
    producer.flush().await?;

    // Let the broker push the batch and the messages SIT in the receiver queue: this is the
    // dwell the histogram must observe.
    tokio::time::sleep(DWELL).await;

    for _ in 0..TOTAL_MSGS {
        let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
            .await
            .expect("consumer.receive timeout")
            .expect("consumer.receive error");
        consumer.ack(msg.message_id).await.ok();
    }

    let consumer_snapshot = consumer.stats();
    let consumer_hist = consumer
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    let producer_snapshot = producer.stats();

    producer.close().await?;
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        consumer_hist.len(),
        TOTAL_MSGS as u64,
        "one receive_latency_hist sample per received message"
    );
    // The load-bearing assertion: a pinned or constant `now` cannot produce this.
    assert!(
        consumer_snapshot.receive_latency_max_ms >= DWELL.as_millis() as u64 - DWELL_SLACK_MS,
        "a real queue dwell of {DWELL:?} must be visible in receive_latency_max_ms, got {} \
         (a pinned/constant `now` would report 0)",
        consumer_snapshot.receive_latency_max_ms
    );
    assert!(
        consumer_snapshot.receive_latency_max_ms < SANE_UPPER_MS,
        "receive_latency_max_ms={} is not a plausible millisecond value",
        consumer_snapshot.receive_latency_max_ms
    );
    assert!(consumer_snapshot.receive_latency_max_ms >= consumer_snapshot.receive_latency_p99_ms);
    assert!(consumer_snapshot.receive_latency_p99_ms >= consumer_snapshot.receive_latency_p50_ms);
    // The producer leg of the same fix: `apply_receipt` stamps `now - enqueued_at` from the
    // instant `handle_frame` was given, so a real broker round-trip must land in a plausible
    // millisecond range too.
    assert!(
        producer_snapshot.send_latency_max_ms < SANE_UPPER_MS,
        "send_latency_max_ms={} is not a plausible millisecond value",
        producer_snapshot.send_latency_max_ms
    );

    Ok(())
}
