// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for issue #860 against a real Apache Pulsar 4.x standalone broker:
//! COMPRESSED batched entries crossing the Java ↔ magnetar boundary in both directions.
//!
//! ## The production failure
//!
//! A magnetar `Reader` with `receiver_queue_size(1000)` over a topic a Java producer filled with
//! compressed batches delivered nothing and stalled for good, with nothing logged; the broker
//! reported the reader's `availablePermits` at `-1000`. Uncompressed batches streamed fine.
//! The mirror fault ran the other way: Java `pulsar-client consume` read 0/100 of a topic
//! magnetar had produced with LZ4 and batching, while LZ4 without batching and batching without
//! compression both read 100/100.
//!
//! ## Why
//!
//! A Java producer compresses the WHOLE packed batch body
//! (`BatchMessageContainerImpl.getCompressedBatchMetadataAndPayload`) and a Java consumer
//! decompresses it before it splits it into members (`ConsumerImpl.uncompressPayloadIfNeeded`,
//! then `receiveIndividualMessagesFromBatch`). magnetar split first, so it read compressed bytes
//! as member sizes, surfaced nothing and never refunded the `numMessagesInBatch` permits the
//! broker charged; and magnetar's producer compressed each member on its own and sent the
//! concatenation raw under a batch-level codec stamp, which no Java consumer can decompress.
//! [ADR-0112](../../../specs/adr/0112-compress-and-decompress-a-batch-as-one-body.md) moves both
//! sides to the Java layout.
//!
//! ## What this pins that the lower layers cannot
//!
//! The proto, both runtimes and the differential harness pin the mechanism against bodies packed
//! and compressed by hand. Only a real broker and the real Java tools can prove the two layouts
//! actually agree: `pulsar-perf` and `pulsar-client` both run INSIDE the broker container, so
//! these are the bytes the Java client really writes and the decoder the Java client really runs.
//!
//! 1. **Java → magnetar.** `pulsar-perf produce -bm 20 -z LZ4` fills the topic; a magnetar `Reader`
//!    from the earliest position must read every message, and the broker must still report a
//!    positive `availablePermits` for it afterwards — the client kept replenishing.
//! 2. **magnetar → Java.** A magnetar producer with LZ4 and batching fills the topic;
//!    `pulsar-client consume` must read every message.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_compressed_batch_interop -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::InitialPosition;
use magnetar::proto::types::CompressionKind;
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_admin::{AdminClient, TopicStats};
use testcontainers::core::{CmdWaitFor, ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.2.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container. See
/// `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

/// Messages `pulsar-perf` publishes. Fifty LZ4 batches of twenty — far past the half-queue
/// refill point of a 1000-permit reader, which is where the issue #860 stall set in.
const JAVA_MESSAGES: usize = 1000;

/// The unit every `pulsar-perf` payload repeats — compressible on purpose (see the test).
const JAVA_PAYLOAD_UNIT: &str = "magnetar-860|";

/// Repeats of [`JAVA_PAYLOAD_UNIT`] per payload.
const JAVA_PAYLOAD_REPEATS: usize = 80;

/// Size of every `pulsar-perf` payload, so a member decoded at the wrong length is visible.
const JAVA_MESSAGE_SIZE: usize = JAVA_PAYLOAD_UNIT.len() * JAVA_PAYLOAD_REPEATS;

/// The reader's receiver queue — the issue #860 reproduction's own setting.
const READER_QUEUE_SIZE: usize = 1000;

/// Messages magnetar publishes for the Java consumer: five batches of [`MAGNETAR_BATCH`].
const MAGNETAR_MESSAGES: usize = 100;
const MAGNETAR_BATCH: usize = 20;

/// Per-read patience. A healthy run reads everything in well under a second; the stalled
/// reader of issue #860 never reads anything, so this bounds how long a regression takes to
/// fail rather than how long a pass takes.
const READ_TIMEOUT: Duration = Duration::from_secs(15);

/// Wall-clock cap on the in-container Java consumer (`timeout <secs>`). A consumer that cannot
/// decode a batch discards it and waits forever for messages that will never come.
const JAVA_CONSUME_TIMEOUT_SECS: &str = "90";

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

/// Start a Pulsar 4.x standalone container and return (`service_url`, `admin_url`, `container`).
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
        .with_startup_timeout(Duration::from_mins(3))
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

/// Run a Java CLI inside the broker container, wait for it to exit, and return
/// (`exit_code`, `stdout`, `stderr`). The exit code is read only once the process has exited
/// (`CmdWaitFor::exit()`); before that testcontainers is free to answer `None`.
async fn run_in_container(
    container: &testcontainers::ContainerAsync<GenericImage>,
    command: &[&str],
) -> Result<(Option<i64>, String, String), Box<dyn std::error::Error>> {
    let mut out = container
        .exec(
            ExecCommand::new(command.iter().map(|arg| (*arg).to_owned()))
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;
    let stdout = String::from_utf8_lossy(&out.stdout_to_vec().await?).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr_to_vec().await?).into_owned();
    Ok((out.exit_code().await?, stdout, stderr))
}

/// The broker's own `availablePermits` for every consumer of every subscription on the topic.
fn broker_available_permits(stats: &TopicStats) -> Vec<i64> {
    stats
        .subscriptions
        .as_object()
        .into_iter()
        .flat_map(serde_json::Map::values)
        .filter_map(|sub| sub.get("consumers").and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|consumer| {
            consumer
                .get("availablePermits")
                .and_then(serde_json::Value::as_i64)
        })
        .collect()
}

/// Java → magnetar: a magnetar `Reader` reads every message of Java's LZ4-compressed batches.
///
/// Before ADR-0112 the reader surfaced none of them: the first batch arrived, its members were
/// split out of the COMPRESSED bytes, nothing parsed, nothing was queued, and with nothing to
/// pop the reader never replenished a permit — the broker reported `availablePermits = -1000`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_reader_reads_java_lz4_batches() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, container) = start_pulsar().await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-860-java-{suffix}");

    // ~1 KiB payloads, written into the container first. Measured 2026-10-08: on the 4.2.4
    // image, twenty-message batches of 130-byte payloads arrived with NO codec stamp although
    // pulsar-perf logged `"compression" : "LZ4"`; on 4.0.4, batches of `-s 128` payloads did
    // arrive compressed. Small batches would therefore turn this into a test of the
    // uncompressed path, which always worked; the per-message assertion below keeps that from
    // happening silently.
    let produce = format!(
        "printf '%s' '{payload}' > /tmp/payload-860.txt && \
         bin/pulsar-perf produce -bm 20 -b 1000 -z LZ4 -m {JAVA_MESSAGES} -r 5000 \
         -f /tmp/payload-860.txt {topic}",
        payload = JAVA_PAYLOAD_UNIT.repeat(JAVA_PAYLOAD_REPEATS),
    );
    let (code, stdout, stderr) = run_in_container(&container, &["sh", "-c", &produce]).await?;
    assert_eq!(
        code,
        Some(0),
        "pulsar-perf produce must succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let reader = client
        .reader(topic.clone())
        .receiver_queue_size(READER_QUEUE_SIZE)
        .start_position(InitialPosition::Earliest)
        .create()
        .await?;
    let mut read = 0usize;
    while read < JAVA_MESSAGES {
        let Some(message) = reader.read_next_with_timeout(READ_TIMEOUT).await? else {
            break;
        };
        assert_eq!(
            message.payload.len(),
            JAVA_MESSAGE_SIZE,
            "every member must surface decompressed, at the size pulsar-perf wrote"
        );
        assert_eq!(
            message.message_id.batch_size, 20,
            "pulsar-perf must really have batched — an unbatched message proves nothing here"
        );
        assert!(
            message.metadata.uncompressed_size.is_some(),
            "the batch must really have been compressed — Java stamps `uncompressed_size` only \
             on a compressed batch, and an uncompressed one proves nothing here"
        );
        read += 1;
    }
    assert_eq!(
        read, JAVA_MESSAGES,
        "a magnetar Reader must read every message of Java's LZ4 batches (issue #860)"
    );

    // The broker's view: the reader kept replenishing, so its permit balance is positive.
    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let permits = broker_available_permits(&admin.topic_stats(&topic).await?);
    assert!(
        !permits.is_empty() && permits.iter().all(|p| *p > 0),
        "the broker must report a positive availablePermits for the reader, got {permits:?}"
    );
    Ok(())
}

/// magnetar → Java: the in-container Java consumer reads every message magnetar produced with
/// LZ4 and batching.
///
/// Before ADR-0112 it read none: magnetar compressed each member on its own and sent the
/// concatenation raw under the batch-level LZ4 stamp, so the Java consumer's whole-body
/// `Lz4RawDecompressor` call failed on every entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_java_consumer_reads_magnetar_lz4_batches() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-860-magnetar-{suffix}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let producer = client
        .producer(topic.clone())
        .compression(CompressionKind::Lz4)
        .batching(MAGNETAR_BATCH, 1_000_000)
        .batching_max_publish_delay(Duration::from_mins(1))
        .create()
        .await?;
    // Enqueue every send before awaiting any, so the message-count cap is the only thing that
    // flushes and every entry is a full batch (the `e2e_batch_ack_timeout_shared` pattern).
    let sends: Vec<_> = (0..MAGNETAR_MESSAGES)
        .map(|i| {
            producer.send(
                OutgoingMessage::with_payload(format!("magnetar-860-{i}|").repeat(8).into_bytes())
                    .into(),
            )
        })
        .collect();
    for send in sends {
        send.await?;
    }
    producer.close().await?;

    let count = MAGNETAR_MESSAGES.to_string();
    let (code, stdout, stderr) = run_in_container(
        &container,
        &[
            "timeout",
            JAVA_CONSUME_TIMEOUT_SECS,
            "bin/pulsar-client",
            "consume",
            "-s",
            "magnetar-e2e-860",
            "-p",
            "Earliest",
            "-n",
            &count,
            &topic,
        ],
    )
    .await?;
    let consumed = stdout.matches("----- got message -----").count();
    assert_eq!(
        (code, consumed),
        (Some(0), MAGNETAR_MESSAGES),
        "Java pulsar-client must read every message of magnetar's LZ4 batches (issue #860)\n\
         stdout tail:\n{}\nstderr tail:\n{}",
        tail(&stdout),
        tail(&stderr)
    );
    Ok(())
}

/// The last few lines of a CLI transcript, for a readable failure message.
fn tail(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}

/// A magnetar consumer over `topic` from the earliest position, reading `count` payloads.
async fn read_payloads(
    client: &PulsarClient,
    topic: &str,
    count: usize,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-860-reader")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = tokio::time::timeout(READ_TIMEOUT, consumer.receive()).await??;
        payloads.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    Ok(payloads)
}

/// magnetar → magnetar: a ONE-message batch — what a batching producer flushes when the publish
/// delay fires on a single send — reaches the consumer unframed, compressed or not.
///
/// `flush_batch` stamps `num_messages_in_batch = Some(1)`; Java reads that as a batch of one,
/// but magnetar read it as a plain message and handed the `[u32][SingleMessageMetadata]` framing
/// to the application as payload — silently, and with LZ4 too since the batch body is now
/// compressed as one block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_one_message_batch_reaches_the_consumer_unframed()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    for codec in [CompressionKind::None, CompressionKind::Lz4] {
        let suffix = Uuid::new_v4().simple().to_string();
        let topic = format!("persistent://public/default/magnetar-e2e-860-one-{suffix}");
        let producer = client
            .producer(topic.clone())
            .compression(codec)
            .batching(MAGNETAR_BATCH, 1_000_000)
            .batching_max_publish_delay(Duration::from_millis(50))
            .create()
            .await?;
        let payload = format!("lonely-{codec:?}|").repeat(16).into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
        producer.close().await?;
        assert_eq!(
            read_payloads(&client, &topic, 1).await?,
            vec![payload],
            "{codec:?}: the member, without its batch framing"
        );
    }
    client.close().await;
    Ok(())
}

/// A send that does not fit the pending batch must not overtake it on the wire: the consumer
/// reads the payloads in send order. Before the fix the overflowing send went out on its own
/// while the batch waited for its publish delay, so the broker stored it first — and with broker
/// deduplication on, the batch's lower sequence ids would then have been dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_overflowing_send_keeps_send_order() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-860-order-{suffix}");
    let producer = client
        .producer(topic.clone())
        .compression(CompressionKind::Lz4)
        .batching(MAGNETAR_BATCH, 1000)
        .batching_max_publish_delay(Duration::from_millis(200))
        .create()
        .await?;
    let mut payloads: Vec<Vec<u8>> = (0..3u8).map(|i| vec![b'a' + i; 400]).collect();
    payloads.push(vec![b'z'; 1200]);
    let sends: Vec<_> = payloads
        .iter()
        .map(|p| producer.send(OutgoingMessage::with_payload(p.clone()).into()))
        .collect();
    for send in sends {
        send.await?;
    }
    producer.close().await?;
    let read = read_payloads(&client, &topic, payloads.len()).await?;
    assert_eq!(
        read.iter().map(|p| (p[0], p.len())).collect::<Vec<_>>(),
        payloads.iter().map(|p| (p[0], p.len())).collect::<Vec<_>>(),
        "the consumer must read the payloads in send order"
    );
    client.close().await;
    Ok(())
}
