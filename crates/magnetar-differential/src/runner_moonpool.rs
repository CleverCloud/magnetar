// SPDX-License-Identifier: Apache-2.0

//! moonpool-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_moonpool::Client`] with
//! [`moonpool_core::TokioProviders`] and returns the resulting
//! [`EventStream`].
//!
//! When `moonpool-sim`'s provider bundle becomes a workspace dep,
//! plug it in here as a sibling `run_with_sim_providers` entry point
//! that takes a seed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, MessageId, SubscribeRequest};
use magnetar_runtime_moonpool::{
    Client, ClientError, ConnectionShared, Consumer, MoonpoolEngine, Producer,
};
use moonpool_core::TokioProviders;

use crate::trace::{Event, EventStream, Op, Trace};

/// Build the per-partition topic name for a given base topic.
/// Mirrors Java `PartitionedProducerImpl`'s topic-naming convention.
fn partition_topic(base: &str, partition: i32) -> String {
    format!("{base}-partition-{partition}")
}

/// Frequency at which the `LocalSet` pump pulses `driver_waker.notify_one()`.
/// Retained after the sans-io waker slab refactor: the moonpool driver is
/// `spawn_local`'d into a [`tokio::task::LocalSet`] (required by
/// [`moonpool_core::TokioProviders`]), and the outer test task and driver
/// task only see each other's wakeups when the `LocalSet` itself is polled.
/// The `Recv` future's waker now fires via
/// `magnetar_proto::consumer::ConsumerState::wake_receivers` on delivery,
/// but the fire originates from the driver task — which never runs unless
/// the `LocalSet` is pumped. This 25 ms tick keeps the `LocalSet` alive
/// while the outer task is parked on a `consumer.receive()`. Removing it
/// is tracked in `docs/follow-ups.md` (`Moonpool` runner `LocalSet` pump).
const KICKER_INTERVAL: Duration = Duration::from_millis(25);

/// Periodic `LocalSet` pump. Drop the returned handle to stop it.
struct Kicker {
    handle: tokio::task::JoinHandle<()>,
}

impl Kicker {
    fn spawn(shared: Arc<ConnectionShared>) -> Self {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(KICKER_INTERVAL).await;
                shared.driver_waker.notify_one();
            }
        });
        Self { handle }
    }
}

impl Drop for Kicker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Run `trace` against the moonpool engine talking to `host_port`
/// (e.g. `127.0.0.1:7654`). Note: the moonpool engine takes a bare
/// `host:port` string, NOT a `pulsar://` URL.
///
/// Internally wraps the engine work in a [`tokio::task::LocalSet`]
/// because moonpool's [`TokioProviders`] task provider uses
/// `tokio::task::spawn_local` to remain compatible with `moonpool-sim`'s
/// single-thread simulator. Differential tests run on
/// `flavor = "current_thread"` runtimes which do **not** ship a
/// pre-installed `LocalSet`, so the wrapper is required to keep the
/// driver task alive.
///
/// # Errors
/// Returns the last engine-level error if the initial connect /
/// producer / consumer open fails.
pub async fn run(host_port: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let local = tokio::task::LocalSet::new();
    local.run_until(run_inner(host_port, trace)).await
}

async fn run_inner(host_port: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default()).await?;
    let _kicker = Kicker::spawn(client.shared().clone());

    let producer = client
        .open_producer(CreateProducerRequest {
            topic: trace.topic.clone(),
            ..Default::default()
        })
        .await?;

    let mut consumer: Option<Consumer<TokioProviders>> = None;

    // Per-partition producers + consumers, opened lazily on first
    // SendPartition / RecvPartition / AckPartition / SeekPartition op
    // targeting that partition. See `runner_tokio.rs` for the rationale.
    let mut part_producers: HashMap<i32, Producer<TokioProviders>> = HashMap::new();
    let mut part_consumers: HashMap<i32, Consumer<TokioProviders>> = HashMap::new();

    for op in &trace.ops {
        match op {
            Op::Send { payload } => {
                let bytes = Bytes::from(payload.clone());
                let event = run_send(&producer, bytes).await;
                stream.push(event);
            }
            Op::SendWithSourceId {
                source_msg_id,
                payload,
            } => {
                let bytes = Bytes::from(payload.clone());
                let event = run_send_with_source_id(&producer, *source_msg_id, bytes).await;
                stream.push(event);
            }
            Op::Recv { timeout } => {
                match ensure_consumer(&client, &mut consumer, &trace.topic, &trace.subscription)
                    .await
                {
                    Ok(c) => stream.push(run_recv(c, *timeout).await),
                    Err(_) => stream.push(Event::RecvTimeout),
                }
            }
            Op::Ack { message_id } => {
                match ensure_consumer(&client, &mut consumer, &trace.topic, &trace.subscription)
                    .await
                {
                    Ok(c) => stream.push(run_ack(c, *message_id).await),
                    Err(_) => stream.push(Event::AckError {
                        kind: "consumer-open-failed".to_owned(),
                    }),
                }
            }
            Op::Nack { message_id } => {
                match ensure_consumer(&client, &mut consumer, &trace.topic, &trace.subscription)
                    .await
                {
                    Ok(c) => {
                        c.negative_ack(*message_id);
                        stream.push(Event::Nacked);
                    }
                    Err(_) => stream.push(Event::Nacked),
                }
            }
            Op::Seek { message_id } => {
                match ensure_consumer(&client, &mut consumer, &trace.topic, &trace.subscription)
                    .await
                {
                    Ok(c) => stream.push(run_seek(c, *message_id).await),
                    Err(_) => stream.push(Event::SeekError {
                        kind: "consumer-open-failed".to_owned(),
                    }),
                }
            }
            Op::SendPartition { partition, payload } => {
                let topic = partition_topic(&trace.topic, *partition);
                match ensure_part_producer(&client, &mut part_producers, *partition, &topic).await {
                    Ok(p) => {
                        let bytes = Bytes::from(payload.clone());
                        stream.push(run_send_partition(p, *partition, bytes).await);
                    }
                    Err(e) => stream.push(Event::SendError { kind: classify(&e) }),
                }
            }
            Op::RecvPartition { partition, timeout } => {
                let topic = partition_topic(&trace.topic, *partition);
                match ensure_part_consumer(
                    &client,
                    &mut part_consumers,
                    *partition,
                    &topic,
                    &trace.subscription,
                )
                .await
                {
                    Ok(c) => stream.push(run_recv_partition(c, *partition, *timeout).await),
                    Err(_) => stream.push(Event::RecvTimeoutPartition {
                        partition: *partition,
                    }),
                }
            }
            Op::AckPartition {
                partition,
                message_id,
            } => {
                let topic = partition_topic(&trace.topic, *partition);
                match ensure_part_consumer(
                    &client,
                    &mut part_consumers,
                    *partition,
                    &topic,
                    &trace.subscription,
                )
                .await
                {
                    Ok(c) => stream.push(run_ack_partition(c, *partition, *message_id).await),
                    Err(_) => stream.push(Event::AckError {
                        kind: "consumer-open-failed".to_owned(),
                    }),
                }
            }
            Op::SeekPartition {
                partition,
                message_id,
            } => {
                let topic = partition_topic(&trace.topic, *partition);
                match ensure_part_consumer(
                    &client,
                    &mut part_consumers,
                    *partition,
                    &topic,
                    &trace.subscription,
                )
                .await
                {
                    Ok(c) => stream.push(run_seek_partition(c, *partition, *message_id).await),
                    Err(_) => stream.push(Event::SeekError {
                        kind: "consumer-open-failed".to_owned(),
                    }),
                }
            }
            Op::Close => {
                if let Some(c) = consumer.take() {
                    let _ = c.close().await;
                }
                let _ = producer.clone().close().await;
                for (_, c) in part_consumers.drain() {
                    let _ = c.close().await;
                }
                for (_, p) in part_producers.drain() {
                    let _ = p.close().await;
                }
                stream.push(Event::Closed);
                client.close().await;
                return Ok(stream);
            }
        }
    }

    if let Some(c) = consumer.take() {
        let _ = c.close().await;
    }
    for (_, c) in part_consumers.drain() {
        let _ = c.close().await;
    }
    for (_, p) in part_producers.drain() {
        let _ = p.close().await;
    }
    client.close().await;
    Ok(stream)
}

// `clippy::map_entry` would have us use the Entry API, but the
// producer/consumer factory call is `async` and `Entry` doesn't
// straddle an `.await`, so `contains_key` + `insert` is the right shape.
#[allow(clippy::map_entry)]
async fn ensure_part_producer<'a>(
    client: &Client<TokioProviders>,
    map: &'a mut HashMap<i32, Producer<TokioProviders>>,
    partition: i32,
    topic: &str,
) -> Result<&'a Producer<TokioProviders>, ClientError> {
    if !map.contains_key(&partition) {
        let p = client
            .open_producer(CreateProducerRequest {
                topic: topic.to_owned(),
                ..Default::default()
            })
            .await?;
        map.insert(partition, p);
    }
    Ok(map.get(&partition).expect("inserted above"))
}

#[allow(clippy::map_entry)]
async fn ensure_part_consumer<'a>(
    client: &Client<TokioProviders>,
    map: &'a mut HashMap<i32, Consumer<TokioProviders>>,
    partition: i32,
    topic: &str,
    sub: &str,
) -> Result<&'a Consumer<TokioProviders>, ClientError> {
    if !map.contains_key(&partition) {
        let c = client
            .subscribe(SubscribeRequest {
                topic: topic.to_owned(),
                subscription: sub.to_owned(),
                receiver_queue_size: 16,
                durable: true,
                ..Default::default()
            })
            .await?;
        map.insert(partition, c);
    }
    Ok(map.get(&partition).expect("inserted above"))
}

async fn run_send_partition(
    producer: &Producer<TokioProviders>,
    partition: i32,
    payload: Bytes,
) -> Event {
    let msg = OutgoingMessage {
        payload: payload.clone(),
        metadata: magnetar_proto::pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    };
    match producer.send(msg).await {
        Ok(message_id) => Event::SentPartition {
            partition,
            message_id,
        },
        Err(e) => Event::SendError { kind: classify(&e) },
    }
}

async fn run_recv_partition(
    consumer: &Consumer<TokioProviders>,
    partition: i32,
    timeout: Duration,
) -> Event {
    match tokio::time::timeout(timeout, consumer.receive()).await {
        Ok(Ok(msg)) => Event::ReceivedPartition {
            partition,
            payload: msg.payload.to_vec(),
            message_id: msg.message_id,
        },
        Ok(Err(_)) | Err(_) => Event::RecvTimeoutPartition { partition },
    }
}

async fn run_ack_partition(
    consumer: &Consumer<TokioProviders>,
    partition: i32,
    message_id: MessageId,
) -> Event {
    match consumer.ack(message_id).await {
        Ok(()) => Event::AckedPartition { partition },
        Err(e) => Event::AckError { kind: classify(&e) },
    }
}

async fn run_seek_partition(
    consumer: &Consumer<TokioProviders>,
    partition: i32,
    message_id: MessageId,
) -> Event {
    match consumer.seek_to_message(message_id).await {
        Ok(()) => Event::SeekedPartition { partition },
        Err(e) => Event::SeekError { kind: classify(&e) },
    }
}

async fn ensure_consumer<'a>(
    client: &Client<TokioProviders>,
    c: &'a mut Option<Consumer<TokioProviders>>,
    topic: &str,
    sub: &str,
) -> Result<&'a Consumer<TokioProviders>, ClientError> {
    if c.is_none() {
        let new = client
            .subscribe(SubscribeRequest {
                topic: topic.to_owned(),
                subscription: sub.to_owned(),
                receiver_queue_size: 16,
                durable: true,
                ..Default::default()
            })
            .await?;
        *c = Some(new);
    }
    Ok(c.as_ref().expect("inserted above"))
}

async fn run_send(producer: &Producer<TokioProviders>, payload: Bytes) -> Event {
    let msg = OutgoingMessage {
        payload: payload.clone(),
        metadata: magnetar_proto::pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    };
    match producer.send(msg).await {
        Ok(message_id) => Event::Sent { message_id },
        Err(e) => Event::SendError { kind: classify(&e) },
    }
}

/// PIP-180 / ADR-0033: replicator-style send. The scripted broker echoes
/// the source id back on `CommandSendReceipt` so the resulting
/// `Event::Sent` carries `message_id == source_msg_id`.
async fn run_send_with_source_id(
    producer: &Producer<TokioProviders>,
    source_msg_id: MessageId,
    payload: Bytes,
) -> Event {
    let fut = producer.send_with_source_message_id(
        source_msg_id,
        payload,
        magnetar_proto::pb::MessageMetadata::default(),
    );
    match fut.await {
        Ok(message_id) => Event::Sent { message_id },
        Err(e) => Event::SendError { kind: classify(&e) },
    }
}

async fn run_recv(consumer: &Consumer<TokioProviders>, timeout: Duration) -> Event {
    match tokio::time::timeout(timeout, consumer.receive()).await {
        Ok(Ok(msg)) => Event::Received {
            payload: msg.payload.to_vec(),
            message_id: msg.message_id,
        },
        Ok(Err(_)) | Err(_) => Event::RecvTimeout,
    }
}

async fn run_ack(consumer: &Consumer<TokioProviders>, message_id: MessageId) -> Event {
    match consumer.ack(message_id).await {
        Ok(()) => Event::Acked,
        Err(e) => Event::AckError { kind: classify(&e) },
    }
}

async fn run_seek(consumer: &Consumer<TokioProviders>, message_id: MessageId) -> Event {
    match consumer.seek_to_message(message_id).await {
        Ok(()) => Event::Seeked,
        Err(e) => Event::SeekError { kind: classify(&e) },
    }
}

fn classify(err: &ClientError) -> String {
    match err {
        ClientError::Engine(_) => "engine".to_owned(),
        ClientError::Broker { code, .. } => format!("broker:{code}"),
        ClientError::Closed => "closed".to_owned(),
        ClientError::Other(_) => "other".to_owned(),
    }
}
