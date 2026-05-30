// SPDX-License-Identifier: Apache-2.0

//! moonpool-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_moonpool::Client`] with
//! [`moonpool_core::TokioProviders`] and returns the resulting
//! [`EventStream`].
//!
//! The engine work runs directly on the ambient tokio runtime — no
//! [`tokio::task::LocalSet`] wrapper. moonpool's [`TokioProviders`]
//! `TaskProvider` is now `Send`-bound: `spawn_task<F>` requires
//! `F: Future<Output = ()> + Send + 'static` and spawns via
//! `tokio::task::Builder::new().spawn(...)` (a plain `tokio::spawn`,
//! NOT `spawn_local`). The driver task therefore runs on any tokio
//! runtime — including the `flavor = "current_thread"` runtimes the
//! differential tests use — and is woken normally by the sans-io waker
//! slab. The old `LocalSet` + `Kicker` pump were dead weight tied to a
//! stale `spawn_local` premise and have been removed.
//!
//! When `moonpool-sim`'s provider bundle becomes a workspace dep,
//! plug it in here as a sibling `run_with_sim_providers` entry point
//! that takes a seed.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, MessageId, SubscribeRequest};
use magnetar_runtime_moonpool::{Client, ClientError, Consumer, MoonpoolEngine, Producer};
use moonpool_core::TokioProviders;

use crate::trace::{Event, EventStream, Op, Trace};

/// Build the per-partition topic name for a given base topic.
/// Mirrors Java `PartitionedProducerImpl`'s topic-naming convention.
fn partition_topic(base: &str, partition: i32) -> String {
    format!("{base}-partition-{partition}")
}

/// Run `trace` against the moonpool engine talking to `host_port`
/// (e.g. `127.0.0.1:7654`). Note: the moonpool engine takes a bare
/// `host:port` string, NOT a `pulsar://` URL.
///
/// The engine work awaits directly on the ambient tokio runtime — there
/// is no [`tokio::task::LocalSet`] wrapper and no periodic pump. moonpool's
/// [`TokioProviders`] `TaskProvider` is `Send`-bound and spawns the driver
/// via `tokio::task::Builder::new().spawn(...)` (a plain `tokio::spawn`,
/// not `spawn_local`), so the driver task runs and is woken normally on the
/// `flavor = "current_thread"` runtimes the differential tests use.
///
/// # Errors
/// Returns the last engine-level error if the initial connect /
/// producer / consumer open fails.
pub async fn run(host_port: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default()).await?;

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

    // PIP-31: the current open txn id, if any. Mirrors `runner_tokio.rs`.
    let mut current_txn: Option<magnetar_proto::TxnId> = None;

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
            Op::NewTxn { timeout_ms } => {
                let timeout = std::time::Duration::from_millis(*timeout_ms);
                match client.new_txn(timeout).await {
                    Ok(txn_id) => {
                        current_txn = Some(txn_id);
                        stream.push(Event::TxnCreated);
                    }
                    Err(e) => stream.push(Event::TxnCreateError { kind: classify(&e) }),
                }
            }
            Op::EndTxn { commit } => {
                let Some(txn_id) = current_txn.take() else {
                    stream.push(Event::TxnEndError {
                        kind: "no-open-txn".to_owned(),
                    });
                    continue;
                };
                let action = if *commit {
                    magnetar_proto::TxnAction::Commit
                } else {
                    magnetar_proto::TxnAction::Abort
                };
                match client.end_txn(txn_id, action).await {
                    Ok(_state) => stream.push(Event::TxnEnded { committed: *commit }),
                    Err(e) => stream.push(Event::TxnEndError { kind: classify(&e) }),
                }
            }
            Op::SendInTxn { payload } => {
                let Some(txn_id) = current_txn else {
                    stream.push(Event::SendInTxnError {
                        kind: "no-open-txn".to_owned(),
                    });
                    continue;
                };
                let bytes = Bytes::from(payload.clone());
                stream.push(run_send_in_txn(&producer, txn_id, bytes).await);
            }
            Op::AckInTxn { message_id } => {
                let Some(txn_id) = current_txn else {
                    stream.push(Event::AckInTxnError {
                        kind: "no-open-txn".to_owned(),
                    });
                    continue;
                };
                match ensure_consumer(&client, &mut consumer, &trace.topic, &trace.subscription)
                    .await
                {
                    Ok(c) => stream.push(run_ack_in_txn(c, *message_id, txn_id).await),
                    Err(_) => stream.push(Event::AckInTxnError {
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

/// PIP-31: publish stamped with `txn_id`. Mirrors `runner_tokio`'s
/// `run_send_in_txn` — populates `OutgoingMessage::txn_id` so the
/// `CommandSend` carries the txn-id halves on the wire.
async fn run_send_in_txn(
    producer: &Producer<TokioProviders>,
    txn_id: magnetar_proto::TxnId,
    payload: Bytes,
) -> Event {
    let msg = OutgoingMessage {
        payload: payload.clone(),
        metadata: magnetar_proto::pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: Some(txn_id),
        source_message_id: None,
    };
    match producer.send(msg).await {
        Ok(message_id) => Event::SentInTxn { message_id },
        Err(e) => Event::SendInTxnError { kind: classify(&e) },
    }
}

/// PIP-31: ack stamped with `txn_id`. Routes through the runtime's
/// `Consumer::ack_with_txn` entry which stamps the txn-id halves onto
/// the `CommandAck` so the scripted broker can stage it against the
/// per-txn ack ledger.
async fn run_ack_in_txn(
    consumer: &Consumer<TokioProviders>,
    message_id: MessageId,
    txn_id: magnetar_proto::TxnId,
) -> Event {
    match consumer.ack_with_txn(message_id, txn_id).await {
        Ok(()) => Event::AckedInTxn,
        Err(e) => Event::AckInTxnError { kind: classify(&e) },
    }
}

fn classify(err: &ClientError) -> String {
    match err {
        ClientError::Engine(_) => "engine".to_owned(),
        ClientError::Broker { code, .. } => format!("broker:{code}"),
        ClientError::Closed => "closed".to_owned(),
        ClientError::ProxyUnsupportedOnUnsupervisedClient { .. } => "proxy-unsupervised".to_owned(),
        ClientError::Other(_) => "other".to_owned(),
    }
}
