// SPDX-License-Identifier: Apache-2.0

//! Tokio-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_tokio::Client`] and returns the resulting
//! [`EventStream`].

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{CreateProducerRequest, MessageId, SubscribeRequest};
use magnetar_runtime_tokio::{Client, ClientError, Consumer, Producer};

use crate::trace::{Event, EventStream, Op, Trace};

/// Build the per-partition topic name for a given base topic.
/// Mirrors Java `PartitionedProducerImpl`'s topic-naming convention.
fn partition_topic(base: &str, partition: i32) -> String {
    format!("{base}-partition-{partition}")
}

/// Run `trace` against the tokio engine talking to `pulsar_url`.
///
/// The runner opens **one** producer and (lazily) **one** consumer for
/// the duration of the trace. `Close` closes both.
///
/// `consumer.receive()` futures register their `Waker` against the
/// per-consumer slab on [`magnetar_proto::consumer::ConsumerState`] and
/// the sans-io layer wakes them directly on message arrival — no
/// background poll-pulse task is required.
///
/// # Errors
/// Returns the last engine-level error if the initial connect /
/// producer / consumer open fails. A failure mid-trace surfaces as
/// `Event::SendError`/`AckError`/etc. inside the [`EventStream`].
pub async fn run(pulsar_url: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();

    let client = Client::connect(pulsar_url, magnetar_proto::ConnectionConfig::default()).await?;

    // `Option` so `Op::DropProducer` can release every clone mid-trace
    // (issue #241 last-clone drop guard). `None` afterwards makes
    // subsequent sends resolve to `SendError { kind: "producer-dropped" }`.
    let mut producer = Some(
        client
            .open_producer_with(
                CreateProducerRequest {
                    topic: trace.topic.clone(),
                    ..Default::default()
                },
                None,
            )
            .await?,
    );

    // Open the consumer lazily on first need (Recv / Ack / Nack / Seek).
    let mut consumer: Option<Consumer> = None;

    // Per-partition producers + consumers, opened lazily on first
    // SendPartition / RecvPartition / AckPartition / SeekPartition op
    // targeting that partition. Each partition is its own logical topic
    // (`<base>-partition-N`) so we hold one producer + one consumer per
    // partition.
    let mut part_producers: HashMap<i32, Producer> = HashMap::new();
    let mut part_consumers: HashMap<i32, Consumer> = HashMap::new();

    // PIP-31: the current open txn id, if any. `NewTxn` populates it;
    // `EndTxn` consumes it. The harness supports one in-flight
    // transaction per trace at a time — matches the scripted broker's
    // per-session state. The txn-id bits are tracked here (and not
    // surfaced on `Event::TxnCreated`) because the broker allocates
    // them and they're not part of the differential equivalence claim.
    let mut current_txn: Option<magnetar_proto::TxnId> = None;

    for op in &trace.ops {
        match op {
            Op::Send { payload } => {
                let bytes = Bytes::from(payload.clone());
                let event = match producer.as_ref() {
                    Some(p) => run_send(p, bytes).await,
                    None => producer_dropped_send_error(),
                };
                stream.push(event);
            }
            Op::SendWithSourceId {
                source_msg_id,
                payload,
            } => {
                let bytes = Bytes::from(payload.clone());
                let event = match producer.as_ref() {
                    Some(p) => run_send_with_source_id(p, *source_msg_id, bytes).await,
                    None => producer_dropped_send_error(),
                };
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
                let event = match producer.as_ref() {
                    Some(p) => run_send_in_txn(p, txn_id, bytes).await,
                    None => producer_dropped_send_in_txn_error(),
                };
                stream.push(event);
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
            Op::DropProducer => {
                // Release every clone WITHOUT close().await — exercises
                // the engines' last-clone drop guard (issue #241). The
                // broker-side CloseProducer is asserted out-of-band via
                // `ScriptedBroker::frame_log_snapshot`.
                if let Some(p) = producer.take() {
                    drop(p);
                }
                stream.push(Event::ProducerDropped);
            }
            Op::Close => {
                // Drain by closing producer and (if open) consumer.
                if let Some(c) = consumer.take() {
                    let _ = c.close().await;
                }
                if let Some(p) = producer.take() {
                    let _ = p.close().await;
                }
                for (_, c) in part_consumers.drain() {
                    let _ = c.close().await;
                }
                for (_, p) in part_producers.drain() {
                    let _ = p.close().await;
                }
                stream.push(Event::Closed);
                // Detach the driver instead of waiting for `client.close()`
                // to join it — the scripted broker drops its session
                // task on shutdown, so a graceful close round-trip is
                // unnecessary and would block on a peer that's about to
                // disappear.
                if let Some(d) = client.take_driver() {
                    d.abort();
                }
                drop(client);
                return Ok(stream);
            }
        }
    }

    // Implicit close if no Close op present.
    if let Some(c) = consumer.take() {
        let _ = c.close().await;
    }
    for (_, c) in part_consumers.drain() {
        let _ = c.close().await;
    }
    for (_, p) in part_producers.drain() {
        let _ = p.close().await;
    }
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    Ok(stream)
}

// `clippy::map_entry` would have us use the Entry API, but the
// producer/consumer factory call is `async` and `Entry` doesn't
// straddle an `.await`, so `contains_key` + `insert` is the right shape.
#[allow(clippy::map_entry)]
async fn ensure_part_producer<'a>(
    client: &Client,
    map: &'a mut HashMap<i32, Producer>,
    partition: i32,
    topic: &str,
) -> Result<&'a Producer, ClientError> {
    if !map.contains_key(&partition) {
        let p = client
            .open_producer_with(
                CreateProducerRequest {
                    topic: topic.to_owned(),
                    ..Default::default()
                },
                None,
            )
            .await?;
        map.insert(partition, p);
    }
    Ok(map.get(&partition).expect("inserted above"))
}

#[allow(clippy::map_entry)]
async fn ensure_part_consumer<'a>(
    client: &Client,
    map: &'a mut HashMap<i32, Consumer>,
    partition: i32,
    topic: &str,
    sub: &str,
) -> Result<&'a Consumer, ClientError> {
    if !map.contains_key(&partition) {
        let c = client
            .subscribe_with(
                SubscribeRequest {
                    topic: topic.to_owned(),
                    subscription: sub.to_owned(),
                    receiver_queue_size: 16,
                    durable: true,
                    ..Default::default()
                },
                None,
            )
            .await?;
        map.insert(partition, c);
    }
    Ok(map.get(&partition).expect("inserted above"))
}

/// Stable bucket for a send op replayed after [`Op::DropProducer`]
/// released the producer — both runners must collapse to the same kind.
fn producer_dropped_send_error() -> Event {
    Event::SendError {
        kind: "producer-dropped".to_owned(),
    }
}

/// [`Op::SendInTxn`] sibling of [`producer_dropped_send_error`].
fn producer_dropped_send_in_txn_error() -> Event {
    Event::SendInTxnError {
        kind: "producer-dropped".to_owned(),
    }
}

async fn run_send_partition(producer: &Producer, partition: i32, payload: Bytes) -> Event {
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

async fn run_recv_partition(consumer: &Consumer, partition: i32, timeout: Duration) -> Event {
    match tokio::time::timeout(timeout, consumer.receive()).await {
        Ok(Ok(msg)) => Event::ReceivedPartition {
            partition,
            payload: msg.payload.to_vec(),
            message_id: msg.message_id,
        },
        Ok(Err(_)) | Err(_) => Event::RecvTimeoutPartition { partition },
    }
}

async fn run_ack_partition(consumer: &Consumer, partition: i32, message_id: MessageId) -> Event {
    match consumer.ack(message_id).await {
        Ok(()) => Event::AckedPartition { partition },
        Err(e) => Event::AckError { kind: classify(&e) },
    }
}

async fn run_seek_partition(consumer: &Consumer, partition: i32, message_id: MessageId) -> Event {
    match consumer.seek_to_message(message_id).await {
        Ok(()) => Event::SeekedPartition { partition },
        Err(e) => Event::SeekError { kind: classify(&e) },
    }
}

async fn ensure_consumer<'a>(
    client: &Client,
    c: &'a mut Option<Consumer>,
    topic: &str,
    sub: &str,
) -> Result<&'a Consumer, ClientError> {
    if c.is_none() {
        let new = client
            .subscribe_with(
                SubscribeRequest {
                    topic: topic.to_owned(),
                    subscription: sub.to_owned(),
                    receiver_queue_size: 16,
                    durable: true,
                    ..Default::default()
                },
                None,
            )
            .await?;
        *c = Some(new);
    }
    Ok(c.as_ref().expect("inserted above"))
}

async fn run_send(producer: &Producer, payload: Bytes) -> Event {
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

/// PIP-180 / ADR-0033: replicator-style send. The scripted broker echoes the
/// source id back on `CommandSendReceipt` so the resulting `Event::Sent`
/// carries `message_id == source_msg_id`.
async fn run_send_with_source_id(
    producer: &Producer,
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

async fn run_recv(consumer: &Consumer, timeout: Duration) -> Event {
    match tokio::time::timeout(timeout, consumer.receive()).await {
        Ok(Ok(msg)) => Event::Received {
            payload: msg.payload.to_vec(),
            message_id: msg.message_id,
        },
        Ok(Err(_)) | Err(_) => Event::RecvTimeout,
    }
}

async fn run_ack(consumer: &Consumer, message_id: MessageId) -> Event {
    match consumer.ack(message_id).await {
        Ok(()) => Event::Acked,
        Err(e) => Event::AckError { kind: classify(&e) },
    }
}

async fn run_seek(consumer: &Consumer, message_id: MessageId) -> Event {
    match consumer.seek_to_message(message_id).await {
        Ok(()) => Event::Seeked,
        Err(e) => Event::SeekError { kind: classify(&e) },
    }
}

/// PIP-31: publish stamped with `txn_id`. The proto `OutgoingMessage`
/// already carries `txn_id: Option<TxnId>`; the runner just plugs the
/// currently-open txn into that slot. The scripted broker treats the
/// send the same as a non-txn publish (the staged-ack ledger only
/// tracks acks; a real broker would route the send to the txn's
/// per-partition pending entries).
async fn run_send_in_txn(
    producer: &Producer,
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
    consumer: &Consumer,
    message_id: MessageId,
    txn_id: magnetar_proto::TxnId,
) -> Event {
    match consumer.ack_with_txn(message_id, txn_id).await {
        Ok(()) => Event::AckedInTxn,
        Err(e) => Event::AckInTxnError { kind: classify(&e) },
    }
}

/// Collapse a [`ClientError`] to a stable category string so the two
/// engines compare equal even when they format error messages with
/// different punctuation. Extend with new buckets as new error kinds
/// surface.
fn classify(err: &ClientError) -> String {
    match err {
        ClientError::Io(_) => "io".to_owned(),
        ClientError::Protocol(_) => "protocol".to_owned(),
        ClientError::Tls(_) => "tls".to_owned(),
        ClientError::Broker { code, .. } => format!("broker:{code}"),
        ClientError::Closed => "closed".to_owned(),
        // Terminal drop on a plain connection (peer close / fatal decode):
        // the proto layer resolved every pending op with `OpOutcome::Terminal`
        // and the engine mapped it to `PeerClosed`. The terminal-error
        // differential test asserts both legs collapse to this same bucket
        // (ADR-0055 §1).
        ClientError::PeerClosed => "peer-closed".to_owned(),
        _ => "other".to_owned(),
    }
}
