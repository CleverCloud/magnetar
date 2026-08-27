// SPDX-License-Identifier: Apache-2.0

//! moonpool-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_moonpool::Client`] with
//! [`moonpool_core::TokioProviders`] and returns the resulting
//! [`EventStream`].
//!
//! The engine work runs directly on the ambient Tokio runtime.
//! Moonpool 0.8's [`TokioProviders`] task provider is `Send`-bound and delegates to
//! `tokio::spawn`, so the driver runs normally on both current-thread and multi-thread Tokio
//! runtimes.
//! Native deterministic-executor coverage belongs to the runtime crate's `SimProviders` chaos
//! suite; this runner intentionally keeps both differential legs on the same real network and
//! wall clock.

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
/// The engine work awaits directly on the ambient Tokio runtime.
/// Moonpool 0.8's [`TokioProviders`] task provider is `Send`-bound and uses `tokio::spawn`, so the
/// driver task is woken normally by the sans-io waker slab.
///
/// # Errors
/// Returns the last engine-level error if the initial connect /
/// producer / consumer open fails.
pub async fn run(host_port: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default()).await?;
    replay(client, trace, None).await
}

/// Run `trace` against the moonpool engine with the auto-reconnect supervisor
/// enabled. The supervised driver transparently redials on a broker-induced
/// drop and replays the in-flight publish / re-subscribes, so a scenario
/// armed with [`crate::broker::ScriptedBroker::drop_connection_after_first_ack`]
/// resumes from the durable cursor instead of failing the op. Mirrors the
/// tokio sibling [`crate::runner_tokio::run_supervised`] so the two legs
/// compare equal.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_supervised(
    host_port: &str,
    trace: &Trace,
    supervisor: magnetar_proto::SupervisorConfig,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(supervisor),
        ..Default::default()
    };
    let client = Client::connect_plain_supervised(&engine, host_port, config, None, None).await?;
    replay(client, trace, None).await
}

/// Sibling of [`run_supervised`] with a caller-supplied `operation_timeout`
/// — see [`crate::runner_tokio::run_supervised_with_operation_timeout`] for
/// the rationale.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_supervised_with_operation_timeout(
    host_port: &str,
    trace: &Trace,
    supervisor: magnetar_proto::SupervisorConfig,
    operation_timeout: Duration,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(supervisor),
        operation_timeout,
        ..Default::default()
    };
    let client = Client::connect_plain_supervised(&engine, host_port, config, None, None).await?;
    replay(client, trace, None).await
}

/// Sibling of [`crate::runner_tokio::run_with_operation_timeout`]: a plain
/// (unsupervised) client with a caller-supplied `operation_timeout`, for the
/// producer-open cancellation scenario (issue #406 / ADR-0100).
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_with_operation_timeout(
    host_port: &str,
    trace: &Trace,
    operation_timeout: Duration,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        operation_timeout,
        ..Default::default()
    };
    let client = Client::connect_plain(&engine, host_port, config)
        .await?
        .with_operation_retry(crate::runner_tokio::fast_operation_retry());
    replay(client, trace, None).await
}

/// Run `trace` with the issue #414 per-consumer stall watchdog armed at
/// `consumer_stall_timeout`.
///
/// The window is deliberately tiny here: a Shared consumer that is granted
/// permits and then handed nothing crosses it while the trace is still running,
/// so the driver's control-event pump has a real `ConsumerStalled` to drain —
/// which is the whole point, since draining it silently is the engine behaviour
/// ADR-0101 specifies and the only way it can go wrong is by escaping as an
/// error or piling up. Nothing in the resulting `EventStream` is timing-derived:
/// the watchdog only ever emits an event the engine consumes, so both legs
/// compare equal regardless of exactly when it fires.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_with_stall_timeout(
    host_port: &str,
    trace: &Trace,
    consumer_stall_timeout: Duration,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        consumer_stall_timeout: Some(consumer_stall_timeout),
        ..Default::default()
    };
    let client = Client::connect_plain(&engine, host_port, config).await?;
    replay(client, trace, None).await
}

/// Run `trace` with the issue #414 stall watchdog armed at `consumer_stall_timeout`
/// AND ADR-0103's bounded automatic recovery armed at `max_attempts` in-place
/// re-subscribes per stall streak.
///
/// Pair it with [`crate::broker::ScriptedBroker::leak_shared_permits_on_consumer_churn`]:
/// the broker wedges its Shared dispatcher on the first churn event, the watchdog notices
/// the survivor holding un-spent permits over an empty queue, and each automatic
/// re-subscribe lifts the broker's leaked aggregate by exactly one receiver-queue window.
/// Whether delivery resumes is then a pure function of `max_attempts` against the size of
/// the leak, which is what makes the same trace produce a `Received` under a sufficient
/// budget and a `RecvTimeout` under an insufficient one — identically on both engines,
/// since the whole mechanism lives in the shared sans-io layer.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_with_stall_auto_recovery(
    host_port: &str,
    trace: &Trace,
    consumer_stall_timeout: Duration,
    max_attempts: u32,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        consumer_stall_timeout: Some(consumer_stall_timeout),
        consumer_stall_auto_recovery: Some(max_attempts),
        ..Default::default()
    };
    let client = Client::connect_plain(&engine, host_port, config).await?;
    replay(client, trace, None).await
}

/// Run `trace` with `ack_timeout` armed on every `Op::OpenSharedConsumer` consumer
/// (issue #436). Mirrors [`crate::runner_tokio::run_with_ack_timeout`] — see it for why the
/// window belongs at the invocation and must stay short.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_with_ack_timeout(
    host_port: &str,
    trace: &Trace,
    ack_timeout: Duration,
) -> Result<EventStream, ClientError> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default()).await?;
    replay(client, trace, Some(ack_timeout)).await
}

async fn replay(
    client: Client<TokioProviders>,
    trace: &Trace,
    shared_ack_timeout: Option<Duration>,
) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();

    // `Option` so `Op::DropProducer` can release every clone mid-trace
    // (issue #241 last-clone drop guard). Mirrors `runner_tokio.rs`.
    let mut producer = Some(
        client
            .open_producer(CreateProducerRequest {
                topic: trace.topic.clone(),
                ..Default::default()
            })
            .await?,
    );

    // `Option` also lets `Op::DropConsumer` release the final clone and
    // a later consumer op reopen it as an ordering barrier.
    let mut consumer: Option<Consumer<TokioProviders>> = None;

    // Per-partition producers + consumers, opened lazily on first
    // SendPartition / RecvPartition / AckPartition / SeekPartition op
    // targeting that partition. See `runner_tokio.rs` for the rationale.
    let mut part_producers: HashMap<i32, Producer<TokioProviders>> = HashMap::new();
    let mut part_consumers: HashMap<i32, Consumer<TokioProviders>> = HashMap::new();

    // Producers opened by `Op::OpenNamedProducer`, held for the whole trace.
    // See `runner_tokio.rs` for why releasing one mid-trace would corrupt the
    // issue #406 close accounting.
    let mut named_producers: Vec<Producer<TokioProviders>> = Vec::new();

    // Issue #414: additional `SubType::Shared` consumers opened by
    // `Op::OpenSharedConsumer`, keyed by their harness-local name. Mirrors
    // `runner_tokio.rs` — they all land on ONE broker-side dispatcher for
    // `(topic, subscription)`.
    let mut shared_consumers: HashMap<String, Consumer<TokioProviders>> = HashMap::new();

    // Issue #436: the harness application's last-received id per Shared consumer plus its
    // dedup set, driving `Op::AckLastReceivedShared`. Mirrors `runner_tokio.rs`.
    let mut last_received: HashMap<String, MessageId> = HashMap::new();
    let mut acked: std::collections::HashSet<MessageId> = std::collections::HashSet::new();

    // PIP-31: the current open txn id, if any. Mirrors `runner_tokio.rs`.
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
            Op::SendBatch { payloads } => {
                let event = match producer.as_ref() {
                    Some(p) => run_send_batch(p, payloads).await,
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
            Op::OpenNamedProducer { name } => {
                stream.push(
                    run_open_named_producer(&client, &trace.topic, name, &mut named_producers)
                        .await,
                );
            }
            Op::OpenSharedConsumer {
                name,
                receiver_queue_size,
            } => {
                stream.push(
                    open_shared_consumer(
                        &client,
                        &mut shared_consumers,
                        name,
                        &trace.topic,
                        &trace.subscription,
                        *receiver_queue_size,
                        shared_ack_timeout,
                    )
                    .await?,
                );
            }
            Op::RecvShared { name, timeout } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                let event = run_recv(consumer, *timeout).await;
                if let Event::Received { message_id, .. } = &event {
                    last_received.insert(name.clone(), *message_id);
                }
                stream.push(event);
            }
            Op::AckShared { name, message_id } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                let event = run_ack(consumer, *message_id).await;
                if matches!(event, Event::Acked) {
                    acked.insert(*message_id);
                }
                stream.push(event);
            }
            Op::AckLastReceivedShared { name } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                let message_id = *last_received
                    .get(name)
                    .expect("trace acks a shared consumer that never received anything");
                if acked.contains(&message_id) {
                    stream.push(Event::AckSkippedDuplicate);
                } else {
                    let event = run_ack(consumer, message_id).await;
                    if matches!(event, Event::Acked) {
                        acked.insert(message_id);
                    }
                    stream.push(event);
                }
            }
            Op::CloseSharedConsumer { name } => {
                // `close` consumes the handle, so close a clone and keep the
                // map entry: later ops must still be able to address the
                // consumer that LEFT the subscription — that is exactly what a
                // #414 caller does when it tries to recover the wrong one.
                let departing = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened")
                    .clone();
                let _ = departing.close().await;
                stream.push(Event::SharedConsumerClosed);
            }
            Op::ResubscribeShared { name } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                stream.push(run_resubscribe_shared(consumer));
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
            Op::DropConsumer => {
                // Release every clone WITHOUT close().await — exercises
                // the engines' last-clone drop guard (issue #342). The
                // broker-side CloseConsumer is asserted out-of-band via
                // `ScriptedBroker::frame_log_snapshot`.
                if let Some(c) = consumer.take() {
                    drop(c);
                }
                stream.push(Event::ConsumerDropped);
            }
            Op::Close => {
                for (_, c) in shared_consumers.drain() {
                    let _ = c.close().await;
                }
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
                client.close().await;
                return Ok(stream);
            }
        }
    }

    for (_, c) in shared_consumers.drain() {
        let _ = c.close().await;
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

/// Issue #436: publish `payloads` as ONE batched broker entry. Mirrors
/// [`crate::runner_tokio`]'s sibling — same [`crate::trace::pack_batch_body`] framing and the
/// same declared `num_messages_in_batch`, so both legs put byte-identical frames on the wire.
async fn run_send_batch(producer: &Producer<TokioProviders>, payloads: &[Vec<u8>]) -> Event {
    let body = crate::trace::pack_batch_body(payloads);
    let num_messages = i32::try_from(payloads.len()).unwrap_or(i32::MAX);
    let msg = OutgoingMessage {
        payload: body.clone(),
        metadata: magnetar_proto::pb::MessageMetadata {
            num_messages_in_batch: Some(num_messages),
            ..Default::default()
        },
        uncompressed_size: u32::try_from(body.len()).unwrap_or(u32::MAX),
        num_messages,
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

/// Issue #414: open one more `SubType::Shared` consumer on the trace's
/// `(topic, subscription)`, held under a harness-local name. 1:1 with
/// `runner_tokio::open_shared_consumer`.
///
/// `receiver_queue_size` is the initial permit grant, and reading it straight
/// back through `Consumer::available_permits()` is the point of the returned
/// event: that accessor now reports the REAL decrementing balance, so both
/// engines must agree on it before any dispatch lands.
async fn open_shared_consumer(
    client: &Client<TokioProviders>,
    map: &mut HashMap<String, Consumer<TokioProviders>>,
    name: &str,
    topic: &str,
    subscription: &str,
    receiver_queue_size: usize,
    ack_timeout: Option<Duration>,
) -> Result<Event, ClientError> {
    let consumer = client
        .subscribe(SubscribeRequest {
            topic: topic.to_owned(),
            subscription: subscription.to_owned(),
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Shared,
            receiver_queue_size,
            durable: true,
            // Issue #436: `None` (the default) leaves the unacked-message tracker unbuilt,
            // which is the shape every other Shared trace runs in.
            ack_timeout,
            ..Default::default()
        })
        .await?;
    let permits = consumer.available_permits();
    map.insert(name.to_owned(), consumer);
    Ok(Event::SharedConsumerOpened { permits })
}

/// Issue #414: the caller-driven in-place recovery.
///
/// `resubscribe()` only stages the `CommandSubscribe` and wakes the driver, so
/// there is nothing here to wait on: the event reports whether the ENGINE
/// accepted the call. That the broker's `Success` actually re-armed the grant is
/// proved by the trace, which publishes after the recovery and receives the
/// message — impossible without a live permit. A consumer that has already been
/// closed is refused, and both engines must refuse it the same way.
fn run_resubscribe_shared(consumer: &Consumer<TokioProviders>) -> Event {
    match consumer.resubscribe() {
        Ok(()) => Event::SharedConsumerResubscribed,
        Err(e) => Event::SharedConsumerResubscribeError { kind: classify(&e) },
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

/// Issue #406 / ADR-0100 sibling of `runner_tokio::run_open_named_producer`.
async fn run_open_named_producer(
    client: &Client<TokioProviders>,
    topic: &str,
    name: &str,
    held: &mut Vec<Producer<TokioProviders>>,
) -> Event {
    match client
        .open_producer(CreateProducerRequest {
            topic: topic.to_owned(),
            producer_name: Some(name.to_owned()),
            ..Default::default()
        })
        .await
    {
        Ok(producer) => {
            held.push(producer);
            Event::NamedProducerOpened
        }
        Err(e) => Event::NamedProducerOpenError { kind: classify(&e) },
    }
}

fn classify(err: &ClientError) -> String {
    match err {
        ClientError::Engine(_) => "engine".to_owned(),
        ClientError::Broker { code, .. } => format!("broker:{code}"),
        ClientError::Closed => "closed".to_owned(),
        ClientError::Other(message) if message.contains("exceeded operation_timeout") => {
            "timeout".to_owned()
        }
        // Terminal drop on a plain connection (peer close / fatal decode):
        // the proto layer resolved every pending op with `OpOutcome::Terminal`
        // and the engine mapped it to `PeerClosed`. The terminal-error
        // differential test asserts both legs collapse to this same bucket
        // (ADR-0055 §1).
        ClientError::PeerClosed => "peer-closed".to_owned(),
        ClientError::ProxyUnsupportedOnUnsupervisedClient { .. } => "proxy-unsupervised".to_owned(),
        ClientError::Other(_) => "other".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_operation_timeout() {
        let error = ClientError::Other("producer open exceeded operation_timeout".to_owned());
        assert_eq!(classify(&error), "timeout");
    }
}
