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
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig::default(),
        None,
    )
    .await
}

/// Run `trace` against the tokio engine with the auto-reconnect supervisor
/// enabled (`config.supervisor = Some(...)`). The driver transparently
/// redials on a broker-induced drop and replays the in-flight publish /
/// re-subscribes, so a scenario armed with
/// [`crate::broker::ScriptedBroker::drop_connection_after_first_ack`] resumes from the
/// durable cursor instead of failing the op. Mirrors the moonpool sibling
/// [`crate::runner_moonpool::run_supervised`] so the two legs compare equal.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_supervised(
    pulsar_url: &str,
    trace: &Trace,
    supervisor: magnetar_proto::SupervisorConfig,
) -> Result<EventStream, ClientError> {
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig {
            supervisor: Some(supervisor),
            ..Default::default()
        },
        None,
    )
    .await
}

/// Run `trace` against the tokio engine with BOTH the auto-reconnect
/// supervisor AND a caller-supplied `operation_timeout` — the ADR-0083
/// write-deadline source. Layered on top of [`run_supervised`] rather than
/// widening its signature: `run_supervised` stays the common case (default
/// 30s `operation_timeout`), and the write-deadline equivalence scenario is
/// the only caller that needs a short deadline to keep the differential
/// test's wall-clock budget small.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_supervised_with_operation_timeout(
    pulsar_url: &str,
    trace: &Trace,
    supervisor: magnetar_proto::SupervisorConfig,
    operation_timeout: Duration,
) -> Result<EventStream, ClientError> {
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig {
            supervisor: Some(supervisor),
            operation_timeout,
            ..Default::default()
        },
        None,
    )
    .await
}

/// Run `trace` against the tokio engine with a caller-supplied
/// `operation_timeout` and NO supervisor. The producer-open cancellation
/// scenario (issue #406 / ADR-0100) needs a short deadline so a withheld
/// `CommandProducerSuccess` gives up quickly, and a plain client so the only
/// thing that can free the pinned name is the engine's own close-before-retry.
///
/// # Errors
/// Same envelope as [`run`].
pub async fn run_with_operation_timeout(
    pulsar_url: &str,
    trace: &Trace,
    operation_timeout: Duration,
) -> Result<EventStream, ClientError> {
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig {
            operation_timeout,
            ..Default::default()
        },
        Some(fast_operation_retry()),
    )
    .await
}

/// Retry policy the producer-open cancellation scenario installs. The default
/// 2 s initial backoff does not fit inside that scenario's short
/// `operation_timeout`, and what it exercises is the SEQUENCE of recovery
/// attempts, not the wait between them. Shared by both runners so the two legs
/// retry in lockstep.
pub(crate) fn fast_operation_retry() -> magnetar_proto::OperationRetryConfig {
    magnetar_proto::OperationRetryConfig {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
        max_retries: Some(8),
    }
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
    pulsar_url: &str,
    trace: &Trace,
    consumer_stall_timeout: Duration,
) -> Result<EventStream, ClientError> {
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig {
            consumer_stall_timeout: Some(consumer_stall_timeout),
            ..Default::default()
        },
        None,
    )
    .await
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
    pulsar_url: &str,
    trace: &Trace,
    consumer_stall_timeout: Duration,
    max_attempts: u32,
) -> Result<EventStream, ClientError> {
    run_with_config(
        pulsar_url,
        trace,
        magnetar_proto::ConnectionConfig {
            consumer_stall_timeout: Some(consumer_stall_timeout),
            consumer_stall_auto_recovery: Some(max_attempts),
            ..Default::default()
        },
        None,
    )
    .await
}

async fn run_with_config(
    pulsar_url: &str,
    trace: &Trace,
    config: magnetar_proto::ConnectionConfig,
    operation_retry: Option<magnetar_proto::OperationRetryConfig>,
) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();

    let mut client = Client::connect(pulsar_url, config).await?;
    if let Some(retry) = operation_retry {
        client = client.with_operation_retry(retry);
    }

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
    // `Option` also lets `Op::DropConsumer` release the final clone and
    // a later consumer op reopen it as an ordering barrier.
    let mut consumer: Option<Consumer> = None;

    // Per-partition producers + consumers, opened lazily on first
    // SendPartition / RecvPartition / AckPartition / SeekPartition op
    // targeting that partition. Each partition is its own logical topic
    // (`<base>-partition-N`) so we hold one producer + one consumer per
    // partition.
    let mut part_producers: HashMap<i32, Producer> = HashMap::new();
    let mut part_consumers: HashMap<i32, Consumer> = HashMap::new();

    // Producers opened by `Op::OpenNamedProducer`, held for the whole trace.
    // Releasing one mid-trace would fire ADR-0057's last-clone drop guard and
    // put a second, asynchronous `CommandCloseProducer` on the wire, which is
    // exactly the frame the issue #406 scenario is counting.
    let mut named_producers: Vec<Producer> = Vec::new();

    // Issue #414: additional `SubType::Shared` consumers opened by
    // `Op::OpenSharedConsumer`, keyed by their harness-local name. They all
    // land on ONE broker-side dispatcher for `(topic, subscription)`, so
    // detaching one mid-drain redelivers its un-acked entries to the survivors.
    let mut shared_consumers: HashMap<String, Consumer> = HashMap::new();

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
                    )
                    .await?,
                );
            }
            Op::RecvShared { name, timeout } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                stream.push(run_recv(consumer, *timeout).await);
            }
            Op::AckShared { name, message_id } => {
                let consumer = shared_consumers
                    .get(name)
                    .expect("trace names a shared consumer it never opened");
                stream.push(run_ack(consumer, *message_id).await);
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
                // Drain by closing producer and (if open) consumer.
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

/// Issue #414: open one more `SubType::Shared` consumer on the trace's
/// `(topic, subscription)`, held under a harness-local name.
///
/// `receiver_queue_size` is the initial permit grant, and reading it straight
/// back through `Consumer::available_permits()` is the point of the returned
/// event: that accessor now reports the REAL decrementing balance, so both
/// engines must agree on it before any dispatch lands.
async fn open_shared_consumer(
    client: &Client,
    map: &mut HashMap<String, Consumer>,
    name: &str,
    topic: &str,
    subscription: &str,
    receiver_queue_size: usize,
) -> Result<Event, ClientError> {
    let consumer = client
        .subscribe_with(
            SubscribeRequest {
                topic: topic.to_owned(),
                subscription: subscription.to_owned(),
                sub_type: magnetar_proto::pb::command_subscribe::SubType::Shared,
                receiver_queue_size,
                ..Default::default()
            },
            None,
        )
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
fn run_resubscribe_shared(consumer: &Consumer) -> Event {
    match consumer.resubscribe() {
        Ok(()) => Event::SharedConsumerResubscribed,
        Err(e) => Event::SharedConsumerResubscribeError { kind: classify(&e) },
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

/// Issue #406 / ADR-0100: open one extra producer under a pinned name. The
/// event is the open's verdict — that is the differential claim — and a
/// successful handle is parked in `held` so it keeps holding the name for the
/// rest of the trace instead of firing a drop-close mid-run.
async fn run_open_named_producer(
    client: &Client,
    topic: &str,
    name: &str,
    held: &mut Vec<Producer>,
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
        ClientError::Timeout(_) => "timeout".to_owned(),
        // Terminal drop on a plain connection (peer close / fatal decode):
        // the proto layer resolved every pending op with `OpOutcome::Terminal`
        // and the engine mapped it to `PeerClosed`. The terminal-error
        // differential test asserts both legs collapse to this same bucket
        // (ADR-0055 §1).
        ClientError::PeerClosed => "peer-closed".to_owned(),
        _ => "other".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_operation_timeout() {
        let error = ClientError::Timeout("producer open exceeded operation_timeout".to_owned());
        assert_eq!(classify(&error), "timeout");
    }
}
