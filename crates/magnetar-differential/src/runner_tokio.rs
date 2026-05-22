// SPDX-License-Identifier: Apache-2.0

//! Tokio-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_tokio::Client`] and returns the resulting
//! [`EventStream`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{CreateProducerRequest, MessageId, SubscribeRequest};
use magnetar_runtime_tokio::{Client, ClientError, ConnectionShared, Consumer, Producer};

use crate::trace::{Event, EventStream, Op, Trace};

/// Frequency at which the kicker pulses `driver_waker.notify_one()`.
///
/// The engine's user-facing futures (`wait_producer_ready`,
/// `EventWaitFut`, etc.) spawn one-shot "orphan" tasks that race for
/// `driver_waker.notified()` permits with the driver loop. In real
/// e2e against a live Pulsar broker, periodic PINGs and ack traffic
/// keep wake-ups flowing through the system. Against the scripted
/// differential broker — which only speaks the bare protocol subset
/// the harness needs and never emits PINGs / acks of its own —
/// orphan tasks can starve and stall a future indefinitely.
///
/// Commit `c983026e3521` made the production driver loop call
/// `driver_waker.notify_waiters()` after every `handle_bytes`. That
/// is sufficient for the short `broker_smoke` handshake +
/// producer-open round-trip (which now passes without any kicker —
/// verified by removing the in-test kicker on 2026-05-22; the test
/// stays green and `[broker]` traces show a normal frame sequence).
///
/// However the longer `golden_traces` multi-op sequences (`Recv`
/// with 2 s timeouts, seek replay, nack redelivery) regress to a
/// ~30 s wall-clock run when the kicker is removed — the
/// `consumer.receive()` futures observe the queued message only
/// after the per-op `tokio::time::timeout` eventually re-polls,
/// which is a separate orphan-task latency leak on the
/// consumer-receive path (the consumer's per-message slab is not
/// yet wired to wake the `Recv` future directly on delivery).
///
/// Until that consumer-side wake path is closed, the kicker stays
/// in for safety. 25 ms is fast enough to keep golden-trace latency
/// in the millisecond range (a 5-op trace adds ~125 ms of kicker
/// overhead worst case) and slow enough that it doesn't dominate
/// the runtime. The long-term fix is to register the `Recv`
/// future's waker against the consumer's per-message waker slab so
/// the sans-io layer wakes it directly on delivery, after which the
/// kicker can be removed.
const KICKER_INTERVAL: Duration = Duration::from_millis(25);

/// Spawn a background kicker. Drop the returned handle to stop it.
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

/// Run `trace` against the tokio engine talking to `pulsar_url`.
///
/// The runner opens **one** producer and (lazily) **one** consumer for
/// the duration of the trace. `Close` closes both.
///
/// # Errors
/// Returns the last engine-level error if the initial connect /
/// producer / consumer open fails. A failure mid-trace surfaces as
/// `Event::SendError`/`AckError`/etc. inside the [`EventStream`].
pub async fn run(pulsar_url: &str, trace: &Trace) -> Result<EventStream, ClientError> {
    let mut stream = EventStream::empty();

    let client = Client::connect(pulsar_url, magnetar_proto::ConnectionConfig::default()).await?;
    let _kicker = Kicker::spawn(client.shared().clone());

    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: trace.topic.clone(),
                ..Default::default()
            },
            None,
        )
        .await?;

    // Open the consumer lazily on first need (Recv / Ack / Nack / Seek).
    let mut consumer: Option<Consumer> = None;

    for op in &trace.ops {
        match op {
            Op::Send { payload } => {
                let bytes = Bytes::from(payload.clone());
                let event = run_send(&producer, bytes).await;
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
            Op::Close => {
                // Drain by closing producer and (if open) consumer.
                // Producer/Consumer expose `close(self)`; clone the
                // handle so the original variable stays valid for the
                // borrow checker (consume the clone).
                if let Some(c) = consumer.take() {
                    let _ = c.close().await;
                }
                let _ = producer.clone().close().await;
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
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    Ok(stream)
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
    };
    match producer.send(msg).await {
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
        _ => "other".to_owned(),
    }
}
