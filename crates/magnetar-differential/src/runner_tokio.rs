// SPDX-License-Identifier: Apache-2.0

//! Tokio-engine runner for the differential harness.
//!
//! Replays a [`Trace`] against the scripted broker using
//! [`magnetar_runtime_tokio::Client`] and returns the resulting
//! [`EventStream`].

use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{CreateProducerRequest, MessageId, SubscribeRequest};
use magnetar_runtime_tokio::{Client, ClientError, Consumer, Producer};

use crate::trace::{Event, EventStream, Op, Trace};

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
