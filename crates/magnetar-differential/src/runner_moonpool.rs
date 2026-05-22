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

/// Frequency at which the kicker pulses `driver_waker.notify_one()`.
/// See the equivalently-named const in [`crate::runner_tokio`] for
/// rationale — both engines share the orphan-task pattern in their
/// event-wait futures.
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
                if let Some(c) = consumer.take() {
                    let _ = c.close().await;
                }
                let _ = producer.clone().close().await;
                stream.push(Event::Closed);
                client.close().await;
                return Ok(stream);
            }
        }
    }

    if let Some(c) = consumer.take() {
        let _ = c.close().await;
    }
    client.close().await;
    Ok(stream)
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
    };
    match producer.send(msg).await {
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
