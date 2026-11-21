// SPDX-License-Identifier: Apache-2.0

//! Tokio ↔ moonpool differential equivalence for a broker `CommandSendError` that answers a
//! BATCHED frame. Layer (d) of the ADR-0024 four-layer test policy.
//!
//! A batching producer folds its per-message publishes into one `CommandSend` carrying
//! `sequence_id = lowest` and `highest_sequence_id = highest`. The broker validates such a frame
//! against its HIGHEST id, so every pre-persist rejection (`MetadataError` for an unencrypted
//! batch on an `encryption_required` topic, `ChecksumError`, `PersistenceError`) names that id.
//! The entry was not persisted, so no receipt follows for the other members: all of them must
//! fail with the broker's error and release their client-wide memory reservation, on both
//! engines, instead of waiting for the send timeout.
//!
//! The single-producer `Trace` model has no way to make the scripted broker reject a publish, so
//! this test drives an in-process broker that answers a batched `CommandSend` with a
//! `CommandSendError` naming its `highest_sequence_id`, only once the scenario has seen the batch
//! in flight, and never answers an unbatched one. One scenario — written
//! once, expanded per engine by `scenario!` — records the outcome of every step, and the two
//! traces must be identical AND match the correct behaviour.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, MemoryLimitPolicy, SupervisorConfig,
    decode_one, encode_command, pb,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const HANG_GUARD: Duration = Duration::from_mins(1);
const LIMIT: u64 = 1000;
/// How long a member of the rejected batch may take to resolve. The correct client resolves it
/// the moment the `CommandSendError` is decoded, so this only bounds a failing run.
const MEMBER_RESOLVE: Duration = Duration::from_secs(3);
/// The text the broker puts in its rejection; every member must surface it.
const BROKER_MESSAGE: &str = "Messages must be encrypted";

#[derive(Debug, Clone, Copy)]
struct SeenSend {
    sequence_id: u64,
    highest_sequence_id: Option<u64>,
}

/// In-process single-broker Pulsar: answers the control plane at once and rejects every batched
/// publish the way a real broker rejects a frame it cannot persist — with the frame's highest id.
#[derive(Default)]
struct Broker {
    sends: Mutex<Vec<SeenSend>>,
    sends_changed: Notify,
    /// The batch rejection waits for one permit, so the scenario observes the batch in flight
    /// (`flush` still waiting) before any member can resolve.
    release_rejection: Notify,
}

impl Broker {
    async fn spawn() -> (String, Arc<Self>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
        let addr = listener.local_addr().expect("local_addr");
        let broker = Arc::new(Self::default());
        let accept = broker.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    return;
                };
                let broker = accept.clone();
                tokio::spawn(async move {
                    let _ = broker.serve(stream).await;
                });
            }
        });
        (addr.to_string(), broker)
    }

    async fn serve(&self, stream: TcpStream) -> std::io::Result<()> {
        let (mut rd, mut wr) = stream.into_split();
        let mut read_buf = BytesMut::with_capacity(64 * 1024);
        loop {
            let mut out = BytesMut::new();
            let mut holds_rejection = false;
            loop {
                let mut framed = read_buf.clone().freeze();
                let before = framed.len();
                let frame = match decode_one(&mut framed) {
                    Ok(f) => f,
                    Err(FrameError::Incomplete { .. }) => break,
                    Err(_) => return Ok(()),
                };
                let consumed = before - framed.len();
                let _ = read_buf.split_to(consumed);
                holds_rejection |= self.answer(&frame, &mut out);
            }
            if holds_rejection {
                self.release_rejection.notified().await;
            }
            if !out.is_empty() {
                wr.write_all(&out).await?;
            }
            if rd.read_buf(&mut read_buf).await? == 0 {
                return Ok(());
            }
        }
    }

    /// Encode the reply to `frame` into `out`; `true` when that reply is the batch rejection.
    fn answer(&self, frame: &magnetar_proto::Frame, out: &mut BytesMut) -> bool {
        let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
            return false;
        };
        let reply = match kind {
            pb::base_command::Type::Connect => pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-batch-frame-send-error-equiv".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            },
            pb::base_command::Type::Ping => pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            },
            pb::base_command::Type::Lookup => {
                let Some(l) = &frame.command.lookup_topic else {
                    return false;
                };
                pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                }
            }
            pb::base_command::Type::Producer => {
                let Some(p) = &frame.command.producer else {
                    return false;
                };
                pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: format!("batch-frame-equiv-{}", p.producer_id),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                }
            }
            pb::base_command::Type::Send => {
                let Some(s) = &frame.command.send else {
                    return false;
                };
                self.sends.lock().push(SeenSend {
                    sequence_id: s.sequence_id,
                    highest_sequence_id: s.highest_sequence_id,
                });
                self.sends_changed.notify_waiters();
                // The unbatched budget probe is never answered, so its admission cannot race a
                // rejection.
                let Some(highest) = s.highest_sequence_id else {
                    return false;
                };
                pb::BaseCommand {
                    r#type: pb::base_command::Type::SendError as i32,
                    send_error: Some(pb::CommandSendError {
                        producer_id: s.producer_id,
                        sequence_id: highest,
                        error: pb::ServerError::MetadataError as i32,
                        message: BROKER_MESSAGE.to_owned(),
                    }),
                    ..Default::default()
                }
            }
            _ => return false,
        };
        let _ = encode_command(out, &reply);
        kind == pb::base_command::Type::Send
    }

    async fn wait_for_sends(&self, n: usize, within: Duration) -> bool {
        tokio::time::timeout(within, async {
            loop {
                let notified = self.sends_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.sends.lock().len() >= n {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    /// `(sequence_id, highest_sequence_id)` of every frame, in arrival order.
    fn frames(&self) -> Vec<(u64, Option<u64>)> {
        self.sends
            .lock()
            .iter()
            .map(|s| (s.sequence_id, s.highest_sequence_id))
            .collect()
    }
}

fn config() -> ConnectionConfig {
    ConnectionConfig {
        memory_limit_bytes: LIMIT,
        memory_limit_policy: MemoryLimitPolicy::FailImmediately,
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    }
}

fn producer_request(topic: &str, enable_batching: bool) -> CreateProducerRequest {
    CreateProducerRequest {
        topic: format!("persistent://public/default/{topic}"),
        enable_batching,
        // The defect is a member that is never answered: the send timeout must not mask it.
        send_timeout: None,
        ..Default::default()
    }
}

fn msg(len: usize) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from(vec![0x5A; len]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(len).expect("test payload fits u32"),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *fut).poll(cx))).await
}

/// One engine-neutral line per observable step.
fn step(label: &str, outcome: &str) -> String {
    format!("{label}: {outcome}")
}

/// Expand the scenario for one engine. `$broker_message` extracts the broker's error text from
/// the engine's error shape; `$exceeded` recognises its memory-limit rejection.
macro_rules! scenario {
    ($client:expr, $broker:expr, $broker_message:expr, $exceeded:expr) => {{
        let client = $client;
        let broker: Arc<Broker> = $broker;
        let mut trace: Vec<String> = Vec::new();

        let batching = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(producer_request("batch-frame-equiv", true)),
        )
        .await
        .expect("open_producer did not time out")
        .expect("open_producer ok");
        let plain = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(producer_request("batch-frame-equiv-plain", false)),
        )
        .await
        .expect("open_producer did not time out")
        .expect("open_producer ok");

        let mut members = Vec::new();
        for _ in 0..3 {
            let mut member = Box::pin(batching.send(msg(100)));
            let queued = poll_once(&mut member).await.is_pending();
            trace.push(step(
                "batched 100 B",
                if queued { "queued" } else { "resolved" },
            ));
            members.push(member);
        }
        // `flush` waits for the acknowledgements, so poll it once to put the batch on the wire.
        let mut flush = Box::pin(batching.flush());
        let flushing = poll_once(&mut flush).await.is_pending();
        trace.push(step("flush", if flushing { "waiting" } else { "resolved" }));
        assert!(
            broker.wait_for_sends(1, HANG_GUARD).await,
            "the batch reaches the broker as one frame"
        );
        let frames = broker.frames();
        trace.push(format!(
            "frames {:?}",
            frames
                .iter()
                .map(|(lowest, highest)| highest.map(|h| h - lowest))
                .collect::<Vec<_>>()
        ));
        // The batch is on the wire and still unanswered: let the broker reject it.
        broker.release_rejection.notify_one();

        // The broker named the highest id; every member must still resolve with its error.
        for (label, index) in [("m2 (named by the broker)", 2), ("m0", 0), ("m1", 1)] {
            let member = &mut members[index];
            let outcome = match tokio::time::timeout(MEMBER_RESOLVE, member).await {
                Ok(Ok(_)) => "acknowledged".to_owned(),
                Ok(Err(err)) => $broker_message(&err).map_or_else(
                    || format!("other error {err:?}"),
                    |text| format!("rejected: {text}"),
                ),
                Err(_) => "still pending".to_owned(),
            };
            trace.push(step(label, &outcome));
        }
        let drained = match tokio::time::timeout(MEMBER_RESOLVE, &mut flush).await {
            Ok(Ok(())) => "drained",
            Ok(Err(_)) => "failed",
            Err(_) => "still pending",
        };
        trace.push(step("flush after the rejection", drained));

        // The whole budget is back only if every member released its bytes.
        let mut whole = Box::pin(plain.send(msg(usize::try_from(LIMIT).expect("limit fits"))));
        let admitted = match poll_once(&mut whole).await {
            Poll::Pending => "queued".to_owned(),
            Poll::Ready(Err(err)) if $exceeded(&err) => "rejected, budget still held".to_owned(),
            other => format!("unexpected {other:?}"),
        };
        trace.push(step("whole budget on a plain producer", &admitted));

        if let Some(driver) = client.take_driver() {
            driver.abort();
        }
        drop(client);
        trace
    }};
}

async fn tokio_trace() -> Vec<String> {
    use magnetar_runtime_tokio::{Client, ClientError};

    let (host_port, broker) = Broker::spawn().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&format!("pulsar://{host_port}"), config()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect ok");
    let broker_message = |err: &ClientError| match err {
        ClientError::SendRejected { message, .. } => Some(message.clone()),
        _ => None,
    };
    let exceeded = |err: &ClientError| matches!(err, ClientError::MemoryLimitExceeded { .. });
    scenario!(client, broker, broker_message, exceeded)
}

async fn moonpool_trace() -> Vec<String> {
    use magnetar_runtime_moonpool::{Client, ClientError, EngineError, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let (host_port, broker) = Broker::spawn().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &host_port, config(), None, None),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect ok");
    let broker_message = |err: &ClientError| match err {
        ClientError::Broker { message, .. } => Some(message.clone()),
        _ => None,
    };
    let exceeded = |err: &ClientError| {
        matches!(
            err,
            ClientError::Engine(EngineError::MemoryLimitExceeded { .. })
        )
    };
    scenario!(client, broker, broker_message, exceeded)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_frame_send_error_is_engine_equivalent() {
    let tokio = tokio_trace().await;
    // Same `LocalSet` isolation as the sibling `client_wide_memory_limit` equivalence test;
    // `TokioProviders` delegates Send tasks to `tokio::spawn`.
    let local = tokio::task::LocalSet::new();
    let moonpool = local.run_until(moonpool_trace()).await;

    let rejected = format!("rejected: {BROKER_MESSAGE}");
    let expected = vec![
        "batched 100 B: queued".to_owned(),
        "batched 100 B: queued".to_owned(),
        "batched 100 B: queued".to_owned(),
        "flush: waiting".to_owned(),
        "frames [Some(2)]".to_owned(),
        format!("m2 (named by the broker): {rejected}"),
        format!("m0: {rejected}"),
        format!("m1: {rejected}"),
        "flush after the rejection: drained".to_owned(),
        "whole budget on a plain producer: queued".to_owned(),
    ];
    assert_eq!(tokio, expected, "tokio trace");
    assert_eq!(
        tokio, moonpool,
        "tokio and moonpool must resolve every member of a rejected batch identically"
    );
}
