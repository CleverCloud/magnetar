// SPDX-License-Identifier: Apache-2.0

//! Runtime-trace equivalence for DLQ republish of dead-lettered BATCH members.
//!
//! This test drives each runtime's public `Consumer::republish_dead_letters`
//! directly. The engine-generic multi-topic façade dispatch is covered at its
//! own layer; this test makes no claim about that orchestration.
//!
//! A batch member's own key, ordering key, event time and properties live in its
//! `SingleMessageMetadata`, not in the shared batch envelope; both engines must republish the
//! member's own values, and an unbatched message must be republished unchanged.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const CHILD_TOPIC: &str = "persistent://public/default/orders-partition-0";
const DLQ_TOPIC: &str = "persistent://public/default/orders-DLQ";

#[derive(Clone, Debug, PartialEq, Eq)]
enum RuntimeEvent {
    Send {
        sequence: u64,
        payload: Bytes,
        key: Option<String>,
        ordering_key: Option<Bytes>,
        event_time: Option<u64>,
        custom: String,
        real_topic: String,
        original_id: String,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    total: usize,
    events: Vec<RuntimeEvent>,
}

#[derive(Default)]
struct BrokerControl {
    events: parking_lot::Mutex<Vec<RuntimeEvent>>,
    pongs: AtomicUsize,
    releases: AtomicUsize,
    changed: Notify,
    release_changed: Notify,
}

impl BrokerControl {
    fn push(&self, event: RuntimeEvent) {
        self.events.lock().push(event);
        self.changed.notify_waiters();
    }

    async fn wait_for_sends(&self, expected: usize) {
        loop {
            if self.events.lock().len() >= expected {
                return;
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.events.lock().len() >= expected {
                continue;
            }
            tokio::time::timeout(HANG_GUARD, changed.as_mut())
                .await
                .expect("broker did not observe the expected DLQ sends");
        }
    }

    async fn wait_for_pongs(&self, expected: usize) {
        loop {
            if self.pongs.load(Ordering::SeqCst) >= expected {
                return;
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pongs.load(Ordering::SeqCst) >= expected {
                continue;
            }
            tokio::time::timeout(HANG_GUARD, changed.as_mut())
                .await
                .expect("driver did not process injected DLQ messages");
        }
    }

    fn release_receipt(&self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.release_changed.notify_waiters();
    }

    fn events(&self) -> Vec<RuntimeEvent> {
        self.events.lock().clone()
    }
}

fn emit_connected(out: &mut BytesMut) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "partitioned-dlq-differential".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
}

fn emit_lookup(out: &mut BytesMut, request_id: u64) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                broker_service_url: None,
                broker_service_url_tls: None,
                response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
                request_id,
                authoritative: Some(true),
                error: None,
                message: None,
                proxy_through_service_url: Some(false),
            }),
            ..Default::default()
        },
    )
    .expect("encode LookupResponse");
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode Success");
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "partitioned-dlq-producer".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: Some(0),
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode ProducerSuccess");
}

fn message_command(consumer_id: u64, entry: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 11,
                entry_id: entry,
                partition: Some(0),
                ..Default::default()
            },
            redelivery_count: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn kv(key: &str, value: &str) -> pb::KeyValue {
    pb::KeyValue {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

/// Entry 0: a batch of two members whose own metadata differs from the envelope and from each
/// other. Entry 1: an unbatched message.
fn emit_entries(out: &mut BytesMut, consumer_id: u64) {
    let envelope = pb::MessageMetadata {
        producer_name: "source-producer".to_owned(),
        sequence_id: 0,
        publish_time: 1_700_000_000_000,
        num_messages_in_batch: Some(2),
        partition_key: Some("envelope-key".to_owned()),
        ordering_key: Some(Bytes::from_static(b"envelope-order")),
        event_time: Some(1_700_000_000_500),
        properties: vec![kv("custom", "envelope")],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for member in 0..2_u64 {
        let payload = format!("member-{member}");
        let single = pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("payload fits i32"),
            // Member 1 is keyless and has no event time: no key, no ordering key (Java clears the
            // first member's copy the envelope carries), and only the event time is inherited.
            partition_key: (member == 0).then(|| format!("member-key-{member}")),
            ordering_key: (member == 0).then(|| Bytes::from(format!("member-order-{member}"))),
            event_time: (member == 0).then_some(1_700_000_001_000 + member),
            properties: vec![kv("custom", &format!("member-{member}"))],
            ..Default::default()
        };
        body.extend_from_slice(&(prost::Message::encoded_len(&single) as u32).to_be_bytes());
        prost::Message::encode(&single, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(payload.as_bytes());
    }
    encode_payload(out, &message_command(consumer_id, 0), &envelope, &body)
        .expect("encode batched entry");

    let plain = pb::MessageMetadata {
        producer_name: "source-producer".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000_000,
        partition_key: Some("plain-key".to_owned()),
        ordering_key: Some(Bytes::from_static(b"plain-order")),
        event_time: Some(1_700_000_002_000),
        properties: vec![kv("custom", "plain")],
        ..Default::default()
    };
    encode_payload(out, &message_command(consumer_id, 1), &plain, b"plain")
        .expect("encode unbatched entry");
}

fn emit_ping(out: &mut BytesMut) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Ping as i32,
            ping: Some(pb::CommandPing {}),
            ..Default::default()
        },
    )
    .expect("encode Ping");
}

fn property(metadata: &pb::MessageMetadata, key: &str) -> String {
    metadata
        .properties
        .iter()
        .find(|property| property.key == key)
        .map(|property| property.value.clone())
        .unwrap_or_default()
}

fn observe_send(frame: &magnetar_proto::Frame) -> (RuntimeEvent, u64, u64) {
    let send = frame.command.send.as_ref().expect("CommandSend body");
    let payload = frame.payload.as_ref().expect("CommandSend payload");
    (
        RuntimeEvent::Send {
            sequence: send.sequence_id,
            payload: payload.body.clone(),
            key: payload.metadata.partition_key.clone(),
            ordering_key: payload.metadata.ordering_key.clone(),
            event_time: payload.metadata.event_time,
            custom: property(&payload.metadata, "custom"),
            real_topic: property(&payload.metadata, "REAL_TOPIC"),
            original_id: property(&payload.metadata, "ORIGINAL_MESSAGE_ID"),
        },
        send.producer_id,
        send.sequence_id,
    )
}

fn emit_receipt(out: &mut BytesMut, producer_id: u64, sequence: u64) {
    encode_command(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id,
                sequence_id: sequence,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 90,
                    entry_id: sequence,
                    ..Default::default()
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode SendReceipt");
}

fn observe_ack(out: &mut BytesMut, ack: &pb::CommandAck) {
    if let Some(request_id) = ack.request_id {
        encode_command(
            out,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::AckResponse as i32,
                ack_response: Some(pb::CommandAckResponse {
                    consumer_id: ack.consumer_id,
                    request_id: Some(request_id),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .expect("encode AckResponse");
    }
}

fn try_take_release(releases: &AtomicUsize) -> bool {
    let mut current = releases.load(Ordering::SeqCst);
    loop {
        let Some(next) = current.checked_sub(1) else {
            return false;
        };
        match releases.compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn serve_connection(mut stream: TcpStream, control: Arc<BrokerControl>) {
    let mut read = BytesMut::with_capacity(64 * 1024);
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut pending_receipts = VecDeque::new();
    loop {
        loop {
            let mut cursor = read.clone().freeze();
            let before = cursor.len();
            let frame = match decode_one(&mut cursor) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let _ = read.split_to(before - cursor.len());
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out),
                pb::base_command::Type::Lookup => {
                    let lookup = frame.command.lookup_topic.as_ref().expect("Lookup body");
                    emit_lookup(&mut out, lookup.request_id);
                }
                pb::base_command::Type::Subscribe => {
                    let subscribe = frame.command.subscribe.as_ref().expect("Subscribe body");
                    emit_success(&mut out, subscribe.request_id);
                    assert_eq!(subscribe.topic, CHILD_TOPIC);
                    emit_entries(&mut out, subscribe.consumer_id);
                    emit_ping(&mut out);
                }
                pb::base_command::Type::Producer => {
                    let producer = frame.command.producer.as_ref().expect("Producer body");
                    assert_eq!(producer.topic, DLQ_TOPIC);
                    emit_producer_success(&mut out, producer.request_id);
                }
                pb::base_command::Type::Send => {
                    let (event, producer_id, sequence) = observe_send(&frame);
                    control.push(event);
                    pending_receipts.push_back((producer_id, sequence));
                    emit_ping(&mut out);
                }
                pb::base_command::Type::Ack => {
                    let ack = frame.command.ack.as_ref().expect("Ack body");
                    observe_ack(&mut out, ack);
                }
                pb::base_command::Type::Pong => {
                    control.pongs.fetch_add(1, Ordering::SeqCst);
                    control.changed.notify_waiters();
                }
                pb::base_command::Type::Ping => {
                    encode_command(
                        &mut out,
                        &pb::BaseCommand {
                            r#type: pb::base_command::Type::Pong as i32,
                            pong: Some(pb::CommandPong {}),
                            ..Default::default()
                        },
                    )
                    .expect("encode Pong");
                }
                _ => {}
            }
        }

        while !pending_receipts.is_empty() && try_take_release(&control.releases) {
            let (producer_id, sequence) = pending_receipts
                .pop_front()
                .expect("pending receipt exists");
            emit_receipt(&mut out, producer_id, sequence);
        }

        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() || stream.flush().await.is_err() {
                return;
            }
            out.clear();
        }

        let receipt_released = control.release_changed.notified();
        tokio::pin!(receipt_released);
        receipt_released.as_mut().enable();
        if !pending_receipts.is_empty() && control.releases.load(Ordering::SeqCst) > 0 {
            continue;
        }
        tokio::select! {
            result = stream.read_buf(&mut read) => {
                if matches!(result, Ok(0) | Err(_)) {
                    return;
                }
            }
            () = receipt_released.as_mut(), if !pending_receipts.is_empty() => {}
        }
    }
}

async fn spawn_broker() -> (String, Arc<BrokerControl>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let address = listener.local_addr().expect("broker address");
    let control = Arc::new(BrokerControl::default());
    let broker_control = control.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let session_control = broker_control.clone();
            tokio::spawn(async move { serve_connection(stream, session_control).await });
        }
    });
    (address.to_string(), control, task)
}

async fn finish_run(control: &BrokerControl, republish: tokio::task::JoinHandle<usize>) -> Outcome {
    for _ in 0..3 {
        control.release_receipt();
    }
    control.wait_for_sends(3).await;
    Outcome {
        total: tokio::time::timeout(HANG_GUARD, republish)
            .await
            .expect("republish task did not complete")
            .expect("republish task panicked"),
        events: control.events(),
    }
}

async fn run_tokio() -> Outcome {
    let (address, control, broker) = spawn_broker().await;
    let client = magnetar_runtime_tokio::Client::connect(
        &format!("pulsar://{address}"),
        ConnectionConfig::default(),
    )
    .await
    .expect("tokio connect");
    let consumer = {
        let consumer = client
            .subscribe(SubscribeRequest {
                topic: CHILD_TOPIC.to_owned(),
                subscription: "orders-subscription".to_owned(),
                max_redeliver_count: 1,
                receiver_queue_size: 8,
                ..Default::default()
            })
            .await
            .expect("tokio subscribe");
        control.wait_for_pongs(1).await;
        consumer
    };
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: DLQ_TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect("tokio producer");
    let republish = tokio::spawn(async move {
        consumer
            .republish_dead_letters(&producer)
            .await
            .expect("tokio runtime republish")
    });
    let outcome = finish_run(&control, republish).await;
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    broker.abort();
    outcome
}

async fn run_moonpool() -> Outcome {
    let (address, control, broker) = spawn_broker().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let client = magnetar_runtime_moonpool::Client::connect_plain(
        &engine,
        &address,
        ConnectionConfig::default(),
    )
    .await
    .expect("moonpool connect");
    let consumer = {
        let consumer = client
            .subscribe(SubscribeRequest {
                topic: CHILD_TOPIC.to_owned(),
                subscription: "orders-subscription".to_owned(),
                max_redeliver_count: 1,
                receiver_queue_size: 8,
                ..Default::default()
            })
            .await
            .expect("moonpool subscribe");
        control.wait_for_pongs(1).await;
        consumer
    };
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: DLQ_TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect("moonpool producer");
    let republish = tokio::spawn(async move {
        consumer
            .republish_dead_letters(&producer)
            .await
            .expect("moonpool runtime republish")
    });
    let outcome = finish_run(&control, republish).await;
    client.close().await;
    broker.abort();
    outcome
}

fn send(
    sequence: u64,
    payload: &str,
    key: Option<&str>,
    order: Option<&str>,
    time: u64,
    custom: &str,
    id: &str,
) -> RuntimeEvent {
    RuntimeEvent::Send {
        sequence,
        payload: Bytes::from(payload.to_owned()),
        key: key.map(str::to_owned),
        ordering_key: order.map(|order| Bytes::from(order.to_owned())),
        event_time: Some(time),
        custom: custom.to_owned(),
        real_topic: CHILD_TOPIC.to_owned(),
        original_id: id.to_owned(),
    }
}

/// Each batch member keeps its OWN metadata (the keyless member has no key and inherits only
/// the envelope's event time); the unbatched message is unchanged.
fn expected_events() -> Vec<RuntimeEvent> {
    vec![
        send(
            0,
            "member-0",
            Some("member-key-0"),
            Some("member-order-0"),
            1_700_000_001_000,
            "member-0",
            "11:0:0:0",
        ),
        send(
            1,
            "member-1",
            None,
            None,
            1_700_000_000_500,
            "member-1",
            "11:0:0:1",
        ),
        send(
            2,
            "plain",
            Some("plain-key"),
            Some("plain-order"),
            1_700_000_002_000,
            "plain",
            "11:1:0:-1",
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_member_republish_outcomes_are_equivalent() {
    let tokio = run_tokio().await;
    let moonpool = run_moonpool().await;
    let expected = Outcome {
        total: 3,
        events: expected_events(),
    };
    assert_eq!(tokio, expected, "tokio must match the wire-level oracle");
    assert_eq!(
        moonpool, expected,
        "moonpool must match the wire-level oracle"
    );
    assert_eq!(tokio, moonpool, "runtime republish outcomes diverged");
}
