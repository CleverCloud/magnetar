// SPDX-License-Identifier: Apache-2.0

//! Runtime-boundary coverage for DLQ republish of dead-lettered BATCH members.
//!
//! A batch member's own key, ordering key, event time and properties live in its
//! `SingleMessageMetadata`; `IncomingMessage::metadata` is the batch ENVELOPE shared by every
//! member. The republished DLQ message must carry the member's own values, not the envelope's.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

mod common;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use common::HANG_GUARD;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const CHILD_TOPICS: [&str; 2] = [
    "persistent://public/default/orders-partition-0",
    "persistent://public/default/orders-partition-1",
];
const DLQ_TOPIC: &str = "persistent://public/default/orders-DLQ";

#[derive(Clone, Debug, PartialEq, Eq)]
enum WireEvent {
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
    Receipt {
        sequence: u64,
    },
    Ack {
        consumer_id: u64,
        ledger: u64,
        entry: u64,
    },
}

#[derive(Default)]
struct BrokerControl {
    events: parking_lot::Mutex<Vec<WireEvent>>,
    pongs: AtomicUsize,
    releases: AtomicUsize,
    changed: Notify,
    release_changed: Notify,
}

impl BrokerControl {
    fn push(&self, event: WireEvent) {
        self.events.lock().push(event);
        self.changed.notify_waiters();
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
                .expect("driver did not process the injected DLQ messages");
        }
    }

    fn release_receipt(&self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.release_changed.notify_waiters();
    }

    fn trace(&self) -> Vec<WireEvent> {
        self.events.lock().clone()
    }
}

fn emit_connected(out: &mut BytesMut) {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "partitioned-dlq-runtime-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    encode_command(out, &command).expect("encode Connected");
}

fn emit_lookup(out: &mut BytesMut, request_id: u64) {
    let command = pb::BaseCommand {
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
    };
    encode_command(out, &command).expect("encode LookupResponse");
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    encode_command(out, &command).expect("encode Success");
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let command = pb::BaseCommand {
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
    };
    encode_command(out, &command).expect("encode ProducerSuccess");
}

fn emit_batch(out: &mut BytesMut, consumer_id: u64) {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 11,
                entry_id: 0,
                partition: Some(0),
                ..Default::default()
            },
            redelivery_count: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    };
    // The envelope: what the batch container stamped for the whole entry.
    let metadata = pb::MessageMetadata {
        producer_name: "source-producer".to_owned(),
        sequence_id: 0,
        publish_time: 1_700_000_000_000,
        num_messages_in_batch: Some(2),
        partition_key: Some("envelope-key".to_owned()),
        ordering_key: Some(Bytes::from_static(b"envelope-order")),
        event_time: Some(1_700_000_000_500),
        properties: vec![pb::KeyValue {
            key: "custom".to_owned(),
            value: "envelope".to_owned(),
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for member in 0..2_u64 {
        let payload = format!("member-{member}");
        let single = pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("payload fits i32"),
            partition_key: Some(format!("member-key-{member}")),
            ordering_key: Some(Bytes::from(format!("member-order-{member}"))),
            event_time: Some(1_700_000_001_000 + member),
            properties: vec![pb::KeyValue {
                key: "custom".to_owned(),
                value: format!("member-{member}"),
            }],
            ..Default::default()
        };
        body.extend_from_slice(&(prost::Message::encoded_len(&single) as u32).to_be_bytes());
        prost::Message::encode(&single, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(payload.as_bytes());
    }
    encode_payload(out, &command, &metadata, &body).expect("encode batched entry");
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

fn observe_send(frame: &magnetar_proto::Frame) -> (WireEvent, u64, u64) {
    let send = frame.command.send.as_ref().expect("CommandSend body");
    let payload = frame.payload.as_ref().expect("CommandSend payload");
    (
        WireEvent::Send {
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
    let command = pb::BaseCommand {
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
    };
    encode_command(out, &command).expect("encode SendReceipt");
}

fn observe_ack(out: &mut BytesMut, ack: &pb::CommandAck) -> WireEvent {
    let message_id = ack.message_id.first().expect("source message id in Ack");
    if let Some(request_id) = ack.request_id {
        let command = pb::BaseCommand {
            r#type: pb::base_command::Type::AckResponse as i32,
            ack_response: Some(pb::CommandAckResponse {
                consumer_id: ack.consumer_id,
                request_id: Some(request_id),
                ..Default::default()
            }),
            ..Default::default()
        };
        encode_command(out, &command).expect("encode AckResponse");
    }
    WireEvent::Ack {
        consumer_id: ack.consumer_id,
        ledger: message_id.ledger_id,
        entry: message_id.entry_id,
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
    let mut written_receipts = Vec::new();
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
                    assert_eq!(subscribe.topic, CHILD_TOPICS[0]);
                    emit_batch(&mut out, subscribe.consumer_id);
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
                    let event = observe_ack(&mut out, ack);
                    control.push(event);
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
            written_receipts.push(sequence);
        }

        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() || stream.flush().await.is_err() {
                return;
            }
            out.clear();
            for sequence in written_receipts.drain(..) {
                control.push(WireEvent::Receipt { sequence });
            }
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

/// What the DLQ must receive: each member's OWN key / ordering key / event time / property.
fn expected_sends() -> Vec<WireEvent> {
    (0..2_u64)
        .map(|member| WireEvent::Send {
            sequence: member,
            payload: Bytes::from(format!("member-{member}")),
            key: Some(format!("member-key-{member}")),
            ordering_key: Some(Bytes::from(format!("member-order-{member}"))),
            event_time: Some(1_700_000_001_000 + member),
            custom: format!("member-{member}"),
            real_topic: CHILD_TOPICS[0].to_owned(),
            original_id: format!("11:0:0:{member}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_lettered_batch_members_republish_with_their_own_metadata() {
    let (address, control, broker) = spawn_broker().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain(&engine, &address, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: CHILD_TOPICS[0].to_owned(),
            subscription: "orders-subscription".to_owned(),
            max_redeliver_count: 1,
            receiver_queue_size: 8,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe");
    control.wait_for_pongs(1).await;
    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: DLQ_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open producer did not time out")
    .expect("open producer");

    control.release_receipt();
    control.release_receipt();
    let republished = tokio::time::timeout(HANG_GUARD, consumer.republish_dead_letters(&producer))
        .await
        .expect("republish did not time out")
        .expect("runtime republish");
    assert_eq!(republished, 2, "both batch members are dead-lettered");

    let sends: Vec<WireEvent> = control
        .trace()
        .into_iter()
        .filter(|event| matches!(event, WireEvent::Send { .. }))
        .collect();
    assert_eq!(sends, expected_sends());

    client.close().await;
    broker.abort();
}
