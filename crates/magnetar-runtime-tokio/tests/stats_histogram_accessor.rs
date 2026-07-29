// SPDX-License-Identifier: Apache-2.0

//! Issue #347 (`aggregate_stats` zeroes fields), ADR-0024 layer (b): drive a
//! consumer and a producer through this engine's `ConnectionShared` and
//! assert the raw-histogram accessors backing `ConsumerApi::
//! receive_latency_histogram` / `ProducerApi::send_latency_histogram`
//! (`ConsumerState::receive_latency_histogram` / `ProducerState::
//! send_latency_histogram`) return the recorded sample counts.
//!
//! Mirrors the existing `cumulative_batch_ack_prune.rs` convention: this
//! layer drives the sans-io `Connection` directly through the engine's
//! `ConnectionShared` wrapper (proving the wiring works through the real
//! per-slot mutex, not just the bare proto struct) rather than constructing
//! a full `Consumer`/`Producer` handle over a live driver loop — those
//! wrapper methods are a one-line `self.slot.state.lock().<accessor>()`
//! delegation with no engine-specific logic to exercise.
//!
//! Note: both `ConsumerState::pop_message` and `ProducerState::
//! apply_receipt` record latency via `Instant::elapsed()` — real wall-clock
//! time, not the synthetic `now` these tests inject at delivery — so this
//! test asserts sample **counts**, never specific millisecond values.
//!
//! The two `_via_live_facade` tests below are a second, deliberately
//! different layer: they drive a real `Consumer` / `Producer` over a
//! loopback broker and call the *runtime facade* accessors
//! (`Consumer::receive_latency_histogram` / `Producer::
//! send_latency_histogram`, the one-line `self.slot.state.lock().
//! <accessor>()` delegations) directly — the `ConnectionShared`-level tests
//! above only exercise `ConsumerState`/`ProducerState`, never those
//! delegating wrapper bodies. Moonpool sibling of these two:
//! `crates/magnetar-runtime-moonpool/tests/stats_histogram_accessor.rs`.

#![allow(clippy::too_many_lines)]

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, FrameError,
    SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::{Client, ConnectionShared};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::common::{HANG_GUARD, handshake_response_bytes};

/// Broker pushes this many messages; each `pop_message` call must stamp
/// exactly one `receive_latency_hist` sample.
const N: usize = 5;

/// Producer sends this many messages; each applied `CommandSendReceipt`
/// must stamp exactly one `send_latency_hist` sample.
const SENDS: u64 = 3;

fn deliver_message(conn: &mut Connection, t0: Instant, handle: ConsumerHandle, entry: u64) {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 9,
                entry_id: entry,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "magnetar-test-prod".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(
        &mut frame,
        &msg_cmd,
        &metadata,
        format!("m{entry}").as_bytes(),
    )
    .expect("encode message frame");
    conn.handle_bytes(t0, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
}

#[test]
fn consumer_receive_latency_histogram_reflects_popped_messages() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/stats-hist-accessor".to_owned(),
        subscription: "magnetar-test-stats-hist-accessor".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N,
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(req);
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: subscribe_request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &buf).expect("Success");
    let _ = conn.poll_event();

    conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();

    // No samples before any pop.
    let empty = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram();
    assert!(
        empty.is_none_or(|h| h.is_empty()),
        "no receive_latency_hist samples before any pop_message"
    );

    for entry in 0..N as u64 {
        deliver_message(&mut conn, t0, handle, entry);
    }
    for _ in 0..N {
        conn.pop_message(handle).expect("queued message");
    }

    let hist = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    assert_eq!(
        hist.len(),
        N as u64,
        "one receive_latency_hist sample per popped message"
    );
}

#[test]
fn producer_send_latency_histogram_reflects_receipts() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/stats-hist-accessor-producer".to_owned(),
        ..Default::default()
    });

    // No samples before any receipt.
    let empty = conn
        .producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram();
    assert!(
        empty.is_none_or(|h| h.is_empty()),
        "no send_latency_hist samples before any CommandSendReceipt"
    );

    for entry in 0..SENDS {
        let seq = conn
            .send(
                producer,
                magnetar_proto::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("send queues");

        let receipt = pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: producer.0,
                sequence_id: seq.0,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: entry,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &receipt).expect("encode CommandSendReceipt");
        conn.handle_bytes(t0, &buf).expect("apply receipt");
        while conn.poll_event().is_some() {}
    }

    let hist = conn
        .producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram()
        .expect("send_latency_hist initialised");
    assert_eq!(
        hist.len(),
        SENDS,
        "one send_latency_hist sample per applied CommandSendReceipt"
    );
}

/// Minimal broker for the consumer live-facade test: answers CONNECT /
/// LOOKUP / SUBSCRIBE normally, then pushes exactly one message on the
/// first `CommandFlow` it sees.
async fn serve_single_message_broker_conn(stream: &mut TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut consumer_id = 0u64;
    let mut delivered = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "stats-hist-live-broker".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out_buf, &cmd);
                }
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        let cmd = pb::BaseCommand {
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
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        consumer_id = s.consumer_id;
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: s.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                pb::base_command::Type::Flow if !delivered => {
                    delivered = true;
                    let msg_cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Message as i32,
                        message: Some(pb::CommandMessage {
                            consumer_id,
                            message_id: pb::MessageIdData {
                                ledger_id: 1,
                                entry_id: 1,
                                partition: None,
                                batch_index: None,
                                ack_set: Vec::new(),
                                batch_size: None,
                                first_chunk_message_id: None,
                            },
                            redelivery_count: Some(0),
                            ack_set: Vec::new(),
                            consumer_epoch: None,
                        }),
                        ..Default::default()
                    };
                    let metadata = pb::MessageMetadata {
                        producer_name: "stats-hist-live-broker".to_owned(),
                        sequence_id: 1,
                        publish_time: 0,
                        num_messages_in_batch: Some(1),
                        ..Default::default()
                    };
                    let _ = encode_payload(&mut out_buf, &msg_cmd, &metadata, b"live-hist-payload");
                }
                _ => {}
            }
        }
        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }
        if matches!(stream.read_buf(&mut read_buf).await, Ok(0) | Err(_)) {
            return;
        }
    }
}

async fn spawn_single_message_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                serve_single_message_broker_conn(&mut stream).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// Live-facade sibling of `consumer_receive_latency_histogram_reflects_popped_messages`:
/// drives a real `Consumer` over a loopback broker and calls
/// `Consumer::receive_latency_histogram` (consumer.rs) — the runtime facade
/// delegation the `ConnectionShared`-level test above never touches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_receive_latency_histogram_accessor_reflects_popped_messages_via_live_facade() {
    let url = spawn_single_message_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/stats-hist-accessor-live".to_owned(),
            subscription: "stats-hist-accessor-live".to_owned(),
            sub_type: pb::command_subscribe::SubType::Shared,
            receiver_queue_size: 8,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    assert!(
        consumer
            .receive_latency_histogram()
            .is_none_or(|h| h.is_empty()),
        "no receive_latency_hist samples before any receive()"
    );

    let msg = tokio::time::timeout(HANG_GUARD, consumer.receive())
        .await
        .expect("receive did not time out")
        .expect("receive ok");
    assert_eq!(msg.payload.as_ref(), b"live-hist-payload");

    let hist = consumer
        .receive_latency_histogram()
        .expect("receive_latency_histogram initialised after the receive() pop");
    assert_eq!(
        hist.len(),
        1,
        "the live facade accessor must reflect the one popped message"
    );

    client.close().await;
}

/// Minimal broker for the producer live-facade test: answers CONNECT /
/// LOOKUP / PRODUCER normally, then echoes a `CommandSendReceipt` for every
/// `CommandSend` it sees.
async fn serve_producer_receipt_broker_conn(stream: &mut TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "stats-hist-live-producer-broker".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out_buf, &cmd);
                }
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        let cmd = pb::BaseCommand {
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
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::ProducerSuccess as i32,
                            producer_success: Some(pb::CommandProducerSuccess {
                                request_id: p.request_id,
                                producer_name: "stats-hist-live-producer".to_owned(),
                                last_sequence_id: Some(-1),
                                schema_version: None,
                                topic_epoch: Some(0),
                                producer_ready: Some(true),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                pb::base_command::Type::Send => {
                    if let Some(s) = &frame.command.send {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::SendReceipt as i32,
                            send_receipt: Some(pb::CommandSendReceipt {
                                producer_id: s.producer_id,
                                sequence_id: s.sequence_id,
                                message_id: Some(pb::MessageIdData {
                                    ledger_id: 3,
                                    entry_id: 1,
                                    partition: None,
                                    batch_index: None,
                                    ack_set: Vec::new(),
                                    batch_size: None,
                                    first_chunk_message_id: None,
                                }),
                                highest_sequence_id: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out_buf, &cmd);
                    }
                }
                _ => {}
            }
        }
        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }
        if matches!(stream.read_buf(&mut read_buf).await, Ok(0) | Err(_)) {
            return;
        }
    }
}

async fn spawn_producer_receipt_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                serve_producer_receipt_broker_conn(&mut stream).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// Live-facade sibling of `producer_send_latency_histogram_reflects_receipts`:
/// drives a real `Producer` over a loopback broker and calls
/// `Producer::send_latency_histogram` (producer.rs) — the runtime facade
/// delegation the `ConnectionShared`-level test above never touches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_send_latency_histogram_accessor_reflects_receipts_via_live_facade() {
    let url = spawn_producer_receipt_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/stats-hist-accessor-live-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    assert!(
        producer
            .send_latency_histogram()
            .is_none_or(|h| h.is_empty()),
        "no send_latency_hist samples before any CommandSendReceipt"
    );

    tokio::time::timeout(
        HANG_GUARD,
        producer.send(OutgoingMessage {
            payload: bytes::Bytes::from_static(b"hi"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 2,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        }),
    )
    .await
    .expect("send did not time out")
    .expect("send ok");

    let hist = producer
        .send_latency_histogram()
        .expect("send_latency_histogram initialised after the receipt");
    assert_eq!(
        hist.len(),
        1,
        "the live facade accessor must reflect the one applied receipt"
    );

    client.close().await;
}
