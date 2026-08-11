// SPDX-License-Identifier: Apache-2.0

//! Differential regressions for reconnect safety issues #395-#398 and #403.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, CreateProducerRequest, MessageId, OpOutcome,
    PendingOpKey, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};

fn connect(conn: &mut Connection, at: Instant) {
    conn.begin_handshake().expect("handshake");
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    conn.handle_bytes(at, &frame).expect("Connected");
    let _ = conn.poll_event();
}

fn tokio_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn producer_success(conn: &mut Connection, request_id: u64, at: Instant) {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "reconnect-safety".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode ProducerSuccess");
    conn.handle_bytes(at, &frame).expect("ProducerSuccess");
    while conn.poll_event().is_some() {}
}

fn subscribe_success(conn: &mut Connection, request_id: u64, at: Instant) {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
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
    conn.handle_bytes(at, &frame).expect("subscribe Success");
    while conn.poll_event().is_some() {}
}

fn batch_reset_projection(conn: &mut Connection, at: Instant) -> Vec<(i32, String)> {
    connect(conn, at);
    let request_id = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/diff-batch-reset".to_owned(),
        enable_batching: true,
        max_batch_size_bytes: 4096,
        max_messages_in_batch: 100,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    producer_success(conn, request_id, at);
    let mut sequences = Vec::new();
    for payload in [b"a".as_slice(), b"b".as_slice()] {
        sequences.push(
            conn.send(
                producer,
                OutgoingMessage {
                    payload: Bytes::copy_from_slice(payload),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                at,
            )
            .expect("queue batch member"),
        );
    }
    assert_eq!(conn.flush_producer(producer, 0, at), 1);
    let _ = conn.poll_transmit();
    conn.reset();
    sequences
        .into_iter()
        .map(
            |sequence_id| match conn.take_outcome(PendingOpKey::Send(producer, sequence_id)) {
                Some(OpOutcome::SendError { code, message, .. }) => (code, message),
                other => panic!("expected SendError, got {other:?}"),
            },
        )
        .collect()
}

fn deliver_batch(conn: &mut Connection, consumer_id: u64, at: Instant) {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 11,
                entry_id: 7,
                ..Default::default()
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "diff".to_owned(),
        sequence_id: 0,
        publish_time: 0,
        num_messages_in_batch: Some(4),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for _ in 0..4 {
        let single = pb::SingleMessageMetadata {
            payload_size: 1,
            ..Default::default()
        };
        let len = prost::Message::encoded_len(&single);
        body.extend_from_slice(&(len as u32).to_be_bytes());
        prost::Message::encode(&single, &mut body).expect("encode single metadata");
        body.extend_from_slice(b"x");
    }
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &command, &metadata, &body).expect("encode batch");
    conn.handle_bytes(at, &frame).expect("deliver batch");
    while conn.poll_event().is_some() {}
}

fn stale_batch_ack_projection(conn: &mut Connection, at: Instant) -> Vec<i64> {
    connect(conn, at);
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/diff-batch-ack".to_owned(),
        subscription: "diff-batch-ack".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    deliver_batch(conn, consumer.0, at);
    conn.reset();
    let _ = conn.ack(
        consumer,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 11,
                entry_id: 7,
                partition: -1,
                batch_index: 1,
                batch_size: 4,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        at,
    );
    let mut wire = conn.poll_transmit();
    decode_one(&mut wire)
        .expect("CommandAck frame")
        .command
        .ack
        .expect("CommandAck")
        .message_id[0]
        .ack_set
        .clone()
}

fn resume_projection(
    conn: &mut Connection,
    at: Instant,
    durable: bool,
) -> Option<pb::MessageIdData> {
    connect(conn, at);
    let original_start = MessageId {
        ledger_id: 1,
        entry_id: 2,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    let request_id = conn.peek_next_request_id_for_test();
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/diff-resume".to_owned(),
        subscription: "diff-resume".to_owned(),
        sub_type: pb::command_subscribe::SubType::KeyShared,
        durable,
        start_message_id: Some(original_start),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    subscribe_success(conn, request_id, at);
    let _ = conn.ack(
        consumer,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 9,
                entry_id: 9,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        at,
    );
    let _ = conn.poll_transmit();
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    conn.handle_bytes(at, &frame).expect("reconnected");
    let _ = conn.poll_transmit();
    assert_eq!(conn.rebuild_consumers().len(), 1);
    let mut wire = conn.poll_transmit();
    decode_one(&mut wire)
        .expect("CommandSubscribe")
        .command
        .subscribe
        .expect("subscribe")
        .start_message_id
}

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn tokio_memory_projection() -> (bool, usize, bool) {
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig {
        memory_limit_bytes: 64,
        memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
        ..ConnectionConfig::default()
    });
    shared.try_reserve_memory(64).expect("fill budget");
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wake.clone());
    let parked = shared.try_reserve_memory_or_register(64, &waker).is_err();
    shared.release_memory(64);
    let wake_count = wake.0.load(Ordering::SeqCst);
    let progressed = shared.try_reserve_memory(64).is_ok();
    (parked, wake_count, progressed)
}

fn moonpool_memory_projection() -> (bool, usize, bool) {
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig {
        memory_limit_bytes: 64,
        memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
        ..ConnectionConfig::default()
    });
    shared.try_reserve_memory(64).expect("fill budget");
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wake.clone());
    let parked = shared.try_reserve_memory_or_register(64, &waker).is_err();
    shared.release_memory(64);
    let wake_count = wake.0.load(Ordering::SeqCst);
    let progressed = shared.try_reserve_memory(64).is_ok();
    (parked, wake_count, progressed)
}

#[test]
fn engines_agree_that_non_replayable_batches_fail_on_reset() {
    let tokio = tokio_projection(batch_reset_projection);
    let moonpool = moonpool_projection(batch_reset_projection);
    assert_eq!(tokio, moonpool);
    assert_eq!(
        tokio,
        vec![
            (
                -1,
                "batched send cannot be replayed after connection reset".to_owned(),
            );
            2
        ]
    );
}

#[test]
fn engines_agree_on_conservative_stale_batch_ack() {
    let tokio = tokio_projection(stale_batch_ack_projection);
    let moonpool = moonpool_projection(stale_batch_ack_projection);
    assert_eq!(tokio, moonpool);
    assert_eq!(tokio, vec![0b1101]);
}

#[test]
fn engines_agree_that_reattach_uses_only_authoritative_start_positions() {
    let tokio_durable = tokio_projection(|conn, at| resume_projection(conn, at, true));
    let moonpool_durable = moonpool_projection(|conn, at| resume_projection(conn, at, true));
    assert_eq!(tokio_durable, moonpool_durable);
    assert!(tokio_durable.is_none());

    let tokio_non_durable = tokio_projection(|conn, at| resume_projection(conn, at, false));
    let moonpool_non_durable = moonpool_projection(|conn, at| resume_projection(conn, at, false));
    assert_eq!(tokio_non_durable, moonpool_non_durable);
    let start = tokio_non_durable.expect("non-durable original start position");
    assert_eq!((start.ledger_id, start.entry_id), (1, 2));
}

#[test]
fn engines_agree_on_producer_block_progress() {
    let tokio = tokio_memory_projection();
    let moonpool = moonpool_memory_projection();
    assert_eq!(tokio, moonpool);
    assert_eq!(tokio, (true, 1, true));
}
