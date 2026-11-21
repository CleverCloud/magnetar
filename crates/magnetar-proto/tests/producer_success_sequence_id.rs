// SPDX-License-Identifier: Apache-2.0

//! `CommandProducerSuccess` must not move `last_sequence_id_published` backwards.
//!
//! `ProducerState::set_initial_sequence_id` stamps the counter with `initial_sequence_id - 1` "so
//! `producer_last_sequence_id_published()` returns the resume point until the first ack lands",
//! and `apply_receipt` advances it to the highest sequence id the broker acknowledged. The
//! `CommandProducerSuccess` arm then overwrites it unconditionally with the broker's
//! `last_sequence_id`, on the first attach and on every re-attach. A broker that keeps no
//! deduplication state answers `-1`, so the resume point is gone before the `Producer` handle is
//! even returned, and an acknowledged id moves backwards each time the producer re-attaches.
//!
//! Layer (a) of the ADR-0024 four-layer test policy. Mirrored by the `magnetar-runtime-tokio` and
//! `magnetar-runtime-moonpool` tests of the same name and by the `magnetar-differential`
//! equivalence scenario.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, ProducerHandle, SequenceId,
    encode_command, pb,
};

fn connected_frame() -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandConnected");
    buf
}

fn producer_success_frame(request_id: u64, last_sequence_id: i64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "producer-success-sequence-id".to_owned(),
                last_sequence_id: Some(last_sequence_id),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandProducerSuccess");
    buf
}

fn send_receipt_frame(producer: ProducerHandle, sequence_id: SequenceId) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: producer.0,
                sequence_id: sequence_id.0,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: sequence_id.0,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandSendReceipt");
    buf
}

fn message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(b"sequenced"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 9,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn connected(now: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("apply Connected");
    let _ = conn.poll_transmit();
    conn
}

/// Stages `CommandProducer` for a new producer and returns its handle and open request id.
fn create_producer(
    conn: &mut Connection,
    initial_sequence_id: Option<u64>,
) -> (ProducerHandle, u64) {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-success-sequence-id".to_owned(),
        initial_sequence_id,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    (handle, request_id)
}

/// The broker acknowledges the attachment `request_id` with `broker_last`.
fn ack_producer_success(conn: &mut Connection, now: Instant, request_id: u64, broker_last: i64) {
    conn.handle_bytes(now, &producer_success_frame(request_id, broker_last))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
}

/// Opens a producer and has the broker acknowledge the attachment with `broker_last`.
fn open_producer(
    conn: &mut Connection,
    now: Instant,
    initial_sequence_id: Option<u64>,
    broker_last: i64,
) -> ProducerHandle {
    let (handle, request_id) = create_producer(conn, initial_sequence_id);
    ack_producer_success(conn, now, request_id, broker_last);
    handle
}

/// A producer resumed from a known checkpoint reports that checkpoint as its last published
/// sequence id until the first receipt lands (`set_initial_sequence_id`). A broker without
/// deduplication state answers `-1` on the attach, which must not discard it.
#[test]
fn producer_success_keeps_the_initial_sequence_id_resume_point() {
    let now = Instant::now();
    let mut conn = connected(now);

    let (handle, request_id) = create_producer(&mut conn, Some(100));
    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        99,
        "before the attachment is acknowledged the producer must report its resume point"
    );

    ack_producer_success(&mut conn, now, request_id, -1);

    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        99,
        "a producer opened with initial_sequence_id=100 must keep reporting its resume point \
         (99) once the broker acknowledged the attachment with last_sequence_id=-1"
    );
}

/// A receipt advances the counter to the acknowledged id; the re-attach of the same producer on
/// a new session, answered with `-1` by a broker without deduplication state, must not move it
/// backwards.
#[test]
fn producer_success_on_reattach_does_not_regress_the_acknowledged_sequence_id() {
    let now = Instant::now();
    let mut conn = connected(now);
    let handle = open_producer(&mut conn, now, None, -1);

    let seq = conn.send(handle, message(), 0, now).expect("queue send");
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &send_receipt_frame(handle, seq))
        .expect("apply SendReceipt");
    while conn.poll_event().is_some() {}
    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        seq.0 as i64,
        "the receipt must advance the counter to the acknowledged sequence id"
    );

    // The transport drops; the supervisor re-handshakes and re-attaches the producer.
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("apply reconnect Connected");
    let _ = conn.poll_transmit();
    let reattach_ids = conn.rebuild_producers();
    assert_eq!(
        reattach_ids.len(),
        1,
        "the live producer must be re-attached"
    );
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success_frame(reattach_ids[0].0, -1))
        .expect("apply re-attach ProducerSuccess");
    while conn.poll_event().is_some() {}

    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        seq.0 as i64,
        "re-attaching must not move last_sequence_id_published backwards from the id the broker \
         already acknowledged ({}) to the broker's last_sequence_id=-1",
        seq.0
    );
}

/// Guard for the fix, green today: a broker that does keep deduplication state reports the
/// highest sequence id it persisted for the producer name, and a fresh producer with no resume
/// point of its own adopts it.
#[test]
fn producer_success_adopts_the_brokers_last_sequence_id_on_a_fresh_producer() {
    let now = Instant::now();
    let mut conn = connected(now);

    let handle = open_producer(&mut conn, now, None, 41);

    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        41,
        "a fresh producer must report the last_sequence_id the broker persisted for its name"
    );
}
