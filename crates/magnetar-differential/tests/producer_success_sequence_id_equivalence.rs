// SPDX-License-Identifier: Apache-2.0

//! `CommandProducerSuccess` must not move `last_sequence_id_published` backwards, identically on
//! both engines (ADR-0024 layer (d)).
//!
//! `ProducerState::set_initial_sequence_id` stamps the counter with `initial_sequence_id - 1` so
//! the resume point is what the producer reports until the first ack lands, and `apply_receipt`
//! advances it to the highest acknowledged sequence id. The `CommandProducerSuccess` arm then
//! overwrites it unconditionally with the broker's `last_sequence_id`, on the first attach and on
//! every re-attach, so a broker without deduplication state (`-1`) erases the resume point and
//! regresses an acknowledged id. Both engines wrap the same `magnetar-proto` state machine, so
//! they agree on the value today; the scenarios also pin the value they must agree on.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, ProducerHandle, SequenceId,
    encode_command, pb,
};

fn connected_frame() -> BytesMut {
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
    frame
}

fn producer_success_frame(request_id: u64, last_sequence_id: i64) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
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
    .expect("encode ProducerSuccess");
    frame
}

fn send_receipt_frame(producer: ProducerHandle, sequence_id: SequenceId) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
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
    .expect("encode SendReceipt");
    frame
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

fn connect(conn: &mut Connection, at: Instant) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(at, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
}

fn open_producer(
    conn: &mut Connection,
    at: Instant,
    initial_sequence_id: Option<u64>,
    broker_last: i64,
) -> ProducerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-success-sequence-id".to_owned(),
        initial_sequence_id,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(at, &producer_success_frame(request_id, broker_last))
        .expect("ProducerSuccess");
    while conn.poll_event().is_some() {}
    producer
}

/// A producer resumed from `initial_sequence_id = 100` attaches against a broker that answers
/// `last_sequence_id = -1`. Returns the counter after the attachment.
fn resume_point_after_attach(conn: &mut Connection, at: Instant) -> Vec<i64> {
    connect(conn, at);
    let producer = open_producer(conn, at, Some(100), -1);
    vec![conn.producer_last_sequence_id_published(producer)]
}

/// One send is acknowledged, the transport drops and the producer re-attaches against a broker
/// that answers `last_sequence_id = -1`. Returns the counter after the receipt and after the
/// re-attach.
fn counter_across_reattach(conn: &mut Connection, at: Instant) -> Vec<i64> {
    connect(conn, at);
    let producer = open_producer(conn, at, None, -1);

    let seq = conn.send(producer, message(), 0, at).expect("queue send");
    let _ = conn.poll_transmit();
    conn.handle_bytes(at, &send_receipt_frame(producer, seq))
        .expect("SendReceipt");
    while conn.poll_event().is_some() {}
    let after_receipt = conn.producer_last_sequence_id_published(producer);

    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(at, &connected_frame())
        .expect("reconnect Connected");
    let _ = conn.poll_transmit();
    let reattach_ids = conn.rebuild_producers();
    let _ = conn.poll_transmit();
    for request_id in reattach_ids {
        conn.handle_bytes(at, &producer_success_frame(request_id.0, -1))
            .expect("re-attach ProducerSuccess");
    }
    while conn.poll_event().is_some() {}
    vec![
        after_receipt,
        conn.producer_last_sequence_id_published(producer),
    ]
}

/// A fresh producer attaches against a broker that kept deduplication state for its name.
/// Returns the counter after the attachment.
fn adopted_after_attach(conn: &mut Connection, at: Instant) -> Vec<i64> {
    connect(conn, at);
    let producer = open_producer(conn, at, None, 41);
    vec![conn.producer_last_sequence_id_published(producer)]
}

fn assert_engines_report(
    scenario: &str,
    run: impl Fn(&mut Connection, Instant) -> Vec<i64>,
    expected: &[i64],
) {
    let tokio = tokio_projection(&run);
    let moonpool = moonpool_projection(&run);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on last_sequence_id_published ({scenario})"
    );
    assert_eq!(
        tokio, expected,
        "last_sequence_id_published must follow the expected trace on both engines ({scenario})"
    );
}

#[test]
fn engines_keep_the_initial_sequence_id_resume_point_across_producer_success() {
    assert_engines_report(
        "initial_sequence_id=100, broker last_sequence_id=-1",
        resume_point_after_attach,
        &[99],
    );
}

#[test]
fn engines_do_not_regress_the_acknowledged_sequence_id_on_reattach() {
    assert_engines_report(
        "receipt for sequence 0, then a re-attach answered with last_sequence_id=-1",
        counter_across_reattach,
        &[0, 0],
    );
}

#[test]
fn engines_adopt_the_brokers_last_sequence_id_on_a_fresh_producer() {
    assert_engines_report(
        "no initial_sequence_id, broker last_sequence_id=41",
        adopted_after_attach,
        &[41],
    );
}
