// SPDX-License-Identifier: Apache-2.0

//! `CommandProducerSuccess` must not move `last_sequence_id_published` backwards, exercised
//! through the moonpool engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-tokio` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/producer_success_sequence_id.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! `ProducerState::set_initial_sequence_id` stamps the counter with `initial_sequence_id - 1` so
//! the resume point is what the producer reports until the first ack lands, and `apply_receipt`
//! advances it to the highest acknowledged sequence id. The `CommandProducerSuccess` arm then
//! overwrites it unconditionally with the broker's `last_sequence_id`, on the first attach and on
//! every re-attach. A broker that keeps no deduplication state answers `-1`, so the resume point
//! is gone before the `Producer` handle is returned, and an acknowledged id moves backwards each
//! time the producer re-attaches. The engine's `Producer::last_sequence_id_published` reads that
//! slot field.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, ProducerHandle, SequenceId, encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

fn producer_success(request_id: u64, last_sequence_id: i64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "magnetar-test-producer".to_owned(),
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

fn send_receipt(producer: ProducerHandle, sequence_id: SequenceId) -> BytesMut {
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

fn connected_shared(now: Instant) -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(now, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }
    shared
}

/// A producer resumed from a known checkpoint reports that checkpoint as its last published
/// sequence id until the first receipt lands. A broker without deduplication state answers `-1`
/// on the attach, which must not discard it.
#[test]
fn producer_success_keeps_the_initial_sequence_id_resume_point() {
    let now = Instant::now();
    let shared = connected_shared(now);
    let mut conn = shared.inner.lock();

    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-success-sequence-id".to_owned(),
        initial_sequence_id: Some(100),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        99,
        "before the attachment is acknowledged the producer must report its resume point"
    );

    conn.handle_bytes(now, &producer_success(request_id, -1))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}

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
    let shared = connected_shared(now);
    let mut conn = shared.inner.lock();

    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-success-sequence-id".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success(request_id, -1))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}

    let seq = conn.send(handle, message(), 0, now).expect("queue send");
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &send_receipt(handle, seq))
        .expect("apply SendReceipt");
    while conn.poll_event().is_some() {}
    let acknowledged = i64::try_from(seq.0).expect("sequence id fits i64");
    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        acknowledged,
        "the receipt must advance the counter to the acknowledged sequence id"
    );

    // The transport drops; the supervisor re-handshakes and re-attaches the producer.
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("reconnect Connected");
    let _ = conn.poll_transmit();
    let reattach_ids = conn.rebuild_producers();
    assert_eq!(
        reattach_ids.len(),
        1,
        "the live producer must be re-attached"
    );
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success(reattach_ids[0].0, -1))
        .expect("apply re-attach ProducerSuccess");
    while conn.poll_event().is_some() {}

    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        acknowledged,
        "re-attaching must not move last_sequence_id_published backwards from the id the broker \
         already acknowledged ({acknowledged}) to the broker's last_sequence_id=-1"
    );
}

/// Guard for the fix, green today: a broker that does keep deduplication state reports the
/// highest sequence id it persisted for the producer name, and a fresh producer with no resume
/// point of its own adopts it.
#[test]
fn producer_success_adopts_the_brokers_last_sequence_id_on_a_fresh_producer() {
    let now = Instant::now();
    let shared = connected_shared(now);
    let mut conn = shared.inner.lock();

    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/producer-success-sequence-id".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success(request_id, 41))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}

    assert_eq!(
        conn.producer_last_sequence_id_published(handle),
        41,
        "a fresh producer must report the last_sequence_id the broker persisted for its name"
    );
}
