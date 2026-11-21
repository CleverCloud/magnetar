// SPDX-License-Identifier: Apache-2.0

//! A `CommandSendReceipt` must be applied in time bounded by the producer's own in-flight sends,
//! never by a number the broker wrote, exercised through the tokio engine's
//! [`ConnectionShared`] state. Mirrors the matching `magnetar-runtime-moonpool` test, its
//! `magnetar-proto` unit test (`crates/magnetar-proto/tests/send_receipt_range.rs`) and the
//! `magnetar-differential` equivalence scenario.
//!
//! The driver applies every inbound frame under `shared.inner.lock()`. The `SendReceipt` arm
//! fans a receipt out over `[lowest, highest_sequence_id]` one sequence id at a time, and nothing
//! but `highest >= lowest && highest != u64::MAX` bounds `highest`, so a receipt that claims a
//! range far wider than anything the producer sent keeps the driver in the loop with the
//! connection mutex held. Every user call (`send`, `flush`, `close`, ...) takes that mutex, so
//! the client stops, and no timeout can fire because the same thread would have to run it.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle, SequenceId,
    encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// How long a user call may wait for the connection mutex while the driver applies one receipt.
/// The correct client holds it for microseconds; the bound only keeps a failing run from hanging.
const DEADLINE: Duration = Duration::from_secs(10);

/// How far past its own sequence id the hostile receipt claims the broker persisted: at tens of
/// nanoseconds per sequence id the unbounded walk needs hours.
const CLAIMED_SPAN: u64 = 1 << 40;

fn producer_success(request_id: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "send-receipt-range".to_owned(),
                last_sequence_id: Some(-1),
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

/// The broker's `CommandSendReceipt` for `sequence_id`, claiming it persisted everything up to
/// and including `highest_sequence_id`.
fn ranged_receipt(
    producer: ProducerHandle,
    sequence_id: SequenceId,
    highest_sequence_id: u64,
) -> BytesMut {
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
                highest_sequence_id: Some(highest_sequence_id),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandSendReceipt");
    buf
}

fn message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(b"ranged"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 6,
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

fn open_producer(shared: &ConnectionShared, now: Instant, batching: bool) -> ProducerHandle {
    let mut conn = shared.inner.lock();
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/send-receipt-range".to_owned(),
        enable_batching: batching,
        max_batch_size_bytes: 4096,
        max_messages_in_batch: 100,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success(request_id))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
    handle
}

/// A receipt claiming a range far wider than the producer's in-flight sends must not keep the
/// connection mutex away from the rest of the client.
#[test]
fn send_receipt_with_an_unbounded_highest_sequence_id_returns_promptly() {
    let now = Instant::now();
    let shared = connected_shared(now);
    let producer = open_producer(&shared, now, false);
    let seq = {
        let mut conn = shared.inner.lock();
        let seq = conn.send(producer, message(), 0, now).expect("queue send");
        let _ = conn.poll_transmit();
        seq
    };

    // The driver: applies the broker's frame under the connection mutex. If the receipt spins it
    // is abandoned and dies with the test process.
    let driver_holds_the_lock = Arc::new(AtomicBool::new(false));
    let driver_flag = Arc::clone(&driver_holds_the_lock);
    let driver_shared = Arc::clone(&shared);
    std::thread::spawn(move || {
        let mut conn = driver_shared.inner.lock();
        driver_flag.store(true, Ordering::SeqCst);
        // Whether the receipt is applied, clamped or refused is the fix's call: only the time it
        // holds the mutex is pinned here.
        let _ = conn.handle_bytes(now, &ranged_receipt(producer, seq, seq.0 + CLAIMED_SPAN));
    });
    let started = Instant::now();
    while !driver_holds_the_lock.load(Ordering::SeqCst) && started.elapsed() < DEADLINE {
        std::thread::yield_now();
    }
    assert!(
        driver_holds_the_lock.load(Ordering::SeqCst),
        "the driver thread never took the connection mutex"
    );

    // A user call: it needs the same mutex.
    assert!(
        shared.inner.try_lock_for(DEADLINE).is_some(),
        "the connection mutex was still held {DEADLINE:?} after the driver began applying a \
         CommandSendReceipt whose highest_sequence_id lies {CLAIMED_SPAN} past the only send in \
         flight: the SendReceipt arm walks the broker-supplied range with the mutex held"
    );
}

/// Guard for the fix, green today: a genuine batch receipt still resolves every member of the
/// range it names.
#[test]
fn send_receipt_still_resolves_every_member_of_a_real_batch() {
    let now = Instant::now();
    let shared = connected_shared(now);
    let producer = open_producer(&shared, now, true);
    let mut conn = shared.inner.lock();
    let first = conn.send(producer, message(), 0, now).expect("first");
    let second = conn.send(producer, message(), 0, now).expect("second");
    assert_eq!(
        conn.flush_producer(producer, 0, now),
        1,
        "the two messages leave as one batch frame"
    );
    let _ = conn.poll_transmit();

    conn.handle_bytes(now, &ranged_receipt(producer, first, second.0))
        .expect("apply the batch receipt");
    while conn.poll_event().is_some() {}

    for seq in [first, second] {
        assert!(
            matches!(
                conn.take_outcome(PendingOpKey::Send(producer, seq)),
                Some(OpOutcome::SendReceipt { .. })
            ),
            "the batch receipt for [{first:?}, {second:?}] must resolve member {seq:?}"
        );
    }
}
