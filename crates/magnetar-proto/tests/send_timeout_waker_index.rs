// SPDX-License-Identifier: Apache-2.0

//! A `send_timeout` sweep that expires only the OLDEST in-flight sends must not misroute the
//! wakers of the sends that survive it.
//!
//! `ProducerState` keeps its in-flight sends in a queue and a `sequence id -> position` index
//! beside it. Every removal from the middle of the queue (`apply_receipt`, `apply_send_error`)
//! shifts the index left, but the `send_timeout` sweep pops the front of the queue and only drops
//! the popped keys, so every survivor's recorded position is then one too high per expired send.
//! `register_waker` trusts that position without checking the sequence id, so a survivor's future
//! that registers (or re-registers) after the sweep parks its waker on the send BEHIND it and
//! overwrites the waker that send's own future had parked. The receipt then wakes the wrong task
//! and the victim's task is never woken: a send that completed stays pending on a healthy
//! connection.
//!
//! Layer (a) of the ADR-0024 four-layer test policy. Mirrored by the `magnetar-runtime-tokio` and
//! `magnetar-runtime-moonpool` tests of the same name and by the `magnetar-differential`
//! equivalence scenario.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle,
    SequenceId, encode_command, pb,
};

const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// A waker that counts how often the state machine woke it.
struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWake>, Waker) {
    let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker: Waker = Arc::clone(&inner).into();
    (inner, waker)
}

fn wakes(counter: &CountingWake) -> usize {
    counter.0.load(Ordering::SeqCst)
}

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

fn producer_success_frame(request_id: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "send-timeout-waker-index".to_owned(),
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
        payload: Bytes::from_static(b"in flight"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 9,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// A producer with sends `a` (oldest), `b` and `c` in flight; `b` and `c` were enqueued ten
/// seconds after `a`. `c`'s future was polled before the sweep and parked `c_waker`; the sweep
/// then expires `a` alone, and `b`'s future is polled for the first time after it and parks
/// `b_waker`. The broker then acknowledges `b` and `c`: each receipt must wake the waker of the
/// send it acknowledges, and no other.
#[test]
fn survivors_of_a_partial_send_timeout_sweep_are_woken_by_their_own_receipts() {
    let t0 = Instant::now();
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_transmit();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/send-timeout-waker-index".to_owned(),
        send_timeout: Some(SEND_TIMEOUT),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_frame(create_rid))
        .expect("ProducerSuccess");
    while conn.poll_event().is_some() {}

    let younger = t0 + Duration::from_secs(10);
    let a = conn.send(producer, message(), 0, t0).expect("queue a");
    let b = conn.send(producer, message(), 0, younger).expect("queue b");
    let c = conn.send(producer, message(), 0, younger).expect("queue c");
    let _ = conn.poll_transmit();
    assert_eq!(conn.producer_pending_count(producer), 3);

    // `c`'s future was polled before the sweep.
    let (c_count, c_waker) = counting_waker();
    conn.register_waker(PendingOpKey::Send(producer, c), c_waker);

    // Only `a` has outlived `send_timeout` (30 s from t0); `b` and `c` expire at t0 + 40 s.
    let swept_at = t0 + SEND_TIMEOUT + Duration::from_secs(1);
    conn.handle_timeout(swept_at);
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Send(producer, a)),
            Some(OpOutcome::SendError { .. })
        ),
        "precondition: the sweep expired the oldest send"
    );
    assert_eq!(
        conn.producer_pending_count(producer),
        2,
        "precondition: the sweep left `b` and `c` in flight"
    );

    // `b`'s future is polled for the first time after the sweep.
    let (b_count, b_waker) = counting_waker();
    conn.register_waker(PendingOpKey::Send(producer, b), b_waker);

    conn.handle_bytes(swept_at, &send_receipt_frame(producer, b))
        .expect("receipt for b");
    assert_eq!(
        (wakes(&b_count), wakes(&c_count)),
        (1, 0),
        "the receipt for `b` must wake `b`'s waker and nobody else's"
    );

    conn.handle_bytes(swept_at, &send_receipt_frame(producer, c))
        .expect("receipt for c");
    assert_eq!(
        (wakes(&b_count), wakes(&c_count)),
        (1, 1),
        "the receipt for `c` must wake `c`'s own waker, which `b`'s registration must not have \
         overwritten"
    );
}
