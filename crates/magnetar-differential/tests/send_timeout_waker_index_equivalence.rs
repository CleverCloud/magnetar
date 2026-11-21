// SPDX-License-Identifier: Apache-2.0

//! A `send_timeout` sweep that expires only the oldest in-flight sends must not misroute the
//! wakers of the sends that survive it, identically on both engines (ADR-0024 layer (d)).
//!
//! The producer's `sequence id -> position` index is not shifted when the sweep pops the front of
//! the in-flight queue, so a survivor's recorded position is one too high per expired send and
//! `register_waker` parks the waker of a send polled after the sweep on the send behind it,
//! overwriting that send's waker. The receipt then wakes the wrong task and the victim's task is
//! never woken. Both engines register through the shared `Connection::register_waker`, so they
//! must agree on which waker each receipt wakes, and each receipt must wake the waker of the send
//! it acknowledges.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

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

fn producer_success_frame(request_id: u64) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
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
        payload: Bytes::from_static(b"in flight"),
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

/// How often `(b's waker, c's waker)` had been woken after the receipt of `b`, and after the
/// receipt of `c`.
type WakeCounts = ((usize, usize), (usize, usize));

/// A producer has sends `a` (oldest), `b` and `c` in flight; `b` and `c` were enqueued ten seconds
/// after `a`. `c`'s future was polled before the sweep and parked a waker; the sweep then expires
/// `a` alone, and `b`'s future is polled for the first time after it and parks its own. The
/// broker then acknowledges `b` and `c`.
fn wake_counts_after_a_partial_sweep(conn: &mut Connection, t0: Instant) -> WakeCounts {
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

    let (b_count, b_waker) = counting_waker();
    conn.register_waker(PendingOpKey::Send(producer, b), b_waker);

    conn.handle_bytes(swept_at, &send_receipt_frame(producer, b))
        .expect("receipt for b");
    let after_b = (wakes(&b_count), wakes(&c_count));
    conn.handle_bytes(swept_at, &send_receipt_frame(producer, c))
        .expect("receipt for c");
    let after_c = (wakes(&b_count), wakes(&c_count));
    (after_b, after_c)
}

#[test]
fn engines_wake_each_survivor_of_a_partial_send_timeout_sweep_by_its_own_receipt() {
    let tokio = tokio_projection(wake_counts_after_a_partial_sweep);
    let moonpool = moonpool_projection(wake_counts_after_a_partial_sweep);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on which waker each receipt wakes after a partial send-timeout sweep"
    );
    assert_eq!(
        tokio,
        ((1, 0), (1, 1)),
        "each receipt must wake the waker of the send it acknowledges and no other \
         ((b, c) wake counts after the receipt of b, then of c); both engines report {tokio:?}"
    );
}
