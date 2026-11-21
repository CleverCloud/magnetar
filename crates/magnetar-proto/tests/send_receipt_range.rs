// SPDX-License-Identifier: Apache-2.0

//! A `CommandSendReceipt` must be applied in time bounded by the producer's own in-flight sends,
//! never by a number the broker wrote.
//!
//! A batching producer folds its per-message publishes into one `CommandSend` carrying
//! `sequence_id = lowest` and `highest_sequence_id = highest`, and the broker's receipt echoes
//! that range, so the `SendReceipt` arm fans it out over `[lowest, highest]`. The arm walks the
//! range one sequence id at a time and bounds `highest` by nothing but `highest >= lowest` and
//! `highest != u64::MAX` (the Java `-1` sentinel), so a receipt that claims a range far wider
//! than anything the producer sent keeps the thread in the loop for as long as the range is
//! long, with the connection mutex and the producer slot lock both held. Every other operation
//! of the client waits on one of the two, so the whole client stops, and no timeout or
//! heartbeat can fire because the same thread would have to run it.
//!
//! The receipt is applied on a worker thread and the test waits for it with a deadline, so a
//! regression fails here instead of hanging the run.
//!
//! Layer (a) of the ADR-0024 four-layer test policy. Mirrored by the `magnetar-runtime-tokio` and
//! `magnetar-runtime-moonpool` tests of the same name and by the `magnetar-differential`
//! equivalence scenario.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle,
    SequenceId, encode_command, pb,
};

/// How long applying one receipt may take. The correct client needs microseconds; the bound only
/// keeps a failing run from hanging.
const DEADLINE: Duration = Duration::from_secs(10);

/// How far past its own sequence id the hostile receipt claims the broker persisted: at tens of
/// nanoseconds per sequence id the unbounded walk needs hours.
const CLAIMED_SPAN: u64 = 1 << 40;

/// The worker's state: still applying the receipt, returned from it, or unwound.
const RUNNING: u8 = 0;
const RETURNED: u8 = 1;
const PANICKED: u8 = 2;

/// Marks the worker `PANICKED` when it unwinds before reporting `RETURNED`.
struct UnwindMark(Arc<AtomicU8>);

impl Drop for UnwindMark {
    fn drop(&mut self) {
        let _ = self
            .0
            .compare_exchange(RUNNING, PANICKED, Ordering::SeqCst, Ordering::SeqCst);
    }
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
fn ranged_receipt_frame(
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

fn connected(now: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("apply Connected");
    let _ = conn.poll_transmit();
    conn
}

fn open_producer(conn: &mut Connection, now: Instant, batching: bool) -> ProducerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/send-receipt-range".to_owned(),
        enable_batching: batching,
        max_batch_size_bytes: 4096,
        max_messages_in_batch: 100,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &producer_success_frame(request_id))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
    handle
}

/// A receipt claiming a range far wider than the producer's in-flight sends must be applied in
/// bounded time.
#[test]
fn send_receipt_with_an_unbounded_highest_sequence_id_returns_promptly() {
    let state = Arc::new(AtomicU8::new(RUNNING));
    let worker_state = Arc::clone(&state);
    // The worker owns the connection; if the receipt spins, it is abandoned and dies with the
    // test process.
    std::thread::spawn(move || {
        let _mark = UnwindMark(Arc::clone(&worker_state));
        let now = Instant::now();
        let mut conn = connected(now);
        let producer = open_producer(&mut conn, now, false);
        let seq = conn.send(producer, message(), 0, now).expect("queue send");
        let _ = conn.poll_transmit();

        // One send is in flight; the broker claims it persisted `CLAIMED_SPAN` more. Whether the
        // receipt is applied, clamped or refused is the fix's call: only the time it takes is
        // pinned here.
        let _ = conn.handle_bytes(
            now,
            &ranged_receipt_frame(producer, seq, seq.0 + CLAIMED_SPAN),
        );
        worker_state.store(RETURNED, Ordering::SeqCst);
    });

    let started = Instant::now();
    while state.load(Ordering::SeqCst) == RUNNING && started.elapsed() < DEADLINE {
        std::thread::sleep(Duration::from_millis(10));
    }
    match state.load(Ordering::SeqCst) {
        RETURNED => {}
        PANICKED => panic!("the worker thread panicked before the receipt was applied"),
        _ => panic!(
            "applying a CommandSendReceipt whose highest_sequence_id lies {CLAIMED_SPAN} past the \
             only send in flight was still running after {DEADLINE:?}: the SendReceipt arm walks \
             the broker-supplied range with the connection mutex and the producer slot lock held"
        ),
    }
}

/// Guard for the fix, green today: a genuine batch receipt still resolves every member of the
/// range it names.
#[test]
fn send_receipt_still_resolves_every_member_of_a_real_batch() {
    let now = Instant::now();
    let mut conn = connected(now);
    let producer = open_producer(&mut conn, now, true);
    let first = conn.send(producer, message(), 0, now).expect("first");
    let second = conn.send(producer, message(), 0, now).expect("second");
    assert_eq!(
        conn.flush_producer(producer, 0, now),
        1,
        "the two messages leave as one batch frame"
    );
    let _ = conn.poll_transmit();

    conn.handle_bytes(now, &ranged_receipt_frame(producer, first, second.0))
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
