// SPDX-License-Identifier: Apache-2.0

//! A closed producer must not keep a batch-flush deadline armed in `poll_timeout()`.
//!
//! A batching producer with a `batching_max_publish_delay` arms `first_added_at + delay` as soon
//! as its first message sits in the batch, and `Connection::poll_timeout` surfaces that deadline
//! for every producer slot. Closing the producer leaves the unflushed batch in place until the
//! broker acknowledges the close, and `flush_batch` returns at once for a closed producer without
//! clearing `first_added_at`. `handle_timeout` therefore selects the slot as due, flushes nothing,
//! and the deadline stays in the past: the driver computes a zero sleep, takes its timer arm,
//! locks the connection, calls `handle_timeout` again and loops, until the close ack, a reset or
//! `fail_all_pending` empties the batch. The broker answers a close only once the producer's
//! earlier publishes are persisted, so the loop lasts as long as that persistence takes.
//!
//! The test plays the driver's timer arm against the state machine under a bounded round budget,
//! so a regression fails here instead of spinning.
//!
//! Layer (a) of the ADR-0024 four-layer test policy. Mirrored by the `magnetar-runtime-tokio` and
//! `magnetar-runtime-moonpool` tests of the same name and by the `magnetar-differential`
//! equivalence scenario.

use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, ProducerHandle, encode_command, pb,
};

const PUBLISH_DELAY: Duration = Duration::from_millis(10);

/// Two publish delays after the first message: its batch deadline is long expired.
const LATER: Duration = Duration::from_millis(20);

/// How many times the driver may re-enter its timer arm for one instant before the test calls it
/// a spin. The correct client needs none for a closed producer, and one at most to retire a
/// deadline.
const ROUND_BUDGET: usize = 8;

/// Which close entry point the producer used.
#[derive(Clone, Copy, Debug)]
enum Close {
    /// `Producer::close()`: the awaited `CommandCloseProducer`.
    Awaited,
    /// The last-clone drop guard: the fire-and-forget close.
    Forgotten,
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
                producer_name: "closed-batch-deadline".to_owned(),
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

/// A ready connection with one batching producer whose only message sits in the batch, waiting on
/// `batching_max_publish_delay`. Returns the instant of that message.
fn batching_producer_with_unflushed_batch(conn: &mut Connection) -> (ProducerHandle, Instant) {
    let t0 = Instant::now();
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("apply Connected");
    let _ = conn.poll_transmit();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/closed-batch-deadline".to_owned(),
        enable_batching: true,
        batching_max_publish_delay: Some(PUBLISH_DELAY),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_frame(create_rid))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}

    conn.send(
        producer,
        OutgoingMessage {
            payload: Bytes::from_static(b"waiting in the batch"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 20,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        },
        0,
        t0,
    )
    .expect("queue send");
    let _ = conn.poll_transmit();
    (producer, t0)
}

/// Play the driver's timer arm at `now`: while `poll_timeout()` is not in the future, the driver
/// computes a zero sleep, takes the arm and calls `handle_timeout(now)`. Returns how many rounds
/// that took, stopping at `ROUND_BUDGET`.
fn timer_rounds_at(conn: &mut Connection, now: Instant) -> usize {
    let mut rounds = 0;
    while conn.poll_timeout().is_some_and(|d| d <= now) && rounds < ROUND_BUDGET {
        conn.handle_timeout(now);
        rounds += 1;
    }
    rounds
}

fn assert_no_spin_after_close(close: Close) {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(SystemTime::now),
    );
    let (producer, t0) = batching_producer_with_unflushed_batch(&mut conn);
    assert_eq!(
        conn.poll_timeout(),
        Some(t0 + PUBLISH_DELAY),
        "the batch deadline is armed while the producer is open"
    );

    // The broker has not acknowledged the close: it waits on the producer's earlier publishes.
    match close {
        Close::Awaited => conn.close_producer(producer),
        Close::Forgotten => conn.close_producer_forget(producer),
    };

    let now = t0 + LATER;
    let rounds = timer_rounds_at(&mut conn, now);
    assert!(
        rounds <= 1,
        "the driver's timer arm re-entered {rounds} times at one instant after a {close:?} \
         producer close: poll_timeout() = {:?} is still due at {now:?}",
        conn.poll_timeout()
    );
    assert!(
        conn.poll_timeout().is_none_or(|d| d > now),
        "a closed producer's batch deadline must not stay armed in the past; poll_timeout() = \
         {:?} at {now:?} ({close:?})",
        conn.poll_timeout()
    );
}

#[test]
fn awaited_close_does_not_leave_the_batch_deadline_armed_in_the_past() {
    assert_no_spin_after_close(Close::Awaited);
}

#[test]
fn forgotten_close_does_not_leave_the_batch_deadline_armed_in_the_past() {
    assert_no_spin_after_close(Close::Forgotten);
}

/// Control: an OPEN producer's expired batch deadline is retired by the first timer round, which
/// flushes the batch. Pins the harness and keeps a fix from dropping the deadline altogether.
#[test]
fn open_producer_batch_deadline_is_retired_by_one_timer_round() {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(SystemTime::now),
    );
    let (_producer, t0) = batching_producer_with_unflushed_batch(&mut conn);
    assert_eq!(conn.poll_timeout(), Some(t0 + PUBLISH_DELAY));

    let now = t0 + LATER;
    assert_eq!(
        timer_rounds_at(&mut conn, now),
        1,
        "one round flushes the expired batch"
    );
    assert!(
        conn.poll_timeout().is_none_or(|d| d > now),
        "the flushed batch leaves no due deadline; poll_timeout() = {:?}",
        conn.poll_timeout()
    );
}
