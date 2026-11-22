// SPDX-License-Identifier: Apache-2.0

//! A closed producer must not keep a batch-flush deadline armed in `poll_timeout()`, exercised
//! through the tokio engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-moonpool` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/closed_producer_batch_deadline.rs`) and the
//! `magnetar-differential` equivalence scenario.
//!
//! A batching producer with a `batching_max_publish_delay` arms `first_added_at + delay` as soon
//! as its first message sits in the batch, and `Connection::poll_timeout` surfaces that deadline
//! for every producer slot. Closing the producer leaves the unflushed batch in place until the
//! broker acknowledges the close, and `flush_batch` returns at once for a closed producer without
//! clearing `first_added_at`. `handle_timeout` therefore selects the slot as due, flushes nothing,
//! and the deadline stays in the past: the driver computes a zero sleep, takes its timer arm,
//! locks the connection, calls `handle_timeout` again and loops, until the close ack, a reset or
//! `fail_all_pending` empties the batch.
//!
//! The test plays the driver's timer arm against the connection state under a bounded round
//! budget, so a regression fails here instead of spinning.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{Connection, CreateProducerRequest, ProducerHandle, pb};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

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

fn producer_success(request_id: u64) -> bytes::BytesMut {
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(
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
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("apply Connected");
    let _ = conn.poll_event();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/closed-batch-deadline".to_owned(),
        enable_batching: true,
        batching_max_publish_delay: Some(PUBLISH_DELAY),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success(create_rid))
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
    let shared = ConnectionShared::new(magnetar_proto::ConnectionConfig::default());
    let mut conn = shared.inner.lock();
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
