// SPDX-License-Identifier: Apache-2.0

//! A closed producer must not keep a batch-flush deadline armed, identically on both engines
//! (ADR-0024 layer (d)).
//!
//! A batching producer with a `batching_max_publish_delay` arms `first_added_at + delay` as soon
//! as its first message sits in the batch, and `Connection::poll_timeout` surfaces that deadline
//! for every producer slot. Closing the producer leaves the unflushed batch in place until the
//! broker acknowledges the close, and `flush_batch` returns at once for a closed producer without
//! clearing `first_added_at`. Were the deadline still reported, `handle_timeout` would select the
//! slot as due, flush nothing, and leave the deadline in the past: each engine's driver would
//! compute a zero sleep, take its timer arm, call `handle_timeout` again and loop, until the close
//! ack, a reset or `fail_all_pending` empties the batch. Both engines wrap the same
//! `magnetar-proto` state machine, so they must agree, and the agreed answer must not be a spin.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{Connection, ConnectionConfig, CreateProducerRequest, encode_command, pb};

const PUBLISH_DELAY: Duration = Duration::from_millis(10);

/// Two publish delays after the first message: its batch deadline is long expired.
const LATER: Duration = Duration::from_millis(20);

/// How many times a driver may re-enter its timer arm for one instant before the test calls it a
/// spin. The correct client needs none for a closed producer, and one at most to retire a
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

/// What a driver sees after a producer close with a batch still waiting on its publish delay.
#[derive(Debug, PartialEq, Eq)]
struct TimerView {
    /// Timer-arm rounds the driver needed at `LATER` before `poll_timeout()` left the past.
    rounds: usize,
    /// The deadline `poll_timeout()` reports at the end, relative to the first message.
    next_wake: Option<Duration>,
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
                producer_name: "closed-batch-deadline".to_owned(),
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

/// One batching producer holds a message in its batch, the producer is closed and the broker has
/// not acknowledged the close. Plays the driver's timer arm at `LATER`: while `poll_timeout()` is
/// not in the future, take the arm and call `handle_timeout`.
fn timer_view(conn: &mut Connection, t0: Instant, close: Close) -> TimerView {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/closed-batch-deadline".to_owned(),
        enable_batching: true,
        batching_max_publish_delay: Some(PUBLISH_DELAY),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_frame(create_rid))
        .expect("ProducerSuccess");
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

    match close {
        Close::Awaited => conn.close_producer(producer),
        Close::Forgotten => conn.close_producer_forget(producer),
    };

    let now = t0 + LATER;
    let mut rounds = 0;
    while conn.poll_timeout().is_some_and(|d| d <= now) && rounds < ROUND_BUDGET {
        conn.handle_timeout(now);
        rounds += 1;
    }
    TimerView {
        rounds,
        next_wake: conn.poll_timeout().map(|d| d.saturating_duration_since(t0)),
    }
}

fn assert_engines_do_not_spin(close: Close) {
    let tokio = tokio_projection(|conn, t0| timer_view(conn, t0, close));
    let moonpool = moonpool_projection(|conn, t0| timer_view(conn, t0, close));
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the timer a closed producer leaves armed ({close:?})"
    );
    assert!(
        tokio.rounds <= 1,
        "a driver re-entered its timer arm {} times at one instant after a {close:?} producer \
         close: the batch deadline of a closed producer stays armed in the past; both engines \
         report {tokio:?}",
        tokio.rounds
    );
    assert!(
        tokio.next_wake.is_none_or(|d| d > LATER),
        "a closed producer's batch deadline must not stay armed in the past; both engines report \
         {tokio:?} ({close:?})"
    );
}

#[test]
fn engines_leave_no_batch_deadline_armed_after_an_awaited_producer_close() {
    assert_engines_do_not_spin(Close::Awaited);
}

#[test]
fn engines_leave_no_batch_deadline_armed_after_a_forgotten_producer_close() {
    assert_engines_do_not_spin(Close::Forgotten);
}
