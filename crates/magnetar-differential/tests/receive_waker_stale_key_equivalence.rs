// SPDX-License-Identifier: Apache-2.0

//! A `receive()` future's stale receive-waker key must not evict the waker of a different
//! `receive()` future that registered after a delivery drained the slab, identically on both
//! engines (ADR-0024 layer (d)).
//!
//! A message arrival drains the whole per-consumer waker slab and a drained slab hands key `0` out
//! again, but each woken `receive()` future still holds the key it was given before the drain, and
//! `cancel_receive_waker` removes whatever occupies a key. A woken future that runs after a
//! sibling already popped the message, resolved and started a new `receive()` therefore evicts the
//! sibling's new waker, and the next arrival wakes only the evictor. Both engines' `ReceiveFut`
//! drive the shared `Connection` through the same `pop_message`, `cancel_consumer_receive_waker`
//! and `register_consumer_receive_waker` calls, so they must agree on which wakers a delivery
//! wakes, and it must wake every receive future that is parked.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, encode_payload,
    pb,
};

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

fn success_frame(request_id: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandSuccess");
    buf
}

/// One non-batched `CommandMessage` for `(7, entry)` addressed to `handle`.
fn message_frame(handle: ConsumerHandle, entry: u64) -> BytesMut {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id: entry,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "receive-waker-stale-key".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &command, &metadata, b"payload").expect("encode message frame");
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

/// How often `(F1's waker, F2''s waker)` had been woken by the second message.
type WakeCounts = (usize, usize);

/// Two `receive()` futures, F1 and F2, are parked on one consumer. A message arrives and wakes
/// both. F2 runs first: it pops the message, resolves, and its task calls `receive()` again (F2'),
/// which finds the queue empty and parks. F1, woken by the same arrival, runs late with the key it
/// was given before the drain: it finds the queue empty, cancels that key and parks again. A second
/// message then arrives.
fn wake_counts_after_a_late_poll_of_a_woken_receive(
    conn: &mut Connection,
    t0: Instant,
) -> WakeCounts {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_transmit();

    let subscribe_rid = conn.peek_next_request_id_for_test();
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/receive-waker-stale-key".to_owned(),
        subscription: "magnetar-test-receive-waker-stale-key".to_owned(),
        ..Default::default()
    });
    conn.handle_bytes(t0, &success_frame(subscribe_rid))
        .expect("Success");
    while conn.poll_event().is_some() {}
    let _ = conn.initial_flow(consumer, t0);
    let _ = conn.poll_transmit();

    let (f1_count, f1_waker) = counting_waker();
    let (f2_count, f2_waker) = counting_waker();
    let f1_key = conn
        .register_consumer_receive_waker(consumer, f1_waker)
        .expect("F1 parks");
    let f2_key = conn
        .register_consumer_receive_waker(consumer, f2_waker)
        .expect("F2 parks");

    let t1 = t0 + Duration::from_millis(1);
    conn.handle_bytes(t1, &message_frame(consumer, 1))
        .expect("first message");
    assert_eq!(
        (wakes(&f1_count), wakes(&f2_count)),
        (1, 1),
        "precondition: the arrival woke both parked receive futures"
    );

    assert!(
        conn.pop_message(consumer, t1).is_some(),
        "precondition: F2 pops the first message"
    );
    conn.cancel_consumer_receive_waker(consumer, f2_key);
    assert!(
        conn.pop_message(consumer, t1).is_none(),
        "precondition: F2' finds the queue empty"
    );
    let (f2_next_count, f2_next_waker) = counting_waker();
    let _f2_next_key = conn
        .register_consumer_receive_waker(consumer, f2_next_waker)
        .expect("F2' parks");

    assert!(
        conn.pop_message(consumer, t1).is_none(),
        "precondition: F1 finds the queue empty"
    );
    conn.cancel_consumer_receive_waker(consumer, f1_key);
    let (f1_again_count, f1_again_waker) = counting_waker();
    let _f1_again_key = conn
        .register_consumer_receive_waker(consumer, f1_again_waker)
        .expect("F1 parks again");

    let t2 = t0 + Duration::from_millis(2);
    conn.handle_bytes(t2, &message_frame(consumer, 2))
        .expect("second message");
    (wakes(&f1_again_count), wakes(&f2_next_count))
}

#[test]
fn engines_wake_every_parked_receive_future_when_a_woken_sibling_polls_late() {
    let tokio = tokio_projection(wake_counts_after_a_late_poll_of_a_woken_receive);
    let moonpool = moonpool_projection(wake_counts_after_a_late_poll_of_a_woken_receive);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on which parked receive wakers a delivery wakes after a woken \
         sibling polled late with a stale slab key"
    );
    assert_eq!(
        tokio,
        (1, 1),
        "the next arrival must wake both parked receive futures, F2' included, whose waker F1's \
         stale-key cancel must not have evicted ((F1, F2') wake counts); both engines report \
         {tokio:?}"
    );
}
