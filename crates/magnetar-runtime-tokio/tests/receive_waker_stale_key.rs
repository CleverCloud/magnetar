// SPDX-License-Identifier: Apache-2.0

//! A `receive()` future's stale receive-waker key must not evict the waker of a different
//! `receive()` future that registered after a delivery drained the slab, exercised through the
//! tokio engine's [`ConnectionShared`] state. Mirrors the matching `magnetar-runtime-moonpool`
//! test, its `magnetar-proto` unit test (`crates/magnetar-proto/tests/receive_waker_stale_key.rs`)
//! and the `magnetar-differential` equivalence scenario.
//!
//! `ConsumerState` parks every in-flight `receive()` future on a per-consumer waker slab and hands
//! the future a raw slab key. A message arrival drains the WHOLE slab (`wake_receivers`), and a
//! drained slab hands key `0` out again, but each woken future still holds the key it was given
//! before the drain. `cancel_receive_waker` removes whatever occupies a key without checking that
//! the caller still owns it, and both engines' `ReceiveFut::poll` cancel the stored key on an
//! empty re-poll, after a successful pop and on `Drop`. A woken future that runs after a sibling
//! already popped the message, resolved and started a new `receive()` therefore evicts the
//! sibling's NEW waker: the next arrival wakes only the evictor and the sibling's task stays
//! parked while a message sits in the queue. The calls below are, in order, the ones
//! `ReceiveFut::poll` makes on the connection (`pop_message`, `cancel_consumer_receive_waker` of
//! the stored key, `register_consumer_receive_waker`).
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

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

/// Two `receive()` futures, F1 and F2, are parked on one consumer. A message arrives and wakes
/// both. F2 runs first: it pops the message, resolves, and its task calls `receive()` again (F2'),
/// which finds the queue empty and parks. F1, woken by the same arrival, runs late with the key it
/// was given before the drain: it finds the queue empty, cancels that key and parks again. The next
/// arrival must wake both parked futures.
#[test]
fn a_woken_receive_future_does_not_evict_the_waker_a_sibling_parked_after_the_drain() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
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

    // F1 and F2 poll an empty queue and park.
    let (f1_count, f1_waker) = counting_waker();
    let (f2_count, f2_waker) = counting_waker();
    let f1_key = conn
        .register_consumer_receive_waker(consumer, f1_waker)
        .expect("F1 parks");
    let f2_key = conn
        .register_consumer_receive_waker(consumer, f2_waker)
        .expect("F2 parks");

    // The first message wakes both and drains the slab.
    let t1 = t0 + Duration::from_millis(1);
    conn.handle_bytes(t1, &message_frame(consumer, 1))
        .expect("first message");
    assert_eq!(
        (wakes(&f1_count), wakes(&f2_count)),
        (1, 1),
        "precondition: the arrival woke both parked receive futures"
    );

    // F2 runs first: it pops the message and cancels the key it held, which the drain already
    // freed. Its task then calls `receive()` again and the new future parks on the empty queue.
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

    // F1 runs late: the queue is empty, so it cancels the key it held across the drain and parks
    // again.
    assert!(
        conn.pop_message(consumer, t1).is_none(),
        "precondition: F1 finds the queue empty"
    );
    conn.cancel_consumer_receive_waker(consumer, f1_key);
    let (f1_again_count, f1_again_waker) = counting_waker();
    let _f1_again_key = conn
        .register_consumer_receive_waker(consumer, f1_again_waker)
        .expect("F1 parks again");

    // The second message must wake every parked receive future, F2' included.
    let t2 = t0 + Duration::from_millis(2);
    conn.handle_bytes(t2, &message_frame(consumer, 2))
        .expect("second message");
    assert_eq!(
        wakes(&f2_next_count),
        1,
        "the waker F2' parked must still be in the slab when the next message arrives: F1 \
         cancelled a stale key that the drain had freed and the slab had handed to F2'"
    );
    assert_eq!(
        wakes(&f1_again_count),
        1,
        "the waker F1 parked again must be woken by the next message"
    );
}
