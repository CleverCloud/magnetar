// SPDX-License-Identifier: Apache-2.0

//! A `next_active_change()` future's stale waker key must not evict the waker of a different
//! `next_active_change()` future that registered after a transition drained the waker set,
//! exercised through the moonpool engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-tokio` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/active_change_waker_stale_key.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! `ConsumerState` parks every in-flight `next_active_change()` future on a per-consumer waker set
//! and hands the future a key, exactly as it does for `receive()`. A recorded transition (or a
//! close) drains the WHOLE set, and each woken future still holds the key it was given before the
//! drain. A key that the set handed out again would let `cancel_active_change_waker`, which both
//! engines' `ActiveChangeFut::poll` call on every re-poll, after a successful pop and on `Drop`,
//! evict the waker of a sibling that registered after the drain: the next transition would then
//! wake only the evictor and the sibling's task would stay parked while a transition sits in the
//! ring. The calls below are, in order, the ones `ActiveChangeFut::poll` makes on the connection
//! (`pop_consumer_active_change`, `cancel_consumer_active_change_waker` of the stored key,
//! `register_consumer_active_change_waker`).
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, pb};
use magnetar_runtime_moonpool::ConnectionShared;

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

/// One `CommandActiveConsumerChange` for `handle`.
fn active_change_frame(handle: ConsumerHandle, is_active: bool) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ActiveConsumerChange as i32,
            active_consumer_change: Some(pb::CommandActiveConsumerChange {
                consumer_id: handle.0,
                is_active: Some(is_active),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandActiveConsumerChange");
    buf
}

/// Two `next_active_change()` futures, F1 and F2, are parked on one consumer. A transition is
/// recorded and wakes both. F2 runs first: it pops the transition, resolves, and its task calls
/// `next_active_change()` again (F2'), which finds the ring empty and parks. F1, woken by the same
/// transition, runs late with the key it was given before the drain: it finds the ring empty,
/// cancels that key and parks again. The next transition must wake both parked futures.
#[test]
fn a_woken_active_change_future_does_not_evict_the_waker_a_sibling_parked_after_the_drain() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_transmit();

    let subscribe_rid = conn.peek_next_request_id_for_test();
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/active-change-waker-stale-key".to_owned(),
        subscription: "magnetar-test-active-change-waker-stale-key".to_owned(),
        ..Default::default()
    });
    conn.handle_bytes(t0, &success_frame(subscribe_rid))
        .expect("Success");
    while conn.poll_event().is_some() {}
    let _ = conn.initial_flow(consumer, t0);
    let _ = conn.poll_transmit();

    // F1 and F2 poll an empty ring and park.
    let (f1_count, f1_waker) = counting_waker();
    let (f2_count, f2_waker) = counting_waker();
    let f1_key = conn
        .register_consumer_active_change_waker(consumer, f1_waker)
        .expect("F1 parks");
    let f2_key = conn
        .register_consumer_active_change_waker(consumer, f2_waker)
        .expect("F2 parks");

    // The first transition wakes both and drains the set.
    let t1 = t0 + Duration::from_millis(1);
    conn.handle_bytes(t1, &active_change_frame(consumer, true))
        .expect("first transition");
    assert_eq!(
        (wakes(&f1_count), wakes(&f2_count)),
        (1, 1),
        "precondition: the transition woke both parked active-change futures"
    );

    // F2 runs first: it pops the transition and cancels the key it held, which the drain already
    // took. Its task then calls `next_active_change()` again and the new future parks on the
    // empty ring.
    assert_eq!(
        conn.pop_consumer_active_change(consumer),
        Some(true),
        "precondition: F2 pops the first transition"
    );
    conn.cancel_consumer_active_change_waker(consumer, f2_key);
    assert_eq!(
        conn.pop_consumer_active_change(consumer),
        None,
        "precondition: F2' finds the ring empty"
    );
    let (f2_next_count, f2_next_waker) = counting_waker();
    let _f2_next_key = conn
        .register_consumer_active_change_waker(consumer, f2_next_waker)
        .expect("F2' parks");

    // F1 runs late: the ring is empty, so it cancels the key it held across the drain and parks
    // again.
    assert_eq!(
        conn.pop_consumer_active_change(consumer),
        None,
        "precondition: F1 finds the ring empty"
    );
    conn.cancel_consumer_active_change_waker(consumer, f1_key);
    let (f1_again_count, f1_again_waker) = counting_waker();
    let _f1_again_key = conn
        .register_consumer_active_change_waker(consumer, f1_again_waker)
        .expect("F1 parks again");

    // The second transition must wake every parked future, F2' included.
    let t2 = t0 + Duration::from_millis(2);
    conn.handle_bytes(t2, &active_change_frame(consumer, false))
        .expect("second transition");
    assert_eq!(
        wakes(&f2_next_count),
        1,
        "the waker F2' parked must still be in the set when the next transition is recorded: F1 \
         cancelled a stale key that the drain had taken and that must not name F2''s entry"
    );
    assert_eq!(
        wakes(&f1_again_count),
        1,
        "the waker F1 parked again must be woken by the next transition"
    );
}
