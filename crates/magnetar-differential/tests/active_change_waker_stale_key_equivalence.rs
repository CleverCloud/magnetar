// SPDX-License-Identifier: Apache-2.0

//! A `next_active_change()` future's stale waker key must not evict the waker of a different
//! `next_active_change()` future that registered after a transition drained the waker set,
//! identically on both engines (ADR-0024 layer (d)).
//!
//! A recorded active/standby transition drains the whole per-consumer active-change waker set, and
//! each woken `next_active_change()` future still holds the key it was given before the drain. A
//! key that the set handed out again would let the late future's cancel evict the waker of a
//! sibling that registered after the drain, and the next transition would wake only the evictor.
//! Both engines' `ActiveChangeFut` drive the shared `Connection` through the same
//! `pop_consumer_active_change`, `cancel_consumer_active_change_waker` and
//! `register_consumer_active_change_waker` calls, so they must agree on which wakers a transition
//! wakes, and it must wake every future that is parked.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, pb,
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

/// How often `(F1's waker, F2''s waker)` had been woken by the second transition.
type WakeCounts = (usize, usize);

/// Two `next_active_change()` futures, F1 and F2, are parked on one consumer. A transition is
/// recorded and wakes both. F2 runs first: it pops the transition, resolves, and its task calls
/// `next_active_change()` again (F2'), which finds the ring empty and parks. F1, woken by the same
/// transition, runs late with the key it was given before the drain: it finds the ring empty,
/// cancels that key and parks again. A second transition is then recorded.
fn wake_counts_after_a_late_poll_of_a_woken_active_change(
    conn: &mut Connection,
    t0: Instant,
) -> WakeCounts {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
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

    let (f1_count, f1_waker) = counting_waker();
    let (f2_count, f2_waker) = counting_waker();
    let f1_key = conn
        .register_consumer_active_change_waker(consumer, f1_waker)
        .expect("F1 parks");
    let f2_key = conn
        .register_consumer_active_change_waker(consumer, f2_waker)
        .expect("F2 parks");

    let t1 = t0 + Duration::from_millis(1);
    conn.handle_bytes(t1, &active_change_frame(consumer, true))
        .expect("first transition");
    assert_eq!(
        (wakes(&f1_count), wakes(&f2_count)),
        (1, 1),
        "precondition: the transition woke both parked active-change futures"
    );

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

    let t2 = t0 + Duration::from_millis(2);
    conn.handle_bytes(t2, &active_change_frame(consumer, false))
        .expect("second transition");
    (wakes(&f1_again_count), wakes(&f2_next_count))
}

#[test]
fn engines_wake_every_parked_active_change_future_when_a_woken_sibling_polls_late() {
    let tokio = tokio_projection(wake_counts_after_a_late_poll_of_a_woken_active_change);
    let moonpool = moonpool_projection(wake_counts_after_a_late_poll_of_a_woken_active_change);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on which parked active-change wakers a transition wakes after a \
         woken sibling polled late with a stale key"
    );
    assert_eq!(
        tokio,
        (1, 1),
        "the next transition must wake both parked active-change futures, F2' included, whose \
         waker F1's stale-key cancel must not have evicted ((F1, F2') wake counts); both engines \
         report {tokio:?}"
    );
}
