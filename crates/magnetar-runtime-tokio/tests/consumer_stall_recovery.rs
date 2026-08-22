// SPDX-License-Identifier: Apache-2.0

//! Per-consumer stall watchdog + in-place recovery, issue #414: the tokio mirror
//! of `magnetar-runtime-moonpool/tests/consumer_stall_recovery.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): three `#[test]` functions here mirror the
//! moonpool file's three.
//!
//! ## The failure this covers
//!
//! Issue #414: a Pulsar **Shared** subscription wedges broker-side after
//! consumer churn. The survivors receive their first ~20 messages and then
//! nothing, forever. The broker's own `availablePermits` for the subscription
//! goes hugely negative (`-177300` in production), `acks_failed` stays `0`, and
//! the client reports no error at all — only a superuser `topics unload`
//! recovers it.
//!
//! The wire protocol carries only monotonic client → broker permit increments
//! (`CommandFlow`), so the client cannot itself drive the broker's counter
//! negative: the fault is broker-side. What the client CAN do is notice, and
//! offer a cheaper first rung on the recovery ladder than unloading the topic.
//! The connection keepalive of ADR-0058 is no help — `PING` / `PONG` keeps
//! flowing on a connection whose dispatcher wedged for ONE subscription, so
//! `last_activity` never ages and no connection-level deadline ever fires.
//!
//! ## What this pins
//!
//! The sans-io [`magnetar_proto::consumer::ConsumerState`] stall watchdog and
//! the in-place re-subscribe driven through the tokio engine's
//! [`magnetar_runtime_tokio::ConnectionShared`] wrapper with synthetic
//! [`std::time::Instant`]s — no driver task, no TCP listener. Deadlines are
//! therefore exact rather than wall-clock-dependent, and the moonpool sibling
//! pins the identical behaviour under the deterministic-simulation engine.
//!
//! 1. A granted-but-silent consumer surfaces exactly ONE `ConsumerStalled` per stall episode, and
//!    `poll_timeout` advertises the deadline so a real driver wakes for it deterministically.
//! 2. Both re-arm paths: a broker dispatch restarts the window, and so does an in-place
//!    re-subscribe.
//! 3. Two Shared consumers on one subscription: closing one leaves the survivor's own permits,
//!    queue, and watchdog untouched — the client-side half of the churn window issue #414 reports.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, ConsumerHandle, SubscribeRequest, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

/// Stall window used throughout. Matches the production value recommended on
/// `ConnectionConfig::consumer_stall_timeout`.
const WINDOW: Duration = Duration::from_secs(30);
const RQ: usize = 8;

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn success_frame(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandSuccess");
    buf
}

fn message_frame(handle: ConsumerHandle, entry_id: u64, payload: &[u8]) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id,
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
        producer_name: "stall-test-producer".to_owned(),
        sequence_id: entry_id,
        publish_time: 1_700_000_000_000,
        num_messages_in_batch: Some(1),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, payload).expect("encode message frame");
    frame
}

/// Handshake once, then attach one acked, flowed `Shared` consumer per name —
/// several consumers on ONE subscription, which is the shape issue #414 wedges.
fn open_shared_consumers(
    shared: &ConnectionShared,
    subscription: &str,
    names: &[&str],
    at: Instant,
) -> Vec<ConsumerHandle> {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        while conn.poll_event().is_some() {}
    }
    let mut handles = Vec::with_capacity(names.len());
    for name in names {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/stall".to_owned(),
            subscription: subscription.to_owned(),
            sub_type: pb::command_subscribe::SubType::Shared,
            consumer_name: Some((*name).to_owned()),
            receiver_queue_size: RQ,
            ..Default::default()
        });
        conn.handle_bytes(at, &success_frame(request_id))
            .expect("subscribe Success");
        assert!(conn.consume_initial_consumer_subscribe_completion(handle));
        while conn.poll_event().is_some() {}
        let _ = conn.initial_flow(handle, at);
        let _ = conn.poll_transmit();
        handles.push(handle);
    }
    handles
}

/// Connection config with the #414 watchdog armed. `stats_interval` is disabled
/// so the only deadlines in play are the keepalive and the watchdog, which keeps
/// the `poll_timeout` assertions unambiguous.
fn watchdog_config() -> ConnectionConfig {
    ConnectionConfig {
        consumer_stall_timeout: Some(WINDOW),
        stats_interval: None,
        ..ConnectionConfig::default()
    }
}

/// Drain every `ConsumerStalled` currently queued, as
/// `(handle, permit_balance, stalled_for)`.
fn drain_stalls(shared: &ConnectionShared) -> Vec<(ConsumerHandle, u32, Duration)> {
    let mut conn = shared.inner.lock();
    let mut out = Vec::new();
    while let Some(event) = conn.poll_event() {
        if let ConnectionEvent::ConsumerStalled {
            handle,
            permit_balance,
            stalled_for,
        } = event
        {
            out.push((handle, permit_balance, stalled_for));
        }
    }
    out
}

#[test]
fn silent_broker_surfaces_one_consumer_stalled_event_and_arms_its_deadline() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handle = open_shared_consumers(&shared, "sub-stall-once", &["solo"], t0)[0];

    // The first tick only seeds the silence window; `poll_timeout` must then
    // advertise it, otherwise a real driver would only sweep opportunistically
    // on some unrelated deadline (seed-divergent under the moonpool engine).
    shared.inner.lock().handle_timeout(t0);
    assert!(
        drain_stalls(&shared).is_empty(),
        "the seeding tick must not report"
    );
    assert_eq!(
        shared.inner.lock().poll_timeout(),
        Some(t0 + WINDOW),
        "the stall deadline is nearer than the 30 s keepalive and must win"
    );

    // One millisecond short: still nothing.
    shared
        .inner
        .lock()
        .handle_timeout(t0 + WINDOW.saturating_sub(Duration::from_millis(1)));
    assert!(
        drain_stalls(&shared).is_empty(),
        "a tick inside the window must not report"
    );

    // At the deadline: exactly one event, carrying the un-spent balance the
    // broker never dispatched against.
    shared.inner.lock().handle_timeout(t0 + WINDOW);
    assert_eq!(
        drain_stalls(&shared),
        vec![(handle, RQ as u32, WINDOW)],
        "one event per stall episode, carrying the balance and the silence duration"
    );

    // A reported window arms no further wake: `poll_timeout` falls back to the
    // keepalive deadline instead of re-scheduling a sweep that could only
    // re-report the same episode. Without this the driver would wake once per
    // window, forever, for a consumer nobody is going to hear about again.
    assert_eq!(
        shared.inner.lock().poll_timeout(),
        Some(t0 + WINDOW + ConnectionConfig::default().keepalive_interval),
        "after the report only the keepalive deadline remains armed"
    );

    // Every later tick in the same episode stays silent — the operator gets one
    // alert per wedge, not one per sweep.
    for extra in [1u64, 30, 3_600] {
        shared
            .inner
            .lock()
            .handle_timeout(t0 + WINDOW + Duration::from_secs(extra));
    }
    assert!(
        drain_stalls(&shared).is_empty(),
        "the once-per-episode latch must hold across an hour of further ticks"
    );
}

#[test]
fn dispatch_and_in_place_resubscribe_both_re_arm_the_watchdog() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handle = open_shared_consumers(&shared, "sub-stall-rearm", &["solo"], t0)[0];

    shared.inner.lock().handle_timeout(t0);
    shared.inner.lock().handle_timeout(t0 + WINDOW);
    assert_eq!(drain_stalls(&shared).len(), 1, "first stall episode");

    // ── Re-arm path 1: the broker resumes. One dispatch is enough.
    let resumed = t0 + WINDOW + Duration::from_secs(1);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(resumed, &message_frame(handle, 1, b"back"))
            .expect("deliver");
        let _ = conn.pop_message(handle, resumed);
        let _ = conn.poll_transmit();
        while conn.poll_event().is_some() {}
    }
    shared.inner.lock().handle_timeout(resumed);
    assert!(
        drain_stalls(&shared).is_empty(),
        "the dispatch re-seeded the window rather than re-reporting the old one"
    );
    shared
        .inner
        .lock()
        .handle_timeout(resumed + WINDOW.saturating_sub(Duration::from_millis(1)));
    assert!(
        drain_stalls(&shared).is_empty(),
        "the new window is measured from the dispatch, not from the first stall"
    );
    shared.inner.lock().handle_timeout(resumed + WINDOW);
    assert_eq!(
        drain_stalls(&shared).len(),
        1,
        "a consumer that wedges twice reports twice"
    );

    // ── Re-arm path 2: the caller-driven recovery. The in-place re-subscribe
    // zeroes the permit mirrors, and the broker's ack re-arms both the grant and
    // the watchdog.
    let recovered = resumed + WINDOW;
    let resub_request_id = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        conn.resubscribe_consumer_in_place(handle)
            .expect("a live, acked, un-gated consumer is eligible");
        assert_eq!(
            conn.consumer_available_permits(handle),
            0,
            "the broker recreates its dispatcher slot at zero permits"
        );
        let _ = conn.poll_transmit();
        request_id
    };
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(recovered, &success_frame(resub_request_id))
            .expect("resubscribe Success");
        while conn.poll_event().is_some() {}
    }
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "the re-subscribe ack re-arms the full receiver-queue grant"
    );

    // The recovery restarted the window instead of inheriting the reported one:
    // a fresh episode has to run its full length before it can alert again.
    shared.inner.lock().handle_timeout(recovered);
    assert!(drain_stalls(&shared).is_empty(), "re-seed only");
    shared
        .inner
        .lock()
        .handle_timeout(recovered + WINDOW.saturating_sub(Duration::from_millis(1)));
    assert!(
        drain_stalls(&shared).is_empty(),
        "still one millisecond short of the post-recovery window"
    );
    shared.inner.lock().handle_timeout(recovered + WINDOW);
    assert_eq!(
        drain_stalls(&shared).len(),
        1,
        "and a consumer that wedges again after recovery still reports"
    );
}

#[test]
fn closing_one_shared_consumer_leaves_the_survivor_untouched() {
    // The client-side half of issue #414's churn window. The broker's
    // redistribution of the departed consumer's backlog is modelled in the
    // `magnetar-differential` scripted broker; what the client owes is simpler
    // and absolute: tearing one consumer down must not disturb another
    // consumer's permits, queue, or stall watchdog — they are per-slot state,
    // and a survivor that lost its grant to a sibling's close would be a
    // client-side reproduction of the very wedge the issue reports.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handles = open_shared_consumers(&shared, "sub-stall-churn", &["a", "b"], t0);
    let (leaver, survivor) = (handles[0], handles[1]);

    // Both are granted and dispatched to — a live, sharing subscription.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &message_frame(leaver, 0, b"to-a"))
            .expect("deliver to a");
        conn.handle_bytes(t0, &message_frame(survivor, 1, b"to-b"))
            .expect("deliver to b");
        while conn.poll_event().is_some() {}
    }
    assert_eq!(
        shared.inner.lock().consumer_available_permits(survivor),
        RQ as u32 - 1,
        "the survivor spent exactly the one permit its own dispatch cost"
    );

    // One consumer leaves mid-stream.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.close_consumer(leaver, t0);
        let _ = conn.poll_transmit();
        while conn.poll_event().is_some() {}
    }

    // The survivor is untouched: same permits, same queued message, still able
    // to receive and to replenish.
    assert_eq!(
        shared.inner.lock().consumer_available_permits(survivor),
        RQ as u32 - 1,
        "a sibling's close must not touch this consumer's broker grant"
    );
    assert_eq!(
        shared.inner.lock().consumer_queue_len(survivor),
        1,
        "and must not drop what the survivor already has buffered"
    );
    {
        let mut conn = shared.inner.lock();
        let message = conn
            .pop_message(survivor, t0)
            .expect("the survivor keeps draining across the churn");
        assert_eq!(message.payload.as_ref(), b"to-b");
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    // And the survivor's own watchdog is unaffected by the churn: a consumer
    // that keeps receiving inside the window is never reported, however many
    // siblings left. (A survivor that then goes genuinely silent WOULD be
    // reported — the event says "no dispatch despite outstanding permits", which
    // on a drained, idle subscription is the truth and not a fault; the caller
    // correlates it with the broker's backlog before acting. That is why the
    // watchdog only ever emits an event and never recovers on its own.)
    shared.inner.lock().handle_timeout(t0);
    assert!(drain_stalls(&shared).is_empty(), "the seeding tick");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0 + WINDOW / 2, &message_frame(survivor, 2, b"more"))
            .expect("keep dispatching to the survivor");
        let _ = conn.pop_message(survivor, t0 + WINDOW / 2);
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }
    shared.inner.lock().handle_timeout(t0 + WINDOW);
    let stalls = drain_stalls(&shared);
    assert!(
        stalls.iter().all(|(handle, _, _)| *handle != survivor),
        "a survivor that received inside the window must not be reported, got {stalls:?}"
    );
}
