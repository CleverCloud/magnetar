// SPDX-License-Identifier: Apache-2.0

//! Per-consumer stall watchdog + in-place recovery, issue #414, under the
//! deterministic moonpool clock.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): twelve `#[test]` functions here mirror the
//! twelve in `magnetar-runtime-tokio/tests/consumer_stall_recovery.rs`.
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
//! the in-place re-subscribe driven through the moonpool engine's
//! [`magnetar_runtime_moonpool::ConnectionShared`] wrapper with synthetic
//! [`std::time::Instant`]s — no driver task, no TCP listener, no wall clock.
//! Every deadline in these tests is exact virtual time, so the assertions are
//! seed-independent by construction; the mirrored `magnetar-runtime-tokio` file
//! pins the identical behaviour against the tokio engine.
//!
//! 1. A granted-but-silent consumer surfaces exactly ONE `ConsumerStalled` per stall episode, and
//!    `poll_timeout` advertises the deadline so a real driver wakes for it deterministically.
//! 2. Both re-arm paths: a broker dispatch restarts the window, and so does an in-place
//!    re-subscribe.
//! 3. Two Shared consumers on one subscription: closing one leaves the survivor's own permits,
//!    queue, and watchdog untouched — the client-side half of the churn window issue #414 reports.
//! 4. ADR-0103's opt-in automatic recovery: disarmed it emits no wire traffic at all, armed it
//!    re-subscribes at most `consumer_stall_auto_recovery` times per stall streak and then stops,
//!    reporting the episode either way so the broker-side defect is never papered over.
//! 5. The budget resets on one broker dispatch unit actually arriving — and on nothing else, the
//!    recovery's own re-subscribe included, which is what makes the bound a bound.
//! 6. A consumer the in-place re-attach may not touch — a pending unsubscribe, the one ineligible
//!    state that is still a stall candidate — is reported and left completely alone: no
//!    `CommandSubscribe`, no mutation, no budget spent.
//! 7. A reported Failover standby — the one shape that satisfies the stall predicate permanently
//!    and legitimately — is reported and skipped, spends nothing, and hands its untouched budget to
//!    the consumer it becomes on promotion.
//! 8. ADR-0108's phase 1 rejected: a broker that refuses the recovery `CommandCloseConsumer` leaves
//!    the consumer byte-for-byte untouched, no `CommandSubscribe` follows it, and the consumer
//!    stays both reportable and recoverable.
//! 9. The session lost between the two phases — through `reset` or `fail_all_pending` — leaves the
//!    re-attach to the reconnect rebuild and clears the in-flight marker, so the next attempt is
//!    not gated by a dead one.
//! 10. `Failover` and non-durable subscriptions are refused outright, because the close the
//!     recovery depends on would trigger an election or discard a cursor — and the stall is still
//!     reported, since withholding the repair never withholds the diagnosis.
//! 11. A second recovery while phase 1 is still awaiting its ack is refused off `pending_requests`,
//!     so exactly one `CommandCloseConsumer` reaches the broker per recovery and the re-attach
//!     follows only that one close.
//! 12. A consumer that became ineligible while the close flew — an unsubscribe raced it — is not
//!     re-attached by phase 2, because the shared eligibility gate is re-run there.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, ConsumerHandle, SubscribeRequest, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

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
    open_consumers(
        shared,
        pb::command_subscribe::SubType::Shared,
        true,
        subscription,
        names,
        at,
    )
}

/// One acked, flowed `Failover` consumer — the subscription type the broker announces
/// active/standby for (issue #348), and therefore the only one the standby pre-check can
/// ever apply to.
fn open_failover_consumer(
    shared: &ConnectionShared,
    subscription: &str,
    name: &str,
    at: Instant,
) -> ConsumerHandle {
    open_consumers(
        shared,
        pb::command_subscribe::SubType::Failover,
        true,
        subscription,
        &[name],
        at,
    )[0]
}

fn open_consumers(
    shared: &ConnectionShared,
    sub_type: pb::command_subscribe::SubType,
    durable: bool,
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
            sub_type,
            durable,
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

    // ── Re-arm path 2: the caller-driven recovery. Since ADR-0108 that is a
    // `CommandCloseConsumer` whose ack releases the re-subscribe; only then do the
    // permit mirrors follow the slot the broker really dropped, and the
    // re-subscribe's own ack re-arms both the grant and the watchdog.
    let recovered = resumed + WINDOW;
    let permits_before = shared.inner.lock().consumer_available_permits(handle);
    let close_request_id = {
        let mut conn = shared.inner.lock();
        let request_id = conn
            .resubscribe_consumer_in_place(handle)
            .expect("a live, acked, un-gated durable non-Failover consumer is eligible");
        assert_eq!(
            conn.consumer_available_permits(handle),
            permits_before,
            "phase 1 mutates nothing: the broker has not agreed to drop the slot yet, \
             and a close it rejects must leave a consumer the watchdog can still see"
        );
        let _ = conn.poll_transmit();
        request_id.0
    };
    let _resub_request_id = ack_resubscribe(&shared, close_request_id, recovered);
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
    // watchdog emits an event and, unless `consumer_stall_auto_recovery` is
    // armed, never recovers on its own.)
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

/// Connection config with the #414 watchdog armed AND ADR-0103's bounded automatic
/// recovery armed at `max_attempts` in-place re-subscribes per stall streak.
fn auto_recovery_config(max_attempts: u32) -> ConnectionConfig {
    ConnectionConfig {
        consumer_stall_auto_recovery: Some(max_attempts),
        ..watchdog_config()
    }
}

/// One watchdog sweep at `at`, reported as a `(stall events, recovery request id)`
/// pair — the second element being `Some` exactly when the automatic recovery acted.
///
/// The request id is how an emitted recovery is counted without a new public accessor:
/// since ADR-0108 the recovery's first wire act is a `CommandCloseConsumer`, and in
/// these traces it is the only thing a sweep can consume a request id for, so a sweep
/// that advanced the counter by exactly one emitted exactly one recovery — and the id
/// it consumed is the one the broker's `Success` must carry back to release phase 2.
fn sweep(shared: &ConnectionShared, at: Instant) -> (usize, Option<u64>) {
    let before = shared.inner.lock().peek_next_request_id_for_test();
    shared.inner.lock().handle_timeout(at);
    let after = shared.inner.lock().peek_next_request_id_for_test();
    let stalls = drain_stalls(shared).len();
    assert!(
        after == before || after == before + 1,
        "one sweep may emit at most one in-place recovery, saw {before} -> {after}"
    );
    (stalls, (after != before).then_some(before))
}

/// Drive both phases of an in-place recovery to completion: ack the
/// `CommandCloseConsumer` the watchdog emitted, which releases the `CommandSubscribe`,
/// then ack that too — which is what releases the deferred initial `CommandFlow` and
/// re-arms both the grant and the window.
///
/// Returns the re-subscribe's request id, so a caller can assert the second phase
/// happened at all rather than inferring it from the grant.
fn ack_resubscribe(shared: &ConnectionShared, close_request_id: u64, at: Instant) -> u64 {
    let resubscribe_request_id = {
        let mut conn = shared.inner.lock();
        let next = conn.peek_next_request_id_for_test();
        conn.handle_bytes(at, &success_frame(close_request_id))
            .expect("recovery close Success");
        while conn.poll_event().is_some() {}
        let after = conn.peek_next_request_id_for_test();
        assert_eq!(
            after,
            next + 1,
            "the close ack must release exactly one CommandSubscribe"
        );
        next
    };
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &success_frame(resubscribe_request_id))
        .expect("resubscribe Success");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    resubscribe_request_id
}

/// Recovery budget for [`auto_recovery_resubscribes_up_to_the_bound_and_then_escalates`].
const MAX_ATTEMPTS: u32 = 2;

#[test]
fn auto_recovery_resubscribes_up_to_the_bound_and_then_escalates() {
    // ── Control: the watchdog alone. ADR-0101 shipped it event-only, and that is still
    // what an application that arms `consumer_stall_timeout` and nothing else gets: a
    // report, and not one byte of recovery traffic. If this ever starts emitting a
    // re-subscribe, opt-in stopped being opt-in.
    let t0 = Instant::now();
    let reporting_only = ConnectionShared::new(watchdog_config());
    let _ = open_shared_consumers(&reporting_only, "sub-report-only", &["solo"], t0)[0];
    let (stalls, resubscribe) = sweep(&reporting_only, t0 + WINDOW);
    assert_eq!(
        stalls, 1,
        "the watchdog still reports with recovery disarmed"
    );
    assert_eq!(
        resubscribe, None,
        "an unset `consumer_stall_auto_recovery` must emit no wire traffic at all"
    );

    // ── Armed with a budget of two.
    let shared = ConnectionShared::new(auto_recovery_config(MAX_ATTEMPTS));
    let handle = open_shared_consumers(&shared, "sub-auto-recover", &["solo"], t0)[0];

    // `initial_flow` armed the window at `t0`, so the first episode closes exactly one
    // window later — no seeding sweep needed, and no keepalive interval added on top.
    let mut at = t0;
    for attempt in 1..=MAX_ATTEMPTS {
        at += WINDOW;
        let (stalls, resubscribe) = sweep(&shared, at);
        assert_eq!(
            stalls, 1,
            "attempt {attempt}: the event is emitted whether or not recovery acts — \
             arming recovery must never suppress the diagnosis"
        );
        let request_id = resubscribe.unwrap_or_else(|| {
            panic!("attempt {attempt}: a consumer inside its budget must be recovered")
        });
        assert_eq!(
            shared.inner.lock().consumer_available_permits(handle),
            RQ as u32,
            "attempt {attempt}: ADR-0108's phase 1 is a CommandCloseConsumer alone and \
             mutates nothing — the broker has not agreed to drop the slot yet"
        );
        ack_resubscribe(&shared, request_id, at);
        assert_eq!(
            shared.inner.lock().consumer_available_permits(handle),
            RQ as u32,
            "attempt {attempt}: the re-subscribe ack re-arms the full receiver-queue grant"
        );
    }

    // Budget exhausted. The broker never dispatched, so nothing reset the counter: the
    // next episode reports and escalates instead of re-subscribing a third time. This is
    // the dispatcher-WIDE arm of issue #414 — one fresh grant per attempt cannot lift an
    // aggregate observed at `-177300`, so the client stops and leaves `topics unload` to
    // the operator rather than re-subscribing forever.
    at += WINDOW;
    let (stalls, resubscribe) = sweep(&shared, at);
    assert_eq!(stalls, 1, "the exhausted episode is still reported");
    assert_eq!(
        resubscribe, None,
        "a consumer whose budget is spent must emit no further re-subscribe"
    );

    // And it stays stopped: the last attempt was the last thing that re-armed the window,
    // so with no dispatch there is no further episode and no further traffic — one
    // escalation, not one per window forever.
    for extra in [1u64, 60, 3_600] {
        let (stalls, resubscribe) = sweep(&shared, at + Duration::from_secs(extra));
        assert_eq!(
            stalls, 0,
            "the once-per-episode latch holds after exhaustion"
        );
        assert_eq!(
            resubscribe, None,
            "and no recovery traffic resumes on its own"
        );
    }
}

#[test]
fn a_broker_dispatch_between_stalls_restores_the_auto_recovery_budget() {
    // The budget is per stall STREAK, not per consumer lifetime: a consumer that wedges,
    // is recovered, runs healthily for a while and later wedges again deserves its full
    // budget the second time. The only thing that grants it back is a dispatch unit
    // genuinely arriving — deliberately NOT the recovery's own re-subscribe, which zeroes
    // the same permit mirrors a real churn boundary does and would therefore refund every
    // attempt that bought it, leaving no bound at all.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(auto_recovery_config(1));
    let handle = open_shared_consumers(&shared, "sub-auto-budget", &["solo"], t0)[0];

    // Streak one spends the single attempt.
    let first = t0 + WINDOW;
    let (stalls, resubscribe) = sweep(&shared, first);
    assert_eq!(stalls, 1, "first stall episode");
    let request_id = resubscribe.expect("the first attempt is inside the budget");
    ack_resubscribe(&shared, request_id, first);

    // The broker resumes: one message, popped by the user. That is the whole definition
    // of progress the budget resets on.
    let resumed = first + Duration::from_secs(1);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(resumed, &message_frame(handle, 0, b"progress"))
            .expect("deliver");
        let message = conn
            .pop_message(handle, resumed)
            .expect("the dispatch reaches the user");
        assert_eq!(message.payload.as_ref(), b"progress");
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    // Re-seed the window on the dispatch, then wedge again.
    let (stalls, resubscribe) = sweep(&shared, resumed);
    assert_eq!(stalls, 0, "the dispatch re-seeded rather than reported");
    assert_eq!(
        resubscribe, None,
        "and re-seeding is not a recovery attempt"
    );
    let second = resumed + WINDOW;
    let (stalls, resubscribe) = sweep(&shared, second);
    assert_eq!(stalls, 1, "second stall episode");
    let request_id = resubscribe
        .expect("the dispatch restored the budget, so this streak may spend its own attempt");
    ack_resubscribe(&shared, request_id, second);

    // Nothing arrived this time, so the second streak's budget is now spent too.
    let (stalls, resubscribe) = sweep(&shared, second + WINDOW);
    assert_eq!(stalls, 1, "the exhausted episode of the second streak");
    assert_eq!(
        resubscribe, None,
        "a streak with no dispatch in it gets exactly `max_attempts` attempts"
    );
}

#[test]
fn a_consumer_the_recovery_may_not_touch_is_reported_but_never_re_subscribed() {
    // The refusal path, and the only state that reaches it: a pending unsubscribe is
    // simultaneously a stall CANDIDATE (the broker still holds un-spent permits over an
    // empty queue, nothing is closed, paused, seeking, terminal, or re-attaching) and
    // INELIGIBLE for an in-place re-attach, because that pending unsubscribe owns this
    // consumer's fate. Every other ineligible state also suppresses candidacy, so no
    // stall episode opens for it and the refusal is never reached.
    //
    // Charging that refusal to the budget would let an unrelated teardown race burn the
    // recovery a genuinely wedged consumer still needs, and re-subscribing anyway would
    // resurrect a consumer the caller is in the middle of retiring. So the watchdog must
    // still REPORT — the silence is real — while putting nothing at all on the wire.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(auto_recovery_config(MAX_ATTEMPTS));
    let handle = open_shared_consumers(&shared, "sub-auto-refused", &["solo"], t0)[0];

    {
        let mut conn = shared.inner.lock();
        let _ = conn.unsubscribe(handle, false);
        // Drop the `CommandUnsubscribe` this staged so the sweep below starts from an
        // empty outbound buffer.
        let _ = conn.poll_transmit();
        while conn.poll_event().is_some() {}
    }

    let (stalls, resubscribe) = sweep(&shared, t0 + WINDOW);
    assert_eq!(
        stalls, 1,
        "a consumer the client cannot recover is still one the operator needs told about"
    );
    assert_eq!(
        resubscribe, None,
        "an in-place re-attach for a consumer with an unsubscribe in flight must be \
         refused, not merely deferred"
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "and the refusal must mutate nothing: zeroing the mirrors for a consumer we then \
         decline to re-subscribe leaves it strictly worse off than the stall it was in"
    );
}

/// Encode a `CommandActiveConsumerChange` — the Failover active/standby announcement
/// (issue #348) whose `is_active` the recovery's standby pre-check reads.
///
/// Hand-encoded rather than driven through the scripted broker's
/// `announce_active_consumer_on_subscribe` knob: that knob exists for issue #427's
/// post-`Success` announcement and only ever sends `is_active: true`, so it structurally
/// cannot set up a standby.
fn active_consumer_change_frame(handle: ConsumerHandle, is_active: bool) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ActiveConsumerChange as i32,
        active_consumer_change: Some(pb::CommandActiveConsumerChange {
            consumer_id: handle.0,
            is_active: Some(is_active),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandActiveConsumerChange");
    buf
}

/// Feed one broker active/standby announcement for `handle`.
fn announce_active(
    shared: &ConnectionShared,
    handle: ConsumerHandle,
    is_active: bool,
    at: Instant,
) {
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &active_consumer_change_frame(handle, is_active))
        .expect("ActiveConsumerChange");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

/// Recovery budget for the Failover-standby test. Exactly one attempt, so any charge at
/// all would be visible: the test asserts no `CommandCloseConsumer` ever leaves the
/// connection and the permit mirror never moves, across standby episodes and after
/// promotion alike.
const STANDBY_BUDGET: u32 = 1;

#[test]
fn a_reported_failover_standby_is_reported_but_never_costs_an_attempt() {
    // A `Failover` standby satisfies the stall predicate PERMANENTLY and legitimately: it
    // holds the initial grant the broker acked at subscribe time over an empty queue, in a
    // dispatch-eligible state, and the broker correctly dispatches nothing to it because
    // the active consumer owns the subscription. It also never receives the one thing that
    // gives the budget back — a dispatch unit — so an unguarded recovery spends its whole
    // budget on every healthy standby in a failover group and never recovers it.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(auto_recovery_config(STANDBY_BUDGET));
    let handle = open_failover_consumer(&shared, "sub-failover-standby", "standby", t0);
    announce_active(&shared, handle, false, t0);

    // The report is unchanged from 1.5.0 — ADR-0101's event means SILENCE, not fault, and
    // a standby is genuinely silent — while the recovery is skipped outright.
    let (stalls, resubscribe) = sweep(&shared, t0 + WINDOW);
    assert_eq!(
        stalls, 1,
        "a standby still reports: suppressing it would make the event mean something \
         different depending on a knob the event does not carry"
    );
    assert_eq!(
        resubscribe, None,
        "the broker is not dispatching to a standby BY DESIGN, so there is nothing to \
         recover and no `CommandSubscribe` may go out"
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "and the skip mutates nothing"
    );
    for extra in [1u64, 2, 60] {
        let (stalls, resubscribe) = sweep(&shared, t0 + WINDOW + Duration::from_secs(extra));
        assert_eq!(
            stalls, 0,
            "the once-per-episode latch holds for a standby too"
        );
        assert_eq!(
            resubscribe, None,
            "and no attempt is ever made while standby"
        );
    }

    // ── Promotion. The skip spent nothing — `consumer_available_permits` is still the
    // untouched initial grant and no `CommandSubscribe` ever went out — so a promoted
    // consumer inherits everything it would have had.
    let promoted = t0 + WINDOW * 2;
    announce_active(&shared, handle, true, promoted);

    // Promotion alone does not restart the window: issue #307's re-arm calls
    // `initial_flow` only at `granted_permits == 0`, and ADR-0102 makes that a no-op for a
    // consumer that already holds its grant. Lose and regain candidacy so a fresh episode
    // can open — pausing drops the window on the next sweep, un-pausing lets the one after
    // re-seed it.
    shared.inner.lock().set_paused(handle, true);
    let (stalls, resubscribe) = sweep(&shared, promoted);
    assert_eq!(
        (stalls, resubscribe),
        (0, None),
        "a paused consumer never stalls"
    );
    shared.inner.lock().set_paused(handle, false);
    let (stalls, resubscribe) = sweep(&shared, promoted);
    assert_eq!((stalls, resubscribe), (0, None), "re-seeding tick only");

    // Since ADR-0108 a promoted ACTIVE `Failover` consumer is refused too, and by a
    // second, independent gate: the recovery's `CommandCloseConsumer` really detaches the
    // consumer, so the broker runs an election and this consumer returns at the end of the
    // priority order — a subscription-wide reshuffle to repair one slot. The standby
    // pre-check of ADR-0103 is therefore redundant for `Failover` specifically, and is
    // retained because it is what documents WHY a standby's silence is correct.
    //
    // The diagnosis survives both gates. That is the invariant: withholding the repair
    // never withholds the report.
    let (stalls, resubscribe) = sweep(&shared, promoted + WINDOW);
    assert_eq!(stalls, 1, "the promoted consumer's own stall episode");
    assert_eq!(
        resubscribe, None,
        "a `Failover` consumer has no in-place repair at any point in its lifecycle: \
         rung 1 is unavailable and the ladder continues at recreate / topics unload"
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "and a refused recovery mutates nothing, in either gate"
    );
}

// ---------------------------------------------------------------------------
// ADR-0108 — the recovery's first phase, and what happens when it does not land.
//
// `resubscribe_consumer_in_place` emits `CommandCloseConsumer` and defers EVERY
// mutation to that close's `Success`, because a `CommandSubscribe` naming a
// consumer id the broker still holds is a broker-side no-op: `ServerCnx
// .handleSubscribe` finds the id in its own per-connection map, answers
// `sendSuccessResponse(requestId)` and returns without touching the dispatcher or
// `availablePermits` (v4.0.4 `ServerCnx.java:1320-1326`, v4.2.4 `:1408-1414`,
// master `:2037-2044`).
//
// The three tests below pin the three ways phase 2 can fail to run.
// ---------------------------------------------------------------------------

/// The broker's `CommandError` for `request_id`.
fn error_frame(request_id: u64, code: pb::ServerError) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: code as i32,
            message: "broker says no".to_owned(),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandError");
    buf
}

/// Every `CommandCloseConsumer` and `CommandSubscribe` request id this connection
/// has staged, drained in one pass (`poll_transmit` empties the buffer).
fn drain_closes_and_subscribes(shared: &ConnectionShared) -> (Vec<u64>, Vec<u64>) {
    let mut out = shared.inner.lock().poll_transmit();
    let (mut closes, mut subscribes) = (Vec::new(), Vec::new());
    while !out.is_empty() {
        let frame = magnetar_proto::decode_one(&mut out).expect("decode outbound");
        if let Some(close) = frame.command.close_consumer {
            closes.push(close.request_id);
        } else if let Some(sub) = frame.command.subscribe {
            subscribes.push(sub.request_id);
        }
    }
    (closes, subscribes)
}

/// A broker that REJECTS the recovery close must leave the consumer exactly as the
/// caller found it, and no `CommandSubscribe` may follow.
///
/// This is the whole reason every mutation sits in phase 2. Zeroing the mirrors up
/// front and then failing to close would leave a consumer that is still wedged AND
/// invisible to the watchdog — `is_stall_candidate` requires `permit_balance > 0` —
/// which is strictly worse than the stall it was called to repair.
#[test]
fn a_rejected_recovery_close_leaves_the_consumer_untouched_and_recoverable() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handle = open_shared_consumers(&shared, "sub-close-rejected", &["solo"], t0)[0];

    let close_request_id = shared
        .inner
        .lock()
        .resubscribe_consumer_in_place(handle)
        .expect("a live, acked, un-gated durable non-Failover consumer is eligible")
        .0;
    let (closes, subscribes) = drain_closes_and_subscribes(&shared);
    assert_eq!(closes, vec![close_request_id], "phase 1 is the close alone");
    assert!(
        subscribes.is_empty(),
        "the re-attach is owed to the close ack"
    );

    shared
        .inner
        .lock()
        .handle_bytes(
            t0,
            &error_frame(close_request_id, pb::ServerError::ServiceNotReady),
        )
        .expect("recovery close Error");

    let (closes, subscribes) = drain_closes_and_subscribes(&shared);
    assert!(
        closes.is_empty() && subscribes.is_empty(),
        "a rejected close must not be followed by a CommandSubscribe — that is exactly \
         the live-id no-op ADR-0108 exists to stop emitting, got closes={closes:?} \
         subscribes={subscribes:?}"
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "the broker still holds this consumer's permits, so the client's mirror must \
         still describe them"
    );

    // Still detectable, and still recoverable: a rejected close is one spent attempt,
    // not a terminal state.
    shared.inner.lock().handle_timeout(t0);
    let (stalls, recovery) = sweep(&shared, t0 + WINDOW);
    assert_eq!(stalls, 1, "a consumer left wedged must still be reported");
    assert_eq!(
        recovery, None,
        "recovery is disarmed in this config; the report is the whole effect"
    );
    assert!(
        shared
            .inner
            .lock()
            .resubscribe_consumer_in_place(handle)
            .is_some(),
        "a rejected close leaves the consumer eligible for another attempt"
    );
}

/// Losing the session between the two phases hands the re-attach to the reconnect
/// path, which is its only correct owner: the broker reaped that consumer along with
/// the connection its close was written on.
///
/// `reset` and `fail_all_pending` both take `pending_requests` wholesale, which is
/// what clears the in-flight marker — and both must consume the entry WITHOUT
/// recording an outcome, since a recovery close has no waiter by construction and one
/// leaked entry per attempt is the issue #241 shape.
#[test]
fn a_session_lost_between_the_two_phases_defers_to_the_reconnect_rebuild() {
    let t0 = Instant::now();
    for (label, lose_session) in [
        (
            "reset",
            (|shared: &ConnectionShared| shared.inner.lock().reset()) as fn(&ConnectionShared),
        ),
        ("fail_all_pending", |shared: &ConnectionShared| {
            shared.inner.lock().fail_all_pending("engine shutting down");
        }),
    ] {
        let shared = ConnectionShared::new(watchdog_config());
        let handle = open_shared_consumers(&shared, "sub-close-session-lost", &["solo"], t0)[0];
        let close_request_id = shared
            .inner
            .lock()
            .resubscribe_consumer_in_place(handle)
            .expect("eligible")
            .0;
        let _ = drain_closes_and_subscribes(&shared);

        lose_session(&shared);

        // The close never lands, so phase 2 never runs — correctly, because the
        // broker dropped that consumer with the session.
        let (closes, subscribes) = drain_closes_and_subscribes(&shared);
        assert!(
            closes.is_empty() && subscribes.is_empty(),
            "{label}: the dead session's recovery must not be replayed, got \
             closes={closes:?} subscribes={subscribes:?}"
        );

        // And nothing from it keeps gating the next one: the marker lived in
        // `pending_requests`, which both paths emptied.
        let retry = shared.inner.lock().resubscribe_consumer_in_place(handle);
        match retry {
            Some(request_id) => assert_ne!(
                request_id.0, close_request_id,
                "{label}: a fresh close, not the abandoned one"
            ),
            None => panic!("{label}: the consumer must be recoverable again"),
        }
    }
}

/// `Failover` and non-durable subscriptions have no in-place recovery, because the
/// close it depends on is not a repair for either: it triggers a broker-side election,
/// or it discards a cursor that lives only as long as the consumer (ADR-0099).
///
/// Refused with no wire traffic and no mutation — and, for a `Failover` consumer, the
/// stall is still REPORTED, because ADR-0101's event means silence and a wedged active
/// `Failover` consumer is genuinely silent. Only the repair is withheld.
#[test]
fn failover_and_non_durable_subscriptions_are_refused_without_being_touched() {
    let t0 = Instant::now();
    for (label, sub_type, durable) in [
        ("failover", pb::command_subscribe::SubType::Failover, true),
        ("non-durable", pb::command_subscribe::SubType::Shared, false),
    ] {
        let shared = ConnectionShared::new(watchdog_config());
        let handle = open_consumers(
            &shared,
            sub_type,
            durable,
            "sub-no-in-place-repair",
            &["solo"],
            t0,
        )[0];

        assert_eq!(
            shared.inner.lock().resubscribe_consumer_in_place(handle),
            None,
            "{label}: closing this consumer is not a repair, so refuse it"
        );
        let (closes, subscribes) = drain_closes_and_subscribes(&shared);
        assert!(
            closes.is_empty() && subscribes.is_empty(),
            "{label}: a refused recovery must put nothing on the wire, got \
             closes={closes:?} subscribes={subscribes:?}"
        );
        assert_eq!(
            shared.inner.lock().consumer_available_permits(handle),
            RQ as u32,
            "{label}: a refused recovery must not touch the permit mirrors"
        );

        // The diagnosis is never suppressed by the repair being unavailable.
        shared.inner.lock().handle_timeout(t0);
        let (stalls, recovery) = sweep(&shared, t0 + WINDOW);
        assert_eq!(stalls, 1, "{label}: the wedge is still reported");
        assert_eq!(recovery, None, "{label}: and nothing is attempted");
    }
}

/// A second recovery while phase 1 is still in flight must not emit a second close.
///
/// Two `CommandCloseConsumer` frames for one consumer id would leave the second
/// answered against a consumer the first already removed, and its `Success` would
/// drive a second `CommandSubscribe` behind the re-attach the first one already made.
/// The in-flight marker is read straight out of `pending_requests`, which is what
/// makes a lost session clear it for free.
#[test]
fn a_second_recovery_is_refused_while_the_close_is_in_flight() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handle = open_shared_consumers(&shared, "sub-close-already-pending", &["solo"], t0)[0];

    let close_request_id = shared
        .inner
        .lock()
        .resubscribe_consumer_in_place(handle)
        .expect("eligible")
        .0;
    assert_eq!(
        shared.inner.lock().resubscribe_consumer_in_place(handle),
        None,
        "a recovery close is already awaiting its ack"
    );
    let (closes, subscribes) = drain_closes_and_subscribes(&shared);
    assert_eq!(
        closes,
        vec![close_request_id],
        "exactly one close on the wire"
    );
    assert!(
        subscribes.is_empty(),
        "and no re-attach before the close ack"
    );

    // Once the close is answered and the re-attach is out, the marker is gone — the
    // `flow_on_subscribe_ack` gate owns the consumer from here.
    shared
        .inner
        .lock()
        .handle_bytes(t0, &success_frame(close_request_id))
        .expect("recovery close Success");
    let (closes, subscribes) = drain_closes_and_subscribes(&shared);
    assert!(closes.is_empty(), "still exactly one close per recovery");
    assert_eq!(subscribes.len(), 1, "the re-attach follows the close ack");
}

/// The close lands, but by then the consumer has an owner for its next
/// `CommandSubscribe` — the application unsubscribed it while phase 1 was in flight.
///
/// Phase 2 must not re-attach it: `emit_in_place_consumer_resubscribe` re-runs the
/// shared eligibility gate for exactly this reason, so the consumer is left to the
/// path that now owns it rather than being raced back onto the connection.
#[test]
fn a_consumer_that_became_ineligible_while_the_close_flew_is_not_re_attached() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(watchdog_config());
    let handle = open_shared_consumers(&shared, "sub-close-then-ineligible", &["solo"], t0)[0];

    let close_request_id = shared
        .inner
        .lock()
        .resubscribe_consumer_in_place(handle)
        .expect("eligible")
        .0;
    let _ = drain_closes_and_subscribes(&shared);

    // The application unsubscribes while phase 1 is on the wire. That is the one
    // ineligible state which is also a live stall candidate, so it is the realistic
    // shape of this race.
    let _ = shared.inner.lock().try_unsubscribe(handle, false);
    let _ = drain_closes_and_subscribes(&shared);

    shared
        .inner
        .lock()
        .handle_bytes(t0, &success_frame(close_request_id))
        .expect("recovery close Success");

    let (closes, subscribes) = drain_closes_and_subscribes(&shared);
    assert!(
        closes.is_empty() && subscribes.is_empty(),
        "the pending unsubscribe owns this consumer's fate now, so phase 2 must emit \
         nothing, got closes={closes:?} subscribes={subscribes:?}"
    );
}
