// SPDX-License-Identifier: Apache-2.0

//! `Auto` receiver-queue growth under real dispatch-driven starvation, the
//! churn-window guard that must NOT be mistaken for it (issue #349), and the
//! adjust schedule's arming bootstrap (`docs/follow-ups.md` §4). The moonpool
//! mirror of `magnetar-runtime-tokio/tests/receiver_queue_auto_growth.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): nine `#[test]` functions here mirror the
//! tokio file's nine.
//!
//! ## What this pins
//!
//! Issue #349 split the consumer's permit mirror into `granted_permits` (a
//! purely additive record of every grant sent to the broker) and
//! `permit_balance` (the client-accounted balance, decremented once per broker dispatch
//! unit as it arrives). This file drives that split through the moonpool
//! engine's public [`magnetar_runtime_moonpool::ConnectionShared`] surface (no
//! driver task, no TCP listener — the same synthetic-clock pattern the
//! sibling `consumer_flow_control_edge.rs` uses):
//!
//! 1. `auto_receiver_queue_grows_under_real_dispatch_starvation` — real message deliveries (not a
//!    synthetic field write) drain the client-accounted permit balance to zero across a sustained
//!    multi-tick ramp; the target must double on each tick that observes genuine starvation.
//! 2. `auto_receiver_queue_skips_growth_during_churn_window` — a same-broker `CommandCloseConsumer`
//!    zeroes the permit mirror as part of the #307 re-attach dance; an adjust tick landing in that
//!    churn window must NOT misread the zero as starvation and grow.
//! 3. `auto_adjust_schedule_armed_by_initial_flow` — `Connection::initial_flow` alone puts the
//!    adjust deadline on `poll_timeout`, with no `handle_timeout` ever having run.
//! 4. `auto_adjust_schedule_survives_continuous_ack_response_traffic` — the follow-ups §4
//!    regression: a continuous `CommandAckResponse` stream refreshes `last_activity` on every frame
//!    and so defers the keepalive deadline indefinitely; the armed adjust deadline must be unmoved
//!    by it, and the tick on that deadline must still grow the target.
//! 5. `auto_growth_tops_up_the_broker_window_after_refund_rounds` and
//!    `auto_consumer_keeps_a_path_to_its_next_refill_after_refund_rounds` — a growth tick sizes its
//!    grant against the live broker window, not the cumulative grant record that every refill
//!    inflates, so refunds can neither leave the window short of the target nor strand the
//!    consumer.
//! 6. `auto_shrink_converges_the_broker_window_to_the_smaller_target` — the out-of-memory shrink
//!    must bring the broker window down, not only the refill threshold.
//! 7. `auto_oom_guard_forgets_the_bytes_a_reset_dropped` — a reset that drops the receiver queue
//!    must also drop the bytes the guard counts as buffered.
//!
//! 8. `auto_growth_after_a_shrink_is_covered_by_the_surplus_the_window_still_holds` — a growth
//!    right after the out-of-memory shrink grants nothing the broker window does not lack: the
//!    permits a shrink could not take back cover the target's regained size.
//!
//! Scenarios 5–8 keep the broker faithful (it never dispatches more than the granted permits),
//! unlike the ramp tests, which over-deliver on purpose.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Auto, ConnectionConfig, ConsumerHandle, MessageId, RequestId, SubscribeRequest,
    decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

/// Auto floor; the queue seeds here and doubles under repeated starvation.
const MIN: usize = 100;
/// Wide-open byte budget so the doubling rule, not the OOM cap, governs growth.
const MAX_BYTES: usize = 128 * 1024 * 1024;
/// Adjust tick cadence.
const TICK: Duration = Duration::from_secs(1);
/// Deliveries driven before each tick to drain the permit BALANCE to zero.
/// Safely exceeds the largest balance any tick in this ramp can observe
/// (400, ahead of the third growth tick); the decrement is `saturating_sub`,
/// so an over-generous batch just bottoms the balance out at zero.
const DRAIN_BATCH: u64 = 1000;

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

/// Subscribe an `Auto`-policy consumer over a handshaked connection, drain the
/// outbound `CommandSubscribe`, and feed the initial flow.
fn open_auto_consumer(
    shared: &ConnectionShared,
    topic: &str,
    min: usize,
    max_bytes: usize,
    adjust_interval: Duration,
    at: Instant,
) -> ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }
    let mut conn = shared.inner.lock();
    let handle = conn.subscribe(SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-auto-growth".to_owned(),
        receiver_queue_policy: Some(std::sync::Arc::new(Auto::new(min, max_bytes))),
        receiver_queue_adjust_interval: Some(adjust_interval),
        ..Default::default()
    });
    let _ = conn.initial_flow(handle, at);
    let _ = conn.poll_transmit();
    handle
}

/// Build a synthetic broker `CommandMessage` + payload addressed to `handle`
/// at broker entry `entry_id`. Each call is one distinct dispatch unit.
fn drain_message_frame(handle: ConsumerHandle, entry_id: u64, payload: &[u8]) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
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
        producer_name: "auto-growth-drain".to_owned(),
        sequence_id: entry_id,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode drain message frame");
    frame
}

/// Encode a broker-initiated same-broker `CommandCloseConsumer` frame (issue
/// #307 shape: `assigned_broker_service_url = None`).
fn close_consumer_frame(handle: ConsumerHandle) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id: handle.0,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandCloseConsumer");
    buf
}

/// Decode every `CommandFlow` grant for `handle` out of the connection's
/// outbound buffer, in order.
fn drain_flow_grants(shared: &ConnectionShared, handle: ConsumerHandle) -> Vec<u32> {
    let mut conn = shared.inner.lock();
    let mut out = conn.poll_transmit();
    let mut grants = Vec::new();
    while !out.is_empty() {
        let frame = decode_one(&mut out).expect("decode outbound");
        if frame.command.r#type == pb::base_command::Type::Flow as i32 {
            let flow = frame.command.flow.expect("flow body");
            if flow.consumer_id == handle.0 {
                grants.push(flow.message_permits);
            }
        }
    }
    grants
}

#[test]
fn auto_receiver_queue_grows_under_real_dispatch_starvation() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-growth",
        MIN,
        MAX_BYTES,
        TICK,
        t0,
    );

    let mut next_entry_id = 0u64;
    let mut targets = Vec::new();
    let mut grants = Vec::new();

    // The schedule was already armed at `t0` by `initial_flow` (follow-ups §4);
    // this tick lands before the `t0 + TICK` deadline and adjusts nothing.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0);
        let _ = conn.poll_transmit();
    }

    for i in 1..=4u32 {
        // Real dispatch-driven starvation: deliver enough messages to drain
        // the balance to zero — no manual field write.
        {
            let mut conn = shared.inner.lock();
            for _ in 0..DRAIN_BATCH {
                let frame = drain_message_frame(handle, next_entry_id, b"x");
                next_entry_id += 1;
                conn.handle_bytes(t0, &frame)
                    .expect("deliver drain message");
            }
            while conn.poll_event().is_some() {}
            let _ = conn.poll_transmit();
            conn.handle_timeout(t0 + TICK * i);
            targets.push(conn.consumer_receiver_queue_size(handle));
        }
        grants.extend(drain_flow_grants(&shared, handle));
    }

    assert_eq!(
        targets,
        vec![200, 400, 800, 1600],
        "sustained real dispatch-driven starvation must ramp by bounded doubling"
    );
    assert_eq!(
        grants,
        vec![100, 200, 400, 800],
        "each incremental flow tops the additive grant mirror up to the new target"
    );
}

#[test]
fn auto_receiver_queue_skips_growth_during_churn_window() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-growth-churn",
        MIN,
        MAX_BYTES,
        TICK,
        t0,
    );

    // The schedule is already armed at `t0` (`initial_flow`); this tick just
    // settles the connection before the churn event.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0);
        let _ = conn.poll_transmit();
    }

    // Same-broker bundle reassignment: the broker tears the consumer down
    // and re-subscribes it in place, zeroing the permit mirrors.
    {
        let mut conn = shared.inner.lock();
        let close = close_consumer_frame(handle);
        conn.handle_bytes(t0, &close).expect("handle broker close");
        let _ = conn.poll_transmit();
    }

    // Tick past the adjust interval while sitting in the churn window.
    let target = {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + TICK);
        conn.consumer_receiver_queue_size(handle)
    };
    assert_eq!(target, MIN, "a churn-window tick must not grow the target");
    assert!(
        drain_flow_grants(&shared, handle).is_empty(),
        "a churn-window tick must not emit an adjust-driven flow"
    );
}

/// Encode a broker `CommandAckResponse` resolving `request_id` for `handle`.
fn ack_response_frame(handle: ConsumerHandle, request_id: RequestId) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id: handle.0,
            request_id: Some(request_id.0),
            error: None,
            message: None,
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAckResponse");
    buf
}

/// One full ack round-trip at `at`: ack a message id, then feed the broker's
/// `CommandAckResponse` back in. This is the traffic shape a consumer that
/// awaits every individual ack produces, and every decoded frame refreshes the
/// connection's `last_activity` keepalive baseline (ADR-0058).
fn ack_round_trip(shared: &ConnectionShared, handle: ConsumerHandle, entry_id: u64, at: Instant) {
    let mut conn = shared.inner.lock();
    let request_id = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 4,
                entry_id,
                partition: -1,
                batch_index: -1,
                batch_size: -1,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        at,
    );
    let _ = conn.poll_transmit();
    conn.handle_bytes(at, &ack_response_frame(handle, request_id))
        .expect("handle AckResponse");
    let _ = conn.take_outcome(magnetar_proto::PendingOpKey::Request(request_id));
}

#[test]
fn auto_adjust_schedule_armed_by_initial_flow() {
    // follow-ups §4: `Connection::initial_flow` is the schedule's dedicated
    // bootstrap. With no `handle_timeout` ever having run, `poll_timeout` must
    // already report the adjust deadline — that is what the driver needs in
    // order to schedule the very first tick.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-growth-arming",
        MIN,
        MAX_BYTES,
        TICK,
        t0,
    );

    let conn = shared.inner.lock();
    assert_eq!(
        conn.consumer_receiver_queue_size(handle),
        MIN,
        "Auto seeds at its floor"
    );
    assert_eq!(
        conn.poll_timeout(),
        Some(t0 + TICK),
        "the adjust deadline must be armed by `initial_flow` alone, and must win the \
         `poll_timeout` minimum against the far-away default keepalive deadline"
    );
}

#[test]
fn auto_adjust_schedule_survives_continuous_ack_response_traffic() {
    // follow-ups §4 regression. Every decoded inbound frame refreshes
    // `last_activity` (ADR-0058's single refresh site), so a consumer awaiting
    // each individual ack slides the keepalive deadline forward forever. While
    // arming lived only in `handle_timeout`'s fallback arm, keepalive was the
    // ONLY deadline an unarmed `Auto` consumer had — so `handle_timeout` never
    // ran, the schedule never armed, and `Auto` never scaled under exactly the
    // load it exists to handle.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-growth-busy",
        MIN,
        MAX_BYTES,
        TICK,
        t0,
    );

    // Real dispatch-driven starvation: drain the initial grant's balance.
    {
        let mut conn = shared.inner.lock();
        for entry_id in 0..DRAIN_BATCH {
            let frame = drain_message_frame(handle, entry_id, b"x");
            conn.handle_bytes(t0, &frame)
                .expect("deliver drain message");
        }
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    // Nine ack round-trips at 100 ms cadence: continuous inbound traffic across
    // the whole sub-interval window, each frame pushing keepalive further out.
    let step = Duration::from_millis(100);
    for k in 1..=9u32 {
        let at = t0 + step * k;
        ack_round_trip(&shared, handle, u64::from(k), at);
        assert_eq!(
            shared.inner.lock().poll_timeout(),
            Some(t0 + TICK),
            "ack-response traffic must not defer the armed adjust deadline (round {k})"
        );
    }

    // The driver wakes on the armed deadline and the adjust runs there.
    let target = {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + TICK);
        conn.consumer_receiver_queue_size(handle)
    };
    assert_eq!(
        target,
        MIN * 2,
        "the armed tick must observe the drained permit balance and double the target"
    );
    assert_eq!(
        drain_flow_grants(&shared, handle),
        vec![MIN as u32],
        "the delta tops the additive grant mirror up to the new target"
    );
}

// ---------------------------------------------------------------------------
// Accounting drift between the three counters `Auto` reasons about
// (`granted_permits`, the broker window, `queued_bytes`). Unlike the ramp tests
// above, which over-deliver on purpose, every scenario below keeps the broker
// faithful: it never dispatches more than the permits the client has granted.
// ---------------------------------------------------------------------------

/// Deliver `count` single-message dispatch units of `payload` to `handle` and
/// drop the resulting `Message` events. Outbound bytes are left alone.
fn deliver(
    shared: &ConnectionShared,
    handle: ConsumerHandle,
    count: u64,
    payload: &[u8],
    next_entry_id: &mut u64,
    at: Instant,
) {
    let mut conn = shared.inner.lock();
    for _ in 0..count {
        let frame = drain_message_frame(handle, *next_entry_id, payload);
        *next_entry_id += 1;
        conn.handle_bytes(at, &frame).expect("deliver message");
    }
    while conn.poll_event().is_some() {}
}

/// Pop every queued message of `handle`, as an application draining its
/// receiver would. Refill flows the pops stage stay in the outbound buffer.
/// Returns the number popped.
fn pop_all(shared: &ConnectionShared, handle: ConsumerHandle, at: Instant) -> u64 {
    let mut conn = shared.inner.lock();
    let mut popped = 0;
    while conn.pop_message(handle, at).is_some() {
        popped += 1;
    }
    popped
}

fn available_permits(shared: &ConnectionShared, handle: ConsumerHandle) -> u32 {
    shared.inner.lock().consumer_available_permits(handle)
}

fn target(shared: &ConnectionShared, handle: ConsumerHandle) -> usize {
    shared.inner.lock().consumer_receiver_queue_size(handle)
}

/// An `Auto(100, 400_000)` consumer fed 1000-byte messages (byte-budget cap
/// 400) after four rounds in which the broker dispatched the whole 100-permit
/// window and the application popped all of it, with a fifth window
/// dispatched and left unpopped. The refunds of the four rounds raised the
/// cumulative grant record to 500 while the broker window stayed at 100, and
/// the permit balance is zero, so the next adjust tick sees starvation. The
/// byte budget is never approached (100 000 buffered of 400 000).
fn refund_rounds_then_starve(shared: &ConnectionShared, t0: Instant) -> (ConsumerHandle, u64) {
    let handle = open_auto_consumer(
        shared,
        "persistent://public/default/auto-refund-rounds",
        MIN,
        400_000,
        TICK,
        t0,
    );
    let payload = [b'x'; 1000];
    let mut next_entry_id = 0u64;
    for _ in 0..4 {
        deliver(shared, handle, 100, &payload, &mut next_entry_id, t0);
        assert_eq!(pop_all(shared, handle, t0), 100, "the window is popped");
    }
    deliver(shared, handle, 100, &payload, &mut next_entry_id, t0);
    let _ = drain_flow_grants(shared, handle);
    // Read the three values one lock at a time: the mutex is not reentrant.
    let balance = available_permits(shared, handle);
    let queued = shared.inner.lock().consumer_queue_len(handle);
    let current_target = target(shared, handle);
    assert_eq!(
        (balance, queued, current_target),
        (0, 100, MIN),
        "precondition: the broker window is still the original 100, fully dispatched"
    );
    (handle, next_entry_id)
}

#[test]
fn auto_growth_tops_up_the_broker_window_after_refund_rounds() {
    // ADR-0147: the growth grant was sized against the cumulative grant record
    // (500) instead of the live window (100), so the target doubled to 200 and
    // the `200 - 500` delta saturated to zero: no flow, and a window that no
    // longer matches the target.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let (handle, _) = refund_rounds_then_starve(&shared, t0);

    shared.inner.lock().handle_timeout(t0 + TICK);

    assert_eq!(
        target(&shared, handle),
        200,
        "starvation doubles the target"
    );
    assert_eq!(
        drain_flow_grants(&shared, handle),
        vec![100],
        "growing 100 -> 200 over a window of 100 grants the missing 100 permits"
    );
}

#[test]
fn auto_consumer_keeps_a_path_to_its_next_refill_after_refund_rounds() {
    // ADR-0147, consequence: the target reached 400 (refill threshold 200) while
    // the broker window stayed at 100, so no pop could ever refill again and the
    // consumer waited on a broker holding no permit.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let (handle, mut next_entry_id) = refund_rounds_then_starve(&shared, t0);

    shared.inner.lock().handle_timeout(t0 + TICK);
    let _ = drain_flow_grants(&shared, handle);
    for round in 2..=4u32 {
        pop_all(&shared, handle, t0);
        let held = available_permits(&shared, handle);
        deliver(
            &shared,
            handle,
            u64::from(held),
            &[b'x'; 1000],
            &mut next_entry_id,
            t0,
        );
        let _ = drain_flow_grants(&shared, handle);
        shared.inner.lock().handle_timeout(t0 + TICK * round);
        let _ = drain_flow_grants(&shared, handle);
    }

    pop_all(&shared, handle, t0);
    assert!(
        available_permits(&shared, handle) > 0,
        "once the application has drained what it holds, the broker must hold permits again"
    );
}

#[test]
fn auto_shrink_converges_the_broker_window_to_the_smaller_target() {
    // ADR-0147: a shrink under memory pressure stores the smaller target and
    // emits nothing, and each pop then refunds one for one, so the broker window
    // never falls and the buffered-bytes bound is not enforced.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    // 20 000-byte budget; the cap stays at 400 so only the OOM branch lowers the target.
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-shrink-window",
        MIN,
        20_000,
        TICK,
        t0,
    );
    let mut next_entry_id = 0u64;

    // Grow 100 -> 200 -> 400 on 1-byte messages: deliver and tick only, no pops.
    for i in 1..=2u32 {
        deliver(&shared, handle, 100, b"x", &mut next_entry_id, t0);
        shared.inner.lock().handle_timeout(t0 + TICK * i);
        let _ = drain_flow_grants(&shared, handle);
    }
    assert_eq!(target(&shared, handle), 400, "grown to 400");

    // The broker dispatches the 200 permits it still holds as 100-byte
    // messages: 20 200 buffered bytes, past the budget, so the tick halves it.
    let held = available_permits(&shared, handle);
    assert_eq!(
        held, 200,
        "precondition: 200 permits still held by the broker"
    );
    deliver(
        &shared,
        handle,
        u64::from(held),
        &[b'x'; 100],
        &mut next_entry_id,
        t0,
    );
    shared.inner.lock().handle_timeout(t0 + TICK * 3);
    assert_eq!(
        target(&shared, handle),
        200,
        "precondition: the out-of-memory guard halved the target"
    );
    assert!(
        drain_flow_grants(&shared, handle).is_empty(),
        "a shrink emits no flow"
    );

    // The application drains and the broker keeps dispatching what it holds. No
    // tick runs, so the target stays 200: the permits held after a drain must fit it.
    let mut held_after_drain = 0;
    for _ in 0..3 {
        pop_all(&shared, handle, t0);
        held_after_drain = available_permits(&shared, handle);
        deliver(
            &shared,
            handle,
            u64::from(held_after_drain),
            b"x",
            &mut next_entry_id,
            t0,
        );
        let _ = drain_flow_grants(&shared, handle);
    }
    assert!(
        held_after_drain as usize <= target(&shared, handle),
        "after the drain the broker holds {held_after_drain} permits, more than the shrunk \
         target {}; the window never shrank",
        target(&shared, handle)
    );
}

#[test]
fn auto_oom_guard_forgets_the_bytes_a_reset_dropped() {
    // `reset` cleared the receiver queue but not the buffered-byte
    // counter, so the dropped messages kept counting toward the out-of-memory
    // guard for good and pinned the target at its floor.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    // 10 000-byte budget; the first window of 100 x 100-byte messages fills it exactly.
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-reset-bytes",
        MIN,
        10_000,
        TICK,
        t0,
    );
    let mut next_entry_id = 0u64;
    deliver(&shared, handle, 100, &[b'x'; 100], &mut next_entry_id, t0);
    assert_eq!(shared.inner.lock().consumer_queue_len(handle), 100);

    // The transport drops: the queue is discarded and the client re-grants its window.
    {
        let mut conn = shared.inner.lock();
        conn.reset();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
        assert_eq!(conn.consumer_queue_len(handle), 0, "reset drops the queue");
        let _ = conn.initial_flow(handle, t0);
        let _ = conn.poll_transmit();
    }

    // The broker redelivers a window of tiny messages: 100 live bytes, far
    // below the budget, with the permit balance drained.
    deliver(&shared, handle, 100, b"x", &mut next_entry_id, t0);
    assert_eq!(
        available_permits(&shared, handle),
        0,
        "precondition: window dispatched"
    );

    shared.inner.lock().handle_timeout(t0 + TICK);

    assert_eq!(
        target(&shared, handle),
        MIN * 2,
        "100 live bytes of a 10 000-byte budget leave room: starvation must double the target"
    );
    assert_eq!(drain_flow_grants(&shared, handle), vec![MIN as u32]);
}

#[test]
fn auto_growth_after_a_shrink_is_covered_by_the_surplus_the_window_still_holds() {
    // After a shrink the broker still holds the old window (400 here) against a target of 200.
    // A growth back to 400 needs no new permit: the permits are already there, and a flow for
    // the 200 the target gained would put the window at 600, past the byte budget the target is
    // sized for.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    // 20 000-byte budget; the cap stays at 400 so only the budget decides the target.
    let handle = open_auto_consumer(
        &shared,
        "persistent://public/default/auto-shrink-then-grow",
        MIN,
        20_000,
        TICK,
        t0,
    );
    let mut next_entry_id = 0u64;

    // Grow 100 -> 200 -> 400 on 1-byte messages: deliver and tick only, no pops.
    for i in 1..=2u32 {
        deliver(&shared, handle, 100, b"x", &mut next_entry_id, t0);
        shared.inner.lock().handle_timeout(t0 + TICK * i);
        let _ = drain_flow_grants(&shared, handle);
    }
    assert_eq!(target(&shared, handle), 400, "precondition: grown to 400");

    // The 200 permits the broker still holds arrive as 99-byte messages: exactly the
    // 20 000-byte budget, so the next tick halves the target. The broker keeps its 400.
    deliver(&shared, handle, 200, &[b'x'; 99], &mut next_entry_id, t0);
    shared.inner.lock().handle_timeout(t0 + TICK * 3);
    assert_eq!(
        target(&shared, handle),
        200,
        "precondition: the out-of-memory guard halved the target"
    );
    assert!(
        drain_flow_grants(&shared, handle).is_empty(),
        "a shrink emits no flow"
    );

    // The application frees one byte, which puts the buffer back under the budget while the
    // balance is still zero: the next tick sees starvation with room and doubles.
    {
        let mut conn = shared.inner.lock();
        assert!(conn.pop_message(handle, t0).is_some());
        conn.handle_timeout(t0 + TICK * 4);
    }
    assert_eq!(
        target(&shared, handle),
        400,
        "starvation with room grows the target back"
    );
    assert!(
        drain_flow_grants(&shared, handle).is_empty(),
        "the broker already holds the window the target regained: nothing to grant"
    );

    // The surplus is spent: the application's refunds are granted in full again, up to the 400
    // window and not beyond.
    pop_all(&shared, handle, t0);
    assert_eq!(
        available_permits(&shared, handle),
        400,
        "the window is the target, no more and no less"
    );
}
