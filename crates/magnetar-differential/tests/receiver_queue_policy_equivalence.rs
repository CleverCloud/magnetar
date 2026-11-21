// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` / flow-grant parity for the
//! pluggable receiver-queue policy (issue #301, PIP-74 auto-scaled queue).
//!
//! An [`magnetar_proto::Auto`] consumer recomputes its receiver-queue target from
//! [`magnetar_proto::FlowStats`] on every adjust tick driven by
//! `Connection::handle_timeout(now)`. A grown target emits an incremental
//! `CommandFlow`. The whole decision is pure sans-io state both engines wrap
//! behind a `parking_lot::Mutex`, so the differential claim is that **the
//! flow-grant trajectory under starvation is byte-for-byte identical across the
//! two engines** for the same synthetic clock + frame history.
//!
//! This test subscribes an `Auto` consumer over each engine's locked
//! `Connection`, forces the initial flow, then before each adjust tick drives
//! REAL message deliveries (issue #349: not a synthetic `available_permits = 0`
//! field write) to drain the client-accounted permit balance to zero, and advances a
//! SHARED synthetic [`Instant`] across several adjust ticks — capturing the
//! receiver-queue target trajectory and the ordered `CommandFlow` grants. The
//! two engines must agree, and the trajectory must be the correct
//! bounded-doubling ramp.
//!
//! ## Why real deliveries, not a field write
//!
//! Issue #349 split the consumer's permit mirror into two counters:
//! `ConsumerState::granted_permits` (a purely additive record of every grant
//! sent to the broker — never decremented by dispatch) and
//! `ConsumerState::permit_balance` (the client-accounted balance, decremented once per
//! broker dispatch unit as it arrives). `Auto::adjust`'s starvation signal
//! (`FlowStats::available_permits`) is now fed from `permit_balance`, and
//! `adjust_receiver_queue` guards against `granted_permits == 0` (a churn
//! window, e.g. reset / same-broker `CloseConsumer`) by skipping the tick
//! entirely. A synthetic field write that zeroes BOTH counters together no
//! longer distinguishes "real starvation" from "churn window" the way the
//! production code does, so this test drains the balance the same way the
//! broker would: by actually dispatching messages.
//!
//! ## Second scenario: the arming bootstrap under a busy connection
//!
//! `receiver_queue_adjust_arming_agrees_under_continuous_ack_traffic` pins the
//! `docs/follow-ups.md` §4 fix across both engines. `Connection::initial_flow`
//! arms the adjust schedule from its injected `now`, so the first tick's
//! deadline is a function of the subscribe-ack instant alone. The scenario
//! drives continuous `CommandAckResponse` traffic — every decoded frame
//! refreshes `last_activity` and pushes the keepalive deadline out (ADR-0058's
//! single refresh site) — and captures the `poll_timeout()` sequence as offsets
//! from `t0`. Both engines must report the same immovable adjust deadline: a
//! deadline that slid with the traffic would mean the schedule never armed,
//! which is precisely the defect, and under moonpool it would also make the
//! first adjust land at a seed-dependent simulated instant.
//!
//! ## Further scenarios: accounting drift under a faithful broker
//!
//! The scenarios above over-deliver on purpose (`DRAIN_BATCH`). The four at the end of the file
//! keep the broker faithful — it never dispatches more than the permits the client granted — and
//! pin that the counters `Auto` reasons about stay consistent across both engines:
//! `receiver_queue_growth_after_refund_rounds_agrees_and_tops_up_the_window` (a growth grant is
//! sized against the live broker window, not the cumulative grant record every refill inflates),
//! `receiver_queue_shrink_agrees_and_converges_the_broker_window` (the out-of-memory shrink brings
//! the broker window down, not only the refill threshold) and
//! `receiver_queue_oom_guard_after_reset_agrees_and_forgets_dropped_bytes` (a reset that drops the
//! receiver queue drops the bytes the guard counts as buffered) and
//! `receiver_queue_growth_after_shrink_agrees_and_grants_only_what_the_window_lacks` (a growth
//! right after a shrink is covered by the permits the shrink could not take back).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Auto, Connection, ConnectionConfig, ConsumerHandle, MessageId, PendingOpKey,
    RequestId, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};

/// Auto floor; the queue seeds here and doubles under repeated starvation.
const MIN: usize = 100;
/// Wide-open byte budget so the doubling rule, not the OOM cap, governs growth.
const MAX_BYTES: usize = 128 * 1024 * 1024;
/// Adjust tick cadence.
const TICK: Duration = Duration::from_secs(1);
/// Number of adjust ticks to drive after arming the schedule.
const TICKS: usize = 4;
/// Deliveries driven before each tick to drain the permit BALANCE to zero.
/// The largest balance any tick in this 4-tick doubling ramp can observe is
/// 400 (the delta granted ahead of the fourth tick — see the module doc's
/// derivation in the sibling proto/runtime tests); this safely exceeds every
/// tick's requirement, and the decrement is `saturating_sub`, so an
/// over-generous batch just bottoms the balance out at zero rather than
/// under/over-shooting.
const DRAIN_BATCH: u64 = 1000;
/// Ack round-trips driven inside one adjust interval by the arming scenario.
/// Nine 100 ms steps stay strictly inside the 1 s `TICK`, so every captured
/// `poll_timeout()` is taken while the armed deadline is still in the future.
const ACK_ROUND_TRIPS: usize = 9;

/// The observable reaction both engines must agree on: the seeded target, the
/// receiver-queue target after each adjust tick, and the ordered `CommandFlow`
/// grants emitted across the whole tick sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reaction {
    seeded_target: usize,
    targets_per_tick: Vec<usize>,
    flow_grants: Vec<u32>,
}

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

/// Build a synthetic broker `CommandMessage` + payload addressed to `handle`
/// at broker entry `entry_id`. Each call is one distinct dispatch unit — one
/// permit the broker spends to push it.
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
        producer_name: "auto-rq-equiv-drain".to_owned(),
        sequence_id: entry_id,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode drain message frame");
    frame
}

/// Decode every `CommandFlow` grant for `handle` out of the connection's
/// outbound buffer, in order.
fn drain_flow_grants(conn: &mut Connection, handle: ConsumerHandle) -> Vec<u32> {
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

/// Drive handshake + subscribe-Auto + initial-flow + N adjust ticks over one
/// engine's locked `Connection`, returning the captured reaction. Before each
/// tick, real message deliveries drain the client-accounted permit balance to
/// zero so every tick observes genuine dispatch-driven starvation — exactly
/// the ramp PIP-74 auto-scaling targets.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Reaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/auto-rq-equiv".to_owned(),
        subscription: "sub-auto-rq-equiv".to_owned(),
        receiver_queue_policy: Some(Arc::new(Auto::new(MIN, MAX_BYTES))),
        receiver_queue_adjust_interval: Some(TICK),
        ..Default::default()
    };
    let handle: ConsumerHandle = conn.subscribe(req);
    // Force the initial flow (seeds the broker grant at the floor, and arms the
    // adjust schedule at `t0` — follow-ups §4).
    let _ = conn.initial_flow(handle, t0);
    // Drain the subscribe + initial-flow frames; we only track adjust-driven flows.
    let _ = conn.poll_transmit();

    let seeded_target = conn.consumer_receiver_queue_size(handle);

    let mut targets_per_tick = Vec::with_capacity(TICKS);
    let mut flow_grants = Vec::new();
    let mut next_entry_id: u64 = 0;

    // Already armed at `t0`; this tick lands short of the `t0 + TICK` deadline
    // and adjusts nothing.
    conn.handle_timeout(t0);
    let _ = conn.poll_transmit();

    for i in 1..=TICKS {
        // Issue #349: drain the client-accounted permit balance via genuine dispatch
        // before the tick observes it.
        for _ in 0..DRAIN_BATCH {
            let frame = drain_message_frame(handle, next_entry_id, b"x");
            next_entry_id += 1;
            conn.handle_bytes(t0, &frame)
                .expect("deliver drain message");
        }
        // Drop the Message events + confirm no stray outbound bytes leaked
        // from delivery alone (delivery never emits a flow on its own here —
        // no frame in this test is popped, dead-lettered, a PIP-33 marker or
        // an incomplete chunk, so `consumed_since_flow` never moves).
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();

        let now = t0 + TICK * (i as u32);
        conn.handle_timeout(now);
        targets_per_tick.push(conn.consumer_receiver_queue_size(handle));
        flow_grants.extend(drain_flow_grants(conn, handle));
    }

    Reaction {
        seeded_target,
        targets_per_tick,
        flow_grants,
    }
}

#[test]
fn receiver_queue_policy_auto_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: both engines ramp the Auto receiver
    // queue identically under real dispatch-driven starvation.
    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Auto receiver-queue ramp"
    );

    // And the ramp is the correct bounded-doubling trajectory on both
    // engines: seed at the floor, then 200, 400, 800, 1600 across four
    // ticks. A growth tick sizes its grant against the window the broker
    // holds, which is the PREVIOUS target (ADR-0147; not the cumulative
    // `granted_permits`), so each incremental flow tops the window up from
    // the previous target to the new one: 100->200 (+100), 200->400 (+200),
    // 400->800 (+400), 800->1600 (+800).
    let expected = Reaction {
        seeded_target: MIN,
        targets_per_tick: vec![200, 400, 800, 1600],
        flow_grants: vec![100, 200, 400, 800],
    };
    assert_eq!(
        tokio_reaction, expected,
        "Auto must ramp by bounded doubling under real dispatch-driven starvation, got {tokio_reaction:?}"
    );
}

/// The observable reaction under continuous ack-response traffic, captured as
/// offsets from `t0` so it is comparable across engines without leaking the
/// absolute `Instant` into the equality check.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BusyReaction {
    /// `poll_timeout()` right after `initial_flow`, relative to `t0`.
    armed_deadline_offset: Option<Duration>,
    /// `poll_timeout()` after each ack round-trip, relative to `t0`.
    deadline_offsets_per_ack: Vec<Option<Duration>>,
    /// Receiver-queue target after the tick on the armed deadline.
    target_after_armed_tick: usize,
    /// Ordered `CommandFlow` grants emitted by that tick.
    flow_grants: Vec<u32>,
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

/// Drive handshake + subscribe-Auto + initial-flow, then a run of sub-interval
/// ack round-trips, over one engine's locked `Connection`.
///
/// Each round-trip decodes an inbound frame, which refreshes `last_activity`
/// (ADR-0058's single refresh site) and so pushes the keepalive deadline out.
/// The captured `poll_timeout()` sequence is the differential claim: on BOTH
/// engines the armed adjust deadline must sit at `t0 + TICK` and stay there,
/// unmoved by the traffic — which is what makes the first adjust tick fire at
/// the same simulated instant under moonpool as under tokio (follow-ups §4).
fn lock_and_run_busy(conn: &mut Connection, t0: Instant) -> BusyReaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/auto-rq-busy".to_owned(),
        subscription: "sub-auto-rq-busy".to_owned(),
        receiver_queue_policy: Some(Arc::new(Auto::new(MIN, MAX_BYTES))),
        receiver_queue_adjust_interval: Some(TICK),
        ..Default::default()
    };
    let handle: ConsumerHandle = conn.subscribe(req);
    let _ = conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();

    let armed_deadline_offset = conn.poll_timeout().map(|d| d.duration_since(t0));

    // Real dispatch-driven starvation before the tick observes the balance.
    for entry_id in 0..DRAIN_BATCH {
        let frame = drain_message_frame(handle, entry_id, b"x");
        conn.handle_bytes(t0, &frame)
            .expect("deliver drain message");
    }
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();

    // Continuous ack round-trips across the whole sub-interval window.
    let step = Duration::from_millis(100);
    let mut deadline_offsets_per_ack = Vec::with_capacity(ACK_ROUND_TRIPS);
    for k in 1..=ACK_ROUND_TRIPS {
        let at = t0 + step * u32::try_from(k).expect("small loop counter");
        let request_id = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![MessageId {
                    ledger_id: 4,
                    entry_id: k as u64,
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
        let _ = conn.take_outcome(PendingOpKey::Request(request_id));
        deadline_offsets_per_ack.push(conn.poll_timeout().map(|d| d.duration_since(t0)));
    }

    conn.handle_timeout(t0 + TICK);
    let target_after_armed_tick = conn.consumer_receiver_queue_size(handle);
    let flow_grants = drain_flow_grants(conn, handle);

    BusyReaction {
        armed_deadline_offset,
        deadline_offsets_per_ack,
        target_after_armed_tick,
        flow_grants,
    }
}

#[test]
fn receiver_queue_adjust_arming_agrees_under_continuous_ack_traffic() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run_busy(&mut conn, t0)
    };

    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run_busy(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Auto adjust-schedule arming under \
         continuous ack-response traffic"
    );

    // And the shared behaviour is the right one: armed at `t0 + TICK` by
    // `initial_flow` alone, held there across every ack round-trip (a sliding
    // keepalive-derived deadline here would mean the schedule never armed —
    // the follow-ups §4 defect), and the tick on that deadline doubles the
    // target with a matching incremental grant.
    let expected = BusyReaction {
        armed_deadline_offset: Some(TICK),
        deadline_offsets_per_ack: vec![Some(TICK); ACK_ROUND_TRIPS],
        target_after_armed_tick: MIN * 2,
        flow_grants: vec![MIN as u32],
    };
    assert_eq!(
        tokio_reaction, expected,
        "the armed adjust deadline must be immovable by inbound traffic, got {tokio_reaction:?}"
    );
}

// ---------------------------------------------------------------------------
// Accounting drift between the counters `Auto` reasons about
// (`granted_permits`, the broker window, `queued_bytes`). The scenarios above
// over-deliver on purpose; every scenario below keeps the broker faithful —
// it never dispatches more than the permits the client has granted — so the
// expected window does not depend on how the drift is repaired.
// ---------------------------------------------------------------------------

/// Handshake + subscribe an `Auto(min, max_bytes)` consumer + initial flow, and
/// drain the outbound frames.
fn open_auto(
    conn: &mut Connection,
    topic: &str,
    min: usize,
    max_bytes: usize,
    t0: Instant,
) -> ConsumerHandle {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let handle = conn.subscribe(SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "sub-auto-rq-drift".to_owned(),
        receiver_queue_policy: Some(Arc::new(Auto::new(min, max_bytes))),
        receiver_queue_adjust_interval: Some(TICK),
        ..Default::default()
    });
    let _ = conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();
    handle
}

/// Dispatch `count` single-message units of `payload` to `handle` and drop the
/// resulting `Message` events. Outbound bytes are left alone.
fn deliver(
    conn: &mut Connection,
    handle: ConsumerHandle,
    count: u64,
    payload: &[u8],
    next_entry_id: &mut u64,
    at: Instant,
) {
    for _ in 0..count {
        let frame = drain_message_frame(handle, *next_entry_id, payload);
        *next_entry_id += 1;
        conn.handle_bytes(at, &frame).expect("deliver message");
    }
    while conn.poll_event().is_some() {}
}

/// Pop every queued message of `handle`, as an application draining its
/// receiver would. Returns the number popped.
fn pop_all(conn: &mut Connection, handle: ConsumerHandle, at: Instant) -> u64 {
    let mut popped = 0;
    while conn.pop_message(handle, at).is_some() {
        popped += 1;
    }
    popped
}

/// Reaction of an `Auto(100, 400_000)` consumer that popped four full windows
/// of 1000-byte messages (the refunds raise the cumulative grant record to 500
/// while the broker window stays 100) and then starved.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RefundRoundsReaction {
    /// Receiver-queue target after the first adjust tick.
    target_after_tick: usize,
    /// `CommandFlow` grants that tick emitted.
    grants_after_tick: Vec<u32>,
    /// Permits the broker holds after three more faithful rounds and a final
    /// drain by the application.
    permits_after_final_drain: u32,
}

fn run_refund_rounds(conn: &mut Connection, t0: Instant) -> RefundRoundsReaction {
    let handle = open_auto(
        conn,
        "persistent://public/default/auto-rq-refund-rounds",
        MIN,
        400_000,
        t0,
    );
    let payload = [b'x'; 1000];
    let mut next_entry_id = 0u64;
    for _ in 0..4 {
        deliver(conn, handle, 100, &payload, &mut next_entry_id, t0);
        assert_eq!(pop_all(conn, handle, t0), 100, "the window is popped");
    }
    deliver(conn, handle, 100, &payload, &mut next_entry_id, t0);
    let _ = drain_flow_grants(conn, handle);
    assert_eq!(
        (
            conn.consumer_available_permits(handle),
            conn.consumer_queue_len(handle),
            conn.consumer_receiver_queue_size(handle)
        ),
        (0, 100, MIN),
        "precondition: the broker window is still the original 100, fully dispatched"
    );

    conn.handle_timeout(t0 + TICK);
    let target_after_tick = conn.consumer_receiver_queue_size(handle);
    let grants_after_tick = drain_flow_grants(conn, handle);

    for round in 2..=4u32 {
        pop_all(conn, handle, t0);
        let held = u64::from(conn.consumer_available_permits(handle));
        deliver(conn, handle, held, &payload, &mut next_entry_id, t0);
        let _ = drain_flow_grants(conn, handle);
        conn.handle_timeout(t0 + TICK * round);
        let _ = drain_flow_grants(conn, handle);
    }
    pop_all(conn, handle, t0);

    RefundRoundsReaction {
        target_after_tick,
        grants_after_tick,
        permits_after_final_drain: conn.consumer_available_permits(handle),
    }
}

#[test]
fn receiver_queue_growth_after_refund_rounds_agrees_and_tops_up_the_window() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_refund_rounds(&mut conn, t0)
    };
    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_refund_rounds(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on Auto growth after refund rounds"
    );
    assert_eq!(
        tokio_reaction.target_after_tick, 200,
        "starvation doubles the target, got {tokio_reaction:?}"
    );
    assert_eq!(
        tokio_reaction.grants_after_tick,
        vec![100],
        "growing 100 -> 200 over a window of 100 grants the missing 100 permits, got {tokio_reaction:?}"
    );
    assert!(
        tokio_reaction.permits_after_final_drain > 0,
        "the consumer must keep a path to its next refill, got {tokio_reaction:?}"
    );
}

/// Reaction of an `Auto(100, 20_000)` consumer whose window grew to 400 on tiny
/// messages and whose buffered bytes then crossed the budget.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShrinkReaction {
    /// Target after the tick that crossed the byte budget.
    target_after_shrink: usize,
    /// `CommandFlow` grants that tick emitted.
    grants_on_shrink: Vec<u32>,
    /// Permits the broker holds after the application drained for three faithful rounds.
    permits_held_after_drain: u32,
}

fn run_shrink(conn: &mut Connection, t0: Instant) -> ShrinkReaction {
    let handle = open_auto(
        conn,
        "persistent://public/default/auto-rq-shrink-window",
        MIN,
        20_000,
        t0,
    );
    let mut next_entry_id = 0u64;
    // Grow 100 -> 200 -> 400 on 1-byte messages: deliver and tick only, no pops.
    for i in 1..=2u32 {
        deliver(conn, handle, 100, b"x", &mut next_entry_id, t0);
        conn.handle_timeout(t0 + TICK * i);
        let _ = drain_flow_grants(conn, handle);
    }
    assert_eq!(
        conn.consumer_receiver_queue_size(handle),
        400,
        "precondition: grown to 400"
    );
    // The broker dispatches the 200 permits it still holds as 100-byte messages:
    // 20 200 buffered bytes, past the budget.
    let held = conn.consumer_available_permits(handle);
    assert_eq!(
        held, 200,
        "precondition: 200 permits still held by the broker"
    );
    deliver(
        conn,
        handle,
        u64::from(held),
        &[b'x'; 100],
        &mut next_entry_id,
        t0,
    );
    conn.handle_timeout(t0 + TICK * 3);
    let target_after_shrink = conn.consumer_receiver_queue_size(handle);
    let grants_on_shrink = drain_flow_grants(conn, handle);

    // No tick runs from here on, so the target stays where the shrink left it.
    let mut permits_held_after_drain = 0;
    for _ in 0..3 {
        pop_all(conn, handle, t0);
        permits_held_after_drain = conn.consumer_available_permits(handle);
        deliver(
            conn,
            handle,
            u64::from(permits_held_after_drain),
            b"x",
            &mut next_entry_id,
            t0,
        );
        let _ = drain_flow_grants(conn, handle);
    }
    ShrinkReaction {
        target_after_shrink,
        grants_on_shrink,
        permits_held_after_drain,
    }
}

#[test]
fn receiver_queue_shrink_agrees_and_converges_the_broker_window() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_shrink(&mut conn, t0)
    };
    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_shrink(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Auto out-of-memory shrink"
    );
    assert_eq!(
        (
            tokio_reaction.target_after_shrink,
            &tokio_reaction.grants_on_shrink
        ),
        (200, &Vec::new()),
        "the out-of-memory guard halves the target and a shrink emits no flow, got {tokio_reaction:?}"
    );
    assert!(
        tokio_reaction.permits_held_after_drain as usize <= tokio_reaction.target_after_shrink,
        "the broker window never shrank to the smaller target, got {tokio_reaction:?}"
    );
}

/// Reaction of an `Auto(100, 10_000)` consumer whose first window of 100 x
/// 100-byte messages exactly filled the byte budget before a transport reset.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResetBytesReaction {
    /// Receiver-queue length right after the reset.
    queue_len_after_reset: usize,
    /// Target after the first starved tick on the redelivered 1-byte window.
    target_after_tick: usize,
    /// `CommandFlow` grants that tick emitted.
    grants_after_tick: Vec<u32>,
}

fn run_reset_bytes(conn: &mut Connection, t0: Instant) -> ResetBytesReaction {
    let handle = open_auto(
        conn,
        "persistent://public/default/auto-rq-reset-bytes",
        MIN,
        10_000,
        t0,
    );
    let mut next_entry_id = 0u64;
    deliver(conn, handle, 100, &[b'x'; 100], &mut next_entry_id, t0);
    assert_eq!(conn.consumer_queue_len(handle), 100, "precondition");

    // The transport drops: the queue is discarded and the client re-grants its window.
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let queue_len_after_reset = conn.consumer_queue_len(handle);
    let _ = conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();

    // The broker redelivers a window of tiny messages: 100 live bytes of a
    // 10 000-byte budget, with the permit balance drained.
    deliver(conn, handle, 100, b"x", &mut next_entry_id, t0);
    assert_eq!(
        conn.consumer_available_permits(handle),
        0,
        "precondition: window dispatched"
    );
    conn.handle_timeout(t0 + TICK);
    ResetBytesReaction {
        queue_len_after_reset,
        target_after_tick: conn.consumer_receiver_queue_size(handle),
        grants_after_tick: drain_flow_grants(conn, handle),
    }
}

#[test]
fn receiver_queue_oom_guard_after_reset_agrees_and_forgets_dropped_bytes() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_reset_bytes(&mut conn, t0)
    };
    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_reset_bytes(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Auto out-of-memory guard after a reset"
    );
    let expected = ResetBytesReaction {
        queue_len_after_reset: 0,
        target_after_tick: MIN * 2,
        grants_after_tick: vec![MIN as u32],
    };
    assert_eq!(
        tokio_reaction, expected,
        "100 live bytes of a 10 000-byte budget leave room: starvation must double the target"
    );
}

/// Reaction of an `Auto(100, 20_000)` consumer whose window shrank from 400 to 200 under memory
/// pressure and whose target then grew back to 400 while the broker still held the old window.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShrinkThenGrowReaction {
    /// Target after the tick that crossed the byte budget.
    target_after_shrink: usize,
    /// Target after the tick that found the buffer under budget and the balance at zero.
    target_after_regrowth: usize,
    /// `CommandFlow` grants that regrowth tick emitted.
    grants_on_regrowth: Vec<u32>,
    /// Permits the broker holds once the application has drained the queue.
    permits_after_drain: u32,
}

fn run_shrink_then_grow(conn: &mut Connection, t0: Instant) -> ShrinkThenGrowReaction {
    let handle = open_auto(
        conn,
        "persistent://public/default/auto-rq-shrink-then-grow",
        MIN,
        20_000,
        t0,
    );
    let mut next_entry_id = 0u64;
    // Grow 100 -> 200 -> 400 on 1-byte messages: deliver and tick only, no pops.
    for i in 1..=2u32 {
        deliver(conn, handle, 100, b"x", &mut next_entry_id, t0);
        conn.handle_timeout(t0 + TICK * i);
        let _ = drain_flow_grants(conn, handle);
    }
    // The 200 permits the broker still holds arrive as 99-byte messages: exactly the 20 000-byte
    // budget, so the next tick halves the target.
    deliver(conn, handle, 200, &[b'x'; 99], &mut next_entry_id, t0);
    conn.handle_timeout(t0 + TICK * 3);
    let target_after_shrink = conn.consumer_receiver_queue_size(handle);
    let _ = drain_flow_grants(conn, handle);

    // One pop frees a byte: back under the budget with the balance still zero.
    assert!(conn.pop_message(handle, t0).is_some());
    conn.handle_timeout(t0 + TICK * 4);
    let target_after_regrowth = conn.consumer_receiver_queue_size(handle);
    let grants_on_regrowth = drain_flow_grants(conn, handle);

    pop_all(conn, handle, t0);
    ShrinkThenGrowReaction {
        target_after_shrink,
        target_after_regrowth,
        grants_on_regrowth,
        permits_after_drain: conn.consumer_available_permits(handle),
    }
}

#[test]
fn receiver_queue_growth_after_shrink_agrees_and_grants_only_what_the_window_lacks() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_shrink_then_grow(&mut conn, t0)
    };
    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        run_shrink_then_grow(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on Auto growth right after a shrink"
    );
    let expected = ShrinkThenGrowReaction {
        target_after_shrink: 200,
        target_after_regrowth: 400,
        grants_on_regrowth: Vec::new(),
        permits_after_drain: 400,
    };
    assert_eq!(
        tokio_reaction, expected,
        "the broker already holds the window the target regained: nothing to grant, and the window ends at the target"
    );
}
