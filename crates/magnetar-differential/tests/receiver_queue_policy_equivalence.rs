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
//! field write) to drain the broker-side permit BALANCE to zero, and advances a
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
//! `ConsumerState::permit_balance` (the REAL balance, decremented once per
//! broker dispatch unit as it arrives). `Auto::adjust`'s starvation signal
//! (`FlowStats::available_permits`) is now fed from `permit_balance`, and
//! `adjust_receiver_queue` guards against `granted_permits == 0` (a churn
//! window, e.g. reset / same-broker `CloseConsumer`) by skipping the tick
//! entirely. A synthetic field write that zeroes BOTH counters together no
//! longer distinguishes "real starvation" from "churn window" the way the
//! production code does, so this test drains the balance the same way the
//! broker would: by actually dispatching messages.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Auto, Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
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
/// tick, real message deliveries drain the broker-side permit BALANCE to
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
    // Force the initial flow (seeds the broker grant at the floor).
    let _ = conn.initial_flow(handle);
    // Drain the subscribe + initial-flow frames; we only track adjust-driven flows.
    let _ = conn.poll_transmit();

    let seeded_target = conn.consumer_receiver_queue_size(handle);

    let mut targets_per_tick = Vec::with_capacity(TICKS);
    let mut flow_grants = Vec::new();
    let mut next_entry_id: u64 = 0;

    // First tick arms the adjust schedule (no adjust happens on it).
    conn.handle_timeout(t0);
    let _ = conn.poll_transmit();

    for i in 1..=TICKS {
        // Issue #349: drain the REAL permit balance via genuine dispatch
        // before the tick observes it.
        for _ in 0..DRAIN_BATCH {
            let frame = drain_message_frame(handle, next_entry_id, b"x");
            next_entry_id += 1;
            conn.handle_bytes(t0, &frame)
                .expect("deliver drain message");
        }
        // Drop the Message events + confirm no stray outbound bytes leaked
        // from delivery alone (delivery never emits a flow on its own here
        // — `consumed_since_flow` only moves on `pop_message`, which this
        // test never calls).
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
    // ticks. `granted_permits` (the additive grant mirror the want-have
    // delta is computed against — issue #349 design item 1) is NEVER
    // decremented by dispatch, so each incremental flow tops it up from the
    // PREVIOUS target to the new one: 100->200 (+100), 200->400 (+200),
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
