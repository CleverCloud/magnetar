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
//! `Connection`, forces the initial flow, drains the broker grant to zero
//! (starvation), then advances a SHARED synthetic [`Instant`] across several
//! adjust ticks — capturing the receiver-queue target trajectory and the ordered
//! `CommandFlow` grants. The two engines must agree, and the trajectory must be
//! the correct bounded-doubling ramp.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Auto, Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, decode_one,
    encode_command, pb,
};

/// Auto floor; the queue seeds here and doubles under repeated starvation.
const MIN: usize = 100;
/// Wide-open byte budget so the doubling rule, not the OOM cap, governs growth.
const MAX_BYTES: usize = 128 * 1024 * 1024;
/// Adjust tick cadence.
const TICK: Duration = Duration::from_secs(1);
/// Number of adjust ticks to drive after arming the schedule.
const TICKS: usize = 4;

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

/// Drive handshake + subscribe-Auto + initial-flow + starvation + N adjust ticks
/// over one engine's locked `Connection`, returning the captured reaction. The
/// broker grant is zeroed before the ticks so every tick observes starvation —
/// exactly the ramp PIP-74 auto-scaling targets.
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

    // Zero the broker grant so each tick observes `available_permits == 0`.
    if let Some(slot) = conn.consumer(handle) {
        slot.state.lock().available_permits = 0;
    }

    let mut targets_per_tick = Vec::with_capacity(TICKS);
    let mut flow_grants = Vec::new();
    // The first tick arms the adjust schedule (no adjust); each later tick runs
    // one adjust. Drive TICKS+1 ticks so we capture TICKS adjustments.
    for i in 0..=TICKS {
        let now = t0 + TICK * (i as u32);
        conn.handle_timeout(now);
        if i > 0 {
            targets_per_tick.push(conn.consumer_receiver_queue_size(handle));
            flow_grants.extend(drain_flow_grants(conn, handle));
            // Re-zero the grant so the next tick still sees starvation (the
            // incremental flow just topped it back up to the target).
            if let Some(slot) = conn.consumer(handle) {
                slot.state.lock().available_permits = 0;
            }
        } else {
            // Arming tick: no adjust, drain nothing meaningful.
            let _ = conn.poll_transmit();
        }
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
    // queue identically under starvation.
    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Auto receiver-queue ramp"
    );

    // And the ramp is the correct bounded-doubling trajectory on both engines:
    // seed at the floor, then 200, 400, 800, 1600 across four ticks, each
    // doubling emitting an incremental flow whose magnitude equals the doubled
    // target (the grant was re-zeroed before each tick).
    let expected = Reaction {
        seeded_target: MIN,
        targets_per_tick: vec![200, 400, 800, 1600],
        flow_grants: vec![200, 400, 800, 1600],
    };
    assert_eq!(
        tokio_reaction, expected,
        "Auto must ramp by bounded doubling under starvation, got {tokio_reaction:?}"
    );
}
