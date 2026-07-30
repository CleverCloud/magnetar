// SPDX-License-Identifier: Apache-2.0

//! `Auto` receiver-queue growth under real dispatch-driven starvation, the
//! churn-window guard that must NOT be mistaken for it (issue #349), and the
//! adjust schedule's arming bootstrap (`docs/follow-ups.md` §4). The tokio
//! mirror of `magnetar-runtime-moonpool/tests/receiver_queue_auto_growth.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): four `#[test]` functions here mirror the
//! moonpool file's four.
//!
//! ## What this pins
//!
//! Issue #349 split the consumer's permit mirror into `granted_permits` (a
//! purely additive record of every grant sent to the broker) and
//! `permit_balance` (the REAL balance, decremented once per broker dispatch
//! unit as it arrives). This file drives that split through the tokio
//! engine's public [`magnetar_runtime_tokio::ConnectionShared`] surface (no
//! driver task, no TCP listener — the same synthetic-clock pattern the
//! sibling `consumer_flow_control_edge.rs` uses):
//!
//! 1. `auto_receiver_queue_grows_under_real_dispatch_starvation` — real message deliveries (not a
//!    synthetic field write) drain the broker-side permit balance to zero across a sustained
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

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Auto, ConnectionConfig, ConsumerHandle, MessageId, RequestId, SubscribeRequest,
    decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

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
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
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
