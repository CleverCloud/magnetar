// SPDX-License-Identifier: Apache-2.0

//! Consumer flow-control edge: the tokio mirror of
//! `magnetar-runtime-moonpool/tests/consumer_flow_control_edge.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): two `#[test]` functions here mirror the
//! moonpool file's two.
//!
//! ## What this pins
//!
//! The consumer's broker-facing permit accounting (the sans-io
//! [`magnetar_proto::consumer::ConsumerState`] flow-control loop) driven
//! through the tokio engine's [`magnetar_runtime_tokio::ConnectionShared`]
//! wrapper with synthetic [`std::time::Instant`]s — no driver task, no TCP
//! listener. The flow-control math lives entirely in the sans-io proto layer,
//! so this exercises the identical engine surface the production tokio
//! runtime drives; the moonpool sibling pins the same behaviour under the
//! deterministic-simulation engine.
//!
//! ## Shape (both `#[test]` functions)
//!
//! 1. Handshake at `t0`, subscribe with a small `receiver_queue_size`, ack the subscribe, and force
//!    the initial flow — the broker is granted exactly `receiver_queue_size` permits.
//! 2. The broker pushes messages **up to the granted permit** — never more.
//! 3. The consumer pops them; once consumption crosses the half-queue threshold the proto layer
//!    auto-emits a replenishment `CommandFlow`, the broker is re-granted, and `available_permits`
//!    climbs back.
//! 4. Assert: received count equals pushed count, a replenishment flow fired, granted permits never
//!    under-run, and the per-window consumed counter resets after each flow.
//!
//! The first `#[test]` walks one full queue + one replenishment window; the
//! second pins the `receiver_queue_size = 1` lower-bound edge (where the
//! half-threshold floors to 1 so every pop owes a flow) covering the `max(1)`
//! branch in [`ConsumerState::maybe_flow`].

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, SubscribeRequest, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

/// Drive the handshake + subscribe + initial-flow round-trip and return the
/// consumer handle, fully past the open with the broker granted
/// `receiver_queue_size` permits.
fn open_consumer(
    shared: &ConnectionShared,
    topic: &str,
    receiver_queue_size: usize,
    at: Instant,
) -> magnetar_proto::ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        let connected = handshake_response_bytes();
        conn.handle_bytes(at, &connected).expect("Connected");
        let _ = conn.poll_event();
    }

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-flow".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size,
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(req);
        (handle, request_id)
    };

    // Ack the subscribe so the consumer is `Ready`.
    {
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: subscribe_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &success).expect("encode CommandSuccess");
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &buf).expect("Success");
        let _ = conn.poll_event();
    }

    // Force the initial flow: the broker is granted `receiver_queue_size`
    // permits. Drain the outbound so later wire assertions see flow frames in
    // isolation.
    {
        let mut conn = shared.inner.lock();
        conn.initial_flow(handle);
        let _ = conn.poll_transmit();
    }
    handle
}

/// Build a synthetic broker `CommandMessage` + payload addressed to `handle`,
/// at ledger/entry `(ledger_id, entry_id)`. Each call is one distinct entry —
/// i.e. one permit the broker spends to push it.
fn message_frame(
    handle: magnetar_proto::ConsumerHandle,
    ledger_id: u64,
    entry_id: u64,
    payload: &[u8],
) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id,
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
        producer_name: "magnetar-test-prod".to_owned(),
        sequence_id: entry_id,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode message frame");
    frame
}

/// Synthetic `CommandConnected` matching the production handshake shape.
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

/// Decode every `CommandFlow` queued on the outbound buffer and return the
/// permit grants in order. Non-flow frames are ignored (there should be none
/// here, but we stay robust to incidental keepalive traffic).
fn drain_flow_permits(out: &mut Bytes) -> Vec<u32> {
    let mut grants = Vec::new();
    while !out.is_empty() {
        let frame = decode_one(out).expect("decode outbound frame");
        if frame.command.r#type == pb::base_command::Type::Flow as i32 {
            let flow = frame.command.flow.expect("flow body present");
            grants.push(flow.message_permits);
        }
    }
    grants
}

/// Full-window walk: grant `RQ = 8` permits, push exactly 8 entries (up to the
/// permit), pop all 8, and assert the replenishment flow fires, the received
/// count equals the pushed count, and the permit counter never under-runs.
#[test]
fn flow_control_replenishes_without_permit_underrun() {
    const RQ: usize = 8;
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_consumer(&shared, "persistent://public/default/flow", RQ, t0);

    // The initial flow granted exactly `RQ` permits to the broker.
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "initial flow must grant exactly receiver_queue_size permits",
    );

    // Broker pushes up to the permit — exactly `RQ` entries, never more. Each
    // arrival surfaces one `Message` event; count them to prove the push.
    let mut pushed = 0_u32;
    for i in 0..RQ {
        let frame = message_frame(handle, 9, i as u64, format!("m{i}").as_bytes());
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
        while let Some(evt) = conn.poll_event() {
            if matches!(evt, ConnectionEvent::Message { .. }) {
                pushed += 1;
            }
        }
    }
    assert_eq!(
        pushed, RQ as u32,
        "broker pushed up to the granted permit ({RQ}); every push must surface a Message",
    );
    assert_eq!(
        shared.inner.lock().consumer_queue_len(handle),
        RQ,
        "all pushed messages sit in the receiver queue awaiting pop",
    );

    // Pop every message. Crossing the half-queue threshold (RQ/2 = 4) triggers
    // the proto layer's `maybe_flow`, which queues a replenishment `CommandFlow`
    // and bumps `available_permits`. Drain the wire after each pop and record
    // the grants.
    let mut received = 0_u32;
    let mut replenish_grants: Vec<u32> = Vec::new();
    for _ in 0..RQ {
        let (msg, mut out) = {
            let mut conn = shared.inner.lock();
            let msg = conn.pop_message(handle);
            (msg, conn.poll_transmit())
        };
        assert!(msg.is_some(), "every queued message must pop");
        received += 1;
        replenish_grants.extend(drain_flow_permits(&mut out));
    }

    // Invariant 1: received count == pushed count. No message lost, none double
    // counted.
    assert_eq!(
        received, pushed,
        "received count ({received}) must equal pushed count ({pushed})",
    );

    // Invariant 2: at least one replenishment flow fired (consumption crossed
    // the half-queue threshold), and every grant is a positive permit batch.
    assert!(
        !replenish_grants.is_empty(),
        "draining a full receiver queue must emit at least one replenishment CommandFlow",
    );
    assert!(
        replenish_grants.iter().all(|&p| p > 0),
        "every replenishment flow must grant a positive permit batch, got {replenish_grants:?}",
    );

    // Invariant 3: permits never under-run. After replenishment the broker is
    // granted *more* than the initial window (initial RQ + the replenished
    // batches), and the counter is a monotone-or-saturating `u32` — it never
    // wraps below zero.
    let final_permits = shared.inner.lock().consumer_available_permits(handle);
    let total_granted: u32 = RQ as u32 + replenish_grants.iter().sum::<u32>();
    assert_eq!(
        final_permits, total_granted,
        "available_permits ({final_permits}) must equal the initial grant plus every \
         replenishment ({total_granted}) — no underflow, no drift",
    );
    assert!(
        final_permits >= RQ as u32,
        "after a full drain + replenishment the permit count must not fall below the \
         initial grant ({RQ}); got {final_permits}",
    );

    // Invariant 4: a further pop with an empty queue is a clean `None` and does
    // not perturb the permit counter (no spurious flow, no underflow on the
    // empty path).
    let (empty_pop, leftover) = {
        let mut conn = shared.inner.lock();
        let m = conn.pop_message(handle);
        (m, conn.poll_transmit())
    };
    assert!(empty_pop.is_none(), "popping an empty queue yields None");
    assert!(
        leftover.is_empty(),
        "an empty-queue pop must not queue a spurious flow frame",
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        total_granted,
        "the permit counter is unchanged by an empty-queue pop",
    );
}

/// Lower-bound edge: `receiver_queue_size = 1` floors the half-threshold to
/// `max(1) == 1`, so every single pop owes a fresh permit. Push one, pop one,
/// confirm exactly one replenishment of one permit fires and the count stays
/// in lockstep across several windows — the permit counter never under-runs
/// even when the window is a single message.
#[test]
fn flow_control_single_permit_window_never_underruns() {
    const RQ: usize = 1;
    const WINDOWS: u64 = 5;
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_consumer(&shared, "persistent://public/default/flow-edge", RQ, t0);

    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "initial flow grants the single-message receiver-queue permit",
    );

    let mut pushed = 0_u32;
    let mut received = 0_u32;
    let mut total_replenished = 0_u32;

    // Walk several single-message windows. Each window: broker spends its one
    // permit to push, user pops, and the half-threshold (floored to 1) makes
    // `maybe_flow` re-grant exactly one permit. The received/pushed counts must
    // stay in lockstep and permits must never under-run.
    for w in 0..WINDOWS {
        let frame = message_frame(handle, 11, w, format!("edge-{w}").as_bytes());
        let mut saw_msg = false;
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(t0 + Duration::from_millis(w), &frame)
                .expect("deliver edge message");
            while let Some(evt) = conn.poll_event() {
                if let ConnectionEvent::Message { message, .. } = evt {
                    assert_eq!(
                        message.payload,
                        Bytes::from(format!("edge-{w}").into_bytes()),
                        "payload round-trips intact for window {w}",
                    );
                    saw_msg = true;
                }
            }
        }
        assert!(
            saw_msg,
            "window {w}: the single pushed message must surface"
        );
        pushed += 1;

        let (msg, mut out) = {
            let mut conn = shared.inner.lock();
            let msg = conn.pop_message(handle);
            (msg, conn.poll_transmit())
        };
        assert!(msg.is_some(), "window {w}: the single message must pop");
        received += 1;

        let grants = drain_flow_permits(&mut out);
        assert_eq!(
            grants,
            vec![1],
            "window {w}: a single-message queue owes exactly one replenishment permit",
        );
        total_replenished += grants.iter().sum::<u32>();

        // After each window the broker is granted the initial permit plus every
        // replenishment so far — a strictly non-decreasing count, never an
        // underflow.
        let permits = shared.inner.lock().consumer_available_permits(handle);
        assert_eq!(
            permits,
            RQ as u32 + total_replenished,
            "window {w}: permits track the initial grant plus replenishments, no underflow",
        );
        assert!(
            permits >= RQ as u32,
            "window {w}: permits never fall below the initial grant",
        );
    }

    assert_eq!(
        received, pushed,
        "received count ({received}) must equal pushed count ({pushed}) across all windows",
    );
    assert_eq!(
        received, WINDOWS as u32,
        "every window delivered exactly one message",
    );
    assert_eq!(
        total_replenished, WINDOWS as u32,
        "each of the {WINDOWS} single-message windows replenished exactly one permit",
    );
}
