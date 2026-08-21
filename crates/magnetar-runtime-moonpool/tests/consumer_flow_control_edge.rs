// SPDX-License-Identifier: Apache-2.0

//! Consumer flow-control edge: the permit handshake under the deterministic
//! moonpool clock.
//!
//! ## What this pins
//!
//! The consumer's broker-facing permit accounting (the sans-io
//! [`magnetar_proto::consumer::ConsumerState`] flow-control loop) driven
//! through the moonpool engine's [`magnetar_runtime_moonpool::ConnectionShared`]
//! wrapper with synthetic [`std::time::Instant`]s — no driver task, no TCP
//! listener, no wall clock. The flow-control math lives entirely in the
//! sans-io proto layer, so the engine surface exercised here is exactly the
//! one the deterministic-simulation runtime drives in production; the
//! mirrored `magnetar-runtime-tokio` file pins the identical behaviour
//! against the tokio engine, keeping the runtime 1:1 test count (ADR-0024).
//!
//! ## Shape (all four `#[test]` functions)
//!
//! 1. Handshake at virtual `t0`, subscribe with a small `receiver_queue_size`, ack the subscribe,
//!    and force the initial flow. The broker is now granted exactly `receiver_queue_size` permits.
//! 2. The broker pushes messages **up to the granted permit** — never more.
//! 3. The consumer pops them; once consumption crosses the half-queue threshold the proto layer
//!    auto-emits a replenishment `CommandFlow`, the broker is re-granted, and `available_permits`
//!    climbs back.
//! 4. Assert the invariants: the received count equals the pushed count, a replenishment flow was
//!    queued on the wire, the granted permits never under-run (the `u32` permit counter is
//!    monotone-or-saturating, never wrapping below zero), and the per-window consumed counter
//!    resets after each flow.
//!
//! The first `#[test]` walks one full queue + one replenishment window; the
//! second pins the lower-bound edge (`receiver_queue_size = 1`, where the
//! half-threshold floors to 1 so every single pop owes a flow) so we cover
//! the `max(1)` branch in [`ConsumerState::maybe_flow`]. The third (issue #307)
//! pins the Failover-promotion re-flow: a subscribed-but-zero-permit consumer
//! promoted to active re-arms its flow so `receive()` does not starve forever.
//! The fourth pins permit conservation across accepted out-of-order chunks.

#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, SubscribeRequest, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

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
        conn.initial_flow(handle, at);
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

/// Build one chunk entry for a three-chunk logical message. Each chunk uses a
/// distinct broker entry id while the shared UUID binds them for reassembly.
fn chunk_message_frame(
    handle: magnetar_proto::ConsumerHandle,
    chunk_id: i32,
    payload: &[u8],
) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 13,
                entry_id: u64::try_from(chunk_id).expect("non-negative chunk id"),
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
        producer_name: "chunk-flow-producer".to_owned(),
        sequence_id: 7,
        publish_time: 1_700_000_000_000,
        uuid: Some("chunk-flow".to_owned()),
        num_chunks_from_msg: Some(3),
        chunk_id: Some(chunk_id),
        total_chunk_msg_size: Some(6),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode chunk frame");
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
            let msg = conn.pop_message(handle, std::time::Instant::now());
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

    // Invariant 3: permits never under-run. Issue #414 re-pointed
    // `available_permits` at the REAL decrementing balance, so the exact
    // arithmetic is now "everything granted, minus everything the broker
    // spent" — which is a strictly stronger statement than the cumulative
    // grant this used to assert, because it ties the wire grants AND the
    // dispatches together. The counter is a saturating `u32`; it never wraps
    // below zero.
    let final_permits = shared.inner.lock().consumer_available_permits(handle);
    let total_granted: u32 = RQ as u32 + replenish_grants.iter().sum::<u32>();
    let expected_balance = total_granted - pushed;
    assert_eq!(
        final_permits, expected_balance,
        "available_permits ({final_permits}) must equal every grant ({total_granted}) \
         minus every dispatch unit the broker spent ({pushed}) — no underflow, no drift",
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
        let m = conn.pop_message(handle, std::time::Instant::now());
        (m, conn.poll_transmit())
    };
    assert!(empty_pop.is_none(), "popping an empty queue yields None");
    assert!(
        leftover.is_empty(),
        "an empty-queue pop must not queue a spurious flow frame",
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        expected_balance,
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
            let msg = conn.pop_message(handle, std::time::Instant::now());
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

        // After each window the broker holds exactly what it has been granted
        // minus what it has spent. Issue #414 re-pointed `available_permits` at
        // the REAL balance, so a single-message window settles back at exactly
        // `RQ` after every push/pop pair rather than climbing with the
        // cumulative grant — the strictly stronger statement, since it pins the
        // dispatch side too.
        let permits = shared.inner.lock().consumer_available_permits(handle);
        assert_eq!(
            permits,
            RQ as u32 + total_replenished - received,
            "window {w}: permits track every grant minus every dispatch, no underflow",
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

/// Pulsar spends one broker permit per chunk entry. Accepted incomplete chunks
/// must therefore replenish flow before the logical message is reassembled;
/// the completing chunk remains tied to the eventual user-visible pop.
#[test]
fn accepted_incomplete_chunks_replenish_flow_before_reassembly() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_consumer(&shared, "persistent://public/default/chunk-flow", 2, t0);

    let mut grants = Vec::new();
    for (chunk_id, body) in [(0, b"aa".as_slice()), (2, b"cc"), (1, b"bb")] {
        let frame = chunk_message_frame(handle, chunk_id, body);
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver chunk");
        grants.push(drain_flow_permits(&mut conn.poll_transmit()));
    }
    assert_eq!(grants, vec![vec![1], vec![1], vec![]]);
    assert_eq!(shared.inner.lock().consumer_queue_len(handle), 1);

    let (message, mut out) = {
        let mut conn = shared.inner.lock();
        (
            conn.pop_message(handle, std::time::Instant::now())
                .expect("reassembled message"),
            conn.poll_transmit(),
        )
    };
    assert_eq!(message.payload.as_ref(), b"aabbcc");
    assert_eq!(drain_flow_permits(&mut out), vec![1]);
}

/// Subscribe a `Failover` consumer and ack the subscribe so it is registered
/// and `Ready`, but **do not** force the initial flow — the consumer sits at
/// `available_permits == 0`, exactly the state a standby holds (or that a
/// re-attached consumer holds between the gated re-subscribe and the broker's
/// flow). Returns the handle.
fn open_failover_standby(
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
        subscription: "magnetar-test-failover".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        receiver_queue_size,
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(req);
        (handle, request_id)
    };

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
        // Drain the subscribe frame; NO initial flow is forced.
        let _ = conn.poll_transmit();
    }
    handle
}

/// Encode a `CommandActiveConsumerChange` frame for `handle`.
fn active_consumer_change_frame(
    handle: magnetar_proto::ConsumerHandle,
    is_active: bool,
) -> BytesMut {
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

/// Failover promotion re-arms flow (issue #307). A subscribed Failover consumer
/// sitting at `available_permits == 0` (standby, or re-attached pre-flow) gets a
/// `CommandActiveConsumerChange { is_active: true }`. The proto layer must
/// re-arm the initial flow — granting `receiver_queue_size` permits and queuing
/// a `CommandFlow` — so a `receive()` against a non-empty broker backlog is not
/// starved forever. A redundant promotion (permits already outstanding) must
/// NOT double-flow.
#[test]
fn failover_promotion_rearms_flow_so_receive_does_not_starve() {
    const RQ: usize = 8;
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_failover_standby(&shared, "persistent://public/default/failover", RQ, t0);

    // Standby: zero broker-side permits. Without the #307 fix, a promoted
    // consumer would sit here forever.
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        0,
        "a standby Failover consumer holds zero permits until promotion re-arms flow",
    );

    // Promotion to active.
    let promote = active_consumer_change_frame(handle, true);
    let grants = {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &promote).expect("active-change");
        let mut out = conn.poll_transmit();
        drain_flow_permits(&mut out)
    };

    // Flow re-armed: permits back to the receiver-queue size and exactly one
    // grant of `RQ` permits went out on the wire.
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "promotion to active must re-arm the initial flow",
    );
    assert_eq!(
        grants,
        vec![RQ as u32],
        "promotion emits exactly one CommandFlow granting receiver_queue_size permits",
    );

    // A redundant promotion (permits already outstanding) must not double-flow.
    let promote_again = active_consumer_change_frame(handle, true);
    let regrants = {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &promote_again)
            .expect("active-change");
        let mut out = conn.poll_transmit();
        drain_flow_permits(&mut out)
    };
    assert!(
        regrants.is_empty(),
        "a consumer that already holds permits must not be re-flowed on a redundant promotion",
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        RQ as u32,
        "permits unchanged by the redundant promotion — no double-flow",
    );
}
