// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for the Failover
//! active-consumer-change re-flow (issue #307).
//!
//! When a Failover standby consumer (subscribed, but holding zero broker-side
//! permits) is promoted to active via `CommandActiveConsumerChange { is_active:
//! true }`, the sans-io [`magnetar_proto::Connection`] re-arms the initial flow
//! so a `receive()` against a non-empty broker backlog is not starved forever.
//!
//! The re-flow decision lives entirely in the sans-io proto layer that both
//! engines wrap behind a `parking_lot::Mutex`, so the differential claim is
//! that **the observable reaction to the promotion is identical across the two
//! engines** — same `available_permits` trajectory, same ordered `CommandFlow`
//! grants on the wire, same surfaced `ActiveConsumerChanged` event. This test
//! drives the SAME synthetic frame sequence (handshake, subscribe Failover, ack,
//! promote, redundant promote) over each engine's locked `Connection` and
//! compares the captured reaction byte-for-byte. A synthetic [`Instant`] both
//! engines pin keeps the runs comparable.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};

const RQ: usize = 8;
const CHUNK_RQ: usize = 2;

/// The observable reaction to a Failover promotion the two engines must agree
/// on: the permit count after promotion, the ordered `CommandFlow` grants that
/// went out on the wire, whether the `ActiveConsumerChanged` event surfaced, and
/// the permit count + grants after a redundant second promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reaction {
    permits_before: u32,
    permits_after_promote: u32,
    grants_on_promote: Vec<u32>,
    saw_active_event: bool,
    permits_after_redundant: u32,
    grants_on_redundant: Vec<u32>,
    /// Issue #348: the per-slot active-state surface (`ConsumerState::is_active`
    /// / `active_changes`) must agree across engines exactly like the permit
    /// trajectory above — it is fed by the SAME per-slot-locked
    /// `record_active_change` call inside the `ActiveConsumerChange` arm.
    is_active_before: Option<bool>,
    is_active_after_promote: Option<bool>,
    /// Drained via `Connection::pop_consumer_active_change` after the
    /// promotion — the transition(s) a parked `next_active_change()` future
    /// would observe.
    drained_after_promote: Vec<bool>,
    is_active_after_redundant: Option<bool>,
    drained_after_redundant: Vec<bool>,
}

/// Observable permit and delivery trace for one out-of-order chunked message.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkFlowReaction {
    grants_after_chunks: Vec<Vec<u32>>,
    queue_len_before_pop: usize,
    payload: Vec<u8>,
    grants_after_pop: Vec<u32>,
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

/// Encode a `CommandActiveConsumerChange` frame for `handle`.
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

fn chunk_message_frame(handle: ConsumerHandle, chunk_id: i32, payload: &[u8]) -> BytesMut {
    let command = pb::BaseCommand {
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
    let mut buf = BytesMut::new();
    encode_payload(&mut buf, &command, &metadata, payload).expect("encode chunk frame");
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

/// Drive handshake + subscribe-Failover + ack + promote + redundant-promote over
/// one engine's locked `Connection`, returning the captured reaction. The
/// consumer is left at zero permits before the promotion (no initial flow is
/// forced) — exactly the starved standby / re-attached-pre-flow state that
/// issue #307 fixes.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Reaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/failover-equiv".to_owned(),
        subscription: "sub-failover-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        receiver_queue_size: RQ,
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(req);

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
    conn.handle_bytes(t0, &buf).expect("Success");
    let _ = conn.poll_event();
    // Drain the subscribe frame; NO initial flow is forced — the consumer is a
    // starved standby.
    let _ = conn.poll_transmit();

    let permits_before = conn.consumer_available_permits(handle);
    // Issue #348: nothing has recorded an active-change yet.
    let is_active_before = conn.consumer_is_active(handle);

    // Promote to active.
    let promote = active_consumer_change_frame(handle, true);
    conn.handle_bytes(t0, &promote).expect("active-change");
    let mut saw_active_event = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ActiveConsumerChanged { handle: h, active } = ev {
            if h == handle && active {
                saw_active_event = true;
            }
        }
    }
    let grants_on_promote = drain_flow_grants(conn, handle);
    let permits_after_promote = conn.consumer_available_permits(handle);
    let is_active_after_promote = conn.consumer_is_active(handle);
    let mut drained_after_promote = Vec::new();
    while let Some(active) = conn.pop_consumer_active_change(handle) {
        drained_after_promote.push(active);
    }

    // Redundant promotion: permits already outstanding, must not double-flow.
    let promote_again = active_consumer_change_frame(handle, true);
    conn.handle_bytes(t0, &promote_again)
        .expect("active-change");
    while conn.poll_event().is_some() {}
    let grants_on_redundant = drain_flow_grants(conn, handle);
    let permits_after_redundant = conn.consumer_available_permits(handle);
    let is_active_after_redundant = conn.consumer_is_active(handle);
    let mut drained_after_redundant = Vec::new();
    while let Some(active) = conn.pop_consumer_active_change(handle) {
        drained_after_redundant.push(active);
    }

    Reaction {
        permits_before,
        permits_after_promote,
        grants_on_promote,
        saw_active_event,
        permits_after_redundant,
        grants_on_redundant,
        is_active_before,
        is_active_after_promote,
        drained_after_promote,
        is_active_after_redundant,
        drained_after_redundant,
    }
}

/// Drive the issue-#331 accepted-chunk trace through one engine connection.
fn lock_and_run_chunk_flow(conn: &mut Connection, t0: Instant) -> ChunkFlowReaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/chunk-flow-equiv".to_owned(),
        subscription: "sub-chunk-flow-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        receiver_queue_size: CHUNK_RQ,
        ..Default::default()
    });
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut success_frame = BytesMut::new();
    encode_command(&mut success_frame, &success).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &success_frame).expect("Success");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();

    let mut grants_after_chunks = Vec::new();
    for (chunk_id, body) in [(0, b"aa".as_slice()), (2, b"cc"), (1, b"bb")] {
        let frame = chunk_message_frame(handle, chunk_id, body);
        conn.handle_bytes(t0, &frame).expect("deliver chunk");
        grants_after_chunks.push(drain_flow_grants(conn, handle));
    }

    let queue_len_before_pop = conn.consumer_queue_len(handle);
    let message = conn
        .pop_message(handle, std::time::Instant::now())
        .expect("reassembled message");
    let payload = message.payload.to_vec();
    let grants_after_pop = drain_flow_grants(conn, handle);

    ChunkFlowReaction {
        grants_after_chunks,
        queue_len_before_pop,
        payload,
        grants_after_pop,
    }
}

#[test]
fn failover_active_reflow_event_streams_agree() {
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

    // The differential equivalence claim: both engines react identically to the
    // Failover promotion.
    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on the Failover active-change re-flow"
    );

    // And the reaction is the correct #307 behaviour on both engines.
    let expected = Reaction {
        permits_before: 0,
        permits_after_promote: RQ as u32,
        grants_on_promote: vec![RQ as u32],
        saw_active_event: true,
        permits_after_redundant: RQ as u32,
        grants_on_redundant: vec![],
        // Issue #348: `record_active_change` fires unconditionally on every
        // `ActiveConsumerChange` frame — including the redundant promotion,
        // which does NOT re-flow but DOES still record the transition.
        is_active_before: None,
        is_active_after_promote: Some(true),
        drained_after_promote: vec![true],
        is_active_after_redundant: Some(true),
        drained_after_redundant: vec![true],
    };
    assert_eq!(
        tokio_reaction, expected,
        "promotion must re-arm exactly one flow of {RQ} permits and not double-flow on a \
         redundant promotion, got {tokio_reaction:?}"
    );
}

#[test]
fn chunk_flow_replenishment_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_reaction = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run_chunk_flow(&mut conn, t0)
    };
    let moonpool_reaction = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run_chunk_flow(&mut conn, t0)
    };

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool engines diverged on accepted chunk permit replenishment"
    );
    assert_eq!(
        tokio_reaction,
        ChunkFlowReaction {
            grants_after_chunks: vec![vec![1], vec![1], vec![]],
            queue_len_before_pop: 1,
            payload: b"aabbcc".to_vec(),
            grants_after_pop: vec![1],
        },
    );
}
