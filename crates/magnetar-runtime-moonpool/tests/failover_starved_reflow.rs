// SPDX-License-Identifier: Apache-2.0

//! Failover promotion re-arm for a flow-STARVED consumer — issue #331 lineage.
//!
//! ## The corner this pins
//!
//! A consumer that was fed once (`granted_permits > 0`) can drain its REAL permit
//! balance (#349) to zero through dispatch units that are debited but never popped —
//! dead-lettered messages are the deterministic client-side path (`classify_and_queue`
//! debits the balance for a DLQ-routed message that no `pop_message` will ever count).
//! Once `permit_balance == 0` with too little queued to cross the `maybe_flow`
//! threshold, every recovery mechanism declines:
//!
//! - `maybe_flow` is unreachable — there is nothing left to pop;
//! - the #414 stall watchdog requires `permit_balance > 0` for candidacy;
//! - the #307 promotion re-arm gated on the ADDITIVE `granted_permits == 0` mirror, which a
//!   previously-fed consumer never satisfies again outside a churn boundary.
//!
//! The broker sits at zero permits, the client waits for dispatch, and the pair is
//! wedged forever (`availablePermits=0`, `msgRateOut=0`, no close, no reconnect) — the
//! production shape observed on a 12-partition Failover fleet where one partition's
//! elected consumer went silent while eleven drained.
//!
//! The fix routes `ConsumerState::is_flow_starved` into both `initial_flow` gates, so a
//! `CommandActiveConsumerChange { is_active: true }` re-arms exactly this consumer while
//! a healthy fed consumer (permits still in flight) keeps the #427 no-double-grant
//! contract.
//!
//! Mirrored 1:1 in `magnetar-runtime-tokio/tests/failover_starved_reflow.rs`
//! (ADR-0024 `check-runtime-test-parity`).

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

mod common;

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, SubscribeRequest, decode_one, encode_command, encode_payload,
    pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Receiver queue for every test. `maybe_flow`'s threshold is `max(RQ / 2, 1)` = 4.
const RQ: usize = 8;
/// `SubscribeRequest::max_redeliver_count`: a frame whose `redelivery_count` exceeds
/// this routes to the DLQ pending list instead of the queue.
const MAX_REDELIVER: u32 = 1;
/// Redelivery counter stamped on the synthetic frames — strictly greater than
/// [`MAX_REDELIVER`], so every one of them dead-letters.
const OVER_REDELIVERED: u32 = 2;

/// Handshake, subscribe a `Failover` consumer with a DLQ threshold, ack the subscribe,
/// and force the initial flow so the broker holds exactly [`RQ`] permits. Drains the
/// outbound buffer so later wire assertions see only what the scenario produces.
fn open_failover_consumer(shared: &ConnectionShared, topic: &str, at: Instant) -> ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        while conn.poll_event().is_some() {}
    }

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-starved-reflow".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        receiver_queue_size: RQ,
        max_redeliver_count: MAX_REDELIVER,
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        (conn.subscribe(req), request_id)
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
        while conn.poll_event().is_some() {}
        conn.initial_flow(handle, at);
        let _ = conn.poll_transmit();
    }
    handle
}

/// One synthetic broker `CommandMessage` + payload addressed to `handle`, with an
/// explicit `redelivery_count` so a test can push it over the DLQ threshold.
fn message_frame(
    handle: ConsumerHandle,
    ledger_id: u64,
    entry_id: u64,
    redelivery_count: u32,
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
            redelivery_count: Some(redelivery_count),
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
    encode_payload(&mut frame, &msg_cmd, &metadata, b"starved").expect("encode message frame");
    frame
}

/// A broker `CommandActiveConsumerChange` frame for `handle`.
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
    encode_command(&mut buf, &cmd).expect("encode ActiveConsumerChange");
    buf
}

/// Decode every `CommandFlow` on the outbound buffer and return the granted permits.
fn drain_flow_permits(out: &mut Bytes) -> Vec<u32> {
    let mut grants = Vec::new();
    while !out.is_empty() {
        let Ok(frame) = decode_one(out) else { break };
        if frame.command.r#type == pb::base_command::Type::Flow as i32 {
            if let Some(flow) = frame.command.flow {
                grants.push(flow.message_permits);
            }
        }
    }
    grants
}

/// Drive the consumer into the starved state: the broker dispatches the full granted
/// window as over-redelivered frames, every one of which dead-letters — debiting the
/// real balance to zero while `consumed_since_flow` stays at zero (nothing is ever
/// queued, so nothing is ever popped).
fn starve(shared: &ConnectionShared, handle: ConsumerHandle, at: Instant) {
    let mut conn = shared.inner.lock();
    for entry in 0..RQ as u64 {
        let frame = message_frame(handle, 7, entry, OVER_REDELIVERED);
        conn.handle_bytes(at, &frame).expect("Message frame");
    }
    while conn.poll_event().is_some() {}
    // No flow may have been emitted by delivery itself: dead-lettered units never pop.
    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        Vec::<u32>::new(),
        "dead-lettered dispatch must not replenish flow on its own"
    );
}

/// A starved, previously-fed Failover consumer is re-armed by promotion: the fix's
/// `is_flow_starved` branch grants a fresh receiver-queue window where the additive
/// `granted_permits == 0` gate alone refused forever.
#[test]
fn promotion_rearms_a_starved_previously_fed_consumer() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_failover_consumer(&shared, "persistent://t/ns/starved-reflow", at);

    starve(&shared, handle, at);

    let mut conn = shared.inner.lock();
    // Demotion then promotion — the rolling-election shape a Failover fleet produces.
    conn.handle_bytes(at, &active_consumer_change_frame(handle, false))
        .expect("ACC false");
    let _ = conn.poll_transmit();
    conn.handle_bytes(at, &active_consumer_change_frame(handle, true))
        .expect("ACC true");
    while conn.poll_event().is_some() {}

    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        vec![RQ as u32],
        "promotion must re-arm a starved consumer with a fresh receiver-queue grant"
    );
}

/// The #427 no-double-grant contract survives the fix: a fed consumer with permits
/// still in flight (`permit_balance > 0`) gets NO flow on promotion.
#[test]
fn promotion_grants_nothing_to_a_fed_unstarved_consumer() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_failover_consumer(&shared, "persistent://t/ns/fed-no-regrant", at);

    {
        // Two ordinary deliveries: balance RQ -> RQ-2, both queued and poppable, so the
        // consumer is neither starved nor at a churn boundary.
        let mut conn = shared.inner.lock();
        for entry in 0..2u64 {
            let frame = message_frame(handle, 9, entry, 0);
            conn.handle_bytes(at, &frame).expect("Message frame");
        }
        while conn.poll_event().is_some() {}
        let _ = conn.poll_transmit();
    }

    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &active_consumer_change_frame(handle, true))
        .expect("ACC true");
    while conn.poll_event().is_some() {}

    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        Vec::<u32>::new(),
        "a fed, unstarved consumer must not be double-granted on promotion (#427)"
    );
}
