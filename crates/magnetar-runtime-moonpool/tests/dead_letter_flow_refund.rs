// SPDX-License-Identifier: Apache-2.0

//! Issue #437: a dead-lettered dispatch unit returns its flow permit at routing time.
//!
//! ## The corner this pins
//!
//! The broker charges one permit per dispatch unit — `ackedCount - totalMessages` in
//! `Consumer#sendMessages` — with a dead-letter-bound unit inside `totalMessages`. The
//! client mirrors that debit in `ConsumerState::record_dispatch_unit`, so
//! `permit_balance` falls for a dead-lettered entry exactly as it does for a queued one.
//!
//! What the client did NOT do was hand the permit back. `consumed_since_flow` — the
//! counter `maybe_flow` compares against the half-queue threshold — only ever moved when
//! a unit was popped, buffered as an incomplete chunk, or filtered as a PIP-33 marker. A
//! dead-lettered unit is never queued, so it is never popped, and no later path
//! compensated: `drain_dead_letter` is a `mem::take`, `ack` does not touch the ledger,
//! and the runtimes' `republish_dead_letters_with_properties` is drain, send, ack.
//!
//! So each dead-lettered unit was one permit the broker had spent that the client never
//! re-granted. A receiver queue's worth of poison drove `permit_balance` to zero with
//! `consumed_since_flow` still at zero: `maybe_flow` unreachable, the issue #414 stall
//! watchdog blind (it requires `permit_balance > 0`), and the issue #307 promotion re-arm
//! reachable only on a Failover election — so a `Shared` consumer had no exit at all.
//! `availablePermits=0`, `msgRateOut=0`, no error, no reconnect.
//!
//! Java refunds at routing time in both the single and the batched path:
//! `ConsumerImpl.messageReceived` calls `increaseAvailablePermits(cnx)` right after it
//! decides to skip the message, and `receiveIndividualMessagesFromBatch` accumulates
//! `skippedMessages` and calls `increaseAvailablePermits(cnx, skippedMessages)` once per
//! entry. The fix is the same rule: a unit the broker charged is refunded the moment the
//! client decides it will never be popped.
//!
//! ## What this asserts
//!
//! Over the real wire path, on the engine's own `Connection`: the inbound frame that
//! pushes the refund across the half-queue threshold carries the `CommandFlow` out, the
//! balance recovers to the full window, a consumer fed nothing but poison never reaches
//! `is_flow_starved`, and draining plus acking those dead letters credits nothing a
//! second time.
//!
//! Mirrored 1:1 in the sibling engine's `tests/dead_letter_flow_refund.rs`
//! (ADR-0024 `check-runtime-test-parity`).

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

mod common;

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    AckRequest, ConnectionConfig, ConsumerHandle, SubscribeRequest, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Receiver queue for every test. `maybe_flow`'s threshold is `max(RQ / 2, 1)` = 4.
const RQ: usize = 8;
/// `SubscribeRequest::max_redeliver_count`: a frame whose `redelivery_count` exceeds
/// this routes to the dead-letter pending list instead of the queue.
const MAX_REDELIVER: u32 = 1;
/// Redelivery counter stamped on the poison frames — strictly greater than
/// [`MAX_REDELIVER`], so every one of them dead-letters.
const OVER_REDELIVERED: u32 = 2;
/// Ledger the synthetic frames are addressed to.
const LEDGER: u64 = 7;

/// Handshake, subscribe a `Shared` consumer with a dead-letter threshold, ack the
/// subscribe, and force the initial flow so the broker holds exactly [`RQ`] permits.
/// Drains the outbound buffer so later wire assertions see only what the scenario
/// produces.
fn open_dlq_consumer(shared: &ConnectionShared, topic: &str, at: Instant) -> ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        while conn.poll_event().is_some() {}
    }

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-dlq-flow-refund".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
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
/// explicit `redelivery_count` so a test can push it over the dead-letter threshold.
fn message_frame(handle: ConsumerHandle, entry_id: u64, redelivery_count: u32) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: LEDGER,
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
    encode_payload(&mut frame, &msg_cmd, &metadata, b"poison").expect("encode message frame");
    frame
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

/// A whole receiver queue of dead-lettered dispatch units re-grants its own width, half a
/// window at a time, on the very frames that cross the threshold — and leaves the
/// consumer holding a full window rather than starved at zero.
#[test]
fn dead_lettered_dispatch_refills_flow_on_the_same_frame() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_dlq_consumer(&shared, "persistent://t/ns/dlq-flow-refund", at);

    let mut conn = shared.inner.lock();
    for entry in 0..RQ as u64 {
        let frame = message_frame(handle, entry, OVER_REDELIVERED);
        conn.handle_bytes(at, &frame).expect("Message frame");
    }
    while conn.poll_event().is_some() {}

    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        vec![RQ as u32 / 2, RQ as u32 / 2],
        "a unit the broker charged must be refunded when the client decides it will never \
         be popped — Java refunds the same unit inside `messageReceived`"
    );
    assert_eq!(
        conn.consumer_available_permits(handle),
        RQ as u32,
        "the real balance must be back to the full window, not parked at zero"
    );
    assert!(
        !conn
            .consumer(handle)
            .expect("consumer slot")
            .state
            .lock()
            .is_flow_starved(),
        "permits alone must no longer wedge a poison-fed Shared consumer"
    );
    assert_eq!(
        conn.drain_dead_letter(handle).len(),
        RQ,
        "and every refunded unit is still buffered for the application to republish"
    );
}

/// The refund happens exactly once per unit. Draining the dead letters and acking them —
/// what `republish_dead_letters` does after it publishes — must credit nothing further,
/// or the client would over-grant and the broker's `availablePermits` would run ahead of
/// the window the consumer actually asked for (the #427 double-grant class).
#[test]
fn draining_and_acking_dead_letters_emits_no_second_flow() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_dlq_consumer(&shared, "persistent://t/ns/dlq-no-double-credit", at);

    let mut conn = shared.inner.lock();
    for entry in 0..RQ as u64 {
        let frame = message_frame(handle, entry, OVER_REDELIVERED);
        conn.handle_bytes(at, &frame).expect("Message frame");
    }
    while conn.poll_event().is_some() {}
    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        vec![RQ as u32 / 2, RQ as u32 / 2],
        "the routing-time refunds land first"
    );

    let drained = conn.drain_dead_letter(handle);
    assert_eq!(drained.len(), RQ, "the application takes the whole buffer");
    for message in &drained {
        let _ = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![message.message_id],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            at,
        );
    }

    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        Vec::<u32>::new(),
        "draining and acking a dead letter must not credit the flow ledger a second time"
    );
    assert_eq!(
        conn.consumer_available_permits(handle),
        RQ as u32,
        "the window is what the consumer granted itself, no more"
    );
}

/// One flow ledger, several refund sites: a stream mixing popped and dead-lettered units
/// crosses the half-queue threshold on their sum and grants exactly once.
#[test]
fn mixed_popped_and_dead_lettered_units_cross_the_threshold_once() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_dlq_consumer(&shared, "persistent://t/ns/dlq-mixed-ledger", at);

    let mut conn = shared.inner.lock();
    // Two well-behaved units, delivered and popped by the application.
    for entry in 0..2u64 {
        let frame = message_frame(handle, entry, 0);
        conn.handle_bytes(at, &frame).expect("Message frame");
    }
    while conn.poll_event().is_some() {}
    for _ in 0..2 {
        conn.pop_message(handle, at).expect("queued unit");
    }
    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        Vec::<u32>::new(),
        "two of four is short of the threshold"
    );

    // Two poison units, dead-lettered on arrival. Their refunds complete the half-queue.
    for entry in 2..4u64 {
        let frame = message_frame(handle, entry, OVER_REDELIVERED);
        conn.handle_bytes(at, &frame).expect("Message frame");
    }
    while conn.poll_event().is_some() {}

    assert_eq!(
        drain_flow_permits(&mut conn.poll_transmit()),
        vec![RQ as u32 / 2],
        "popped and dead-lettered units share ONE ledger and grant once on their sum"
    );
    assert_eq!(
        conn.consumer_available_permits(handle),
        RQ as u32,
        "four units spent, four refunded, one grant of four"
    );
}
