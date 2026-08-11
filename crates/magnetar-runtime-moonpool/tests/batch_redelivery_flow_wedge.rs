// SPDX-License-Identifier: Apache-2.0

// Each scenario is one readable step-by-step synthetic frame sequence. Splitting them into
// sub-helpers would hide the exact ordering the test pins. We accept the line count.
#![allow(clippy::too_many_lines)]
#![allow(clippy::expect_used)]

//! Issue #436: a `Shared` consumer on a topic carrying BATCHED entries, with `ack_timeout`
//! armed, wedges its flow control within the hour — every consumer ends at zero or negative
//! `availablePermits`, `msgRateOut` drops to 0, and the permits never come back. The same
//! binary with `ack_timeout` raised past the run length never wedges. The reporter also
//! observed a single batched entry pinning the subscription's mark-delete position for days,
//! with the first individually-deleted range starting exactly one entry past it.
//!
//! ## The desired behaviour these tests assert
//!
//! The broker re-dispatches a partially-acked batched entry as ONE entry and tells the client
//! which positions are still outstanding through [`pb::CommandMessage::ack_set`] — the same
//! bitset semantics as the outbound `BatchAckEntry` (bit `i` SET ⇒ position `i` is still
//! unacked). The Java client reads it in `ConsumerImpl.receiveIndividualMessagesFromBatch`
//! and does three things magnetar does not do yet:
//!
//! 1. it SKIPS every position whose bit is clear, so the application never sees a message it
//!    already acknowledged (and the ack-timeout tracker never re-registers it);
//! 2. it charges those positions NO permit. The broker debits
//!    `MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages)` when it re-dispatches
//!    the entry (`Consumer.java:433-434`, `acknowledgmentAtBatchIndexLevelEnabled=true`), so it
//!    charges the subscription only the positions it still expects to be consumed. The client
//!    mirror must debit the same count, and `receiveIndividualMessagesFromBatch` deliberately keeps
//!    those positions out of `skippedMessages` so `increaseAvailablePermits` never fires for them —
//!    "Broker … did not decrease the permits in the broker-side. So do not acquire more permits for
//!    this message" (`ConsumerImpl.java:1798-1862`);
//! 3. it treats the delivered bitset as the authoritative starting state for the entry, so acking
//!    the remaining positions completes the entry and the resulting `CommandAck` carries NO
//!    `ack_set` — the broker is free to advance mark-delete past it.
//!
//! Today `ConsumerState::deliver`'s batch branch explodes `0..num_messages_in_batch`
//! unconditionally and `pb::CommandMessage::ack_set` is never read, so all three fail. The
//! four tests below pin them one at a time.
//!
//! ## Why this is runtime-integration territory rather than a proto unit test
//!
//! The wedge is a loop between three subsystems that only meet at the connection: batch
//! explosion (`ConsumerState::deliver`), permit accounting (`record_dispatch_unit` /
//! `maybe_flow`, which re-arms only on `pop_message`), and the ack-timeout tracker (driven by
//! `Connection::handle_timeout`). Driving them through the engine's `ConnectionShared` with
//! synthetic frames and synthetic [`Instant`]s reproduces it with no listener, no wall clock
//! and no Docker; the mirrored `magnetar-runtime-tokio` file pins the identical behaviour
//! against the production engine, keeping the runtime 1:1 test count (ADR-0024).

mod common;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, ConsumerHandle, MessageId, SubscribeRequest,
    decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Sub-messages packed into the batched broker entry under test
/// (`metadata.num_messages_in_batch`).
const BATCH_SIZE: i32 = 8;
/// Ledger of the single batched entry every test drives.
const LEDGER: u64 = 12;
/// Entry id of the single batched entry every test drives.
const ENTRY: u64 = 5;
/// Positions the application acknowledges: `0..ACKED_PREFIX`, i.e. 0..=5.
const ACKED_PREFIX: i32 = 6;
/// The two positions that stay outstanding: 6 and 7.
const REMAINING: [i32; 2] = [6, 7];
/// Broker view of the entry on re-dispatch: positions 0..=5 acknowledged (bits clear),
/// positions 6 and 7 still unacked (bits set). One `u64` word covers a batch of 8.
const REDELIVERED_ACK_SET: i64 = 0b1100_0000;
/// Receiver queue used by every test. `maybe_flow`'s threshold is `max(RQ / 2, 1)` = 4.
const RQ: usize = 8;
/// Ack-timeout window for the two tests that arm the unacked-message tracker.
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Handshake, subscribe a `Shared` consumer, ack the subscribe, and force the initial flow so
/// the broker holds exactly `RQ` permits. Drains the outbound buffer so later wire assertions
/// see only the frames the scenario produces.
fn open_batch_consumer(
    shared: &ConnectionShared,
    topic: &str,
    ack_timeout: Option<Duration>,
    at: Instant,
) -> ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        while conn.poll_event().is_some() {}
    }

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-436".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: RQ,
        ack_timeout,
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

/// Build one synthetic BATCHED broker entry on `(LEDGER, ENTRY)` carrying [`BATCH_SIZE`]
/// packed `(u32 single_size)(SingleMessageMetadata)(payload)` sub-messages — the wire shape
/// `ConsumerState::deliver` explodes.
///
/// `ack_set` is the broker's per-position view carried on [`pb::CommandMessage`] (empty on a
/// first dispatch, populated on a re-dispatch); `redelivery_count` is the broker's
/// redelivery counter for the entry.
fn batch_entry_frame(handle: ConsumerHandle, ack_set: Vec<i64>, redelivery_count: u32) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: LEDGER,
                entry_id: ENTRY,
                partition: None,
                batch_index: None,
                ack_set: Vec::new(),
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(redelivery_count),
            ack_set,
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "magnetar-test-436".to_owned(),
        sequence_id: ENTRY,
        publish_time: 0,
        num_messages_in_batch: Some(BATCH_SIZE),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for idx in 0..BATCH_SIZE {
        let payload = format!("batch-436-{idx}").into_bytes();
        let single = pb::SingleMessageMetadata {
            payload_size: payload.len() as i32,
            ..Default::default()
        };
        let single_len = prost::Message::encoded_len(&single);
        body.extend_from_slice(&(single_len as u32).to_be_bytes());
        prost::Message::encode(&single, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(&payload);
    }
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, &body).expect("encode batched entry");
    frame
}

/// Build one synthetic NON-batched broker entry: one dispatch unit, one permit. Used to carry a
/// flow window over `maybe_flow`'s refill threshold without adding batch positions to it.
fn single_message_frame(handle: ConsumerHandle, ledger: u64, entry: u64) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: ledger,
                entry_id: entry,
                partition: None,
                batch_index: None,
                ack_set: Vec::new(),
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "magnetar-test-436".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, b"single-436").expect("encode single entry");
    frame
}

/// Individually acknowledge one position of the batched entry under test.
fn ack_batch_position(conn: &mut Connection, handle: ConsumerHandle, position: i32, at: Instant) {
    let _ = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: LEDGER,
                entry_id: ENTRY,
                partition: -1,
                batch_index: position,
                batch_size: BATCH_SIZE,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        at,
    );
}

/// Decode every `CommandFlow` on the outbound buffer and return its permit grants in order.
/// Every other frame kind is skipped, so incidental traffic (acks, keepalive) cannot perturb
/// the accounting.
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

/// Decode every `CommandAck` on the outbound buffer and return, per acked message id, the
/// `ack_set` it carried. An EMPTY vector is a "full" ack — the batch is complete and the
/// broker may advance mark-delete past the entry.
fn drain_ack_sets(out: &mut Bytes) -> Vec<Vec<i64>> {
    let mut sets = Vec::new();
    while !out.is_empty() {
        let Ok(frame) = decode_one(out) else { break };
        if frame.command.r#type == pb::base_command::Type::Ack as i32 {
            if let Some(ack) = frame.command.ack {
                sets.extend(ack.message_id.into_iter().map(|id| id.ack_set));
            }
        }
    }
    sets
}

/// Decode every `CommandRedeliverUnacknowledgedMessages` on the outbound buffer and return
/// the batch positions it re-requests, sorted and de-duplicated across all such frames.
fn drain_redelivered_positions(out: &mut Bytes) -> Vec<i32> {
    let mut positions = Vec::new();
    while !out.is_empty() {
        let Ok(frame) = decode_one(out) else { break };
        if frame.command.r#type == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32 {
            if let Some(redeliver) = frame.command.redeliver_unacknowledged_messages {
                positions.extend(
                    redeliver
                        .message_ids
                        .into_iter()
                        .map(|id| id.batch_index.unwrap_or(-1)),
                );
            }
        }
    }
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// Deliver the entry once with no `ack_set` (a first dispatch), pop all [`BATCH_SIZE`]
/// sub-messages, and acknowledge positions `0..ACKED_PREFIX`. Leaves the outbound buffer
/// drained and returns the flow permits granted while draining the queue.
fn deliver_pop_and_ack_prefix(
    shared: &ConnectionShared,
    handle: ConsumerHandle,
    at: Instant,
) -> Vec<u32> {
    let frame = batch_entry_frame(handle, Vec::new(), 0);
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &frame).expect("first dispatch");
    while conn.poll_event().is_some() {}
    assert_eq!(
        conn.consumer_queue_len(handle),
        BATCH_SIZE as usize,
        "a first dispatch of the batched entry must surface every position",
    );

    let mut grants = Vec::new();
    for position in 0..BATCH_SIZE {
        let msg = conn
            .pop_message(handle, at)
            .expect("every position of a first dispatch must pop");
        assert_eq!(
            msg.message_id.batch_index, position,
            "positions pop in batch order",
        );
        grants.extend(drain_flow_permits(&mut conn.poll_transmit()));
    }
    for position in 0..ACKED_PREFIX {
        ack_batch_position(&mut conn, handle, position, at);
    }
    grants.extend(drain_flow_permits(&mut conn.poll_transmit()));
    grants
}

/// Positions the broker reports as still unacked must be the ONLY ones a re-dispatched batched
/// entry surfaces to the application — and the only ones that re-enter the ack-timeout
/// tracker.
///
/// Today the batch branch of `ConsumerState::deliver` explodes `0..num_messages_in_batch`
/// unconditionally, so all eight positions are re-queued: the application sees six duplicates
/// of messages it already acknowledged, and `classify_and_queue` re-registers all six in the
/// unacked-message tracker, which then asks the broker to redeliver them again on the next
/// sweep. That is the self-sustaining loop behind issue #436.
#[test]
fn redelivered_batch_skips_positions_cleared_in_the_delivered_ack_set() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_batch_consumer(
        &shared,
        "persistent://public/default/436-skip",
        Some(ACK_TIMEOUT),
        t0,
    );
    let _ = deliver_pop_and_ack_prefix(&shared, handle, t0);

    // The broker re-dispatches the same entry, reporting that only positions 6 and 7 are
    // still outstanding.
    let redelivery_at = t0 + Duration::from_millis(500);
    {
        let frame = batch_entry_frame(handle, vec![REDELIVERED_ACK_SET], 1);
        let mut conn = shared.inner.lock();
        conn.handle_bytes(redelivery_at, &frame)
            .expect("re-dispatch");
        while conn.poll_event().is_some() {}
    }

    assert_eq!(
        shared.inner.lock().consumer_queue_len(handle),
        REMAINING.len(),
        "a re-dispatched batched entry must queue only the positions the broker still reports \
         as unacked ({REMAINING:?}); every position whose ack_set bit is clear was already \
         acknowledged and must never reach the application a second time (issue #436)",
    );

    let popped: Vec<i32> = {
        let mut conn = shared.inner.lock();
        let mut popped = Vec::new();
        while let Some(msg) = conn.pop_message(handle, redelivery_at) {
            popped.push(msg.message_id.batch_index);
        }
        popped
    };
    assert_eq!(
        popped,
        REMAINING.to_vec(),
        "only the outstanding positions may be handed to the application on re-dispatch",
    );

    // The ack-timeout tracker must agree: one sweep past the window re-requests exactly the
    // two positions that are genuinely outstanding. Before the fix the six acked positions
    // are back in the tracker and are re-requested from the broker forever.
    let mut out = {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(redelivery_at + ACK_TIMEOUT + Duration::from_millis(500));
        conn.poll_transmit()
    };
    assert_eq!(
        drain_redelivered_positions(&mut out),
        REMAINING.to_vec(),
        "only the positions the broker reports as unacked may re-enter the ack-timeout \
         tracker; a re-registered acked position is re-requested on every sweep",
    );
}

/// A position the broker reports as already acknowledged in the delivered `ack_set` costs a
/// permit on NEITHER side.
///
/// Ground truth, apache/pulsar master (3bf3ec2) with
/// `acknowledgmentAtBatchIndexLevelEnabled=true` — the production configuration behind issue
/// #436:
///
/// - the broker debits `MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages)` as it
///   (re-)dispatches the entry (`Consumer.sendMessages`, `Consumer.java:433-434`). It charges the
///   subscription `totalMessages - ackedCount` — only the positions it still expects the client to
///   consume.
/// - the Java client therefore leaves those positions out of `skippedMessages`, so
///   `increaseAvailablePermits` never fires for them: "Broker … did not decrease the permits in the
///   broker-side. So do not acquire more permits for this message"
///   (`ConsumerImpl.receiveIndividualMessagesFromBatch`, `ConsumerImpl.java:1798-1862`). Handing
///   them back would over-credit a broker that never charged them.
///
/// So a re-dispatched eight-position entry whose `ack_set` marks six positions acked costs
/// exactly TWO permits: the client mirror debits two, queues two, and the flow ledger eventually
/// carries exactly those two units.
///
/// Today `deliver` explodes all eight positions and `classify_and_queue` calls
/// `record_dispatch_unit` for each, so the mirror debits eight against a broker that charged two.
/// Every re-dispatch drives the mirror six permits further below the broker's real
/// `availablePermits`, and the client stops replenishing long before the broker is actually out —
/// the `permits=0` / `msgRateOut=0` wedge of issue #436. The drift is unbounded, which is how a
/// consumer in that report reached `permits=-3535`.
#[test]
fn redelivered_acked_positions_cost_no_permit_on_either_side() {
    /// Ledger carrying the two non-batched entries that close the flow window.
    const SINGLE_LEDGER: u64 = 13;

    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_batch_consumer(&shared, "persistent://public/default/436-flow", None, t0);

    // `initial_flow` granted exactly `RQ`. A FIRST dispatch has no acked positions, so the broker
    // charges the whole batch and the client mirror debits the whole batch.
    let mut granted = RQ as u32;
    let mut charged = 0_u32;
    granted += deliver_pop_and_ack_prefix(&shared, handle, t0)
        .iter()
        .sum::<u32>();
    charged += BATCH_SIZE as u32;

    // Anchor: with the queue fully drained the mirror is exactly every grant minus every permit
    // the broker charged. This holds before and after the fix — it is what proves the wire
    // accounting below is complete rather than accidentally balanced.
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        granted - charged,
        "after a full window the permit mirror is every grant minus every permit charged",
    );

    // Re-dispatch. The `ack_set` marks six of eight positions acked, so the broker charges two.
    let redelivery_at = t0 + Duration::from_millis(500);
    let redispatch_grants = {
        let frame = batch_entry_frame(handle, vec![REDELIVERED_ACK_SET], 1);
        let mut conn = shared.inner.lock();
        conn.handle_bytes(redelivery_at, &frame)
            .expect("re-dispatch");
        while conn.poll_event().is_some() {}
        drain_flow_permits(&mut conn.poll_transmit())
    };
    charged += REMAINING.len() as u32;

    assert!(
        redispatch_grants.is_empty(),
        "a re-dispatch must not acquire permits for the positions the broker reports as already \
         acked: the broker charged `totalMessages - ackedCount`, so handing those back would \
         over-credit it (Consumer.java:433-434); got {redispatch_grants:?}",
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        granted - charged,
        "the permit mirror must debit only the {} positions the broker still expects to be \
         consumed, not all {BATCH_SIZE}. A mirror that debits the whole batch drifts further \
         below the broker's real availablePermits on every re-dispatch and stops replenishing \
         while the broker still holds permits (issue #436)",
        REMAINING.len(),
    );

    // The application consumes exactly what it is owed: the two positions it never acked.
    {
        let mut conn = shared.inner.lock();
        for _ in REMAINING {
            assert!(
                conn.pop_message(handle, redelivery_at).is_some(),
                "the outstanding positions must pop after a re-dispatch",
            );
            granted += drain_flow_permits(&mut conn.poll_transmit())
                .iter()
                .sum::<u32>();
        }
    }

    // Those two pops sit below `maybe_flow`'s `max(RQ / 2, 1)` refill threshold, so nothing has
    // gone back on the wire yet. Two ordinary non-batched entries carry the window over the
    // threshold: the grant that fires must cover exactly the two popped re-dispatched units plus
    // the two new ones. The six positions the broker never charged never enter the flow ledger.
    let mut window_grants = Vec::new();
    for entry in 0..2_u64 {
        let at = redelivery_at + Duration::from_millis(100 * (entry + 1));
        let frame = single_message_frame(handle, SINGLE_LEDGER, entry);
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame).expect("single entry");
        while conn.poll_event().is_some() {}
        charged += 1;
        assert!(
            conn.pop_message(handle, at).is_some(),
            "the non-batched entry must pop",
        );
        window_grants.extend(drain_flow_permits(&mut conn.poll_transmit()));
    }
    granted += window_grants.iter().sum::<u32>();

    assert_eq!(
        window_grants,
        vec![(RQ / 2).max(1) as u32],
        "the flow ledger must carry exactly the two popped re-dispatched units plus the two new \
         non-batched entries — never the six positions the broker never charged",
    );
    assert_eq!(
        shared.inner.lock().consumer_available_permits(handle),
        granted - charged,
        "permit conservation across the whole run: every grant minus every permit the broker \
         charged, a re-dispatch charging only its still-unacked positions",
    );
}

/// The `ack_set` the broker delivers with a re-dispatched batched entry is the authoritative
/// starting state for that entry's PIP-54 tracker.
///
/// A session that first sees `(LEDGER, ENTRY)` as a re-dispatch carrying "only 6 and 7 are
/// unacked" must seed `BatchAckEntry` from that bitset. Acking 6 then 7 then completes the
/// entry, so the second `CommandAck` carries NO `ack_set` and the broker may advance
/// mark-delete past it. Today `BatchAckEntry::fresh` always builds an all-unacked bitset, so
/// the six positions the broker already accounted for are re-asserted as unacked on every
/// partial ack, the entry never reaches "fully acked", and its tracker entry never clears —
/// the same unbounded-growth class as issue #326, and the "one batched entry has been pinning
/// the mark-delete position for days" symptom in issue #436.
#[test]
fn delivered_ack_set_seeds_the_batch_ack_tracker() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_batch_consumer(&shared, "persistent://public/default/436-seed", None, t0);

    // Fresh session: the first thing this consumer ever sees for the entry is a re-dispatch
    // whose ack_set reports positions 0..=5 as already acknowledged.
    {
        let frame = batch_entry_frame(handle, vec![REDELIVERED_ACK_SET], 1);
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("re-dispatch");
        while conn.poll_event().is_some() {}
        while conn.pop_message(handle, t0).is_some() {}
        let _ = conn.poll_transmit();
    }

    // Acking the first outstanding position leaves exactly one position unacked.
    let mut after_first = {
        let mut conn = shared.inner.lock();
        ack_batch_position(&mut conn, handle, REMAINING[0], t0);
        conn.poll_transmit()
    };
    assert_eq!(
        drain_ack_sets(&mut after_first),
        vec![vec![1_i64 << REMAINING[1]]],
        "the partial ack must report only the positions the broker still considers unacked — \
         the delivered ack_set is the authoritative seed, not an all-unacked bitset",
    );

    // Acking the last outstanding position completes the entry: a FULL ack, no ack_set.
    let mut after_last = {
        let mut conn = shared.inner.lock();
        ack_batch_position(&mut conn, handle, REMAINING[1], t0);
        conn.poll_transmit()
    };
    assert_eq!(
        drain_ack_sets(&mut after_last),
        vec![Vec::<i64>::new()],
        "acking the last outstanding position completes the entry, so the CommandAck must \
         carry no ack_set and let the broker advance mark-delete past it (issue #436: one \
         batched entry pinned the mark-delete position for days)",
    );
    assert_eq!(
        shared
            .inner
            .lock()
            .consumer_stats(handle)
            .expect("consumer stats")
            .pending_batch_acks,
        0,
        "a completed entry must drop out of the batch-ack tracker; an entry that can never \
         reach 'fully acked' leaks for the lifetime of the connection",
    );
}

/// An application that de-duplicates re-dispatched messages must converge: every ack-timeout
/// sweep re-requests the SAME outstanding positions, never a growing set that folds in
/// positions the broker has already accounted for.
///
/// Today the first sweep correctly re-requests 6 and 7, the broker re-dispatches the entry,
/// `deliver` explodes all eight positions, and `classify_and_queue` re-registers the six acked
/// ones in the unacked-message tracker (the first two were evicted by the sweep that just
/// fired). The second sweep therefore re-requests all eight, and every later one does the
/// same — the redelivery request never converges, and each cycle re-spends broker permits on
/// positions nothing will ever ack. Java's skip keeps the request set fixed at the genuinely
/// outstanding positions.
#[test]
fn ack_timeout_redelivery_of_a_deduplicating_app_converges() {
    /// Sweeps to walk. Three is the minimum that distinguishes "converged" from "grew once".
    const CYCLES: u32 = 4;

    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_batch_consumer(
        &shared,
        "persistent://public/default/436-converge",
        Some(ACK_TIMEOUT),
        t0,
    );
    let _ = deliver_pop_and_ack_prefix(&shared, handle, t0);

    let mut at = t0;
    let mut requested: Vec<Vec<i32>> = Vec::new();
    for cycle in 0..CYCLES {
        // The ack-timeout window elapses: the tracker asks the broker to redeliver whatever it
        // still holds.
        at += ACK_TIMEOUT + Duration::from_millis(500);
        let mut out = {
            let mut conn = shared.inner.lock();
            conn.handle_timeout(at);
            conn.poll_transmit()
        };
        let positions = drain_redelivered_positions(&mut out);
        assert_eq!(
            positions,
            REMAINING.to_vec(),
            "sweep {cycle}: the ack-timeout tracker must only ever re-request the positions \
             that are genuinely outstanding; re-requesting an already-acked position makes \
             the redelivery loop self-sustaining (issue #436)",
        );
        requested.push(positions);

        // The broker honours the request by re-dispatching the whole entry, again reporting
        // which positions remain unacked. The application de-duplicates: it pops whatever
        // arrives and acknowledges nothing it has already acknowledged.
        at += Duration::from_millis(100);
        {
            let frame = batch_entry_frame(handle, vec![REDELIVERED_ACK_SET], cycle + 1);
            let mut conn = shared.inner.lock();
            conn.handle_bytes(at, &frame).expect("re-dispatch");
            while conn.poll_event().is_some() {}
            while conn.pop_message(handle, at).is_some() {}
            let _ = conn.poll_transmit();
        }
    }

    assert!(
        requested.windows(2).all(|pair| pair[0] == pair[1]),
        "the redelivery request set must be identical on every sweep — it converged only if \
         it never grew: {requested:?}",
    );
}
