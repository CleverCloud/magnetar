// SPDX-License-Identifier: Apache-2.0

// The scenario is one readable sequence of assertions over one event stream, each naming the
// behaviour it pins. Splitting them into helpers would hide which assertion is the wedge and
// which is the invariant. We accept the line count.
#![allow(clippy::too_many_lines)]

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for issue #436 — a `Shared`
//! subscription on a topic carrying BATCHED entries, with `ack_timeout` armed, whose flow
//! control wedges once the unacked-message tracker fires.
//!
//! ## What the reporter saw
//!
//! Twelve `Shared` consumers over a twelve-partition topic whose entries pack 1024 messages
//! each. Within the hour every consumer sits at `availablePermits` `0` or negative,
//! `msgRateOut` is `0`, `msgRateRedeliver` is `0`, the subscription is NOT blocked on unacked
//! messages, and the application's own probe reports it acking essentially everything it
//! receives. Raising `ack_timeout` past the run length — nothing else changed — makes the
//! wedge disappear. One batched entry had additionally pinned the subscription's mark-delete
//! position for days, with the first individually-deleted range starting exactly one entry
//! past it.
//!
//! ## What the harness now models
//!
//! The scripted broker gained the four behaviours that make the scenario expressible at all
//! (see `broker.rs`): batched ledger entries, per-MESSAGE permits charged as
//! `batch_size - ackedCount` with a forced whole-entry dispatch that may drive the balance
//! negative, a per-entry unacked bitset that a `CommandAck.ack_set` AND-accumulates into and
//! that gates the mark-delete advance, and an ack-timeout redelivery that actually reaches the
//! `Shared` dispatcher — before this it filled a per-consumer queue the Shared dispatch walk
//! never reads, so an ack-timeout redelivery on a `Shared` subscription was a silent no-op.
//!
//! ## The desired behaviour this asserts
//!
//! A partially-acked batched entry is re-dispatched as ONE entry carrying
//! `CommandMessage.ack_set`, the bitset of positions that are still outstanding. The consumer
//! must deliver ONLY those positions. Today's `ConsumerState::deliver` never reads that field
//! and explodes `0..num_messages_in_batch` unconditionally, so the redelivery hands the
//! application `batch_size` messages of which only the outstanding ones are new — and an
//! application that does not acknowledge the same message twice therefore has nothing to
//! acknowledge, never reaches the positions that genuinely still owe an ack, and spends its
//! receive capacity re-consuming its own history. That is the wedge: the entry never
//! completes, the mark-delete position never moves past it, and the freshly published backlog
//! behind it never gets delivered.
//!
//! Three assertions carry that claim and all three are RED against the client as it stands:
//! the first message of the redelivery is position 0 rather than 6, the entry published after
//! the redelivery reaches the application two messages out of eight, and the mark-delete log
//! is empty because the first entry never completes. The permit-balance assertions at the end
//! are an invariant pin rather than part of the wedge proof — they already hold today, and
//! they are here so the fix cannot trade one accounting fault for another.
//!
//! The proto/runtime half of the claim is pinned by the sibling
//! `crates/magnetar-runtime-{tokio,moonpool}/tests/batch_redelivery_flow_wedge.rs`; this file
//! is the end-to-end statement of it, across both engines, over a real socket.
//!
//! ## Timing
//!
//! Both differential legs run on the real tokio clock, so [`ACK_TIMEOUT`] is wall-clock time
//! this test actually waits. **If this test ever flakes, that constant is the first suspect**:
//! everything after the redelivery must complete well inside one further `ACK_TIMEOUT` window,
//! or a slow leg picks up a second tracker firing the other leg never saw and the two event
//! streams diverge on timing rather than on behaviour. Raise it (it costs one wall-clock
//! window per leg) rather than trimming the tail of the trace, which is what the assertions
//! read.

use std::collections::BTreeSet;
use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;

/// Messages packed into each published broker entry. Small enough to read in a failure
/// message, wide enough that "one entry" and "one permit" are visibly different things.
const BATCH_SIZE: usize = 8;

/// Receiver queue for the single Shared consumer. `maybe_flow`'s threshold is
/// `max(RQ / 2, 1)` = 4, so the consumer re-arms twice while draining one entry.
const RQ: usize = 8;

/// Positions of the first entry the application acknowledges before the tracker fires:
/// `0..ACKED_PREFIX`. The rest are the tail whose acks are still outstanding — issue #436's
/// `acks_sent` lagging `msgs_received` by exactly the unacked count.
const ACKED_PREFIX: usize = 6;

/// The two positions of the first entry that are still unacked when the tracker fires.
const REMAINING: [i32; 2] = [6, 7];

/// Ack-timeout window. Real wall-clock time on both legs — see the module doc's Timing note.
const ACK_TIMEOUT: Duration = Duration::from_millis(600);

/// Receive budget. Comfortably longer than [`ACK_TIMEOUT`], so the receive that waits out the
/// tracker resolves on the redelivery rather than on impatience.
const RECV_TIMEOUT: Duration = Duration::from_secs(3);

const TOPIC: &str = "persistent://public/default/batch-redelivery-436";
const SUBSCRIPTION: &str = "sub-batch-redelivery-436";
const CONSUMER: &str = "app";

/// Payloads of the entry published first — the one that ends up partially acked.
fn first_batch() -> Vec<Vec<u8>> {
    (0..BATCH_SIZE)
        .map(|i| format!("first-{i}").into_bytes())
        .collect()
}

/// Payloads of the entry published AFTER the redelivery cycle. Nothing about it is unusual;
/// whether it reaches the application at all is the "does consumption resume" assertion.
fn second_batch() -> Vec<Vec<u8>> {
    (0..BATCH_SIZE)
        .map(|i| format!("second-{i}").into_bytes())
        .collect()
}

fn recv() -> Op {
    Op::RecvShared {
        name: CONSUMER.to_owned(),
        timeout: RECV_TIMEOUT,
    }
}

fn ack_last() -> Op {
    Op::AckLastReceivedShared {
        name: CONSUMER.to_owned(),
    }
}

/// Batch position of a `Received` event, or `None` for any other event shape.
fn received_batch_index(event: &Event) -> Option<i32> {
    match event {
        Event::Received { message_id, .. } => Some(message_id.batch_index),
        _ => None,
    }
}

/// Payload of a `Received` event as a `String`, or `None` for any other event shape.
fn received_payload(event: &Event) -> Option<String> {
    match event {
        Event::Received { payload, .. } => Some(String::from_utf8_lossy(payload).into_owned()),
        _ => None,
    }
}

/// Index of the op that waits out the ack-timeout and takes the FIRST message of the
/// redelivery. Everything before it is the steady-state drain of the first entry.
const FIRST_REDELIVERED_RECV: usize = 17;
/// The op acknowledging that first redelivered message.
const FIRST_REDELIVERED_ACK: usize = 18;
/// Index of the first op that receives from the entry published after the redelivery.
const POST_RECOVERY_RECV_START: usize = 22;

/// The issue #436 trace.
///
/// One batched entry, drained and acked bar its last two positions, then the ack-timeout
/// redelivery, then a second batched entry published behind it. The application acknowledges
/// what it is handed and never acknowledges the same message twice
/// ([`Op::AckLastReceivedShared`]) — the dedup is not a harness convenience, it is the
/// property that turns "the consumer re-delivered what I already acked" into "the entry never
/// completes".
fn trace() -> Trace {
    let mut ops = vec![
        // 0
        Op::SendBatch {
            payloads: first_batch(),
        },
        // 1
        Op::OpenSharedConsumer {
            name: CONSUMER.to_owned(),
            receiver_queue_size: RQ,
        },
        // 2
        recv(),
        // 3
        ack_last(),
        // 4 — the same message acknowledged twice. An application that de-duplicates sends
        // nothing the second time, and the trace pins that here so the behaviour is asserted
        // in its own right rather than only observed in the wreckage after the redelivery.
        ack_last(),
    ];
    // 5..=14 — positions 1..=5: receive, acknowledge.
    for _ in 1..ACKED_PREFIX {
        ops.push(recv());
        ops.push(ack_last());
    }
    // 15, 16 — positions 6 and 7: received, NOT acknowledged. The tracker's window opened when
    // the entry was delivered, so it is these two that expire.
    ops.push(recv());
    ops.push(recv());
    // 17..=20 — the redelivery. The broker re-dispatches the entry with `ack_set = {6, 7}`;
    // the consumer must hand the application position 6, then position 7, and the application
    // acknowledges both because it has not seen them acknowledged before.
    ops.push(recv());
    ops.push(ack_last());
    ops.push(recv());
    ops.push(ack_last());
    // 21 — fresh traffic behind the recovered entry.
    ops.push(Op::SendBatch {
        payloads: second_batch(),
    });
    // 22..=29 — exactly one receive per message of that entry. Not a generous budget on
    // purpose: a subscription whose delivery capacity is spent re-consuming acknowledged
    // history has none left for the backlog, which is `msgRateOut = 0` from the application's
    // side of the socket.
    for _ in 0..BATCH_SIZE {
        ops.push(recv());
    }
    // 30
    ops.push(Op::Close);
    Trace::new(TOPIC, SUBSCRIPTION, ops)
}

#[tokio::test(flavor = "current_thread")]
async fn batch_redelivery_flow_event_streams_agree() {
    let trace = trace();

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    broker.clear_consumer_permit_log();
    broker.clear_mark_delete_log();
    let tokio_stream = runner_tokio::run_with_ack_timeout(&pulsar_url, &trace, ACK_TIMEOUT)
        .await
        .expect("tokio runner");
    let tokio_permits = broker.consumer_permit_log_snapshot();
    let tokio_mark_deletes = broker.mark_delete_log_snapshot();

    broker.clear_frame_log();
    broker.clear_consumer_permit_log();
    broker.clear_mark_delete_log();
    let moonpool_stream = runner_moonpool::run_with_ack_timeout(&host_port, &trace, ACK_TIMEOUT)
        .await
        .expect("moonpool runner");
    let moonpool_mark_deletes = broker.mark_delete_log_snapshot();

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the issue #436 batched-entry ack-timeout \
         redelivery sequence",
    );

    let events = &tokio_stream.events;

    // Baseline: the first entry drains one position at a time and the application
    // acknowledges each exactly once. The duplicate acknowledgement at op 4 sends nothing.
    assert_eq!(
        received_batch_index(&events[2]),
        Some(0),
        "the batched entry must explode into its packed positions, got {:?}",
        events[2],
    );
    assert_eq!(events[3], Event::Acked, "the first acknowledgement is real");
    assert_eq!(
        events[4],
        Event::AckSkippedDuplicate,
        "an application does not acknowledge the same message twice, got {:?}",
        events[4],
    );
    assert_eq!(
        received_batch_index(&events[15]),
        Some(REMAINING[0]),
        "position {} must be delivered and left unacknowledged, got {:?}",
        REMAINING[0],
        events[15],
    );
    assert_eq!(
        received_batch_index(&events[16]),
        Some(REMAINING[1]),
        "position {} must be delivered and left unacknowledged, got {:?}",
        REMAINING[1],
        events[16],
    );

    // (1) The redelivery must carry only what the entry still owes.
    //
    // The broker re-dispatched the entry with `ack_set = {6, 7}` — bit set ⇒ still unacked.
    // A consumer that honours it hands the application position 6 first. Today's
    // `ConsumerState::deliver` ignores the field and re-explodes the whole entry, so the
    // application is handed position 0 again: a message it acknowledged long ago, which
    // costs it a receive and buys the subscription nothing.
    assert_eq!(
        received_batch_index(&events[FIRST_REDELIVERED_RECV]),
        Some(REMAINING[0]),
        "the ack-timeout redelivery must deliver only the positions the broker still lists as \
         unacked in CommandMessage.ack_set — position {} first — never one the application has \
         already acknowledged; got {:?}",
        REMAINING[0],
        events[FIRST_REDELIVERED_RECV],
    );
    assert_eq!(
        events[FIRST_REDELIVERED_ACK],
        Event::Acked,
        "the redelivered position must be one the application still owes an acknowledgement \
         for, so acking it reaches the broker; got {:?}",
        events[FIRST_REDELIVERED_ACK],
    );
    assert_eq!(
        received_batch_index(&events[19]),
        Some(REMAINING[1]),
        "the second redelivered position must be {}, got {:?}",
        REMAINING[1],
        events[19],
    );
    assert_eq!(
        events[20],
        Event::Acked,
        "and acking it must reach the broker too, got {:?}",
        events[20],
    );

    // (2) Consumption resumes: every message of the entry published after the recovery
    // reaches the application. This is `msgRateOut` recovering, seen from the client.
    let post_recovery: BTreeSet<String> = (POST_RECOVERY_RECV_START
        ..POST_RECOVERY_RECV_START + BATCH_SIZE)
        .filter_map(|index| received_payload(&events[index]))
        .collect();
    let expected: BTreeSet<String> = second_batch()
        .iter()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect();
    assert_eq!(
        post_recovery, expected,
        "every message published after the redelivery must reach the application; a \
         subscription still re-delivering acknowledged history has no delivery capacity left \
         for the backlog, which is the msgRateOut = 0 issue #436 reports",
    );

    // (3) The partially-acked entry completes, so the mark-delete position moves past it.
    // One entry that never completes pins the position however many later entries are
    // individually deleted around it — the "single batched entry pinning mark-delete for
    // days" half of the report.
    let furthest = |log: &[(String, usize)]| {
        log.iter()
            .filter(|(subscription, _)| subscription == SUBSCRIPTION)
            .map(|(_, position)| *position)
            .max()
            .unwrap_or(0)
    };
    assert!(
        furthest(&tokio_mark_deletes) >= 1,
        "the partially-acked entry must reach fully-acked so the mark-delete position \
         advances past it; got {tokio_mark_deletes:?}",
    );
    assert_eq!(
        furthest(&tokio_mark_deletes),
        furthest(&moonpool_mark_deletes),
        "both engines must drive the subscription's mark-delete position to the same place",
    );

    // (4) Permit accounting invariant, and NOT part of the wedge proof: measured against the
    // client as it stands today, both of these already hold. A forced whole-entry dispatch to
    // a consumer holding fewer permits than the entry is wide legitimately drives the balance
    // negative — by at most one entry's width — and the consumer's own flow brings it back.
    // They are pinned because the fix must not trade one accounting fault for another: a
    // consumer that hands the skipped positions' permits back must hand back exactly what the
    // broker spent on them, and the balance the broker ends holding is the number issue #436
    // reports at 0 or below.
    let floor = -(BATCH_SIZE as i64 - 1);
    let lowest = tokio_permits
        .iter()
        .map(|(_, balance)| *balance)
        .min()
        .expect("the trace dispatches, so the broker debited at least once");
    assert!(
        lowest >= floor,
        "a dispatch may only overdraw a consumer by less than one entry's width ({floor}), \
         got {lowest} in {tokio_permits:?}",
    );
    let final_balance = tokio_permits
        .last()
        .map(|(_, balance)| *balance)
        .expect("non-empty above");
    assert!(
        final_balance >= 0,
        "the broker's per-consumer permit balance must recover to non-negative once the \
         application has drained what it was handed, got {final_balance} in {tokio_permits:?}",
    );

    assert_eq!(
        events[30],
        Event::Closed,
        "the trace closes with the consumer detached",
    );
}

/// The scripted broker's batch-ack bookkeeping must not perturb a plain, unbatched `Shared`
/// subscription: one message per entry, one permit per dispatch, whole-entry acknowledgement,
/// and a mark-delete position that advances entry by entry.
///
/// Issue #436 widened `broker.rs`'s permit arithmetic from per-entry to per-message and gave
/// every acknowledgement a bitset to intersect. Both collapse to exactly the old behaviour at
/// `batch_size <= 1`, and this pins that collapse rather than leaving it to the other Shared
/// traces to notice by accident.
#[tokio::test(flavor = "current_thread")]
async fn unbatched_shared_delivery_is_unchanged() {
    let payloads: Vec<Vec<u8>> = (0..3u8)
        .map(|i| format!("plain-{i}").into_bytes())
        .collect();
    let mut ops: Vec<Op> = payloads
        .iter()
        .map(|payload| Op::Send {
            payload: payload.clone(),
        })
        .collect();
    ops.push(Op::OpenSharedConsumer {
        name: CONSUMER.to_owned(),
        receiver_queue_size: RQ,
    });
    for _ in &payloads {
        ops.push(recv());
        ops.push(ack_last());
    }
    ops.push(Op::Close);
    let trace = Trace::new(
        "persistent://public/default/batch-redelivery-436-plain",
        "sub-batch-redelivery-436-plain",
        ops,
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    broker.clear_mark_delete_log();
    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let mark_deletes = broker.mark_delete_log_snapshot();

    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the unbatched Shared control sequence",
    );

    let events = &tokio_stream.events;
    for (offset, payload) in payloads.iter().enumerate() {
        let index = 4 + offset * 2;
        assert_eq!(
            events[index],
            Event::Received {
                payload: payload.clone(),
                message_id: MessageId {
                    ledger_id: 1,
                    entry_id: offset as u64,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                    #[cfg(feature = "scalable-topics")]
                    segment_id: None,
                },
            },
            "an unbatched entry must still arrive with batch_index -1 and batch_size 0, \
             got {:?}",
            events[index],
        );
        assert_eq!(
            events[index + 1],
            Event::Acked,
            "and its whole-entry acknowledgement must resolve, got {:?}",
            events[index + 1],
        );
    }
    assert_eq!(
        mark_deletes
            .iter()
            .map(|(_, position)| *position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "a whole-entry acknowledgement completes its entry outright, so the mark-delete \
         position advances one entry per ack",
    );
}
