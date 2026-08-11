// SPDX-License-Identifier: Apache-2.0

// The scenario is one readable sequence of assertions over one event stream, each naming the
// behaviour it pins. Splitting them into helpers would hide which assertion is the wedge and
// which is the invariant. We accept the line count.
#![allow(clippy::too_many_lines)]

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for issue #437 — a `Shared`
//! subscription with a dead-letter threshold whose flow control wedges the moment an entry
//! crosses that threshold.
//!
//! ## What the reporter saw
//!
//! A `Shared` subscription on a poison-heavy topic stops consuming. The broker reports
//! `availablePermits=0` and `msgRateOut=0` for the consumer, the client reports no error and
//! never reconnects, and `Consumer::available_permits()` reads `0`. Nothing recovers it short
//! of a re-subscribe: the issue #414 stall watchdog requires `permit_balance > 0` to consider
//! a consumer a candidate, and the issue #307 promotion re-arm only fires on a `Failover`
//! election a `Shared` subscription never has.
//!
//! ## The arithmetic
//!
//! The broker charges one permit per dispatch unit, with a dead-letter-bound unit inside the
//! `ackedCount - totalMessages` debit of `Consumer#sendMessages`. The client mirrored that
//! debit (`record_dispatch_unit`) but never credited the flow ledger back: `consumed_since_flow`
//! moved only on a pop, an incomplete chunk, or a PIP-33 marker, and a dead-lettered unit is
//! never queued, so it is never popped. Net `-1` permit per dead-lettered unit, one-way, until
//! a churn boundary zeroes both mirrors.
//!
//! Java has no such gap. `ConsumerImpl.messageReceived` calls `increaseAvailablePermits(cnx)`
//! immediately after it decides to skip an over-redelivered message, and
//! `receiveIndividualMessagesFromBatch` accumulates `skippedMessages` and calls
//! `increaseAvailablePermits(cnx, skippedMessages)` once per entry.
//!
//! ## What the harness now models
//!
//! Three additions, mirrored byte-for-byte in both runners: `Op::OpenSharedConsumer` carries
//! `max_redeliver_count` so a Shared consumer can HAVE a dead-letter threshold at all,
//! `Op::NackShared` hands an entry back to the Shared dispatcher so the broker re-dispatches
//! it with an incremented `redelivery_count` (`Op::Nack` targets the trace's default single
//! consumer, whose scripted dispatch path stamps `redelivery_count: 0` unconditionally), and
//! `Op::DrainDeadLettersShared` reads the dead-letter buffer back. The scripted broker itself
//! needed no change: it already counts redeliveries per entry, stamps the count on
//! re-dispatch, and gates dispatch on the consumer holding a permit.
//!
//! ## The desired behaviour this asserts
//!
//! A one-permit window makes every step exact. The consumer is granted one permit, spends it
//! on the poison entry, refunds it on the pop, and re-grants. Two nacks later the third
//! dispatch arrives over the threshold and is dead-lettered instead of queued — and at that
//! moment the permit the broker spent must come back, or the broker's per-consumer balance
//! parks at zero and the sentinel published behind the poison is never dispatched.
//!
//! That sentinel receive is the RED assertion: against the client as it stands it resolves to
//! [`Event::RecvTimeout`] on BOTH engines — the streams agree, they just agree on the wedge.
//!
//! ## Timing
//!
//! Both legs run on the real tokio clock. [`RECV_TIMEOUT`] is wall-clock time a wedged leg
//! actually waits, and the trace's nacks are immediate (no `negative_ack_redelivery_delay` is
//! configured, so `Connection::negative_ack` emits `CommandRedeliverUnacknowledgedMessages`
//! on the spot rather than deferring to the nack tracker). If this test ever flakes, raise
//! [`RECV_TIMEOUT`] rather than trimming the tail of the trace.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;

/// One permit. `maybe_flow`'s threshold is `max(RQ / 2, 1)` = 1, so every refund is visible
/// as its own `CommandFlow` and the broker's balance is either 1 or 0 at every step.
const RQ: usize = 1;

/// `SubscribeRequest::max_redeliver_count`. The client dead-letters a dispatch whose
/// `redelivery_count` is STRICTLY greater than this, matching Java's
/// `redeliveryCount > deadLetterPolicy.getMaxRedeliverCount()`.
const MAX_REDELIVER: u32 = 1;

/// Receive budget. A wedged leg waits this out twice over (once per engine); a healthy leg
/// resolves immediately.
const RECV_TIMEOUT: Duration = Duration::from_secs(3);

const TOPIC: &str = "persistent://public/default/dead-letter-flow-refund-437";
const SUBSCRIPTION: &str = "sub-dead-letter-flow-refund-437";
const CONSUMER: &str = "app";

/// The entry that ends up dead-lettered.
const POISON: &[u8] = b"poison";
/// The entry published behind it. Whether it is dispatched at all is the wedge assertion.
const SENTINEL: &[u8] = b"sentinel";

/// Index of the receive that must surface [`SENTINEL`].
const SENTINEL_RECV: usize = 7;
/// Index of the dead-letter drain.
const DRAIN: usize = 8;

fn mid(ledger_id: u64, entry_id: u64) -> MessageId {
    MessageId {
        ledger_id,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    }
}

fn recv() -> Op {
    Op::RecvShared {
        name: CONSUMER.to_owned(),
        timeout: RECV_TIMEOUT,
    }
}

fn nack() -> Op {
    Op::NackShared {
        name: CONSUMER.to_owned(),
        message_id: mid(1, 0),
    }
}

/// Payload of a `Received` event as a `String`, or `None` for any other event shape.
fn received_payload(event: &Event) -> Option<String> {
    match event {
        Event::Received { payload, .. } => Some(String::from_utf8_lossy(payload).into_owned()),
        _ => None,
    }
}

/// The issue #437 trace.
///
/// One poison entry, received and nacked twice so its third dispatch crosses the dead-letter
/// threshold, then one sentinel entry published behind it. With a one-permit window the
/// sentinel is dispatchable if and only if the dead-lettered unit returned its permit.
fn trace() -> Trace {
    Trace::new(
        TOPIC,
        SUBSCRIPTION,
        vec![
            // 0 — the poison entry, published before the consumer attaches.
            Op::Send {
                payload: POISON.to_vec(),
            },
            // 1 — one permit, one dead-letter redelivery allowed.
            Op::OpenSharedConsumer {
                name: CONSUMER.to_owned(),
                receiver_queue_size: RQ,
                max_redeliver_count: MAX_REDELIVER,
            },
            // 2, 3 — first dispatch (`redelivery_count` 0): queued, popped, nacked.
            recv(),
            nack(),
            // 4, 5 — second dispatch (`redelivery_count` 1): still at the threshold, so
            // still queued, popped, nacked.
            recv(),
            nack(),
            // 6 — the third dispatch (`redelivery_count` 2) crosses the threshold and is
            // dead-lettered rather than queued. Fresh traffic goes out behind it.
            Op::Send {
                payload: SENTINEL.to_vec(),
            },
            // 7 — the wedge. The broker spent the window's one permit on the dead-lettered
            // dispatch; unless the client handed it back, there is nothing left to dispatch
            // the sentinel with and this times out.
            recv(),
            // 8 — and the dead-lettered entry is where it belongs: the buffer, not the queue.
            Op::DrainDeadLettersShared {
                name: CONSUMER.to_owned(),
            },
            // 9
            Op::Close,
        ],
    )
}

#[tokio::test(flavor = "current_thread")]
async fn dead_letter_flow_refund_event_streams_agree() {
    let trace = trace();

    // One broker per leg. The scripted broker's per-entry `redelivery_counts` and its
    // per-subscription cursor both persist for the life of the instance, so a shared
    // instance would hand the second leg a dispatcher that has already advanced past the
    // poison entry and already counted its redeliveries.
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_stream = runner_tokio::run(&broker_t.pulsar_url(), &trace)
        .await
        .expect("tokio runner");
    let tokio_permits = broker_t.consumer_permit_log_snapshot();
    let tokio_grants = broker_t.flow_grant_log_snapshot();
    broker_t.shutdown().await;

    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_stream = runner_moonpool::run(&broker_m.host_port(), &trace)
        .await
        .expect("moonpool runner");
    let moonpool_permits = broker_m.consumer_permit_log_snapshot();
    let moonpool_grants = broker_m.flow_grant_log_snapshot();
    broker_m.shutdown().await;

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the issue #437 dead-letter flow-refund sequence",
    );

    let events = &tokio_stream.events;

    // Baseline: the window is one permit, and the two sub-threshold dispatches are ordinary
    // deliveries the application pops and hands back.
    assert_eq!(
        events[1],
        Event::SharedConsumerOpened { permits: RQ as u32 },
        "the consumer opens holding exactly its receiver-queue window, got {:?}",
        events[1],
    );
    assert_eq!(
        received_payload(&events[2]).as_deref(),
        Some("poison"),
        "the first dispatch is below the threshold and reaches the application, got {:?}",
        events[2],
    );
    assert_eq!(events[3], Event::Nacked);
    assert_eq!(
        received_payload(&events[4]).as_deref(),
        Some("poison"),
        "and so is the redelivery at `redelivery_count` 1, got {:?}",
        events[4],
    );
    assert_eq!(events[5], Event::Nacked);

    // (1) The wedge. The third dispatch is dead-lettered, so the client decided on arrival
    // that it will never be popped — and a unit the broker charged must be refunded at that
    // moment, exactly as `ConsumerImpl.messageReceived` refunds the one it skips. Without
    // that refund the broker's per-consumer balance is zero, the sentinel is never
    // dispatched, and this is `Event::RecvTimeout` — issue #437's msgRateOut = 0 seen from
    // the client's side of the socket.
    assert_eq!(
        received_payload(&events[SENTINEL_RECV]).as_deref(),
        Some("sentinel"),
        "the entry published behind a dead-lettered one must still be dispatched: the \
         dead-lettered unit's permit belongs back in the flow ledger at routing time; \
         got {:?}",
        events[SENTINEL_RECV],
    );

    // (2) And it really was dead-lettered — routed to the buffer, never to the queue. A
    // consumer that simply queued it would deliver it at op 7 and pass assertion (1) for the
    // wrong reason.
    assert_eq!(
        events[DRAIN],
        Event::DeadLettersDrained { count: 1 },
        "the over-redelivered entry belongs on the dead-letter pending list, got {:?}",
        events[DRAIN],
    );

    // (3) The broker-side balance, which is the number issue #437 reports at zero. Both
    // engines must drive it to the same place, and it must not end parked at zero with a
    // backlog still unread.
    let final_balance = |log: &[(u64, i64)]| log.last().map(|(_, balance)| *balance);
    assert_eq!(
        final_balance(&tokio_permits),
        final_balance(&moonpool_permits),
        "both engines must leave the broker's per-consumer permit balance in the same state; \
         tokio {tokio_permits:?} vs moonpool {moonpool_permits:?}",
    );
    assert!(
        tokio_permits
            .last()
            .is_some_and(|(_, balance)| *balance >= 0),
        "the broker's per-consumer balance must not end parked below zero; \
         got {tokio_permits:?}",
    );
    assert_eq!(
        tokio_grants, moonpool_grants,
        "both engines must issue the same sequence of permit grants; tokio {tokio_grants:?} \
         vs moonpool {moonpool_grants:?}",
    );
    assert_eq!(
        tokio_grants.len(),
        5,
        "five grants: the initial window, one per popped poison delivery, ONE for the \
         dead-lettered unit the client will never pop, and one for the sentinel; \
         got {tokio_grants:?}",
    );

    assert_eq!(
        events[9],
        Event::Closed,
        "the trace closes with the consumer detached",
    );
}
