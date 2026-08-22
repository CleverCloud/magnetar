// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for a Pulsar
//! **Shared** subscription under consumer churn, and for the caller-driven
//! in-place recovery — issue #414.
//!
//! Until this landed the scripted broker had no shared-dispatcher state at all:
//! every consumer walked the ledger on its own cursor, so two consumers on one
//! Shared subscription each received the whole ledger and the #414 failure mode
//! was not even expressible in the harness. `broker.rs` now models what a Shared
//! dispatcher actually is — ONE cursor per `(topic, subscription)`, handed out
//! round-robin to whoever holds permits, with a detaching consumer's un-acked
//! entries returned to the survivors.
//!
//! Two scenarios, both asserted identical across the two engines:
//!
//! 1. **Churn.** Two Shared consumers with one-permit windows split a four-entry backlog. One
//!    detaches mid-drain holding an un-acked entry; the survivor must receive that entry (the
//!    redelivery) and then keep draining the rest of the ledger. Before the shared dispatcher
//!    existed both consumers would simply have replayed the whole ledger independently and this
//!    scenario would have proved nothing.
//! 2. **Recovery.** `Consumer::resubscribe()` on a live Shared consumer: the in-place re-attach
//!    zeroes the permit mirrors, re-emits `CommandSubscribe` for the SAME consumer id, and the
//!    broker's `Success` re-arms the grant. The event carries the re-armed balance read back
//!    through `Consumer::available_permits()` — which issue #414 re-pointed at the REAL
//!    decrementing balance, so both engines must report the same number.
//!
//! The one-permit receiver-queue window is deliberate: it makes the round-robin
//! hand-off deterministic instead of letting whichever consumer subscribed first
//! swallow the whole backlog, which is what keeps the two legs comparable.
//!
//! Two further scenarios reproduce the broker-side fault itself and pin ADR-0103's
//! bounded automatic recovery against it — see
//! [`ScriptedBroker::leak_shared_permits_on_consumer_churn`] and the two
//! `wedged_shared_dispatcher_*` tests at the bottom of this file.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;

/// Receiver-queue size for every Shared consumer in these traces. One permit
/// per consumer makes each dispatch an explicit, observable hand-off.
const RQ: usize = 1;

const RECV_TIMEOUT: Duration = Duration::from_secs(2);

fn message_id(entry_id: u64) -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    }
}

/// Payloads for the four-entry backlog, in ledger order.
fn payloads() -> Vec<Vec<u8>> {
    (0..4u8)
        .map(|i| format!("entry-{i}").into_bytes())
        .collect()
}

/// Entry id of a `Received` event, or `None` for any other event shape.
fn received_entry(event: &Event) -> Option<u64> {
    match event {
        Event::Received { message_id, .. } => Some(message_id.entry_id),
        _ => None,
    }
}

/// The churn trace: publish the whole backlog first, then attach both consumers.
///
/// Publishing first matters — with no producer traffic interleaving the
/// consumers' subscribe + flow frames, the dispatcher's hand-off is a pure
/// function of the trace, which is what makes the two engine legs comparable at
/// all.
fn churn_trace() -> Trace {
    let mut ops: Vec<Op> = payloads()
        .into_iter()
        .map(|payload| Op::Send { payload })
        .collect();
    ops.extend([
        Op::OpenSharedConsumer {
            name: "a".to_owned(),
            receiver_queue_size: RQ,
        },
        Op::OpenSharedConsumer {
            name: "b".to_owned(),
            receiver_queue_size: RQ,
        },
        // One receive each. Neither acks, so whatever "b" is holding is still
        // owed by the subscription when it leaves.
        Op::RecvShared {
            name: "a".to_owned(),
            timeout: RECV_TIMEOUT,
        },
        Op::RecvShared {
            name: "b".to_owned(),
            timeout: RECV_TIMEOUT,
        },
        // The churn: "b" detaches mid-drain. Its un-acked in-flight entries go
        // back to the dispatcher's redelivery pool.
        Op::CloseSharedConsumer {
            name: "b".to_owned(),
        },
        // The survivor drains the rest of the backlog plus the redelivery.
        Op::RecvShared {
            name: "a".to_owned(),
            timeout: RECV_TIMEOUT,
        },
        Op::RecvShared {
            name: "a".to_owned(),
            timeout: RECV_TIMEOUT,
        },
        Op::RecvShared {
            name: "a".to_owned(),
            timeout: RECV_TIMEOUT,
        },
        // Recovering the WRONG consumer: an operator reacting to a stall may
        // reach for the one that already left. `resubscribe()` must refuse a
        // closed consumer — cleanly, without touching its state, and identically
        // on both engines — rather than resurrect a retired dispatcher slot.
        Op::ResubscribeShared {
            name: "b".to_owned(),
        },
        // A late joiner: the backlog is drained and the redelivery pool is
        // empty, so it is handed nothing at all. Detaching it (via the trailing
        // `Op::Close`) must therefore hand nothing BACK — a consumer that held
        // no in-flight entry has none to redeliver, and a dispatcher that
        // redelivered on an empty detach would duplicate the whole backlog on
        // every scale-down.
        Op::OpenSharedConsumer {
            name: "late".to_owned(),
            receiver_queue_size: RQ,
        },
        Op::Close,
    ]);
    Trace::new(
        "persistent://public/default/shared-churn-equiv",
        "sub-shared-churn",
        ops,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn shared_subscription_churn_event_streams_agree() {
    let trace = churn_trace();

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the Shared-subscription churn sequence",
    );

    let events = &tokio_stream.events;

    // Both opens report the full receiver-queue grant through
    // `available_permits()` — the accessor issue #414 re-pointed at the REAL
    // decrementing balance.
    for (index, name) in [(4usize, "a"), (5usize, "b")] {
        assert_eq!(
            events[index],
            Event::SharedConsumerOpened { permits: RQ as u32 },
            "op {index} ({name}) must report its full initial grant, got {:?}",
            events[index],
        );
    }

    // The dispatcher SPLIT the backlog. Under the pre-#414 scripted broker —
    // one independent cursor per consumer — both consumers walked the whole
    // ledger, so these two receives would have returned the SAME entry and the
    // #414 failure mode was not expressible in this harness at all.
    let a_first = received_entry(&events[6]).expect("consumer a must receive");
    let b_first = received_entry(&events[7]).expect("consumer b must receive");
    assert_ne!(
        a_first, b_first,
        "two consumers on ONE Shared subscription must be handed DIFFERENT entries; \
         a duplicate here means the dispatcher is not sharing a cursor",
    );

    assert_eq!(
        events[8],
        Event::SharedConsumerClosed,
        "the mid-drain detach must resolve",
    );

    // The survivor drains everything that is left, and the entry the departed
    // consumer never acked comes back to it. Nothing is stranded on the consumer
    // that left — the property a Shared subscription owes its survivors, and the
    // one issue #414 reports the real broker failing to honour after churn.
    let survivor: Vec<u64> = [9usize, 10, 11]
        .iter()
        .map(|index| {
            received_entry(&events[*index]).unwrap_or_else(|| {
                panic!(
                    "op {index}: survivor must keep receiving, got {:?}",
                    events[*index]
                )
            })
        })
        .collect();
    assert!(
        survivor.contains(&b_first),
        "the departed consumer's un-acked entry ({b_first}) must be redelivered to the \
         survivor, got {survivor:?}",
    );
    let mut delivered: Vec<u64> = survivor.clone();
    delivered.push(a_first);
    delivered.push(b_first);
    delivered.sort_unstable();
    delivered.dedup();
    assert_eq!(
        delivered,
        vec![0, 1, 2, 3],
        "every published entry must reach a live consumer exactly once across the churn",
    );
    assert_eq!(
        survivor.iter().filter(|entry| **entry == b_first).count(),
        1,
        "the redelivery must land exactly once, got {survivor:?}",
    );

    // The departed consumer cannot be re-subscribed: it is closed, which is one
    // of the states `Connection::resubscribe_consumer_in_place` refuses without
    // mutating anything. Both engines must surface the same refusal.
    assert!(
        matches!(events[12], Event::SharedConsumerResubscribeError { .. }),
        "re-subscribing the consumer that already left must be refused, got {:?}",
        events[12],
    );

    // The late joiner finds an exhausted cursor and an empty redelivery pool.
    assert_eq!(
        events[13],
        Event::SharedConsumerOpened { permits: RQ as u32 },
        "a consumer joining a drained subscription still gets its full grant, got {:?}",
        events[13],
    );
    assert_eq!(
        events[14],
        Event::Closed,
        "the trace closes with every consumer detached",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn shared_consumer_resubscribe_event_streams_agree() {
    let trace = Trace::new(
        "persistent://public/default/shared-resub-equiv",
        "sub-shared-resub",
        vec![
            Op::Send {
                payload: b"backlog-0".to_vec(),
            },
            Op::OpenSharedConsumer {
                name: "a".to_owned(),
                receiver_queue_size: RQ,
            },
            Op::RecvShared {
                name: "a".to_owned(),
                timeout: RECV_TIMEOUT,
            },
            Op::AckShared {
                name: "a".to_owned(),
                message_id: message_id(0),
            },
            // The #414 recovery ladder's first rung: re-attach this consumer id
            // in place on the live socket. The permit mirrors are zeroed, a
            // fresh `CommandSubscribe` goes out, and the broker's `Success`
            // re-arms the grant — which is what the event reports.
            Op::ResubscribeShared {
                name: "a".to_owned(),
            },
            // The re-armed grant is real: a message published after the recovery
            // is dispatched against it.
            Op::Send {
                payload: b"after-recovery".to_vec(),
            },
            Op::RecvShared {
                name: "a".to_owned(),
                timeout: RECV_TIMEOUT,
            },
            // Deliberately NO `Op::Close`: the trace just stops, so the runner's
            // implicit teardown is what closes the consumer — the shape a client
            // that simply exits produces, and the one that leaves the broker to
            // detach it from the Shared dispatcher.
        ],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the in-place re-subscribe recovery",
    );
    assert_eq!(
        tokio_stream.events[4],
        Event::SharedConsumerResubscribed,
        "the in-place re-attach must be accepted on a live consumer, got {:?}",
        tokio_stream.events[4],
    );
    assert_eq!(
        tokio_stream.events[6],
        Event::Received {
            payload: b"after-recovery".to_vec(),
            message_id: message_id(1),
        },
        "the re-armed grant must actually carry dispatch, got {:?}",
        tokio_stream.events[6],
    );
}

/// Window for the stall-watchdog leg. Tiny on purpose: the consumer below is
/// granted permits and handed nothing, so it crosses this while the trace's own
/// receive timeout is still running.
const STALL_TIMEOUT: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "current_thread")]
async fn stalled_shared_consumer_is_drained_by_both_drivers() {
    // A Shared consumer that is granted permits and then handed nothing is
    // exactly the issue #414 shape. With `consumer_stall_timeout` armed, the
    // sans-io layer surfaces one `ConsumerStalled`, and each engine's driver
    // must DRAIN it: silently, without logging it twice (ADR-0054's
    // single-owner rule), without treating it as an error that tears the
    // connection down, and without letting it accumulate in the proto event
    // queue that nothing else polls.
    //
    // The trace publishes nothing, so `RecvShared` resolves to `RecvTimeout` on
    // both engines and the consumer stays open and usable across the stall —
    // which is what proves the drain was silent rather than fatal. No `Op::Close`
    // either, so the runner's implicit teardown detaches a consumer that never
    // held a single in-flight entry.
    let trace = Trace::new(
        "persistent://public/default/shared-stall-equiv",
        "sub-shared-stall",
        vec![
            Op::OpenSharedConsumer {
                name: "idle".to_owned(),
                receiver_queue_size: RQ,
            },
            Op::RecvShared {
                name: "idle".to_owned(),
                timeout: RECV_TIMEOUT,
            },
            // Still alive after the stall was reported and drained: publish, and
            // the same consumer receives it on the permits it still holds.
            Op::Send {
                payload: b"after-stall".to_vec(),
            },
            Op::RecvShared {
                name: "idle".to_owned(),
                timeout: RECV_TIMEOUT,
            },
        ],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run_with_stall_timeout(&pulsar_url, &trace, STALL_TIMEOUT)
        .await
        .expect("tokio runner");
    broker.clear_frame_log();
    let moonpool_stream =
        runner_moonpool::run_with_stall_timeout(&host_port, &trace, STALL_TIMEOUT)
            .await
            .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the stalled-consumer sequence",
    );

    let events = &tokio_stream.events;
    assert_eq!(
        events[0],
        Event::SharedConsumerOpened { permits: RQ as u32 },
        "the consumer opens with its full grant un-spent — the state the watchdog watches",
    );
    assert_eq!(
        events[1],
        Event::RecvTimeout,
        "nothing is published, so the receive times out and the stall window elapses",
    );
    assert_eq!(
        events[3],
        Event::Received {
            payload: b"after-stall".to_vec(),
            message_id: message_id(0),
        },
        "the consumer must still be granted and usable after its stall was drained, got {:?}",
        events[3],
    );
}

// ---------------------------------------------------------------------------
// Issue #414 / ADR-0103 — the broker-side wedge, and bounded automatic recovery
// from it.
//
// Everything above models a CORRECT Shared dispatcher. These two model the fault
// itself: `leak_shared_permits_on_consumer_churn` makes a detach subtract the
// departing consumer's remaining permits from the subscription's aggregate permit
// counter TWICE, so each churn event leaks that many permits with no credit
// behind them. Once the aggregate crosses zero the dispatcher's read gate closes
// and the subscription stops dispatching entirely — the survivor still holds
// per-consumer permits, the backlog is still non-empty, the connection is still
// healthy, and nothing moves. That is the client-visible shape issue #414 reports,
// with the broker's own `availablePermits` for the subscription at `-177300`.
//
// The client cannot cause it: the wire protocol carries only monotonic client →
// broker permit increments, so a negative aggregate is by construction a
// broker-side accounting fault. It is a HYPOTHESIS about the churn-path accounting
// that reproduces the reported signature, not a verified reading of any broker
// source — `UPSTREAM-ISSUE-DRAFT.md` frames it that way for Pulsar's maintainers.
//
// What the two tests then pin is the recovery arithmetic, which is the whole
// reason ADR-0103's budget is a small integer rather than "retry until it works".
// An in-place re-subscribe zeroes this consumer's permits broker-side and the
// client answers the `Success` with one fresh full-window `CommandFlow`, so ONE
// attempt lifts the aggregate by exactly `receiver_queue_size`. The same trace and
// the same leak therefore recover under a sufficient budget and stay wedged under
// an insufficient one — which is why a leak of `-177300` against a 1000-message
// queue is an operator's `topics unload` and not a client's retry loop.
// ---------------------------------------------------------------------------

/// Stall window for the wedge scenarios. Long enough that no episode can close
/// while the opening ops are still running (they are milliseconds over loopback),
/// short enough that two full episodes fit inside `WEDGE_RECV_TIMEOUT`.
///
/// **If either of these tests ever flakes, this constant is the first suspect.** The
/// traces assume the three opening ops complete within this window of the survivor's
/// initial grant; a machine slow enough to close an episode BEFORE the churn would spend
/// a recovery attempt while the aggregate is still positive, leaving the recovering leg
/// one attempt short. Raise it (and `WEDGE_RECV_TIMEOUT` with it) rather than widening
/// the budgets, which is what the tests are asserting.
const WEDGE_STALL_TIMEOUT: Duration = Duration::from_millis(300);

/// Receive budget for the wedge scenarios: comfortably longer than the two stall
/// episodes a recovering leg needs, so a `RecvTimeout` there would be a real
/// failure to recover rather than an impatient assertion.
const WEDGE_RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Survivor's receiver queue. Each automatic recovery attempt credits the broker's
/// leaked aggregate by exactly this much.
const SURVIVOR_RQ: usize = 2;

/// The departing consumer's receiver queue. It receives nothing (the topic is
/// empty while it is attached), so it detaches holding all four permits and the
/// armed double-subtraction leaks 4 — two survivor windows' worth, which is what
/// makes one attempt insufficient and two sufficient.
const LEAVER_RQ: usize = 4;

/// Publish-after-churn trace: two Shared consumers, one leaves immediately, then a
/// message is published that only an unwedged dispatcher can deliver.
///
/// Nothing is published before the churn on purpose. The departing consumer must
/// leave holding its FULL grant, because the leak is exactly what it still holds —
/// and a survivor that never received anything is unambiguously stall-eligible
/// (un-spent permits over an empty queue) from the moment it is granted.
fn wedge_trace(topic: &str, subscription: &str) -> Trace {
    Trace::new(
        topic,
        subscription,
        vec![
            Op::OpenSharedConsumer {
                name: "survivor".to_owned(),
                receiver_queue_size: SURVIVOR_RQ,
            },
            Op::OpenSharedConsumer {
                name: "leaver".to_owned(),
                receiver_queue_size: LEAVER_RQ,
            },
            // The churn. Aggregate: 2 + 4 granted, minus 4 returned, minus a second
            // 4 that was never credited → -2, with the survivor still holding 2.
            Op::CloseSharedConsumer {
                name: "leaver".to_owned(),
            },
            Op::Send {
                payload: b"after-wedge".to_vec(),
            },
            Op::RecvShared {
                name: "survivor".to_owned(),
                timeout: WEDGE_RECV_TIMEOUT,
            },
        ],
    )
}

/// How many `CommandSubscribe` frames the broker saw.
///
/// Two are the opens; every further one is an automatic in-place re-subscribe the
/// stall watchdog drove. This is the discriminating assertion for the bound — the
/// exhausted leg's user-visible outcome (`RecvTimeout`) is by construction the same
/// one a client with no recovery at all would produce.
fn subscribe_frame_count(broker: &ScriptedBroker) -> usize {
    let subscribe = magnetar_proto::pb::base_command::Type::Subscribe as i32;
    broker
        .frame_log_snapshot()
        .into_iter()
        .filter(|kind| *kind == subscribe)
        .count()
}

#[tokio::test(flavor = "current_thread")]
async fn wedged_shared_dispatcher_is_recovered_by_bounded_auto_recovery() {
    // Budget of two against a leak of two survivor windows: the first attempt lifts
    // the aggregate from -2 to 0 — still gated, since the real dispatcher's read
    // gate is `> 0` — and the second lifts it to 2 and the backlog finally moves.
    // That the recovery took TWO attempts rather than one is the point: this is a
    // subscription-wide corruption being paid down one receiver-queue window at a
    // time, not a per-consumer glitch a single re-attach clears.
    const MAX_ATTEMPTS: u32 = 2;
    let trace = wedge_trace(
        "persistent://public/default/shared-wedge-recovered",
        "sub-shared-wedge-recovered",
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    broker.leak_shared_permits_on_consumer_churn();
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    broker.clear_frame_log();
    let tokio_stream = runner_tokio::run_with_stall_auto_recovery(
        &pulsar_url,
        &trace,
        WEDGE_STALL_TIMEOUT,
        MAX_ATTEMPTS,
    )
    .await
    .expect("tokio runner");
    let tokio_subscribes = subscribe_frame_count(&broker);

    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run_with_stall_auto_recovery(
        &host_port,
        &trace,
        WEDGE_STALL_TIMEOUT,
        MAX_ATTEMPTS,
    )
    .await
    .expect("moonpool runner");
    let moonpool_subscribes = subscribe_frame_count(&broker);

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged recovering a wedged Shared dispatcher",
    );

    let events = &tokio_stream.events;
    assert_eq!(
        events[0],
        Event::SharedConsumerOpened {
            permits: SURVIVOR_RQ as u32
        },
        "the survivor opens with its full, un-spent grant — the state the watchdog watches",
    );
    assert_eq!(
        events[1],
        Event::SharedConsumerOpened {
            permits: LEAVER_RQ as u32
        },
        "and so does the consumer that is about to leave holding all of it",
    );
    assert_eq!(events[2], Event::SharedConsumerClosed, "the churn event");
    assert!(
        matches!(events[3], Event::Sent { .. }),
        "publishing is producer-side and unaffected by a wedged dispatcher, got {:?}",
        events[3],
    );
    assert_eq!(
        events[4],
        Event::Received {
            payload: b"after-wedge".to_vec(),
            message_id: message_id(0),
        },
        "the backlog must reach the survivor once automatic recovery has credited the \
         broker's leaked aggregate back above zero, got {:?}",
        events[4],
    );

    // Two opens plus exactly `MAX_ATTEMPTS` automatic re-subscribes, on BOTH engines.
    // The whole mechanism lives in the shared sans-io layer precisely so this number
    // cannot drift between them.
    let expected = 2 + MAX_ATTEMPTS as usize;
    assert_eq!(
        (tokio_subscribes, moonpool_subscribes),
        (expected, expected),
        "each engine must send two opens and exactly {MAX_ATTEMPTS} recovery re-subscribes",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wedged_shared_dispatcher_exhausts_an_insufficient_auto_recovery_budget() {
    // The same trace and the same leak with a budget of one. The single attempt
    // lifts the aggregate from -2 to 0, which is still gated, and then the client
    // stops: no third `CommandSubscribe`, one `warn!` naming `pulsar-admin topics
    // unload`, and the message never arrives.
    //
    // This is also the negative control for the sibling test above — it proves the
    // wedge is real and that automatic recovery, not the trace, is what clears it.
    // And it is the shape issue #414 actually reported: an aggregate at `-177300`
    // is ~178 receiver-queue windows deep, so no sane client budget reaches it and
    // the escalation is the honest answer rather than an unbounded retry loop.
    const MAX_ATTEMPTS: u32 = 1;
    let trace = wedge_trace(
        "persistent://public/default/shared-wedge-exhausted",
        "sub-shared-wedge-exhausted",
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    broker.leak_shared_permits_on_consumer_churn();
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    broker.clear_frame_log();
    let tokio_stream = runner_tokio::run_with_stall_auto_recovery(
        &pulsar_url,
        &trace,
        WEDGE_STALL_TIMEOUT,
        MAX_ATTEMPTS,
    )
    .await
    .expect("tokio runner");
    let tokio_subscribes = subscribe_frame_count(&broker);

    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run_with_stall_auto_recovery(
        &host_port,
        &trace,
        WEDGE_STALL_TIMEOUT,
        MAX_ATTEMPTS,
    )
    .await
    .expect("moonpool runner");
    let moonpool_subscribes = subscribe_frame_count(&broker);

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged exhausting the auto-recovery budget",
    );
    assert_eq!(
        tokio_stream.events[4],
        Event::RecvTimeout,
        "one receiver-queue window is not enough to pay down this leak, so the \
         subscription stays wedged and nothing is delivered, got {:?}",
        tokio_stream.events[4],
    );

    let expected = 2 + MAX_ATTEMPTS as usize;
    assert_eq!(
        (tokio_subscribes, moonpool_subscribes),
        (expected, expected),
        "the budget is a hard cap: two opens and exactly {MAX_ATTEMPTS} recovery \
         re-subscribe, not one per stall window for the life of the consumer",
    );
}
