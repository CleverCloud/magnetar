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
