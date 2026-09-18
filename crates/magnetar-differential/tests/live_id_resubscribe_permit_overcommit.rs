// SPDX-License-Identifier: Apache-2.0

//! Issue #414 client-side residue (a): what `Consumer::resubscribe()` costs the
//! broker's permit counter when the broker does NOT recreate the consumer slot.
//!
//! PR 425 shipped `Connection::resubscribe_consumer_in_place`
//! (`crates/magnetar-proto/src/conn.rs:7975`). It zeroes the client's permit mirrors,
//! re-emits `CommandSubscribe` for the SAME consumer id, and lets the re-subscribe
//! `Success` arm re-issue a full `CommandFlow(receiver_queue_size)`
//! (`conn.rs:2704` → `consumer.rs:1411-1415`). Its doc comment states the premise this
//! file is built to measure:
//!
//! > the broker recreates its dispatcher slot at `availablePermits = 0`
//!
//! **Premise encoded here, verified elsewhere.** Apache Pulsar's `ServerCnx
//! .handleSubscribe` looks the consumer id up in its own per-connection map first and,
//! on a hit for an already-completed subscribe, answers `CommandSuccess` and returns
//! without touching the dispatcher, the cursor, or `availablePermits` (apache/pulsar
//! v4.0.4 `ServerCnx.java:1319-1326`). That reading is verified against the Pulsar
//! source independently of this file; everything below is conditional on it.
//!
//! The scripted broker's default `Subscribe` arm encodes the OPPOSITE premise — it
//! re-`insert`s a fresh `ConsumerState` and sets `c.permits = 0`
//! (`crates/magnetar-differential/src/broker.rs:1611-1648`), commented "mirrors the real
//! broker". So no pre-existing differential test can see this: the broker under test
//! absorbs the second grant. `ScriptedBroker::subscribe_on_live_consumer_id_is_a_success_noop`
//! selects the faithful model as an explicit, opt-in knob (default off), leaving every
//! other scenario on the model it was written against.
//!
//! The invariant asserted: **one in-place recovery must leave the broker holding
//! `receiver_queue_size` permits for that consumer, not `2 x receiver_queue_size`.**
//! A client that over-grants is adding to the very counter issue #414 observed at
//! `-177300`, in the opposite direction, without the broker ever having agreed.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};

/// Receiver-queue size for the consumer under test. Eight rather than one so a
/// double grant reads as an unmistakable doubling instead of an off-by-one.
const RQ: usize = 8;

/// Long enough for the recovery's `CommandSubscribe` -> `CommandSuccess` ->
/// `CommandFlow` round trip over loopback to have been processed by the broker
/// before the log is read. Nothing is ever published in this trace, so this
/// receive is EXPECTED to time out — it is the quiescence point, not an
/// assertion about delivery.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(2);

/// One Shared consumer, one `resubscribe()`, then quiescence.
///
/// Deliberately publishes NOTHING before the recovery. A drained backlog would
/// leave the broker's balance at zero when the second grant lands, and `0 + RQ`
/// masks the over-commit exactly.
fn overcommit_trace(topic: &str, subscription: &str) -> Trace {
    Trace::new(
        topic,
        subscription,
        vec![
            Op::OpenSharedConsumer {
                name: "a".to_owned(),
                receiver_queue_size: RQ,
                max_redeliver_count: 0,
            },
            // The #414 recovery entry point, on a consumer that is still live.
            Op::ResubscribeShared {
                name: "a".to_owned(),
            },
            Op::RecvShared {
                name: "a".to_owned(),
                timeout: QUIESCE_TIMEOUT,
            },
        ],
    )
}

/// The broker's own per-consumer permit balance after every credit and every
/// dispatch debit, for the single consumer in this trace.
fn broker_permit_balances(broker: &ScriptedBroker) -> Vec<i64> {
    broker
        .consumer_permit_log_snapshot()
        .into_iter()
        .map(|(_, balance_after)| balance_after)
        .collect()
}

/// Per-engine preconditions: the trace ran as written, and the recovery's grant
/// actually reached the broker. Without the second check the permit assertion
/// could pass vacuously on a missed sync.
fn assert_measurable(engine: &str, stream: &magnetar_differential::EventStream, balances: &[i64]) {
    assert_eq!(
        stream.events[0],
        Event::SharedConsumerOpened { permits: RQ as u32 },
        "{engine}: the consumer must open on its full receiver-queue grant, got {:?}",
        stream.events[0],
    );
    assert_eq!(
        stream.events[1],
        Event::SharedConsumerResubscribed,
        "{engine}: the in-place recovery must be accepted, got {:?}",
        stream.events[1],
    );
    assert_eq!(
        balances.len(),
        2,
        "{engine}: expected exactly two broker-side permit credits (the initial grant \
         and the recovery grant); got {balances:?}. Fewer means the recovery's \
         CommandFlow never landed and the measurement below would be vacuous.",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn one_in_place_resubscribe_must_not_double_the_brokers_permit_window() {
    let trace = overcommit_trace(
        "persistent://public/default/live-id-resubscribe-overcommit",
        "sub-live-id-resubscribe-overcommit",
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    // Model the real broker: a re-subscribe for a live consumer id is a
    // `CommandSuccess` no-op. See the module header for the premise and its scope.
    broker.subscribe_on_live_consumer_id_is_a_success_noop();
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    broker.clear_consumer_permit_log();
    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let tokio_balances = broker_permit_balances(&broker);

    broker.clear_consumer_permit_log();
    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");
    let moonpool_balances = broker_permit_balances(&broker);

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged recovering a live Shared consumer in place",
    );

    assert_measurable("tokio", &tokio_stream, &tokio_balances);
    assert_measurable("moonpool", &moonpool_stream, &moonpool_balances);

    // Both engines in ONE assertion so a failure reports BOTH measurements —
    // the mechanism lives in the shared sans-io layer, so neither engine can
    // be the explanation.
    let expected = vec![RQ as i64, RQ as i64];
    assert_eq!(
        (&tokio_balances, &moonpool_balances),
        (&expected, &expected),
        "ONE in-place resubscribe over-committed the broker's permit counter. The \
         broker answered the re-subscribe for a live consumer id with a bare \
         CommandSuccess and kept its pre-recovery balance (apache/pulsar v4.0.4 \
         ServerCnx.java:1319-1326), then the client's SubscribeSuccess arm granted a \
         second full window of {RQ} on top of it (conn.rs SubscribeSuccess arm -> \
         ConsumerState::initial_flow). Broker-observed balances: tokio \
         {tokio_balances:?}, moonpool {moonpool_balances:?}; expected {expected:?} on \
         both. Each recovery attempt hands the broker receiver_queue_size permits it \
         never agreed to.",
    );
}

/// The control for the test above: with the knob OFF — the scripted broker's
/// historical fresh-slot model — the same trace shows no over-commit at all,
/// because the broker itself zeroes the counter the client is about to re-grant.
///
/// This is what makes the knob, not the trace, the discriminator: it pins that the
/// existing harness is structurally unable to observe residue (a), and that adding
/// the knob changed no client behavior.
#[tokio::test(flavor = "current_thread")]
async fn the_fresh_slot_broker_model_absorbs_the_second_grant() {
    let trace = overcommit_trace(
        "persistent://public/default/live-id-resubscribe-fresh-slot",
        "sub-live-id-resubscribe-fresh-slot",
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    // Knob deliberately NOT armed.
    let pulsar_url = broker.pulsar_url();

    broker.clear_consumer_permit_log();
    let stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let balances = broker_permit_balances(&broker);

    assert_eq!(
        stream.events[1],
        Event::SharedConsumerResubscribed,
        "the in-place recovery must be accepted, got {:?}",
        stream.events[1],
    );
    assert_eq!(
        balances,
        vec![RQ as i64, RQ as i64],
        "under the fresh-slot model the broker zeroes the counter on the re-subscribe, \
         so the client's second full grant lands on an empty slot and the over-commit \
         is invisible; got {balances:?}",
    );
}
