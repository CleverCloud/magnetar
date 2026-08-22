// SPDX-License-Identifier: Apache-2.0

//! Initial permit grant — tokio ↔ moonpool differential equivalence
//! (issue #426). Layer (d) of the ADR-0024 four-layer test policy.
//!
//! Both engines' fresh-subscribe paths used to follow the sans-io
//! `Connection::initial_flow` with a raw
//! `Connection::flow(handle, receiver_queue_size)`. `initial_flow` emits the
//! `CommandFlow` **and** updates the client-side mirrors; the raw call emitted a
//! second, wire-only frame that no mirror accounted for. The broker therefore
//! held `2 × receiver_queue_size` permits for a freshly-subscribed consumer
//! while `available_permits()` and `FlowStats` reported `1 ×` — the client's
//! view of the broker's balance was wrong from the very first frame, and the
//! broker could hand the consumer twice the messages its own queue was sized
//! for.
//!
//! The scripted broker's flow-grant log is the oracle: every `CommandFlow` it
//! received, as `(consumer_id, message_permits)` in arrival order **across
//! sessions**. The per-consumer `permits` balance cannot answer this — dispatch
//! spends it, and the redial replaces the session state that holds it.
//!
//! The trace is the drop + redial shape of
//! `reconnect_replay_gating_equivalence`: the broker closes the socket right
//! after the first ack-response, so the supervised client redials and
//! `rebuild_consumers` re-attaches the consumer. That covers BOTH grant sites in
//! one leg — the fresh subscribe and the post-reconnect re-attach — and each
//! must grant the receiver-queue size exactly once.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{HANG_GUARD, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::{MessageId, SupervisorConfig};

/// The `receiver_queue_size` both runners hard-code when they open the trace's
/// consumer (`ensure_consumer` in each runner).
const RECEIVER_QUEUE_SIZE: u32 = 16;

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

/// Tight backoff so the redial lands well inside the per-leg timeout budget.
fn supervisor() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
        ..SupervisorConfig::default()
    }
}

/// One `receiver_queue_size` grant on the fresh subscribe, one more on the
/// post-redial `rebuild_consumers` re-attach, and nothing else. Two pops out of
/// a 16-permit window stay under `maybe_flow`'s `receiver_queue_size / 2`
/// replenishment threshold, so every entry in the log is an initial grant.
fn assert_one_grant_per_attach(engine: &str, grants: &[(u64, u32)]) {
    let consumer_ids: BTreeSet<u64> = grants.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        consumer_ids.len(),
        1,
        "{engine} leg must grant permits to exactly one consumer, grants: {grants:?}",
    );
    assert_eq!(
        grants.len(),
        2,
        "{engine} leg must grant exactly once per attach — fresh subscribe, then the \
         post-redial re-attach (issue #426), grants: {grants:?}",
    );
    for (index, (_, permits)) in grants.iter().enumerate() {
        assert_eq!(
            *permits, RECEIVER_QUEUE_SIZE,
            "{engine} grant {index} must be exactly the receiver-queue size, grants: {grants:?}",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_flow_grants_one_receiver_queue_per_attach_on_both_engines() {
    let trace = Trace::new(
        "persistent://public/default/diff-initial-grant",
        "sub-initial-grant",
        vec![
            Op::Send {
                payload: b"before-drop".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(5),
            },
            Op::Ack {
                message_id: mid(1, 0),
            },
            Op::Send {
                payload: b"after-redial".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(5),
            },
            Op::Ack {
                message_id: mid(1, 1),
            },
            Op::Close,
        ],
    );

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    broker_t.drop_connection_after_first_ack();
    let tokio_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_tokio::run_supervised(&broker_t.pulsar_url(), &trace, supervisor()),
    )
    .await
    .expect("tokio leg must not hang across the drop + redial")
    .expect("tokio runner");
    let tokio_grants = broker_t.flow_grant_log_snapshot();
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.drop_connection_after_first_ack();
    let moonpool_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_moonpool::run_supervised(&broker_m.host_port(), &trace, supervisor()),
    )
    .await
    .expect("moonpool leg must not hang across the drop + redial")
    .expect("moonpool runner");
    let moonpool_grants = broker_m.flow_grant_log_snapshot();
    broker_m.shutdown().await;

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the initial-grant trace {trace:?}",
    );
    assert_one_grant_per_attach("tokio", &tokio_grants);
    assert_one_grant_per_attach("moonpool", &moonpool_grants);
    assert_eq!(
        tokio_grants, moonpool_grants,
        "engine permit-grant sequences diverged for the initial-grant trace",
    );
}
