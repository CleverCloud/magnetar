// SPDX-License-Identifier: Apache-2.0

//! Error-path equivalence for [`Op::SendBatch`], the batched-publish op issue #436 added to the
//! differential harness (ADR-0105).
//!
//! `Op::SendBatch` arrived with only its happy path exercised: every trace that uses it publishes
//! against a live producer on a healthy connection, so neither of the two error outcomes the
//! runners already spell out for the plain [`Op::Send`] had a caller. Both are ordinary
//! user-reachable states, not defensive dead code:
//!
//! 1. **the producer is gone** — the trace dropped it ([`Op::DropProducer`]) and then published, so
//!    the runner has no producer to send on and must surface the harness's stable
//!    `producer-dropped` bucket rather than panicking on an `Option`;
//! 2. **the send itself fails** — the connection went terminal under the publish, so the batched
//!    `send()` future resolves `Err` and must be classified exactly as the plain one is.
//!
//! Both are pinned here across BOTH engines rather than on one, because the point of every file in
//! this crate is that tokio and moonpool agree; an error bucket that differed between them would be
//! a divergence no single-engine test could see. The mechanics are lifted from
//! `terminal_error_equivalence.rs` — its decode-fatal injection is the only terminal path the
//! scripted broker offers — with the op swapped.
//!
//! # Why its own integration-test binary
//!
//! Same reason as `terminal_error_equivalence.rs` and `corrupted_frame_equivalence.rs`: one
//! self-contained differential concern per binary keeps the per-leg `timeout` budgets local and the
//! broker wiring obvious. This file deliberately does not touch
//! `batch_redelivery_flow_equivalence.rs`, whose trace and assertions are the issue #436
//! regression statement.

#![forbid(unsafe_code)]

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, HANG_GUARD, Op, Trace, runner_moonpool, runner_tokio};

/// Payloads packed into the batched entry each trace attempts to publish. Two is enough to make it
/// a batch; the content never reaches a broker in either scenario.
fn payloads() -> Vec<Vec<u8>> {
    vec![b"batch-error-0".to_vec(), b"batch-error-1".to_vec()]
}

/// Publishing a batched entry after [`Op::DropProducer`] must surface the harness's stable
/// `producer-dropped` bucket on both engines — the same one the plain [`Op::Send`] surfaces — and
/// must not hang or panic on the absent producer.
#[tokio::test(flavor = "current_thread")]
async fn send_batch_after_producer_drop_is_equivalent_across_engines() {
    let trace = Trace::new(
        "persistent://public/default/diff-send-batch-dropped",
        "sub-send-batch-dropped",
        vec![
            Op::DropProducer,
            Op::SendBatch {
                payloads: payloads(),
            },
            // The plain sibling, on the same dropped producer, in the same trace: it is what makes
            // "SendBatch behaves like Send here" an assertion rather than a claim about one op.
            Op::Send {
                payload: b"plain-after-drop".to_vec(),
            },
        ],
    );

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_tokio::run(&broker_t.pulsar_url(), &trace),
    )
    .await
    .expect("tokio leg must not hang publishing on a dropped producer")
    .expect("tokio runner");
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_moonpool::run(&broker_m.host_port(), &trace),
    )
    .await
    .expect("moonpool leg must not hang publishing on a dropped producer")
    .expect("moonpool runner");
    broker_m.shutdown().await;

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged publishing a batched entry on a dropped producer",
    );
    assert_eq!(
        tokio_stream.events.len(),
        3,
        "the drop and both publishes must each surface an event, got {:?}",
        tokio_stream.events,
    );
    assert_eq!(
        tokio_stream.events[0],
        Event::ProducerDropped,
        "the trace releases the producer first",
    );
    assert_eq!(
        tokio_stream.events[1], tokio_stream.events[2],
        "a batched publish on a dropped producer must land in exactly the same error bucket as \
         a plain one, got {:?} and {:?}",
        tokio_stream.events[1], tokio_stream.events[2],
    );
    assert!(
        matches!(tokio_stream.events[1], Event::SendError { .. }),
        "publishing with no producer must surface a SendError, got {:?}",
        tokio_stream.events[1],
    );
}

/// A batched publish that the connection kills under it must classify its `Err` exactly as the
/// plain one does — `peer-closed` for the scripted broker's decode-fatal terminal drop
/// (ADR-0055 §1) — on both engines.
///
/// The trace sends the batch FIRST, so it is the in-flight publish the broker answers with the
/// unparseable command frame; the plain send that follows is issued after the plain driver has run
/// `fail_all_pending`, and pins that both ops agree on the terminal bucket from either side of the
/// drop (ADR-0059).
#[tokio::test(flavor = "current_thread")]
async fn send_batch_on_a_terminal_connection_is_equivalent_across_engines() {
    let trace = Trace::new(
        "persistent://public/default/diff-send-batch-terminal",
        "sub-send-batch-terminal",
        vec![
            Op::SendBatch {
                payloads: payloads(),
            },
            Op::Send {
                payload: b"plain-after-terminal-drop".to_vec(),
            },
        ],
    );

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    broker_t.inject_decode_fatal_frame_on_send();
    let tokio_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_tokio::run(&broker_t.pulsar_url(), &trace),
    )
    .await
    .expect("tokio leg must not hang on the terminal batched publish")
    .expect("tokio runner");
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.inject_decode_fatal_frame_on_send();
    let moonpool_stream = tokio::time::timeout(
        HANG_GUARD,
        runner_moonpool::run(&broker_m.host_port(), &trace),
    )
    .await
    .expect("moonpool leg must not hang on the terminal batched publish")
    .expect("moonpool runner");
    broker_m.shutdown().await;

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the terminal batched-publish trace",
    );
    assert_eq!(
        tokio_stream.events,
        vec![
            Event::SendError {
                kind: "peer-closed".to_owned(),
            },
            Event::SendError {
                kind: "peer-closed".to_owned(),
            },
        ],
        "the in-flight BATCHED publish and the plain send issued after the terminal drop must \
         both surface the terminal peer-closed outcome on both engines",
    );
}
