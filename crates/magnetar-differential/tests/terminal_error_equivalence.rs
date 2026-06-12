// SPDX-License-Identifier: Apache-2.0

//! Terminal-error differential equivalence (ADR-0055 §1 — ADR-0024 layer d
//! for the plain-connection terminal-fail-fast fix).
//!
//! The scripted broker completes a normal `CommandConnect` →
//! `CommandConnected` → lookup → `CommandProducerSuccess` handshake, then
//! answers the first in-flight `CommandSend` with ONE **decode-fatal**
//! command frame ([`ScriptedBroker::inject_decode_fatal_frame_on_send`]) — a
//! corrupt length prefix whose command bytes are not valid protobuf — and
//! closes the session.
//!
//! Unlike the recoverable CRC32C drop in `corrupted_frame_equivalence.rs`, a
//! decode-fatal command frame is unparseable from that byte on: the proto
//! decode loop surfaces a fatal `Frame(Decode(..))`, the plain
//! (non-supervised) driver exits, and `Connection::fail_all_pending`
//! resolves the in-flight send future with `OpOutcome::Terminal`. Each engine
//! maps that to `ClientError::PeerClosed`. Both legs must behave identically:
//!
//! 1. the in-flight `send()` resolves PROMPTLY with a terminal error (no hang) — the
//!    `tokio::time::timeout` wrapper around each runner leg fails the test if either engine stalls;
//! 2. the resulting [`EventStream`]s compare equal byte-for-byte: the send op collapses to
//!    `Event::SendError { kind: "peer-closed" }` on both engines, and the trailing `Close` to
//!    `Event::Closed`.
//!
//! # Why its own integration-test binary with ONE test fn
//!
//! Mirrors `corrupted_frame_equivalence.rs`: a single self-contained
//! differential scenario per binary keeps the harness wiring obvious and the
//! per-leg `timeout` budgets local. No global subscriber is installed here
//! (the terminal path emits no point-of-detection log this test asserts on),
//! but the one-fn-per-binary shape is kept for symmetry with its sibling.

#![forbid(unsafe_code)]

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};

#[tokio::test(flavor = "current_thread")]
async fn terminal_decode_fatal_on_send_is_equivalent_across_engines() {
    // A single in-flight `Send` triggers the broker's decode-fatal reply.
    // The trace stops at the send: the runner's implicit teardown aborts the
    // (already-dead) driver without issuing a fresh `CloseProducer` request
    // — a request registered AFTER the terminal drop has no live driver to
    // resolve it, so a trailing `Op::Close` here would mask the terminal
    // signal under a teardown stall. The in-flight send is the load-bearing
    // observation; one op keeps the equivalence claim sharp.
    let trace = Trace::new(
        "persistent://public/default/diff-terminal-exit",
        "sub-terminal-exit",
        vec![Op::Send {
            payload: b"in-flight-when-peer-dies".to_vec(),
        }],
    );

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    broker_t.inject_decode_fatal_frame_on_send();
    let tokio_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run(&broker_t.pulsar_url(), &trace),
    )
    .await
    .expect("tokio leg must not hang after the decode-fatal terminal drop")
    .expect("tokio runner");
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.inject_decode_fatal_frame_on_send();
    let moonpool_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_moonpool::run(&broker_m.host_port(), &trace),
    )
    .await
    .expect("moonpool leg must not hang after the decode-fatal terminal drop")
    .expect("moonpool runner");
    broker_m.shutdown().await;

    // ── Equivalence claim: both engines surface the SAME terminal outcome
    // on the in-flight send and tear down identically. ──
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the terminal-exit trace {trace:?}",
    );
    assert_eq!(tokio_stream.events.len(), 1);
    // The in-flight send resolves with the terminal `PeerClosed`, which the
    // runners collapse to the stable `peer-closed` category string. This is
    // the load-bearing assertion: a hang would have tripped the per-leg
    // `timeout` above, and a NON-terminal classification (e.g. a plain
    // `closed` or `other`) would mean the engine failed to map
    // `OpOutcome::Terminal` to `PeerClosed` (ADR-0055 §1).
    assert_eq!(
        tokio_stream.events[0],
        Event::SendError {
            kind: "peer-closed".to_owned(),
        },
        "the in-flight send must surface the terminal PeerClosed outcome, \
         not a hang or a mis-classified error",
    );
}

/// ADR-0059 / follow-ups §4.1: a NEW op issued AFTER the terminal drop must
/// surface the SAME terminal `peer-closed` outcome on BOTH engines — not a
/// hang, not a divergence. This is the new-op companion to the in-flight
/// differential claim above (ADR-0055 §1).
///
/// The trace issues TWO sends on the same producer. The first triggers the
/// broker's decode-fatal terminal drop (resolving as `peer-closed`, exactly as
/// the single-send test). The second is issued AFTER the plain driver has run
/// `fail_all_pending` (slot `closed`) and latched `no_driver`: it must fast-fail
/// SYNCHRONOUSLY via the slot-close + `no_driver` mapping, producing a second
/// identical `SendError { kind: "peer-closed" }` on each engine. The per-leg
/// `timeout` is the no-hang guard; the byte-for-byte `EventStream` compare is
/// the equivalence claim.
#[tokio::test(flavor = "current_thread")]
async fn terminal_new_send_after_drop_is_equivalent_across_engines() {
    let trace = Trace::new(
        "persistent://public/default/diff-terminal-newop",
        "sub-terminal-newop",
        vec![
            // (1) triggers the decode-fatal terminal drop → peer-closed.
            Op::Send {
                payload: b"in-flight-when-peer-dies".to_vec(),
            },
            // (2) issued after the connection is terminal → must ALSO be
            // peer-closed (slot closed + no_driver latched), not a hang.
            Op::Send {
                payload: b"after-terminal-drop".to_vec(),
            },
        ],
    );

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    broker_t.inject_decode_fatal_frame_on_send();
    let tokio_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run(&broker_t.pulsar_url(), &trace),
    )
    .await
    .expect("tokio leg must not hang on the post-terminal send")
    .expect("tokio runner");
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.inject_decode_fatal_frame_on_send();
    let moonpool_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_moonpool::run(&broker_m.host_port(), &trace),
    )
    .await
    .expect("moonpool leg must not hang on the post-terminal send")
    .expect("moonpool runner");
    broker_m.shutdown().await;

    // ── Equivalence claim: both engines surface TWO identical peer-closed
    // send outcomes (the in-flight one + the post-terminal new op). ──
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the new-op terminal trace {trace:?}",
    );
    assert_eq!(
        tokio_stream.events.len(),
        2,
        "both the in-flight and the post-terminal send must surface an event",
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
        "both the in-flight send AND the post-terminal new send must surface \
         the terminal peer-closed outcome on both engines (ADR-0059)",
    );
}
