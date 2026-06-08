// SPDX-License-Identifier: Apache-2.0

//! Drop + redial reconnect equivalence (docs/follow-ups.md §4.2 — ADR-0024
//! layer d for the re-attach replay fix).
//!
//! The scripted broker is armed with
//! [`ScriptedBroker::drop_connection_after`], so it closes the socket mid
//! scenario after writing a fixed number of frames, forcing the supervised
//! client to redial. The knob also switches the broker into **resume mode**:
//! the ledger, per-topic entry-id sequence, and durable per-subscription
//! cursor live in the cross-session store, so the replayed in-flight publish
//! and the re-subscribe after the redial resume from the acked position
//! instead of starting fresh (ADR-0055 §3 shape).
//!
//! Both engine legs run the SAME trace through the SUPERVISED runner
//! ([`runner_tokio::run_supervised`] / [`runner_moonpool::run_supervised`]) so
//! the auto-reconnect driver transparently recovers the drop. The equivalence
//! claim is the strongest the harness offers: the two [`EventStream`]s must
//! compare equal **in order**, not merely as a set — a redial that resumed
//! from the wrong cursor (re-delivering an acked message, or skipping the
//! un-acked tail) would reorder or drop an event and the streams would
//! diverge.
//!
//! # Why its own integration-test binary with ONE test fn
//!
//! Mirrors `corrupted_frame_equivalence.rs` / `terminal_error_equivalence.rs`:
//! a single self-contained differential scenario per binary keeps the harness
//! wiring obvious and the per-leg `timeout` budgets local.

#![forbid(unsafe_code)]

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::{MessageId, SupervisorConfig};

/// Build a message id with default partition/batch fields so the trace can
/// spell ids tersely (mirrors `tests/golden_traces.rs`).
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

/// Frames the broker writes before it drops the session. The handshake +
/// first round-trip emits, in this exact order (verified identical on both
/// engine legs — the differential parity assertion below would catch any
/// drift):
///
/// 1. `CommandConnected` (Connect reply)
/// 2. `CommandLookupTopicResponse` (producer's Lookup reply)
/// 3. `CommandProducerSuccess` (Producer open)
/// 4. `CommandSendReceipt` (first send — entry `(1, 0)`)
/// 5. `CommandLookupTopicResponse` (consumer's Lookup reply, on first Recv)
/// 6. `CommandSuccess` (Subscribe)
/// 7. pushed `CommandMessage` (first recv delivers `(1, 0)`)
/// 8. `CommandAckResponse` (first ack — advances the durable cursor to 1)
///
/// Dropping after frame 8 closes the connection once the first message is
/// durably acked, so the SECOND send is issued across the redial and the
/// re-subscribe must resume from cursor 1 — redelivering nothing already
/// acked, delivering only the new entry `(1, 1)`.
const DROP_AFTER_FRAMES: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_redial_replay_is_equivalent_across_engines() {
    // Send A → Recv A → Ack A   (broker drops right after A's ack response)
    // Send B → Recv B → Ack B   (replayed / resumed on the redialled session)
    // Close.
    //
    // The durable cursor advances to 1 on Ack A, so after the redial the
    // re-subscribe resumes from entry 1: Recv B must surface `(1, 1)`, the
    // NEW message — never a re-delivery of the already-acked `(1, 0)`.
    let trace = Trace::new(
        "persistent://public/default/diff-drop-redial",
        "sub-drop-redial",
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
    broker_t.drop_connection_after(DROP_AFTER_FRAMES);
    let tokio_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run_supervised(&broker_t.pulsar_url(), &trace, supervisor()),
    )
    .await
    .expect("tokio leg must not hang across the drop + redial")
    .expect("tokio runner");
    broker_t.shutdown().await;

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.drop_connection_after(DROP_AFTER_FRAMES);
    let moonpool_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_moonpool::run_supervised(&broker_m.host_port(), &trace, supervisor()),
    )
    .await
    .expect("moonpool leg must not hang across the drop + redial")
    .expect("moonpool runner");
    broker_m.shutdown().await;

    // ── Equivalence claim: identical event ORDER across engines. ──
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the drop + redial trace {trace:?}",
    );

    // Pin the resume semantics so a future regression that silently breaks the
    // durable-cursor resume (e.g. re-delivering the acked head, or losing the
    // un-acked tail) fails LOUDLY rather than merely diverging.
    let events = &tokio_stream.events;
    assert_eq!(events.len(), 7, "one event per op");
    assert!(
        matches!(&events[0], Event::Sent { message_id } if *message_id == mid(1, 0)),
        "first send → broker entry (1, 0); got {:?}",
        events[0]
    );
    assert!(
        matches!(&events[1], Event::Received { message_id, payload }
            if *message_id == mid(1, 0) && payload == b"before-drop"),
        "first recv delivers the pre-drop message; got {:?}",
        events[1]
    );
    assert!(
        matches!(events[2], Event::Acked),
        "first ack; got {:?}",
        events[2]
    );
    assert!(
        matches!(&events[3], Event::Sent { message_id } if *message_id == mid(1, 1)),
        "second send resumes the entry-id sequence → (1, 1) on the redialled \
         session (de-duplicated by (topic, sequence_id) on replay); got {:?}",
        events[3]
    );
    assert!(
        matches!(&events[4], Event::Received { message_id, payload }
            if *message_id == mid(1, 1) && payload == b"after-redial"),
        "second recv resumes from the durable cursor (1): it delivers ONLY the \
         new entry (1, 1), never a re-delivery of the acked (1, 0); got {:?}",
        events[4]
    );
    assert!(
        matches!(events[5], Event::Acked),
        "second ack; got {:?}",
        events[5]
    );
    assert!(
        matches!(events[6], Event::Closed),
        "close; got {:?}",
        events[6]
    );
}
