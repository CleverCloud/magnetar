// SPDX-License-Identifier: Apache-2.0

//! ADR-0053 — OpenTelemetry context propagation differential equivalence.
//!
//! Pins the invariant that both engines produce byte-identical
//! `EventStream`s on a regular send → recv → ack cycle.
//! Neither differential runner injects `OTel` context (injection is
//! façade-level, tokio-only per ADR-0053), so both event streams MUST
//! be clean of `traceparent`/`tracestate` properties — the test
//! catches any accidental injection leak at the runner level.
//!
//! The companion layers are:
//! - `crates/magnetar-proto/src/conn.rs` (property round-trip unit test)
//! - `crates/magnetar-runtime-tokio/tests/otel_context_propagation.rs`
//! - `crates/magnetar-runtime-moonpool/tests/otel_context_propagation.rs`
//! - `crates/magnetar/tests/e2e_otel_context_propagation.rs`

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;

fn mid(ledger: u64, entry: u64) -> MessageId {
    MessageId {
        ledger_id: ledger,
        entry_id: entry,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    }
}

async fn assert_equivalent(trace: &Trace) -> magnetar_differential::EventStream {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, trace)
        .await
        .expect("tokio runner");
    let moonpool_stream = runner_moonpool::run(&host_port, trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for OTel-context trace {trace:?}",
    );

    broker.shutdown().await;
    tokio_stream
}

/// Both engines produce identical event streams on a send → recv → ack
/// cycle. Pins the invariant that neither runner injects `OTel` properties
/// (injection lives at the façade layer per ADR-0053).
#[tokio::test(flavor = "current_thread")]
async fn send_recv_ack_event_stream_parity_no_otel_leak() {
    let trace = Trace::new(
        "persistent://public/default/otel-equiv-t",
        "otel-equiv-sub",
        vec![
            Op::Send {
                payload: b"hello-otel-equiv".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(2),
            },
            Op::Ack {
                message_id: mid(1, 0),
            },
            Op::Close,
        ],
    );
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 4);
    assert!(matches!(stream.events[0], Event::Sent { .. }));
    assert!(matches!(stream.events[1], Event::Received { .. }));
    assert!(matches!(stream.events[2], Event::Acked));
    assert!(matches!(stream.events[3], Event::Closed));
}
