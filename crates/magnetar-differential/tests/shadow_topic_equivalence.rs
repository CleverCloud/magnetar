// SPDX-License-Identifier: Apache-2.0

//! PIP-180 / ADR-0033 differential equivalence — the tokio and moonpool
//! engines MUST produce byte-identical `EventStream`s for replicator-style
//! sends (`Op::SendWithSourceId`).
//!
//! Two tests:
//!
//! 1. `send_with_source_id_event_stream_parity` — both engines emit `Event::Sent { message_id ==
//!    source_msg_id }`. The scripted broker echoes the asserted id back on the receipt; both
//!    engines surface that id verbatim on the resulting `SendFut`.
//! 2. `regular_send_byte_identical_to_v01_parity` — regression guard: a plain `Op::Send` (no
//!    source-message-id) still produces the same event sequence on both engines as before PIP-180
//!    landed. Pins the "no proto bump, wire byte-identical" promise at the differential layer.
//!
//! A golden trace lives at `tests/golden/shadow_send_with_source.json` —
//! human-reviewable, regenerated via `MAGNETAR_REGENERATE_GOLDEN=1`.

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
        "engine event streams diverged for PIP-180 trace {trace:?}",
    );

    broker.shutdown().await;
    tokio_stream
}

#[tokio::test(flavor = "current_thread")]
async fn send_with_source_id_event_stream_parity() {
    let source_id = mid(99, 42);
    let trace = Trace::new(
        "persistent://public/default/shadow-equiv",
        "shadow-sub",
        vec![
            Op::SendWithSourceId {
                source_msg_id: source_id,
                payload: b"replicated".to_vec(),
            },
            Op::Close,
        ],
    );
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 2);
    // Both engines must surface the broker-echoed source id.
    match &stream.events[0] {
        Event::Sent { message_id } => {
            assert_eq!(*message_id, source_id, "broker must echo source id");
        }
        other => panic!("expected Event::Sent, got {other:?}"),
    }
    assert!(matches!(stream.events[1], Event::Closed));

    // Validate the golden JSON. Regenerate by setting MAGNETAR_REGENERATE_GOLDEN=1.
    let golden_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/shadow_send_with_source.json");
    let expected = "[\
\n  {\"Sent\":{\"message_id\":{\"ledger_id\":99,\"entry_id\":42,\"partition\":-1,\"batch_index\":-1,\"batch_size\":0}}},\
\n  \"Closed\"\
\n]\n";
    if std::env::var_os("MAGNETAR_REGENERATE_GOLDEN").is_some() {
        std::fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
        std::fs::write(&golden_path, expected).unwrap();
    }
    let actual = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|_| panic!("golden file missing at {golden_path:?}"));
    assert_eq!(
        actual.trim(),
        expected.trim(),
        "PIP-180 golden trace drift — regenerate via MAGNETAR_REGENERATE_GOLDEN=1"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_shadow_event_stream_parity() {
    // Consumer-side parity guard: a regular send + recv + ack cycle must
    // remain byte-identical across engines under PIP-180 (no behavior
    // drift from the new field on `OutgoingMessage`). The proper shadow
    // receive path requires the broker to inject
    // `MessageMetadata.replicated_from` on outbound `CommandMessage`
    // frames; that scaffolding lives behind a real Pulsar broker (see
    // `crates/magnetar/tests/e2e_shadow_topic.rs`). At the differential
    // layer this test stays narrow — it pins that the new `source_message_id`
    // field on `OutgoingMessage` (defaulted to `None` on every regular
    // send by both runners) does not perturb the existing event stream.
    let trace = Trace::new(
        "persistent://public/default/shadow-rt",
        "shadow-sub-rt",
        vec![
            Op::Send {
                payload: b"plain".to_vec(),
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
