// SPDX-License-Identifier: Apache-2.0

//! Corrupted-frame differential equivalence (ADR-0054 —
//! ADR-0024 layer d for the proto checksum point-of-detection diff).
//!
//! The scripted broker completes a normal `CommandConnect` →
//! `CommandConnected` handshake and then writes ONE CRC32C-corrupted frame
//! ([`ScriptedBroker::inject_corrupted_frame_after_connected`]) before any
//! lookup traffic. Both engines must behave identically:
//!
//! 1. the proto layer logs the mismatch ONCE at the point of detection (the `error!` with
//!    `computed` / `expected` fields in the decode loop) — and the engine drains the companion
//!    `ConnectionEvent::ChecksumMismatch` **silently** (single-owner rule: exactly one log line per
//!    engine leg, no duplicate engine-side record);
//! 2. the connection survives the drop ("CRC32C verify or drop", workspace invariant 4);
//! 3. subsequent traffic flows — the trace's send / recv / ack round-trip resolves and the two
//!    [`EventStream`]s compare equal byte-for-byte.
//!
//! # Why this file is its own integration-test binary with ONE test fn
//!
//! The proto `error!` fires on whatever task is driving the engine's read
//! loop, so the capturing subscriber must be **global**
//! (`fmt().init()`). A global subscriber can be installed exactly once per
//! process, so this file holds a single test fn and shares the binary with
//! no other test (same pattern as the runtime crates'
//! `tests/logging_no_secrets.rs` / `tests/logging_checksum.rs`).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;
use parking_lot::Mutex;

/// The proto point-of-detection record (`crates/magnetar-proto/src/conn.rs`
/// decode loop). Exactly one occurrence per engine leg proves the corrupted
/// frame hit the engine AND that the engine drained the companion event
/// silently (ADR-0054 single-owner rule).
const CHECKSUM_LOG: &str = "CRC32C checksum mismatch; corrupt frame dropped";

/// Shared in-memory sink for the global fmt subscriber.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Helper: build a message id with default partition/batch fields so the
/// trace can spell ids tersely (mirrors `tests/golden_traces.rs`).
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

#[tokio::test(flavor = "current_thread")]
async fn corrupted_frame_drop_is_equivalent_across_engines() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    // Send → Recv → Ack → Close AFTER the corrupted frame: resolving the
    // full round-trip is the "subsequent traffic flows" proof on each
    // engine. The corrupted frame sits between `CommandConnected` and the
    // lookup reply on the wire, so by the time the producer open inside
    // each runner resolves, the corruption has already been processed.
    let trace = Trace::new(
        "persistent://public/default/diff-corrupt-frame",
        "sub-corrupt-frame",
        vec![
            Op::Send {
                payload: b"after-corruption".to_vec(),
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

    // ── Tokio leg ──
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    broker_t.inject_corrupted_frame_after_connected();
    let tokio_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run(&broker_t.pulsar_url(), &trace),
    )
    .await
    .expect("tokio leg must not hang after the corrupt-frame drop")
    .expect("tokio runner");
    broker_t.shutdown().await;
    let tokio_hits = sink.contents().matches(CHECKSUM_LOG).count();
    assert_eq!(
        tokio_hits,
        1,
        "tokio leg: exactly one proto point-of-detection error! \
         (single-owner rule — engine drains the event silently); captured:\n{}",
        sink.contents(),
    );

    // ── Moonpool leg ──
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    broker_m.inject_corrupted_frame_after_connected();
    let moonpool_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_moonpool::run(&broker_m.host_port(), &trace),
    )
    .await
    .expect("moonpool leg must not hang after the corrupt-frame drop")
    .expect("moonpool runner");
    broker_m.shutdown().await;
    let total_hits = sink.contents().matches(CHECKSUM_LOG).count();
    assert_eq!(
        total_hits,
        2,
        "moonpool leg: exactly one additional proto point-of-detection error! \
         (single-owner rule — engine drains the event silently); captured:\n{}",
        sink.contents(),
    );

    // ── Equivalence claim: both engines survive the corrupted frame and the
    // subsequent traffic resolves identically. ──
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the corrupted-frame trace {trace:?}",
    );
    assert_eq!(tokio_stream.events.len(), 4);
    assert!(matches!(tokio_stream.events[0], Event::Sent { .. }));
    assert!(matches!(tokio_stream.events[1], Event::Received { .. }));
    assert!(matches!(tokio_stream.events[2], Event::Acked));
    assert!(matches!(tokio_stream.events[3], Event::Closed));
}
