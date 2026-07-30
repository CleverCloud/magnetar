// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d) for the ADR-0085 hardening: the tokio and
//! moonpool health probes must **agree** on which endpoint strings they refuse.
//!
//! # What this test proves
//!
//! `TokioHealthProbe::authority` and `MoonpoolHealthProbe::authority` were
//! byte-identical copies of the same parser, and rotted identically: an input
//! carrying `"://"` with an unrecognised scheme (a bit-flipped
//! `"ptlsar://host:6650"`) fell through to the unstripped string and was
//! truncated into the nonsense authority `"ptlsar:"`, which each engine then
//! dialled. Both now delegate to [`magnetar_proto::probe_authority`], so
//! agreement is structural rather than coincidental — and this test pins it, so
//! a future engine that re-forks its own parser surfaces here.
//!
//! # Why the verdict alone is not the witness
//!
//! Both engines returned `false` for the corrupted endpoint BEFORE the fix too
//! (the fabricated authority simply failed to resolve / connect). A test
//! asserting only `poll_probe == false` would be green on both sides of the fix
//! and prove nothing.
//!
//! The discriminating observable is **whether I/O was attempted**, witnessed by
//! the tracing event each engine emits. This test therefore asserts, for each
//! engine:
//!
//! 1. **corrupted scheme** → `Ready(false)` **via** the `cannot parse endpoint` event, with no `DNS
//!    lookup failed` / `connect failed` event. Red pre-fix on both engines.
//! 2. **bare `host:port` on a closed local port** → `Ready(false)` **via** the I/O-failure event,
//!    and NOT via `cannot parse endpoint`. This is the guard against over-correction: the fix must
//!    not turn the legitimate scheme-less form into a parse rejection.
//!
//! Assertion (2) is what makes (1) meaningful — together they pin the exact
//! boundary the fix moved, in both directions, on both engines.
//!
//! # Scope note
//!
//! Unlike `corrupted_broker_scheme_equivalence.rs` — which had to settle for a
//! proto-decode invariant because the fix it accompanied was moonpool-only —
//! this test asserts the changed behaviour directly, because here both engines
//! genuinely share the code path. No broker is involved: a refused endpoint
//! never reaches a socket, and the control case dials a closed loopback port.

#![forbid(unsafe_code)]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use magnetar_runtime_moonpool::auto_cluster_failover::MoonpoolHealthProbe;
use magnetar_runtime_tokio::auto_cluster_failover::TokioHealthProbe;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;

/// A single-bit corruption of the `pulsar` scheme word — the exact shape
/// moonpool-sim's bit-flip chaos produced for issue #364.
const CORRUPTED_SCHEME_ENDPOINT: &str = "ptlsar://broker-sim.proxy.internal:6650";

/// In-memory `MakeWriter` sink — same shape as
/// `tests/corrupted_frame_equivalence.rs`.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }

    fn clear(&self) {
        self.0.lock().clear();
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

/// What one probe run observed: the verdict plus which branch logged it.
#[derive(Debug, PartialEq, Eq)]
struct ProbeObservation {
    verdict: bool,
    /// The endpoint was refused before any I/O (`cannot parse endpoint`).
    refused_at_parse: bool,
    /// The engine dialled and the dial failed (`DNS lookup failed`,
    /// `TCP connect failed`, `connect failed`, or `connect timed out`).
    attempted_io: bool,
}

fn classify(logs: &str, verdict: bool) -> ProbeObservation {
    ProbeObservation {
        verdict,
        refused_at_parse: logs.contains("cannot parse endpoint"),
        attempted_io: logs.contains("DNS lookup failed")
            || logs.contains("TCP connect failed")
            || logs.contains("connect failed")
            || logs.contains("connect timed out"),
    }
}

/// Bind an ephemeral port, capture it, then drop the listener so nothing
/// answers — the control endpoint for assertion (2).
fn closed_local_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local_addr").port()
}

fn probe_tokio(endpoint: &str, sink: &CaptureWriter) -> ProbeObservation {
    sink.clear();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let probe = TokioHealthProbe::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let verdict =
        rt.block_on(async { poll_fn(|cx| probe.poll_probe(endpoint, deadline, cx)).await });
    classify(&sink.contents(), verdict)
}

fn probe_moonpool(endpoint: &str, sink: &CaptureWriter) -> ProbeObservation {
    sink.clear();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let verdict = rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let probe = MoonpoolHealthProbe::new(TokioProviders::new());
                let deadline = Instant::now() + Duration::from_secs(5);
                poll_fn(|cx| probe.poll_probe(endpoint, deadline, cx)).await
            })
            .await
    });
    classify(&sink.contents(), verdict)
}

/// One test, one global subscriber, both engines — the runs are sequential and
/// the sink is cleared between them, so each engine's classification is read
/// from only its own output.
#[test]
fn both_engines_agree_on_refusing_a_corrupted_scheme_endpoint() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    // (1) Corrupted scheme — refused at parse time, no I/O, on BOTH engines.
    let corrupted_tokio = probe_tokio(CORRUPTED_SCHEME_ENDPOINT, &sink);
    let corrupted_moonpool = probe_moonpool(CORRUPTED_SCHEME_ENDPOINT, &sink);

    let expected_refusal = ProbeObservation {
        verdict: false,
        refused_at_parse: true,
        attempted_io: false,
    };
    assert_eq!(
        corrupted_tokio, expected_refusal,
        "tokio must refuse '{CORRUPTED_SCHEME_ENDPOINT}' at parse time, without dialling",
    );
    assert_eq!(
        corrupted_moonpool, expected_refusal,
        "moonpool must refuse '{CORRUPTED_SCHEME_ENDPOINT}' at parse time, without dialling",
    );
    assert_eq!(
        corrupted_tokio, corrupted_moonpool,
        "both engines must treat a corrupted scheme identically",
    );

    // (2) Control: a legitimate scheme-less `host:port` must still be PARSED
    // and dialled — the fix must not over-correct into refusing it. The port
    // is closed, so the dial fails and the verdict is still `false`; what
    // separates this from case (1) is WHICH branch produced the `false`.
    let bare = format!("127.0.0.1:{}", closed_local_port());
    let bare_tokio = probe_tokio(&bare, &sink);
    let bare_moonpool = probe_moonpool(&bare, &sink);

    let expected_dial_failure = ProbeObservation {
        verdict: false,
        refused_at_parse: false,
        attempted_io: true,
    };
    assert_eq!(
        bare_tokio, expected_dial_failure,
        "tokio must still parse and dial a bare host:port ({bare})",
    );
    assert_eq!(
        bare_moonpool, expected_dial_failure,
        "moonpool must still parse and dial a bare host:port ({bare})",
    );
    assert_eq!(
        bare_tokio, bare_moonpool,
        "both engines must treat a bare host:port identically",
    );

    // The two cases must be distinguishable — if they collapse, the witness
    // has stopped discriminating and the assertions above are vacuous.
    assert_ne!(
        corrupted_tokio, bare_tokio,
        "refused-at-parse and dialled-and-failed must remain distinguishable",
    );
}
