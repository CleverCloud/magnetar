// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (c) for the ADR-0085 hardening — the moonpool
//! twin of `magnetar-runtime-tokio/tests/probe_corrupted_scheme.rs`.
//!
//! A health-probe endpoint carrying an unrecognised URL scheme must be
//! **refused at parse time**, with no [`moonpool_core::NetworkProvider::connect`]
//! attempted against a fabricated authority.
//!
//! # Why the verdict alone cannot witness this fix
//!
//! `MoonpoolHealthProbe` reported `false` for `"ptlsar://host:6650"` BEFORE the
//! fix too — it truncated the corrupted URL to the nonsense authority
//! `"ptlsar:"`, handed that to `NetworkProvider::connect`, and the connect
//! failed. So `assert!(!verdict)` is green on both sides of the fix and proves
//! nothing.
//!
//! What actually changed is *whether I/O is attempted at all*. The witness is
//! therefore the emitted tracing event, which flips from
//! `"MoonpoolHealthProbe: connect failed"` (carrying `authority=ptlsar:`) to
//! `"MoonpoolHealthProbe: cannot parse endpoint"`. This file asserts both the
//! presence of the parse-refusal event and the absence of the connect-failure
//! one, so reverting the fix turns it red.
//!
//! # Why this is a dedicated test binary
//!
//! It installs a **global** capturing subscriber, mirroring
//! `tests/logging_no_secrets.rs` and
//! `magnetar-differential/tests/corrupted_frame_equivalence.rs`. Sharing a
//! binary with the other probe tests would let `tracing`'s per-callsite
//! interest cache be primed by a sibling test running with no subscriber
//! installed, which makes the assertions racy. One binary, one subscriber, one
//! test.
//!
//! `TokioProviders` (not `SimProviders`) is deliberate: this test pins the
//! parse-refusal contract on the production-shaped provider path, exactly as
//! the neighbouring `MoonpoolHealthProbe` tests in
//! `src/auto_cluster_failover.rs` do. No virtual clock is involved — a refused
//! endpoint never reaches the timer.

#![forbid(unsafe_code)]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use magnetar_runtime_moonpool::auto_cluster_failover::MoonpoolHealthProbe;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;

/// A single-bit corruption of the `pulsar` scheme word — the exact shape
/// moonpool-sim's bit-flip chaos produced for issue #364, and the input
/// ADR-0085 named.
const CORRUPTED_SCHEME_ENDPOINT: &str = "ptlsar://broker-sim.proxy.internal:6650";

/// In-memory `MakeWriter` sink so the test can read back what the probe
/// logged. Same shape as the one in
/// `magnetar-differential/tests/corrupted_frame_equivalence.rs`.
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

#[test]
fn moonpool_probe_corrupted_scheme_reports_unhealthy_without_connect() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    // `TokioProviders::task()` spawns onto a `LocalSet`, matching the shape the
    // in-module probe tests use.
    let verdict = rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let probe = MoonpoolHealthProbe::new(TokioProviders::new());
                let deadline = Instant::now() + Duration::from_secs(5);
                poll_fn(|cx| probe.poll_probe(CORRUPTED_SCHEME_ENDPOINT, deadline, cx)).await
            })
            .await
    });

    assert!(
        !verdict,
        "an endpoint with an unrecognised scheme must read unhealthy",
    );

    let logs = sink.contents();
    assert!(
        logs.contains("cannot parse endpoint"),
        "the probe must refuse the corrupted endpoint at parse time; captured logs:\n{logs}",
    );
    assert!(
        !logs.contains("connect failed"),
        "the probe must NOT dial a fabricated authority derived from a corrupted scheme \
         (pre-fix this logged `authority=ptlsar:`); captured logs:\n{logs}",
    );
    assert!(
        !logs.contains("connect timed out"),
        "no connect may be attempted for an unparseable endpoint; captured logs:\n{logs}",
    );
}
