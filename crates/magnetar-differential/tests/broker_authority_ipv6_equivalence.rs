// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d) for the ADR-0087 unification: the tokio and moonpool
//! health probes must **agree** on what a port-less bracketed IPv6 endpoint
//! resolves to.
//!
//! # What this test proves
//!
//! ADR-0085 moved the endpoint parse into
//! [`magnetar_proto::probe_authority`] but carried one limitation forward
//! verbatim: default-port synthesis triggered on "the authority contains no
//! `:`", which is never true of a bracketed IPv6 literal, so `pulsar://[::1]`
//! got no port and every dialer rejected it. ADR-0087 closed that in the shared
//! helper, so both engines gained the fix in the same commit and neither can
//! lose it independently.
//!
//! This test pins that agreement from the outside, so a future engine that
//! re-forks its own parser — the failure mode ADR-0085 was written about —
//! surfaces here rather than in production.
//!
//! # Why the verdict alone is not the witness
//!
//! Both engines returned `false` for `pulsar://[::1]` before the fix (the
//! port-less authority does not resolve) and both normally return `false` after
//! it (nothing listens on `[::1]:6650`). A test asserting only
//! `poll_probe == false` is green on both sides and proves nothing — the same
//! trap `probe_authority_equivalence.rs` documents.
//!
//! The discriminating observable is **how far the probe got**: pre-fix it died
//! at name resolution on a port-less authority; post-fix it reaches a dial
//! against the synthesised port. Each engine names that target on its
//! I/O-failure event — tokio as `addr=`, moonpool as `authority=` — so the
//! classification is per-engine and the comparison happens on the normalised
//! shape below, exactly as `corrupted_broker_scheme_equivalence.rs` does with
//! its `RejectionShape`.
//!
//! # Why this is hermetic
//!
//! Nothing is bound and no fixed port is claimed. A bracketed literal needs no
//! DNS. If the dial is refused (the ordinary case) the engine logs the target it
//! tried; if something *is* bound on `[::1]:6650` on a developer box the verdict
//! is simply `true`. Both count as "reached the dial", both are impossible
//! without the fix, and — the actual assertion — both engines must report the
//! same one.
//!
//! # Scope note
//!
//! Assertion (2) is the guard against over-correction: teaching the parser about
//! brackets must not loosen the scheme rule ADR-0085 added, so a corrupted
//! scheme must still be refused before any I/O, on both engines.

#![forbid(unsafe_code)]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use magnetar_runtime_moonpool::auto_cluster_failover::MoonpoolHealthProbe;
use magnetar_runtime_tokio::auto_cluster_failover::TokioHealthProbe;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;

/// A port-less bracketed IPv6 loopback literal — the shape ADR-0085 recorded as
/// an accepted limitation and ADR-0087 closed.
const PORTLESS_IPV6_ENDPOINT: &str = "pulsar://[::1]";

/// The dial target the `pulsar://` default port must produce.
const EXPECTED_TARGET: &str = "[::1]:6650";

/// A single-bit corruption of the `pulsar` scheme word — the control input for
/// assertion (2), and the exact shape moonpool-sim's bit-flip chaos produced for
/// issue #364.
const CORRUPTED_SCHEME_ENDPOINT: &str = "ptlsar://broker-sim.proxy.internal:6650";

/// In-memory `MakeWriter` sink — same shape as
/// `tests/probe_authority_equivalence.rs`.
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

/// How far one probe run got, normalised across the two engines' differing log
/// field names.
#[derive(Debug, PartialEq, Eq)]
struct ProbeObservation {
    /// The endpoint was refused before any I/O (`cannot parse endpoint`).
    refused_at_parse: bool,
    /// The probe reached a dial against the expected target — either it
    /// connected (`verdict`), or the failure it logged names that target.
    reached_expected_dial: bool,
}

/// Classify a tokio run. Tokio names the resolved socket address as `addr` on
/// its connect-failure event, and the pre-fix failure mode was
/// `DNS lookup failed` on the port-less authority — which never mentions
/// [`EXPECTED_TARGET`], so it classifies as "did not reach the dial".
fn classify_tokio(logs: &str, verdict: bool) -> ProbeObservation {
    ProbeObservation {
        refused_at_parse: logs.contains("cannot parse endpoint"),
        reached_expected_dial: verdict || logs.contains(&format!("addr={EXPECTED_TARGET}")),
    }
}

/// Classify a moonpool run. Moonpool dials the authority string directly and
/// names it as `authority` on both its connect-failure and connect-timeout
/// events.
fn classify_moonpool(logs: &str, verdict: bool) -> ProbeObservation {
    ProbeObservation {
        refused_at_parse: logs.contains("cannot parse endpoint"),
        reached_expected_dial: verdict || logs.contains(&format!("authority={EXPECTED_TARGET}")),
    }
}

fn probe_tokio(endpoint: &str, sink: &CaptureWriter) -> (String, bool) {
    sink.clear();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let probe = TokioHealthProbe::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let verdict =
        rt.block_on(async { poll_fn(|cx| probe.poll_probe(endpoint, deadline, cx)).await });
    (sink.contents(), verdict)
}

fn probe_moonpool(endpoint: &str, sink: &CaptureWriter) -> (String, bool) {
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
    (sink.contents(), verdict)
}

/// One test, one global subscriber, both engines — the runs are sequential and
/// the sink is cleared between them, so each engine's classification is read
/// from only its own output.
#[test]
fn both_engines_agree_on_the_portless_ipv6_default_port() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    // (1) Port-less bracketed IPv6 — parsed, and dialled against the
    // synthesised default port, on BOTH engines.
    let (logs, verdict) = probe_tokio(PORTLESS_IPV6_ENDPOINT, &sink);
    let ipv6_tokio = classify_tokio(&logs, verdict);
    let tokio_logs = logs;
    let (logs, verdict) = probe_moonpool(PORTLESS_IPV6_ENDPOINT, &sink);
    let ipv6_moonpool = classify_moonpool(&logs, verdict);
    let moonpool_logs = logs;

    let expected_reached = ProbeObservation {
        refused_at_parse: false,
        reached_expected_dial: true,
    };
    assert_eq!(
        ipv6_tokio, expected_reached,
        "tokio must resolve '{PORTLESS_IPV6_ENDPOINT}' to '{EXPECTED_TARGET}' and dial it — \
         pre-ADR-0087 it derived the port-less '[::1]' and died at name resolution; \
         captured logs:\n{tokio_logs}",
    );
    assert_eq!(
        ipv6_moonpool, expected_reached,
        "moonpool must resolve '{PORTLESS_IPV6_ENDPOINT}' to '{EXPECTED_TARGET}' and dial it; \
         captured logs:\n{moonpool_logs}",
    );
    assert_eq!(
        ipv6_tokio, ipv6_moonpool,
        "both engines must treat a port-less bracketed IPv6 endpoint identically",
    );

    // (2) Control: teaching the parser about brackets must NOT loosen the
    // scheme rule. A corrupted scheme is still refused before any I/O, on both
    // engines — this is what keeps assertion (1) from being an over-correction.
    let (logs, verdict) = probe_tokio(CORRUPTED_SCHEME_ENDPOINT, &sink);
    let corrupted_tokio = classify_tokio(&logs, verdict);
    let (logs, verdict) = probe_moonpool(CORRUPTED_SCHEME_ENDPOINT, &sink);
    let corrupted_moonpool = classify_moonpool(&logs, verdict);

    let expected_refusal = ProbeObservation {
        refused_at_parse: true,
        reached_expected_dial: false,
    };
    assert_eq!(
        corrupted_tokio, expected_refusal,
        "tokio must still refuse '{CORRUPTED_SCHEME_ENDPOINT}' at parse time",
    );
    assert_eq!(
        corrupted_moonpool, expected_refusal,
        "moonpool must still refuse '{CORRUPTED_SCHEME_ENDPOINT}' at parse time",
    );
    assert_eq!(
        corrupted_tokio, corrupted_moonpool,
        "both engines must treat a corrupted scheme identically",
    );

    // The two cases must stay distinguishable — if they collapse, the witness
    // has stopped discriminating and the assertions above are vacuous.
    assert_ne!(
        ipv6_tokio, corrupted_tokio,
        "reached-the-dial and refused-at-parse must remain distinguishable",
    );
}
