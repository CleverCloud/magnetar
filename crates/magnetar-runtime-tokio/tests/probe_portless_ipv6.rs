// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (b) for the ADR-0087 unification: a health-probe endpoint
//! that is a **port-less bracketed IPv6 literal** (`pulsar://[::1]`) must
//! resolve to the scheme's default port, so the probe gets as far as dialling
//! `[::1]:6650` instead of handing the port-less `[::1]` to
//! [`tokio::net::lookup_host`], which rejects it as an invalid socket address.
//!
//! # Why the verdict alone cannot witness this fix
//!
//! Nothing normally listens on `[::1]:6650`, so [`TokioHealthProbe`] reports
//! `false` on both sides of the fix — `assert!(!verdict)` proves nothing here,
//! exactly as in `probe_corrupted_scheme.rs`.
//!
//! What changed is **how far the probe got**, witnessed by which tracing event
//! it emitted. Before ADR-0087 the default-port synthesis triggered on "the
//! authority contains no `:`" — never true of a bracketed IPv6 literal, whose
//! colons belong to the address — so `probe_authority` returned `Some("[::1]")`
//! and the probe died at name resolution with
//! `"TokioHealthProbe: DNS lookup failed"` carrying `authority=[::1]`. After it,
//! the authority resolves and the probe reaches the connect.
//!
//! So the red/green witness is the **absence** of `DNS lookup failed`: present
//! pre-fix, impossible post-fix for a bracketed literal with a port.
//!
//! # Why this is hermetic — no listener, no IPv6 connectivity, no fixed port
//!
//! The assertions never depend on the dial succeeding:
//!
//! - `cannot parse endpoint` must be absent — the fix appends a port, it does not start refusing
//!   bracketed literals.
//! - `DNS lookup failed` must be absent — this is the pre-fix failure mode and the actual
//!   regression witness.
//! - Whichever way the dial then went is checked on its own terms: a refused connect (the ordinary
//!   case) must name `addr=[::1]:6650`, proving the synthesised port is what got dialled; a
//!   successful connect (something *is* bound there on a dev box) must report `verdict == true`.
//!   Both outcomes are consistent with the fix, and neither can be reached at all without it.
//!
//! A bracketed literal needs no DNS, and a host with IPv6 loopback disabled
//! fails the connect with a different `errno` while emitting the same event. So
//! this test binds nothing and races nothing.
//!
//! # Why this is a dedicated test binary
//!
//! It installs a **global** capturing subscriber, mirroring
//! `tests/probe_corrupted_scheme.rs` and `tests/logging_no_secrets.rs`. Sharing
//! a binary with the other probe tests would let `tracing`'s per-callsite
//! interest cache be primed by a sibling running with no subscriber installed,
//! which makes the assertions racy. One binary, one subscriber, one test.
//!
//! The moonpool engine carries the identically-shaped
//! `tests/probe_portless_ipv6.rs` (ADR-0024 layer (c)), and
//! `magnetar-differential/tests/broker_authority_ipv6_equivalence.rs` pins that
//! both engines agree (layer (d)).

#![forbid(unsafe_code)]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use magnetar_runtime_tokio::auto_cluster_failover::TokioHealthProbe;
use parking_lot::Mutex;

/// A port-less bracketed IPv6 loopback literal — the shape ADR-0085 recorded as
/// an accepted limitation and ADR-0087 closed.
const PORTLESS_IPV6_ENDPOINT: &str = "pulsar://[::1]";

/// The address the synthesised port must produce once the authority resolves.
const EXPECTED_ADDR: &str = "addr=[::1]:6650";

/// In-memory `MakeWriter` sink so the test can read back what the probe logged.
/// Same shape as the one in `tests/probe_corrupted_scheme.rs`.
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
fn tokio_probe_portless_ipv6_endpoint_resolves_the_scheme_default_port() {
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

    let probe = TokioHealthProbe::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let verdict = rt.block_on(async {
        poll_fn(|cx| probe.poll_probe(PORTLESS_IPV6_ENDPOINT, deadline, cx)).await
    });

    let logs = sink.contents();

    assert!(
        !logs.contains("cannot parse endpoint"),
        "'{PORTLESS_IPV6_ENDPOINT}' must parse to a dialable authority, not be refused — \
         ADR-0087 appends a port to bracketed literals, it does not reject them; \
         captured logs:\n{logs}",
    );
    assert!(
        !logs.contains("DNS lookup failed"),
        "'{PORTLESS_IPV6_ENDPOINT}' must resolve: pre-ADR-0087 the probe derived the port-less \
         authority '[::1]' and died here with `authority=[::1]`, which is exactly the \
         regression this asserts against; captured logs:\n{logs}",
    );

    if logs.contains("TCP connect failed") {
        assert!(
            logs.contains(EXPECTED_ADDR),
            "the refused dial must name '{EXPECTED_ADDR}', proving the synthesised default \
             port is what got dialled; captured logs:\n{logs}",
        );
    } else {
        assert!(
            verdict,
            "the probe neither logged a failed connect nor reported healthy — it must have \
             reached the dial for '{PORTLESS_IPV6_ENDPOINT}'; captured logs:\n{logs}",
        );
    }
}
