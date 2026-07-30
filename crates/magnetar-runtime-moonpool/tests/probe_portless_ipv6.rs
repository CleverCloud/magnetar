// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (c) for the ADR-0087 unification — the moonpool twin of
//! `magnetar-runtime-tokio/tests/probe_portless_ipv6.rs`.
//!
//! A health-probe endpoint that is a **port-less bracketed IPv6 literal**
//! (`pulsar://[::1]`) must resolve to the scheme's default port, so
//! [`moonpool_core::NetworkProvider::connect`] is handed `[::1]:6650` rather
//! than the port-less `[::1]` it cannot turn into an address.
//!
//! # Why the verdict alone cannot witness this fix
//!
//! Nothing normally listens on `[::1]:6650`, so the probe reports `false` on
//! both sides of the fix — `assert!(!verdict)` proves nothing, exactly as in
//! `tests/probe_corrupted_scheme.rs`.
//!
//! What changed is the **authority the probe derived**, emitted as a structured
//! field on `"MoonpoolHealthProbe: connect failed"`. Before ADR-0087 the
//! default-port synthesis triggered on "the authority contains no `:`" — never
//! true of a bracketed IPv6 literal, whose colons belong to the address — so
//! `probe_authority` returned `Some("[::1]")` and that port-less string is what
//! got dialled. The witness is `authority=[::1]:6650`.
//!
//! # Why this is hermetic — no listener, no IPv6 connectivity, no fixed port
//!
//! The assertions never depend on the dial succeeding:
//!
//! - `cannot parse endpoint` must be absent — the fix appends a port, it does not start refusing
//!   bracketed literals.
//! - A failed dial (the ordinary case) must carry `authority=[::1]:6650`. Pre-fix this same branch
//!   logged `authority=[::1]`, so it is the red/green witness.
//! - A successful dial (something *is* bound there on a dev box) must report `verdict == true`.
//!   Both outcomes are consistent with the fix; neither is reachable without it.
//!
//! A bracketed literal needs no DNS, and a host with IPv6 loopback disabled
//! fails the connect with a different `errno` while logging the same authority.
//! So this test binds nothing and races nothing.
//!
//! # Why this is a dedicated test binary
//!
//! It installs a **global** capturing subscriber, mirroring
//! `tests/probe_corrupted_scheme.rs` and `tests/logging_no_secrets.rs`. Sharing
//! a binary with the other probe tests would let `tracing`'s per-callsite
//! interest cache be primed by a sibling running with no subscriber installed,
//! which makes the assertions racy. One binary, one subscriber, one test.
//!
//! `TokioProviders` (not `SimProviders`) is deliberate and matches
//! `tests/probe_corrupted_scheme.rs`: this pins the authority-derivation
//! contract on the production-shaped provider path. No virtual clock is
//! involved — the dial resolves long before the timer matters.

#![forbid(unsafe_code)]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};

use magnetar_proto::HealthProbe;
use magnetar_runtime_moonpool::auto_cluster_failover::MoonpoolHealthProbe;
use moonpool_core::TokioProviders;
use parking_lot::Mutex;

/// A port-less bracketed IPv6 loopback literal — the shape ADR-0085 recorded as
/// an accepted limitation and ADR-0087 closed.
const PORTLESS_IPV6_ENDPOINT: &str = "pulsar://[::1]";

/// The authority the probe must derive: the `pulsar://` scheme's default port
/// appended after the closing bracket.
const EXPECTED_AUTHORITY: &str = "authority=[::1]:6650";

/// The pre-ADR-0087 authority, asserted absent so a revert cannot slip through
/// on a substring match — `[::1]:6650` contains `[::1]`, so checking only for
/// the expected value would also be satisfied by the wrong one. The trailing
/// space is the field separator `tracing`'s fmt layer emits before the next
/// field (`error=`), which pins the value as complete.
const PRE_FIX_AUTHORITY: &str = "authority=[::1] ";

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
fn moonpool_probe_portless_ipv6_endpoint_dials_the_scheme_default_port() {
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
    // sibling probe tests use.
    let verdict = rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let probe = MoonpoolHealthProbe::new(TokioProviders::new());
                let deadline = Instant::now() + Duration::from_secs(5);
                poll_fn(|cx| probe.poll_probe(PORTLESS_IPV6_ENDPOINT, deadline, cx)).await
            })
            .await
    });

    let logs = sink.contents();

    assert!(
        !logs.contains("cannot parse endpoint"),
        "'{PORTLESS_IPV6_ENDPOINT}' must parse to a dialable authority, not be refused — \
         ADR-0087 appends a port to bracketed literals, it does not reject them; \
         captured logs:\n{logs}",
    );

    if logs.contains("connect failed") || logs.contains("connect timed out") {
        assert!(
            logs.contains(EXPECTED_AUTHORITY),
            "the dial must name '{EXPECTED_AUTHORITY}', proving the synthesised default port is \
             what got dialled; pre-ADR-0087 this branch logged the port-less 'authority=[::1]'; \
             captured logs:\n{logs}",
        );
        assert!(
            !logs.contains(PRE_FIX_AUTHORITY),
            "the port-less pre-fix authority must not appear; captured logs:\n{logs}",
        );
    } else {
        assert!(
            verdict,
            "the probe neither logged a failed dial nor reported healthy — it must have reached \
             the dial for '{PORTLESS_IPV6_ENDPOINT}'; captured logs:\n{logs}",
        );
    }
}
