// SPDX-License-Identifier: Apache-2.0

//! Sans-io cluster-failover health probe — Java parity for PIP-121.
//!
//! Mirrors the spirit of `org.apache.pulsar.client.api.AutoClusterFailover`'s
//! probe callback: the policy machinery (priority-ordered URL list, failover /
//! failback bookkeeping) lives in an engine crate; the probe contract itself
//! lives here so every engine can host its own implementation.
//!
//! # Why a `poll_*` shape instead of `async fn`?
//!
//! `magnetar-proto` is sans-io (see [ADR-0004]) and pulls in no async runtime.
//! A trait method returning a `Pin<Box<dyn Future>>` would either drag the
//! engine's executor concept into the proto crate or force every implementor
//! to box a future allocated against an unknown executor. Neither is
//! acceptable here.
//!
//! `quinn-proto` solved the same problem with the `poll_*` family
//! ([`Connection::poll_event`](crate::Connection::poll_event),
//! [`Connection::poll_timeout`](crate::Connection::poll_timeout)). We follow
//! the same convention: the implementor parks `cx.waker()` and returns
//! `Poll::Pending` while the probe is in flight. Engines built on tokio,
//! glommio, or moonpool implement the trait in whatever style fits their I/O
//! model; the trait surface stays runtime-agnostic.
//!
//! # Contract
//!
//! - The implementor parses the `endpoint` string (typically a Pulsar service URL such as
//!   `pulsar://broker:6650` or a `host:port` pair).
//! - `deadline` lets the runtime time-box a probe. An implementor that can honour the deadline
//!   SHOULD treat overshoot as `Ready(false)`; one that cannot honour it MAY ignore the value but
//!   is expected to make probes complete quickly (well under the policy's check interval).
//! - `cx.waker()` MUST be parked while the probe is `Pending` so the caller is re-polled when the
//!   probe resolves. Implementors that complete inline may return `Poll::Ready(...)` without
//!   touching `cx`.
//! - Probes MUST be re-entrant: the same probe instance is invoked against every URL in the
//!   priority list on every probe cycle, sometimes concurrently if the policy fans them out.
//! - A `true` outcome means the endpoint is reachable AND serving (per the implementor's definition
//!   — TCP connect, admin REST `/brokers/health`, etc.). A `false` outcome means unhealthy; the
//!   policy machinery decides what to do with the verdict.
//!
//! # See also
//!
//! - [`crate::ServiceUrlProvider`] — the sans-io provider trait the policy ultimately feeds.
//! - [ADR-0016] — PIP-121 cluster-failover decisions.
//! - [ADR-0023] — extraction of this trait into `magnetar-proto`.
//!
//! [ADR-0004]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md
//! [ADR-0016]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0016-pip-121-cluster-failover.md
//! [ADR-0023]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0023-health-probe-trait-extraction.md

use core::fmt::Debug;
use std::task::{Context, Poll};
use std::time::Instant;

/// Sans-io health probe — Java parity for the `AutoClusterFailover` probe
/// callback (PIP-121).
///
/// Implementors typically live in an engine crate (`magnetar-runtime-tokio`,
/// `magnetar-runtime-moonpool`) and bridge whatever async I/O primitive their
/// runtime exposes into the synchronous `poll_*` contract documented at the
/// module level.
///
/// # Why `Send + Sync + Debug`
///
/// The probe lives behind an [`std::sync::Arc`] shared between the policy's
/// background driver and any caller that inspects its state. `Debug` lets the
/// policy emit useful tracing without leaking implementor internals.
pub trait HealthProbe: Send + Sync + Debug {
    /// Poll the health of `endpoint`.
    ///
    /// - Returns [`Poll::Ready`]`(true)` if the endpoint is reachable and serving.
    /// - Returns [`Poll::Ready`]`(false)` if the endpoint is unhealthy or the deadline was hit.
    /// - Returns [`Poll::Pending`] while the probe is in flight; the implementor MUST register
    ///   `cx.waker()` so the caller is re-polled on completion.
    ///
    /// `deadline` is the absolute instant by which the probe should have
    /// resolved. Implementors that cannot honour it inline may still rely on
    /// the policy's outer timeout, but probes that overshoot the check
    /// interval will skew the failover bookkeeping.
    ///
    /// # Re-entrancy
    ///
    /// The same instance is invoked against multiple endpoints (and possibly
    /// the same endpoint repeatedly). Implementations must therefore key any
    /// in-flight state by the endpoint string.
    fn poll_probe(&self, endpoint: &str, deadline: Instant, cx: &mut Context<'_>) -> Poll<bool>;
}

/// Canonical parse of a [`HealthProbe`] `endpoint` string into the
/// `host:port` authority an engine hands to its dialer
/// (`tokio::net::lookup_host`, `moonpool_core::NetworkProvider::connect`, …).
///
/// Accepts exactly three shapes, mirroring the module-level contract:
///
/// | Input                              | Output                       |
/// | ---------------------------------- | ---------------------------- |
/// | `pulsar://host:port[/path…]`       | `Some("host:port")`          |
/// | `pulsar+ssl://host:port[/path…]`   | `Some("host:port")`          |
/// | `host:port` (no scheme)            | `Some("host:port")`          |
/// | `pulsar://host` (no port)          | `Some("host:6650")`          |
/// | `pulsar+ssl://host` (no port)      | `Some("host:6651")`          |
/// | `pulsar://[::1]` (no port)         | `Some("[::1]:6650")`         |
/// | anything else containing `"://"`   | `None`                       |
/// | empty authority                    | `None`                       |
///
/// # Why unrecognised schemes are rejected rather than passed through
///
/// The obvious implementation —
/// `strip_prefix("pulsar+ssl://").or_else(|| strip_prefix("pulsar://")).unwrap_or(endpoint)` —
/// silently falls through to the ORIGINAL unstripped string when the scheme
/// is neither Pulsar scheme. The subsequent `split('/').next()` then truncates
/// a bit-flipped `"ptlsar://broker:6650"` into the nonsense authority
/// `"ptlsar:"`, and the engine dials that fabricated target instead of
/// refusing the input. Both engines carried that bug verbatim until it was
/// replaced by this shared helper (ADR-0085).
///
/// Returning `None` is the safe outcome: per the [`HealthProbe`] contract an
/// endpoint that cannot be parsed is reported unhealthy, so a corrupted URL
/// costs one probe verdict and zero I/O.
///
/// # Bracketed IPv6 literals
///
/// A bracketed IPv6 literal is full of colons that belong to the *address*, so
/// "does this authority already carry a port?" cannot be answered with
/// `contains(':')` — that test reports "already ported" for `[::1]` and
/// suppresses the synthesis. The private `authority_has_explicit_port` answers
/// it properly: for a bracketed authority the port, when present, follows the
/// closing `]`. (Named without an intra-doc link on purpose — it is private, and
/// linking it from this public item trips `rustdoc::private_intra_doc_links`
/// under the workspace's `RUSTDOCFLAGS="-D warnings"`.)
///
/// Until ADR-0087 this function (and the three parsers now delegating to it)
/// shared the naive test, so `pulsar://[::1]` got no port and the dialer
/// rejected it. That was ADR-0085's one documented limitation; closing it in
/// one place closed it for every caller at once, which is the whole point of
/// the parse living here.
///
/// An **unterminated** bracket (`pulsar://[::1`) is malformed and gets no
/// synthesised port either — appending one to a string we cannot parse would
/// only fabricate a different kind of garbage.
///
/// # Sans-io
///
/// A hand-rolled scan with no `url` crate, matching the `extract_pulsar_host`
/// precedent in this crate's `conn_types` module, so [`magnetar-proto`](crate)
/// keeps its zero-I/O dependency surface ([ADR-0004]).
///
/// [ADR-0004]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md
/// [ADR-0085]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0085-probe-endpoint-parsing-in-proto.md
#[must_use]
pub fn probe_authority(endpoint: &str) -> Option<String> {
    let (rest, default_port) = if let Some(rest) = endpoint.strip_prefix("pulsar+ssl://") {
        (rest, Some(6651u16))
    } else if let Some(rest) = endpoint.strip_prefix("pulsar://") {
        (rest, Some(6650u16))
    } else if endpoint.contains("://") {
        // Unrecognised scheme — refuse rather than truncate. See the
        // "Why unrecognised schemes are rejected" section above.
        return None;
    } else {
        (endpoint, None)
    };

    // Trim trailing path segments — `pulsar://host:port/anything` becomes
    // `host:port`. Bare `host:port` round-trips unchanged.
    let host_port = rest.split('/').next().unwrap_or(rest);

    // Must precede the synthesis below, otherwise `"pulsar://"` would yield
    // the portless-host branch and produce the garbage authority `":6650"`.
    if host_port.is_empty() {
        return None;
    }

    Some(match default_port {
        Some(port) if !authority_has_explicit_port(host_port) => format!("{host_port}:{port}"),
        _ => host_port.to_owned(),
    })
}

/// Does `authority` already carry an explicit `:port`?
///
/// The naive answer — `authority.contains(':')` — is wrong for a bracketed
/// IPv6 literal, whose colons belong to the address: it reports `true` for
/// `[::1]` and so suppresses [`probe_authority`]'s default-port synthesis,
/// yielding a port-less authority every dialer rejects. In a bracketed
/// authority the port, when present, always follows the closing `]`.
///
/// An unterminated bracket (`[::1`) is malformed; report `true` so no port is
/// appended to a string we cannot parse. That keeps such input byte-identical
/// to what this function returned before the bracket handling existed — the
/// caller's dialer rejects it either way, and inventing `[::1:6650` would just
/// swap one unusable authority for another.
///
/// Not a `Host`/`Authority` type: `magnetar-proto` parses this by hand to keep
/// its zero-I/O dependency surface (see the [`probe_authority`] `# Sans-io`
/// section).
fn authority_has_explicit_port(authority: &str) -> bool {
    if authority.starts_with('[') {
        // `rfind` rather than `find`: the closing bracket of the literal is the
        // LAST one, so a nested-looking `[[::1]]` still measures from the end.
        match authority.rfind(']') {
            Some(close) => authority[close + 1..].starts_with(':'),
            None => true,
        }
    } else {
        authority.contains(':')
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    /// Counter `Waker` — `magnetar-proto` deliberately does not depend on
    /// `futures-task`, so we hand-roll the minimum needed to exercise the
    /// `Poll`-shaped trait.
    #[derive(Default)]
    struct CountingWaker {
        count: AtomicUsize,
    }

    impl CountingWaker {
        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Synchronous probe that resolves with a fixed verdict — exercises the
    /// "inline ready" branch (no waker registration).
    #[derive(Debug)]
    struct AlwaysReady(bool);

    impl HealthProbe for AlwaysReady {
        fn poll_probe(
            &self,
            _endpoint: &str,
            _deadline: Instant,
            _cx: &mut Context<'_>,
        ) -> Poll<bool> {
            Poll::Ready(self.0)
        }
    }

    /// Probe that returns `Pending` until `flip()` is called, then `Ready` —
    /// exercises the waker-park branch.
    #[derive(Debug)]
    struct FlipOnDemand {
        ready: std::sync::atomic::AtomicBool,
        last_waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    impl FlipOnDemand {
        fn new() -> Self {
            Self {
                ready: std::sync::atomic::AtomicBool::new(false),
                last_waker: std::sync::Mutex::new(None),
            }
        }

        fn flip(&self) {
            self.ready.store(true, Ordering::SeqCst);
            if let Some(w) = self.last_waker.lock().unwrap().take() {
                w.wake();
            }
        }
    }

    impl HealthProbe for FlipOnDemand {
        fn poll_probe(
            &self,
            _endpoint: &str,
            _deadline: Instant,
            cx: &mut Context<'_>,
        ) -> Poll<bool> {
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(true)
            } else {
                // Re-park the waker so the caller is re-polled when `flip()` fires.
                *self.last_waker.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn waker_with_counter() -> (std::task::Waker, Arc<CountingWaker>) {
        let cw = Arc::new(CountingWaker::default());
        let waker = std::task::Waker::from(cw.clone());
        (waker, cw)
    }

    #[test]
    fn always_ready_probe_returns_inline_without_touching_waker() {
        let probe = AlwaysReady(true);
        let (waker, counter) = waker_with_counter();
        let mut cx = Context::from_waker(&waker);
        let deadline = Instant::now() + std::time::Duration::from_secs(1);

        assert!(matches!(
            probe.poll_probe("pulsar://broker:6650", deadline, &mut cx),
            Poll::Ready(true)
        ));
        assert_eq!(counter.count(), 0, "inline-Ready probes must not wake");
    }

    #[test]
    fn always_unhealthy_probe_returns_false() {
        let probe = AlwaysReady(false);
        let (waker, _counter) = waker_with_counter();
        let mut cx = Context::from_waker(&waker);
        let deadline = Instant::now();

        assert!(matches!(
            probe.poll_probe("pulsar://broker:6650", deadline, &mut cx),
            Poll::Ready(false)
        ));
    }

    #[test]
    fn pending_probe_wakes_caller_when_completion_arrives() {
        let probe = FlipOnDemand::new();
        let (waker, counter) = waker_with_counter();
        let mut cx = Context::from_waker(&waker);
        let deadline = Instant::now() + std::time::Duration::from_secs(1);

        assert!(matches!(
            probe.poll_probe("pulsar://broker:6650", deadline, &mut cx),
            Poll::Pending
        ));
        assert_eq!(counter.count(), 0, "no completion yet — no wake");

        probe.flip();
        assert_eq!(counter.count(), 1, "flip() must wake the parked waker");

        // Subsequent poll observes the flipped state.
        assert!(matches!(
            probe.poll_probe("pulsar://broker:6650", deadline, &mut cx),
            Poll::Ready(true)
        ));
    }

    // ----- probe_authority ----------------------------------------------------

    #[test]
    fn probe_authority_strips_pulsar_scheme() {
        assert_eq!(
            probe_authority("pulsar://broker.local:6650"),
            Some("broker.local:6650".to_owned()),
        );
        assert_eq!(
            probe_authority("pulsar+ssl://broker.local:6651"),
            Some("broker.local:6651".to_owned()),
        );
    }

    #[test]
    fn probe_authority_passes_through_bare_host_port() {
        assert_eq!(
            probe_authority("127.0.0.1:6650"),
            Some("127.0.0.1:6650".to_owned()),
        );
    }

    #[test]
    fn probe_authority_trims_trailing_path() {
        assert_eq!(
            probe_authority("pulsar://broker.local:6650/admin/v2"),
            Some("broker.local:6650".to_owned()),
        );
    }

    #[test]
    fn probe_authority_rejects_empty_input() {
        assert_eq!(probe_authority(""), None);
        // A scheme with no authority behind it must reject too, NOT synthesise
        // the garbage authority `":6650"` from an empty host.
        assert_eq!(probe_authority("pulsar://"), None);
        assert_eq!(probe_authority("pulsar+ssl://"), None);
    }

    /// Regression test for ADR-0085: an input
    /// carrying `"://"` with a scheme that is neither Pulsar scheme must be
    /// REFUSED, not truncated.
    ///
    /// Before the fix both engines' `authority()` fell through to the
    /// unstripped string and `split('/')` reduced it to `"ptlsar:"`, which the
    /// probe then dialled. `"ptlsar://…"` is the exact single-bit corruption
    /// of the `pulsar` scheme word that moonpool-sim's bit-flip chaos produced
    /// for issue #364.
    #[test]
    fn probe_authority_rejects_unrecognised_scheme() {
        assert_eq!(
            probe_authority("ptlsar://broker-sim.proxy.internal:6650"),
            None,
            "a bit-flipped pulsar scheme must be refused, not truncated to 'ptlsar:'",
        );
        assert_eq!(probe_authority("http://broker:8080"), None);
        assert_eq!(probe_authority("https://broker:8443"), None);
        // Not a Pulsar scheme even though it starts with the right letters.
        assert_eq!(probe_authority("pulsarx://broker:6650"), None);
    }

    /// A scheme-carrying endpoint with no explicit port resolves to the
    /// scheme's default port instead of a portless authority the dialer would
    /// reject. Mirrors `magnetar_runtime_moonpool`'s `proxy_broker_authority`.
    #[test]
    fn probe_authority_synthesises_default_port() {
        assert_eq!(
            probe_authority("pulsar://broker.local"),
            Some("broker.local:6650".to_owned()),
        );
        assert_eq!(
            probe_authority("pulsar+ssl://broker.local"),
            Some("broker.local:6651".to_owned()),
        );
        // Path-only trailing segment still resolves the default port.
        assert_eq!(
            probe_authority("pulsar://broker.local/admin/v2"),
            Some("broker.local:6650".to_owned()),
        );
        // An explicit port always wins over the default.
        assert_eq!(
            probe_authority("pulsar://broker.local:7000"),
            Some("broker.local:7000".to_owned()),
        );
        // A scheme-less bare host has no scheme to take a default from, so it
        // is returned verbatim — the caller's dialer decides what to do.
        assert_eq!(
            probe_authority("broker.local"),
            Some("broker.local".to_owned()),
        );
    }

    /// Regression test for ADR-0087, which closed the one limitation ADR-0085
    /// accepted: a port-less bracketed IPv6 literal now gets the scheme's
    /// default port like any other port-less host.
    ///
    /// The third assertion is the red/green witness. It previously read
    /// `Some("[::1]")` — under the name
    /// `probe_authority_leaves_bracketed_ipv6_untouched`, whose whole job was
    /// to pin the gap as a recorded decision. Reverting
    /// [`authority_has_explicit_port`] to `contains(':')` turns it red.
    #[test]
    fn probe_authority_synthesises_default_port_for_bracketed_ipv6() {
        // An explicit port still wins, on both schemes.
        assert_eq!(
            probe_authority("pulsar://[::1]:6650"),
            Some("[::1]:6650".to_owned()),
        );
        assert_eq!(
            probe_authority("pulsar+ssl://[2001:db8::1]:6651"),
            Some("[2001:db8::1]:6651".to_owned()),
        );
        // The closed gap: the port-less form now resolves to the scheme default
        // instead of staying port-less and being rejected by the dialer.
        assert_eq!(
            probe_authority("pulsar://[::1]"),
            Some("[::1]:6650".to_owned()),
            "a port-less bracketed IPv6 literal must take the scheme default port",
        );
        assert_eq!(
            probe_authority("pulsar+ssl://[2001:db8::1]"),
            Some("[2001:db8::1]:6651".to_owned()),
        );
        // A trailing path is trimmed before the port question is asked.
        assert_eq!(
            probe_authority("pulsar://[::1]/admin/v2"),
            Some("[::1]:6650".to_owned()),
        );
        // Malformed — unterminated bracket. No port is invented; the input is
        // returned as-is, exactly as before the bracket handling existed.
        assert_eq!(probe_authority("pulsar://[::1"), Some("[::1".to_owned()));
        // Scheme-less: there is no scheme to take a default from, so the
        // bracket handling must not start synthesising one here either.
        assert_eq!(probe_authority("[::1]"), Some("[::1]".to_owned()));
    }

    #[test]
    fn trait_object_is_send_sync() {
        // Compile-time check: an `Arc<dyn HealthProbe>` must be `Send + Sync`
        // because the policy stores it inside a `Send + Sync` struct.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<dyn HealthProbe>>();
    }
}
