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
use std::net::Ipv6Addr;
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

/// Canonical parse of a Pulsar endpoint string into the
/// `host:port` authority an engine hands to its dialer
/// (`tokio::net::lookup_host`, `moonpool_core::NetworkProvider::connect`, …).
///
/// `schemeless_default_port` supplies the runtime's bootstrap default when a
/// broker advertises a bare host with no scheme and no port. A recognised
/// Pulsar scheme always selects its own default, and an explicit port always
/// wins over either default. Pulsar schemes are matched ASCII-case-insensitively,
/// as required for URI schemes.
///
/// Accepts the following shapes:
///
/// | Input                                  | Fallback     | Output                       |
/// | -------------------------------------- | ------------ | ---------------------------- |
/// | `pulsar://host:port[/path…]`           | any          | `Some("host:port")`          |
/// | `pulsar+ssl://host:port[/path…]`       | any          | `Some("host:port")`          |
/// | `host:port` (no scheme)                | any          | `Some("host:port")`          |
/// | `pulsar://host` (no port)              | any          | `Some("host:6650")`          |
/// | `pulsar+ssl://host` (no port)          | any          | `Some("host:6651")`          |
/// | `host` (no scheme or port)             | `Some(6650)` | `Some("host:6650")`          |
/// | `host` (no scheme or port)             | `None`       | `Some("host")`               |
/// | `[::1]` (no scheme or port)            | `Some(6650)` | `Some("[::1]:6650")`         |
/// | unrecognised scheme / invalid authority | any          | `None`                       |
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
/// Returning `None` is the safe outcome: health probes report the endpoint
/// unhealthy, and DIRECT-routing callers reject it before pool insertion or
/// I/O.
///
/// # Bracketed IPv6 literals
///
/// A bracketed IPv6 literal is full of colons that belong to the *address*, so
/// "does this authority already carry a port?" cannot be answered with
/// `contains(':')`. The private validator parses the bracket body as
/// [`Ipv6Addr`] and accepts only an empty suffix or `:<u16>`.
///
/// Until ADR-0087 this function (and the three parsers now delegating to it)
/// shared the naive test, so `pulsar://[::1]` got no port and the dialer
/// rejected it. That was ADR-0085's one documented limitation; closing it in
/// one place closed it for every caller at once, which is the whole point of
/// the parse living here.
///
/// An unterminated bracket, an invalid IPv6 body, an empty/non-numeric/out-of-range
/// explicit port, or an unbracketed authority containing multiple colons is
/// rejected before a dial target is constructed.
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
pub fn broker_authority(endpoint: &str, schemeless_default_port: Option<u16>) -> Option<String> {
    let (scheme, rest) = split_broker_endpoint(endpoint)?;
    let default_port = match scheme {
        BrokerEndpointScheme::Pulsar => Some(6650),
        BrokerEndpointScheme::PulsarTls => Some(6651),
        BrokerEndpointScheme::Schemeless => schemeless_default_port,
    };

    // Trim trailing path segments — `pulsar://host:port/anything` becomes
    // `host:port`. Bare `host:port` round-trips unchanged.
    let authority = rest.split('/').next().unwrap_or(rest);
    let has_explicit_port = validate_authority(authority)?;

    Some(match (has_explicit_port, default_port) {
        (false, Some(port)) => format!("{authority}:{port}"),
        _ => authority.to_owned(),
    })
}

/// Scheme classification for a broker endpoint accepted by
/// [`broker_authority`].
///
/// URI schemes are ASCII case-insensitive. `PULSAR://`, `Pulsar://`, and the
/// lowercase spelling therefore all classify as [`Self::Pulsar`]; the same
/// rule applies to [`Self::PulsarTls`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerEndpointScheme {
    /// `pulsar://` in any ASCII letter case.
    Pulsar,
    /// `pulsar+ssl://` in any ASCII letter case.
    PulsarTls,
    /// No `://` scheme marker is present.
    Schemeless,
}

/// Classify the scheme of a broker endpoint without parsing its authority.
///
/// Returns `None` for an explicit scheme other than `pulsar` or
/// `pulsar+ssl`. Authority validation remains the responsibility of
/// [`broker_authority`].
#[must_use]
pub fn broker_endpoint_scheme(endpoint: &str) -> Option<BrokerEndpointScheme> {
    split_broker_endpoint(endpoint).map(|(scheme, _rest)| scheme)
}

fn split_broker_endpoint(endpoint: &str) -> Option<(BrokerEndpointScheme, &str)> {
    if let Some(rest) = strip_prefix_ascii_case(endpoint, "pulsar+ssl://") {
        Some((BrokerEndpointScheme::PulsarTls, rest))
    } else if let Some(rest) = strip_prefix_ascii_case(endpoint, "pulsar://") {
        Some((BrokerEndpointScheme::Pulsar, rest))
    } else if endpoint.contains("://") {
        // Unrecognised scheme — refuse rather than truncate. See the
        // "Why unrecognised schemes are rejected" section above.
        None
    } else {
        Some((BrokerEndpointScheme::Schemeless, endpoint))
    }
}

fn strip_prefix_ascii_case<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let candidate = input.get(..prefix.len())?;
    candidate
        .eq_ignore_ascii_case(prefix)
        .then(|| &input[prefix.len()..])
}

/// Parse a [`HealthProbe`] endpoint without inventing a default for a
/// scheme-less, portless authority.
///
/// Recognised Pulsar schemes still supply their own defaults. The absence of a
/// scheme leaves a bare host unchanged, preserving the health-probe contract.
#[must_use]
pub fn probe_authority(endpoint: &str) -> Option<String> {
    broker_authority(endpoint, None)
}

/// Validate `authority` and report whether it carries an explicit port.
fn validate_authority(authority: &str) -> Option<bool> {
    if authority.starts_with('[') {
        let close = authority.find(']')?;
        let host = &authority[1..close];
        host.parse::<Ipv6Addr>().ok()?;
        let suffix = &authority[close + 1..];
        return match suffix {
            "" => Some(false),
            _ => validate_port(suffix.strip_prefix(':')?).map(|()| true),
        };
    }

    if authority.is_empty() || authority.contains('[') || authority.contains(']') {
        return None;
    }

    match authority.split_once(':') {
        None => Some(false),
        Some((host, port)) => {
            if host.is_empty() || port.contains(':') {
                return None;
            }
            validate_port(port).map(|()| true)
        }
    }
}

fn validate_port(port: &str) -> Option<()> {
    if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    port.parse::<u16>().ok().map(|_| ())
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
        // Malformed — an unterminated bracket is rejected before a caller can
        // hand it to a dialer.
        assert_eq!(probe_authority("pulsar://[::1"), None);
        // Scheme-less: there is no scheme to take a default from, so the
        // bracket handling must not start synthesising one here either.
        assert_eq!(probe_authority("[::1]"), Some("[::1]".to_owned()));
    }

    #[test]
    fn broker_authority_applies_only_the_selected_default() {
        let cases = [
            ("broker.local", Some(6650), Some("broker.local:6650")),
            ("broker.local", Some(6651), Some("broker.local:6651")),
            ("broker.local", None, Some("broker.local")),
            (
                "pulsar://broker.local",
                Some(6651),
                Some("broker.local:6650"),
            ),
            (
                "pulsar+ssl://broker.local",
                Some(6650),
                Some("broker.local:6651"),
            ),
            (
                "PULSAR://broker.local",
                Some(6651),
                Some("broker.local:6650"),
            ),
            (
                "PuLsAr+SsL://broker.local",
                Some(6650),
                Some("broker.local:6651"),
            ),
            ("broker.local:7000", Some(6650), Some("broker.local:7000")),
            ("[::1]", Some(6650), Some("[::1]:6650")),
        ];
        for (input, fallback, expected) in cases {
            assert_eq!(
                broker_authority(input, fallback).as_deref(),
                expected,
                "unexpected normalization for {input:?}",
            );
        }
    }

    #[test]
    fn broker_authority_rejects_structurally_unusable_authorities() {
        for input in [
            "",
            "pulsar://",
            "broker:",
            "broker:abc",
            "broker:65536",
            "2001:db8::1",
            "[::1",
            "[not-ipv6]",
            "[::1]suffix",
            "[::1]:",
            "[::1]:abc",
            "[::1]:65536",
        ] {
            assert_eq!(
                broker_authority(input, Some(6650)),
                None,
                "{input:?} must be rejected before a dial",
            );
        }
    }

    #[test]
    fn trait_object_is_send_sync() {
        // Compile-time check: an `Arc<dyn HealthProbe>` must be `Send + Sync`
        // because the policy stores it inside a `Send + Sync` struct.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<dyn HealthProbe>>();
    }
}
