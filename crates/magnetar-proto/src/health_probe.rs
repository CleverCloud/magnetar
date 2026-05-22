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
//! - [ADR-0022] — extraction of this trait into `magnetar-proto`.
//!
//! [ADR-0004]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md
//! [ADR-0016]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0016-pip-121-cluster-failover.md
//! [ADR-0022]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0022-health-probe-trait-extraction.md

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

    #[test]
    fn trait_object_is_send_sync() {
        // Compile-time check: an `Arc<dyn HealthProbe>` must be `Send + Sync`
        // because the policy stores it inside a `Send + Sync` struct.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<dyn HealthProbe>>();
    }
}
