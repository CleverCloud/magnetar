// SPDX-License-Identifier: Apache-2.0

//! PIP-121 `AutoClusterFailover` — health-driven cluster failover.
//!
//! Mirrors Java `org.apache.pulsar.client.api.AutoClusterFailover`. The
//! provider holds a primary URL + a list of fallback URLs in priority
//! order; a background tokio task probes each one at the configured
//! interval and elects the highest-priority healthy URL as the active
//! one. `get_service_url()` returns the active URL; the supervised
//! reconnect path consults it on every (re)connect attempt.
//!
//! # Design
//!
//! - **Probe contract** — the policy delegates the actual health check to an
//!   [`magnetar_proto::HealthProbe`]: a sans-io trait whose `poll_probe` method returns
//!   `Poll<bool>`. Engines can host whichever implementation matches their runtime (this crate
//!   ships [`TokioHealthProbe`], which does the classic DNS lookup + TCP connect dance).
//! - **Failover policy** — first-healthy-wins in priority order. The primary URL is index 0;
//!   failover candidates follow.
//! - **Failback** — when the primary returns to healthy, the active URL reverts to it. (Mirrors
//!   Java's `failoverDelayMs` minus the linger timer — that knob is a follow-up; today the failback
//!   is immediate on the next probe cycle.)
//!
//! # Sans-io discipline
//!
//! `AutoClusterFailover` itself lives in `magnetar-runtime-tokio` (not
//! `magnetar-proto`) because it spawns a tokio task and orchestrates time.
//! The trait it delegates to (`magnetar_proto::HealthProbe`) is sans-io and
//! lives in the proto crate, which keeps moonpool free to ship its own
//! probe impl without dragging tokio into `magnetar-proto`.
//! See [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! and [ADR-0023](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0023-health-probe-trait-extraction.md).

use std::collections::HashMap;
use std::future::{Future as _, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use magnetar_proto::{HealthProbe, ServiceUrlProvider};
use tokio::task::{JoinError, JoinHandle};

/// PIP-121 health-driven cluster failover service URL provider.
///
/// Cheap to clone — internal state is `Arc<Mutex<...>>` + an `Arc<dyn HealthProbe>`.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::time::{Duration, Instant};
/// use std::task::{Context, Poll};
/// use magnetar_proto::HealthProbe;
/// use magnetar_runtime_tokio::auto_cluster_failover::AutoClusterFailover;
///
/// #[derive(Debug)]
/// struct AlwaysHealthy;
/// impl HealthProbe for AlwaysHealthy {
///     fn poll_probe(&self, _endpoint: &str, _deadline: Instant, _cx: &mut Context<'_>) -> Poll<bool> {
///         Poll::Ready(true)
///     }
/// }
///
/// # async fn run() {
/// let failover = AutoClusterFailover::new(
///     vec![
///         "pulsar://primary.cluster:6650".to_owned(),
///         "pulsar://standby-east.cluster:6650".to_owned(),
///         "pulsar://standby-west.cluster:6650".to_owned(),
///     ],
///     Arc::new(AlwaysHealthy),
/// );
/// // Start the background prober. Returns a JoinHandle; drop it to detach.
/// let _ = failover.start(Duration::from_secs(5));
/// # }
/// ```
#[derive(Clone)]
#[allow(
    missing_debug_implementations,
    reason = "manual Debug impl below, intentionally excludes the probe trait object"
)]
pub struct AutoClusterFailover {
    /// URLs in priority order. Index 0 is the primary; subsequent indices
    /// are fallbacks. Never empty (constructor panics otherwise).
    urls: Arc<Vec<String>>,
    /// Health-probe trait object applied to every URL.
    probe: Arc<dyn HealthProbe>,
    /// Index of the currently-active URL inside `urls`. Mutated by the
    /// background prober; read by `get_service_url()`. Lock-free
    /// `AtomicUsize` — no compound critical section is required, the
    /// prober only ever overwrites with a single index and the readers
    /// only `load` once.
    active: Arc<AtomicUsize>,
}

impl std::fmt::Debug for AutoClusterFailover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug the probe trait-object via its own Debug bound — gives the
        // implementor a chance to log a useful name.
        f.debug_struct("AutoClusterFailover")
            .field("urls", &self.urls)
            .field("probe", &self.probe)
            .field("active_index", &self.active.load(Ordering::Relaxed))
            .finish()
    }
}

impl AutoClusterFailover {
    /// Construct an `AutoClusterFailover` with the given priority-ordered
    /// URL list and a health-probe trait object. The primary URL (index 0)
    /// is the active URL until the prober demotes it.
    ///
    /// Preserved for ergonomic parity with prior releases — equivalent to
    /// [`Self::new_with_probe`], named to match the original PIP-121
    /// builder shape.
    ///
    /// # Panics
    ///
    /// Panics if `urls` is empty.
    #[must_use]
    pub fn new(urls: Vec<String>, probe: Arc<dyn HealthProbe>) -> Self {
        Self::new_with_probe(urls, probe)
    }

    /// Construct an `AutoClusterFailover` from any [`HealthProbe`] trait
    /// object. Identical to [`Self::new`]; the explicit `_with_probe`
    /// spelling exists to make the dependency on the sans-io probe trait
    /// obvious at call sites.
    ///
    /// # Panics
    ///
    /// Panics if `urls` is empty.
    #[must_use]
    pub fn new_with_probe(urls: Vec<String>, probe: Arc<dyn HealthProbe>) -> Self {
        assert!(
            !urls.is_empty(),
            "AutoClusterFailover requires at least one URL"
        );
        Self {
            urls: Arc::new(urls),
            probe,
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Spawn the background prober. Returns the [`JoinHandle`] so the
    /// caller can abort it (typically on client shutdown).
    ///
    /// The prober wakes every `interval`, probes each URL in priority
    /// order, and snaps the active index to the first healthy one (i.e.
    /// failback to the primary as soon as it recovers).
    ///
    /// Each `poll_probe` call is given a deadline of `interval` from
    /// the tick start. Implementors that honour the deadline get an
    /// upper bound on probe latency; those that ignore it still benefit
    /// from `MissedTickBehavior::Skip` keeping the cycle from queueing
    /// up backlog.
    pub fn start(&self, interval: Duration) -> JoinHandle<()> {
        let urls = self.urls.clone();
        let probe = self.probe.clone();
        let active = self.active.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate first tick — first probe runs on tick 2.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let deadline = Instant::now() + interval;
                let mut new_active: Option<usize> = None;
                for (idx, url) in urls.iter().enumerate() {
                    // Adapt the sans-io `poll_probe` into an async wait via
                    // `poll_fn` — the probe parks `cx.waker()` while pending
                    // and we get re-polled on completion. No channels.
                    let healthy = poll_fn(|cx| probe.poll_probe(url, deadline, cx)).await;
                    if healthy {
                        new_active = Some(idx);
                        break;
                    }
                }
                if let Some(idx) = new_active {
                    let prev = active.swap(idx, Ordering::Relaxed);
                    if prev != idx {
                        tracing::info!(
                            from_index = prev,
                            to_index = idx,
                            to_url = %urls[idx],
                            "AutoClusterFailover: switching active URL",
                        );
                    }
                }
                // No healthy candidate — keep the current active URL.
                // The supervisor's reconnect attempts will still try the
                // unreachable URL; the next probe cycle reconsiders.
            }
        })
    }

    /// Snapshot the index of the currently-active URL. Mostly useful for
    /// tests and observability.
    #[must_use]
    pub fn active_index(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

impl ServiceUrlProvider for AutoClusterFailover {
    fn get_service_url(&self) -> String {
        let idx = self.active_index();
        // Bounds-clamp — should always be valid because the prober only
        // assigns indices it iterated from `urls.iter().enumerate()`, but
        // defensive against a future bug.
        self.urls
            .get(idx)
            .or_else(|| self.urls.first())
            .cloned()
            .unwrap_or_default()
    }
}

/// Tokio-backed [`HealthProbe`] implementation — DNS lookup + TCP connect.
///
/// Each `poll_probe` call spawns a background tokio task that:
/// 1. parses the endpoint string (accepts `pulsar://host:port`, `pulsar+ssl://host:port`, or a bare
///    `host:port`),
/// 2. runs [`tokio::net::lookup_host`] against the resolved authority,
/// 3. attempts a [`tokio::net::TcpStream::connect`] to the first reachable candidate,
/// 4. all wrapped in [`tokio::time::timeout_at(deadline, ...)`] so the probe respects the
///    caller-supplied bound.
///
/// In-flight probes are keyed by endpoint string so two concurrent calls
/// against the same URL coalesce onto one task; this matches the policy's
/// expected access pattern (the prober iterates URLs sequentially, but
/// nothing in the trait contract precludes parallel use).
///
/// # Why a [`tokio::task::JoinHandle`] slab and not just `async fn`?
///
/// The sans-io [`HealthProbe`] trait is `poll_probe(...) -> Poll<bool>`,
/// not `async fn probe(...)`. Bridging tokio's async I/O into the
/// synchronous poll contract requires owning a future across multiple
/// polls. Spawning a tokio task and polling its `JoinHandle` is the
/// runtime-natural way to do that (the alternative — a `Pin<Box<dyn
/// Future>>` stashed inside a `Mutex` — runs into pinning headaches and
/// needs `unsafe` to project through the lock guard).
pub struct TokioHealthProbe {
    /// In-flight probe state keyed by endpoint string. A `None` slot means
    /// no probe is running; `Some(handle)` is an in-flight task. Once the
    /// task resolves we clear the slot so the next `poll_probe` starts a
    /// fresh one. The mutex is `std::sync::Mutex` because the critical
    /// section is short and never `.await`s.
    inflight: Mutex<HashMap<String, JoinHandle<bool>>>,
}

impl Default for TokioHealthProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TokioHealthProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inflight_count = self.inflight.lock().ok().map_or(0, |g| g.len());
        f.debug_struct("TokioHealthProbe")
            .field("inflight", &inflight_count)
            .finish()
    }
}

impl TokioHealthProbe {
    /// Construct a probe with no in-flight state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Strip the optional `pulsar://` / `pulsar+ssl://` scheme so we have an
    /// `authority` ready for [`tokio::net::lookup_host`]. Returns `None` for
    /// inputs we cannot interpret as `host:port`.
    fn authority(endpoint: &str) -> Option<String> {
        let stripped = endpoint
            .strip_prefix("pulsar+ssl://")
            .or_else(|| endpoint.strip_prefix("pulsar://"))
            .unwrap_or(endpoint);
        // Trim trailing path segments — `pulsar://host:port/anything` becomes
        // `host:port`. Bare `host:port` round-trips unchanged.
        let auth = stripped.split('/').next().unwrap_or(stripped);
        if auth.is_empty() {
            None
        } else {
            Some(auth.to_owned())
        }
    }

    /// Spawn the actual DNS + TCP-connect probe task for `endpoint` with
    /// the deadline honoured. Verdict semantics: `true` iff at least one
    /// resolved address accepted a TCP connection before the deadline.
    fn spawn_probe(endpoint: &str, deadline: Instant) -> JoinHandle<bool> {
        let endpoint_owned = endpoint.to_owned();
        tokio::spawn(async move {
            let Some(authority) = Self::authority(&endpoint_owned) else {
                tracing::debug!(endpoint = %endpoint_owned, "TokioHealthProbe: cannot parse endpoint");
                return false;
            };
            let probe = async move {
                let addrs = match tokio::net::lookup_host(&authority).await {
                    Ok(addrs) => addrs,
                    Err(e) => {
                        tracing::debug!(
                            authority = %authority,
                            error = %e,
                            "TokioHealthProbe: DNS lookup failed",
                        );
                        return false;
                    }
                };
                for addr in addrs {
                    match tokio::net::TcpStream::connect(addr).await {
                        Ok(_stream) => {
                            return true;
                        }
                        Err(e) => {
                            tracing::trace!(
                                %addr,
                                error = %e,
                                "TokioHealthProbe: TCP connect failed",
                            );
                        }
                    }
                }
                false
            };
            // Honour the deadline supplied by the caller. Overshoot collapses
            // to `false` — the policy treats slow/unresponsive endpoints the
            // same as unreachable ones (`.unwrap_or_default()` defaults `bool`
            // to `false`, which is exactly what we want).
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), probe)
                .await
                .unwrap_or_default()
        })
    }
}

impl HealthProbe for TokioHealthProbe {
    fn poll_probe(&self, endpoint: &str, deadline: Instant, cx: &mut Context<'_>) -> Poll<bool> {
        // Critical section: get-or-insert the in-flight handle, then poll it.
        // The lock is held only across the `entry` / `poll` calls; the task
        // body itself runs outside the lock.
        // A poisoned mutex shouldn't happen unless a probe task panicked
        // mid-lock — which we never do because we hold the lock only over
        // a HashMap mutation. Treat poison as unhealthy to be safe.
        let Ok(mut guard) = self.inflight.lock() else {
            return Poll::Ready(false);
        };
        let handle = guard
            .entry(endpoint.to_owned())
            .or_insert_with(|| Self::spawn_probe(endpoint, deadline));
        match Pin::new(handle).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(verdict)) => {
                guard.remove(endpoint);
                Poll::Ready(verdict)
            }
            Poll::Ready(Err(join_err)) => {
                guard.remove(endpoint);
                // A panicked probe task is a programming error in this crate,
                // not a broker fault. Surface "unhealthy" so the policy can
                // still make forward progress; the tracing log lets us
                // diagnose.
                Self::trace_join_failure(endpoint, &join_err);
                Poll::Ready(false)
            }
        }
    }
}

impl TokioHealthProbe {
    fn trace_join_failure(endpoint: &str, err: &JoinError) {
        tracing::error!(
            endpoint = %endpoint,
            error = %err,
            "TokioHealthProbe: probe task failed (panic or cancellation)",
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Test-only probe that returns a fixed verdict inline — exercises the
    /// "happy path" of the failover loop without any I/O.
    #[derive(Debug)]
    struct ConstProbe(bool);
    impl HealthProbe for ConstProbe {
        fn poll_probe(
            &self,
            _endpoint: &str,
            _deadline: Instant,
            _cx: &mut Context<'_>,
        ) -> Poll<bool> {
            Poll::Ready(self.0)
        }
    }

    #[test]
    fn empty_url_list_panics() {
        let r = std::panic::catch_unwind(|| {
            AutoClusterFailover::new(vec![], Arc::new(ConstProbe(true)))
        });
        assert!(r.is_err());
    }

    #[test]
    fn new_with_probe_is_an_alias_of_new() {
        // Same inputs → same shape. Either constructor is acceptable; the
        // test asserts they behave identically.
        let a = AutoClusterFailover::new(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        let b = AutoClusterFailover::new_with_probe(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        assert_eq!(a.active_index(), b.active_index());
        assert_eq!(a.get_service_url(), b.get_service_url());
    }

    #[test]
    fn initial_active_is_primary() {
        let f = AutoClusterFailover::new(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        assert_eq!(f.active_index(), 0);
        assert_eq!(f.get_service_url(), "pulsar://a:6650");
    }

    /// Verify the prober switches to the second URL when the first probe
    /// fails, then back when the first recovers. Uses an atomic counter
    /// to flip the probe's verdict mid-test.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn failover_switches_on_unhealthy_primary() {
        #[derive(Debug)]
        struct Flipping {
            primary_healthy: AtomicUsize,
        }
        impl HealthProbe for Flipping {
            fn poll_probe(
                &self,
                endpoint: &str,
                _deadline: Instant,
                _cx: &mut Context<'_>,
            ) -> Poll<bool> {
                let healthy = if endpoint.contains("primary") {
                    self.primary_healthy.load(Ordering::SeqCst) != 0
                } else {
                    true
                };
                Poll::Ready(healthy)
            }
        }

        let probe = Arc::new(Flipping {
            primary_healthy: AtomicUsize::new(1),
        });
        let f = AutoClusterFailover::new(
            vec![
                "pulsar://primary:6650".into(),
                "pulsar://standby:6650".into(),
            ],
            probe.clone(),
        );
        let handle = f.start(Duration::from_millis(100));

        // Tick 1: primary healthy → active=0.
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        assert_eq!(f.active_index(), 0);

        // Flip the primary unhealthy → next tick should switch to standby.
        probe.primary_healthy.store(0, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        assert_eq!(f.active_index(), 1);
        assert_eq!(f.get_service_url(), "pulsar://standby:6650");

        // Recover primary → next tick should switch back.
        probe.primary_healthy.store(1, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        assert_eq!(f.active_index(), 0);

        handle.abort();
    }

    // ----- TokioHealthProbe ---------------------------------------------------

    #[test]
    fn tokio_probe_authority_strips_pulsar_scheme() {
        assert_eq!(
            TokioHealthProbe::authority("pulsar://broker.local:6650"),
            Some("broker.local:6650".to_owned()),
        );
        assert_eq!(
            TokioHealthProbe::authority("pulsar+ssl://broker.local:6651"),
            Some("broker.local:6651".to_owned()),
        );
    }

    #[test]
    fn tokio_probe_authority_passes_through_bare_host_port() {
        assert_eq!(
            TokioHealthProbe::authority("127.0.0.1:6650"),
            Some("127.0.0.1:6650".to_owned()),
        );
    }

    #[test]
    fn tokio_probe_authority_trims_trailing_path() {
        assert_eq!(
            TokioHealthProbe::authority("pulsar://broker.local:6650/admin/v2"),
            Some("broker.local:6650".to_owned()),
        );
    }

    #[test]
    fn tokio_probe_authority_rejects_empty_input() {
        assert_eq!(TokioHealthProbe::authority(""), None);
    }

    /// Connect to a real local TCP listener — proves the
    /// `poll_probe` → `TokioHealthProbe::spawn_probe` integration end-to-end
    /// against an actual `bind` + `accept` pair. No mocking; the listener is
    /// the OS.
    #[tokio::test(flavor = "current_thread")]
    async fn tokio_probe_reports_healthy_for_live_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        // Accept in the background so the connect can complete.
        let accept = tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let probe = TokioHealthProbe::new();
        let endpoint = format!("pulsar://{addr}");
        let deadline = Instant::now() + Duration::from_secs(2);
        let verdict = poll_fn(|cx| probe.poll_probe(&endpoint, deadline, cx)).await;
        assert!(verdict, "live listener must read healthy");
        accept.abort();
    }

    /// Connect to a port nothing is listening on — the kernel returns
    /// ECONNREFUSED quickly, so the probe verdict is `false` well within the
    /// deadline.
    #[tokio::test(flavor = "current_thread")]
    async fn tokio_probe_reports_unhealthy_for_closed_port() {
        // Bind, capture the port, then drop the listener so nothing answers.
        let probe_port = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            listener.local_addr().expect("local_addr").port()
        };
        let probe = TokioHealthProbe::new();
        let endpoint = format!("127.0.0.1:{probe_port}");
        let deadline = Instant::now() + Duration::from_secs(2);
        let verdict = poll_fn(|cx| probe.poll_probe(&endpoint, deadline, cx)).await;
        assert!(!verdict, "closed port must read unhealthy");
    }
}
