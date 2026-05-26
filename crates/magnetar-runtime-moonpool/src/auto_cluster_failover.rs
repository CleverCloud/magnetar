// SPDX-License-Identifier: Apache-2.0

//! PIP-121 `AutoClusterFailover` — health-driven cluster failover for the
//! moonpool engine.
//!
//! Mirrors Java `org.apache.pulsar.client.api.AutoClusterFailover` and the
//! tokio-engine counterpart in
//! `magnetar_runtime_tokio::auto_cluster_failover` one-for-one. The
//! moonpool variant is generic over
//! [`moonpool_core::Providers`] so the probe loop, the TCP probe socket
//! dance, and the deadline plumbing all run through the simulator under
//! `moonpool-sim` and survive deterministic chaos.
//!
//! # Design
//!
//! - **Probe contract** — the policy delegates the actual health check to a sans-io
//!   [`magnetar_proto::HealthProbe`] (lifted out of the tokio crate per
//!   [ADR-0023](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0023-health-probe-trait-extraction.md)).
//!   The trait surface is identical to the tokio engine's; only the implementation differs.
//! - **Failover policy** — first-healthy-wins in priority order. The primary URL is index 0;
//!   failover candidates follow.
//! - **Failback** — when the primary returns to healthy, the active URL reverts to it on the next
//!   probe tick. (Same simplification as the tokio engine — Java's `failoverDelayMs` linger timer
//!   is a follow-up.)
//! - **Probe loop** — driven by [`moonpool_core::TaskProvider::spawn_task`] +
//!   [`moonpool_core::TimeProvider::sleep`]. The loop sleeps for `interval` between ticks; on every
//!   tick it probes URLs in priority order via [`HealthProbe::poll_probe`] adapted through
//!   `std::future::poll_fn`.
//!
//! # Sans-io discipline
//!
//! [`AutoClusterFailover<P>`] is engine-side because it spawns a task and
//! orchestrates time. The trait it delegates to
//! ([`magnetar_proto::HealthProbe`]) is sans-io and lives in the proto crate;
//! the moonpool implementation
//! ([`MoonpoolHealthProbe<P>`]) does its DNS lookup + TCP connect through
//! the [`moonpool_core::NetworkProvider`] so the same code path is driven
//! by [`moonpool_core::TokioProviders`] in production and by `SimProviders`
//! in deterministic-simulation tests. See
//! [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! and
//! [ADR-0023](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0023-health-probe-trait-extraction.md).
//!
//! # No channels
//!
//! Per [ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md):
//! the probe slot uses an [`Arc<parking_lot::Mutex<ProbeSlot>>`] + a
//! [`tokio::sync::Notify`] single-cell wakeup; the active-URL slot uses an
//! [`Arc<parking_lot::Mutex<usize>>`]. No `mpsc`/`broadcast`/`watch`/`oneshot`.

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use magnetar_proto::{HealthProbe, ServiceUrlProvider};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TimeProvider};
use parking_lot::Mutex;

/// PIP-121 health-driven cluster failover service URL provider — moonpool
/// engine variant.
///
/// Generic over [`moonpool_core::Providers`] so the same type drives a
/// production [`moonpool_core::TokioProviders`] bundle and a deterministic
/// simulator. Cheap to clone — internal state is `Arc<Mutex<...>>` + an
/// `Arc<dyn HealthProbe>`.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::task::{Context, Poll};
/// use std::time::{Duration, Instant};
///
/// use magnetar_proto::HealthProbe;
/// use magnetar_runtime_moonpool::auto_cluster_failover::AutoClusterFailover;
/// use moonpool_core::TokioProviders;
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
/// let providers = TokioProviders::new();
/// let failover = AutoClusterFailover::new(
///     vec![
///         "pulsar://primary.cluster:6650".to_owned(),
///         "pulsar://standby-east.cluster:6650".to_owned(),
///         "pulsar://standby-west.cluster:6650".to_owned(),
///     ],
///     Arc::new(AlwaysHealthy),
/// );
/// // Start the background prober. Returns a JoinHandle; drop it to detach.
/// let _ = failover.start(&providers, Duration::from_secs(5));
/// # }
/// ```
#[allow(
    missing_debug_implementations,
    reason = "manual Debug impl below, intentionally excludes the probe trait object"
)]
pub struct AutoClusterFailover<P: Providers> {
    /// URLs in priority order. Index 0 is the primary; subsequent indices
    /// are fallbacks. Never empty (constructor panics otherwise).
    urls: Arc<Vec<String>>,
    /// Health-probe trait object applied to every URL.
    probe: Arc<dyn HealthProbe>,
    /// Index of the currently-active URL inside `urls`. Mutated by the
    /// background prober; read by `get_service_url()`.
    active: Arc<Mutex<usize>>,
    /// `PhantomData` so the type carries the provider bundle without
    /// holding one. Constructors are cheap; the provider bundle is only
    /// needed when [`Self::start`] spawns the probe loop.
    _providers: std::marker::PhantomData<fn() -> P>,
}

impl<P: Providers> Clone for AutoClusterFailover<P> {
    fn clone(&self) -> Self {
        Self {
            urls: self.urls.clone(),
            probe: self.probe.clone(),
            active: self.active.clone(),
            _providers: std::marker::PhantomData,
        }
    }
}

impl<P: Providers> std::fmt::Debug for AutoClusterFailover<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug the probe trait-object via its own Debug bound — gives the
        // implementor a chance to log a useful name.
        f.debug_struct("AutoClusterFailover")
            .field("urls", &self.urls)
            .field("probe", &self.probe)
            .field("active_index", &*self.active.lock())
            .finish()
    }
}

impl<P: Providers> AutoClusterFailover<P> {
    /// Construct an `AutoClusterFailover` with the given priority-ordered
    /// URL list and a health-probe trait object. The primary URL (index 0)
    /// is the active URL until the prober demotes it.
    ///
    /// Preserved for ergonomic parity with the tokio engine — equivalent to
    /// [`Self::new_with_probe`].
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
            active: Arc::new(Mutex::new(0)),
            _providers: std::marker::PhantomData,
        }
    }

    /// Spawn the background prober on the [`moonpool_core::TaskProvider`]
    /// carried by `providers`. Returns the [`tokio::task::JoinHandle`] the
    /// task provider produces so the caller can abort it (typically on
    /// client shutdown).
    ///
    /// The prober sleeps for `interval` between ticks via
    /// [`moonpool_core::TimeProvider::sleep`]; on every tick it probes each
    /// URL in priority order and snaps the active index to the first
    /// healthy one (failback to the primary as soon as it recovers).
    ///
    /// Each [`HealthProbe::poll_probe`] call is given a deadline of
    /// `interval` from the tick start (computed against the host
    /// [`Instant`] clock; under `moonpool-sim` the *waiting* between ticks
    /// is virtual, but the deadline plumbing still serves as an upper
    /// bound for cooperatively-honoured probes).
    ///
    /// `providers` is consumed by reference; the task provider's
    /// `spawn_task` clones the bits it needs.
    pub fn start(&self, providers: &P, interval: Duration) -> tokio::task::JoinHandle<()> {
        let urls = self.urls.clone();
        let probe = self.probe.clone();
        let active = self.active.clone();
        let time = providers.time().clone();
        providers
            .task()
            .spawn_task("magnetar-moonpool-auto-cluster-failover", async move {
                loop {
                    // Sleep first — matches the tokio engine, which consumes its
                    // immediate `interval.tick()` so the first probe runs on
                    // tick 2. Under sim the sleep is virtual.
                    if time.sleep(interval).await.is_err() {
                        // The time provider shut down (sim run ending). Treat
                        // it as a clean stop — nothing left to probe.
                        return;
                    }
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
                        let mut guard = active.lock();
                        if *guard != idx {
                            tracing::info!(
                                from_index = *guard,
                                to_index = idx,
                                to_url = %urls[idx],
                                "AutoClusterFailover (moonpool): switching active URL",
                            );
                            *guard = idx;
                        }
                    }
                    // No healthy candidate — keep the current active URL. The
                    // supervisor's reconnect attempts will still try the
                    // unreachable URL; the next probe cycle reconsiders.
                }
            })
    }

    /// Snapshot the index of the currently-active URL. Mostly useful for
    /// tests and observability.
    #[must_use]
    pub fn active_index(&self) -> usize {
        *self.active.lock()
    }
}

impl<P: Providers> ServiceUrlProvider for AutoClusterFailover<P> {
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

/// moonpool-backed [`HealthProbe`] implementation — DNS lookup + TCP
/// connect routed through [`moonpool_core::NetworkProvider`] so the dance
/// is reproducible under deterministic simulation.
///
/// Each `poll_probe` call (against an endpoint with no in-flight slot)
/// spawns a background task on [`moonpool_core::TaskProvider`] that:
/// 1. parses the endpoint string (accepts `pulsar://host:port`, `pulsar+ssl://host:port`, or a bare
///    `host:port`),
/// 2. runs [`NetworkProvider::connect`] against the resulting `host:port` authority,
/// 3. wraps the connect attempt in [`TimeProvider::timeout`] so the probe respects the
///    caller-supplied deadline.
///
/// In-flight probes are keyed by endpoint string so two concurrent calls
/// against the same URL coalesce onto one task; this matches the policy's
/// expected access pattern (the prober iterates URLs sequentially, but
/// nothing in the trait contract precludes parallel use).
///
/// # Why a probe-slot map and not just `async fn`?
///
/// The sans-io [`HealthProbe`] trait is
/// `poll_probe(...) -> Poll<bool>`, not `async fn probe(...)`. Bridging
/// the moonpool single-threaded `?Send` providers into the synchronous
/// poll contract requires owning per-endpoint state across multiple polls
/// without holding a non-`Send` future across an `.await`. The
/// `ProbeSlot` pattern stores the verdict + the parked
/// [`Waker`] in a [`parking_lot::Mutex`]; the spawned task writes the
/// verdict and wakes the parked waker through a
/// [`tokio::sync::Notify`]-free path (we wake the [`Waker`] directly to
/// preserve the no-channels invariant — see
/// [ADR-0003](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)).
pub struct MoonpoolHealthProbe<P: Providers> {
    /// In-flight probe state keyed by endpoint string. A missing key means
    /// no probe is running; once the spawned task resolves we drain the
    /// slot from `poll_probe` so the next call starts a fresh one.
    ///
    /// `parking_lot::Mutex` (not `std::sync::Mutex`) for consistency with
    /// the rest of the moonpool engine; the critical section is short and
    /// never `.await`s.
    inflight: Arc<Mutex<HashMap<String, Arc<Mutex<ProbeSlot>>>>>,
    /// Provider bundle clone used to spawn probe tasks and dial the
    /// network. `Providers: Clone` so this is cheap to keep alive.
    providers: P,
}

impl<P: Providers> Default for MoonpoolHealthProbe<P>
where
    P: Default,
{
    fn default() -> Self {
        Self::new(P::default())
    }
}

impl<P: Providers> std::fmt::Debug for MoonpoolHealthProbe<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inflight_count = self.inflight.lock().len();
        f.debug_struct("MoonpoolHealthProbe")
            .field("inflight", &inflight_count)
            .finish_non_exhaustive()
    }
}

impl<P: Providers> MoonpoolHealthProbe<P> {
    /// Construct a probe bound to the given provider bundle.
    #[must_use]
    pub fn new(providers: P) -> Self {
        Self {
            inflight: Arc::new(Mutex::new(HashMap::new())),
            providers,
        }
    }

    /// Strip the optional `pulsar://` / `pulsar+ssl://` scheme so we have
    /// a `host:port` ready for [`NetworkProvider::connect`]. Returns
    /// `None` for inputs we cannot interpret as `host:port`.
    fn authority(endpoint: &str) -> Option<String> {
        let stripped = endpoint
            .strip_prefix("pulsar+ssl://")
            .or_else(|| endpoint.strip_prefix("pulsar://"))
            .unwrap_or(endpoint);
        // Trim trailing path segments — `pulsar://host:port/anything`
        // becomes `host:port`. Bare `host:port` round-trips unchanged.
        let auth = stripped.split('/').next().unwrap_or(stripped);
        if auth.is_empty() {
            None
        } else {
            Some(auth.to_owned())
        }
    }

    /// Spawn the actual TCP-connect probe task for `endpoint` honouring
    /// `deadline`. Verdict semantics: `true` iff
    /// [`NetworkProvider::connect`] succeeded before the deadline.
    fn spawn_probe(&self, endpoint: &str, deadline: Instant, slot: Arc<Mutex<ProbeSlot>>) {
        let endpoint_owned = endpoint.to_owned();
        let providers = self.providers.clone();
        let inflight = self.inflight.clone();
        providers
            .task()
            .clone()
            .spawn_task("magnetar-moonpool-health-probe", async move {
                let verdict = run_probe::<P>(&providers, &endpoint_owned, deadline).await;
                // Publish the verdict + wake whoever was parked. The slot
                // value is kept until `poll_probe` drains it; the
                // `inflight` map entry stays in place until then so the
                // first re-poll observes the verdict and removes it.
                let waker_opt = {
                    let mut g = slot.lock();
                    g.verdict = Some(verdict);
                    g.waker.take()
                };
                if let Some(w) = waker_opt {
                    w.wake();
                }
                // Belt-and-braces: if `poll_probe` raced our wake and never
                // re-polled (e.g. its future was dropped), the slot will sit
                // in `inflight` until the next call against this endpoint
                // sees `verdict.is_some()` and clears it. That's fine — the
                // map is bounded by the URL list length.
                let _ = inflight; // silence unused-Arc lint
            });
    }
}

/// Per-endpoint probe state shared between the spawned task and
/// `poll_probe`. Lives behind an [`Arc<parking_lot::Mutex<_>>`]:
/// - the task writes [`Self::verdict`] then [`Waker::wake`]s the parked waker,
/// - `poll_probe` checks [`Self::verdict`]: when `Some`, the verdict is consumed and the `inflight`
///   slot dropped; when `None`, the latest [`Waker`] is stashed in [`Self::waker`] for the task to
///   wake on completion.
///
/// Holds no futures, no channels — only a verdict cell, a "spawned once"
/// flag, and a single waker slot. The waker slot is overwritten on every
/// re-poll: standard `Future` semantics.
#[derive(Default)]
struct ProbeSlot {
    /// Probe verdict (filled by the spawned task). `None` while in
    /// flight; `Some(true)` or `Some(false)` once resolved.
    verdict: Option<bool>,
    /// Latest waker parked by `poll_probe`. Replaced on every re-poll;
    /// taken by the spawned task on completion.
    waker: Option<Waker>,
    /// `true` once `spawn_probe` has been called for this slot. Prevents
    /// re-spawning when a stale poll lands while a task is still in
    /// flight.
    spawned: bool,
}

/// The actual probe body. Resolves `true` iff
/// [`NetworkProvider::connect`] returns `Ok` before `deadline`.
///
/// Split out into a free async fn so the spawned task body stays small
/// and the unit tests can drive `MoonpoolHealthProbe::authority` without
/// needing a task runtime.
async fn run_probe<P: Providers>(providers: &P, endpoint: &str, deadline: Instant) -> bool {
    let Some(authority) = MoonpoolHealthProbe::<P>::authority(endpoint) else {
        tracing::debug!(endpoint = %endpoint, "MoonpoolHealthProbe: cannot parse endpoint");
        return false;
    };

    // Convert the absolute deadline into a relative duration the moonpool
    // `TimeProvider::timeout` API understands. Overshoot collapses to
    // `Duration::ZERO`, which means "fire immediately" — same outcome
    // as a connect that exceeded the deadline.
    let now = Instant::now();
    let dur = deadline.saturating_duration_since(now);

    let connect_fut = async {
        match providers.network().connect(&authority).await {
            Ok(stream) => {
                // The stream is dropped as we leave the async block; we
                // only cared about reachability. Bind it explicitly so a
                // future refactor that swaps the type for something
                // needing graceful shutdown will get a compile error
                // here.
                let _stream: <P::Network as NetworkProvider>::TcpStream = stream;
                true
            }
            Err(e) => {
                tracing::debug!(
                    authority = %authority,
                    error = %e,
                    "MoonpoolHealthProbe: connect failed",
                );
                false
            }
        }
    };

    if let Ok(verdict) = providers.time().timeout(dur, connect_fut).await {
        verdict
    } else {
        tracing::debug!(
            authority = %authority,
            "MoonpoolHealthProbe: connect timed out",
        );
        false
    }
}

impl<P: Providers + Send + Sync> HealthProbe for MoonpoolHealthProbe<P> {
    fn poll_probe(&self, endpoint: &str, deadline: Instant, cx: &mut Context<'_>) -> Poll<bool> {
        // Fast-path: look up the slot under the inflight map. If a slot
        // exists with a verdict, take it and clear the map entry. If a
        // slot exists without a verdict, park the waker and return
        // Pending. Otherwise create a slot, spawn the probe, park the
        // waker.
        let slot = {
            let mut map = self.inflight.lock();
            map.entry(endpoint.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(ProbeSlot::default())))
                .clone()
        };

        // Fast verdict-take path. Holding the slot lock while we mutate
        // the inflight map would be a lock-ordering hazard (inflight ⇒
        // slot is the spawn order); take a snapshot under the slot
        // lock and clear the map entry after.
        let verdict_opt = {
            let mut g = slot.lock();
            if let Some(v) = g.verdict.take() {
                Some(v)
            } else {
                g.waker = Some(cx.waker().clone());
                None
            }
        };

        if let Some(v) = verdict_opt {
            // Drop the inflight entry so the next call against this
            // endpoint starts a fresh probe. We drop *only* if the slot
            // we observed is the one currently in the map (defensive
            // against an intervening insert by a parallel caller — the
            // `Arc::ptr_eq` makes this a no-op when the entries
            // differ).
            let mut map = self.inflight.lock();
            if let Some(current) = map.get(endpoint) {
                if Arc::ptr_eq(current, &slot) {
                    map.remove(endpoint);
                }
            }
            return Poll::Ready(v);
        }

        // Slot was freshly inserted iff this is the first poll. We
        // detect "freshly inserted" by re-locking the slot and checking
        // whether the verdict is still None and no spawn happened yet.
        // The simplest robust strategy: spawn unconditionally when the
        // slot's `verdict` is still None AND no waker was previously
        // parked. That's a heuristic; a tighter approach uses an
        // explicit "spawned" flag on the slot. We use the explicit flag
        // for clarity.
        //
        // We piggyback on the fact that `default()` leaves both
        // `verdict` and `waker` at `None`. The first poll sets `waker`;
        // we trigger spawn on the path where `waker` was just set for
        // the first time. To make that thread-safe, we check (under the
        // slot lock) a separate `spawned` flag.
        //
        // The simplest implementation: track a separate `Arc<AtomicBool>`
        // per slot. We chose a struct-field bool inside `ProbeSlot` to
        // keep allocations bounded.

        // Now actually trigger the spawn if the slot is fresh. We do
        // this under a short critical section that re-locks the slot.
        let needs_spawn = {
            let mut g = slot.lock();
            if g.verdict.is_some() {
                // Verdict landed between the take above and here — this
                // shouldn't typically happen, but be defensive.
                false
            } else if g.spawned {
                false
            } else {
                g.spawned = true;
                true
            }
        };

        if needs_spawn {
            self.spawn_probe(endpoint, deadline, slot.clone());

            // Re-check the slot one more time — the spawned task may have
            // resolved synchronously under a sim provider that completes
            // `connect` inline. If so, claim the verdict here.
            let claimed = {
                let mut g = slot.lock();
                g.verdict.take()
            };
            if let Some(v) = claimed {
                let mut map = self.inflight.lock();
                if let Some(current) = map.get(endpoint) {
                    if Arc::ptr_eq(current, &slot) {
                        map.remove(endpoint);
                    }
                }
                return Poll::Ready(v);
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    use magnetar_proto::{HealthProbe, ServiceUrlProvider};
    use moonpool_core::TokioProviders;

    use super::{AutoClusterFailover, MoonpoolHealthProbe};

    /// Test-only probe that returns a fixed verdict inline — exercises the
    /// "happy path" of the failover loop without any I/O. Mirrors the
    /// `ConstProbe` in the tokio crate's `auto_cluster_failover.rs`.
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
            AutoClusterFailover::<TokioProviders>::new(vec![], Arc::new(ConstProbe(true)))
        });
        assert!(r.is_err());
    }

    #[test]
    fn new_with_probe_is_an_alias_of_new() {
        // Same inputs → same shape. Either constructor is acceptable; the
        // test asserts they behave identically. Mirrors the tokio engine's
        // identically-named test.
        let a = AutoClusterFailover::<TokioProviders>::new(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        let b = AutoClusterFailover::<TokioProviders>::new_with_probe(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        assert_eq!(a.active_index(), b.active_index());
        assert_eq!(a.get_service_url(), b.get_service_url());
    }

    #[test]
    fn initial_active_is_primary() {
        let f = AutoClusterFailover::<TokioProviders>::new(
            vec!["pulsar://a:6650".into(), "pulsar://b:6650".into()],
            Arc::new(ConstProbe(true)),
        );
        assert_eq!(f.active_index(), 0);
        assert_eq!(f.get_service_url(), "pulsar://a:6650");
    }

    /// Verify the prober switches to the second URL when the first probe
    /// fails, then back when the first recovers. Mirrors the tokio engine's
    /// `failover_switches_on_unhealthy_primary` — same probe shape, same
    /// expectations, but driven through the moonpool task provider.
    ///
    /// Real-time scheduling: the moonpool task provider uses
    /// `tokio::task::spawn_local`, which interacts awkwardly with
    /// `tokio::time::pause` + `advance` (the spawn_local task often
    /// doesn't observe the time advance until well after the test
    /// future has resumed). Real time is short, deterministic enough at
    /// this scale, and exercises the production scheduling path.
    #[tokio::test(flavor = "current_thread")]
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

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let providers = TokioProviders::new();
                let probe = Arc::new(Flipping {
                    primary_healthy: AtomicUsize::new(1),
                });
                let f = AutoClusterFailover::<TokioProviders>::new(
                    vec![
                        "pulsar://primary:6650".into(),
                        "pulsar://standby:6650".into(),
                    ],
                    probe.clone(),
                );
                let tick = Duration::from_millis(40);
                let handle = f.start(&providers, tick);

                // Tick 1: primary healthy → active=0.
                tokio::time::sleep(tick + Duration::from_millis(10)).await;
                assert_eq!(f.active_index(), 0);

                // Flip the primary unhealthy → next tick should switch to standby.
                probe.primary_healthy.store(0, Ordering::SeqCst);
                tokio::time::sleep(tick + Duration::from_millis(10)).await;
                assert_eq!(f.active_index(), 1);
                assert_eq!(f.get_service_url(), "pulsar://standby:6650");

                // Recover primary → next tick should switch back.
                probe.primary_healthy.store(1, Ordering::SeqCst);
                tokio::time::sleep(tick + Duration::from_millis(10)).await;
                assert_eq!(f.active_index(), 0);

                handle.abort();
            })
            .await;
    }

    // ----- MoonpoolHealthProbe ------------------------------------------------

    #[test]
    fn moonpool_probe_authority_strips_pulsar_scheme() {
        assert_eq!(
            MoonpoolHealthProbe::<TokioProviders>::authority("pulsar://broker.local:6650"),
            Some("broker.local:6650".to_owned()),
        );
        assert_eq!(
            MoonpoolHealthProbe::<TokioProviders>::authority("pulsar+ssl://broker.local:6651"),
            Some("broker.local:6651".to_owned()),
        );
    }

    #[test]
    fn moonpool_probe_authority_passes_through_bare_host_port() {
        assert_eq!(
            MoonpoolHealthProbe::<TokioProviders>::authority("127.0.0.1:6650"),
            Some("127.0.0.1:6650".to_owned()),
        );
    }

    #[test]
    fn moonpool_probe_authority_trims_trailing_path() {
        assert_eq!(
            MoonpoolHealthProbe::<TokioProviders>::authority("pulsar://broker.local:6650/admin/v2"),
            Some("broker.local:6650".to_owned()),
        );
    }

    #[test]
    fn moonpool_probe_authority_rejects_empty_input() {
        assert_eq!(MoonpoolHealthProbe::<TokioProviders>::authority(""), None,);
    }

    /// Connect to a real local TCP listener through the moonpool
    /// [`NetworkProvider`] — proves the
    /// `poll_probe` → spawn → `NetworkProvider::connect` integration
    /// end-to-end against an actual `bind` + `accept` pair. No mocking;
    /// the listener is the OS.
    #[tokio::test(flavor = "current_thread")]
    async fn moonpool_probe_reports_healthy_for_live_listener() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind");
                let addr = listener.local_addr().expect("local_addr");
                // Accept in the background so the connect can complete.
                let accept = tokio::spawn(async move {
                    let _ = listener.accept().await;
                });

                let probe = MoonpoolHealthProbe::new(TokioProviders::new());
                let endpoint = format!("pulsar://{addr}");
                let deadline = Instant::now() + Duration::from_secs(2);
                let verdict =
                    std::future::poll_fn(|cx| probe.poll_probe(&endpoint, deadline, cx)).await;
                assert!(verdict, "live listener must read healthy");
                accept.abort();
            })
            .await;
    }

    /// Connect to a port nothing is listening on — the kernel returns
    /// ECONNREFUSED quickly, so the probe verdict is `false` well within
    /// the deadline. Mirrors the tokio engine's identically-named test.
    #[tokio::test(flavor = "current_thread")]
    async fn moonpool_probe_reports_unhealthy_for_closed_port() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Bind, capture the port, then drop the listener so nothing answers.
                let probe_port = {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind");
                    listener.local_addr().expect("local_addr").port()
                };
                let probe = MoonpoolHealthProbe::new(TokioProviders::new());
                let endpoint = format!("127.0.0.1:{probe_port}");
                let deadline = Instant::now() + Duration::from_secs(2);
                let verdict =
                    std::future::poll_fn(|cx| probe.poll_probe(&endpoint, deadline, cx)).await;
                assert!(!verdict, "closed port must read unhealthy");
            })
            .await;
    }
}
