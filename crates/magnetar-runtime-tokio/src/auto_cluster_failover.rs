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
//! - **Probe function** — user supplies an `async fn(url) -> bool`. Implementors can
//!   `tokio::net::lookup_host` then `TcpStream::connect`, or hit the admin REST
//!   `/admin/v2/brokers/health` endpoint, or do whatever is most accurate for their topology.
//! - **Failover policy** — first-healthy-wins in priority order. The primary URL is index 0;
//!   failover candidates follow.
//! - **Failback** — when the primary returns to healthy, the active URL reverts to it. (Mirrors
//!   Java's `failoverDelayMs` minus the linger timer — that knob is a follow-up; today the failback
//!   is immediate on the next probe cycle.)
//!
//! # Sans-io discipline
//!
//! Lives in `magnetar-runtime-tokio` (not `magnetar-proto`) because it
//! spawns a tokio task and polls health. The trait it implements
//! (`magnetar_proto::ServiceUrlProvider`) is sync and lives in the proto
//! crate; this implementation reads its shared state under a mutex.
//! See [ADR-0004](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use magnetar_proto::ServiceUrlProvider;
use tokio::task::JoinHandle;

/// Boxed future returned by the health-probe callback.
pub type HealthProbeFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Health probe callback. Given a URL, returns `true` if the cluster is
/// reachable AND serving (per the implementor's definition).
///
/// The same closure is invoked against every URL in the priority list on
/// every probe cycle; implementations must be re-entrant.
pub trait HealthProbe: Send + Sync + std::fmt::Debug {
    /// Probe `url`. Resolves to `true` for healthy.
    fn probe<'a>(&'a self, url: &'a str) -> HealthProbeFuture<'a>;
}

/// PIP-121 health-driven cluster failover service URL provider.
///
/// Cheap to clone — internal state is `Arc<Mutex<...>>` + an `Arc<dyn HealthProbe>`.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use magnetar_runtime_tokio::auto_cluster_failover::{
///     AutoClusterFailover, HealthProbe, HealthProbeFuture,
/// };
///
/// #[derive(Debug)]
/// struct AlwaysHealthy;
/// impl HealthProbe for AlwaysHealthy {
///     fn probe<'a>(&'a self, _url: &'a str) -> HealthProbeFuture<'a> {
///         Box::pin(async { true })
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
    reason = "manual Debug impl below, intentionally excludes the probe closure"
)]
pub struct AutoClusterFailover {
    /// URLs in priority order. Index 0 is the primary; subsequent indices
    /// are fallbacks. Never empty (constructor panics otherwise).
    urls: Arc<Vec<String>>,
    /// Health-probe callback applied to every URL.
    probe: Arc<dyn HealthProbe>,
    /// Index of the currently-active URL inside `urls`. Mutated by the
    /// background prober; read by `get_service_url()`.
    active: Arc<Mutex<usize>>,
}

impl std::fmt::Debug for AutoClusterFailover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug the probe trait-object via its own Debug bound — gives the
        // implementor a chance to log a useful name. Active index is read
        // best-effort.
        f.debug_struct("AutoClusterFailover")
            .field("urls", &self.urls)
            .field("probe", &self.probe)
            .field("active_index", &self.active.lock().ok().map(|g| *g))
            .finish()
    }
}

impl AutoClusterFailover {
    /// Construct an `AutoClusterFailover` with the given priority-ordered
    /// URL list and a health-probe callback. The primary URL (index 0)
    /// is the active URL until the prober demotes it.
    ///
    /// # Panics
    ///
    /// Panics if `urls` is empty.
    #[must_use]
    pub fn new(urls: Vec<String>, probe: Arc<dyn HealthProbe>) -> Self {
        assert!(
            !urls.is_empty(),
            "AutoClusterFailover requires at least one URL"
        );
        Self {
            urls: Arc::new(urls),
            probe,
            active: Arc::new(Mutex::new(0)),
        }
    }

    /// Spawn the background prober. Returns the [`JoinHandle`] so the
    /// caller can abort it (typically on client shutdown).
    ///
    /// The prober wakes every `interval`, probes each URL in priority
    /// order, and snaps the active index to the first healthy one (i.e.
    /// failback to the primary as soon as it recovers).
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
                let mut new_active: Option<usize> = None;
                for (idx, url) in urls.iter().enumerate() {
                    if probe.probe(url).await {
                        new_active = Some(idx);
                        break;
                    }
                }
                if let Some(idx) = new_active {
                    if let Ok(mut guard) = active.lock() {
                        if *guard != idx {
                            tracing::info!(
                                from_index = *guard,
                                to_index = idx,
                                to_url = %urls[idx],
                                "AutoClusterFailover: switching active URL",
                            );
                            *guard = idx;
                        }
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
        match self.active.lock() {
            Ok(g) => *g,
            Err(poison) => *poison.into_inner(),
        }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Debug)]
    struct ConstProbe(bool);
    impl HealthProbe for ConstProbe {
        fn probe<'a>(&'a self, _url: &'a str) -> HealthProbeFuture<'a> {
            let v = self.0;
            Box::pin(async move { v })
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
            fn probe<'a>(&'a self, url: &'a str) -> HealthProbeFuture<'a> {
                let healthy = if url.contains("primary") {
                    self.primary_healthy.load(Ordering::SeqCst) != 0
                } else {
                    true
                };
                Box::pin(async move { healthy })
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
}
