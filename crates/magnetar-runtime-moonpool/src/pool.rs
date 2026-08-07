// SPDX-License-Identifier: Apache-2.0

//! Per-broker connection pool for the Apache Pulsar Proxy and for
//! multi-broker DIRECT routing (ADR-0039 — base proxy entry +
//! 2026-06-01 amendment), moonpool engine flavour.
//!
//! 1:1 with [`magnetar_runtime_tokio::pool`]. Stays generic over
//! [`moonpool_core::Providers`] so the pool behaves identically in production
//! `TokioProviders` runs and `moonpool-sim` deterministic substrates.
//!
//! Two routing shapes share this pool, keyed on
//! `(logical_broker_url, physical_dial_address)`:
//!
//! 1. **Proxy-routed** (`proxy_through_service_url = true` on the lookup): every pool entry dials
//!    the same `physical` (the proxy on the bootstrap address);
//!    `CommandConnect.proxy_to_broker_url` is `Some(logical)` so the proxy forwards every frame on
//!    that connection to the resolved broker.
//! 2. **Direct multi-broker** (`proxy_through_service_url = false` plus a `broker_service_url` that
//!    names a broker *other than* the bootstrap): the pool dials the resolved broker directly
//!    (`logical == physical`), `CommandConnect.proxy_to_broker_url` is **`None`** (we are talking
//!    directly to the broker, no proxy in the middle). The 2026-06-01 amendment to ADR-0039 wires
//!    this path so the second producer / consumer on a multi-broker cluster lands on the broker the
//!    lookup actually resolved to, instead of bouncing on the bootstrap with
//!    `ServerError::NotConnected "not served by this instance"`.
//!
//! See [`magnetar_runtime_tokio::pool`] for the design notes — both engines
//! pull the same shared contract out of `magnetar-proto`'s
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`] +
//! [`ConnectionConfig::proxy_to_broker_url`].
//!
//! # Provider-native single-flight dialing
//!
//! Moonpool 0.8 requires provider futures to be `Send`.
//! [`get_or_open`] still assigns the dial, handshake, and supervised-driver creation to one
//! [`moonpool_core::TaskProvider::spawn_task`] task so concurrent callers share one pending result.
//! `TokioProviders` schedules that task through Tokio; `moonpool_sim::SimProviders` schedules it
//! on Moonpool's seeded deterministic executor.
//! Waiting callers park on [`tokio::sync::Notify`] and read the published
//! `Arc<Mutex<Option<Result<...>>>>` slot.
//!
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`]: magnetar_proto::event::LookupOutcome::Connect
//! [`ConnectionConfig::proxy_to_broker_url`]: magnetar_proto::ConnectionConfig::proxy_to_broker_url

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use magnetar_proto::ConnectionConfig;
use moonpool_core::{Providers, TaskProvider, TimeError, TimeProvider};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::dns::DnsResolver;
use crate::driver::{DriverHandle, ReconnectContext, spawn_supervised as spawn_supervised_driver};
use crate::transport::Transport;
use crate::{ConnectionShared, EngineError, handshake_plain, make_shared_with_providers};

/// Building blocks for `(logical, physical)` pool entries. Cloneable so the
/// pool can hand a snapshot to each `get_or_open` call. `P` is the moonpool
/// providers bundle; it must be `Clone` (it already is — `Providers` requires
/// it).
#[derive(Clone)]
pub(crate) struct ConnectionFactory<P: Providers> {
    /// The `host:port` the bootstrap connection dialled. On the proxy path every pool entry dials
    /// this same address (it is the proxy). On the multi-broker DIRECT path the per-call
    /// `physical` argument to [`get_or_open`] overrides it, so each direct entry dials its own
    /// broker. Mirrors the tokio pool's `factory.url`.
    pub(crate) addr: String,
    /// Template `ConnectionConfig`. Cloned per entry; `proxy_to_broker_url`
    /// is overwritten with the logical broker URL before handshake.
    pub(crate) bootstrap_config: ConnectionConfig,
    /// Runtime-owned operation retry policy, separate from the public
    /// `ConnectionConfig` source-compatible surface.
    pub(crate) operation_retry: Arc<Mutex<magnetar_proto::OperationRetryConfig>>,
    /// Moonpool providers — the pool re-uses them to spawn the per-entry
    /// supervised driver. `Providers` is `Clone` so a fresh snapshot per
    /// entry is cheap.
    pub(crate) providers: P,
    /// Reused rustls configuration for TLS bootstrap, controller, and segment
    /// entries. `None` means every pool transport is plaintext.
    pub(crate) tls_config: Option<Arc<rustls::ClientConfig>>,
    /// PIP-121 service-URL provider (cluster failover). Shared across pool
    /// entries — every supervised loop polls it on reconnect.
    pub(crate) service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    /// Pluggable DNS resolver.
    pub(crate) dns_resolver: Option<Arc<dyn DnsResolver>>,
    /// Protocol default used when a DIRECT lookup advertises a scheme-less
    /// broker hostname. The current pooled constructor is plaintext-only, so
    /// it records Pulsar's plaintext default (`6650`).
    pub(crate) schemeless_default_port: u16,
}

impl<P: Providers> std::fmt::Debug for ConnectionFactory<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `providers` and `bootstrap_config` are intentionally omitted —
        // they're verbose handle bundles, not config metadata. Use
        // `finish_non_exhaustive` so `clippy::missing_fields_in_debug`
        // accepts the surface.
        f.debug_struct("ConnectionFactory")
            .field("addr", &self.addr)
            .field(
                "has_service_url_provider",
                &self.service_url_provider.is_some(),
            )
            .field("has_dns_resolver", &self.dns_resolver.is_some())
            .field("tls", &self.tls_config.is_some())
            .field("schemeless_default_port", &self.schemeless_default_port)
            .finish_non_exhaustive()
    }
}

/// Composite key — mirror of the tokio pool's
/// `(logical, physical, connection_index)` shape. The `connection_index ∈
/// [0, connections_per_broker)` is the `connections_per_broker` fan-out (Java
/// `ClientBuilder#connectionsPerBroker`, issue #314, ADR-0073); at the default
/// `connections_per_broker = 1` the index is always `0`, collapsing back to one
/// entry per `(logical, physical)`. See
/// [`magnetar_runtime_tokio::pool`] for the full rationale — the engines key
/// identically so the differential suite stays in lock-step.
type PoolKey = (String, String, usize);

/// Result the dial task publishes to waiters. `Send` because the outer
/// `get_or_open` future (which itself must be `Send` for the facade's
/// `CreateProducerApi`/`SubscribeApi` traits) reads it. `Result<Arc<...>,
/// EngineError>` is `Send` on both arms.
type DialOutcome = Result<Arc<ConnectionShared>, EngineError>;

/// Slot that a dial task publishes its result through. Waiters race against
/// it: clone the handles under the entries-map lock, drop the lock, then
/// `loop { peek slot; else notified.await }`.
///
/// We don't gate on a `oneshot`-style channel (banned, ADR-0003); instead the
/// dial task stores its result in the `Mutex<Option<...>>` slot and notifies
/// every waiter via [`Notify::notify_waiters`]. Late waiters that arrive
/// AFTER the notify wakeup hit the populated slot on their first peek.
///
/// Result is wrapped in [`Arc`] so multiple waiters can each clone-out a
/// reference cheaply — [`EngineError`] itself isn't `Clone` (its `Io` arm
/// carries a non-`Clone` [`std::io::Error`]).
struct PendingDial {
    notify: Arc<Notify>,
    result: Arc<Mutex<Option<Arc<DialOutcome>>>>,
    cancel: Arc<Notify>,
    completed: Arc<Notify>,
    is_complete: Arc<AtomicBool>,
}

impl PendingDial {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            result: Arc::new(Mutex::new(None)),
            cancel: Arc::new(Notify::new()),
            completed: Arc::new(Notify::new()),
            is_complete: Arc::new(AtomicBool::new(false)),
        }
    }

    fn handles(&self) -> Self {
        Self {
            notify: self.notify.clone(),
            result: self.result.clone(),
            cancel: self.cancel.clone(),
            completed: self.completed.clone(),
            is_complete: self.is_complete.clone(),
        }
    }

    fn mark_complete(&self) {
        self.is_complete.store(true, Ordering::Release);
        self.completed.notify_waiters();
    }

    async fn cancel_and_wait(&self) {
        let completed = self.completed.notified();
        let mut completed = std::pin::pin!(completed);
        completed.as_mut().enable();
        self.cancel.notify_one();
        if !self.is_complete.load(Ordering::Acquire) {
            completed.await;
        }
    }
}

struct PendingCompletion(PendingDial);

impl Drop for PendingCompletion {
    fn drop(&mut self) {
        {
            let mut result = self.0.result.lock();
            if result.is_none() {
                *result = Some(Arc::new(Err(EngineError::PeerClosed)));
            }
        }
        self.0.notify.notify_waiters();
        self.0.mark_complete();
    }
}

/// State of one pool entry — `Pending` while a dial task is in flight,
/// `Ready` once the connection has handshaked and is owned by a supervised
/// driver.
enum EntryState {
    /// Dial in flight. Late callers join the existing dial instead of
    /// kicking off a second one — the race resolution the tokio
    /// `ProxyConnectionPool` does post-`build_entry`, we do it pre-dial
    /// here, which is cleaner under the spawn-task pattern.
    Pending(PendingDial),
    /// Connection is up and ready for `CommandProducer` / `CommandSubscribe`.
    Ready {
        shared: Arc<ConnectionShared>,
        driver: Mutex<Option<DriverHandle>>,
    },
}

/// Moonpool pool of `ConnectionShared` keyed by
/// `(logical broker URL, physical dial address)`. See the module docs and
/// [`magnetar_runtime_tokio::pool::ProxyConnectionPool`].
pub(crate) struct ProxyConnectionPool<P: Providers> {
    factory: ConnectionFactory<P>,
    /// Latched before close drains the map so detached dial tasks cannot
    /// promote a late success back into a closed pool.
    closed: AtomicBool,
    /// `parking_lot::Mutex` per ADR-0003 / repo convention. Critical sections
    /// are short (HashMap mutations + clones of `Arc<EntryState>`).
    entries: Mutex<HashMap<PoolKey, Arc<EntryState>>>,
}

impl<P: Providers> ProxyConnectionPool<P> {
    pub(crate) fn set_operation_retry_config(&self, config: magnetar_proto::OperationRetryConfig) {
        *self.factory.operation_retry.lock() = config;
    }
}

impl<P: Providers> std::fmt::Debug for ProxyConnectionPool<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot: Vec<_> = self.entries.lock().keys().cloned().collect();
        f.debug_struct("ProxyConnectionPool")
            .field("factory", &self.factory)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .field("entries", &snapshot)
            .finish()
    }
}

impl<P: Providers> ProxyConnectionPool<P> {
    pub(crate) fn new(factory: ConnectionFactory<P>) -> Arc<Self> {
        Arc::new(Self {
            factory,
            closed: AtomicBool::new(false),
            entries: Mutex::new(HashMap::new()),
        })
    }

    /// Bootstrap dial target — every pool entry dials this same physical
    /// address. Mirrors the tokio engine's `ProxyConnectionPool::bootstrap_url`.
    #[allow(dead_code)] // diagnostics-only accessor; kept on parity with tokio
    pub(crate) fn bootstrap_addr(&self) -> &str {
        &self.factory.addr
    }

    /// Default port inherited by a scheme-less DIRECT broker authority.
    pub(crate) const fn schemeless_default_port(&self) -> u16 {
        self.factory.schemeless_default_port
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn scalable_url_allowed(&self, url: &str) -> bool {
        self.factory
            .bootstrap_config
            .redirect_url_allow_list
            .as_ref()
            .is_none_or(|allow_list| allow_list.is_allowed(url))
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn bootstrap_uses_proxy_target(&self) -> bool {
        self.factory.bootstrap_config.proxy_to_broker_url.is_some()
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn uses_tls(&self) -> bool {
        self.factory.tls_config.is_some()
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn task_provider(&self) -> P::Task {
        self.factory.providers.task().clone()
    }

    /// Number of currently-tracked entries (Ready + Pending). Used by tests
    /// and diagnostics.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

impl<P: Providers + Send + Sync> ProxyConnectionPool<P> {
    /// Close every pool entry. Idempotent.
    pub(crate) async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // Snapshot under-lock so we don't hold the lock across `.await`.
        let drained: Vec<Arc<EntryState>> = self.entries.lock().drain().map(|(_, v)| v).collect();
        for state in drained {
            match &*state {
                EntryState::Ready { shared, driver } => {
                    {
                        let mut conn = shared.inner.lock();
                        conn.close();
                    }
                    shared.driver_waker.notify_one();
                    let handle = driver.lock().take();
                    if let Some(handle) = handle {
                        let _ = handle.join().await;
                    }
                }
                EntryState::Pending(pending) => {
                    {
                        let mut result = pending.result.lock();
                        if result.is_none() {
                            *result = Some(Arc::new(Err(EngineError::PeerClosed)));
                        }
                    }
                    pending.notify.notify_waiters();
                    pending.cancel_and_wait().await;
                }
            }
        }
    }
}

/// Get or lazily open the pool entry for `(logical, physical)`.
///
/// `logical` is the broker URL the lookup resolved to. `physical` is the
/// `host:port` magnetar actually dials.
///
/// `proxy_to_broker_url` controls the `CommandConnect.proxy_to_broker_url`
/// field on the entry's CONNECT frame:
///
/// * `Some(host_port)` — proxy path (the value the Pulsar Proxy expects, `host:port` form, no
///   scheme). The pool entry rides on `physical` (= the proxy address) and the proxy forwards each
///   frame to the broker named in `proxy_to_broker_url`. Mirrors Java `Commands.newConnect(...,
///   targetBroker)`.
/// * `None` — direct multi-broker routing. The pool entry dials `physical` (= the resolved broker)
///   directly, no proxy in the middle. ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)".
///
/// `index` is the `connections_per_broker` fan-out slot (ADR-0073, issue #314): callers asking
/// for the same `(logical, physical)` but different `index` get distinct connections, so a logical
/// producer fleet can spread its sends across several broker connections. The default
/// `connections_per_broker = 1` always passes `index = 0`, collapsing to one entry per
/// `(logical, physical)`. Mirrors `magnetar_runtime_tokio::pool::ProxyConnectionPool::get_or_open`.
///
/// Concurrency: if two callers race for the same key, only one dial task
/// is spawned; the loser awaits the winner's [`PendingDial`].
///
/// # Task ownership
///
/// The dial work runs in one [`TaskProvider::spawn_task`] task.
/// The outer future awaits `Notify` and reads the shared result slot, so racing callers observe
/// one connection attempt and one published outcome.
///
/// Taking the pool by `Arc<...>` (rather than `&self`) lets the spawned
/// dial task keep the pool alive without borrowing through a method
/// signature.
pub(crate) async fn get_or_open<P>(
    pool: Arc<ProxyConnectionPool<P>>,
    logical: &str,
    physical: &str,
    proxy_to_broker_url: Option<String>,
    index: usize,
) -> Result<Arc<ConnectionShared>, EngineError>
where
    P: Providers + Send + Sync,
{
    if pool.closed.load(Ordering::Acquire) {
        return Err(pool_closed_error());
    }

    let key: PoolKey = (logical.to_owned(), physical.to_owned(), index);

    // Fast path or race-resolution decision under the lock.
    let pending = {
        let mut entries = pool.entries.lock();
        if pool.closed.load(Ordering::Acquire) {
            return Err(pool_closed_error());
        }
        if let Some(state) = entries.get(&key).cloned() {
            match &*state {
                EntryState::Ready { shared, .. } => return Ok(shared.clone()),
                EntryState::Pending(pending) => pending.handles(),
            }
        } else {
            let pending = PendingDial::new();
            let handles = pending.handles();
            let entry = Arc::new(EntryState::Pending(pending));
            // State-consistency mirror of the tokio pool's insert site
            // (`magnetar_runtime_tokio::pool::ProxyConnectionPool::get_or_open`):
            // we reach this arm only inside the `else` of the `get(&key)` miss,
            // with the entries-lock held continuously — so `key` is provably
            // absent and inserting the fresh `Pending` must not clobber an
            // existing entry. A `Some` here would mean a second dial races the
            // same key (a pool-bookkeeping bug) and would orphan the clobbered
            // entry's `PendingDial`/`Ready` state. Cannot fire on legitimate
            // broker/wire input — pure map bookkeeping under the same lock.
            let clobbered = entries.insert(key.clone(), entry.clone());
            debug_assert!(
                clobbered.is_none(),
                "pool entry insert clobbered a live entry — double registration for one key"
            );
            drop(entries);
            spawn_dial(
                pool.clone(),
                physical.to_owned(),
                proxy_to_broker_url,
                key.clone(),
                entry,
                handles.handles(),
            );
            handles
        }
    };

    // The spawned dial owns the single operation-timeout deadline so timing
    // out also drops the in-flight transport instead of merely abandoning a
    // detached task. Waiters only park on the published result.
    loop {
        let notified = pending.notify.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if let Some(outcome) = pending.result.lock().as_ref().map(Arc::clone) {
            return match &*outcome {
                Ok(shared) => Ok(shared.clone()),
                Err(err) => Err(clone_engine_error(err)),
            };
        }
        notified.await;
    }
}

/// Open (or reuse) an additional connection to the **bootstrap** broker for
/// `connections_per_broker > 1` (ADR-0073, issue #314). Mirror of the tokio
/// pool's `get_or_open_bootstrap_sibling`.
///
/// `index` must be `≥ 1`: index `0` is the bootstrap connection itself, owned
/// directly by [`crate::Client`] and never tracked in the pool. The sibling
/// replicates the bootstrap CONNECT exactly — it dials the same physical address
/// ([`ConnectionFactory::addr`]) and carries the same
/// `CommandConnect.proxy_to_broker_url` the bootstrap used. Entries are keyed
/// `(bootstrap_addr, bootstrap_addr, index)`, disjoint from the proxy /
/// multi-broker-direct entries (whose `logical` is the resolved broker URL).
pub(crate) async fn get_or_open_bootstrap_sibling<P>(
    pool: Arc<ProxyConnectionPool<P>>,
    index: usize,
) -> Result<Arc<ConnectionShared>, EngineError>
where
    P: Providers + Send + Sync,
{
    let authority = pool.factory.addr.clone();
    let proxy = pool.factory.bootstrap_config.proxy_to_broker_url.clone();
    get_or_open(pool, &authority, &authority, proxy, index).await
}

/// Spawn the dial + handshake + supervised-driver task through the Moonpool [`TaskProvider`].
///
/// `TokioProviders` uses Tokio; `moonpool_sim::SimProviders` uses Moonpool's deterministic
/// executor.
fn spawn_dial<P>(
    pool: Arc<ProxyConnectionPool<P>>,
    physical: String,
    proxy_to_broker_url: Option<String>,
    key: PoolKey,
    expected_entry: Arc<EntryState>,
    pending: PendingDial,
) where
    P: Providers + Send + Sync,
{
    let factory = pool.factory.clone();
    let task = pool.factory.providers.task().clone();
    // The provider join handle remains detached, but `PendingDial` carries a
    // cancellation/completion handshake. `close()` cancels and awaits that
    // completion, while the provider-owned operation timeout bounds abandoned
    // waiters and stalled handshakes.
    let _detached = task.spawn_task("magnetar-moonpool-pool-dial", async move {
        let _completion = PendingCompletion(pending.handles());
        let time = factory.providers.time().clone();
        let operation_timeout = factory.bootstrap_config.operation_timeout;
        let build = time.timeout(
            operation_timeout,
            build_entry_async::<P>(&factory, &physical, proxy_to_broker_url),
        );
        let mut build = std::pin::pin!(build);
        let cancelled = pending.cancel.notified();
        let mut cancelled = std::pin::pin!(cancelled);
        cancelled.as_mut().enable();
        let outcome = moonpool_core::select! {
            biased;
            () = &mut cancelled => Err(EngineError::PeerClosed),
            timed = &mut build => match timed {
                Ok(outcome) => outcome,
                Err(TimeError::Elapsed) => Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "pool dial to {physical} exceeded operation_timeout \
                         ({operation_timeout:?})"
                    ),
                ))),
                Err(TimeError::Shutdown) => Err(EngineError::PeerClosed),
            },
        };
        let mut orphaned_success = None;
        let published = {
            let mut map = pool.entries.lock();
            let is_current_generation = map
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &expected_entry));
            let may_promote = is_current_generation && !pool.closed.load(Ordering::Acquire);

            match outcome {
                Ok((shared, driver)) if may_promote => {
                    let waiter_shared = shared.clone();
                    map.insert(
                        key,
                        Arc::new(EntryState::Ready {
                            shared,
                            driver: Mutex::new(Some(driver)),
                        }),
                    );
                    Ok(waiter_shared)
                }
                Ok(pair) => {
                    if is_current_generation {
                        map.remove(&key);
                    }
                    orphaned_success = Some(pair);
                    Err(EngineError::PeerClosed)
                }
                Err(err) if may_promote => {
                    map.remove(&key);
                    Err(clone_engine_error(&err))
                }
                Err(_) => {
                    if is_current_generation {
                        map.remove(&key);
                    }
                    Err(EngineError::PeerClosed)
                }
            }
        };

        // `close()` may already have published `PeerClosed`; never overwrite
        // that terminal result with a late dial outcome.
        {
            let mut result = pending.result.lock();
            if result.is_none() {
                *result = Some(Arc::new(published));
            }
        }
        pending.notify.notify_waiters();

        // A successful dial that lost the generation race (most importantly,
        // one completing after `close()`) owns a live supervised driver that
        // is no longer reachable through the map. Shut it down here instead
        // of leaking the connection or letting it outlive the pool.
        if let Some((shared, driver)) = orphaned_success {
            {
                let mut conn = shared.inner.lock();
                conn.close();
            }
            shared.driver_waker.notify_one();
            let _ = driver.join().await;
        }
    });
}

/// Internal: dial + handshake + spawn the supervised driver.
///
/// Returns the `(shared, driver)` pair the pool entry will own.
/// Moonpool 0.8 provider futures are `Send`, so this function can run through either
/// `TokioTaskProvider` or `SimTaskProvider`.
///
/// `physical` is the `host:port` we dial; `proxy_to_broker_url` is what we
/// put on `CommandConnect.proxy_to_broker_url` (proxy path) or `None` for
/// the multi-broker DIRECT path. See [`get_or_open`] for the routing
/// shape mapping.
async fn build_entry_async<P: Providers>(
    factory: &ConnectionFactory<P>,
    physical: &str,
    proxy_to_broker_url: Option<String>,
) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
    // Per-entry ConnectionConfig: clone the bootstrap, override the
    // `proxy_to_broker_url` according to the routing shape:
    //   * `Some(host_port)` — proxy path, CONNECT carries the logical broker URL so the proxy can
    //     forward subsequent frames.
    //   * `None` — direct multi-broker path, CONNECT carries no `proxy_to_broker_url` (we are
    //     dialling the broker directly).
    let mut cfg = factory.bootstrap_config.clone();
    cfg.proxy_to_broker_url = proxy_to_broker_url;

    let connect_timeout = cfg.connect_timeout;
    let operation_timeout = cfg.operation_timeout;
    let tls_host = factory
        .tls_config
        .as_ref()
        .map(|_| crate::transport::split_host_port(physical).map(|(host, _)| host.to_owned()))
        .transpose()?;
    let mut transport = crate::dial_with_retry::<P, _, _>(
        factory.providers.time(),
        cfg.connect_max_retries,
        operation_timeout,
        || {
            let tls_config = factory.tls_config.clone();
            let tls_host = tls_host.clone();
            async move {
                Transport::<P>::connect_selected(
                    factory.providers.network(),
                    physical,
                    tls_host.as_deref().zip(tls_config),
                    factory.dns_resolver.as_deref(),
                    factory.providers.time(),
                    connect_timeout,
                )
                .await
            }
        },
    )
    .await?;

    let shared = make_shared_with_providers::<P>(&factory.providers, cfg);
    shared
        .inner
        .lock()
        .set_operation_retry_config(factory.operation_retry.lock().clone());
    // `None` — the spawned task wraps this whole build in the single
    // provider-owned operation timeout, so handshake reads remain bounded
    // without arming a second timer inside `handshake_plain`.
    handshake_plain::<P>(
        &shared,
        &mut transport,
        factory.providers.time(),
        None,
        physical,
        false,
    )
    .await?;

    let ctx = ReconnectContext {
        host_port: physical.to_owned(),
        tls_config: factory.tls_config.clone(),
        tls_server_name: None,
        service_url_provider: factory.service_url_provider.clone(),
        dns_resolver: factory.dns_resolver.clone(),
    };
    let driver =
        spawn_supervised_driver::<P>(shared.clone(), transport, ctx, factory.providers.clone());

    Ok((shared, driver))
}

fn pool_closed_error() -> EngineError {
    EngineError::Config("connection pool is closed".to_owned())
}

/// `EngineError` is not `Clone` (it carries `io::Error` which isn't either),
/// so we hand-roll a shallow copy of the structurally-copyable variants and
/// downgrade `Io` to a re-wrapped `io::Error` carrying the original kind +
/// message. Used when the dial task must publish the same error to multiple
/// parked waiters.
fn clone_engine_error(err: &EngineError) -> EngineError {
    match err {
        EngineError::Io(io) => EngineError::Io(std::io::Error::new(io.kind(), io.to_string())),
        EngineError::PeerClosed => EngineError::PeerClosed,
        EngineError::Config(msg) => EngineError::Config(msg.clone()),
        EngineError::Protocol(p) => EngineError::Config(format!("protocol error: {p}")),
        EngineError::HandshakeFailed(msg) => EngineError::HandshakeFailed(msg.clone()),
        EngineError::Tls(t) => EngineError::Config(format!("tls error: {t}")),
        EngineError::MemoryLimitExceeded {
            current,
            limit,
            requested,
        } => EngineError::MemoryLimitExceeded {
            current: *current,
            limit: *limit,
            requested: *requested,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use moonpool_core::TokioProviders;

    use super::*;

    fn dummy_factory() -> ConnectionFactory<TokioProviders> {
        ConnectionFactory {
            addr: "broker.example.com:6650".to_owned(),
            bootstrap_config: ConnectionConfig {
                operation_timeout: Duration::from_secs(30),
                ..ConnectionConfig::default()
            },
            operation_retry: Arc::new(Mutex::new(magnetar_proto::OperationRetryConfig::default())),
            providers: TokioProviders::new(),
            tls_config: None,
            service_url_provider: None,
            dns_resolver: None,
            schemeless_default_port: 6650,
        }
    }

    // 1:1 parity with the tokio engine's `pool.rs` unit suite
    // (`crates/magnetar-runtime-tokio/src/pool.rs`): two tests, `fresh_pool_is_empty`
    // and a Debug-format smoke. The end-to-end pool behaviour is covered by the
    // integration test (`tests/proxy_multi_conn.rs`) — adding extra moonpool-only
    // unit tests here would trip the ADR-0024 parity gate.

    #[tokio::test(flavor = "current_thread")]
    async fn fresh_pool_is_empty() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        assert_eq!(pool.len(), 0);

        // Closing a pool with an in-flight dial must publish a terminal
        // outcome to every waiter instead of silently dropping the map entry
        // and leaving the detached dial able to resurrect it later.
        let pending = PendingDial::new();
        let result = pending.result.clone();
        let worker = pending.handles();
        let worker_task = tokio::spawn(async move {
            worker.cancel.notified().await;
            worker.mark_complete();
        });
        pool.entries.lock().insert(
            ("logical".to_owned(), "physical".to_owned(), 0),
            Arc::new(EntryState::Pending(pending)),
        );
        pool.close().await;
        worker_task.await.expect("pending worker exits on cancel");
        let outcome = result
            .lock()
            .as_ref()
            .cloned()
            .expect("pool close must resolve pending dials");
        assert!(matches!(&*outcome, Err(EngineError::PeerClosed)));
    }

    #[test]
    fn debug_includes_pool_state() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        let s = format!("{pool:?}");
        assert!(s.contains("ProxyConnectionPool"));
        assert!(s.contains("entries"));
    }
}
