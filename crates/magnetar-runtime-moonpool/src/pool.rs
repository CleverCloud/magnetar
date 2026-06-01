// SPDX-License-Identifier: Apache-2.0

//! Per-broker connection pool for the Apache Pulsar Proxy (ADR-0039),
//! moonpool engine flavour.
//!
//! 1:1 with [`magnetar_runtime_tokio::pool`]. Stays generic over
//! [`moonpool_core::Providers`] so the pool behaves identically in production
//! `TokioProviders` runs and `moonpool-sim` deterministic substrates.
//!
//! See [`magnetar_runtime_tokio::pool`] for the design notes — both engines
//! pull the same shared contract out of `magnetar-proto`'s
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`] +
//! [`ConnectionConfig::proxy_to_broker_url`].
//!
//! # `Send` propagation on the moonpool path
//!
//! `moonpool_core::NetworkProvider` is declared `#[async_trait(?Send)]`
//! (single-core design — moonpool-core 0.6.0 `src/network.rs:14`). A naïve
//! `network.connect(...).await` directly inside `get_or_open` would break
//! `Send` propagation up to the facade's `CreateProducerApi` /
//! `SubscribeApi` traits (`Pin<Box<dyn Future + Send>>` — see
//! `crates/magnetar/src/engine/mod.rs`). To keep the outer future `Send`,
//! [`get_or_open`] off-loads the dial + handshake + driver-spawn into a task
//! created via [`moonpool_core::TaskProvider::spawn_task`] (which uses
//! `spawn_local` internally — no `Send` bound on the spawned future). The
//! outer future only awaits a [`tokio::sync::Notify`] and reads an
//! `Arc<Mutex<Option<Result<...>>>>` slot, all of which are `Send`.
//!
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`]: magnetar_proto::event::LookupOutcome::Connect
//! [`ConnectionConfig::proxy_to_broker_url`]: magnetar_proto::ConnectionConfig::proxy_to_broker_url

use std::collections::HashMap;
use std::sync::Arc;

use magnetar_proto::ConnectionConfig;
use moonpool_core::{Providers, TaskProvider};
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
    /// The `host:port` the proxy listens on. Every pool entry dials this same
    /// address — only `CommandConnect.proxy_to_broker_url` differs per entry.
    pub(crate) addr: String,
    /// Template `ConnectionConfig`. Cloned per entry; `proxy_to_broker_url`
    /// is overwritten with the logical broker URL before handshake.
    pub(crate) bootstrap_config: ConnectionConfig,
    /// Moonpool providers — the pool re-uses them to spawn the per-entry
    /// supervised driver. `Providers` is `Clone` so a fresh snapshot per
    /// entry is cheap.
    pub(crate) providers: P,
    /// PIP-121 service-URL provider (cluster failover). Shared across pool
    /// entries — every supervised loop polls it on reconnect.
    pub(crate) service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    /// Pluggable DNS resolver.
    pub(crate) dns_resolver: Option<Arc<dyn DnsResolver>>,
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
            .finish_non_exhaustive()
    }
}

/// Composite key — mirror of the tokio pool's `(logical, physical)` shape.
/// `randomKey` multiplexing (the Java client's third component) is punted
/// the same way the tokio engine punts it (ADR-0039 §"Decision").
type PoolKey = (String, String);

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
}

impl PendingDial {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            result: Arc::new(Mutex::new(None)),
        }
    }

    fn handles(&self) -> Self {
        Self {
            notify: self.notify.clone(),
            result: self.result.clone(),
        }
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
    /// `parking_lot::Mutex` per ADR-0003 / repo convention. Critical sections
    /// are short (HashMap mutations + clones of `Arc<EntryState>`).
    entries: Mutex<HashMap<PoolKey, Arc<EntryState>>>,
}

impl<P: Providers> std::fmt::Debug for ProxyConnectionPool<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot: Vec<_> = self.entries.lock().keys().cloned().collect();
        f.debug_struct("ProxyConnectionPool")
            .field("factory", &self.factory)
            .field("entries", &snapshot)
            .finish()
    }
}

impl<P: Providers> ProxyConnectionPool<P> {
    pub(crate) fn new(factory: ConnectionFactory<P>) -> Arc<Self> {
        Arc::new(Self {
            factory,
            entries: Mutex::new(HashMap::new()),
        })
    }

    /// Bootstrap dial target — every pool entry dials this same physical
    /// address. Mirrors the tokio engine's `ProxyConnectionPool::bootstrap_url`.
    #[allow(dead_code)] // diagnostics-only accessor; kept on parity with tokio
    pub(crate) fn bootstrap_addr(&self) -> &str {
        &self.factory.addr
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
        // Snapshot under-lock so we don't hold the lock across `.await`.
        let drained: Vec<Arc<EntryState>> = self.entries.lock().drain().map(|(_, v)| v).collect();
        for state in drained {
            if let EntryState::Ready { shared, driver } = &*state {
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
            // `Pending` entries: the spawned dial task owns its
            // half-built state; dropping the entry here is sufficient
            // because `close()` is called from `Client::close` after the
            // bootstrap is torn down — the proxy will fail any in-flight
            // handshake, and the dial task's error path evicts the slot.
        }
    }
}

/// Get or lazily open the pool entry for `(logical, physical=pool.factory.addr)`.
///
/// Concurrency: if two callers race for the same key, only one dial task
/// is spawned; the loser awaits the winner's [`PendingDial`].
///
/// # Send-safety
///
/// This future stays `Send` even though `network.connect(...)` is `?Send`:
/// the dial work is hoisted into a task spawned via
/// [`TaskProvider::spawn_task`], which uses `spawn_local` and therefore
/// imposes no `Send` bound on the spawned future. The outer future only
/// awaits `Notify` + reads a `Mutex<Option<...>>` slot. See the module
/// header for the full justification.
///
/// Taking the pool by `Arc<...>` (rather than `&self`) lets the spawned
/// dial task keep the pool alive without borrowing through a method
/// signature.
pub(crate) async fn get_or_open<P>(
    pool: Arc<ProxyConnectionPool<P>>,
    logical: &str,
) -> Result<Arc<ConnectionShared>, EngineError>
where
    P: Providers + Send + Sync,
{
    let key: PoolKey = (logical.to_owned(), pool.factory.addr.clone());

    // Fast path or race-resolution decision under the lock.
    let pending = {
        let mut entries = pool.entries.lock();
        if let Some(state) = entries.get(&key).cloned() {
            match &*state {
                EntryState::Ready { shared, .. } => return Ok(shared.clone()),
                EntryState::Pending(pending) => pending.handles(),
            }
        } else {
            let pending = PendingDial::new();
            let handles = pending.handles();
            entries.insert(key.clone(), Arc::new(EntryState::Pending(pending)));
            drop(entries);
            spawn_dial(
                pool.clone(),
                logical.to_owned(),
                key.clone(),
                handles.handles(),
            );
            handles
        }
    };

    // Park until the dial task publishes the outcome.
    loop {
        if let Some(outcome) = pending.result.lock().as_ref().map(Arc::clone) {
            return match &*outcome {
                Ok(shared) => Ok(shared.clone()),
                Err(err) => Err(clone_engine_error(err)),
            };
        }
        pending.notify.notified().await;
    }
}

/// Spawn the dial + handshake + supervised-driver task. The task runs on
/// the moonpool [`TaskProvider`] (single-thread `spawn_local` semantics),
/// so the `?Send` `network.connect(...)` inside [`build_entry_async`]
/// doesn't propagate back to the caller's future.
fn spawn_dial<P>(
    pool: Arc<ProxyConnectionPool<P>>,
    logical: String,
    key: PoolKey,
    pending: PendingDial,
) where
    P: Providers + Send + Sync,
{
    let factory = pool.factory.clone();
    let task = pool.factory.providers.task().clone();
    // `spawn_task` returns a `JoinHandle<()>`; we deliberately detach the
    // task — its lifetime is bound by the pool's `Arc<...>` and the
    // outcome it produces is delivered to waiters via `pending.notify`
    // and the entries-map promotion below. Drop, don't `.await`.
    let _detached = task.spawn_task("magnetar-moonpool-pool-dial", async move {
        let outcome = build_entry_async::<P>(&factory, &logical).await;
        // Publish the outcome to waiters BEFORE swapping the entry-state
        // to Ready/Removed, so a freshly-polling waiter sees the slot
        // populated either via the `notify` wake-up (parked waiters) or
        // on its first peek (waiters that arrived after `notify_waiters`
        // already fired).
        let outcome_for_waiters: Arc<DialOutcome> = Arc::new(match &outcome {
            Ok((shared, _)) => Ok(shared.clone()),
            Err(err) => Err(clone_engine_error(err)),
        });
        *pending.result.lock() = Some(outcome_for_waiters);
        pending.notify.notify_waiters();

        // Promote the entry from Pending → Ready, or evict on error so a
        // subsequent `open_producer` / `subscribe` call re-dials instead of
        // forever returning the same cached error. Mirrors the implicit
        // behaviour the tokio engine gets from `build_entry` running inside
        // `get_or_open` (no entry is registered on failure paths).
        let mut map = pool.entries.lock();
        if let Ok((shared, driver)) = outcome {
            map.insert(
                key,
                Arc::new(EntryState::Ready {
                    shared,
                    driver: Mutex::new(Some(driver)),
                }),
            );
        } else {
            map.remove(&key);
        }
    });
}

/// Internal: dial + handshake + spawn supervised driver. Returns the
/// `(shared, driver)` pair the pool entry will own. This function is `?Send`
/// because `Transport::connect_with_resolver` calls `network.connect(...)`,
/// which moonpool declares `#[async_trait(?Send)]`. It is therefore only
/// called from inside a `spawn_task`-spawned task whose future is not
/// required to be `Send`.
async fn build_entry_async<P: Providers>(
    factory: &ConnectionFactory<P>,
    logical: &str,
) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
    // Per-entry ConnectionConfig: clone the bootstrap, override the
    // `proxy_to_broker_url` so `CommandConnect` carries the logical broker
    // URL on this connection.
    let mut cfg = factory.bootstrap_config.clone();
    cfg.proxy_to_broker_url = Some(logical.to_owned());

    let mut transport = Transport::<P>::connect_with_resolver(
        factory.providers.network(),
        &factory.addr,
        factory.dns_resolver.as_deref(),
    )
    .await?;

    let shared = make_shared_with_providers::<P>(&factory.providers, cfg);
    handshake_plain::<P>(&shared, &mut transport).await?;

    let ctx = ReconnectContext {
        host_port: factory.addr.clone(),
        service_url_provider: factory.service_url_provider.clone(),
        dns_resolver: factory.dns_resolver.clone(),
    };
    let driver =
        spawn_supervised_driver::<P>(shared.clone(), transport, ctx, factory.providers.clone());

    Ok((shared, driver))
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
            providers: TokioProviders::new(),
            service_url_provider: None,
            dns_resolver: None,
        }
    }

    // 1:1 parity with the tokio engine's `pool.rs` unit suite
    // (`crates/magnetar-runtime-tokio/src/pool.rs`): two tests, `fresh_pool_is_empty`
    // and a Debug-format smoke. The end-to-end pool behaviour is covered by the
    // integration test (`tests/proxy_multi_conn.rs`) — adding extra moonpool-only
    // unit tests here would trip the ADR-0024 parity gate.

    #[test]
    fn fresh_pool_is_empty() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn debug_includes_pool_state() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        let s = format!("{pool:?}");
        assert!(s.contains("ProxyConnectionPool"));
        assert!(s.contains("entries"));
    }
}
