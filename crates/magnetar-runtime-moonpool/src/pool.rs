// SPDX-License-Identifier: Apache-2.0

//! Per-broker connection pool for the Apache Pulsar Proxy (ADR-0039),
//! moonpool engine flavour.
//!
//! Mirror of [`magnetar_runtime_tokio::pool`]. Stays generic over
//! [`moonpool_core::Providers`] so the pool behaves identically in production
//! `TokioProviders` runs and `moonpool-sim` deterministic substrates.
//!
//! See [`magnetar_runtime_tokio::pool`] for the design notes — both engines
//! pull the same shared contract out of `magnetar-proto`'s
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`] +
//! [`ConnectionConfig::proxy_to_broker_url`].
//!
//! [`LookupOutcome::Connect { proxy_through_service_url, .. }`]: magnetar_proto::event::LookupOutcome::Connect
//! [`ConnectionConfig::proxy_to_broker_url`]: magnetar_proto::ConnectionConfig::proxy_to_broker_url

use std::collections::HashMap;
use std::sync::Arc;

use magnetar_proto::ConnectionConfig;
use moonpool_core::Providers;
use parking_lot::Mutex;

use crate::dns::DnsResolver;
use crate::driver::{DriverHandle, ReconnectContext, spawn_supervised as spawn_supervised_driver};
use crate::transport::Transport;
use crate::{ConnectionShared, EngineError, handshake_plain};

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
        f.debug_struct("ConnectionFactory")
            .field("addr", &self.addr)
            .field(
                "has_service_url_provider",
                &self.service_url_provider.is_some(),
            )
            .field("has_dns_resolver", &self.dns_resolver.is_some())
            .finish()
    }
}

type PoolKey = (String, String);

struct Entry {
    shared: Arc<ConnectionShared>,
    driver: Mutex<Option<DriverHandle>>,
}

/// Moonpool pool of `ConnectionShared` keyed by
/// `(logical broker URL, physical dial address)`. See [`crate::pool`] module
/// docs and [`magnetar_runtime_tokio::pool::ProxyConnectionPool`].
pub(crate) struct ProxyConnectionPool<P: Providers> {
    factory: ConnectionFactory<P>,
    entries: Mutex<HashMap<PoolKey, Arc<Entry>>>,
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

    /// Get or lazily open the pool entry for `(logical, physical)`. The
    /// caller passes the broker URL the proxy advertised; the dial target is
    /// always the bootstrap `addr` (the proxy). Returns a handshaked
    /// `Arc<ConnectionShared>` ready for `CommandProducer` /
    /// `CommandSubscribe`.
    pub(crate) async fn get_or_open(
        &self,
        logical: &str,
    ) -> Result<Arc<ConnectionShared>, EngineError> {
        let key: PoolKey = (logical.to_owned(), self.factory.addr.clone());

        if let Some(entry) = self.entries.lock().get(&key) {
            return Ok(entry.shared.clone());
        }

        let entry = self.build_entry(logical).await?;

        {
            let mut entries = self.entries.lock();
            if let Some(existing) = entries.get(&key) {
                let shared = existing.shared.clone();
                drop(entries);
                // Lost the race: tear down our half-built entry.
                if let Some(handle) = entry.driver.lock().take() {
                    handle.abort();
                }
                {
                    let mut conn = entry.shared.inner.lock();
                    conn.close();
                }
                return Ok(shared);
            }
            entries.insert(key, entry.clone());
        }
        Ok(entry.shared.clone())
    }

    async fn build_entry(&self, logical: &str) -> Result<Arc<Entry>, EngineError> {
        let mut cfg = self.factory.bootstrap_config.clone();
        cfg.proxy_to_broker_url = Some(logical.to_owned());

        let mut transport = Transport::<P>::connect_with_resolver(
            self.factory.providers.network(),
            &self.factory.addr,
            self.factory.dns_resolver.as_deref(),
        )
        .await?;

        let shared = ConnectionShared::new(cfg);
        handshake_plain::<P>(&shared, &mut transport).await?;

        let ctx = ReconnectContext {
            host_port: self.factory.addr.clone(),
            service_url_provider: self.factory.service_url_provider.clone(),
            dns_resolver: self.factory.dns_resolver.clone(),
        };
        let driver = spawn_supervised_driver::<P>(
            shared.clone(),
            transport,
            ctx,
            self.factory.providers.clone(),
        );

        Ok(Arc::new(Entry {
            shared,
            driver: Mutex::new(Some(driver)),
        }))
    }

    /// Close every pool entry. Idempotent.
    pub(crate) async fn close(&self) {
        let drained: Vec<Arc<Entry>> = self.entries.lock().drain().map(|(_, v)| v).collect();
        for entry in drained {
            {
                let mut conn = entry.shared.inner.lock();
                conn.close();
            }
            entry.shared.driver_waker.notify_one();
            let handle = entry.driver.lock().take();
            if let Some(handle) = handle {
                let _ = handle.join().await;
            }
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use moonpool_core::TokioProviders;

    use super::*;

    fn dummy_factory() -> ConnectionFactory<TokioProviders> {
        ConnectionFactory {
            addr: "broker.example.com:6650".to_owned(),
            bootstrap_config: ConnectionConfig {
                operation_timeout: Duration::from_secs(30),
                ..ConnectionConfig::default()
            },
            providers: TokioProviders::default(),
            service_url_provider: None,
            dns_resolver: None,
        }
    }

    use std::time::Duration;

    #[test]
    fn fresh_pool_is_empty() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn factory_clone_preserves_addr() {
        let factory = dummy_factory();
        let cloned = factory.clone();
        assert_eq!(factory.addr, cloned.addr);
        assert_eq!(
            factory.bootstrap_config.operation_timeout,
            cloned.bootstrap_config.operation_timeout
        );
    }

    #[test]
    fn debug_includes_pool_state() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        let s = format!("{pool:?}");
        assert!(s.contains("ProxyConnectionPool"));
        assert!(s.contains("entries"));
    }

    #[test]
    fn factory_debug_does_not_leak_providers() {
        let factory = dummy_factory();
        let s = format!("{factory:?}");
        assert!(s.contains("ConnectionFactory"));
        assert!(s.contains("broker.example.com:6650"));
        // The providers bundle is intentionally NOT in Debug output —
        // it's a verbose handle bundle, not config metadata.
        assert!(!s.contains("TokioProviders"));
    }

    #[test]
    fn pool_arc_is_clone() {
        // Sanity that the `Arc<Self>` returned by `new` is cheaply
        // shareable — the engine creates one per `Client` and the
        // bootstrap connect path needs to hand the same Arc to every
        // future open-producer / open-consumer call.
        let pool = ProxyConnectionPool::new(dummy_factory());
        let cloned: Arc<ProxyConnectionPool<TokioProviders>> = pool.clone();
        assert_eq!(pool.len(), cloned.len());
    }
}
