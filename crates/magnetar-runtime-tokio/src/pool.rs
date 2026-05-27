// SPDX-License-Identifier: Apache-2.0

//! Per-broker connection pool for the Apache Pulsar Proxy (ADR-0039).
//!
//! When a `CommandLookupTopic` answer carries
//! `proxy_through_service_url = true`, the proxy expects the client to open a
//! **new** connection back to the proxy address (the "physical" target) with
//! `CommandConnect.proxy_to_broker_url` set to the resolved broker URL (the
//! "logical" target). The proxy then forwards every frame on that connection to
//! the resolved broker.
//!
//! This module mirrors the upstream Java client's
//! [`ConnectionPool`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java):
//! a `HashMap<(logical_address, physical_address), Arc<ConnectionShared>>`
//! keyed by the broker URL (logical) and proxy URL (physical). Each entry
//! owns its own supervised driver loop (ADR-0028 anti-thrash + ADR-0024
//! per-handle backoff still apply unchanged) and stays alive for the lifetime
//! of the `Client`.
//!
//! The pool is opt-in by topology: when every lookup answer reports
//! `proxy_through_service_url = false` (single-broker setups, no proxy), the
//! pool stays empty and behaviour is byte-identical to the pre-ADR-0039
//! single-connection client.
//!
//! See [ADR-0039](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md)
//! for the design and [issue #15](https://github.com/CleverCloud/magnetar/issues/15)
//! for the motivating incident.

use std::collections::HashMap;
use std::sync::Arc;

use magnetar_proto::ConnectionConfig;
use parking_lot::Mutex;

use crate::ConnectionShared;
use crate::dns::DnsResolver;
use crate::driver::{DriverHandle, ReconnectContext, spawn_supervised as spawn_supervised_driver};
use crate::error::ClientError;
use crate::transport::Transport;
use crate::url_parse::{ParsedUrl, Scheme};

/// Building blocks the pool re-uses when it has to lazily dial a new
/// `ConnectionShared`. Captures the same surface
/// [`crate::Client::connect_with_resolver_and_provider`] passes through, minus
/// the per-entry `proxy_to_broker_url` (which the pool sets itself when it
/// opens an entry).
#[derive(Clone)]
pub(crate) struct ConnectionFactory {
    /// The proxy / broker URL the bootstrap connection dialled. Every pool
    /// entry dials the **same** physical address — only the per-entry
    /// `CommandConnect.proxy_to_broker_url` differs. Mirrors the Java
    /// pool's `physicalAddress`.
    pub(crate) url: ParsedUrl,
    /// rustls config (shared `Arc`). `None` for plain `pulsar://` URLs.
    pub(crate) tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Template `ConnectionConfig` cloned per entry. The pool overrides
    /// `proxy_to_broker_url` on the clone; everything else (auth, supervisor,
    /// memory limit, etc.) carries over.
    pub(crate) bootstrap_config: ConnectionConfig,
    /// Optional in-band auth provider for `CommandAuthChallenge`. Each pool
    /// entry shares the same provider, so a refreshed token propagates
    /// naturally across every pinned broker connection.
    pub(crate) auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    /// PIP-121 `ServiceUrlProvider` — when set, every pool entry's supervisor
    /// re-resolves the broker URL via the provider on every reconnect attempt.
    pub(crate) service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    /// Pluggable DNS resolver (Java `ClientBuilder#dnsResolver`).
    pub(crate) dns_resolver: Option<Arc<dyn DnsResolver>>,
}

impl std::fmt::Debug for ConnectionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `bootstrap_config` is omitted — `ConnectionConfig` is a verbose
        // bundle and the surface is meant for diagnostics, not full
        // round-tripping. `finish_non_exhaustive` silences
        // `clippy::missing_fields_in_debug`.
        f.debug_struct("ConnectionFactory")
            .field("url", &self.url)
            .field("tls", &self.tls_config.is_some())
            .field("has_auth_provider", &self.auth_provider.is_some())
            .field(
                "has_service_url_provider",
                &self.service_url_provider.is_some(),
            )
            .field("has_dns_resolver", &self.dns_resolver.is_some())
            .finish_non_exhaustive()
    }
}

/// Composite key — the Java client uses an `InetSocketAddress`-typed
/// `(logical, physical, randomKey)` triple
/// ([`ConnectionPool.Key`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java#L99)).
/// We collapse to `(logical, physical)` for v0.1 (`randomKey` multiplexing is
/// follow-up #—): magnetar's per-handle slot mutex (ADR-0038) already
/// parallelises the hot path inside one connection, so the extra fan-out gain
/// from running N parallel connections per broker is not worth the API
/// complexity until we measure contention warranting it.
type PoolKey = (String, String);

/// Tracking entry inside the pool. Owns the supervised driver handle so
/// `Client::close` can join every spawned task on shutdown.
struct Entry {
    shared: Arc<ConnectionShared>,
    /// `Some` while the driver is running; `None` after `Client::close` joined
    /// it. The handle is taken out under `entries_lock` + `driver` field's own
    /// `Mutex` discipline.
    driver: Mutex<Option<DriverHandle>>,
}

/// Pool of `ConnectionShared` keyed by `(logical broker URL, physical dial
/// URL)`. See module docs.
pub(crate) struct ProxyConnectionPool {
    factory: ConnectionFactory,
    /// `parking_lot::Mutex` per ADR-0003 / repo convention. Critical sections
    /// are short (HashMap mutations + clones of `Arc<Entry>`).
    entries: Mutex<HashMap<PoolKey, Arc<Entry>>>,
}

impl std::fmt::Debug for ProxyConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot: Vec<_> = self.entries.lock().keys().cloned().collect();
        f.debug_struct("ProxyConnectionPool")
            .field("factory", &self.factory)
            .field("entries", &snapshot)
            .finish()
    }
}

impl ProxyConnectionPool {
    pub(crate) fn new(factory: ConnectionFactory) -> Arc<Self> {
        Arc::new(Self {
            factory,
            entries: Mutex::new(HashMap::new()),
        })
    }

    /// Snapshot of the currently-tracked `(logical, physical)` pairs — used by
    /// tests and diagnostics.
    #[cfg(test)]
    pub(crate) fn keys(&self) -> Vec<(String, String)> {
        self.entries.lock().keys().cloned().collect()
    }

    /// Get or lazily open the pool entry for `(logical, physical)`. The
    /// returned `Arc<ConnectionShared>` is **already handshaked** —
    /// `CommandConnected` has fired, so the caller can immediately queue
    /// `CommandProducer` / `CommandSubscribe` on it.
    ///
    /// `logical` is the broker URL the proxy resolved to (the value passed
    /// into `CommandConnect.proxy_to_broker_url`). `physical` is the URL
    /// magnetar dials — almost always the original `service_url` the client
    /// was built with (i.e. the proxy address).
    ///
    /// Concurrency: if two callers race for the same key, only one connection
    /// is opened; the loser drops its half-built `Entry` and gets the
    /// winner's `Arc`.
    pub(crate) async fn get_or_open(
        &self,
        logical: &str,
        physical: &ParsedUrl,
    ) -> Result<Arc<ConnectionShared>, ClientError> {
        let key: PoolKey = (
            logical.to_owned(),
            format!("{}:{}", physical.host, physical.port),
        );

        // Fast path — already open.
        if let Some(entry) = self.entries.lock().get(&key) {
            return Ok(entry.shared.clone());
        }

        // Slow path — dial, handshake, register. `Transport::connect_with_resolver`
        // and the supervisor's handshake-wait both `.await`, so we MUST NOT
        // hold the entries-lock across them.
        let entry = self.build_entry(logical, physical).await?;

        // Race resolution: another caller may have populated the key while we
        // were dialling. The winner stays; we drop ours.
        {
            let mut entries = self.entries.lock();
            if let Some(existing) = entries.get(&key) {
                let shared = existing.shared.clone();
                drop(entries);
                // Best-effort tear-down of the loser entry. The driver future
                // is still inside `entry.driver`; aborting it cleans up the
                // task.
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

    async fn build_entry(
        &self,
        logical: &str,
        physical: &ParsedUrl,
    ) -> Result<Arc<Entry>, ClientError> {
        // Per-entry ConnectionConfig: clone the bootstrap, override the
        // `proxy_to_broker_url` so `CommandConnect` carries the logical
        // broker URL on this connection. Everything else (auth, supervisor,
        // memory limit, schema, etc.) tracks the bootstrap config 1:1.
        let mut cfg = self.factory.bootstrap_config.clone();
        cfg.proxy_to_broker_url = Some(logical.to_owned());

        let socket = Transport::connect_with_resolver(
            physical,
            self.factory.tls_config.clone(),
            self.factory.dns_resolver.as_deref(),
        )
        .await?;

        let shared = ConnectionShared::with_auth(cfg, self.factory.auth_provider.clone());

        // Kick off the CONNECT frame before the driver starts reading, so the
        // driver's first poll has something to flush.
        shared.inner.lock().begin_handshake()?;
        shared.driver_waker.notify_one();

        let ctx = ReconnectContext {
            url: physical.clone(),
            tls_config: self.factory.tls_config.clone(),
            service_url_provider: self.factory.service_url_provider.clone(),
            dns_resolver: self.factory.dns_resolver.clone(),
        };
        let driver = spawn_supervised_driver(shared.clone(), socket, ctx);

        // Block until the new socket has finished its handshake — the caller
        // expects a ready-to-use connection.
        if let Err(err) = crate::client::wait_connected(shared.clone()).await {
            driver.abort();
            return Err(err);
        }

        Ok(Arc::new(Entry {
            shared,
            driver: Mutex::new(Some(driver)),
        }))
    }

    /// Close every pool entry. Calls `Connection::close` on each shared
    /// state-machine, wakes the supervisors, then joins their driver tasks.
    /// Idempotent: a second call is a no-op (entries cleared after the
    /// first).
    pub(crate) async fn close(&self) {
        // Snapshot under-lock so we don't hold the lock across `.await`.
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

    /// Number of currently-tracked entries. Exposed for diagnostics + tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Scheme of the bootstrap dial address — used by
    /// [`crate::Client::lookup_topic`] to pick between `broker_service_url` and
    /// `broker_service_url_tls` returned by the proxy.
    #[must_use]
    pub(crate) fn bootstrap_scheme(&self) -> Scheme {
        self.factory.url.scheme
    }

    /// Clone the bootstrap [`ParsedUrl`] — used by the [`crate::Client`] to feed
    /// [`Self::get_or_open`] (every pool entry dials the same physical address).
    #[must_use]
    pub(crate) fn bootstrap_url(&self) -> ParsedUrl {
        self.factory.url.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::url_parse::Scheme;

    fn dummy_factory() -> ConnectionFactory {
        ConnectionFactory {
            url: ParsedUrl {
                host: "broker.example.com".to_owned(),
                port: 6650,
                scheme: Scheme::Plain,
            },
            tls_config: None,
            bootstrap_config: ConnectionConfig {
                operation_timeout: Duration::from_secs(30),
                ..ConnectionConfig::default()
            },
            auth_provider: None,
            service_url_provider: None,
            dns_resolver: None,
        }
    }

    #[test]
    fn fresh_pool_is_empty() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        assert_eq!(pool.len(), 0);
        assert!(pool.keys().is_empty());
    }

    #[test]
    fn debug_includes_keys_snapshot() {
        let pool = ProxyConnectionPool::new(dummy_factory());
        let s = format!("{pool:?}");
        // Debug shouldn't panic and should mention the type.
        assert!(s.contains("ProxyConnectionPool"));
        assert!(s.contains("entries"));
    }

    // End-to-end tests that actually dial a fake broker through the proxy
    // live in `tests/proxy_multi_conn.rs` — they need the runtime's full
    // dial path which can't be exercised from inside a unit test module
    // without re-implementing the driver wiring.
}
