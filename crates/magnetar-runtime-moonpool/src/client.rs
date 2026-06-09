// SPDX-License-Identifier: Apache-2.0

//! Top-level `Client` façade for the moonpool engine.
//!
//! Mirrors [`magnetar_runtime_tokio::Client`] but is generic over
//! [`moonpool_core::Providers`] so the same façade runs on production tokio
//! sockets and on a `moonpool-sim` deterministic substrate.
//!
//! ## M2 surface
//!
//! - [`Client::connect_plain`] — TCP-only handshake.
//! - [`Client::close`] / [`Client::is_closed`] / [`Client::is_connected`].
//! - [`Client::lookup_topic`] — `CommandLookupTopic` round-trip.
//! - [`Client::partitioned_topic_metadata`] — partition count.
//! - [`Client::watch_topic_list`] — PIP-145 watcher subscribe (initial snapshot).
//! - [`Client::next_topic_list_change`] — PIP-145 watcher delta stream.
//!
//! Producer / Consumer façades land in M3 / M4. TLS and reconnect land in
//! later milestones.
//!
//! ## No-channels invariant
//!
//! Futures here follow the same pattern as the tokio engine: park on the
//! sans-io `Connection`'s `Waker` slab via
//! [`magnetar_proto::Connection::register_waker`], or — for event-stream-style
//! polling such as [`Client::next_topic_list_change`] — on a
//! [`tokio::sync::Notify`]. No `mpsc` / `oneshot` / `watch` / `broadcast`
//! channels of any flavour. See `GUIDELINES.md` §"No-channels rule".

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use magnetar_proto::event::LookupOutcome;
use magnetar_proto::{ConnectionConfig, OpOutcome, PendingOpKey, RequestId};
use moonpool_core::Providers;
use parking_lot::Mutex;

use crate::driver::DriverHandle;
use crate::pool::ProxyConnectionPool;
use crate::{ConnectionShared, EngineError, MoonpoolEngine, TopicListChange};

/// Engine-layer error surfaced by [`Client`]. Wraps [`EngineError`] with a
/// dedicated `Broker` variant for request-correlated server errors so the
/// surface matches the tokio engine's `ClientError`.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Underlying socket / TLS / protocol failure surfaced by the moonpool
    /// engine.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// Generic broker error correlated with a pending request.
    #[error("broker error: code={code} message={message}")]
    Broker {
        /// Pulsar wire-protocol `ServerError` code.
        code: i32,
        /// Broker-supplied error string.
        message: String,
    },
    /// The connection has been locally closed before the request completed.
    #[error("connection is closed")]
    Closed,
    /// The peer closed the connection with no recovery path: a plain
    /// (non-supervised) driver hit a terminal drop and resolved every pending
    /// op with [`magnetar_proto::OpOutcome::Terminal`]. Mirrors the tokio
    /// engine's `ClientError::PeerClosed`.
    #[error("peer closed the connection")]
    PeerClosed,
    /// A lookup answered `proxy_through_service_url = true` but the client has no proxy
    /// connection pool because it was built via [`Client::connect_plain`] or
    /// [`Client::from_parts`] (no supervisor → no pool — each pool entry needs its own
    /// supervised driver loop). Switch to [`Client::connect_plain_supervised`] to use
    /// the pool. See ADR-0039.
    #[error(
        "lookup of topic '{topic}' requires proxy routing (proxy_through_service_url=true) \
         but this moonpool client was built without a supervisor; rebuild with \
         Client::connect_plain_supervised"
    )]
    ProxyUnsupportedOnUnsupervisedClient {
        /// The topic whose lookup triggered the proxy-routing requirement.
        topic: String,
    },

    /// Catch-all for engine-internal misconfiguration.
    #[error("other: {0}")]
    Other(String),
}

/// Outcome of a [`Client::lookup_topic`] call.
///
/// Re-export of [`magnetar_proto::event::LookupOutcome`]. The state machine
/// has already followed any `Redirect` chain internally; the user **only**
/// sees a terminal outcome — `Connect` or `Failed`. Intermediate `Redirected`
/// variants ride the proto events queue for diagnostics only and never
/// resolve the user-facing future (HIGH-4 from the lookup multi-agent review).
pub type LookupTopicResult = LookupOutcome;

/// Top-level magnetar client, moonpool engine flavour.
///
/// Holds the shared connection state plus the driver task handle. Generic
/// over the [`Providers`] bundle so callers can plug in `TokioProviders` in
/// production or a `moonpool-sim` bundle in tests.
pub struct Client<P: Providers> {
    shared: Arc<ConnectionShared>,
    driver: Mutex<Option<DriverHandle>>,
    /// Per-broker proxy connection pool (ADR-0039). Populated only when the
    /// client was built via [`Client::connect_plain_supervised`] (which
    /// captures the providers + bootstrap config needed to lazily dial pool
    /// entries). The other connect entrypoints — [`Client::connect_plain`]
    /// and [`Client::from_parts`] — leave this `None`, so a lookup answering
    /// `proxy_through_service_url = true` on those paths still surfaces
    /// [`ClientError::ProxyUnsupportedOnUnsupervisedClient`].
    pool: Option<Arc<ProxyConnectionPool<P>>>,
    /// Held only so `Client` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers.
    _providers: std::marker::PhantomData<fn() -> P>,
}

/// Decision returned by [`Client::lookup_topic_target`] driving where the data ops for the
/// resolved topic should ride (ADR-0039). Mirror of the tokio engine's `LookupTarget` — the
/// moonpool [`Client::lookup_topic`] accessor still returns the raw `LookupOutcome` so existing
/// callers keep their full proto view; runtime code (producer / consumer open paths) uses
/// this routing-decision enum instead.
///
/// Both routing shapes ride through the moonpool [`ProxyConnectionPool`] (see
/// [`Client::resolve_target`]). ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)" documents
/// the symmetry with the tokio engine.
#[derive(Debug, Clone)]
pub(crate) enum LookupTarget {
    /// Direct connection.
    /// * `broker_url = None` — no broker URL advertised; the bootstrap connection serves as the
    ///   data plane.
    /// * `broker_url = Some(url)` — the lookup resolved to a specific broker. Routed through the
    ///   [`ProxyConnectionPool`] with `CommandConnect.proxy_to_broker_url = None` (dialling the
    ///   broker directly), unless `url` matches the bootstrap's `host:port` — in which case the
    ///   bootstrap-equality fast path reuses the bootstrap connection (parity with Java's
    ///   pool-identity check).
    Direct {
        #[allow(dead_code)]
        broker_url: Option<String>,
    },
    /// Proxy-routed: a pool entry dialling the bootstrap (proxy) address with
    /// `CommandConnect.proxy_to_broker_url = Some(broker_url)`.
    Proxy {
        #[allow(dead_code)]
        broker_url: String,
    },
}

impl<P: Providers> std::fmt::Debug for Client<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<P: Providers> Client<P> {
    /// Connect to a Pulsar broker over the moonpool [`NetworkProvider`] and
    /// run the plaintext handshake.
    ///
    /// `addr` is a moonpool `host:port` string (NOT a `pulsar://` URL — strip
    /// the scheme before calling). For TLS, use [`MoonpoolEngine::connect_tls`]
    /// (backed by `RustlsByteAdapter` over the moonpool byte pipe).
    ///
    /// Returns once the broker has responded with `CommandConnected`.
    ///
    /// # Errors
    /// Surfaces [`EngineError`] flavours wrapped in
    /// [`ClientError::Engine`].
    ///
    /// [`NetworkProvider`]: moonpool_core::NetworkProvider
    pub async fn connect_plain(
        engine: &MoonpoolEngine<P>,
        addr: &str,
        config: ConnectionConfig,
    ) -> Result<Self, ClientError> {
        let (shared, driver) = engine.connect_plain(addr, config).await?;
        Ok(Self {
            shared,
            driver: Mutex::new(Some(driver)),
            pool: None,
            _providers: std::marker::PhantomData,
        })
    }

    /// Connect via the supervised driver. When [`ConnectionConfig::supervisor`]
    /// is `Some`, the driver auto-reconnects on transient socket failures
    /// using the moonpool [`Providers`]; sleeps go through
    /// [`moonpool_core::TimeProvider::sleep`] so the backoff schedule is
    /// deterministic under `moonpool-sim`.
    ///
    /// `service_url_provider` is the PIP-121 cluster-failover hook —
    /// when `Some`, every reconnect attempt polls the provider for a fresh
    /// `pulsar://host:port` (or `pulsar+ssl://host:port`) URL before
    /// dialling. Use [`magnetar_proto::ControlledClusterFailover`] for
    /// externally-driven URL swaps; the runtime polls it synchronously.
    /// `dns_resolver` mirrors Java's `ClientBuilder#dnsResolver`.
    ///
    /// # Errors
    /// Same envelope as [`Self::connect_plain`].
    pub async fn connect_plain_supervised(
        engine: &MoonpoolEngine<P>,
        addr: &str,
        config: ConnectionConfig,
        service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
        dns_resolver: Option<Arc<dyn crate::DnsResolver>>,
    ) -> Result<Self, ClientError> {
        let (shared, driver) = engine
            .connect_plain_supervised(
                addr,
                config.clone(),
                service_url_provider.clone(),
                dns_resolver.clone(),
            )
            .await?;
        // ADR-0039: capture the bootstrap inputs into a `ConnectionFactory`
        // so the proxy pool can lazily dial per-broker pinned connections
        // when a `proxy_through_service_url = true` lookup arrives. The
        // bootstrap connection itself does NOT set `proxy_to_broker_url`
        // (it stays the lookup-and-control plane).
        let factory = crate::pool::ConnectionFactory {
            addr: addr.to_owned(),
            bootstrap_config: config,
            providers: engine.providers().clone(),
            service_url_provider,
            dns_resolver,
        };
        let pool = ProxyConnectionPool::new(factory);
        Ok(Self {
            shared,
            driver: Mutex::new(Some(driver)),
            pool: Some(pool),
            _providers: std::marker::PhantomData,
        })
    }

    /// Wrap an existing `(shared, driver)` pair produced by
    /// [`MoonpoolEngine::connect_plain`] (or its supervised / TLS
    /// variants) into a [`Client`].
    ///
    /// Mirrors the inline construction inside [`Self::connect_plain`]
    /// and friends — exposed so the `magnetar` façade can use a
    /// [`Client`] as the engine's `ClientState` without going through
    /// one of the connect helpers (e.g. when callers want full control
    /// over which engine method connects, or want to test the surface
    /// against a hand-rolled connection).
    #[must_use]
    pub fn from_parts(shared: Arc<ConnectionShared>, driver: DriverHandle) -> Self {
        Self {
            shared,
            driver: Mutex::new(Some(driver)),
            pool: None,
            _providers: std::marker::PhantomData,
        }
    }

    /// Surrender the driver handle, leaving the [`Client`] without a
    /// driver to abort on [`Self::close`]. Mirrors
    /// `PulsarClient::<MoonpoolEngine<P>>::take_driver` — exposed so the
    /// façade can delegate without re-implementing the take.
    #[must_use]
    pub fn take_driver(&self) -> Option<DriverHandle> {
        self.driver.lock().take()
    }

    /// Borrow the shared connection state. Mostly useful for tests and
    /// instrumentation.
    #[must_use]
    pub fn shared(&self) -> &Arc<ConnectionShared> {
        &self.shared
    }

    /// `true` while the underlying broker connection is in
    /// [`magnetar_proto::HandshakeState::Connected`]. Mirrors Java
    /// `Producer/Consumer#isConnected` at the connection scope — the moonpool
    /// engine shares a single connection across producers/consumers, so the
    /// same predicate answers both.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// `true` once [`Self::close`] has been called or the broker connection
    /// has otherwise entered a terminal state. Mirrors Java
    /// `PulsarClient#isClosed`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.inner.lock().is_closed()
    }

    /// Close the connection. Drains outbound bytes via the driver loop and
    /// then joins the driver task. If the client owns a proxy pool
    /// (ADR-0039), every pool entry is closed and its supervised driver
    /// joined as part of teardown.
    ///
    /// Idempotent: calling close more than once is a no-op on subsequent
    /// calls (the driver handle is taken on the first call).
    pub async fn close(self)
    where
        P: Send + Sync,
    {
        {
            let mut conn = self.shared.inner.lock();
            conn.close();
        }
        self.shared.driver_waker.notify_one();
        let handle = self.driver.lock().take();
        if let Some(handle) = handle {
            // best-effort close — drop the driver's terminal error.
            let _ = handle.join().await;
        }
        // Tear down the proxy pool (ADR-0039). Pool entries are independent
        // supervised driver loops; each observes its own `is_user_closed()`
        // after `close()` is called and exits cleanly.
        if let Some(pool) = self.pool.as_ref() {
            pool.close().await;
        }
    }

    /// Resolve a `LookupOutcome::Connect` into a routing decision (ADR-0039). When the proxy
    /// advertises `proxy_through_service_url = true`, the data ops MUST ride on a pinned
    /// per-broker pool entry; otherwise the bootstrap connection is the data plane.
    pub(crate) async fn lookup_topic_target(
        &self,
        topic: &str,
    ) -> Result<LookupTarget, ClientError> {
        let outcome = self.lookup_topic(topic, false).await?;
        match outcome {
            LookupOutcome::Connect {
                broker_service_url,
                broker_service_url_tls,
                proxy_through_service_url,
            } => {
                if proxy_through_service_url {
                    // Lookup-driven reconnects on the moonpool engine ride the plaintext
                    // bootstrap pipe even when both URLs are advertised — TLS routing on the
                    // pinned per-broker pool is wired through the engine's `connect_tls`
                    // entry, not through `lookup_topic_target`. Prefer the plain
                    // `broker_service_url` here for that reason.
                    //
                    // The advertised value is normalised to `host:port` via
                    // [`proxy_broker_authority`] before being captured in
                    // [`LookupTarget::Proxy`] so the (currently follow-up) moonpool proxy path
                    // produces the same `CommandConnect.proxy_to_broker_url` wire bytes as the
                    // tokio engine (see `magnetar_runtime_tokio::client::preferred_broker_url`
                    // and ADR-0039).
                    let raw = broker_service_url
                        .or(broker_service_url_tls)
                        .ok_or_else(|| {
                            ClientError::Other(format!(
                                "lookup of '{topic}' set proxy_through_service_url=true but did \
                             not advertise a broker_service_url"
                            ))
                        })?;
                    let broker_url = proxy_broker_authority(&raw);
                    Ok(LookupTarget::Proxy { broker_url })
                } else {
                    // ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)": capture the
                    // resolved broker URL even on the DIRECT branch so the proto-level
                    // routing decision matches the tokio engine. The synchronous
                    // [`Self::resolve_target`] handles the rest (bootstrap match → reuse
                    // bootstrap; other → defer to the moonpool pool follow-up).
                    let broker_url = broker_service_url.or(broker_service_url_tls);
                    Ok(LookupTarget::Direct { broker_url })
                }
            }
            // HIGH-4 (lookup multi-agent review): `Redirected` is never delivered to the
            // user-facing future — the proto state machine chases the chain internally and
            // only publishes terminal outcomes (`Connect` / `Failed`) against the
            // user-facing request-id. Intermediate `Redirected` outcomes ride the proto
            // events queue for diagnostics only. This arm exists solely to keep the match
            // exhaustive (future-proofing) and would only fire on a state-machine bug.
            LookupOutcome::Redirected { .. } => Err(ClientError::Other(
                "BUG: intermediate Redirected outcome leaked to the user-facing future — \
                 proto layer should chase redirects internally and only deliver terminal \
                 outcomes (HIGH-4)"
                    .to_owned(),
            )),
            LookupOutcome::Failed { code, message } => Err(ClientError::Broker { code, message }),
        }
    }

    /// Resolve a [`LookupTarget`] to the `Arc<ConnectionShared>` the caller should drive
    /// CommandProducer / CommandSubscribe on (ADR-0039).
    ///
    /// * [`LookupTarget::Direct { broker_url: None }`] — bootstrap connection (no broker URL was
    ///   advertised; single-broker behaviour).
    /// * [`LookupTarget::Direct { broker_url: Some(url) }`] — multi-broker DIRECT routing. If
    ///   `url`'s `host:port` matches the bootstrap, reuse the bootstrap. Otherwise open (or reuse)
    ///   a pool entry keyed by `(url, host:port)` and dial the resolved broker directly
    ///   (`CommandConnect.proxy_to_broker_url = None`). ADR-0039 §"Multi-broker DIRECT routing
    ///   (2026-06-01)".
    /// * [`LookupTarget::Proxy { broker_url }`] — opens (or reuses) the pool entry keyed by
    ///   `(broker_url, bootstrap host:port)` with `CommandConnect.proxy_to_broker_url =
    ///   Some(broker_url)`.
    ///
    /// **Send-safety on moonpool**: `moonpool_core::NetworkProvider` is
    /// declared `#[async_trait(?Send)]` (single-core design), so dialling a
    /// fresh pool entry inside this future body would break the
    /// `Pin<Box<dyn Future + Send>>` pin on the facade's
    /// [`crate::CreateProducerApi`] / [`crate::SubscribeApi`] trait methods.
    /// The pool side-steps that by hoisting the dial into a task spawned via
    /// [`moonpool_core::TaskProvider::spawn_task`] (which uses
    /// `spawn_local` — no `Send` bound on the spawned future); this function
    /// only `.await`s a [`tokio::sync::Notify`] and a `Mutex<Option<...>>`
    /// slot, both of which are `Send`. See `crate::pool::get_or_open` for
    /// the full mechanism.
    pub(crate) async fn resolve_target(
        &self,
        target: &LookupTarget,
        topic: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError>
    where
        P: Send + Sync,
    {
        match target {
            LookupTarget::Direct { broker_url: None } => Ok(self.shared.clone()),
            LookupTarget::Direct {
                broker_url: Some(broker_url),
            } => self.resolve_direct_broker(broker_url, topic).await,
            LookupTarget::Proxy { broker_url } => {
                let pool = self.pool.as_ref().ok_or_else(|| {
                    ClientError::ProxyUnsupportedOnUnsupervisedClient {
                        topic: topic.to_owned(),
                    }
                })?;
                // Proxy entries dial the same physical address — the proxy URL the bootstrap was
                // built with. `CommandConnect.proxy_to_broker_url = Some(broker_url)` tells the
                // proxy which backend broker this connection serves.
                let physical = pool.bootstrap_addr().to_owned();
                let shared = crate::pool::get_or_open(
                    pool.clone(),
                    broker_url,
                    &physical,
                    Some(broker_url.clone()),
                )
                .await?;
                Ok(shared)
            }
        }
    }

    /// Resolve a multi-broker DIRECT routing target. If the resolved broker's `host:port` matches
    /// the bootstrap's `host:port`, the bootstrap connection is reused (no extra dial). Otherwise
    /// the pool opens (or reuses) a pinned connection that dials the broker directly with
    /// `CommandConnect.proxy_to_broker_url = None`. ADR-0039 §"Multi-broker DIRECT routing
    /// (2026-06-01)".
    ///
    /// `broker_url` may be a full Pulsar URL (`pulsar://host:port` / `pulsar+ssl://host:port`) or a
    /// bare `host:port` pair. Both forms must round-trip to the same parsed `host:port` for the
    /// bootstrap-equality check to bypass the pool dial.
    ///
    /// Falls back to the bootstrap connection when the moonpool client was built without a
    /// supervisor (no pool) — single-broker scenarios still work; multi-broker dial requests would
    /// have nowhere to land.
    async fn resolve_direct_broker(
        &self,
        broker_url: &str,
        _topic: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError>
    where
        P: Send + Sync,
    {
        let Some(pool) = self.pool.as_ref() else {
            // No pool (built via `connect_plain` / `from_parts`) — the bootstrap is the only
            // connection available. Single-broker / bootstrap-equality scenarios still work;
            // a genuine multi-broker dial would have nowhere to land. Mirrors the tokio
            // engine's `from_socket` fallback.
            tracing::warn!(
                broker_url,
                "lookup resolved to a specific broker but moonpool client has no proxy pool \
                 (unsupervised); falling back to bootstrap connection"
            );
            return Ok(self.shared.clone());
        };

        let physical = direct_broker_authority(broker_url);
        // Bootstrap-equality fast path: same `host:port` as the connect-time URL → reuse the
        // bootstrap connection. Saves one TCP handshake on every single-broker / bootstrap-broker
        // lookup, and keeps existing single-broker tests on exactly one socket (no spurious pool
        // entry). Mirrors the tokio engine's identically-named bypass.
        if physical == pool.bootstrap_addr() {
            return Ok(self.shared.clone());
        }

        // Different broker → pin a dedicated pool entry. `logical == broker_url`, `physical` is the
        // `host:port` we dial; the pool entry's CONNECT carries no `proxy_to_broker_url` (DIRECT
        // routing, no proxy in the middle). Two DIRECT lookups to the same broker URL share one
        // entry, just like two PROXY lookups for the same backend share one.
        let shared = crate::pool::get_or_open(pool.clone(), broker_url, &physical, None).await?;
        Ok(shared)
    }

    /// Issue a `CommandLookupTopic` and await the broker's response.
    ///
    /// `authoritative` should be `false` for a fresh lookup; the state
    /// machine flips it to `true` on any internal redirect retry. The
    /// returned [`LookupTopicResult`] is the *terminal* outcome after the
    /// sans-io layer has followed any redirect chain — HIGH-4 (lookup
    /// multi-agent review) makes this end-to-end user-observable.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker returns a `Failed` lookup (including the synthetic
    ///   `Failed` raised when the redirect chain exceeds
    ///   [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops).
    /// - [`ClientError::Other`] when an outcome other than [`OpOutcome::LookupResponse`] arrives on
    ///   this request id (this would be a state-machine bug, not a transient failure).
    pub async fn lookup_topic(
        &self,
        topic: &str,
        authoritative: bool,
    ) -> Result<LookupTopicResult, ClientError> {
        // ADR-0059 / follow-ups §4.1: fast-fail BEFORE registering the lookup
        // request when the bootstrap connection is already terminal with no
        // driver to recover it. Without this, a `CommandLookupTopic` issued on
        // a dead plain connection registers a pending request no driver is left
        // to resolve — the caller hangs forever. The guard fires only when
        // `is_closed()` AND `no_driver`, so a supervised connection mid
        // reconnect (transiently `Failed`, `no_driver == false`) still issues
        // the lookup and recovers transparently. 1:1 with the tokio engine.
        self.shared.fail_if_no_driver()?;
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.lookup(topic, authoritative)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::LookupResponse { outcome, .. } => match outcome {
                LookupOutcome::Failed { code, message } => {
                    Err(ClientError::Broker { code, message })
                }
                other => Ok(other),
            },
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected lookup outcome: {other:?}"
            ))),
        }
    }

    /// Query the broker for the number of partitions of `topic`. Returns
    /// `0` for non-partitioned topics. Mirrors Java
    /// `PulsarClient#getPartitionsForTopic`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the request.
    /// - [`ClientError::Other`] when an unexpected outcome arrives on this request id.
    pub async fn partitioned_topic_metadata(&self, topic: &str) -> Result<u32, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_partitioned_topic_metadata(topic)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::PartitionedMetadata {
                partitions, error, ..
            } => {
                if let Some((code, message)) = error {
                    Err(ClientError::Broker { code, message })
                } else {
                    Ok(partitions)
                }
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected partitioned metadata outcome: {other:?}"
            ))),
        }
    }

    /// Subscribe to a PIP-145 topic-list watcher and return the *initial*
    /// snapshot. Subsequent watcher deltas land on
    /// [`Self::next_topic_list_change`].
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the subscribe.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn watch_topic_list(
        &self,
        namespace: &str,
        pattern: &str,
    ) -> Result<Vec<String>, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.watch_topic_list(namespace, pattern)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::TopicListSnapshot { topics, .. } => Ok(topics),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected topic-list snapshot outcome: {other:?}"
            ))),
        }
    }

    /// Await the next PIP-145 topic-list delta. Resolves with the broker-
    /// reported added / removed topics when the next watcher delta arrives,
    /// or `None` if the connection has closed and no further deltas will
    /// ever arrive.
    ///
    /// Pair with [`Self::watch_topic_list`] to first establish the watcher
    /// subscription. The future is cancel-safe: dropping it without polling
    /// does not lose pending deltas (they stay in the
    /// [`ConnectionShared::topic_list_changes`] queue).
    pub async fn next_topic_list_change(&self) -> Option<TopicListChange> {
        loop {
            if let Some(change) = self.shared.topic_list_changes.lock().pop_front() {
                return Some(change);
            }
            if self.shared.inner.lock().is_closed() {
                return None;
            }
            self.shared.topic_list_notify.notified().await;
        }
    }

    /// Non-blocking peek for the next PIP-145 topic-list delta. Returns
    /// `None` when the queue is empty. Useful for tight loops that want to
    /// drain pending deltas without yielding to the runtime.
    #[must_use]
    pub fn poll_topic_list_change(&self) -> Option<TopicListChange> {
        self.shared.topic_list_changes.lock().pop_front()
    }

    // -------------------------------------------------------------------
    // PIP-460 scalable topics (ADR-0031, experimental). 1:1 with the tokio
    // engine's `Client` methods — drives the proto `Connection` scalable
    // entries + reads driver-drained events via the same buffer + Notify
    // pattern as the PIP-145 topic-list deltas. No channels.
    // -------------------------------------------------------------------

    /// **Experimental** (PIP-460, ADR-0031). Resolve a `topic://...` scalable
    /// topic. Mirrors the tokio engine's `Client::scalable_topic_lookup`.
    #[cfg(feature = "scalable-topics")]
    pub async fn scalable_topic_lookup(
        &self,
        topic: &str,
    ) -> Result<crate::ScalableLookup, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.send_scalable_topic_lookup(topic, false)
        };
        self.shared.driver_waker.notify_one();
        loop {
            let drained = {
                let mut buf = self.shared.scalable_events.lock();
                let pos = buf.iter().position(|ev| {
                    matches!(
                        ev,
                        crate::ScalableEvent::LookupResolved { request_id: r, .. } if *r == request_id
                    )
                });
                pos.and_then(|p| buf.remove(p))
            };
            if let Some(crate::ScalableEvent::LookupResolved {
                controller_broker_url,
                segments,
                lookup_token,
                ..
            }) = drained
            {
                return Ok(crate::ScalableLookup {
                    controller_broker_url,
                    segments,
                    lookup_token,
                });
            }
            if self.shared.inner.lock().is_closed() {
                return Err(ClientError::Other(
                    "connection closed before scalable lookup resolved".to_owned(),
                ));
            }
            self.shared.scalable_notify.notified().await;
        }
    }

    /// **Experimental** (PIP-460, ADR-0031). Open a DAG-watch session.
    /// Mirrors the tokio engine's `Client::open_scalable_dag_watch`.
    #[cfg(feature = "scalable-topics")]
    pub fn open_scalable_dag_watch(
        &self,
        topic: &str,
        lookup_token: u64,
        segments: Vec<magnetar_proto::SegmentDescriptor>,
    ) -> u64 {
        let sid = {
            let mut conn = self.shared.inner.lock();
            conn.open_dag_watch(topic, lookup_token, segments)
        };
        self.shared.driver_waker.notify_one();
        sid
    }

    /// **Experimental** (PIP-460, ADR-0031). Close a DAG-watch session.
    #[cfg(feature = "scalable-topics")]
    pub fn close_scalable_dag_watch(&self, watch_session_id: u64) {
        {
            let mut conn = self.shared.inner.lock();
            let _ = conn.close_dag_watch(watch_session_id);
        }
        self.shared.driver_waker.notify_one();
    }

    /// **Experimental** (PIP-460, ADR-0031). Await the next scalable-topic
    /// event. Mirrors the tokio engine's `Client::next_scalable_event`.
    #[cfg(feature = "scalable-topics")]
    pub async fn next_scalable_event(&self) -> Option<crate::ScalableEvent> {
        loop {
            if let Some(ev) = self.shared.scalable_events.lock().pop_front() {
                return Some(ev);
            }
            if self.shared.inner.lock().is_closed() {
                return None;
            }
            self.shared.scalable_notify.notified().await;
        }
    }

    /// PIP-33: await the next replicated-subscription marker observed on any
    /// consumer of this connection. Mirrors the tokio engine's identically-
    /// named method. Resolves with the buffered observation, or `None` if the
    /// connection has closed and no further markers will arrive.
    pub async fn next_replicated_subscription_marker(
        &self,
    ) -> Option<crate::ObservedReplicatedSubscriptionMarker> {
        loop {
            if let Some(marker) = self
                .shared
                .replicated_subscription_markers
                .lock()
                .pop_front()
            {
                return Some(marker);
            }
            if self.shared.inner.lock().is_closed() {
                return None;
            }
            self.shared
                .replicated_subscription_marker_notify
                .notified()
                .await;
        }
    }

    /// Non-blocking peek for the next replicated-subscription marker
    /// observation. Returns `None` when the buffer is empty.
    #[must_use]
    pub fn poll_replicated_subscription_marker(
        &self,
    ) -> Option<crate::ObservedReplicatedSubscriptionMarker> {
        self.shared
            .replicated_subscription_markers
            .lock()
            .pop_front()
    }

    // -----------------------------------------------------------------
    // Transactions (PIP-31) — mirror `magnetar_runtime_tokio::Client`.
    //
    // Each method enqueues the sans-io frame via `Connection::*`,
    // notifies the driver, parks on a `RequestFut`, and pattern-matches
    // the resolved `OpOutcome`. The protocol-level handshakes already
    // live in `magnetar_proto`; the runtime crate stays I/O-only.
    // -----------------------------------------------------------------

    /// Open a new Pulsar transaction at the broker-side transaction
    /// coordinator (PIP-31). Mirrors Java
    /// `PulsarClient#newTransaction()`. Returns the broker-assigned
    /// [`magnetar_proto::TxnId`] once the TC acknowledges.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the TC rejects the request.
    /// - [`ClientError::Other`] on an unexpected outcome (state-machine bug).
    pub async fn new_txn(
        &self,
        timeout: std::time::Duration,
    ) -> Result<magnetar_proto::TxnId, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.new_txn(timeout)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::NewTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("new_txn: {err}")))
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected new_txn outcome: {other:?}"
            ))),
        }
    }

    /// Register `topic` as a partition this transaction will write to
    /// (PIP-31). Mirrors `Transaction#registerProducedTopic`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the TC rejects the request.
    /// - [`ClientError::Other`] on an unexpected outcome.
    pub async fn add_partition_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: impl Into<String>,
    ) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.add_partition_to_txn(txn, topic.into())
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::AddPartitionToTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("add_partition_to_txn: {err}")))
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected add_partition_to_txn outcome: {other:?}"
            ))),
        }
    }

    /// Register a subscription this transaction will acknowledge on
    /// (PIP-31). Mirrors `Transaction#registerSubscriptionToTxn`.
    ///
    /// Argument order matches the tokio engine's
    /// `magnetar_runtime_tokio::Client::add_subscription_to_txn`
    /// (`(txn, topic, subscription)`); internally we feed the proto layer
    /// the sub-then-topic order it expects.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the TC rejects the request.
    /// - [`ClientError::Other`] on an unexpected outcome.
    pub async fn add_subscription_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.add_subscription_to_txn(txn, subscription.into(), topic.into())
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::AddSubscriptionToTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("add_subscription_to_txn: {err}")))
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected add_subscription_to_txn outcome: {other:?}"
            ))),
        }
    }

    /// Commit or abort an open transaction (PIP-31). Returns the final
    /// transaction state reported by the TC. Mirrors
    /// `Transaction#commit` / `#abort`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the TC rejects the request.
    /// - [`ClientError::Other`] on an unexpected outcome.
    pub async fn end_txn(
        &self,
        txn: magnetar_proto::TxnId,
        action: magnetar_proto::TxnAction,
    ) -> Result<magnetar_proto::TxnState, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.end_txn(txn, action)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::EndTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("end_txn: {err}")))
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected end_txn outcome: {other:?}"
            ))),
        }
    }
}

/// Future that resolves the [`OpOutcome`] correlated with a single
/// `RequestId`. Mirrors the tokio engine's identically-named `RequestFut`:
/// the canonical "wait for a request-id-correlated outcome" future, reused
/// for lookup, partitioned metadata, watch-topic-list-snapshot, and the
/// txn family.
struct RequestFut {
    shared: Arc<ConnectionShared>,
    request_id: RequestId,
}

impl Future for RequestFut {
    type Output = OpOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let key = PendingOpKey::Request(self.request_id);
        let mut conn = self.shared.inner.lock();
        if let Some(outcome) = conn.take_outcome(key) {
            return Poll::Ready(outcome);
        }
        conn.register_waker(key, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for RequestFut {
    /// Drop-time cleanup: clear our entry from the connection's waker slab so
    /// a cancelled lookup / partitioned-metadata / watch-snapshot / txn
    /// future does not leave a dangling [`std::task::Waker`] behind. Mirrors
    /// the tokio engine's
    /// [`magnetar_runtime_tokio::client::RequestFut::drop`].
    /// Lookup multi-agent review MEDIUM-4; ADR-0024 four-layer parity.
    fn drop(&mut self) {
        let key = PendingOpKey::Request(self.request_id);
        self.shared.inner.lock().unregister_waker(key);
    }
}

/// Normalise an advertised broker URL into the `host:port` form expected on
/// `CommandConnect.proxy_to_broker_url`. The Apache Pulsar Proxy parses that field
/// via `InetSocketAddress.createUnresolved`, so passing `pulsar://host:port` makes
/// `validateBrokerTarget()` return `false` and the proxy rejects the handshake with
/// `ServerError.ServiceNotReady "Target broker cannot be validated"` (ADR-0039,
/// parity with Java client + pulsar-rs + the tokio engine).
///
/// Mirrors `magnetar_runtime_tokio::client::preferred_broker_url`. The two engines
/// pick their preferred URL differently — moonpool prefers `broker_service_url` so
/// it can keep riding the plaintext bootstrap pipe (see [`Client::lookup_topic_target`])
/// — but the scheme-strip step is identical.
fn proxy_broker_authority(input: &str) -> String {
    let (rest, default_port) = if let Some(rest) = input.strip_prefix("pulsar+ssl://") {
        (rest, Some(6651u16))
    } else if let Some(rest) = input.strip_prefix("pulsar://") {
        (rest, Some(6650u16))
    } else {
        (input, None)
    };
    let host_port = rest.split('/').next().unwrap_or(rest);
    match default_port {
        Some(port) if !host_port.contains(':') => format!("{host_port}:{port}"),
        _ => host_port.to_owned(),
    }
}

/// Normalise an advertised broker URL into the `host:port` form moonpool's
/// [`crate::transport::Transport::connect_with_resolver`] dials. Used by the
/// multi-broker DIRECT routing path (ADR-0039 §"Multi-broker DIRECT routing
/// (2026-06-01)") — the pool keys on `(logical, physical = host:port)` and dials
/// `physical` directly, so the helper must produce exactly the address shape
/// `connect_with_resolver` consumes.
///
/// Accepts the same input shapes as the tokio engine's
/// `parse_direct_broker_url`: a full Pulsar URL (`pulsar://host:port` or
/// `pulsar+ssl://host:port`) **or** a bare `host:port`. The scheme is stripped
/// and the default port is filled in when the URL omitted it; bare `host:port`
/// input is forwarded unchanged.
///
/// Reuses the same scheme-strip logic as [`proxy_broker_authority`] since
/// moonpool dials by `host:port` regardless of routing shape (TLS posture for
/// per-broker DIRECT dials is the bootstrap's posture — see ADR-0039
/// §"TLS posture"). The two helpers are deliberately distinct so each carries
/// its own contract docstring; both engines pin this contract at the type
/// level (see the tokio counterparts).
fn direct_broker_authority(input: &str) -> String {
    proxy_broker_authority(input)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use magnetar_proto::ConnectionConfig;
    use moonpool_core::TokioProviders;

    use super::{Client, ClientError, LookupTopicResult, proxy_broker_authority};
    use crate::{ConnectionShared, MoonpoolEngine, TopicListChange};

    /// `Client::connect_plain` is generic over `P: Providers` — name it to
    /// confirm the bounds compose with `TokioProviders` without actually
    /// dialling.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_plain_compiles_against_tokio_providers() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut = Client::connect_plain(&engine, "127.0.0.1:6650", ConnectionConfig::default());
    }

    /// `LookupTopicResult` is the re-exported `LookupOutcome`. Smoke test the
    /// alias by constructing a `Connect` variant.
    #[test]
    fn lookup_topic_result_alias_constructs() {
        let _: LookupTopicResult = LookupTopicResult::Connect {
            broker_service_url: Some("pulsar://broker:6650".to_owned()),
            broker_service_url_tls: None,
            proxy_through_service_url: false,
        };
    }

    /// `ClientError::Engine` wraps `EngineError` via `From`.
    #[test]
    fn client_error_from_engine_error() {
        let io_err = std::io::Error::other("dialled into the void");
        let engine: crate::EngineError = io_err.into();
        let client: ClientError = engine.into();
        assert!(matches!(client, ClientError::Engine(_)));
        let s = format!("{client}");
        assert!(s.contains("io error"), "got {s:?}");
    }

    /// `next_topic_list_change` returns the queued change without blocking
    /// when the queue is non-empty. Avoids spinning up a real driver.
    #[test]
    fn next_topic_list_change_drains_queue() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        shared.topic_list_changes.lock().push_back(TopicListChange {
            added: vec!["persistent://t/n/foo".to_owned()],
            removed: vec![],
        });
        // We can't construct `Client<P>` without a driver, so exercise the
        // queue drain path through the shared state directly. This mirrors
        // what `Client::next_topic_list_change` does on its first iteration.
        let popped = shared.topic_list_changes.lock().pop_front();
        assert!(popped.is_some());
        let popped = popped.unwrap();
        assert_eq!(popped.added, vec!["persistent://t/n/foo".to_owned()]);
    }

    /// `poll_topic_list_change` against an empty queue must yield `None`
    /// immediately. Exercised via the shared state to skip the driver.
    #[test]
    fn poll_topic_list_change_empty_yields_none() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        assert!(shared.topic_list_changes.lock().pop_front().is_none());
    }

    /// Sanity: `is_connected` reflects the underlying state machine. We
    /// can't reach `Connected` without a real broker, but at construction
    /// time the connection is in `Init` so both predicates return `false`.
    #[test]
    fn is_connected_and_is_closed_default_false() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let conn = shared.inner.lock();
        assert!(!conn.is_connected());
        assert!(!conn.is_closed());
    }

    /// `Client::connect_plain_supervised` compiles against `TokioProviders`
    /// when handed a `ControlledClusterFailover` for PIP-121.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_supervised_with_controlled_failover_compiles() {
        use std::sync::Arc;

        use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider};

        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let failover = ControlledClusterFailover::new("pulsar://primary:6650");
        let provider: Arc<dyn ServiceUrlProvider> = Arc::new(failover);
        let _fut = Client::connect_plain_supervised(
            &engine,
            "127.0.0.1:6650",
            ConnectionConfig::default(),
            Some(provider),
            None,
        );
    }

    /// `ControlledClusterFailover::set_url` updates the URL the supervisor
    /// will dial on the next reconnect. Exercised through the proto trait
    /// directly so the moonpool runtime doesn't need a live driver.
    #[test]
    fn controlled_failover_set_url_observed_by_provider() {
        use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider};

        let failover = ControlledClusterFailover::new("pulsar://primary:6650");
        assert_eq!(failover.get_service_url(), "pulsar://primary:6650");
        failover.set_url("pulsar://secondary:6650");
        assert_eq!(failover.get_service_url(), "pulsar://secondary:6650");
    }

    /// Confirm `Duration` import is still referenced — the moonpool engine
    /// historically pulled in time helpers that became dead after refactors.
    #[test]
    fn duration_marker() {
        let _ = Duration::from_millis(1);
    }

    #[test]
    fn proxy_broker_authority_strips_pulsar_ssl_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar+ssl://b-c3-n12:6651"),
            "b-c3-n12:6651"
        );
    }

    #[test]
    fn proxy_broker_authority_strips_pulsar_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar://b-c3-n12:6650"),
            "b-c3-n12:6650"
        );
    }

    #[test]
    fn proxy_broker_authority_appends_default_port_for_pulsar_scheme() {
        assert_eq!(proxy_broker_authority("pulsar://b-c3-n12"), "b-c3-n12:6650");
    }

    #[test]
    fn proxy_broker_authority_appends_default_port_for_pulsar_ssl_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar+ssl://b-c3-n12"),
            "b-c3-n12:6651"
        );
    }

    #[test]
    fn proxy_broker_authority_passes_through_bare_host_port() {
        // Defensive: a broker that advertised `host:port` directly (no scheme) is forwarded
        // unchanged.
        assert_eq!(proxy_broker_authority("b-c3-n12:6650"), "b-c3-n12:6650");
    }

    #[test]
    fn proxy_broker_authority_trims_trailing_path_segments() {
        // Real lookup responses don't carry paths, but the helper is the only thing standing
        // between the broker's string and `CommandConnect`, so be defensive.
        assert_eq!(
            proxy_broker_authority("pulsar://b-c3-n12:6650/extra/path"),
            "b-c3-n12:6650"
        );
    }
}
