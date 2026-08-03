// SPDX-License-Identifier: Apache-2.0

//! Top-level `Client` façade for the moonpool engine.
//!
//! Mirrors [`magnetar_runtime_tokio::Client`] but is generic over
//! [`moonpool_core::Providers`] so the same façade runs on production tokio
//! sockets and on a `moonpool-sim` deterministic substrate.
//!
//! The surface includes plain and supervised connections, lookup and partition metadata,
//! producer and consumer creation, topic-list watching, transactions, proxy routing, and clean
//! shutdown.
//! Provider-generic operations use Moonpool task, time, and network providers so the same client
//! runs on `TokioProviders` and `moonpool_sim::SimProviders`.
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use magnetar_proto::event::LookupOutcome;
use magnetar_proto::{ConnectionConfig, OpOutcome, PendingOpKey, RequestId};
use moonpool_core::Providers;
use parking_lot::Mutex;

use crate::driver::DriverHandle;
use crate::pool::ProxyConnectionPool;
use crate::{
    ConnectionShared, EngineError, MoonpoolEngine, SleepProvider, TopicListChange,
    sleep_provider_from_time, tokio_sleep_provider,
};

pub(crate) type OperationDeadline<'a> = Pin<&'a mut (dyn Future<Output = ()> + Send)>;

#[derive(Default)]
struct LookupRetryState {
    broker_failures: u32,
}

#[derive(Clone, Copy)]
enum LookupIssue {
    Initial { authoritative: bool },
    Redirect { authoritative: bool, hops: u8 },
}

pub(crate) fn operation_deadline_error(
    operation: &str,
    last_broker_error: Option<(i32, String)>,
) -> ClientError {
    match last_broker_error {
        Some((code, message)) => ClientError::Broker { code, message },
        None => ClientError::Other(format!("{operation} exceeded operation_timeout")),
    }
}

pub(crate) fn operation_deadline_expired(mut deadline: OperationDeadline<'_>) -> bool {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    deadline.as_mut().poll(&mut cx).is_ready()
}

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
/// Re-export of [`magnetar_proto::event::LookupOutcome`]. This raw accessor
/// surfaces the terminal outcome on the bootstrap connection — `Connect`,
/// `Redirected`, or `Failed`. A `Redirected` is a driveable outcome: the
/// engine path `Client::lookup_topic_target_with_operation_deadline` dials the redirect target
/// broker and re-issues the lookup there (ADR-0039). Callers that drive the
/// dial themselves consume the `Redirected` directly.
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
    /// `connections_per_broker` fan-out (Java `ClientBuilder#connectionsPerBroker`,
    /// ADR-0073, issue #314). `1` (the default) keeps the historical
    /// single-connection-per-broker behaviour. Clamped to `≥ 1` by
    /// [`Self::with_connections_per_broker`]. When [`Self::pool`] is `None`
    /// (the `connect_plain` / `from_parts` paths) the fan-out stays effectively
    /// `1` because there is no pool to dial siblings through. 1:1 with the tokio
    /// engine's identically-named field.
    connections_per_broker: usize,
    /// Round-robin cursor handing out the next connection index for a producer /
    /// consumer at [`Self::resolve_target`]. A plain `AtomicUsize` (not an RNG)
    /// so the spread is deterministic under `moonpool-sim` and matches the tokio
    /// engine bit-for-bit (ADR-0011, differential parity).
    connection_rr: AtomicUsize,
    /// Runtime-owned deadline provider inherited by every consumer opened
    /// through this client. Kept out of the public `ConnectionShared` layout
    /// so provider selection remains an engine concern.
    sleep_provider: Arc<SleepProvider>,
    /// Held only so `Client` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers.
    _providers: std::marker::PhantomData<fn() -> P>,
}

/// Decision returned by [`Client::lookup_topic_target_with_operation_deadline`] driving where the
/// data ops for the resolved topic should ride (ADR-0039). Mirror of the tokio engine's
/// `LookupTarget` — the moonpool [`Client::lookup_topic`] accessor still returns the raw
/// `LookupOutcome` so existing callers keep their full proto view; runtime code (producer /
/// consumer open paths) uses this routing-decision enum instead.
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
    /// Single field-initialisation site shared by [`Self::connect_plain`],
    /// [`Self::connect_plain_supervised`], and [`Self::from_parts`]. Seeds the
    /// default `connections_per_broker` (1) and a fresh round-robin cursor.
    fn assemble(
        shared: Arc<ConnectionShared>,
        driver: DriverHandle,
        pool: Option<Arc<ProxyConnectionPool<P>>>,
        sleep_provider: Arc<SleepProvider>,
    ) -> Self {
        Self {
            shared,
            driver: Mutex::new(Some(driver)),
            pool,
            connections_per_broker: 1,
            connection_rr: AtomicUsize::new(0),
            sleep_provider,
            _providers: std::marker::PhantomData,
        }
    }

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
        Ok(Self::from_parts_with_providers(
            shared,
            driver,
            engine.providers(),
        ))
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
            operation_retry: Arc::new(Mutex::new(magnetar_proto::OperationRetryConfig::default())),
            providers: engine.providers().clone(),
            service_url_provider,
            dns_resolver,
            schemeless_default_port: 6650,
        };
        let pool = ProxyConnectionPool::new(factory);
        Ok(Self::assemble(
            shared,
            driver,
            Some(pool),
            sleep_provider_from_time(engine.providers().time().clone()),
        ))
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
        Self::assemble(shared, driver, None, tokio_sleep_provider())
    }

    /// Wrap an existing `(shared, driver)` pair and bind user-facing
    /// deadlines to the supplied Moonpool provider bundle.
    ///
    /// Use this constructor for `SimProviders` or any custom provider
    /// implementation. [`Self::from_parts`] remains the convenience path for
    /// Tokio-backed callers and therefore installs a [`moonpool_core::TokioTimeProvider`].
    #[must_use]
    pub fn from_parts_with_providers(
        shared: Arc<ConnectionShared>,
        driver: DriverHandle,
        providers: &P,
    ) -> Self {
        Self::assemble(
            shared,
            driver,
            None,
            sleep_provider_from_time(providers.time().clone()),
        )
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

    pub(crate) fn sleep_provider(&self) -> Arc<SleepProvider> {
        self.sleep_provider.clone()
    }

    /// Apply a broker-operation retry policy to the bootstrap and every future
    /// pooled connection.
    #[must_use]
    pub fn with_operation_retry(self, config: magnetar_proto::OperationRetryConfig) -> Self {
        self.shared
            .inner
            .lock()
            .set_operation_retry_config(config.clone());
        if let Some(pool) = &self.pool {
            pool.set_operation_retry_config(config);
        }
        self
    }

    /// Create one provider-backed timer for a caller-visible setup operation.
    #[doc(hidden)]
    pub fn operation_timer(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let timeout = self.shared.inner.lock().operation_timeout();
        let sleep = (self.sleep_provider)(timeout);
        Box::pin(async move {
            let _ = sleep.await;
        })
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

    /// Resolve a lookup into a routing decision (ADR-0039), driving the
    /// redirect-dial loop end-to-end.
    ///
    /// The first lookup rides the bootstrap connection. On `Redirect` the
    /// broker is not the bundle owner; the proto layer surfaces a driveable
    /// `LookupOutcome::Redirected` (it does NOT chase the redirect on the
    /// bootstrap socket — that re-asked the same non-owner and looped to the
    /// cap). We dial the redirect target broker (reusing the per-broker pool)
    /// and re-issue the lookup THERE — Java `BinaryProtoLookupService#findBroker`
    /// recursing on `getConnection(redirectAddress)` — threading the
    /// decremented hop budget so the chain stays bounded by
    /// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`].
    ///
    /// When the terminal `Connect` advertises `proxy_through_service_url =
    /// true`, the data ops ride a pinned per-broker pool entry; otherwise the
    /// resolved broker is the data plane.
    pub(crate) async fn lookup_topic_target_with_operation_deadline(
        &self,
        topic: &str,
        mut deadline: OperationDeadline<'_>,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<(LookupTarget, Arc<ConnectionShared>), ClientError>
    where
        P: Send + Sync,
    {
        let mut current = self.shared.clone();
        let mut next_hop: Option<(bool, u8)> = None;
        let mut retry_state = LookupRetryState::default();
        loop {
            let outcome = match next_hop {
                None => {
                    self.issue_lookup_on(
                        &current,
                        topic,
                        LookupIssue::Initial {
                            authoritative: false,
                        },
                        deadline.as_mut(),
                        &mut retry_state,
                        last_broker_error,
                    )
                    .await?
                }
                Some((authoritative, hops)) => {
                    self.issue_lookup_on(
                        &current,
                        topic,
                        LookupIssue::Redirect {
                            authoritative,
                            hops,
                        },
                        deadline.as_mut(),
                        &mut retry_state,
                        last_broker_error,
                    )
                    .await?
                }
            };

            let lookup = match outcome {
                OpOutcome::LookupResponse { outcome, .. } => outcome,
                OpOutcome::Error { code, message, .. } => {
                    return Err(ClientError::Broker { code, message });
                }
                OpOutcome::Terminal { .. } => return Err(ClientError::PeerClosed),
                other => {
                    return Err(ClientError::Other(format!(
                        "unexpected lookup outcome: {other:?}"
                    )));
                }
            };

            match lookup {
                LookupOutcome::Connect {
                    broker_service_url,
                    broker_service_url_tls,
                    proxy_through_service_url,
                } => {
                    if proxy_through_service_url {
                        // Lookup-driven reconnects on the moonpool engine ride the plaintext
                        // bootstrap pipe even when both URLs are advertised — TLS routing on the
                        // pinned per-broker pool is wired through the engine's `connect_tls`
                        // entry, not through `lookup_topic_target_with_operation_deadline`. Prefer
                        // the plain `broker_service_url` here for that
                        // reason. The advertised value is normalised to
                        // `host:port` via [`proxy_broker_authority`] so the wire
                        // bytes match the tokio engine (ADR-0039).
                        let raw = broker_service_url.or(broker_service_url_tls).ok_or_else(|| {
                            ClientError::Other(format!(
                                "lookup of '{topic}' set proxy_through_service_url=true but did \
                                 not advertise a broker_service_url"
                            ))
                        })?;
                        let broker_url = proxy_broker_authority(&raw)?;
                        return Ok((LookupTarget::Proxy { broker_url }, current));
                    }
                    // ADR-0039 §"Multi-broker DIRECT routing": capture the resolved broker URL
                    // so `resolve_target` routes the data ops to the right broker.
                    let broker_url = broker_service_url.or(broker_service_url_tls);
                    return Ok((LookupTarget::Direct { broker_url }, current));
                }
                LookupOutcome::Redirected {
                    broker_service_url,
                    broker_service_url_tls,
                    authoritative,
                    hops_remaining,
                } => {
                    // Engine-side cap enforcement — defence in depth alongside the proto
                    // floor. A `Redirected` with no budget left must surface the SAME
                    // synthetic Failed the proto layer emits, never dial.
                    if hops_remaining == 0 {
                        return Err(ClientError::Broker {
                            code: 0,
                            message: format!(
                                "lookup redirect cap exceeded ({} hops)",
                                magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS
                            ),
                        });
                    }
                    let raw = broker_service_url
                        .or(broker_service_url_tls)
                        .ok_or_else(|| {
                            ClientError::Other(format!(
                                "lookup of '{topic}' was redirected but the broker advertised no \
                             broker_service_url or broker_service_url_tls to dial"
                            ))
                        })?;
                    tracing::debug!(
                        topic,
                        redirect_url = %raw,
                        hops_remaining,
                        "lookup redirected; dialing redirect target and re-issuing"
                    );
                    // Dial the redirect target. The dial awaits OUTSIDE any proto/connection
                    // lock (ADR-0038) and uses no channel (ADR-0003 — the moonpool pool dial
                    // rides a `spawn_task` + `Notify` park). `resolve_direct_broker` reuses the
                    // bootstrap on a host:port match, else pins a pool entry. This is a
                    // control-plane lookup dial, so it pins the primary connection (index 0) —
                    // lookups never consume a `connections_per_broker` fan-out slot (ADR-0073).
                    current = moonpool_core::select! {
                        biased;
                        () = deadline.as_mut() => {
                            return Err(operation_deadline_error(
                                "lookup redirect dial",
                                last_broker_error.clone(),
                            ));
                        }
                        result = self.resolve_direct_broker(&raw, topic, 0) => result?,
                    };
                    next_hop = Some((authoritative, hops_remaining));
                }
                LookupOutcome::Failed { code, message } => {
                    return Err(ClientError::Broker { code, message });
                }
            }
        }
    }

    /// Set the `connections_per_broker` fan-out (Java
    /// `ClientBuilder#connectionsPerBroker`, ADR-0073, issue #314). `n` is
    /// clamped to `≥ 1`; `1` (the default) keeps the historical
    /// single-connection-per-broker behaviour. 1:1 with the tokio engine.
    #[must_use]
    pub fn with_connections_per_broker(mut self, n: usize) -> Self {
        self.connections_per_broker = n.max(1);
        self
    }

    /// Round-robin the next connection index in `[0, connections_per_broker)` for
    /// a producer / consumer. Returns `0` when fan-out is disabled
    /// (`connections_per_broker <= 1`) or when there is no pool to dial siblings
    /// through, so those paths never touch the atomic and stay byte-identical to
    /// the pre-#314 client. 1:1 with the tokio engine.
    fn pick_connection_index(&self) -> usize {
        if self.connections_per_broker <= 1 || self.pool.is_none() {
            return 0;
        }
        self.connection_rr.fetch_add(1, Ordering::Relaxed) % self.connections_per_broker
    }

    /// Resolve the bootstrap broker's connection for `index`. Index `0` is the
    /// bootstrap connection the lookup landed on; indices `≥ 1` are pool siblings
    /// dialled to the same broker (ADR-0073, #314). Fan-out only applies when the
    /// lookup actually landed on the bootstrap connection — a redirected
    /// `landed_on` (already a distinct pool entry) is returned unchanged. 1:1 with
    /// the tokio engine.
    async fn bootstrap_connection_at_index(
        &self,
        landed_on: &Arc<ConnectionShared>,
        index: usize,
    ) -> Result<Arc<ConnectionShared>, ClientError>
    where
        P: Send + Sync,
    {
        // index ≥ 1 on the bootstrap connection → dedicated sibling. Index 0
        // (the default + every slot-0 producer/consumer) and a redirected
        // `landed_on` (a distinct pool entry, never the bootstrap) ride
        // `landed_on` as-is. `index ≥ 1` implies a pool exists
        // (`pick_connection_index` returns 0 when it does not). 1:1 with tokio.
        if index >= 1 && Arc::ptr_eq(landed_on, &self.shared) {
            let Some(pool) = self.pool.as_ref() else {
                unreachable!(
                    "connections_per_broker index >= 1 implies a pool (pick_connection_index)"
                )
            };
            return Ok(crate::pool::get_or_open_bootstrap_sibling(pool.clone(), index).await?);
        }
        Ok(landed_on.clone())
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
    /// **Provider-native single-flight dialing**: Moonpool 0.8 makes network-provider futures
    /// `Send`.
    /// The pool still owns each dial in one [`moonpool_core::TaskProvider::spawn_task`] task so
    /// racing producer or consumer opens share the same pending result under both
    /// `TokioProviders` and `moonpool_sim::SimProviders`.
    /// This future awaits only the published result; see `crate::pool::get_or_open`.
    pub(crate) async fn resolve_target(
        &self,
        target: &LookupTarget,
        landed_on: &Arc<ConnectionShared>,
        topic: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError>
    where
        P: Send + Sync,
    {
        // `connections_per_broker` fan-out slot for this producer / consumer
        // (ADR-0073, #314). `0` for the default single-connection client. 1:1
        // with the tokio engine.
        let index = self.pick_connection_index();
        match target {
            // Ride the connection the final lookup resolved on — the bootstrap
            // for a non-redirected lookup, or the dialed redirect target after
            // the redirect-dial loop. 1:1 with the tokio engine.
            LookupTarget::Direct { broker_url: None } => {
                self.bootstrap_connection_at_index(landed_on, index).await
            }
            LookupTarget::Direct {
                broker_url: Some(broker_url),
            } => self.resolve_direct_broker(broker_url, topic, index).await,
            LookupTarget::Proxy { broker_url } => {
                let pool = self.pool.as_ref().ok_or_else(|| {
                    ClientError::ProxyUnsupportedOnUnsupervisedClient {
                        topic: topic.to_owned(),
                    }
                })?;
                // Proxy entries dial the same physical address — the proxy URL the bootstrap was
                // built with. `CommandConnect.proxy_to_broker_url = Some(broker_url)` tells the
                // proxy which backend broker this connection serves. `index` fans the same backend
                // broker across N proxy connections.
                let physical = pool.bootstrap_addr().to_owned();
                let shared = crate::pool::get_or_open(
                    pool.clone(),
                    broker_url,
                    &physical,
                    Some(broker_url.clone()),
                    index,
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
        index: usize,
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

        let physical = direct_broker_authority(broker_url, pool.schemeless_default_port())?;
        // Bootstrap-equality fast path: same `host:port` as the connect-time URL → reuse the
        // bootstrap connection. Saves one TCP handshake on every single-broker / bootstrap-broker
        // lookup, and keeps existing single-broker tests on exactly one socket (no spurious pool
        // entry). Mirrors the tokio engine's identically-named bypass.
        if physical == pool.bootstrap_addr() {
            // Bootstrap broker — same fan-out as the `Direct { broker_url: None }`
            // path: index 0 reuses the bootstrap connection, siblings (index ≥ 1)
            // ride dedicated pool entries dialled to the same broker (ADR-0073, #314).
            return self
                .bootstrap_connection_at_index(&self.shared, index)
                .await;
        }

        // Different broker → pin a dedicated pool entry. `logical == broker_url`, `physical` is the
        // `host:port` we dial; the pool entry's CONNECT carries no `proxy_to_broker_url` (DIRECT
        // routing, no proxy in the middle). Two DIRECT lookups to the same broker URL and the same
        // fan-out slot share one entry, just like two PROXY lookups for the same backend share one.
        let shared =
            crate::pool::get_or_open(pool.clone(), broker_url, &physical, None, index).await?;
        Ok(shared)
    }

    /// Issue a `CommandLookupTopic` against the bootstrap connection and await
    /// the broker's response.
    ///
    /// `authoritative` should be `false` for a fresh lookup. The returned
    /// [`LookupTopicResult`] is the terminal outcome on this connection — one
    /// of `Connect` / `Redirected` / `Failed`. On `Redirected` the engine
    /// (via `Self::lookup_topic_target_with_operation_deadline`) dials the redirect target and
    /// re-issues there; this raw accessor surfaces the `Redirected` as-is for
    /// callers that route the dial themselves.
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
        let mut deadline = self.operation_timer();
        let mut last_broker_error = None;
        self.lookup_topic_with_operation_deadline(
            topic,
            authoritative,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
    }

    /// Deadline-aware raw lookup seam.
    #[doc(hidden)]
    pub async fn lookup_topic_with_operation_deadline(
        &self,
        topic: &str,
        authoritative: bool,
        mut deadline: Pin<&mut (dyn Future<Output = ()> + Send)>,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<LookupTopicResult, ClientError> {
        let mut retry_state = LookupRetryState::default();
        let outcome = self
            .issue_lookup_on(
                &self.shared,
                topic,
                LookupIssue::Initial { authoritative },
                deadline.as_mut(),
                &mut retry_state,
                last_broker_error,
            )
            .await?;

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

    /// Issue one `CommandLookupTopic` against `shared` and await its terminal
    /// [`OpOutcome`], with the bounded `SessionLost` re-issue loop
    /// (ADR-0060). `redirect_budget` is `None` for the
    /// user's first lookup (proto seeds the full cap) and `Some(hops)` for a
    /// redirect re-issue on a dialed target (proto clamps + re-checks the cap
    /// in `Connection::lookup_redirect`). 1:1 with the tokio engine's
    /// identically-named helper (ADR-0024).
    async fn issue_lookup_on(
        &self,
        shared: &Arc<ConnectionShared>,
        topic: &str,
        issue: LookupIssue,
        mut deadline: OperationDeadline<'_>,
        retry_state: &mut LookupRetryState,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<OpOutcome, ClientError> {
        // ADR-0059: fast-fail BEFORE registering the lookup
        // when the connection is already terminal with no driver to recover it
        // — otherwise the caller hangs on a request no driver will resolve.
        shared.fail_if_no_driver()?;

        // ADR-0060: bounded lookup-retry on `SessionLost`.
        // `Connection::reset` (supervised reconnect) fails the in-flight lookup
        // with `OpOutcome::SessionLost` but does not re-issue it; on that, park
        // until the connection is live again (or terminal), then re-issue. The
        // budget is only spent on a real broker round-trip.
        let mut reissues_remaining = magnetar_proto::lookup::MAX_LOOKUP_SESSION_REISSUES;
        let retry_config = shared.inner.lock().operation_retry_config().clone();
        loop {
            let request_id = {
                if operation_deadline_expired(deadline.as_mut()) {
                    return Err(operation_deadline_error(
                        "topic lookup",
                        last_broker_error.clone(),
                    ));
                }
                let mut conn = shared.inner.lock();
                match issue {
                    LookupIssue::Initial { authoritative } => conn.lookup(topic, authoritative),
                    LookupIssue::Redirect {
                        authoritative,
                        hops,
                    } => conn.lookup_redirect(topic, authoritative, hops),
                }
            };
            shared.driver_waker.notify_one();
            let request = RequestFut::cancellable(shared.clone(), request_id);
            tokio::pin!(request);
            let outcome = moonpool_core::select! {
                biased;
                () = deadline.as_mut() => {
                    return Err(operation_deadline_error(
                        "topic lookup",
                        last_broker_error.clone(),
                    ));
                }
                outcome = request.as_mut() => outcome,
            };

            if matches!(outcome, OpOutcome::SessionLost { .. }) {
                let readiness = moonpool_core::select! {
                    biased;
                    () = deadline.as_mut() => {
                        return Err(operation_deadline_error(
                            "topic lookup",
                            last_broker_error.clone(),
                        ));
                    }
                    readiness = shared.await_reconnect_or_terminal() => readiness,
                };
                match readiness {
                    crate::LookupReissueReadiness::Reconnected => {
                        if reissues_remaining == 0 {
                            tracing::warn!(
                                topic,
                                max_reissues = magnetar_proto::lookup::MAX_LOOKUP_SESSION_REISSUES,
                                "lookup session-reissue cap exceeded; surfacing PeerClosed"
                            );
                            return Err(ClientError::PeerClosed);
                        }
                        reissues_remaining -= 1;
                        tracing::debug!(
                            topic,
                            reissues_remaining,
                            "lookup severed by reconnect; re-issuing against fresh session"
                        );
                        continue;
                    }
                    crate::LookupReissueReadiness::Terminal => {
                        return Err(ClientError::PeerClosed);
                    }
                }
            }

            if let OpOutcome::LookupResponse {
                outcome: LookupOutcome::Failed { code, .. },
                ..
            } = &outcome
                && magnetar_proto::is_retryable_broker_error(
                    magnetar_proto::OperationKind::Lookup,
                    *code,
                )
            {
                if let OpOutcome::LookupResponse {
                    outcome: LookupOutcome::Failed { code, message },
                    ..
                } = &outcome
                {
                    *last_broker_error = Some((*code, message.clone()));
                }
                retry_state.broker_failures = retry_state.broker_failures.saturating_add(1);
                if retry_config.should_retry_after_failure(retry_state.broker_failures) {
                    let mut sleep = (self.sleep_provider)(
                        retry_config.delay_after_failure(retry_state.broker_failures),
                    );
                    moonpool_core::select! {
                        biased;
                        () = deadline.as_mut() => {
                            return Err(operation_deadline_error(
                                "topic lookup",
                                last_broker_error.clone(),
                            ));
                        }
                        _ = sleep.as_mut() => {}
                    }
                    continue;
                }
            }

            return Ok(outcome);
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
        let mut deadline = self.operation_timer();
        let mut last_broker_error = None;
        self.partitioned_topic_metadata_with_operation_deadline(
            topic,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
    }

    /// Deadline-aware partition-metadata seam used by the engine-generic façade.
    #[doc(hidden)]
    pub async fn partitioned_topic_metadata_with_operation_deadline(
        &self,
        topic: &str,
        mut deadline: Pin<&mut (dyn Future<Output = ()> + Send)>,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<u32, ClientError> {
        let retry_config = self.shared.inner.lock().operation_retry_config().clone();
        let mut broker_failures = 0_u32;
        loop {
            let request_id = {
                if operation_deadline_expired(deadline.as_mut()) {
                    return Err(operation_deadline_error(
                        "partitioned topic metadata",
                        last_broker_error.clone(),
                    ));
                }
                let mut conn = self.shared.inner.lock();
                conn.get_partitioned_topic_metadata(topic)
            };
            self.shared.driver_waker.notify_one();
            let request = RequestFut::cancellable(self.shared.clone(), request_id);
            tokio::pin!(request);
            let outcome = moonpool_core::select! {
                biased;
                () = deadline.as_mut() => {
                    return Err(operation_deadline_error(
                        "partitioned topic metadata",
                        last_broker_error.clone(),
                    ));
                }
                outcome = request.as_mut() => outcome,
            };
            if let OpOutcome::PartitionedMetadata {
                error: Some((code, message)),
                ..
            } = &outcome
                && magnetar_proto::is_retryable_broker_error(
                    magnetar_proto::OperationKind::PartitionedMetadata,
                    *code,
                )
            {
                *last_broker_error = Some((*code, message.clone()));
                broker_failures = broker_failures.saturating_add(1);
                if retry_config.should_retry_after_failure(broker_failures) {
                    let mut sleep =
                        (self.sleep_provider)(retry_config.delay_after_failure(broker_failures));
                    moonpool_core::select! {
                        biased;
                        () = deadline.as_mut() => {
                            return Err(operation_deadline_error(
                                "partitioned topic metadata",
                                last_broker_error.clone(),
                            ));
                        }
                        _ = sleep.as_mut() => {}
                    }
                    continue;
                }
            }
            return match outcome {
                OpOutcome::PartitionedMetadata {
                    partitions, error, ..
                } => {
                    if let Some((code, message)) = error {
                        Err(ClientError::Broker { code, message })
                    } else {
                        Ok(partitions)
                    }
                }
                OpOutcome::Error { code, message, .. } => {
                    Err(ClientError::Broker { code, message })
                }
                OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
                other => Err(ClientError::Other(format!(
                    "unexpected partitioned metadata outcome: {other:?}"
                ))),
            };
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
        let mut deadline = self.operation_timer();
        let mut last_broker_error = None;
        self.watch_topic_list_with_operation_deadline(
            namespace,
            pattern,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
    }

    /// Deadline-aware topic-list snapshot seam used by the engine-generic
    /// pattern-consumer builder.
    #[doc(hidden)]
    pub async fn watch_topic_list_with_operation_deadline(
        &self,
        namespace: &str,
        pattern: &str,
        mut deadline: OperationDeadline<'_>,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<Vec<String>, ClientError> {
        if operation_deadline_expired(deadline.as_mut()) {
            return Err(operation_deadline_error(
                "topic-list snapshot",
                last_broker_error.clone(),
            ));
        }
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.watch_topic_list(namespace, pattern)
        };
        self.shared.driver_waker.notify_one();
        let request = RequestFut::cancellable(self.shared.clone(), request_id);
        tokio::pin!(request);
        let outcome = moonpool_core::select! {
            biased;
            () = deadline.as_mut() => {
                return Err(operation_deadline_error(
                    "topic-list snapshot",
                    last_broker_error.clone(),
                ));
            }
            outcome = request.as_mut() => outcome,
        };
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

    /// **Experimental** (PIP-460, ADR-0093). Open a scalable-topic session for
    /// `topic` and await its first layout.
    ///
    /// Upstream folds lookup and DAG-watch subscribe into one command, so this
    /// both resolves the topic **and** leaves the session open: subsequent
    /// layouts arrive through [`Self::next_scalable_event`] until
    /// [`Self::close_scalable_topic_session`]. `topic` may be a `topic://`, a
    /// `persistent://`, or a short name — the broker returns the canonical
    /// identity in [`crate::ScalableLookup::resolved_topic_name`].
    ///
    /// # Errors
    ///
    /// Fails when the broker did not advertise `supports_scalable_topics` — a
    /// Pulsar 4.x peer, or a 5.x one started with `scalableTopicsEnabled=false`
    /// — and when the connection closes before the first layout lands.
    #[cfg(feature = "scalable-topics")]
    pub async fn scalable_topic_lookup(
        &self,
        topic: &str,
    ) -> Result<crate::ScalableLookup, ClientError> {
        let session_id = {
            let mut conn = self.shared.inner.lock();
            conn.open_scalable_topic_session(topic)
                .map_err(|err| ClientError::Other(err.to_string()))?
        };
        self.shared.driver_waker.notify_one();
        loop {
            // Drain the first terminal event for our session id. A rejected
            // session ends as `DagWatchClosed`, not `LookupResolved` — waiting
            // only for the success variant would hang the caller until the
            // connection closed, which is exactly the shape
            // `scalable_topic_subscribe` avoids by racing its two outcomes.
            let drained = {
                let mut buf = self.shared.scalable_events.lock();
                let pos = buf.iter().position(|ev| {
                    matches!(
                        ev,
                        crate::ScalableEvent::LookupResolved { session_id: s, .. }
                            | crate::ScalableEvent::DagWatchClosed { session_id: s, .. }
                            if *s == session_id
                    )
                });
                pos.and_then(|p| buf.remove(p))
            };
            match drained {
                Some(crate::ScalableEvent::LookupResolved {
                    resolved_topic_name,
                    controller_broker_url,
                    segments,
                    epoch,
                    ..
                }) => {
                    return Ok(crate::ScalableLookup {
                        session_id,
                        resolved_topic_name,
                        controller_broker_url,
                        segments,
                        epoch,
                    });
                }
                Some(crate::ScalableEvent::DagWatchClosed { reason, .. }) => {
                    return Err(ClientError::Other(reason.unwrap_or_else(|| {
                        "scalable-topic session closed before it resolved".to_owned()
                    })));
                }
                _ => {}
            }
            if self.shared.inner.lock().is_closed() {
                return Err(ClientError::Other(
                    "connection closed before scalable lookup resolved".to_owned(),
                ));
            }
            self.shared.scalable_notify.notified().await;
        }
    }

    /// **Experimental** (PIP-460, ADR-0093). Whether the connected broker
    /// advertised the PIP-460 capability. `false` against a Pulsar 4.x peer.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn broker_supports_scalable_topics(&self) -> bool {
        self.shared.inner.lock().broker_supports_scalable_topics()
    }

    /// **Experimental** (PIP-460, ADR-0093). Register as a scalable consumer
    /// with the controller leader and await the initial assignment.
    ///
    /// This is what obtains a **share** of a scalable topic — the
    /// `segment://` topics this consumer owns. Resolving the layout with
    /// [`Self::scalable_topic_lookup`] does not grant one. Rebalances arrive
    /// afterwards as [`crate::ScalableEvent::AssignmentChanged`].
    ///
    /// # Errors
    ///
    /// Fails when the broker did not advertise `supports_scalable_topics`, when
    /// the broker rejects the registration, and when the connection closes
    /// before the assignment lands.
    #[cfg(feature = "scalable-topics")]
    pub async fn scalable_topic_subscribe(
        &self,
        topic: &str,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
        consumer_type: magnetar_proto::ScalableConsumerType,
    ) -> Result<magnetar_proto::ConsumerAssignment, ClientError> {
        {
            let mut conn = self.shared.inner.lock();
            conn.scalable_topic_subscribe(
                topic,
                subscription,
                consumer_name,
                consumer_id,
                consumer_type,
            )
            .map_err(|err| ClientError::Other(err.to_string()))?;
        }
        self.shared.driver_waker.notify_one();
        loop {
            let drained = {
                let mut buf = self.shared.scalable_events.lock();
                let pos = buf.iter().position(|ev| {
                    matches!(
                        ev,
                        crate::ScalableEvent::ConsumerAssigned { consumer_id: c, .. }
                            | crate::ScalableEvent::ConsumerRejected { consumer_id: c, .. }
                            if *c == consumer_id
                    )
                });
                pos.and_then(|p| buf.remove(p))
            };
            match drained {
                Some(crate::ScalableEvent::ConsumerAssigned { assignment, .. }) => {
                    return Ok(assignment);
                }
                Some(crate::ScalableEvent::ConsumerRejected { reason, .. }) => {
                    return Err(ClientError::Other(reason));
                }
                _ => {}
            }
            if self.shared.inner.lock().is_closed() {
                return Err(ClientError::Other(
                    "connection closed before the scalable assignment landed".to_owned(),
                ));
            }
            self.shared.scalable_notify.notified().await;
        }
    }

    /// **Experimental** (PIP-460, ADR-0093). The current assignment for a
    /// registered scalable consumer, or `None` before it resolves.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn scalable_consumer_assignment(
        &self,
        consumer_id: u64,
    ) -> Option<magnetar_proto::ConsumerAssignment> {
        self.shared
            .inner
            .lock()
            .scalable_consumer_assignment(consumer_id)
            .cloned()
    }

    /// **Experimental** (PIP-460, ADR-0093). Open a namespace-level watch over
    /// the scalable topics matching `property_filters` (empty = all).
    ///
    /// # Errors
    ///
    /// Fails when the broker did not advertise `supports_scalable_topics`.
    #[cfg(feature = "scalable-topics")]
    pub fn watch_scalable_topics(
        &self,
        namespace: &str,
        property_filters: Vec<(String, String)>,
    ) -> Result<u64, ClientError> {
        let watch_id = {
            let mut conn = self.shared.inner.lock();
            conn.watch_scalable_topics(namespace, property_filters)
                .map_err(|err| ClientError::Other(err.to_string()))?
        };
        self.shared.driver_waker.notify_one();
        Ok(watch_id)
    }

    /// **Experimental** (PIP-460, ADR-0093). Close a namespace-level watch.
    #[cfg(feature = "scalable-topics")]
    pub fn close_scalable_topics_watch(&self, watch_id: u64) {
        {
            let mut conn = self.shared.inner.lock();
            conn.close_scalable_topics_watch(watch_id);
        }
        self.shared.driver_waker.notify_one();
    }

    /// **Experimental** (PIP-460, ADR-0093). The current matching topic set for
    /// a namespace watch, or `None` for an unknown id.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>> {
        self.shared.inner.lock().scalable_topics_snapshot(watch_id)
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Whether the broker
    /// advertised metadata-driven transaction-coordinator discovery. Gated on
    /// its own feature flag, independent of `supports_scalable_topics`.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn broker_supports_tc_metadata_discovery(&self) -> bool {
        self.shared
            .inner
            .lock()
            .broker_supports_tc_metadata_discovery()
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Open a
    /// transaction-coordinator discovery watch.
    ///
    /// # Errors
    ///
    /// Fails when the broker did not advertise `supports_tc_metadata_discovery`.
    #[cfg(feature = "scalable-topics")]
    pub fn watch_tc_assignments(&self) -> Result<u64, ClientError> {
        let watch_id = {
            let mut conn = self.shared.inner.lock();
            conn.watch_tc_assignments()
                .map_err(|err| ClientError::Other(err.to_string()))?
        };
        self.shared.driver_waker.notify_one();
        Ok(watch_id)
    }

    /// **Experimental** (PIP-460 / PIP-473, ADR-0093). Close a
    /// transaction-coordinator discovery watch.
    #[cfg(feature = "scalable-topics")]
    pub fn close_tc_assignments_watch(&self, watch_id: u64) {
        {
            let mut conn = self.shared.inner.lock();
            conn.close_tc_assignments_watch(watch_id);
        }
        self.shared.driver_waker.notify_one();
    }

    /// **Experimental** (PIP-460, ADR-0093). Close a scalable-topic session.
    #[cfg(feature = "scalable-topics")]
    pub fn close_scalable_topic_session(&self, session_id: u64) {
        {
            let mut conn = self.shared.inner.lock();
            conn.close_scalable_topic_session(session_id);
        }
        self.shared.driver_waker.notify_one();
    }

    /// **Experimental** (PIP-460, ADR-0093). Await the next scalable-topic
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
    ///
    /// Enroll-before-drain (mirror of [`ConnectionShared::await_reconnect_or_terminal`]):
    /// the `Notified` future is created and `enable()`d *before* the buffer drain +
    /// `is_closed()` re-check, so a marker the driver pushes (via
    /// `replicated_subscription_marker_notify.notify_waiters()`, which stores no permit)
    /// between the drain and the park is captured by this already-armed waiter rather than
    /// lost. The previous drain-then-`notified().await` shape hung whenever the marker
    /// landed in that gap (same race fixed for the subscribe-readiness waiter).
    /// No channel (ADR-0003), no virtual-clock read (ADR-0011). 1:1 mirror of the tokio
    /// engine.
    pub async fn next_replicated_subscription_marker(
        &self,
    ) -> Option<crate::ObservedReplicatedSubscriptionMarker> {
        loop {
            // Arm the wakeup BEFORE inspecting the buffer so a marker pushed
            // between the drain and the park is captured by this `Notified`.
            let notified = self.shared.replicated_subscription_marker_notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();

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
            // Neither a buffered marker nor closed — park on the pre-armed
            // waiter, then re-loop and re-arm. A spurious wake just re-drains.
            notified.await;
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
        let outcome = RequestFut::new(self.shared.clone(), request_id).await;
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
        let outcome = RequestFut::new(self.shared.clone(), request_id).await;
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
        let outcome = RequestFut::new(self.shared.clone(), request_id).await;
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
        let outcome = RequestFut::new(self.shared.clone(), request_id).await;
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
    cancel_on_drop: bool,
}

impl RequestFut {
    fn new(shared: Arc<ConnectionShared>, request_id: RequestId) -> Self {
        Self {
            shared,
            request_id,
            cancel_on_drop: false,
        }
    }

    fn cancellable(shared: Arc<ConnectionShared>, request_id: RequestId) -> Self {
        Self {
            shared,
            request_id,
            cancel_on_drop: true,
        }
    }
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
        if self.cancel_on_drop {
            self.shared.inner.lock().cancel_request(self.request_id);
        } else {
            let key = PendingOpKey::Request(self.request_id);
            self.shared.inner.lock().unregister_waker(key);
        }
    }
}

/// Normalise an advertised broker URL into the `host:port` form expected on
/// `CommandConnect.proxy_to_broker_url`. The Apache Pulsar Proxy parses that field
/// via `InetSocketAddress.createUnresolved`, so passing `pulsar://host:port` makes
/// `validateBrokerTarget()` return `false` and the proxy rejects the handshake with
/// `ServerError.ServiceNotReady "Target broker cannot be validated"` (ADR-0039,
/// parity with Java client + pulsar-rs + the tokio engine).
///
/// # Where the parse lives
///
/// The scheme-strip / default-port rule is **not** implemented here. It lives
/// in [`magnetar_proto::probe_authority`], which this function wraps with the
/// caller-specific error type. Until ADR-0087 this body carried its own copy
/// of that rule, arm for arm, agreeing with the other three copies only
/// because each had been written to match — the arrangement that produced the
/// ADR-0085 defect in the first place, where two copies of one rule rotted in
/// lockstep and no cross-engine differential test could see it.
///
/// Consequences of delegating, beyond the drift itself:
///
/// - A port-less bracketed IPv6 literal (`pulsar://[::1]`) now gets the scheme default port. The
///   local copy shared ADR-0085's documented gap here; closing it in `probe_authority` closed it
///   for every caller at once.
/// - `""` and `"pulsar://"` are now `Err`. The local copy had no empty-authority check, so they
///   returned `Ok("")` and `Ok(":6650")` — values that went on to the wire in
///   `CommandConnect.proxy_to_broker_url`.
///
/// Mirrors `magnetar_runtime_tokio::client::preferred_broker_url` in
/// scheme-strip shape (moonpool prefers `broker_service_url` where tokio's
/// TLS-posture pick differs — see
/// [`Client::lookup_topic_target_with_operation_deadline`]) but **not** in
/// error behaviour: `preferred_broker_url` warns and forwards an unrecognised
/// scheme unchanged, relying on the downstream proxy's
/// `validateBrokerTarget()` to reject it, which this helper deliberately does
/// NOT copy. A **bare** `host:port` carrying no `"://"` at all stays accepted
/// unchanged — a legitimate, tested input (see
/// `proxy_broker_authority_passes_through_bare_host_port`), not a corruption.
///
/// [ADR-0085]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0085-probe-endpoint-parsing-in-proto.md
/// [ADR-0087]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0087-unify-broker-url-authority-parsers.md
fn proxy_broker_authority(input: &str) -> Result<String, ClientError> {
    magnetar_proto::probe_authority(input).ok_or_else(|| unusable_broker_authority(input))
}

fn unusable_broker_authority(input: &str) -> ClientError {
    // One message for every rejection class the canonical helper folds into
    // `None` — unrecognised scheme, empty input, scheme with no authority.
    ClientError::Other(format!(
        "broker-advertised URL '{input}' is not a usable authority (expected \
         'pulsar://host[:port]', 'pulsar+ssl://host[:port]', or a scheme-less host with an \
         optional numeric port); refusing to derive a broker authority from it"
    ))
}

/// Normalise an advertised broker URL into the `host:port` form moonpool's
/// [`crate::transport::Transport::connect_with_resolver`] dials. Used by the
/// multi-broker DIRECT routing path (ADR-0039 §"Multi-broker DIRECT routing
/// (2026-06-01)") — the pool keys on `(logical, physical = host:port)` and dials
/// `physical` directly, so the helper must produce exactly the address shape
/// `connect_with_resolver` consumes. A scheme-less broker inherits the
/// bootstrap connection's protocol default before reaching the pool.
///
/// Accepts the same input shapes as the tokio engine's
/// `parse_direct_broker_url`: a full Pulsar URL (`pulsar://host:port` or
/// `pulsar+ssl://host:port`) **or** a scheme-less host with an optional port.
/// An explicit scheme supplies its protocol default; otherwise
/// `schemeless_default_port` supplies it. An explicit port always wins.
///
/// Shares the canonical parser with [`proxy_broker_authority`], but supplies
/// the bootstrap default because the DIRECT transport requires `host:port`.
/// Proxy routing keeps the no-fallback wrapper because a scheme-less logical
/// broker string is forwarded on the wire rather than dialled directly.
///
/// On the DIRECT path the two engines now agree: tokio's
/// `parse_direct_broker_url` rejects the same corrupted-scheme input rather
/// than falling through to its bare-`host:port` fallback, which used to
/// prefix a second scheme onto a string that already carried one and
/// mis-derive a garbage host with the WRONG default port (a distinct latent
/// bug from the truncation this function used to have; both are fixed).
/// Cross-engine equivalence for that rejection is pinned by
/// `crates/magnetar-differential/tests/corrupted_broker_scheme_equivalence.rs`.
/// [`proxy_broker_authority`]'s rejection remains moonpool-specific
/// hardening: tokio's PROXY-path `preferred_broker_url` still forwards an
/// unrecognised scheme unchanged with a warning, relying on the downstream
/// Pulsar Proxy's `validateBrokerTarget()` to reject it.
fn direct_broker_authority(
    input: &str,
    schemeless_default_port: u16,
) -> Result<String, ClientError> {
    magnetar_proto::broker_authority(input, Some(schemeless_default_port))
        .ok_or_else(|| unusable_broker_authority(input))
}

#[cfg(test)]
mod tests {
    use std::future::Future as _;
    use std::sync::atomic::AtomicUsize;
    use std::task::Context;
    use std::time::{Duration, Instant};

    use bytes::BytesMut;
    use magnetar_proto::{ConnectionConfig, encode_command, pb};
    use moonpool_core::TokioProviders;
    use parking_lot::Mutex;

    use super::{
        Client, ClientError, LookupIssue, LookupRetryState, LookupTopicResult,
        direct_broker_authority, proxy_broker_authority,
    };
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
    #[tokio::test(flavor = "current_thread")]
    async fn lookup_topic_result_alias_constructs() {
        let _: LookupTopicResult = LookupTopicResult::Connect {
            broker_service_url: Some("pulsar://broker:6650".to_owned()),
            broker_service_url_tls: None,
            proxy_through_service_url: false,
        };

        // Deterministically drive the terminal half of the SessionLost
        // re-issue decision. The first poll registers a real lookup; reset
        // publishes SessionLost, then the supervisor give-up state makes the
        // readiness waiter choose Terminal instead of re-issuing.
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let connected = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-test".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &connected).expect("encode CommandConnected");
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
        }
        let client: Client<TokioProviders> = Client {
            shared: shared.clone(),
            driver: Mutex::new(None),
            pool: None,
            connections_per_broker: 1,
            connection_rr: AtomicUsize::new(0),
            sleep_provider: crate::tokio_sleep_provider(),
            _providers: std::marker::PhantomData,
        };
        let mut deadline = Box::pin(std::future::pending::<()>());
        let mut retry_state = LookupRetryState::default();
        let mut last_broker_error = None;
        let mut lookup = Box::pin(client.issue_lookup_on(
            &shared,
            "persistent://public/default/terminal-session-lost",
            LookupIssue::Initial {
                authoritative: false,
            },
            deadline.as_mut(),
            &mut retry_state,
            &mut last_broker_error,
        ));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(lookup.as_mut().poll(&mut cx).is_pending());
        {
            let mut conn = shared.inner.lock();
            conn.reset();
            conn.mark_disconnected();
            conn.fail_all_pending("supervisor retry budget exhausted");
        }
        shared.mark_no_driver();
        shared.driver_waker.notify_waiters();
        assert!(matches!(lookup.await, Err(ClientError::PeerClosed)));
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
            proxy_broker_authority("pulsar+ssl://b-c3-n12:6651").unwrap(),
            "b-c3-n12:6651"
        );
    }

    #[test]
    fn proxy_broker_authority_strips_pulsar_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar://b-c3-n12:6650").unwrap(),
            "b-c3-n12:6650"
        );
    }

    #[test]
    fn proxy_broker_authority_appends_default_port_for_pulsar_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar://b-c3-n12").unwrap(),
            "b-c3-n12:6650"
        );
    }

    #[test]
    fn proxy_broker_authority_appends_default_port_for_pulsar_ssl_scheme() {
        assert_eq!(
            proxy_broker_authority("pulsar+ssl://b-c3-n12").unwrap(),
            "b-c3-n12:6651"
        );
    }

    #[test]
    fn proxy_broker_authority_passes_through_bare_host_port() {
        // Defensive: a broker that advertised `host:port` directly (no scheme) is forwarded
        // unchanged.
        assert_eq!(
            proxy_broker_authority("b-c3-n12:6650").unwrap(),
            "b-c3-n12:6650"
        );
    }

    #[test]
    fn proxy_broker_authority_trims_trailing_path_segments() {
        // Real lookup responses don't carry paths, but the helper is the only thing standing
        // between the broker's string and `CommandConnect`, so be defensive.
        assert_eq!(
            proxy_broker_authority("pulsar://b-c3-n12:6650/extra/path").unwrap(),
            "b-c3-n12:6650"
        );
    }

    /// A single-bit corruption of the `pulsar` scheme word, the shape
    /// moonpool-sim's bit-flip chaos actually produced for issue #364.
    const CORRUPTED_SCHEME_BROKER_URL: &str = "ptlsar://broker-sim.proxy.internal:6650";

    /// 1:1 twin of `magnetar_runtime_tokio::client::tests::
    /// parse_direct_broker_url_rejects_corrupted_scheme`.
    ///
    /// Both engines' DIRECT-path helpers now reject an unrecognised scheme
    /// outright. Until the tokio side was fixed, this pair could not exist:
    /// tokio's `parse_direct_broker_url` returned a fabricated
    /// `Ok(ParsedUrl { host: "ptlsar", port: 6650 })` for the identical input,
    /// so there was no behaviour to mirror and the private-fn coverage on this
    /// side lived only in the parity-exempt
    /// `tests/proxy_multi_conn.rs::open_producer_through_proxy_rejects_corrupted_broker_scheme`.
    #[test]
    fn direct_broker_authority_rejects_corrupted_scheme() {
        let err = direct_broker_authority(CORRUPTED_SCHEME_BROKER_URL, 6650)
            .expect_err("a corrupted scheme must not resolve to a dial target");
        assert!(
            matches!(err, ClientError::Other(_)),
            "expected the scheme rejection, got {err:?}",
        );
    }

    /// Twin of tokio's `parse_direct_broker_url_accepts_bare_host_port`:
    /// scheme-less input is a legitimate DIRECT-path shape on both engines.
    /// An explicit port is preserved; a missing port inherits the bootstrap
    /// default.
    #[test]
    fn direct_broker_authority_accepts_bare_host_port() {
        assert_eq!(
            direct_broker_authority("b-c3-n12:6650", 6650).unwrap(),
            "b-c3-n12:6650"
        );
        assert_eq!(
            direct_broker_authority("b-c3-n12", 6650).unwrap(),
            "b-c3-n12:6650"
        );
    }

    /// Twin of tokio's `parse_direct_broker_url_accepts_full_pulsar_url`: the
    /// ordinary shape a real broker advertises still resolves on both engines.
    #[test]
    fn direct_broker_authority_accepts_full_pulsar_url() {
        assert_eq!(
            direct_broker_authority("pulsar://b-c3-n12:6650", 7000).unwrap(),
            "b-c3-n12:6650"
        );
        assert_eq!(
            direct_broker_authority("pulsar+ssl://b-c3-n12", 6650).unwrap(),
            "b-c3-n12:6651"
        );
    }

    /// Both local adapters must follow their respective canonical helper:
    /// proxy parsing has no scheme-less default, while DIRECT parsing inherits
    /// the plaintext bootstrap default.
    #[test]
    fn broker_authority_adapters_follow_canonical_defaults() {
        const CASES: &[&str] = &[
            "pulsar://b-c3-n12:6650",
            "pulsar+ssl://b-c3-n12:6651",
            "PULSAR://b-c3-n12:6650",
            "PuLsAr+SsL://b-c3-n12:6651",
            "pulsar://b-c3-n12",
            "pulsar+ssl://b-c3-n12",
            "pulsar://b-c3-n12:7000",
            "pulsar://b-c3-n12:6650/extra/path",
            "b-c3-n12:6650",
            "b-c3-n12",
            "pulsar://[::1]:6650",
            "pulsar://[::1]",
            "[::1]",
            "ptlsar://broker-sim.proxy.internal:6650",
            "pulsar://broker:abc",
            "pulsar://[::1",
            ":6650",
            "broker:6650:extra",
            "",
            "pulsar://",
        ];

        for input in CASES {
            let proxy = proxy_broker_authority(input);
            let direct = direct_broker_authority(input, 6650);
            let canonical_proxy = magnetar_proto::probe_authority(input);
            let canonical_direct = magnetar_proto::broker_authority(input, Some(6650));

            assert_eq!(
                proxy.as_deref().ok(),
                canonical_proxy.as_deref(),
                "proxy_broker_authority({input:?}) diverged from probe_authority",
            );
            assert_eq!(
                direct.as_deref().ok(),
                canonical_direct.as_deref(),
                "direct_broker_authority({input:?}) diverged from broker_authority",
            );
            if canonical_proxy.is_none() {
                assert!(
                    matches!(proxy, Err(ClientError::Other(_))),
                    "proxy_broker_authority({input:?}) must reject via ClientError::Other, \
                     got {proxy:?}",
                );
            }
            if canonical_direct.is_none() {
                assert!(
                    matches!(direct, Err(ClientError::Other(_))),
                    "direct_broker_authority({input:?}) must reject via ClientError::Other, \
                     got {direct:?}",
                );
            }
        }
    }

    /// Regression test for ADR-0087: the local parser had **no**
    /// empty-authority check, so these two returned `Ok("")` and `Ok(":6650")`
    /// — authorities that went straight on to the wire in
    /// `CommandConnect.proxy_to_broker_url`. `probe_authority` rejects both, so
    /// delegating closed the hole.
    #[test]
    fn proxy_broker_authority_rejects_unusable_authority() {
        for input in ["", "pulsar://", "pulsar+ssl://"] {
            let err = proxy_broker_authority(input).expect_err(
                "an input with no authority must not resolve to a proxy target — pre-ADR-0087 \
                 this yielded Ok(\"\") / Ok(\":6650\")",
            );
            assert!(
                matches!(err, ClientError::Other(_)),
                "expected the authority rejection for {input:?}, got {err:?}",
            );
        }
    }

    /// Twin of tokio's `parse_direct_broker_url_reports_unusable_authority`:
    /// every structural rejection uses the shared operator-facing diagnostic.
    #[test]
    fn direct_broker_authority_reports_unusable_authority() {
        for input in ["pulsar://", "pulsar://broker:abc", "pulsar://[::1"] {
            let err = direct_broker_authority(input, 6650)
                .expect_err("an unusable authority must be rejected");
            assert!(
                err.to_string().contains("not a usable authority"),
                "unexpected rejection for {input:?}: {err}",
            );
        }
    }

    /// Regression test for ADR-0087, the closed half of ADR-0085's documented
    /// limitation: the synthesis used to trigger on "the authority contains no
    /// `:`", which is never true of a bracketed IPv6 literal, so
    /// `pulsar://[::1]` reached the transport port-less.
    #[test]
    fn proxy_broker_authority_synthesises_default_port_for_portless_bracketed_ipv6() {
        assert_eq!(
            proxy_broker_authority("pulsar://[::1]").unwrap(),
            "[::1]:6650"
        );
        assert_eq!(
            proxy_broker_authority("pulsar+ssl://[2001:db8::1]").unwrap(),
            "[2001:db8::1]:6651"
        );
        // The ported form is unaffected, on both routing paths.
        assert_eq!(
            proxy_broker_authority("pulsar://[::1]:6650").unwrap(),
            "[::1]:6650"
        );
        assert_eq!(
            direct_broker_authority("pulsar://[::1]", 6650).unwrap(),
            "[::1]:6650"
        );
    }
}
