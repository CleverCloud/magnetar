// SPDX-License-Identifier: Apache-2.0

//! Top-level `Client` façade.
//!
//! Builds a [`ConnectionShared`](crate::ConnectionShared), wires it to a
//! [`crate::transport::Transport`], starts the driver task, performs the Pulsar handshake, and
//! exposes [`open_producer`](Client::open_producer) / [`subscribe`](Client::subscribe).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, HandshakeState, OpOutcome,
    PendingOpKey, SubscribeRequest,
};
use parking_lot::Mutex;

use crate::ConnectionShared;
use crate::consumer::Consumer;
use crate::dns::DnsResolver;
use crate::driver::{
    DriverHandle, ReconnectContext, spawn as spawn_driver,
    spawn_supervised as spawn_supervised_driver,
};
use crate::error::ClientError;
use crate::pool::{ConnectionFactory, ProxyConnectionPool};
use crate::producer::Producer;
use crate::transport::{Transport, default_tls_config};
use crate::url_parse::{ParsedUrl, Scheme};

/// The top-level magnetar client.
///
/// Holds the bootstrap connection (the one dialled at `connect` time, used for lookup and
/// non-proxied producer / consumer ops) plus an opt-in per-broker connection pool (see
/// the crate-private `pool` module) for the Apache Pulsar Proxy case (ADR-0039): when a
/// `CommandLookupTopic` answer carries
/// `proxy_through_service_url = true`, the runtime lazily opens a second connection back to
/// the same physical address with `CommandConnect.proxy_to_broker_url` set to the logical
/// broker URL and routes the producer / consumer onto that connection.
///
/// The `pool` is `None` on the [`Client::from_socket`] path (test-only, no URL available).
/// Hitting a `proxy_through_service_url = true` lookup on that path surfaces a
/// [`ClientError::ProxyUnsupportedOnSocketClient`] — the user is expected to switch to a
/// URL-based connect to use the pool.
#[derive(Debug)]
pub struct Client {
    shared: Arc<ConnectionShared>,
    driver: Mutex<Option<DriverHandle>>,
    /// Lazy per-broker connection pool (ADR-0039). `None` on the
    /// non-URL [`Client::from_socket`] test path.
    pool: Option<Arc<ProxyConnectionPool>>,
}

/// Decision returned by [`Client::lookup_topic`] driving where the data ops for the resolved
/// topic should ride.
///
/// Mirrors the upstream Java client's
/// [`LookupTopicResult`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/LookupTopicResult.java)
/// (`logicalAddress`, `physicalAddress`, `isUseProxy`).
#[derive(Debug, Clone)]
enum LookupTarget {
    /// Direct connection — the broker is reachable directly (no proxy).
    ///
    /// * `broker_url = None` — the lookup did not advertise a broker URL (single-broker cluster or
    ///   pre-2.4 broker behaviour). The bootstrap connection serves as the data plane.
    /// * `broker_url = Some(url)` — the lookup resolved to a specific broker. If `url` matches the
    ///   bootstrap's `host:port`, the bootstrap connection is reused; otherwise
    ///   `Client::resolve_target` opens (or reuses) a pool entry that dials the resolved broker
    ///   **directly** (`CommandConnect.proxy_to_broker_url = None`). Multi-broker DIRECT routing —
    ///   ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)".
    Direct { broker_url: Option<String> },
    /// Proxy connection — the broker is reachable only through the proxy. Routes through
    /// the [`ProxyConnectionPool`] keyed by `broker_url`; dial target is the bootstrap's
    /// physical address (the proxy) and `CommandConnect.proxy_to_broker_url = Some(url)`.
    Proxy { broker_url: String },
}

/// Dial the transport with a bounded retry.
///
/// The *initial* dial is the one network step the reconnect supervisor cannot
/// recover (there is no connection yet to rebuild), so a dial that blocks — a
/// broker that accepts the SYN but never finishes establishing, or a transient
/// refusal — would otherwise propagate straight to the caller. Each attempt is
/// bounded by the `connect_timeout` chokepoint inside
/// [`Transport::connect_with_resolver`] (the dial closure carries it); here we
/// only retry the transient outcomes it surfaces. Only transient `Io` failures
/// (the chokepoint timeout lands as `Io(TimedOut)`) are retried; a permanent
/// error (bad TLS name/cert, protocol) is surfaced immediately. Production
/// analogue of the moonpool engine's `dial_with_retry` — both consume the same
/// [`ConnectionConfig::connect_max_retries`] / [`ConnectionConfig::operation_timeout`]
/// so the engines retry alike.
///
/// # Dual cap (Java parity)
///
/// Two independent caps bound the retry loop; whichever trips **first** ends
/// it:
///
/// 1. **Count** — `max_retries` re-dials (`connect_max_retries`).
/// 2. **Total budget** — `operation_timeout` (Java `operationTimeoutMs`) measured from the *first*
///    attempt. Tokio runs on real wall time with no virtual-clock hazard, so the elapsed check is a
///    plain [`std::time::Instant`] comparison; this mirrors the moonpool engine's
///    `now()`-comparison dual cap.
///
/// ([ADR-0052](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0052-initial-connect-timeout-retry.md))
async fn dial_with_retry<S, F, Fut>(
    max_retries: u32,
    operation_timeout: std::time::Duration,
    mut dial: F,
) -> Result<S, ClientError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<S, ClientError>>,
{
    let started = std::time::Instant::now();
    let mut attempt: u32 = 0;
    loop {
        let err = match dial().await {
            Ok(socket) => return Ok(socket),
            Err(err) => err,
        };
        // Dual cap: stop on a non-transient error, on the count backstop, or
        // once the total operation budget is spent — whichever trips first.
        if !matches!(err, ClientError::Io(_))
            || attempt >= max_retries
            || started.elapsed() >= operation_timeout
        {
            return Err(err);
        }
        attempt += 1;
        tokio::time::sleep(connect_backoff(attempt)).await;
    }
}

/// Exponential backoff for [`dial_with_retry`]: 50 ms doubling, capped at 1 s.
/// Matches the moonpool engine's schedule so the two engines retry in lockstep.
fn connect_backoff(attempt: u32) -> std::time::Duration {
    let shift = attempt.saturating_sub(1).min(5);
    std::time::Duration::from_millis((50u64 << shift).min(1_000))
}

impl Client {
    /// Connect to the given `url` using the supplied protocol-layer config.
    ///
    /// Performs the TCP connect, the optional TLS handshake (for `pulsar+ssl://`), the Pulsar
    /// `CommandConnect` round-trip, and returns once the broker has confirmed via
    /// `CommandConnected`.
    ///
    /// # Errors
    ///
    /// Surfaces socket I/O, TLS, and protocol errors via [`ClientError`].
    pub async fn connect(url: &str, config: ConnectionConfig) -> Result<Self, ClientError> {
        Self::connect_auth(url, config, None).await
    }

    /// Connect with an in-band auth provider used to answer broker
    /// `CommandAuthChallenge` (PIP-30 / PIP-292) for in-band token refresh.
    pub async fn connect_auth(
        url: &str,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Result<Self, ClientError> {
        let parsed = ParsedUrl::parse(url)?;
        let tls_config = match parsed.scheme {
            crate::url_parse::Scheme::Tls => Some(default_tls_config()?),
            crate::url_parse::Scheme::Plain => None,
        };
        Self::connect_with(parsed, tls_config, config, auth_provider).await
    }

    /// Build a [`rustls::ClientConfig`] whose trust anchors are the PEM-encoded certificate
    /// chain in `pem_bytes` (the system trust store is NOT loaded). Mirrors Java
    /// `ClientBuilder#tlsTrustCertsFilePath` — useful when the broker uses a self-signed cert.
    ///
    /// The rustls crypto provider is picked by the workspace's `crypto-*`
    /// feature (issue #9, ADR-0035) via the explicit
    /// [`crate::tls_crypto::active_provider`] shim.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Other`] if no valid certificate is parsed from the PEM.
    pub fn tls_config_from_pem(pem_bytes: &[u8]) -> Result<Arc<rustls::ClientConfig>, ClientError> {
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::pem::PemObject;

        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(pem_bytes) {
            let cert = cert.map_err(|e| {
                ClientError::Other(format!("failed to parse a trust certificate from PEM: {e}"))
            })?;
            roots.add(cert).map_err(|e| {
                ClientError::Other(format!("rustls rejected a trust certificate: {e}"))
            })?;
        }
        if roots.is_empty() {
            return Err(ClientError::Other(
                "no trust certificates were parsed from the provided PEM".to_owned(),
            ));
        }
        let config =
            rustls::ClientConfig::builder_with_provider(crate::tls_crypto::active_provider())
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    ClientError::Other(format!(
                        "rustls rejected the workspace's default protocol versions: {e}"
                    ))
                })?
                .with_root_certificates(roots)
                .with_no_client_auth();
        Ok(Arc::new(config))
    }

    /// Connect using a pre-parsed URL and an explicit TLS configuration. Intended for advanced
    /// callers that need to customise trust anchors / client certificates / ALPN.
    ///
    /// When `config.supervisor` is `Some`, the resulting driver task is wrapped with the
    /// auto-reconnect supervisor: subsequent transport drops trigger a backoff-driven
    /// reconnect against `url` (and `tls_config` if `pulsar+ssl://`).
    ///
    /// # Errors
    ///
    /// Same as [`Self::connect`].
    pub async fn connect_with(
        url: ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_provider(url, tls_config, config, auth_provider, None).await
    }

    /// Same as [`Self::connect_with`] but also threads a PIP-121
    /// [`magnetar_proto::ServiceUrlProvider`] through to the auto-reconnect supervisor. When
    /// `service_url_provider` is `Some`, the supervisor re-resolves the broker URL via
    /// `provider.get_service_url()` on every reconnect attempt — so cluster-failover policies
    /// can swap broker addresses without the client being rebuilt.
    ///
    /// `service_url_provider = None` matches [`Self::connect_with`] exactly: the cached `url`
    /// is reused for every reconnect.
    pub async fn connect_with_provider(
        url: ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
        service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_resolver_and_provider(
            url,
            tls_config,
            config,
            auth_provider,
            service_url_provider,
            None,
        )
        .await
    }

    /// Same as [`Self::connect_with_provider`] but also threads a pluggable DNS resolver
    /// (Java `ClientBuilder#dnsResolver`) through to the internal
    /// `Transport::connect_with_resolver` entry. When `dns_resolver` is `Some`,
    /// every initial and reconnect dial routes the `(host, port)` lookup through
    /// `resolver.resolve(...)`; when `None`, the runtime falls back to tokio's
    /// built-in [`tokio::net::lookup_host`] — identical to
    /// [`Self::connect_with_provider`].
    pub async fn connect_with_resolver_and_provider(
        url: ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
        service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
        dns_resolver: Option<Arc<dyn DnsResolver>>,
    ) -> Result<Self, ClientError> {
        let connect_timeout = config.connect_timeout;
        let socket = dial_with_retry(config.connect_max_retries, config.operation_timeout, || {
            Transport::connect_with_resolver(
                &url,
                tls_config.clone(),
                dns_resolver.as_deref(),
                connect_timeout,
            )
        })
        .await?;
        Self::start_supervised_handshake(
            socket,
            url,
            tls_config,
            config,
            auth_provider,
            service_url_provider,
            dns_resolver,
        )
        .await
    }

    /// Drive the handshake against an already-connected socket. Useful for tests and for
    /// custom transports (e.g. `tokio::io::duplex` in tests). The auto-reconnect supervisor
    /// is **not** wired in on this path: a raw socket cannot be reopened after a drop.
    pub async fn from_socket<S>(socket: S, config: ConnectionConfig) -> Result<Self, ClientError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        Self::start_handshake(socket, config, None).await
    }

    async fn start_handshake<S>(
        socket: S,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Result<Self, ClientError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let shared = ConnectionShared::with_auth(config, auth_provider);

        // Queue the CONNECT frame BEFORE spawning the driver — otherwise the driver might block
        // on a read before we have anything to flush.
        shared.inner.lock().begin_handshake()?;
        // Wake the driver immediately so it flushes the CONNECT.
        shared.driver_waker.notify_one();

        let driver = spawn_driver(shared.clone(), socket);

        // Park until the state machine emits `ConnectionEvent::Connected`. We do this with a
        // local future that polls the event queue.
        match wait_connected(shared.clone()).await {
            Ok(()) => {
                // Lifecycle record (ADR-0054). The generic-socket path has no
                // URL, so the dial target is reported as the transport kind.
                // `auth_method` is the provider's method name — NEVER
                // `auth_data` (ADR-0054 no-secrets rule).
                let auth_method = shared
                    .auth_provider
                    .as_deref()
                    .map_or("none", |p| p.method());
                tracing::info!(
                    auth_method,
                    transport = "generic-socket",
                    "connection established"
                );
                Ok(Self {
                    shared,
                    driver: Mutex::new(Some(driver)),
                    // `from_socket` has no URL to dial back through, so the proxy pool is
                    // disabled. A lookup that returns `proxy_through_service_url = true`
                    // surfaces `ClientError::ProxyUnsupportedOnSocketClient`.
                    pool: None,
                })
            }
            Err(e) => {
                driver.abort();
                Err(e)
            }
        }
    }

    async fn start_supervised_handshake(
        socket: Transport,
        url: ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
        service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
        dns_resolver: Option<Arc<dyn DnsResolver>>,
    ) -> Result<Self, ClientError> {
        // Clone the connect-time inputs into a `ConnectionFactory` so the proxy pool can
        // lazily open per-broker pinned connections later (ADR-0039). The bootstrap conn
        // itself does NOT set `proxy_to_broker_url` (it's the lookup-and-control plane).
        let factory = ConnectionFactory {
            url: url.clone(),
            tls_config: tls_config.clone(),
            bootstrap_config: config.clone(),
            auth_provider: auth_provider.clone(),
            service_url_provider: service_url_provider.clone(),
            dns_resolver: dns_resolver.clone(),
        };

        let shared = ConnectionShared::with_auth(config, auth_provider);

        shared.inner.lock().begin_handshake()?;
        shared.driver_waker.notify_one();

        // Snapshot the dial identity for the lifecycle record below — `url`
        // and `tls_config` move into the reconnect context.
        let host = url.host.clone();
        let port = url.port;
        let tls = tls_config.is_some();

        let ctx = ReconnectContext {
            url,
            tls_config,
            service_url_provider,
            dns_resolver,
        };
        let driver = spawn_supervised_driver(shared.clone(), socket, ctx);

        match wait_connected(shared.clone()).await {
            Ok(()) => {
                // Lifecycle record (ADR-0054). `auth_method` is the
                // provider's method name — NEVER `auth_data` (no-secrets
                // rule).
                let auth_method = shared
                    .auth_provider
                    .as_deref()
                    .map_or("none", |p| p.method());
                tracing::info!(host = %host, port, tls, auth_method, "connection established");
                Ok(Self {
                    shared,
                    driver: Mutex::new(Some(driver)),
                    pool: Some(ProxyConnectionPool::new(factory)),
                })
            }
            Err(e) => {
                driver.abort();
                Err(e)
            }
        }
    }

    /// Borrow the shared state machine. Mostly useful for tests and instrumentation.
    pub fn shared(&self) -> &Arc<ConnectionShared> {
        &self.shared
    }

    /// Open a producer.
    ///
    /// Returns once the broker has sent `CommandProducerSuccess`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Broker`] if the broker refuses the producer.
    pub async fn open_producer(&self, req: CreateProducerRequest) -> Result<Producer, ClientError> {
        self.open_producer_with(req, None).await
    }

    /// Same as [`Self::open_producer`] but with an optional encryption hook.
    pub async fn open_producer_with(
        &self,
        req: CreateProducerRequest,
        encryptor: Option<Arc<dyn crate::crypto::MessageEncryptor>>,
    ) -> Result<Producer, ClientError> {
        let compression = req.compression;
        // Pulsar requires a `CommandLookupTopic` round-trip before opening a producer or
        // consumer: lookup is what triggers the broker to acquire ownership of the topic's
        // namespace bundle. Skipping it works only when the bundle has already been activated
        // by some prior operation; a fresh broker rejects `CommandProducer` with
        // `ServerError::ServiceNotReady` ("not served by this instance, please redo the
        // lookup"). Java's `PulsarClientImpl#createProducerAsync` does the same lookup.
        //
        // ADR-0039: the lookup result decides whether the producer rides on the bootstrap
        // connection (direct, no proxy) or on a per-broker pool entry (proxy-routed). The
        // `Producer` keeps an `Arc<ConnectionShared>` pointing at whichever connection it
        // was opened against, so subsequent sends / closes go to the right socket.
        let target = self.lookup_topic(&req.topic).await?;
        let topic = req.topic.clone();
        let target_shared = self.resolve_target(target, &topic).await?;
        let (handle, slot) = {
            let mut conn = target_shared.inner.lock();
            let handle = conn.create_producer(req);
            let slot = conn
                .producer(handle)
                .cloned()
                .expect("just-created producer slot must exist");
            (handle, slot)
        };
        target_shared.driver_waker.notify_one();
        wait_producer_ready(&target_shared, handle).await?;
        // Lifecycle record (ADR-0054): the broker-assigned producer name is
        // available once `ProducerReady` has landed. Per-slot read only.
        let producer_name = slot.state.lock().name.clone().unwrap_or_default();
        tracing::info!(
            topic = %slot.identity.topic,
            producer_name = %producer_name,
            handle = ?handle,
            access_mode = ?slot.identity.access_mode,
            "producer created"
        );
        Ok(Producer {
            shared: target_shared,
            handle,
            slot,
            compression,
            encryptor,
        })
    }

    /// Issue a `CommandLookupTopic` for `topic` and decide where the topic's data ops should
    /// ride. Returns one of the [`LookupTarget`] variants:
    ///
    /// - [`LookupTarget::Direct { broker_url: None }`] — no broker URL was advertised on the
    ///   lookup; the bootstrap connection serves as the data plane (single-broker behaviour).
    /// - [`LookupTarget::Direct { broker_url: Some(url) }`] — the lookup resolved to a specific
    ///   broker (`proxy_through_service_url = false`). [`Self::resolve_target`] either reuses the
    ///   bootstrap (when `url` matches the bootstrap `host:port`) or opens a pinned pool entry that
    ///   dials the resolved broker directly. ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)".
    /// - [`LookupTarget::Proxy { broker_url }`] — the lookup answered `proxy_through_service_url =
    ///   true`. The runtime opens (or reuses) a pool entry for `broker_url` with
    ///   `CommandConnect.proxy_to_broker_url = broker_url`; the data ops ride on that pool entry.
    ///   See ADR-0039.
    ///
    /// **HIGH-4 (lookup multi-agent review)**: the proto-layer state machine chases
    /// redirect chains internally and only ever delivers terminal outcomes
    /// (`LookupOutcome::Connect` / `LookupOutcome::Failed`) to the user-facing future.
    /// Intermediate `LookupOutcome::Redirected` outcomes never reach this function —
    /// they're pushed to the proto events queue for diagnostics only — so the broker
    /// URL we observe here is the resolved tail of the chain. The redirect cap from
    /// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] surfaces as `LookupOutcome::Failed`
    /// → [`ClientError::Broker`] when a hostile broker exhausts the budget.
    async fn lookup_topic(&self, topic: &str) -> Result<LookupTarget, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.lookup(topic, false)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::LookupResponse { outcome, .. } => {
                match outcome {
                    magnetar_proto::LookupOutcome::Connect {
                        broker_service_url,
                        broker_service_url_tls,
                        proxy_through_service_url,
                    } => {
                        tracing::debug!(
                            topic,
                            broker_service_url = broker_service_url.as_deref(),
                            broker_service_url_tls = broker_service_url_tls.as_deref(),
                            proxy_through_service_url,
                            "lookup resolved"
                        );
                        if proxy_through_service_url {
                            // ADR-0039: pick the broker URL that matches our TLS posture. If we
                            // are on `pulsar+ssl://` and the broker advertises a TLS URL we
                            // prefer it; otherwise fall back to the plain URL. If the broker
                            // declined to advertise any URL we error out — the proxy cannot
                            // route us.
                            let broker_url = preferred_broker_url(
                            broker_service_url,
                            broker_service_url_tls,
                            self.bootstrap_scheme(),
                        )
                        .ok_or_else(|| ClientError::Other(format!(
                            "lookup of '{topic}' set proxy_through_service_url=true but did \
                             not advertise a broker_service_url or broker_service_url_tls"
                        )))?;
                            Ok(LookupTarget::Proxy { broker_url })
                        } else {
                            // ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)": capture
                            // the resolved broker URL so `resolve_target` can route the data
                            // ops to the right broker on multi-broker clusters. If the
                            // broker declined to advertise a URL we keep `None`, preserving
                            // the pre-amendment behaviour (route on the bootstrap connection)
                            // for single-broker setups.
                            let broker_url = direct_broker_url(
                                broker_service_url,
                                broker_service_url_tls,
                                self.bootstrap_scheme(),
                            );
                            Ok(LookupTarget::Direct { broker_url })
                        }
                    }
                    // HIGH-4: `Redirected` is no longer surfaced to the engine —
                    // the proto state machine chases the chain internally and only
                    // publishes terminal outcomes (`Connect` / `Failed`) against the
                    // user-facing request-id. Intermediate `Redirected` outcomes ride
                    // the proto events queue for diagnostics only. Keep the match
                    // exhaustive so a future variant addition is a hard compile error.
                    magnetar_proto::LookupOutcome::Redirected { .. } => Err(ClientError::Other(
                        "BUG: intermediate Redirected outcome leaked to the user-facing \
                         future — proto layer should chase redirects internally and only \
                         deliver terminal outcomes (HIGH-4)"
                            .to_owned(),
                    )),
                    magnetar_proto::LookupOutcome::Failed { code, message } => {
                        Err(ClientError::Broker { code, message })
                    }
                }
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected lookup outcome: {other:?}"
            ))),
        }
    }

    /// Resolve the [`LookupTarget`] to the `Arc<ConnectionShared>` the caller should drive
    /// CommandProducer / CommandSubscribe on.
    ///
    /// * [`LookupTarget::Direct { broker_url: None }`] — bootstrap connection (no broker URL was
    ///   advertised; single-broker behaviour).
    /// * [`LookupTarget::Direct { broker_url: Some(url) }`] — multi-broker DIRECT routing. If
    ///   `url`'s `host:port` matches the bootstrap, reuse the bootstrap. Otherwise open (or reuse)
    ///   a pool entry keyed by `(url, url)` and dial the resolved broker directly
    ///   (`CommandConnect.proxy_to_broker_url = None`). ADR-0039 §"Multi-broker DIRECT routing
    ///   (2026-06-01)".
    /// * [`LookupTarget::Proxy { broker_url }`] — opens (or reuses) the pool entry keyed by
    ///   `(broker_url, bootstrap URL)` with `CommandConnect.proxy_to_broker_url =
    ///   Some(broker_url)`.
    async fn resolve_target(
        &self,
        target: LookupTarget,
        topic: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError> {
        match target {
            LookupTarget::Direct { broker_url: None } => Ok(self.shared.clone()),
            LookupTarget::Direct {
                broker_url: Some(broker_url),
            } => self.resolve_direct_broker(&broker_url, topic).await,
            LookupTarget::Proxy { broker_url } => {
                let pool = self.pool.as_ref().ok_or_else(|| {
                    ClientError::ProxyUnsupportedOnSocketClient {
                        topic: topic.to_owned(),
                    }
                })?;
                // Every proxy pool entry dials the same physical address — the proxy URL the
                // bootstrap was built with. The proto-layer's `CommandConnect.proxy_to_broker_url`
                // is what tells the proxy which backend broker this connection serves.
                let physical = pool.bootstrap_url();
                pool.get_or_open(&broker_url, &physical, Some(broker_url.clone()))
                    .await
            }
        }
    }

    /// Resolve a multi-broker DIRECT routing target. If `broker_url` matches the bootstrap's
    /// `host:port`, the bootstrap connection is reused (no extra dial). Otherwise the pool
    /// opens (or reuses) a pinned connection that dials `broker_url` directly with
    /// `CommandConnect.proxy_to_broker_url = None`. ADR-0039 §"Multi-broker DIRECT routing
    /// (2026-06-01)".
    ///
    /// `broker_url` may be either a full Pulsar URL (e.g. `pulsar://broker-1:6650`) or a
    /// bare `host:port` (the `host:port` form is what
    /// [`crate::client::direct_broker_url`] / [`preferred_broker_url`] emit). Both forms
    /// must round-trip to the same parsed `(host, port)` for the bootstrap-equality check
    /// to bypass the pool dial.
    async fn resolve_direct_broker(
        &self,
        broker_url: &str,
        topic: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError> {
        let Some(pool) = self.pool.as_ref() else {
            // `from_socket` callers have no URL to dial — the bootstrap is the only
            // connection available. Single-broker scenarios still work; multi-broker
            // dial requests would have nowhere to land.
            tracing::warn!(
                topic,
                broker_url,
                "lookup resolved to a specific broker but client has no dial-able URL \
                 (from_socket); falling back to bootstrap connection"
            );
            return Ok(self.shared.clone());
        };
        let parsed = parse_direct_broker_url(broker_url, pool.bootstrap_scheme())?;

        // Bootstrap-equality fast path: same `host:port` as the connect-time URL → reuse the
        // bootstrap connection. Saves one TCP/TLS handshake on every single-broker /
        // bootstrap-broker lookup, and keeps existing single-broker tests on exactly one
        // socket (no spurious pool entry).
        let bootstrap = pool.bootstrap_url();
        if parsed.host == bootstrap.host && parsed.port == bootstrap.port {
            return Ok(self.shared.clone());
        }

        // Different broker → pin a dedicated pool entry. `logical == physical` here because
        // we are dialling the broker directly (the proxy is not in the picture). The pool
        // is keyed on `(logical, "host:port")` so two DIRECT lookups to the same broker URL
        // share one entry, just like two PROXY lookups for the same backend share one.
        pool.get_or_open(broker_url, &parsed, None).await
    }

    /// Scheme of the bootstrap connection — used by [`Self::lookup_topic`] to pick between
    /// `broker_service_url` and `broker_service_url_tls`. We pull it back from the
    /// `ProxyConnectionPool`'s factory snapshot when available; the `from_socket` path falls
    /// back to `Plain` because there is no URL to consult. The result feeds
    /// [`preferred_broker_url`] for the proxy-pool routing decision.
    fn bootstrap_scheme(&self) -> Scheme {
        match &self.pool {
            Some(pool) => pool.bootstrap_scheme(),
            None => Scheme::Plain,
        }
    }

    /// Open a new Pulsar transaction at the broker-side transaction coordinator (PIP-31).
    /// Mirrors Java `PulsarClient#newTransaction()`. Returns the new [`magnetar_proto::TxnId`]
    /// once the TC acknowledges.
    pub async fn new_txn(
        &self,
        timeout: std::time::Duration,
    ) -> Result<magnetar_proto::TxnId, ClientError> {
        self.ensure_txn_bootstrapped().await?;
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
            magnetar_proto::OpOutcome::NewTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("new_txn: {err}")))
            }
            other => Err(ClientError::Other(format!(
                "unexpected new_txn outcome: {other:?}"
            ))),
        }
    }

    /// Force the broker to load the TC partition we will talk to. Pulsar brokers only assign
    /// a `TransactionMetadataStore` to a TC partition once a client explicitly handshakes via
    /// `CommandTcClientConnectRequest` (or an internal subscription opens the
    /// `__transaction_coordinator_assign-partition-N` topic). The Java client does that
    /// handshake inside `TransactionMetaStoreHandler.connectionOpened` →
    /// `Commands.newTcClientConnectRequest`. We mirror it on first use:
    ///
    /// 1. `CommandLookupTopic` on
    ///    `persistent://pulsar/system/transaction_coordinator_assign-partition-0` — forces the
    ///    broker to take ownership of the matching namespace bundle. Lookup alone is **not**
    ///    enough: ownership transfer is asynchronous, and races with `CommandNewTxn` reach the
    ///    broker before `handleMetadataStoreLoad(tcId)` finishes.
    /// 2. `CommandTcClientConnectRequest(tc_id=0)` — the broker only acknowledges this once the TC
    ///    metadata store for `tc_id` is fully loaded, which closes the race window.
    ///
    /// Subsequent `new_txn` calls observe `txn_bootstrapped` and skip the handshake. The flag
    /// stays set across reconnects: the broker persists the TC store on disk, so once loaded
    /// it survives the connection drop. magnetar currently pins the TC id to `0`
    /// (`TxnClient::new(0)`); multi-TC support would need to fan this handshake out per
    /// `tc_id`.
    async fn ensure_txn_bootstrapped(&self) -> Result<(), ClientError> {
        if self.shared.txn_bootstrapped.load(Ordering::Acquire) {
            return Ok(());
        }
        // Step 1: lookup forces bundle ownership onto this broker. The TC bundle lives on the
        // bootstrap connection regardless of what the lookup returns — `new_txn` and friends
        // below all drive `self.shared`. (If a proxy ever advertised the TC bundle on a
        // different broker we'd still hit the bundle-not-served path on `tc_client_connect`,
        // which is correct: the TC client's broker is configured separately from the data plane.)
        let _ = self
            .lookup_topic("persistent://pulsar/system/transaction_coordinator_assign-partition-0")
            .await?;
        // Step 2: explicit TC handshake — broker only responds once the TC metadata store is
        // loaded, eliminating the race between bundle-ownership-acquire and the first newTxn.
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.tc_client_connect(0)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => {}
            OpOutcome::Error { code, message, .. } => {
                return Err(ClientError::Broker { code, message });
            }
            other => {
                return Err(ClientError::Other(format!(
                    "unexpected tc_client_connect outcome: {other:?}"
                )));
            }
        }
        self.shared.txn_bootstrapped.store(true, Ordering::Release);
        Ok(())
    }

    /// Register `topic` as a partition this transaction will write to (PIP-31).
    /// Mirrors `Transaction#registerProducedTopic`.
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
            magnetar_proto::OpOutcome::AddPartitionToTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("add_partition_to_txn: {err}")))
            }
            other => Err(ClientError::Other(format!(
                "unexpected add_partition_to_txn outcome: {other:?}"
            ))),
        }
    }

    /// Register a subscription this transaction will acknowledge on (PIP-31).
    /// Mirrors `Transaction#registerSubscriptionToTxn`.
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
            magnetar_proto::OpOutcome::AddSubscriptionToTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("add_subscription_to_txn: {err}")))
            }
            other => Err(ClientError::Other(format!(
                "unexpected add_subscription_to_txn outcome: {other:?}"
            ))),
        }
    }

    /// Commit or abort an open transaction (PIP-31). Returns the final transaction state
    /// reported by the TC. Mirrors `Transaction#commit` / `#abort`.
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
            magnetar_proto::OpOutcome::EndTxn { result, .. } => {
                result.map_err(|err| ClientError::Other(format!("end_txn: {err}")))
            }
            other => Err(ClientError::Other(format!(
                "unexpected end_txn outcome: {other:?}"
            ))),
        }
    }

    /// Subscribe to a topic-list watcher (PIP-145) and return the initial topic snapshot
    /// for the given namespace + regex. Subsequent updates are emitted as
    /// `ConnectionEvent::TopicListChanged` events; this snapshot helper does not stream
    /// them — pair with a follow-up event-poll API for that.
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
            magnetar_proto::OpOutcome::TopicListSnapshot { topics, .. } => Ok(topics),
            magnetar_proto::OpOutcome::Error { code, message, .. } => {
                Err(ClientError::Broker { code, message })
            }
            other => Err(ClientError::Other(format!(
                "unexpected topic-list snapshot outcome: {other:?}"
            ))),
        }
    }

    /// Await the next PIP-145 `TopicListChanged` delta. Resolves with the broker-reported
    /// added / removed topics when the next watcher delta arrives, or `None` if the
    /// connection has closed and no further deltas will arrive. Pair with
    /// [`Self::watch_topic_list`] to first establish the watcher subscription.
    ///
    /// The future is cancel-safe: dropping it without polling does not drop pending
    /// deltas, the next `next_topic_list_change` call still sees them.
    pub async fn next_topic_list_change(&self) -> Option<crate::TopicListChange> {
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

    /// Non-blocking peek for the next PIP-145 topic-list delta. Returns `None` when the
    /// queue is empty.
    #[must_use]
    pub fn poll_topic_list_change(&self) -> Option<crate::TopicListChange> {
        self.shared.topic_list_changes.lock().pop_front()
    }

    /// PIP-33: await the next replicated-subscription marker observed by any consumer on
    /// this connection. Resolves once the broker emits a `REPLICATED_SUBSCRIPTION_*`
    /// marker on a subscribed topic (typically once per snapshot interval, default 1s
    /// when the namespace has `replicated_subscription_status=true`), or `None` if the
    /// connection has closed. Markers are filtered off the regular [`Consumer::receive`]
    /// stream — applications that just want to consume messages don't need this.
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

    /// Non-blocking peek for the next replicated-subscription marker observation.
    /// Returns `None` when the buffer is empty.
    #[must_use]
    pub fn poll_replicated_subscription_marker(
        &self,
    ) -> Option<crate::ObservedReplicatedSubscriptionMarker> {
        self.shared
            .replicated_subscription_markers
            .lock()
            .pop_front()
    }

    /// Query the broker for the number of partitions a topic has. Returns `0` for
    /// non-partitioned topics. Mirrors Java `PulsarClient#getPartitionsForTopic`.
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
            magnetar_proto::OpOutcome::PartitionedMetadata {
                partitions, error, ..
            } => {
                if let Some((code, message)) = error {
                    Err(ClientError::Broker { code, message })
                } else {
                    Ok(partitions)
                }
            }
            other => Err(ClientError::Other(format!(
                "unexpected partitioned metadata outcome: {other:?}"
            ))),
        }
    }

    // -------------------------------------------------------------------
    // PIP-460 scalable topics (ADR-0031, experimental). Drives the proto
    // `Connection` scalable entries + reads the driver-drained events from
    // `shared.scalable_events` via the same buffer + Notify pattern as the
    // PIP-145 topic-list deltas. No channels.
    // -------------------------------------------------------------------

    /// **Experimental** (PIP-460, ADR-0031). Resolve a `topic://...` scalable
    /// topic: issue a `CommandScalableTopicLookup` and await the broker's
    /// `CommandScalableTopicLookupResponse`. Returns the controller-broker URL,
    /// the current segment DAG, and the monotonic lookup token to thread into
    /// [`Self::open_scalable_dag_watch`].
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
            // Drain any matching resolved event for our request id.
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

    /// **Experimental** (PIP-460, ADR-0031). Open a DAG-watch session for
    /// `topic`, seeded with the lookup `segments` snapshot + `lookup_token`.
    /// Returns the client-allocated watch session id. The caller drives
    /// updates via [`Self::next_scalable_event`].
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
    /// event (DAG update / drop-on-change / close) drained by the driver.
    /// Returns `None` once the connection closes. Cancel-safe: dropping the
    /// future without polling does not lose buffered events.
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

    /// Subscribe to a topic.
    ///
    /// Returns once the broker has acked the subscribe (`CommandSuccess` correlated with the
    /// request id).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Broker`] if the broker refuses the subscribe.
    pub async fn subscribe(&self, req: SubscribeRequest) -> Result<Consumer, ClientError> {
        self.subscribe_with(req, None).await
    }

    /// Same as [`Self::subscribe`] but with an optional decryption hook (PIP-4).
    pub async fn subscribe_with(
        &self,
        req: SubscribeRequest,
        decryptor: Option<Arc<dyn crate::crypto::MessageDecryptor>>,
    ) -> Result<Consumer, ClientError> {
        let receiver_queue_size = req.receiver_queue_size;
        // See `open_producer_with`: subscribe also needs lookup-driven bundle activation,
        // and ADR-0039 routes proxy-resolved subscribes onto a pinned pool entry.
        let target = self.lookup_topic(&req.topic).await?;
        let topic = req.topic.clone();
        let target_shared = self.resolve_target(target, &topic).await?;
        let (handle, slot) = {
            let mut conn = target_shared.inner.lock();
            let handle = conn.subscribe(req);
            let slot = conn
                .consumer(handle)
                .cloned()
                .expect("just-created consumer slot must exist");
            (handle, slot)
        };
        target_shared.driver_waker.notify_one();
        wait_subscribe_acked(&target_shared, handle).await?;

        // Feed an initial flow so the broker starts delivering.
        {
            let mut conn = target_shared.inner.lock();
            // `initial_flow` returns None when there is no consumer state; ignore that.
            let _ = conn.initial_flow(handle);
            // Also send an explicit FLOW with the configured queue size as a safety net for any
            // sans-io version that gates the initial flow on internal state we haven't reached.
            if receiver_queue_size > 0 {
                conn.flow(handle, receiver_queue_size as u32);
            }
        }
        target_shared.driver_waker.notify_one();

        // Lifecycle record (ADR-0054).
        tracing::info!(
            topic = %slot.identity.topic,
            subscription = %slot.identity.subscription,
            handle = ?handle,
            "consumer subscribed"
        );

        Ok(Consumer {
            shared: target_shared,
            handle,
            slot,
            decryptor,
        })
    }

    /// Close the connection. Sends `CommandCloseConnection`-style state-machine close on the
    /// bootstrap connection, joins its driver, then closes every entry in the proxy pool
    /// (ADR-0039) and joins their drivers.
    ///
    /// Idempotent: calling close more than once does nothing on the subsequent calls.
    pub async fn close(self) {
        {
            let mut conn = self.shared.inner.lock();
            conn.close();
        }
        self.shared.driver_waker.notify_one();
        let handle = self.driver.lock().take();
        if let Some(handle) = handle {
            // We deliberately discard the join result here — close() is best-effort; consumers
            // that want the terminal error should call `join_driver` instead.
            let _ = handle.join().await;
        }
        // Tear down the proxy pool (ADR-0039). Pool entries are independent supervised
        // driver loops; each one observes its own `is_user_closed()` after we call close().
        if let Some(pool) = self.pool.as_ref() {
            pool.close().await;
        }
    }

    /// Take the driver handle so the caller can wait for it explicitly. After this call the
    /// `Client` will not join the driver on `close()`.
    pub fn take_driver(&self) -> Option<DriverHandle> {
        self.driver.lock().take()
    }

    /// Returns `true` while the underlying broker connection is in
    /// [`HandshakeState::Connected`]. Mirrors `org.apache.pulsar.client.api.Producer#isConnected`
    /// at the connection scope (Java exposes it on `Producer`/`Consumer`; magnetar's runtime
    /// keeps all producers/consumers from a single `Client` on one shared connection, so the
    /// same predicate answers both).
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// `true` once [`Self::close`] has been called or the broker connection has otherwise
    /// entered a terminal state. Mirrors Java `PulsarClient#isClosed`. After this returns
    /// `true` no new producer / consumer opens can succeed; pair with [`Self::is_connected`]
    /// for the live test.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.inner.lock().is_closed()
    }

    /// Wall-clock time the broker connection was most recently torn down (peer close, I/O
    /// error, local `close()`). `None` while the connection has never been disconnected.
    ///
    /// Mirrors Java's `Producer/Consumer#getLastDisconnectedTimestamp`. Convert with
    /// [`std::time::SystemTime::duration_since`] if the Java-session-millis-since-epoch number
    /// is needed.
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
    }

    /// Monotonic counter bumped each time the auto-reconnect supervisor severs the broker
    /// session via [`magnetar_proto::Connection::reset`]. Callers detect a reconnect by
    /// observing this value change between two operations on the same `Client`. Mirrors
    /// Java `ClientCnx#getEpoch` at the client-façade scope.
    #[must_use]
    pub fn session_epoch(&self) -> u64 {
        self.shared.inner.lock().session_epoch()
    }
}

/// Pick the broker URL the proxy advertised for the topic — the value to thread into
/// `CommandConnect.proxy_to_broker_url` on the pinned pool entry (ADR-0039). Prefers
/// the TLS URL when the bootstrap connection is on `pulsar+ssl://`, otherwise picks the
/// plain URL. Returns `None` if the broker declined to advertise any URL (which the
/// proxy contract says shouldn't happen but the broker is the broker).
///
/// The advertised value (e.g. `pulsar://broker-c3-n12:6650`) is normalised to the
/// `host:port` form Apache Pulsar Proxy expects on
/// `CommandConnect.proxy_to_broker_url` — the Java reference client and pulsar-rs both
/// send it scheme-less. The proxy parses the field via
/// `InetSocketAddress.createUnresolved`; an unstripped `pulsar://...` makes
/// `validateBrokerTarget()` return `false` and the proxy rejects the handshake with
/// `ServerError.ServiceNotReady "Target broker cannot be validated"`.
fn preferred_broker_url(
    broker_url: Option<String>,
    broker_url_tls: Option<String>,
    scheme: Scheme,
) -> Option<String> {
    let raw = match scheme {
        Scheme::Tls => broker_url_tls.or(broker_url),
        Scheme::Plain => broker_url.or(broker_url_tls),
    }?;
    if let Ok(parsed) = ParsedUrl::parse(&raw) {
        Some(format!("{}:{}", parsed.host, parsed.port))
    } else {
        tracing::warn!(
            broker_url = %crate::log_fields::truncate_broker_str(&raw),
            "lookup advertised broker URL with unparseable scheme; \
             forwarding unchanged — proxy may reject handshake",
        );
        Some(raw)
    }
}

/// Pick the broker URL the lookup advertised on the DIRECT
/// (`proxy_through_service_url = false`) path. Mirrors
/// [`preferred_broker_url`] in TLS-posture preference but **keeps the full Pulsar
/// URL** (e.g. `pulsar://broker-1:6650`) instead of stripping the scheme: on the
/// DIRECT path the URL is fed back into [`ParsedUrl::parse`] to recover the dial
/// target and TLS choice (whereas the proxy path needs the scheme-less
/// `host:port` form `CommandConnect.proxy_to_broker_url` expects per ADR-0045).
///
/// Returns `None` when the lookup omitted both `broker_service_url` and
/// `broker_service_url_tls` (pre-2.4 brokers, single-broker setups). The caller
/// folds `None` into `LookupTarget::Direct { broker_url: None }` so the
/// bootstrap connection serves as the data plane unchanged.
fn direct_broker_url(
    broker_url: Option<String>,
    broker_url_tls: Option<String>,
    scheme: Scheme,
) -> Option<String> {
    match scheme {
        Scheme::Tls => broker_url_tls.or(broker_url),
        Scheme::Plain => broker_url.or(broker_url_tls),
    }
}

/// Parse a broker URL captured on the DIRECT lookup path into a
/// [`ParsedUrl`] usable by the pool's [`Transport::connect_with_resolver`]
/// call. Accepts both the full `pulsar://host:port` form and the bare
/// `host:port` form (the latter is what [`preferred_broker_url`] emits for the
/// proxy path — when a DIRECT lookup ever hands us that form we still want to
/// dial it). The scheme falls back to the bootstrap scheme when missing —
/// brokers in a single cluster typically run the same TLS posture, and the
/// bootstrap's `tls_config` is the only one the pool has on hand.
fn parse_direct_broker_url(
    broker_url: &str,
    bootstrap_scheme: Scheme,
) -> Result<ParsedUrl, ClientError> {
    if let Ok(parsed) = ParsedUrl::parse(broker_url) {
        return Ok(parsed);
    }
    // Try as bare `host:port`. We synthesise a `pulsar://`-prefixed URL using
    // the bootstrap scheme so [`ParsedUrl::parse`] does the host/port split
    // for us — this also catches subtle inputs like IPv6 literals consistently
    // with the rest of the runtime.
    let scheme_prefix = match bootstrap_scheme {
        Scheme::Tls => "pulsar+ssl://",
        Scheme::Plain => "pulsar://",
    };
    let synthetic = format!("{scheme_prefix}{broker_url}");
    ParsedUrl::parse(&synthetic).map_err(|err| {
        ClientError::Other(format!(
            "lookup advertised broker URL '{broker_url}' that is neither a Pulsar URL nor a \
             host:port pair the runtime can dial: {err}"
        ))
    })
}

pub(crate) async fn wait_connected(shared: Arc<ConnectionShared>) -> Result<(), ClientError> {
    ConnectedFut {
        shared,
        helper: None,
    }
    .await
}

/// Future that resolves once the state machine reports `HandshakeState::Connected` (or fails if
/// it transitions to `Failed`/`Closed` before that).
struct ConnectedFut {
    shared: Arc<ConnectionShared>,
    helper: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ConnectedFut {
    fn drop(&mut self) {
        if let Some(h) = self.helper.take() {
            h.abort();
        }
    }
}

impl Future for ConnectedFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut conn = this.shared.inner.lock();
        // Drain events, looking for Connected. We don't care about the others at this stage.
        while let Some(ev) = conn.poll_event() {
            match ev {
                ConnectionEvent::Connected { .. } => return Poll::Ready(Ok(())),
                ConnectionEvent::Closed { reason } => {
                    return Poll::Ready(Err(ClientError::Other(
                        reason.unwrap_or_else(|| "connection closed during handshake".into()),
                    )));
                }
                _ => {
                    // Tolerate other events that may sneak in (none expected pre-handshake).
                }
            }
        }
        match conn.state() {
            HandshakeState::Connected => Poll::Ready(Ok(())),
            HandshakeState::Failed => {
                // Prefer the broker-supplied reason if the peer sent a
                // `CommandError` mid-handshake (proxy auth rejection,
                // namespace not found via proxy_to_broker_url, etc.). Falls
                // back to the opaque message for raw transport drops where no
                // protocol frame ever arrived (TLS error, ECONNREFUSED).
                let msg = conn.handshake_failure_reason().map_or_else(
                    || "handshake failed".to_owned(),
                    |reason| format!("handshake failed: {reason}"),
                );
                Poll::Ready(Err(ClientError::Other(msg)))
            }
            HandshakeState::Closed => Poll::Ready(Err(ClientError::Closed)),
            _ => {
                // Park on the driver waker — it fires after every inbound packet.
                // Abort any prior helper so a stale `notified()` waiter from an
                // earlier poll can't swallow a `notify_one` permit intended for
                // the driver loop.
                drop(conn);
                if let Some(prev) = this.helper.take() {
                    prev.abort();
                }
                let waker = cx.waker().clone();
                let shared = this.shared.clone();
                this.helper = Some(tokio::spawn(async move {
                    shared.driver_waker.notified().await;
                    waker.wake();
                }));
                Poll::Pending
            }
        }
    }
}

/// "Wait for the broker reply to a request-id-keyed command" future.
///
/// Reused for lookup, partitioned-metadata, watch-topic-list-snapshot, and
/// the txn family — i.e. every command whose response is correlated by
/// `request_id` rather than by `producer_id` / `consumer_id`. Mirrors the
/// moonpool engine's identically-named `RequestFut`.
pub(crate) struct RequestFut {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) request_id: magnetar_proto::RequestId,
}

impl Future for RequestFut {
    type Output = magnetar_proto::OpOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let key = magnetar_proto::PendingOpKey::Request(self.request_id);
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
    /// a cancelled `partitioned_metadata` / `lookup` future does not leave a
    /// dangling [`std::task::Waker`] behind. Without this hook the entry is
    /// otherwise reclaimed lazily — either when the matching broker response
    /// finally lands (the dispatcher `remove`s the waker and wakes a no-op
    /// task) or on the next [`magnetar_proto::Connection::reset`] — but for
    /// long-running connections that issue many short-lived lookups whose
    /// callers cancel before the round-trip completes, those entries
    /// accumulate. Defense-in-depth per the lookup multi-agent review
    /// MEDIUM-4 finding. ADR-0024 four-layer coverage lives in
    /// `tests/lookup_drop_unregister.rs`.
    fn drop(&mut self) {
        let key = magnetar_proto::PendingOpKey::Request(self.request_id);
        self.shared.inner.lock().unregister_waker(key);
    }
}

async fn wait_producer_ready(
    shared: &Arc<ConnectionShared>,
    handle: magnetar_proto::ProducerHandle,
) -> Result<(), ClientError> {
    // Drain the event queue until we see ProducerReady/ProducerClosedByBroker for our handle,
    // or until the producer-open request resolves with an Error outcome.
    EventWaitFut {
        shared: shared.clone(),
        matcher: EventMatcher::ProducerReady(handle),
        helper: None,
    }
    .await
}

pub(crate) async fn wait_subscribe_acked(
    shared: &Arc<ConnectionShared>,
    handle: magnetar_proto::ConsumerHandle,
) -> Result<(), ClientError> {
    EventWaitFut {
        shared: shared.clone(),
        matcher: EventMatcher::SubscribeAcked(handle),
        helper: None,
    }
    .await
}

#[derive(Debug, Clone, Copy)]
enum EventMatcher {
    ProducerReady(magnetar_proto::ProducerHandle),
    SubscribeAcked(magnetar_proto::ConsumerHandle),
}

/// Each `Pending` return spawns a helper that awaits `driver_waker.notified()`
/// and wakes the caller; on the next poll (or on drop) the previous helper is
/// aborted. Without that abort, the stale helper from an earlier poll keeps
/// waiting on `driver_waker.notified()` and competes with the driver loop for
/// the `notify_one` permits that user-facing futures emit after enqueueing
/// outbound work — when the helper wins the race, the freshly-queued frame
/// (e.g. the post-subscribe FLOW) sits in `outbound` and the driver stays
/// parked, deterministically hanging the next `receive()`.
struct EventWaitFut {
    shared: Arc<ConnectionShared>,
    matcher: EventMatcher,
    helper: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for EventWaitFut {
    fn drop(&mut self) {
        if let Some(h) = self.helper.take() {
            h.abort();
        }
    }
}

impl Future for EventWaitFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut conn = this.shared.inner.lock();
        // Inspect both events and the outcome slab.
        loop {
            match conn.poll_event() {
                Some(ConnectionEvent::ProducerReady { handle, .. }) => {
                    if let EventMatcher::ProducerReady(h) = this.matcher {
                        if h == handle {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                Some(ConnectionEvent::SubscribeAcked { handle }) => {
                    if let EventMatcher::SubscribeAcked(h) = this.matcher {
                        if h == handle {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                Some(ConnectionEvent::ProducerClosedByBroker {
                    handle,
                    assigned_broker_service_url,
                }) => {
                    if let EventMatcher::ProducerReady(h) = this.matcher {
                        if h == handle {
                            // Broker-forced close — degraded-but-recovering
                            // (warn! per ADR-0054 §2.1); the open future
                            // surfaces `Closed` and the caller decides.
                            let topic = conn
                                .producer(handle)
                                .map(|s| s.identity.topic.clone())
                                .unwrap_or_default();
                            tracing::warn!(
                                handle = ?handle,
                                topic = %topic,
                                assigned_broker_service_url = assigned_broker_service_url
                                    .as_deref()
                                    .map(crate::log_fields::truncate_broker_str),
                                "broker closed producer while waiting for ProducerReady"
                            );
                            return Poll::Ready(Err(ClientError::Closed));
                        }
                    }
                }
                Some(ConnectionEvent::ProducerOpenFailed {
                    handle,
                    code,
                    message,
                }) => {
                    if let EventMatcher::ProducerReady(h) = this.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Broker { code, message }));
                        }
                    }
                }
                Some(ConnectionEvent::ConsumerClosedByBroker {
                    handle,
                    assigned_broker_service_url,
                }) => {
                    if let EventMatcher::SubscribeAcked(h) = this.matcher {
                        if h == handle {
                            // Broker-forced close — warn! per ADR-0054 §2.1.
                            let (topic, subscription) = conn
                                .consumer(handle)
                                .map(|s| {
                                    (s.identity.topic.clone(), s.identity.subscription.clone())
                                })
                                .unwrap_or_default();
                            tracing::warn!(
                                handle = ?handle,
                                topic = %topic,
                                subscription = %subscription,
                                assigned_broker_service_url = assigned_broker_service_url
                                    .as_deref()
                                    .map(crate::log_fields::truncate_broker_str),
                                "broker closed consumer while waiting for SubscribeAcked"
                            );
                            return Poll::Ready(Err(ClientError::Closed));
                        }
                    }
                }
                Some(ConnectionEvent::SubscribeFailed {
                    handle,
                    code,
                    message,
                }) => {
                    if let EventMatcher::SubscribeAcked(h) = this.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Broker { code, message }));
                        }
                    }
                }
                Some(ConnectionEvent::Closed { reason }) => {
                    // Broker/connection-level forced close — warn! per
                    // ADR-0054 §2.1. `reason` is broker-controlled text.
                    tracing::warn!(
                        reason = reason
                            .as_deref()
                            .map(crate::log_fields::truncate_broker_str),
                        "connection closed while waiting for producer/consumer readiness"
                    );
                    return Poll::Ready(Err(ClientError::Other(
                        reason.unwrap_or_else(|| "connection closed".into()),
                    )));
                }
                Some(_) => {} // ignore unrelated events
                None => break,
            }
        }

        // Success-side: `ProducerSuccess` / `Success` in the sans-io layer push
        // `ProducerReady` / `SubscribeAcked` events. Failure-side: a `CommandError` correlated
        // with the pending producer-open / subscribe pushes the matching
        // `ProducerOpenFailed` / `SubscribeFailed` event. Both paths are observed by the match
        // arms above, so we never need to peek at the request-id-keyed outcome slab here.

        drop(conn);

        // Abort the prior helper (if any) before spawning a new one. Otherwise the
        // stale helper from an earlier poll lingers on `driver_waker.notified()`
        // and competes with the driver itself for `notify_one` permits emitted by
        // user-facing futures (post-subscribe FLOW being the classic case).
        if let Some(prev) = this.helper.take() {
            prev.abort();
        }
        let waker = cx.waker().clone();
        let shared = this.shared.clone();
        this.helper = Some(tokio::spawn(async move {
            shared.driver_waker.notified().await;
            waker.wake();
        }));
        Poll::Pending
    }
}

// Keep the unused-imports happy on builds that don't enable the consumer/producer suite.
#[allow(dead_code)]
fn _opoutcome_usage_marker(_o: OpOutcome, _k: PendingOpKey) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferred_broker_url_strips_scheme_on_tls_bootstrap() {
        let got = preferred_broker_url(
            Some("pulsar://b-c3-n12:6650".to_owned()),
            Some("pulsar+ssl://b-c3-n12:6651".to_owned()),
            Scheme::Tls,
        );
        assert_eq!(got.as_deref(), Some("b-c3-n12:6651"));
    }

    #[test]
    fn preferred_broker_url_strips_scheme_on_plain_bootstrap() {
        let got = preferred_broker_url(
            Some("pulsar://b-c3-n12:6650".to_owned()),
            Some("pulsar+ssl://b-c3-n12:6651".to_owned()),
            Scheme::Plain,
        );
        assert_eq!(got.as_deref(), Some("b-c3-n12:6650"));
    }

    #[test]
    fn preferred_broker_url_falls_back_when_preferred_missing() {
        // TLS bootstrap, but the broker only advertised the plain URL — fall back to plain.
        let got =
            preferred_broker_url(Some("pulsar://b-c3-n12:6650".to_owned()), None, Scheme::Tls);
        assert_eq!(got.as_deref(), Some("b-c3-n12:6650"));
    }

    #[test]
    fn preferred_broker_url_default_port_when_url_has_none() {
        // Broker advertised a URL without explicit port. Default port from the URL scheme
        // (NOT the bootstrap scheme) — same convention as pulsar-rs.
        let got = preferred_broker_url(Some("pulsar://b-c3-n12".to_owned()), None, Scheme::Plain);
        assert_eq!(got.as_deref(), Some("b-c3-n12:6650"));

        let got = preferred_broker_url(None, Some("pulsar+ssl://b-c3-n12".to_owned()), Scheme::Tls);
        assert_eq!(got.as_deref(), Some("b-c3-n12:6651"));
    }

    #[test]
    fn preferred_broker_url_returns_none_when_no_url_advertised() {
        assert!(preferred_broker_url(None, None, Scheme::Tls).is_none());
        assert!(preferred_broker_url(None, None, Scheme::Plain).is_none());
    }

    #[test]
    fn preferred_broker_url_passes_through_unparseable_input() {
        // Defensive fallback: if the broker advertised a value we can't parse as a
        // Pulsar URL, forward it as-is (with a warning). Better than dropping the
        // lookup result on the floor.
        let got = preferred_broker_url(Some("not a url".to_owned()), None, Scheme::Plain);
        assert_eq!(got.as_deref(), Some("not a url"));
    }
}
