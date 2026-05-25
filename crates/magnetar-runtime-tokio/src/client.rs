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
use crate::producer::Producer;
use crate::transport::{Transport, default_tls_config};
use crate::url_parse::ParsedUrl;

/// The top-level magnetar client.
///
/// Holds the shared connection state and the driver task. Producers and consumers created from
/// this client share the underlying connection.
#[derive(Debug)]
pub struct Client {
    shared: Arc<ConnectionShared>,
    driver: Mutex<Option<DriverHandle>>,
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
        let config = rustls::ClientConfig::builder()
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
        let socket =
            Transport::connect_with_resolver(&url, tls_config.clone(), dns_resolver.as_deref())
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
            Ok(()) => Ok(Self {
                shared,
                driver: Mutex::new(Some(driver)),
            }),
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
        let shared = ConnectionShared::with_auth(config, auth_provider);

        shared.inner.lock().begin_handshake()?;
        shared.driver_waker.notify_one();

        let ctx = ReconnectContext {
            url,
            tls_config,
            service_url_provider,
            dns_resolver,
        };
        let driver = spawn_supervised_driver(shared.clone(), socket, ctx);

        match wait_connected(shared.clone()).await {
            Ok(()) => Ok(Self {
                shared,
                driver: Mutex::new(Some(driver)),
            }),
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
        self.lookup_topic(&req.topic).await?;
        let handle = {
            let mut conn = self.shared.inner.lock();
            conn.create_producer(req)
        };
        self.shared.driver_waker.notify_one();
        wait_producer_ready(&self.shared, handle).await?;
        Ok(Producer {
            shared: self.shared.clone(),
            handle,
            compression,
            encryptor,
        })
    }

    /// Issue a `CommandLookupTopic` for `topic` and await its resolution.
    ///
    /// Standalone clusters resolve the topic to the same broker we are already connected to,
    /// so the call's only side effect is forcing the broker to activate the topic's
    /// namespace bundle. A multi-broker redirect (where the returned `broker_service_url`
    /// names a different broker) is logged as a warning and treated as success — the actual
    /// "reconnect to that broker" follow-up is tracked in `docs/follow-ups.md`. Until that
    /// lands, the user still hits the bundle-not-served path if the resolved broker differs
    /// from the current one, but the failure surfaces as a `ClientError::Broker` thanks to
    /// the `ProducerOpenFailed` / `SubscribeFailed` events instead of hanging.
    async fn lookup_topic(&self, topic: &str) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.lookup(topic, false)
        };
        self.shared.driver_waker.notify_one();
        let outcome = PartitionedMetadataFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::LookupResponse { outcome, .. } => match outcome {
                magnetar_proto::LookupOutcome::Connect {
                    broker_service_url,
                    broker_service_url_tls,
                    ..
                } => {
                    tracing::debug!(
                        topic,
                        broker_service_url = broker_service_url.as_deref(),
                        broker_service_url_tls = broker_service_url_tls.as_deref(),
                        "lookup resolved"
                    );
                    Ok(())
                }
                magnetar_proto::LookupOutcome::Redirected {
                    broker_service_url,
                    broker_service_url_tls,
                } => {
                    // The proto layer already chases redirects internally and only surfaces
                    // `Redirected` for observability after the redirect chain has settled.
                    // Treat as success.
                    tracing::warn!(
                        topic,
                        broker_service_url = broker_service_url.as_deref(),
                        broker_service_url_tls = broker_service_url_tls.as_deref(),
                        "broker redirected lookup; multi-broker redirect is follow-up work"
                    );
                    Ok(())
                }
                magnetar_proto::LookupOutcome::Failed { code, message } => {
                    Err(ClientError::Broker { code, message })
                }
            },
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected lookup outcome: {other:?}"
            ))),
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
        let outcome = PartitionedMetadataFut {
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
        // Step 1: lookup forces bundle ownership onto this broker.
        self.lookup_topic("persistent://pulsar/system/transaction_coordinator_assign-partition-0")
            .await?;
        // Step 2: explicit TC handshake — broker only responds once the TC metadata store is
        // loaded, eliminating the race between bundle-ownership-acquire and the first newTxn.
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.tc_client_connect(0)
        };
        self.shared.driver_waker.notify_one();
        let outcome = PartitionedMetadataFut {
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
        let outcome = PartitionedMetadataFut {
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
        let outcome = PartitionedMetadataFut {
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
        let outcome = PartitionedMetadataFut {
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
        let outcome = PartitionedMetadataFut {
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

    /// Query the broker for the number of partitions a topic has. Returns `0` for
    /// non-partitioned topics. Mirrors Java `PulsarClient#getPartitionsForTopic`.
    pub async fn partitioned_topic_metadata(&self, topic: &str) -> Result<u32, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_partitioned_topic_metadata(topic)
        };
        self.shared.driver_waker.notify_one();
        let outcome = PartitionedMetadataFut {
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
        // See `open_producer_with`: subscribe also needs lookup-driven bundle activation.
        self.lookup_topic(&req.topic).await?;
        let handle = {
            let mut conn = self.shared.inner.lock();
            conn.subscribe(req)
        };
        self.shared.driver_waker.notify_one();
        wait_subscribe_acked(&self.shared, handle).await?;

        // Feed an initial flow so the broker starts delivering.
        {
            let mut conn = self.shared.inner.lock();
            // `initial_flow` returns None when there is no consumer state; ignore that.
            let _ = conn.initial_flow(handle);
            // Also send an explicit FLOW with the configured queue size as a safety net for any
            // sans-io version that gates the initial flow on internal state we haven't reached.
            if receiver_queue_size > 0 {
                conn.flow(handle, receiver_queue_size as u32);
            }
        }
        self.shared.driver_waker.notify_one();

        Ok(Consumer {
            shared: self.shared.clone(),
            handle,
            decryptor,
        })
    }

    /// Close the connection. Sends `CommandCloseConnection`-style state-machine close, then
    /// waits for the driver task to exit.
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

async fn wait_connected(shared: Arc<ConnectionShared>) -> Result<(), ClientError> {
    ConnectedFut { shared }.await
}

/// Future that resolves once the state machine reports `HandshakeState::Connected` (or fails if
/// it transitions to `Failed`/`Closed` before that).
struct ConnectedFut {
    shared: Arc<ConnectionShared>,
}

impl Future for ConnectedFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
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
                Poll::Ready(Err(ClientError::Other("handshake failed".to_owned())))
            }
            HandshakeState::Closed => Poll::Ready(Err(ClientError::Closed)),
            _ => {
                // Park on the driver waker — it fires after every inbound packet.
                drop(conn);
                let waker = cx.waker().clone();
                let shared = self.shared.clone();
                tokio::spawn(async move {
                    shared.driver_waker.notified().await;
                    waker.wake();
                });
                Poll::Pending
            }
        }
    }
}

struct PartitionedMetadataFut {
    shared: Arc<ConnectionShared>,
    request_id: magnetar_proto::RequestId,
}

impl Future for PartitionedMetadataFut {
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

async fn wait_producer_ready(
    shared: &Arc<ConnectionShared>,
    handle: magnetar_proto::ProducerHandle,
) -> Result<(), ClientError> {
    // Drain the event queue until we see ProducerReady/ProducerClosedByBroker for our handle,
    // or until the producer-open request resolves with an Error outcome.
    EventWaitFut {
        shared: shared.clone(),
        matcher: EventMatcher::ProducerReady(handle),
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
    }
    .await
}

#[derive(Debug, Clone, Copy)]
enum EventMatcher {
    ProducerReady(magnetar_proto::ProducerHandle),
    SubscribeAcked(magnetar_proto::ConsumerHandle),
}

struct EventWaitFut {
    shared: Arc<ConnectionShared>,
    matcher: EventMatcher,
}

impl Future for EventWaitFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        // Inspect both events and the outcome slab.
        loop {
            match conn.poll_event() {
                Some(ConnectionEvent::ProducerReady { handle, .. }) => {
                    if let EventMatcher::ProducerReady(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                Some(ConnectionEvent::SubscribeAcked { handle }) => {
                    if let EventMatcher::SubscribeAcked(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                Some(ConnectionEvent::ProducerClosedByBroker { handle, .. }) => {
                    if let EventMatcher::ProducerReady(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Closed));
                        }
                    }
                }
                Some(ConnectionEvent::ProducerOpenFailed {
                    handle,
                    code,
                    message,
                }) => {
                    if let EventMatcher::ProducerReady(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Broker { code, message }));
                        }
                    }
                }
                Some(ConnectionEvent::ConsumerClosedByBroker { handle, .. }) => {
                    if let EventMatcher::SubscribeAcked(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Closed));
                        }
                    }
                }
                Some(ConnectionEvent::SubscribeFailed {
                    handle,
                    code,
                    message,
                }) => {
                    if let EventMatcher::SubscribeAcked(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Broker { code, message }));
                        }
                    }
                }
                Some(ConnectionEvent::Closed { reason }) => {
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

        let waker = cx.waker().clone();
        let shared = self.shared.clone();
        tokio::spawn(async move {
            shared.driver_waker.notified().await;
            waker.wake();
        });
        Poll::Pending
    }
}

// Keep the unused-imports happy on builds that don't enable the consumer/producer suite.
#[allow(dead_code)]
fn _opoutcome_usage_marker(_o: OpOutcome, _k: PendingOpKey) {}
