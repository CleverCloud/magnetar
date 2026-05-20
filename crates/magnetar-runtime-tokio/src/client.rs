// SPDX-License-Identifier: Apache-2.0

//! Top-level `Client` façade.
//!
//! Builds a [`ConnectionShared`](crate::ConnectionShared), wires it to a
//! [`crate::transport::Transport`], starts the driver task, performs the Pulsar handshake, and
//! exposes [`open_producer`](Client::open_producer) / [`subscribe`](Client::subscribe).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, HandshakeState, OpOutcome,
    PendingOpKey, SubscribeRequest,
};
use parking_lot::Mutex;

use crate::ConnectionShared;
use crate::consumer::Consumer;
use crate::driver::{DriverHandle, spawn as spawn_driver};
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
        let mut roots = rustls::RootCertStore::empty();
        let mut cursor = std::io::Cursor::new(pem_bytes);
        for cert in rustls_pemfile::certs(&mut cursor) {
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
    /// # Errors
    ///
    /// Same as [`Self::connect`].
    pub async fn connect_with(
        url: ParsedUrl,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Result<Self, ClientError> {
        let socket = Transport::connect(&url, tls_config).await?;
        Self::start_handshake(socket, config, auth_provider).await
    }

    /// Drive the handshake against an already-connected socket. Useful for tests and for
    /// custom transports (e.g. `tokio::io::duplex` in tests).
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

    /// Wall-clock time the broker connection was most recently torn down (peer close, I/O
    /// error, local `close()`). `None` while the connection has never been disconnected.
    ///
    /// Mirrors Java's `Producer/Consumer#getLastDisconnectedTimestamp`. Convert with
    /// [`std::time::SystemTime::duration_since`] if the Java-style millis-since-epoch number
    /// is needed.
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
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

async fn wait_subscribe_acked(
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
                Some(ConnectionEvent::ConsumerClosedByBroker { handle, .. }) => {
                    if let EventMatcher::SubscribeAcked(h) = self.matcher {
                        if h == handle {
                            return Poll::Ready(Err(ClientError::Closed));
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

        // Also check the outcome slab — we may have an Error outcome correlated with an open
        // request. We don't have direct access to the request id here, but the connection emits
        // ProducerReady/SubscribeAcked alongside any successful outcome, so the event path is
        // sufficient. For error paths, the matching `Error` outcome is enqueued without a
        // dedicated event; future iterations should surface those — until then we time-out via
        // the connection-level operation_timeout, which the state machine enforces.

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
