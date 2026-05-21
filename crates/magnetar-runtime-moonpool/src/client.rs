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
    /// Catch-all for engine-internal misconfiguration.
    #[error("other: {0}")]
    Other(String),
}

/// Outcome of a [`Client::lookup_topic`] call.
///
/// Re-export of [`magnetar_proto::event::LookupOutcome`]. The state machine
/// has already followed any `Redirect` chain internally; the user sees the
/// terminal outcome (`Connect` or `Failed`) plus — for observability — the
/// last `Redirected` variant if the broker chose to surface it.
pub type LookupTopicResult = LookupOutcome;

/// Top-level magnetar client, moonpool engine flavour.
///
/// Holds the shared connection state plus the driver task handle. Generic
/// over the [`Providers`] bundle so callers can plug in `TokioProviders` in
/// production or a `moonpool-sim` bundle in tests.
pub struct Client<P: Providers> {
    shared: Arc<ConnectionShared>,
    driver: Mutex<Option<DriverHandle>>,
    /// Held only so `Client` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers.
    _providers: std::marker::PhantomData<fn() -> P>,
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
    /// the scheme before calling). TLS lives in a follow-up milestone.
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
            .connect_plain_supervised(addr, config, service_url_provider, dns_resolver)
            .await?;
        Ok(Self {
            shared,
            driver: Mutex::new(Some(driver)),
            _providers: std::marker::PhantomData,
        })
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
    /// then joins the driver task.
    ///
    /// Idempotent: calling close more than once is a no-op on subsequent
    /// calls (the driver handle is taken on the first call).
    pub async fn close(self) {
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
    }

    /// Issue a `CommandLookupTopic` and await the broker's response.
    ///
    /// `authoritative` should be `false` for a fresh lookup; the state
    /// machine flips it to `true` on any internal redirect retry. The
    /// returned [`LookupTopicResult`] is the *terminal* outcome after the
    /// sans-io layer has followed any redirect chain.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker returns a `Failed` lookup.
    /// - [`ClientError::Other`] when an outcome other than [`OpOutcome::LookupResponse`] arrives on
    ///   this request id (this would be a state-machine bug, not a transient failure).
    pub async fn lookup_topic(
        &self,
        topic: &str,
        authoritative: bool,
    ) -> Result<LookupTopicResult, ClientError> {
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
}

/// Future that resolves the [`OpOutcome`] correlated with a single
/// `RequestId`. Mirrors the tokio engine's `PartitionedMetadataFut` — the
/// name there is misleading; it's the canonical "wait for a request-id-
/// correlated outcome" future, reused for lookup, partitioned metadata,
/// watch-topic-list-snapshot, and the txn family.
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use magnetar_proto::ConnectionConfig;
    use moonpool_core::TokioProviders;

    use super::{Client, ClientError, LookupTopicResult};
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
}
