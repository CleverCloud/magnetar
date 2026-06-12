// SPDX-License-Identifier: Apache-2.0

//! The per-connection I/O driver task for the moonpool engine.
//!
//! Mirrors the structure of [`magnetar_runtime_tokio::driver`] but is generic
//! over [`moonpool_core::Providers`] so the same engine works on real Tokio
//! sockets and on a `moonpool-sim` deterministic substrate.
//!
//! One driver task per connection. Owns the [`Transport`] and the per-connection
//! read/write buffers and loops over:
//!
//! 1. drain outbound bytes from the sans-io state machine into a write buffer,
//! 2. flush the write buffer to the socket,
//! 3. park on either fresh outbound work ([`ConnectionShared::driver_waker`]), inbound bytes from
//!    the wire, or the next scheduled timeout,
//! 4. tick timers when their deadline elapses,
//! 5. dispatch semantic events that need runtime-layer work (`AuthChallenge`, `TopicListChanged`,
//!    `TopicMigrated`).
//!
//! The driver does **not** wake user-facing futures itself — the sans-io
//! layer does that when an `OpOutcome` lands. See
//! [GUIDELINES.md] §"No-channels rule".
//!
//! # Supervisor (auto-reconnect)
//!
//! When [`magnetar_proto::ConnectionConfig::supervisor`] is `Some` and the
//! driver is spawned via [`spawn_supervised`], the per-socket loop is wrapped
//! in a backoff-driven reconnect cycle. The cycle:
//!
//! 1. runs [`driver_loop_inner`] until the socket errors or the peer closes,
//! 2. checks whether the user requested a graceful close (state machine `is_closed`) — if so, exits
//!    cleanly,
//! 3. otherwise reads [`magnetar_proto::SupervisorConfig`] off the state machine, builds a
//!    [`magnetar_proto::Backoff`], and sleeps for the next backoff interval (via the moonpool
//!    [`moonpool_core::TimeProvider`] so `moonpool-sim` keeps the schedule deterministic),
//! 4. reconnects via [`Transport::connect_with_resolver`] (routing through the optional
//!    `dns_resolver` carried on [`ReconnectContext`]), calls [`magnetar_proto::Connection::reset`]
//!    (which fails request-bound ops with [`magnetar_proto::OpOutcome::SessionLost`] and snapshots
//!    in-flight publishes for transparent replay), restarts the handshake, and resumes step 1.
//!
//! Stage 3 (producer / consumer state replay): after the new socket completes
//! its handshake, the inner loop calls
//! [`magnetar_proto::Connection::rebuild_producers`] and
//! [`magnetar_proto::Connection::rebuild_consumers`], which re-emit every
//! still-open handle's `CommandProducer` / `CommandSubscribe` against the new
//! transport, and replay every snapshotted in-flight publish verbatim. User-facing
//! send futures stay pending across the reset until the replayed
//! `CommandSendReceipt` lands (at-least-once publish parity with the Java client).
//!
//! [GUIDELINES.md]: https://github.com/CleverCloud/magnetar/blob/main/GUIDELINES.md
//! [`Transport`]: crate::transport::Transport

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionEvent, ConsumerHandle, OpOutcome, PendingOpKey, ProducerHandle};
use moonpool_core::{Providers, TaskProvider, TimeProvider};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::dns::DnsResolver;
use crate::transport::Transport;
use crate::{ConnectionShared, EngineError, ObservedReplicatedSubscriptionMarker, TopicListChange};

/// Default size of the per-connection read buffer. Reads are non-blocking
/// and append-style, so this is just the high-water mark before allocation
/// grows.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

/// Delay before a transient producer-open / subscribe retry leg re-issues its
/// lookup, mirroring the tokio engine's `tokio::time::sleep` of the same
/// duration (`magnetar-runtime-tokio/src/driver.rs`). Scheduled through the
/// injected [`TimeProvider`] — never a host clock — so under `SimProviders`
/// the retry fires at a deterministic point in virtual time (ADR-0011).
const TRANSIENT_RETRY_DELAY: Duration = Duration::from_secs(2);

/// A transient broker rejection that the driver must answer with a delayed
/// lookup-then-retry leg. Drained out of [`handle_pending_events`] (which is
/// non-generic and has no provider access) so the generic
/// [`driver_loop_inner`] can dispatch each one as a detached task through the
/// engine's [`TaskProvider`] + [`TimeProvider`] — matching the tokio engine's
/// `tokio::spawn` + `tokio::time::sleep` serialization so the differential
/// event order stays identical (ADR-0024).
#[derive(Debug, Clone, Copy)]
enum RetryRequest {
    /// The broker bounced a `CommandProducer` with a transient code; re-run
    /// lookup, then [`magnetar_proto::Connection::retry_producer_open`].
    Producer(ProducerHandle),
    /// The broker bounced a `CommandSubscribe` with a transient code; re-run
    /// lookup, then [`magnetar_proto::Connection::retry_consumer_subscribe`].
    Consumer(ConsumerHandle),
}

/// Drain the connection's semantic event queue of events the *driver* must
/// react to, leaving every other event (`Connected`, `SendReceipt`,
/// `Message`, `ProducerReady`, `SubscribeAcked`, …) in the queue for
/// user-facing futures to observe.
///
/// We use [`magnetar_proto::Connection::poll_event_if`] with an explicit
/// allow-list rather than draining the whole queue: an unconditional
/// `poll_event` loop would silently consume the `ProducerReady` /
/// `SubscribeAcked` events that user futures (`ProducerReadyFut`, the
/// moonpool consumer's subscribe wait) are parked on and stall every
/// open-producer / subscribe round-trip.
///
/// Transient producer-open / subscribe rejections are NOT actioned inline:
/// this function is non-generic and has no provider access, and the retry
/// leg must sleep on the injected [`TimeProvider`] (never a host clock) to
/// stay deterministic under `SimProviders` (ADR-0011). Each such event is
/// drained into a [`RetryRequest`] appended to `retries`; the generic
/// [`driver_loop_inner`] dispatches them as detached tasks (1:1 with the
/// tokio engine's `tokio::spawn` shape, so the differential event order
/// stays identical — ADR-0024).
fn handle_pending_events(
    shared: &Arc<ConnectionShared>,
    retries: &mut Vec<RetryRequest>,
) -> Result<(), EngineError> {
    loop {
        let event = shared.inner.lock().poll_event_if(|ev| {
            #[cfg(feature = "scalable-topics")]
            if matches!(
                ev,
                ConnectionEvent::ScalableTopicLookupResolved { .. }
                    | ConnectionEvent::SegmentDagUpdated { .. }
                    | ConnectionEvent::DagChangedDuringConsume { .. }
                    | ConnectionEvent::DagWatchClosed { .. }
            ) {
                return true;
            }
            matches!(
                ev,
                ConnectionEvent::AuthChallenge { .. }
                    | ConnectionEvent::TopicListChanged { .. }
                    | ConnectionEvent::TopicMigrated { .. }
                    | ConnectionEvent::RedirectUrlRejected { .. }
                    | ConnectionEvent::ProducerOpenFailedTransient { .. }
                    | ConnectionEvent::SubscribeFailedTransient { .. }
                    | ConnectionEvent::ReplicatedSubscriptionMarkerObserved { .. }
                    | ConnectionEvent::ChecksumMismatch { .. }
                    | ConnectionEvent::LookupResponse {
                        result: magnetar_proto::LookupOutcome::Redirected { .. },
                        ..
                    }
            )
        });
        let Some(event) = event else {
            return Ok(());
        };
        match event {
            ConnectionEvent::AuthChallenge { method, challenge } => {
                let Some(provider) = shared.auth_provider.clone() else {
                    // `method` is the broker-requested auth method —
                    // hostile-peer-controlled, so it is truncated before
                    // landing in the field (ADR-0054). Mirror of the tokio
                    // driver.
                    tracing::warn!(
                        auth_method = method
                            .as_deref()
                            .map_or("none", crate::log_fields::truncate_broker_str),
                        "broker requested in-band auth refresh but no AuthProvider configured; \
                         the connection will be reset"
                    );
                    return Err(EngineError::Config(
                        "broker requested AUTH_CHALLENGE but client has no auth provider"
                            .to_owned(),
                    ));
                };
                let bytes = challenge.unwrap_or_default();
                // ADR-0054 no-secrets rule: the challenge bytes and the
                // refreshed credential are NEVER logged, at any level.
                tracing::debug!(
                    auth_method = %provider.method(),
                    "auth challenge received; refreshing credentials"
                );
                let refreshed = match provider.respond_to_challenge(&bytes) {
                    Ok(refreshed) => refreshed,
                    Err(err) => {
                        // ADR-0054 auth-path rule: a third-party
                        // `AuthProvider`'s `Display`/`Debug` impl is an
                        // uncontrolled secret channel — log the method plus
                        // a stable error class only, never the provider
                        // error chain. The full error still reaches the
                        // caller via the returned `EngineError`.
                        tracing::warn!(
                            auth_method = %provider.method(),
                            error_class = "auth_refresh_failed",
                            "in-band auth refresh failed; the connection will be reset"
                        );
                        return Err(EngineError::Config(format!("auth refresh failed: {err}")));
                    }
                };
                let method = provider.method().to_owned();
                shared
                    .inner
                    .lock()
                    .submit_auth_response(refreshed, Some(method));
                shared.driver_waker.notify_one();
            }
            ConnectionEvent::TopicListChanged { added, removed } => {
                shared
                    .topic_list_changes
                    .lock()
                    .push_back(TopicListChange { added, removed });
                shared.topic_list_notify.notify_waiters();
            }
            ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle, marker } => {
                // PIP-33 (ADR-0034): drain off the proto-level event queue.
                shared
                    .replicated_subscription_markers
                    .lock()
                    .push_back(ObservedReplicatedSubscriptionMarker { handle, marker });
                shared
                    .replicated_subscription_marker_notify
                    .notify_waiters();
            }
            ConnectionEvent::RedirectUrlRejected {
                source,
                broker_service_url,
                broker_service_url_tls,
            } => {
                // Defence-in-depth mirror of the tokio runtime: the
                // configured `redirect_url_allow_list` refused the
                // broker-advertised URL, so the proto state machine
                // swallowed the redirect / migration command. We surface
                // a `warn!` for operator visibility and **do not**
                // propagate an error — the supervised reconnect arm
                // stays asleep, the original `AuthProvider::initial()`
                // credentials are NOT handed to the unverified host.
                tracing::warn!(
                    source,
                    rejected_url = broker_service_url
                        .as_deref()
                        .map(crate::log_fields::truncate_broker_str),
                    rejected_url_tls = broker_service_url_tls
                        .as_deref()
                        .map(crate::log_fields::truncate_broker_str),
                    "broker-advertised redirect URL rejected by redirect_url_allow_list; \
                     ignoring the hint (auth provider NOT replayed against the unverified host)",
                );
            }
            ConnectionEvent::TopicMigrated {
                producer,
                consumer,
                broker_service_url,
                broker_service_url_tls,
            } => {
                // PIP-188: broker asked us to move the producer / consumer to a different
                // broker. The new URL is a hint: the correct response is to tear the
                // connection down so the supervised reconnect path re-runs lookup. On
                // reconnect, `rebuild_producers` + `rebuild_consumers` re-emit every
                // still-open handle's command so user futures stay live across the
                // migration. We surface the hint via tracing, then return an error from
                // the driver — the supervised loop catches it, calls
                // `Connection::reset`, sleeps the backoff, and reopens.
                tracing::info!(
                    ?producer,
                    ?consumer,
                    new_url = broker_service_url
                        .as_deref()
                        .map(crate::log_fields::truncate_broker_str),
                    new_url_tls = broker_service_url_tls
                        .as_deref()
                        .map(crate::log_fields::truncate_broker_str),
                    "broker requested PIP-188 topic migration; supervised reconnect will fire"
                );
                return Err(EngineError::Config(
                    "PIP-188: broker requested topic migration; resetting connection".to_owned(),
                ));
            }
            ConnectionEvent::ProducerOpenFailedTransient {
                handle,
                code,
                message,
            } => {
                // Broker bounced the `CommandProducer` with a transient code
                // (`ServiceNotReady`, `MetadataError`, `TopicNotFound`) — the
                // typical post-`docker restart` window where the namespace
                // bundle hasn't been re-acquired yet. Pulsar's recommended
                // recovery is "Please redo the lookup": a fresh
                // `CommandLookupTopic` makes the broker (re)acquire bundle
                // ownership, after which the `CommandProducer` retry succeeds.
                // The producer state is RETAINED by the proto layer, so the
                // user's open / send futures stay pending across the retry.
                // Mirror of the tokio driver.
                //
                // `warn!` per ADR-0054 §2.1: degraded-but-recovering background
                // retry, not surfaced as `Err` to any caller while it retries.
                // The broker `message` is hostile-peer-controlled, so it is
                // truncated before landing in the field (ADR-0054 broker-string
                // sanitisation).
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "producer-open transient error; scheduling lookup + retry"
                );
                retries.push(RetryRequest::Producer(handle));
            }
            ConnectionEvent::SubscribeFailedTransient {
                handle,
                code,
                message,
            } => {
                // Consumer-side companion to the producer-open transient arm
                // above. `warn!` per ADR-0054 §2.1 (same level rule); the
                // broker `message` is truncated (ADR-0054). Mirror of the tokio
                // driver.
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "consumer-subscribe transient error; scheduling lookup + retry"
                );
                retries.push(RetryRequest::Consumer(handle));
            }
            // PIP-460 (ADR-0031): mirror the tokio driver's scalable-event
            // drain into the per-client buffer + wake `next_scalable_event`.
            #[cfg(feature = "scalable-topics")]
            ConnectionEvent::ScalableTopicLookupResolved {
                request_id,
                controller_broker_url,
                segments,
                lookup_token,
            } => {
                shared
                    .scalable_events
                    .lock()
                    .push_back(crate::ScalableEvent::LookupResolved {
                        request_id,
                        controller_broker_url,
                        segments,
                        lookup_token,
                    });
                shared.scalable_notify.notify_waiters();
            }
            #[cfg(feature = "scalable-topics")]
            ConnectionEvent::SegmentDagUpdated {
                watch_session_id,
                delta,
            } => {
                shared
                    .scalable_events
                    .lock()
                    .push_back(crate::ScalableEvent::DagUpdated {
                        watch_session_id,
                        delta,
                    });
                shared.scalable_notify.notify_waiters();
            }
            #[cfg(feature = "scalable-topics")]
            ConnectionEvent::DagChangedDuringConsume {
                watch_session_id,
                reason,
            } => {
                shared.scalable_events.lock().push_back(
                    crate::ScalableEvent::DagChangedDuringConsume {
                        watch_session_id,
                        reason,
                    },
                );
                shared.scalable_notify.notify_waiters();
            }
            #[cfg(feature = "scalable-topics")]
            ConnectionEvent::DagWatchClosed {
                watch_session_id,
                reason,
            } => {
                shared
                    .scalable_events
                    .lock()
                    .push_back(crate::ScalableEvent::DagWatchClosed {
                        watch_session_id,
                        reason,
                    });
                shared.scalable_notify.notify_waiters();
            }
            // Diagnostic events consumed SILENTLY — single-owner rule
            // (ADR-0054, decision Q1): `magnetar-proto` owns the
            // point-of-detection logs for CRC32C checksum mismatches and
            // lookup-redirect hops, where it holds the richest context
            // (computed/expected checksum, hop count, chased URL). The
            // engine drains the events here only so they cannot accumulate
            // unbounded in the proto event queue under a corrupting or
            // redirect-happy peer; logging them again here would
            // double-report. The `LookupResponse` arm only ever sees
            // `LookupOutcome::Redirected` — the `poll_event_if` predicate
            // above admits no other lookup result. Mirror of the tokio
            // driver.
            ConnectionEvent::ChecksumMismatch { .. } => {}
            ConnectionEvent::LookupResponse { .. } => {}
            _ => {}
        }
    }
}

/// Dispatch one [`RetryRequest`] as a detached task on the engine's
/// [`TaskProvider`]. The task sleeps `TRANSIENT_RETRY_DELAY` on the injected
/// [`TimeProvider`] (NEVER a host clock — the whole point of routing through
/// the provider is determinism under `SimProviders`, ADR-0011), re-runs
/// lookup so the broker re-acquires bundle ownership, then calls the proto
/// targeted-retry API (`retry_producer_open` / `retry_consumer_subscribe`).
///
/// Detached + spawned to mirror the tokio driver's `tokio::spawn` shape: the
/// retry leg must run concurrently with the driver loop (which keeps pumping
/// the socket), and its serialization must match the tokio engine so the
/// differential `EventStream` order stays identical (ADR-0024).
fn spawn_retry_leg<P>(
    shared: &Arc<ConnectionShared>,
    time: &P::Time,
    task: &P::Task,
    req: RetryRequest,
) where
    P: Providers,
{
    let shared = shared.clone();
    let time = time.clone();
    let _detached = task.spawn_task("magnetar-moonpool-transient-retry", async move {
        let _ = time.sleep(TRANSIENT_RETRY_DELAY).await;
        let topic = {
            let conn = shared.inner.lock();
            match req {
                RetryRequest::Producer(handle) => conn.producer_topic(handle).map(str::to_owned),
                RetryRequest::Consumer(handle) => conn.consumer_topic(handle).map(str::to_owned),
            }
        };
        // The handle was closed / removed between the broker error and this
        // retry — nothing to re-attach.
        let Some(topic) = topic else { return };
        if !lookup_then(&shared, &topic).await {
            return;
        }
        let request_id = {
            let mut conn = shared.inner.lock();
            match req {
                RetryRequest::Producer(handle) => conn.retry_producer_open(handle),
                RetryRequest::Consumer(handle) => conn.retry_consumer_subscribe(handle),
            }
        };
        if request_id.is_some() {
            shared.driver_waker.notify_one();
        }
    });
}

/// Issue a `CommandLookupTopic` and await the broker's
/// `CommandLookupTopicResponse` / `CommandError`. Returns `true` when the
/// lookup landed any outcome (the actual broker disposition is logged but
/// ignored — the caller's next step is a `retry_*` that re-fails if the
/// bundle is still not served). Used by the transient-error retry leg
/// ([`spawn_retry_leg`]) to force the broker to (re)acquire namespace bundle
/// ownership before re-attaching the producer / consumer. Mirror of the tokio
/// engine's `lookup_then`.
///
/// Self-contained `OpOutcome` await (rather than reaching for the
/// module-private `client::RequestFut`): the lookup request id is registered
/// against the proto waker slab, parked on the driver waker, and unregistered
/// on drop so a severed session leaves no dangling `Waker`.
async fn lookup_then(shared: &Arc<ConnectionShared>, topic: &str) -> bool {
    let request_id = {
        let mut conn = shared.inner.lock();
        conn.lookup(topic, false)
    };
    shared.driver_waker.notify_one();
    let outcome = LookupRetryFut {
        shared: shared.clone(),
        key: PendingOpKey::Request(request_id),
    }
    .await;
    if matches!(
        &outcome,
        OpOutcome::LookupResponse { .. } | OpOutcome::Error { .. }
    ) {
        tracing::debug!(?outcome, %topic, "retry-path lookup completed");
        true
    } else {
        tracing::warn!(?outcome, %topic, "retry-path lookup landed unexpected outcome");
        false
    }
}

/// Future resolving the [`OpOutcome`] correlated with a single request id.
/// Local to the driver's transient-retry leg; a thin twin of
/// [`crate::client`]'s module-private `RequestFut` (the canonical
/// request-id-correlated outcome future) so the driver does not have to widen
/// that type's visibility.
struct LookupRetryFut {
    shared: Arc<ConnectionShared>,
    key: PendingOpKey,
}

impl core::future::Future for LookupRetryFut {
    type Output = OpOutcome;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        if let Some(outcome) = conn.take_outcome(self.key) {
            return core::task::Poll::Ready(outcome);
        }
        conn.register_waker(self.key, cx.waker().clone());
        core::task::Poll::Pending
    }
}

impl Drop for LookupRetryFut {
    /// Clear our entry from the connection's waker slab so a lookup severed by
    /// a supervised reconnect (the `OpOutcome::SessionLost` published by
    /// [`magnetar_proto::Connection::reset`]) does not leave a dangling
    /// [`core::task::Waker`] behind. Mirrors the engine's `RequestFut::drop`.
    fn drop(&mut self) {
        self.shared.inner.lock().unregister_waker(self.key);
    }
}

/// Slot used to surface the driver's terminal result to a [`DriverHandle`]
/// joiner. The driver populates it under the mutex, then notifies
/// [`Self::done`].
struct DriverResult {
    result: Mutex<Option<Result<(), EngineError>>>,
    done: Notify,
}

/// Handle to the driver task. Dropping the handle does not stop the driver
/// (it keeps running as long as the [`ConnectionShared`] arc is alive); call
/// [`DriverHandle::abort`] to stop it or [`DriverHandle::join`] to wait for
/// it.
///
/// Joining is implemented over [`tokio::sync::Notify`] rather than the
/// task's join handle because moonpool main's
/// [`TaskProvider::JoinHandle`] is an opaque
/// `Future<Output = Result<(), moonpool_core::JoinError>>` with no
/// `abort()` (it dropped the raw `tokio::task::JoinHandle<()>` it used to
/// expose). We surface the terminal `Result<(), EngineError>` via a shared
/// slot instead, and stop the task *cooperatively* (see [`Self::abort`])
/// because the provider can no longer cancel it.
pub struct DriverHandle {
    /// Type-erased keep-alive for the spawned task handle. `spawn_task`
    /// detaches the task on drop (it lives until its future completes —
    /// i.e. until the connection closes), so this is a lifetime marker
    /// rather than a cancellation lever; moonpool main no longer exposes a
    /// task-level abort through the provider.
    _join: Box<dyn core::any::Any + Send>,
    result: Arc<DriverResult>,
    /// Connection used by [`Self::abort`] to drive a cooperative shutdown:
    /// moonpool main has no task-level cancel, so stopping the driver means
    /// `close()`-ing the connection and waking the loop so it runs its
    /// `should_close` exit path.
    shared: Arc<ConnectionShared>,
}

impl std::fmt::Debug for DriverHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverHandle").finish_non_exhaustive()
    }
}

impl DriverHandle {
    /// Wait for the driver to terminate. Returns whatever error caused it
    /// to exit, or `Ok(())` if it exited cleanly (e.g. because of a local
    /// close + flush).
    ///
    /// # Errors
    ///
    /// Propagates the driver's terminal error. If the driver panicked,
    /// the result slot will not be populated; this is surfaced as
    /// [`EngineError::Config`].
    pub async fn join(self) -> Result<(), EngineError> {
        loop {
            if let Some(res) = self.result.result.lock().take() {
                return res;
            }
            self.result.done.notified().await;
        }
    }

    /// Stop the driver task. moonpool main's [`TaskProvider`] exposes no
    /// task-level cancellation, so abort is *cooperative*: it `close()`-es the
    /// connection and wakes the driver loop, which observes `should_close`,
    /// runs its shutdown path, and populates the result slot with its real
    /// terminal outcome. A subsequent [`Self::join`] therefore waits for the
    /// task to actually finish rather than returning a synthetic result while
    /// the task is still parked. Idempotent — `close()` on an
    /// already-closing connection is a no-op.
    pub fn abort(&self) {
        {
            let mut conn = self.shared.inner.lock();
            conn.close();
        }
        self.shared.driver_waker.notify_one();
    }
}

/// Reconnect context passed to the supervised moonpool driver. Lets the
/// supervisor re-open the TCP connection (and, when wired, the TLS upgrade)
/// to the broker after a transient drop.
///
/// When `service_url_provider` is set, every reconnect attempt re-resolves
/// the broker address via
/// [`magnetar_proto::ServiceUrlProvider::get_service_url`] instead of
/// reusing the cached `host_port`. This is the runtime hook that makes
/// PIP-121 cluster failover (`ControlledClusterFailover` in the sans-io
/// crate, `AutoClusterFailover` in the tokio engine only) able to swap
/// broker URLs between reconnect attempts without re-building the client.
#[derive(Clone)]
pub(crate) struct ReconnectContext {
    /// Cached `host:port` literal — the moonpool engine accepts a raw
    /// authority (no `pulsar://` scheme), so we cache the exact string used
    /// on the initial dial as a fallback.
    pub(crate) host_port: String,
    /// Optional PIP-121 provider polled on every reconnect attempt. When
    /// `None`, the cached `host_port` is reused (matches the pre-PIP-121
    /// behaviour). The provider returns a `pulsar://` or `pulsar+ssl://`
    /// URL; the supervisor strips the scheme + path before dialling.
    pub(crate) service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    /// Optional pluggable DNS resolver invoked on every reconnect attempt
    /// before dialling the broker. When `None`, the runtime falls back to
    /// whatever [`moonpool_core::NetworkProvider::connect`] does with a
    /// `host:port` string. Mirrors Java's `ClientBuilder#dnsResolver`.
    pub(crate) dns_resolver: Option<Arc<dyn DnsResolver>>,
}

impl std::fmt::Debug for ReconnectContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectContext")
            .field("host_port", &self.host_port)
            .field(
                "has_service_url_provider",
                &self.service_url_provider.is_some(),
            )
            .field("has_dns_resolver", &self.dns_resolver.is_some())
            .finish()
    }
}

/// Spawn the driver loop using the moonpool [`TaskProvider`]. The provider
/// is consulted by the driver loop for `sleep`, which is what makes the
/// engine deterministic under `moonpool-sim`.
pub(crate) fn spawn<P>(
    shared: Arc<ConnectionShared>,
    transport: Transport<P>,
    time: P::Time,
    task: &P::Task,
) -> DriverHandle
where
    P: Providers,
{
    let result = Arc::new(DriverResult {
        result: Mutex::new(None),
        done: Notify::new(),
    });
    let result_for_task = result.clone();
    let shared_for_handle = shared.clone();
    let task_for_loop = task.clone();
    let join = task.spawn_task("magnetar-moonpool-driver", async move {
        let driver_shared = shared.clone();
        let outcome = driver_loop_inner::<P>(shared, transport, time, task_for_loop).await;
        // Plain (non-supervised) driver: this is a TERMINAL exit — there is no
        // reconnect to replay against. Fail every pending op so parked
        // subscribe / send / receive futures resolve with a terminal error
        // instead of hanging forever on a connection that is gone (the
        // no-progress stall). `driver_loop_inner` already ran
        // `mark_disconnected()` on its Err paths and `close()` snapped the
        // state on the graceful-close path, so `is_connected()` is already
        // false; `fail_all_pending` only installs the terminal outcomes +
        // `Closed` event and wakes the futures. ADR-0055.
        {
            let reason = match &outcome {
                Ok(()) => "connection closed".to_owned(),
                Err(err) => err.to_string(),
            };
            driver_shared.inner.lock().fail_all_pending(&reason);
        }
        // ADR-0059 / follow-ups §4.1: the plain driver is gone for good — latch
        // the no-driver signal so a NEW op issued after this point fast-fails
        // synchronously with `PeerClosed` at the entry-point guards instead of
        // registering a doomed pending op no driver is left to resolve. Set it
        // AFTER `fail_all_pending` so the slot `closed` flags + terminal
        // outcomes are already in place when a fresh op observes the latch.
        // 1:1 with the tokio engine.
        driver_shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on `driver_waker`, not the waker slab, so they observe the
        // freshly-queued `Closed` event and stop waiting.
        driver_shared.driver_waker.notify_waiters();
        *result_for_task.result.lock() = Some(outcome);
        // `notify_one` (not `notify_waiters`) so a `join()` that registers
        // *after* the task finishes still observes completion via the stored
        // permit instead of missing the wake and hanging.
        result_for_task.done.notify_one();
    });
    DriverHandle {
        _join: Box::new(join),
        result,
        shared: shared_for_handle,
    }
}

/// Spawn the driver with the auto-reconnect supervisor wired in.
///
/// When [`magnetar_proto::ConnectionConfig::supervisor`] is `Some`, the
/// driver re-handshakes against the broker after a transient drop using
/// `reconnect_ctx`. When the supervisor config is `None`, behaviour matches
/// [`spawn`] — driver exits on the first I/O failure.
pub(crate) fn spawn_supervised<P>(
    shared: Arc<ConnectionShared>,
    transport: Transport<P>,
    reconnect_ctx: ReconnectContext,
    providers: P,
) -> DriverHandle
where
    P: Providers,
{
    let result = Arc::new(DriverResult {
        result: Mutex::new(None),
        done: Notify::new(),
    });
    let result_for_task = result.clone();
    let shared_for_handle = shared.clone();
    let time = providers.time().clone();
    let task = providers.task().clone();
    let join = task.spawn_task("magnetar-moonpool-driver-supervised", async move {
        let driver_shared = shared.clone();
        let outcome =
            supervised_driver_loop::<P>(shared, transport, reconnect_ctx, providers, time).await;
        // `supervised_driver_loop` only returns on a GENUINELY-terminal exit
        // (user-requested close, or the supervisor exhausted its reconnect
        // attempt budget) — the per-attempt drop is handled inside the loop via
        // `reset()` + replay. Fail every still-pending op so parked subscribe /
        // send / receive / ack futures resolve with a terminal error instead of
        // hanging forever (the no-progress stall). Mirror of the plain `spawn`
        // above and the tokio runtime. ADR-0055 §1: `fail_all_pending` fires on
        // a supervisor that has exhausted its attempts, never on the
        // per-attempt reconnect.
        {
            let reason = match &outcome {
                Ok(()) => "connection closed".to_owned(),
                Err(err) => err.to_string(),
            };
            driver_shared.inner.lock().fail_all_pending(&reason);
        }
        // ADR-0059 / follow-ups §4.1: `supervised_driver_loop` only returns on
        // a GENUINELY-terminal exit (user close, or the supervisor exhausted
        // its attempt budget) — never on a per-attempt reconnect — so latching
        // the no-driver signal here is safe: a transient `Failed` window mid
        // reconnect never reaches this point. New ops fast-fail at the
        // entry-point guards. Set AFTER `fail_all_pending`. 1:1 with the tokio
        // engine.
        driver_shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on `driver_waker`, not the waker slab.
        driver_shared.driver_waker.notify_waiters();
        *result_for_task.result.lock() = Some(outcome);
        result_for_task.done.notify_one();
    });
    DriverHandle {
        _join: Box::new(join),
        result,
        shared: shared_for_handle,
    }
}

/// The supervised driver loop. Runs [`driver_loop_inner`] on the current
/// socket; on failure, if the supervisor is configured and the user has not
/// closed the connection, sleeps for a backoff interval (using the moonpool
/// [`TimeProvider`] so `moonpool-sim` stays deterministic), reconnects via
/// the moonpool [`moonpool_core::NetworkProvider`], calls
/// [`magnetar_proto::Connection::reset`], restarts the handshake, and
/// resumes.
async fn supervised_driver_loop<P>(
    shared: Arc<ConnectionShared>,
    mut transport: Transport<P>,
    reconnect_ctx: ReconnectContext,
    providers: P,
    time: P::Time,
) -> Result<(), EngineError>
where
    P: Providers,
{
    // Seed the backoff RNG from the shared-arc pointer so independent clients spread
    // their reconnect timing without depending on any I/O. `0` would land on the
    // splitmix default; the (stable, unique) Arc pointer mixes in per-client entropy.
    let seed: u64 = Arc::as_ptr(&shared) as usize as u64;

    // Backoff schedule lives outside the reconnect loop and PERSISTS across cycles for this
    // client. `reset()` snaps `next_delay` back to `initial` only when the previous socket
    // survived past `cfg.drop_grace` — i.e. when the previous reconnect was stable. This
    // stops the "broker accepts handshake then drops in <drop_grace, backoff snaps to
    // initial" storm that ADR-0028's anti-thrash detector escalates against as the second
    // line of defence. Lazy-init from the in-loop cfg snapshot so dynamic config edits to
    // `initial_backoff` / `max_backoff` / `mandatory_stop` (future work) still take effect
    // before the supervisor has had to redial once. Mirror of the tokio runtime.
    let mut backoff: Option<magnetar_proto::Backoff> = None;

    // Give-up budget counter (ADR-0061, follow-ups §3.2). Hoisted
    // OUTSIDE the outer loop so it spans the FULL dial+handshake cycle: a
    // post-dial handshake failure (the `driver_loop_inner` return path after
    // `begin_handshake`) counts against the SAME `max_attempts` budget as a
    // TCP-dial failure, instead of letting the outer loop reset it to 0. Behind
    // a docker-proxy / LB that accepts TCP while the backend is down, the dial
    // always succeeds but the handshake never completes, so the pre-ADR-0061
    // per-cycle counter never reached the budget and the driver retried forever
    // — the exact storm class the anti-thrash supervision was built for. Reset
    // to 0 ONLY when `should_reset_backoff` is true (a socket that survived
    // `drop_grace`), so give-up-reset and backoff-reset share ONE stability
    // definition. Mirror of the tokio runtime.
    let mut give_up_attempts: u32 = 0;

    // `socket_alive_since` lets us decide, once `driver_loop_inner` returns, whether the
    // previous socket lived long enough to count as a stable reconnect (-> `backoff.reset()`)
    // or died inside `drop_grace` (-> keep growing). Routed through the engine-supplied
    // monotonic clock (`shared.now_instant()`) so under `SimProviders` the elapsed-duration
    // gate (`should_reset_backoff`) flows from virtual time — keeping the schedule
    // bit-for-bit reproducible per ADR-0011 "Engines snapshot the host clock at the call
    // boundary; moonpool plugs in virtual clocks". Elapsed durations below use the same
    // provider via `now_instant().saturating_duration_since(...)` rather than the host
    // `Instant::elapsed()`.
    // The transient-retry leg dispatched from inside `driver_loop_inner` needs
    // the engine's `TaskProvider`; snapshot it once here (cloned per inner
    // call) so each reconnect cycle's loop can spawn retries on the same
    // provider.
    let task = providers.task().clone();

    let mut socket_alive_since = shared.now_instant();
    let mut last_inner_result =
        driver_loop_inner::<P>(shared.clone(), transport, time.clone(), task.clone()).await;

    loop {
        // User-requested close beats reconnect. `Failed` (transport drop, from
        // `mark_disconnected`) deliberately does NOT count here — the supervisor exists
        // precisely to retry after that, so the gate is `is_user_closed()` (Closing /
        // Closed only), mirroring the tokio runtime.
        if shared.inner.lock().is_user_closed() {
            return last_inner_result;
        }

        let supervisor_cfg = shared.inner.lock().supervisor_config().cloned();
        // Per-attempt dial budget for each reconnect (ADR-0052): the supervisor
        // loop already retries, so the chokepoint timeout on `Transport::connect`
        // is all the reconnect dial needs to avoid parking on a connect-hang.
        let connect_timeout = shared.inner.lock().connect_timeout();
        let Some(cfg) = supervisor_cfg else {
            return last_inner_result;
        };

        // ADR-0028: feed a TCP-drop signal into the anti-thrash detector if
        // the socket closed within the supervisor's `drop_grace` of the
        // most-recent successful re-attach. Mirror of the tokio runtime
        // (`crates/magnetar-runtime-tokio/src/driver.rs`). Reads time through
        // `shared.now_instant()` so under `SimProviders` the comparison flows
        // from virtual time, satisfying ADR-0011 ("Engines snapshot the host
        // clock at the call boundary; moonpool plugs in virtual clocks").
        // Determinism of the sleep schedule is preserved via `time.sleep`
        // below.
        if cfg.anti_thrash_threshold.is_some() {
            let now = shared.now_instant();
            let should_record = {
                let conn = shared.inner.lock();
                conn.anti_thrash_state()
                    .last_reattach_at()
                    .is_some_and(|t| now.saturating_duration_since(t) <= cfg.drop_grace)
            };
            if should_record {
                shared.inner.lock().record_reattach_outcome(
                    now,
                    magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
                    magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
                );
            }
        }

        // ADR-0028: if the anti-thrash detector has armed a cooldown, sleep
        // until it expires (using the moonpool TimeProvider so sim runs stay
        // deterministic for the sleep itself) before the next redial.
        let cooldown_until = {
            let conn = shared.inner.lock();
            match conn.anti_thrash_tick(shared.now_instant()) {
                magnetar_proto::AntiThrashDisposition::Cooldown { until } => Some(until),
                magnetar_proto::AntiThrashDisposition::Normal => None,
            }
        };
        if let Some(until) = cooldown_until {
            let now = shared.now_instant();
            if until > now {
                let dur = until.saturating_duration_since(now);
                tracing::warn!(
                    cooldown_ms = u64::try_from(dur.as_millis()).unwrap_or(u64::MAX),
                    "supervisor: anti-thrash cooldown engaged; sleeping before next redial"
                );
                let _ = time.sleep(dur).await;
            }
            shared.inner.lock().anti_thrash_state_mut().clear_cooldown();
        }

        // Backoff persistence policy (ADR-0028 alignment): lazy-init on the first redial,
        // then reuse across cycles. `reset()` is gated on the previous socket surviving past
        // `cfg.drop_grace` — sockets that died inside that window count as thrashes, so the
        // schedule keeps growing and successive ProducerReady-then-drop cycles slow down
        // geometrically up to `max_backoff`. Mirror of the tokio runtime.
        //
        // ADR-0061: the give-up budget counter (`give_up_attempts`, hoisted
        // above) shares this SAME stability gate — a socket that survived
        // `drop_grace` resets BOTH the backoff schedule and the give-up budget,
        // so the two share one definition of "the last reconnect counted as
        // stable". A socket that died inside `drop_grace` (or never handshaked
        // at all, behind a TCP-accepting proxy) resets neither.
        let backoff = backoff.get_or_insert_with(|| cfg.build_backoff(seed));
        // ADR-0011: route the elapsed-duration computation through the
        // engine-supplied clock instead of `Instant::elapsed()` (which
        // implicitly reads the host `Instant::now`). Under `SimProviders`
        // this keeps the reset gate honoring virtual time.
        let socket_lifetime = shared
            .now_instant()
            .saturating_duration_since(socket_alive_since);
        if cfg.should_reset_backoff(socket_lifetime) {
            backoff.reset();
            give_up_attempts = 0;
        }

        // Reconnect loop — keep trying until we land a fresh socket + handshake OR
        // exhaust `max_attempts`. The give-up counter spans the full
        // dial+handshake cycle (ADR-0061): each pass through this loop is one
        // dial attempt; a pass that dials successfully but whose post-handshake
        // `driver_loop_inner` later returns (handshake / session failure)
        // re-enters the outer loop without resetting the counter, so the next
        // dial increments from where this one left off.
        let new_transport = loop {
            let delay = backoff.next();
            // Use the moonpool TimeProvider so sim runs stay deterministic.
            let _ = time.sleep(delay).await;

            give_up_attempts = give_up_attempts.saturating_add(1);
            if cfg.should_give_up(give_up_attempts) {
                tracing::warn!(
                    attempt = give_up_attempts,
                    max_attempts = cfg.max_attempts.unwrap_or(0),
                    "supervisor: gave up; reconnect attempt budget exhausted"
                );
                return last_inner_result;
            }
            let attempt = give_up_attempts;

            // Did the user request close while we were sleeping? Same `is_user_closed`
            // gate as the outer loop.
            if shared.inner.lock().is_user_closed() {
                return last_inner_result;
            }

            // PIP-121 cluster failover — re-resolve the broker URL via the provider
            // on every attempt before dialling. The provider is sync + cheap by
            // contract (see `magnetar_proto::ServiceUrlProvider` doc). If no
            // provider is configured, fall back to the cached host:port captured at
            // start time.
            let target_host_port: String =
                if let Some(provider) = reconnect_ctx.service_url_provider.as_ref() {
                    strip_url_to_host_port(&provider.get_service_url()).unwrap_or_else(|| {
                        tracing::warn!(
                            attempt,
                            "supervisor: service-url provider returned an unparseable URL; \
                             falling back to the cached host:port"
                        );
                        reconnect_ctx.host_port.clone()
                    })
                } else {
                    reconnect_ctx.host_port.clone()
                };

            let resolver = reconnect_ctx.dns_resolver.as_deref();
            match Transport::<P>::connect_with_resolver(
                providers.network(),
                &target_host_port,
                resolver,
                providers.time(),
                connect_timeout,
            )
            .await
            {
                Ok(t) => {
                    // ADR-0061: this is a TCP-connect, NOT a confirmed reconnect
                    // — behind a TCP-accepting proxy the dial succeeds while the
                    // backend (and hence the Pulsar handshake) is down. The
                    // TRUE reconnect-success info log fires AFTER the handshake
                    // completes (the post-`begin_handshake` rebuild path);
                    // mislabelling a TCP accept as a reconnect would tell
                    // operators the broker is back when it is not. Mirror of the
                    // tokio runtime.
                    tracing::info!(
                        attempt,
                        target = %target_host_port,
                        "supervisor: TCP connected; handshaking"
                    );
                    break t;
                }
                Err(err) => {
                    tracing::warn!(
                        attempt,
                        target = %target_host_port,
                        error = %err,
                        "supervisor: reconnect attempt failed; will retry"
                    );
                    // Loop and back off again.
                }
            }
        };

        // Got a new transport. Reset the state machine + kick off CONNECT. Stage 3:
        // arm the rebuild flag so the inner loop replays every still-open producer
        // / consumer once the new socket's handshake completes.
        {
            let mut conn = shared.inner.lock();
            conn.reset();
            if let Err(err) = conn.begin_handshake() {
                tracing::error!(error = %err, "supervisor: begin_handshake after reset failed");
                return Err(EngineError::Protocol(err));
            }
        }
        shared
            .pending_rebuild
            .store(true, std::sync::atomic::Ordering::SeqCst);
        shared.driver_waker.notify_one();

        transport = new_transport;
        // ADR-0011: virtual-clock-anchored timestamp; pairs with the
        // `should_reset_backoff` gate above.
        socket_alive_since = shared.now_instant();
        last_inner_result =
            driver_loop_inner::<P>(shared.clone(), transport, time.clone(), task.clone()).await;
    }
}

/// Parse a `pulsar://host:port` / `pulsar+ssl://host:port` URL into its
/// `host:port` authority. Returns `None` for unrecognised schemes or
/// malformed inputs. Kept inline (no `url` dep) since the moonpool engine
/// otherwise doesn't pull in `url`; matches the level of robustness Java's
/// `ServiceUrlProvider` requires (callers are trusted).
fn strip_url_to_host_port(raw: &str) -> Option<String> {
    let rest = raw
        .strip_prefix("pulsar://")
        .or_else(|| raw.strip_prefix("pulsar+ssl://"))?;
    // Trim path / query / fragment if any.
    let rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    if rest.is_empty() {
        return None;
    }
    // Default ports when none provided (matches `Scheme::default_port` in the tokio
    // engine — plain → 6650, tls → 6651). We can't tell the schemes apart cheaply
    // here without re-parsing, so default to 6650 (plaintext); tests / production
    // configs typically include the port.
    if rest.contains(':') {
        Some(rest.to_owned())
    } else {
        let default_port = if raw.starts_with("pulsar+ssl://") {
            6651
        } else {
            6650
        };
        Some(format!("{rest}:{default_port}"))
    }
}

/// The driver loop.
///
/// Implementation notes:
///
/// - **Lock discipline**: every interaction with `magnetar_proto::Connection` happens inside a
///   `parking_lot::Mutex::lock()` critical section that never `.await`s.
/// - **Write path**: drain outbound bytes from the state machine into an owned buffer, release the
///   lock, then `write_all` to the socket.
/// - **Read path**: read directly into a `BytesMut` and hand the slice to the state machine.
///   Partial frames stay in the state machine's internal `inbound` buffer.
/// - **Timeout**: `Connection::poll_timeout` returns the next deadline, if any. We `tokio::select!`
///   against `time.sleep(remaining)`. If no deadline is set, that arm is replaced by a `pending`
///   future.
/// - **Rebuild on reconnect**: after each successful `handle_bytes`, if `shared.pending_rebuild` is
///   set and the state machine has transitioned to `Connected`, replay every still-open producer +
///   consumer via `rebuild_producers` / `rebuild_consumers`. The CAS ensures the replay fires
///   exactly once per reconnect.
pub(crate) async fn driver_loop_inner<P>(
    shared: Arc<ConnectionShared>,
    mut transport: Transport<P>,
    time: P::Time,
    task: P::Task,
) -> Result<(), EngineError>
where
    P: Providers,
{
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);

    loop {
        // 0. Advance the engine's wall-clock atomic from `providers.time().now()`. The proto-layer
        //    wall-clock closure installed by `ConnectionShared::with_auth` reads this atomic, so
        //    `Connection::handle_timeout` batch-publish stamping flows from the moonpool
        //    `TimeProvider` (host clock under `TokioProviders`, virtual time under `SimProviders`).
        {
            let elapsed_ms = time.now().as_millis();
            let now_ms = shared
                .wall_clock_base_ms
                .saturating_add(u64::try_from(elapsed_ms).unwrap_or(u64::MAX));
            shared
                .wall_clock_ms
                .store(now_ms, std::sync::atomic::Ordering::Relaxed);
        }

        // 1. Drain outbound bytes + check if the state machine wants us to terminate. ADR-0040 wave
        //    2: take the owned `TransmitOwned` — the contiguous arm uses the same O(1)
        //    `BytesMut::split()` ownership transfer the legacy `poll_transmit` returned; the
        //    vectored arm carries the producer batch's `[head, payload]` segment list.
        //
        //    moonpool main now exposes vectored writes, so the engine
        //    dispatches the `Vectored` arm via real futures
        //    `write_vectored` on the Plain path (§2 below). moonpool-sim's
        //    `SimTcpStream` records each `IoSlice` as its own ordered
        //    delivery event → segment-granular chaos (drops / reorders at
        //    frame-head vs payload boundaries). `TokioProviders`' `Compat`
        //    stream lacks vectored forwarding so it falls back to a
        //    single-buffer `poll_write` (still correct, just no syscall
        //    reduction). TLS coalesces (rustls owns its own record
        //    buffering). Either way the *bytes* on the wire stay
        //    byte-identical to before and to the tokio engine.
        //
        //    ADR-0038: drain `poll_transmit_owned()` UNDER the connection
        //    lock, then carry the owned `TransmitOwned` out (cheap — each
        //    segment is `Arc`-backed `Bytes`) and drop the lock BEFORE
        //    awaiting the network write. The `parking_lot::Mutex` is never
        //    held across an `.await`.
        let (out, deadline, should_close) = {
            let mut conn = shared.inner.lock();
            let out = conn.poll_transmit_owned();
            let dl = conn.poll_timeout();
            let closing = matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Closing
                    | magnetar_proto::HandshakeState::Closed
                    | magnetar_proto::HandshakeState::Failed
            );
            (out, dl, closing)
        };

        // 2. Flush whatever the state machine produced. This happens outside the lock so user
        //    futures can keep enqueuing.
        if !out.is_empty() {
            let write_result = match &out {
                magnetar_proto::TransmitOwned::Contiguous(buf) => transport.write_all(buf).await,
                magnetar_proto::TransmitOwned::Vectored(segs) => {
                    transport.write_all_vectored(segs).await
                }
            };
            if let Err(err) = write_result {
                shared.inner.lock().mark_disconnected();
                return Err(err.into());
            }
            if let Err(err) = transport.flush().await {
                shared.inner.lock().mark_disconnected();
                return Err(err.into());
            }
        }

        if should_close {
            let _ = transport.shutdown().await;
            return Ok(());
        }

        // 3. Park until something interesting happens. The duration is relative because moonpool's
        //    `TimeProvider::sleep` takes a `Duration`, not an `Instant`. The "now" baseline is
        //    pulled through the engine-supplied clock (`shared.now_instant()`) so sim runs compute
        //    the sleep window against virtual time — pairing with the `Instant` the state machine
        //    itself was handed via `handle_bytes` / `handle_timeout` below (ADR-0011 sans-io clock
        //    injection).
        let sleep_dur = deadline.map(|t| t.saturating_duration_since(shared.now_instant()));

        tokio::select! {
            biased;

            // Driver wake-up from user-facing futures (e.g. a freshly-enqueued send).
            () = shared.driver_waker.notified() => {
                // Loop: poll_transmit will drain whatever the future enqueued.
            }

            // Inbound bytes.
            r = transport.read_buf(&mut read_buf) => {
                let n = match r {
                    Ok(n) => n,
                    Err(err) => {
                        shared.inner.lock().mark_disconnected();
                        return Err(err.into());
                    }
                };
                if n == 0 {
                    // Peer closed cleanly. State-consistency postcondition (asserted on the
                    // *same* guard — no re-lock, so no race with concurrent user futures;
                    // ADR-0038): once `mark_disconnected()` runs the connection must report
                    // `!is_connected()` (state snaps to `Failed`). Mirror of the tokio engine.
                    {
                        let mut conn = shared.inner.lock();
                        conn.mark_disconnected();
                        debug_assert!(
                            !conn.is_connected(),
                            "mark_disconnected() must clear is_connected() (ADR-0038)"
                        );
                    }
                    return Err(EngineError::PeerClosed);
                }
                // ADR-0040 wave 3 (read-path ownership pass-through):
                // hand the freshly-read `BytesMut` chunk to the state
                // machine via `handle_bytes_owned`. When proto's
                // internal `inbound` buffer is empty (the common case
                // after a full-frame decode), the chunk is *swapped*
                // into place with zero memcpy. Mid-frame fall-back
                // re-uses the legacy `extend_from_slice` path.
                let chunk = read_buf.split();
                // Read-buffer postcondition (mirror of the tokio engine): `read_buf` is
                // drained via `split()` on every inbound-arm iteration and never appended to
                // elsewhere, so it is empty when `read_buf()` runs — the freshly split chunk
                // therefore carries exactly the `n` bytes just read. A mismatch would mean
                // stale bytes leaked across loop iterations into the byte stream fed to
                // `handle_bytes_owned`.
                debug_assert_eq!(
                    chunk.len(),
                    n,
                    "read chunk length must equal the byte count just read"
                );
                // ADR-0011: feed the sans-io state machine an Instant pulled
                // through the engine-supplied clock so `SimProviders` runs
                // are bit-for-bit reproducible. The default provider reads
                // `Instant::now()`, so production TokioProviders behaviour
                // is unchanged; SimProviders threads `time.now()` through
                // the closure installed by `MoonpoolEngine::make_shared`.
                let now = shared.now_instant();
                // ADR-0038: the `shared.inner` guard returned by `lock()` is a
                // *temporary* in the `if let` scrutinee, which lives until the
                // end of the consequent block. Re-locking `shared.inner` inside
                // the error branch would re-enter the non-reentrant
                // `parking_lot::Mutex` and self-deadlock the driver task. Bind
                // the result to a `let` first: the guard drops at the `;`,
                // before the branch body takes the lock again. (Surfaced by
                // sim_chaos swizzle-clog seeds 0x56201ccaba82dbc1 /
                // 0xdc638c565234d23f, which drive `handle_bytes_owned` to `Err`
                // mid-reorder.)
                let handle_result = shared.inner.lock().handle_bytes_owned(now, chunk);
                if let Err(err) = handle_result {
                    shared.inner.lock().mark_disconnected();
                    return Err(err.into());
                }
                // Supervisor Stage 3: once the new session's handshake completes, replay every
                // still-open producer + consumer so user-facing handles survive the reconnect
                // transparently. The compare-exchange ensures the rebuild fires exactly once
                // per reconnect even if `handle_bytes` is called multiple times in quick
                // succession.
                if shared
                    .pending_rebuild
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let connected = shared.inner.lock().is_connected();
                    if connected
                        && shared
                            .pending_rebuild
                            .compare_exchange(
                                true,
                                false,
                                std::sync::atomic::Ordering::SeqCst,
                                std::sync::atomic::Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        // ADR-0061: the handshake on the new socket has now
                        // completed (`is_connected()` is true and the
                        // once-per-reconnect compare-exchange won) — this, NOT
                        // the earlier TCP-connect log, is the TRUE
                        // reconnect-success signal operators rely on. It fires
                        // even when there are no handles to replay
                        // (`producers = 0, consumers = 0`), so a TCP accept
                        // behind a down backend (handshake never completes) never
                        // reaches here and is never mislabelled as a reconnect.
                        // Mirror of the tokio runtime.
                        let (n_p, n_c) = {
                            let mut conn = shared.inner.lock();
                            let producers = conn.rebuild_producers();
                            let consumers = conn.rebuild_consumers();
                            (producers.len(), consumers.len())
                        };
                        tracing::info!(
                            producers = n_p,
                            consumers = n_c,
                            "supervisor: reconnected to broker; handshake complete, replayed \
                             producer + consumer state"
                        );
                        // Wake the next loop iteration so `poll_transmit` flushes the
                        // re-emitted `CommandProducer` / `CommandSubscribe` / `CommandFlow`
                        // frames onto the new socket.
                        shared.driver_waker.notify_one();
                    }
                }
                let mut retries: Vec<RetryRequest> = Vec::new();
                handle_pending_events(&shared, &mut retries)?;
                // Dispatch any transient producer-open / subscribe retries as
                // detached tasks on the engine providers — the delayed lookup +
                // re-attach sleeps on the INJECTED clock (`time`), so the leg
                // stays deterministic under `SimProviders` and matches the
                // tokio engine's detached `tokio::spawn` serialization
                // (ADR-0011 / ADR-0024).
                for req in retries {
                    spawn_retry_leg::<P>(&shared, &time, &task, req);
                }
                // Wake event-stream-watching futures (e.g. `ProducerReadyFut`)
                // that parked on `driver_waker.notified()` so they re-poll and
                // observe the freshly-pushed event.
                shared.driver_waker.notify_waiters();
            }

            // Timer fired. `sleep_or_pending` only returns once the duration
            // elapses or the time provider shuts down; both are treated as
            // a tick.
            () = sleep_or_pending::<P>(&time, sleep_dur) => {
                // ADR-0011: route the tick-now through the engine clock so
                // virtual-time sim runs see deterministic timeout firings.
                shared.inner.lock().handle_timeout(shared.now_instant());
            }
        }
    }
}

/// Helper: if `dur` is `Some`, sleep that long; otherwise park forever.
/// Lives outside the `select!` to keep the macro readable.
async fn sleep_or_pending<P: Providers>(time: &P::Time, dur: Option<Duration>) {
    match dur {
        Some(d) => {
            // Ignore the `TimeProvider::Shutdown` variant: in production it
            // never fires; in sim, a shutdown means the test is winding down
            // and the driver should just notice via the next loop iteration.
            let _ = time.sleep(d).await;
        }
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use bytes::BytesMut;
    use magnetar_proto::{ConnectionConfig, ConnectionEvent, ProducerHandle, encode_command, pb};

    use super::{handle_pending_events, strip_url_to_host_port};
    use crate::{ConnectionShared, EngineError};

    /// Build a synthetic `CommandConnected` frame for use in tests that need
    /// the state machine past handshake without running an engine.
    fn handshake_response_bytes() -> BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    /// PIP-188: feeding a `CommandTopicMigrated` to the state machine, then
    /// invoking `handle_pending_events`, returns an `EngineError::Config`.
    /// The supervised driver loop catches this as a recoverable failure and
    /// reopens the connection, mirroring the tokio engine.
    #[test]
    fn topic_migrated_triggers_recoverable_error() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
            // Drain the Connected event so the next poll_event yields the migration.
            match conn.poll_event() {
                Some(ConnectionEvent::Connected { .. }) => {}
                other => panic!("expected Connected, got {other:?}"),
            }
            // Feed CommandTopicMigrated.
            let migrated = pb::BaseCommand {
                r#type: pb::base_command::Type::TopicMigrated as i32,
                topic_migrated: Some(pb::CommandTopicMigrated {
                    resource_id: 42,
                    resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
                    broker_service_url: Some("pulsar://new-broker:6650".to_owned()),
                    broker_service_url_tls: None,
                }),
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            encode_command(&mut buf, &migrated).expect("encode CommandTopicMigrated");
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle migration");
        }
        // The driver's event handler must surface a recoverable Config error so
        // the supervised loop catches it, calls reset+begin_handshake, and
        // reopens. The resource handle should map onto the producer slot.
        let mut retries = Vec::new();
        let err = handle_pending_events(&shared, &mut retries).expect_err("migration must error");
        assert!(
            retries.is_empty(),
            "a topic-migration event must not enqueue a transient retry"
        );
        let msg = format!("{err}");
        assert!(
            matches!(err, EngineError::Config(_)) && msg.contains("PIP-188"),
            "expected PIP-188 config error, got {err:?}"
        );

        // Sanity: confirm ProducerHandle is reachable so any future refactor
        // that hides the constructor surfaces here too. The actual handle
        // routing inside the proto layer is already covered by the
        // magnetar-proto unit tests.
        assert_eq!(ProducerHandle(42), ProducerHandle(42));
    }

    #[test]
    fn strip_url_to_host_port_handles_plain() {
        assert_eq!(
            strip_url_to_host_port("pulsar://broker:6650").as_deref(),
            Some("broker:6650")
        );
    }

    #[test]
    fn strip_url_to_host_port_handles_tls() {
        assert_eq!(
            strip_url_to_host_port("pulsar+ssl://broker.example.com:6651").as_deref(),
            Some("broker.example.com:6651")
        );
    }

    #[test]
    fn strip_url_to_host_port_defaults_plain_port() {
        assert_eq!(
            strip_url_to_host_port("pulsar://broker").as_deref(),
            Some("broker:6650")
        );
    }

    #[test]
    fn strip_url_to_host_port_defaults_tls_port() {
        assert_eq!(
            strip_url_to_host_port("pulsar+ssl://broker").as_deref(),
            Some("broker:6651")
        );
    }

    #[test]
    fn strip_url_to_host_port_strips_path() {
        assert_eq!(
            strip_url_to_host_port("pulsar://broker:6650/admin").as_deref(),
            Some("broker:6650")
        );
    }

    #[test]
    fn strip_url_to_host_port_rejects_unknown_scheme() {
        assert!(strip_url_to_host_port("http://broker:6650").is_none());
    }
}
