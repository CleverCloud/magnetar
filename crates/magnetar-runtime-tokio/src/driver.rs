// SPDX-License-Identifier: Apache-2.0

//! The per-connection I/O driver task.
//!
//! One driver per connection. Owns the I/O resources (TCP / TLS stream), the per-connection
//! read buffer, and the loop that:
//!
//! 1. drains outbound bytes from the sans-io state machine into a write buffer,
//! 2. flushes the write buffer to the socket,
//! 3. reads inbound bytes from the socket into the state machine,
//! 4. ticks timers when the state machine's deadline elapses,
//! 5. parks itself on `shared.driver_waker.notified()` between events.
//!
//! The driver does **not** dispatch wakers — that is the sans-io layer's job. As the state
//! machine processes an inbound frame, it inserts a [`magnetar_proto::OpOutcome`] into a slab and
//! wakes the [`core::task::Waker`] that user futures previously registered via
//! [`magnetar_proto::Connection::register_waker`]. See [GUIDELINES.md] §"No-channels rule".
//!
//! # Supervisor (auto-reconnect)
//!
//! When [`magnetar_proto::ConnectionConfig::supervisor`] is `Some`, the spawn helper wraps the
//! per-socket driver loop in a backoff-driven reconnect cycle. The cycle:
//!
//! 1. runs [`driver_loop_inner`] until the socket errors or the peer closes;
//! 2. checks whether the user requested a graceful close (state machine `is_closed`) — if so, exits
//!    cleanly;
//! 3. otherwise reads [`magnetar_proto::SupervisorConfig`] off the state machine, builds a
//!    [`magnetar_proto::Backoff`], and sleeps for the next backoff interval;
//! 4. reconnects via [`crate::transport::Transport::connect_with_resolver`] (routing through the
//!    optional `dns_resolver` carried on [`ReconnectContext`]), calls
//!    [`magnetar_proto::Connection::reset`] (which fails request-bound ops with
//!    [`magnetar_proto::OpOutcome::SessionLost`] and snapshots in-flight publishes for transparent
//!    replay), restarts the handshake, and resumes step 1.
//!
//! Stage 3 (producer / consumer state replay) wires in here too: after the new socket completes
//! its handshake, the inner loop calls [`magnetar_proto::Connection::rebuild_producers`] and
//! [`magnetar_proto::Connection::rebuild_consumers`], which re-emit every still-open producer's
//! `CommandProducer` (with a bumped `epoch`) and every still-open consumer's `CommandSubscribe`
//! plus `CommandFlow` (resuming from `last_acked_message_id` when known). The producer rebuild
//! also re-issues every snapshotted in-flight publish onto the new session — user-facing send
//! futures stay pending until the replayed `CommandSendReceipt` arrives, never observing the
//! reset. This delivers at-least-once publish parity with the Java client (mirrors
//! `ProducerImpl#resendMessages`).
//!
//! [GUIDELINES.md]: https://github.com/CleverCloud/magnetar/blob/main/GUIDELINES.md

use std::collections::VecDeque;
#[cfg(test)]
use std::io::IoSlice;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionEvent, ConsumerHandle, DriverRetry, OpOutcome, ProducerHandle, RequestId,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::ConnectionShared;
use crate::dns::DnsResolver;
use crate::error::ClientError;
use crate::transport::Transport;
use crate::url_parse::ParsedUrl;

#[derive(Debug, Clone, Copy)]
enum RetryRequest {
    Producer(ProducerHandle, RequestId),
    Consumer(ConsumerHandle, RequestId),
}

/// Drain the connection's semantic event queue of events the *driver* must
/// react to, leaving every other event (e.g. `ProducerReady`,
/// `SubscribeAcked`, `Connected`) in the queue for user-facing futures to
/// observe.
///
/// We use [`magnetar_proto::Connection::poll_event_if`] with an explicit
/// allow-list rather than draining the whole queue: an unconditional
/// `poll_event` loop would silently consume the `ProducerReady` /
/// `SubscribeAcked` events that `EventWaitFut::poll` is parked on and
/// stall every open-producer / subscribe round-trip (regressed in the
/// M8 differential `broker_smoke` test on 2026-05-22; see ADR-0021).
fn handle_pending_events(shared: &Arc<ConnectionShared>) -> Result<(), ClientError> {
    while let Some(retry) = shared.inner.lock().poll_driver_retry() {
        match retry {
            DriverRetry::Producer {
                handle,
                failed_request_id,
                code,
                message,
            } => {
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "producer-open transient error; scheduling lookup + retry"
                );
                spawn_retry_leg(shared, RetryRequest::Producer(handle, failed_request_id));
            }
            DriverRetry::Consumer {
                handle,
                failed_request_id,
                code,
                message,
            } => {
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "consumer-subscribe transient error; scheduling lookup + retry"
                );
                spawn_retry_leg(shared, RetryRequest::Consumer(handle, failed_request_id));
            }
            _ => {}
        }
    }
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
                    // landing in the field (ADR-0054).
                    tracing::warn!(
                        auth_method = method
                            .as_deref()
                            .map_or("none", crate::log_fields::truncate_broker_str),
                        "broker requested in-band auth refresh but no AuthProvider configured; \
                         the connection will be reset"
                    );
                    return Err(ClientError::Other(
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
                        // caller via the returned `ClientError`.
                        tracing::warn!(
                            auth_method = %provider.method(),
                            error_class = "auth_refresh_failed",
                            "in-band auth refresh failed; the connection will be reset"
                        );
                        return Err(ClientError::Other(format!("auth refresh failed: {err}")));
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
                // PIP-145 topic-list watcher delta. Push into the per-client buffer + wake
                // any `Client::next_topic_list_change` future.
                shared
                    .topic_list_changes
                    .lock()
                    .push_back(crate::TopicListChange { added, removed });
                shared.topic_list_notify.notify_waiters();
            }
            ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle, marker } => {
                // PIP-33 (ADR-0034): drain off the proto-level event queue into the
                // per-client buffer so it can't accumulate on idle subscribers.
                shared
                    .replicated_subscription_markers
                    .lock()
                    .push_back(crate::ObservedReplicatedSubscriptionMarker { handle, marker });
                shared
                    .replicated_subscription_marker_notify
                    .notify_waiters();
            }
            ConnectionEvent::RedirectUrlRejected {
                source,
                broker_service_url,
                broker_service_url_tls,
            } => {
                // Defence-in-depth: the configured `redirect_url_allow_list`
                // refused this broker-advertised URL, so the proto state
                // machine swallowed the redirect / migration command. We
                // surface a `warn!` for the operator audit trail and
                // **do not** propagate an error — the supervised reconnect
                // arm stays asleep, the original `AuthProvider::initial()`
                // credentials are NOT handed to the unverified host, and
                // the existing connection keeps serving (the broker that
                // sent the redirect may close the channel separately;
                // that's a normal transport drop, not a credential leak).
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
                // PIP-188: broker asked the client to move the producer / consumer to a
                // different broker. The new URL is a hint: the correct way to honour it
                // is to tear the connection down so the supervised reconnect path re-runs
                // lookup (and yields the new owner). On reconnect,
                // `Connection::rebuild_producers` + `rebuild_consumers` re-emit every
                // still-open handle's `CommandProducer` / `CommandSubscribe` so user
                // futures stay live across the migration.
                //
                // We surface the hint via tracing so operators can see why the reconnect
                // fired, then return an error from the driver — the supervised loop
                // catches it, calls `Connection::reset`, sleeps the backoff, and reopens.
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
                return Err(ClientError::Other(
                    "PIP-188: broker requested topic migration; resetting connection".to_owned(),
                ));
            }
            // PIP-460 (ADR-0031): drain scalable-topic events off the proto
            // queue into the per-client buffer + wake `next_scalable_event`.
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
            // above admits no other lookup result.
            ConnectionEvent::ChecksumMismatch { .. } => {}
            ConnectionEvent::LookupResponse { .. } => {}
            _ => {}
        }
    }
}

fn retry_request_topic(conn: &magnetar_proto::Connection, request: RetryRequest) -> Option<String> {
    if !conn.is_connected() {
        return None;
    }
    match request {
        RetryRequest::Producer(handle, failed_request_id)
            if conn.producer_open_retry_is_current(handle, failed_request_id) =>
        {
            conn.producer_topic(handle).map(str::to_owned)
        }
        RetryRequest::Consumer(handle, failed_request_id)
            if conn.consumer_subscribe_retry_is_current(handle, failed_request_id) =>
        {
            conn.consumer_topic(handle).map(str::to_owned)
        }
        RetryRequest::Producer(_, _) | RetryRequest::Consumer(_, _) => None,
    }
}

pub(crate) fn notify_retry_generation_replaced(shared: &Arc<ConnectionShared>) {
    shared.operation_cancel_notify.notify_waiters();
    shared.driver_waker.notify_one();
}

async fn wait_retry_delay(
    shared: &Arc<ConnectionShared>,
    request: RetryRequest,
    delay: std::time::Duration,
) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        let cancelled = shared.operation_cancel_notify.notified();
        tokio::pin!(cancelled);
        cancelled.as_mut().enable();
        if retry_request_topic(&shared.inner.lock(), request).is_none() {
            return false;
        }
        tokio::select! {
            biased;
            () = cancelled.as_mut() => {}
            () = sleep.as_mut() => {
                return retry_request_topic(&shared.inner.lock(), request).is_some();
            }
        }
    }
}

fn spawn_retry_leg(shared: &Arc<ConnectionShared>, request: RetryRequest) {
    let shared = shared.clone();
    tokio::spawn(async move {
        let delay = {
            let conn = shared.inner.lock();
            let failures = match request {
                RetryRequest::Producer(handle, _) => conn.producer_transient_open_attempts(handle),
                RetryRequest::Consumer(handle, _) => {
                    conn.consumer_transient_subscribe_attempts(handle)
                }
            };
            conn.operation_retry_config().delay_after_failure(failures)
        };
        if !wait_retry_delay(&shared, request, delay).await {
            return;
        }
        let topic = retry_request_topic(&shared.inner.lock(), request);
        let Some(topic) = topic else { return };
        if !lookup_then(&shared, &topic, request).await {
            return;
        }
        let request_id = {
            let mut conn = shared.inner.lock();
            match request {
                RetryRequest::Producer(handle, failed_request_id) => {
                    conn.retry_producer_open_if_current(handle, failed_request_id)
                }
                RetryRequest::Consumer(handle, failed_request_id) => {
                    conn.retry_consumer_subscribe_if_current(handle, failed_request_id)
                }
            }
        };
        if request_id.is_some() {
            shared.driver_waker.notify_one();
        }
    });
}

/// Issue a `CommandLookupTopic` and await the broker's `CommandLookupTopicResponse` /
/// `CommandError`. Returns `true` only after a usable `Connect` outcome.
/// Retryable lookup failures are re-issued under the configured operation
/// policy; a terminal failure terminalizes the opening handle with the exact
/// broker code/message and returns `false`.
async fn lookup_then(shared: &Arc<ConnectionShared>, topic: &str, request: RetryRequest) -> bool {
    use crate::client::RequestFut;

    let retry_config = shared.inner.lock().operation_retry_config().clone();
    let mut failures = 0_u32;
    loop {
        let request_id = {
            let mut conn = shared.inner.lock();
            if retry_request_topic(&conn, request).is_none() {
                return false;
            }
            conn.lookup(topic, false)
        };
        shared.driver_waker.notify_one();
        let outcome_fut = RequestFut::cancellable(shared.clone(), request_id);
        tokio::pin!(outcome_fut);
        let outcome = loop {
            let cancelled = shared.operation_cancel_notify.notified();
            tokio::pin!(cancelled);
            cancelled.as_mut().enable();
            if retry_request_topic(&shared.inner.lock(), request).is_none() {
                return false;
            }
            tokio::select! {
                biased;
                () = cancelled.as_mut() => {}
                outcome = outcome_fut.as_mut() => break outcome,
            }
        };
        if retry_request_topic(&shared.inner.lock(), request).is_none() {
            return false;
        }
        match outcome {
            OpOutcome::LookupResponse {
                outcome: magnetar_proto::LookupOutcome::Connect { .. },
                ..
            } => {
                tracing::debug!(%topic, "retry-path lookup resolved");
                return true;
            }
            OpOutcome::LookupResponse {
                outcome: magnetar_proto::LookupOutcome::Failed { code, message },
                ..
            }
            | OpOutcome::Error { code, message, .. } => {
                failures = failures.saturating_add(1);
                if magnetar_proto::is_retryable_broker_error(
                    magnetar_proto::OperationKind::Lookup,
                    code,
                ) && retry_config.should_retry_after_failure(failures)
                {
                    let delay = retry_config.delay_after_failure(failures);
                    tracing::debug!(
                        %topic,
                        code,
                        failures,
                        ?delay,
                        "retry-path lookup rejected transiently; re-issuing"
                    );
                    if !wait_retry_delay(shared, request, delay).await {
                        return false;
                    }
                    continue;
                }
                terminalize_retry_request(shared, request, code, &message);
                return false;
            }
            OpOutcome::LookupResponse {
                outcome: magnetar_proto::LookupOutcome::Redirected { .. },
                ..
            } => {
                terminalize_retry_request(
                    shared,
                    request,
                    magnetar_proto::pb::ServerError::MetadataError as i32,
                    "retry-path lookup redirected to another broker",
                );
                return false;
            }
            other => {
                tracing::warn!(?other, %topic, "retry-path lookup landed unexpected outcome");
                return false;
            }
        }
    }
}

fn terminalize_retry_request(
    shared: &Arc<ConnectionShared>,
    request: RetryRequest,
    code: i32,
    message: &str,
) {
    let terminalized = {
        let mut conn = shared.inner.lock();
        if retry_request_topic(&conn, request).is_none() {
            false
        } else {
            match request {
                RetryRequest::Producer(handle, _) => {
                    conn.fail_producer_open_with_broker_error(handle, code, message);
                }
                RetryRequest::Consumer(handle, _) => {
                    conn.fail_consumer_subscribe_with_broker_error(handle, code, message);
                }
            }
            true
        }
    };
    if terminalized {
        shared.driver_waker.notify_one();
    }
}

/// Default size of the per-connection read buffer. Reads are non-blocking and append-style, so
/// this is just the high-water mark before allocation grows.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

/// Maximum bytes a driver loop iteration writes before giving the inbound
/// receipt path a chance to run.
const DRIVER_WRITE_BUDGET_BYTES: usize = 256 * 1024;

/// Handle to the driver task. Dropping this does not stop the driver — the driver keeps running
/// as long as the [`ConnectionShared`] arc is alive. Call [`DriverHandle::join`] to wait for it.
#[derive(Debug)]
pub struct DriverHandle {
    join: JoinHandle<Result<(), ClientError>>,
}

impl DriverHandle {
    /// Wait for the driver to terminate. Returns whatever error caused it to exit, or `Ok(())`
    /// if it exited cleanly (e.g. because of a local close + flush).
    ///
    /// # Errors
    ///
    /// Propagates the driver's terminal error, or wraps a [`tokio::task::JoinError`] in
    /// [`ClientError::Other`] if the driver panicked.
    pub async fn join(self) -> Result<(), ClientError> {
        match self.join.await {
            Ok(res) => res,
            Err(e) => Err(ClientError::Other(format!("driver task panicked: {e}"))),
        }
    }

    /// Abort the driver task.
    pub fn abort(&self) {
        self.join.abort();
    }
}

/// Reconnect context passed to the supervised driver. Lets the supervisor re-open the TCP
/// (and optionally TLS) connection to the broker after a transient drop.
///
/// When `service_url_provider` is set, every reconnect attempt re-resolves the broker URL
/// via [`magnetar_proto::ServiceUrlProvider::get_service_url`] instead of reusing the cached
/// `url`. This is the runtime hook that makes PIP-121 cluster failover policies
/// (`AutoClusterFailover`, `ControlledClusterFailover`) able to swap broker URLs between
/// reconnect attempts without re-building the client. See the PIP-121 row in `README.md`.
#[derive(Clone)]
pub(crate) struct ReconnectContext {
    /// Parsed Pulsar URL — `pulsar://` or `pulsar+ssl://` + host + port.
    /// Cached at start; refreshed via `service_url_provider` on every reconnect.
    pub(crate) url: ParsedUrl,
    /// `rustls::ClientConfig` for `pulsar+ssl://`. `None` for plaintext.
    pub(crate) tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Optional PIP-121 provider polled on every reconnect attempt. When `None`, the cached
    /// `url` is reused (matches the pre-PIP-121 behaviour).
    pub(crate) service_url_provider: Option<Arc<dyn magnetar_proto::ServiceUrlProvider>>,
    /// Optional pluggable DNS resolver invoked on every reconnect attempt before dialling
    /// the broker. When `None`, the runtime falls back to tokio's built-in
    /// [`tokio::net::lookup_host`] via [`Transport::connect`]. Mirrors Java's
    /// `ClientBuilder#dnsResolver`.
    pub(crate) dns_resolver: Option<Arc<dyn DnsResolver>>,
}

impl std::fmt::Debug for ReconnectContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectContext")
            .field("url", &self.url)
            .field("tls_enabled", &self.tls_config.is_some())
            .field(
                "has_service_url_provider",
                &self.service_url_provider.is_some(),
            )
            .field("has_dns_resolver", &self.dns_resolver.is_some())
            .finish()
    }
}

/// Compute the terminal-exit reason string fed to
/// [`magnetar_proto::Connection::fail_all_pending`] when a driver task exits
/// for good (plain-driver drop, or a supervisor that exhausted its budget).
///
/// A broker `CommandError` arriving mid-handshake populates
/// [`magnetar_proto::Connection::handshake_failure_reason`] in the proto layer,
/// but that reason survives the socket drop only as a stored field — the
/// driver's inner-loop `Err` is the generic `PeerClosed` (the read just
/// returned 0). Surfacing that generic string would discard the broker's
/// explanation, so when a handshake-failure reason is captured we PREFER it.
/// This is what lets `ConnectedFut`'s `ConnectionEvent::Closed` arm surface the
/// broker's "broker rejected handshake (server_error=…): …" text instead of the
/// opaque "peer closed the connection" — the capture-vs-terminal-drop race that
/// left the reason stranded. The reason is already length-bounded at the proto
/// capture site (ADR-0062); no further truncation here.
fn terminal_reason(conn: &magnetar_proto::Connection, outcome: &Result<(), ClientError>) -> String {
    if let Some(reason) = conn.handshake_failure_reason() {
        return reason.to_owned();
    }
    match outcome {
        Ok(()) => "connection closed".to_owned(),
        Err(err) => err.to_string(),
    }
}

/// Spawn the driver loop on the current tokio runtime — generic-socket flavour for
/// tests / `Client::from_socket`. The auto-reconnect supervisor is **not** active on this
/// spawn path: a generic socket has no notion of "reconnect", so the driver exits on the
/// first I/O failure regardless of [`magnetar_proto::ConnectionConfig::supervisor`].
pub(crate) fn spawn<S>(shared: Arc<ConnectionShared>, socket: S) -> DriverHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // The generic-socket path always issues an explicit `flush()` after every
    // `write_all` — we don't know whether the caller's socket is a TLS stream,
    // a buffered transport, or a test double, so the conservative choice keeps
    // the wire deterministic regardless.
    let join = tokio::spawn(async move {
        let mut socket = socket;
        let outcome = driver_loop_inner(&shared, &mut socket, true).await;
        // Plain (non-supervised) driver: TERMINAL exit, no reconnect. Fail
        // every pending op so parked subscribe / send / receive futures resolve
        // with a terminal error instead of hanging on a connection that is gone
        // (the no-progress stall). `driver_loop_inner` already ran
        // `mark_disconnected()` on its Err paths / `close()` snapped the state
        // on graceful close, so `is_connected()` is already false. Mirror of
        // the moonpool engine's plain spawn. ADR-0055.
        {
            let mut conn = shared.inner.lock();
            let reason = terminal_reason(&conn, &outcome);
            conn.fail_all_pending(&reason);
        }
        // ADR-0059: the plain driver is gone for good — latch
        // the no-driver signal so a NEW op issued after this point fast-fails
        // synchronously with `PeerClosed` at the entry-point guards instead of
        // registering a doomed pending op no driver is left to resolve. Set it
        // AFTER `fail_all_pending` so the slot `closed` flags + terminal
        // outcomes are already in place when a fresh op observes the latch.
        shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on the dedicated event waker rather than the proto waker slab.
        shared.event_waker.notify_waiters();
        shared.driver_waker.notify_waiters();
        outcome
    });
    DriverHandle { join }
}

/// Spawn the driver loop with the auto-reconnect supervisor wired in.
///
/// When [`magnetar_proto::ConnectionConfig::supervisor`] is `Some`, the driver re-handshakes
/// against the broker after a transient drop using `reconnect_ctx`. When the supervisor config
/// is `None`, behaviour matches [`spawn`] — driver exits on the first I/O failure.
pub(crate) fn spawn_supervised(
    shared: Arc<ConnectionShared>,
    socket: Transport,
    reconnect_ctx: ReconnectContext,
) -> DriverHandle {
    let driver_shared = shared.clone();
    let join = tokio::spawn(async move {
        let outcome = supervised_driver_loop(shared, socket, reconnect_ctx).await;
        // `supervised_driver_loop` only returns on a GENUINELY-terminal exit
        // (user-requested close, or the supervisor exhausted its reconnect
        // attempt budget) — the per-attempt drop is handled inside the loop
        // via `reset()` + replay. Fail every still-pending op so parked
        // subscribe / send / receive / ack futures resolve with a terminal
        // error instead of hanging forever (the no-progress stall). ADR-0055
        // §1: `fail_all_pending` fires on a supervisor that has exhausted its
        // attempts, never on the per-attempt reconnect.
        {
            let mut conn = driver_shared.inner.lock();
            let reason = terminal_reason(&conn, &outcome);
            conn.fail_all_pending(&reason);
        }
        // ADR-0059: `supervised_driver_loop` only returns on
        // a GENUINELY-terminal exit (user close, or the supervisor exhausted
        // its attempt budget) — never on a per-attempt reconnect — so latching
        // the no-driver signal here is safe: a transient `Failed` window mid
        // reconnect never reaches this point. New ops issued after this fast
        // fail at the entry-point guards. Set AFTER `fail_all_pending`.
        driver_shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on the dedicated event waker rather than the proto waker slab.
        driver_shared.event_waker.notify_waiters();
        driver_shared.driver_waker.notify_waiters();
        outcome
    });
    DriverHandle { join }
}

/// The supervised driver loop — runs [`driver_loop_inner`] on the current socket, then
/// (if the supervisor is configured and the user has not closed the connection) sleeps for
/// a backoff interval, reconnects, calls [`magnetar_proto::Connection::reset`], restarts the
/// handshake, and resumes.
async fn supervised_driver_loop(
    shared: Arc<ConnectionShared>,
    mut socket: Transport,
    reconnect_ctx: ReconnectContext,
) -> Result<(), ClientError> {
    // Seed the backoff RNG from the address pointer so independent clients to the same broker
    // spread their reconnect timing without depending on any I/O. `0` would land us on the
    // splitmix default; using the (stable, unique) Arc pointer mixes in per-Client entropy.
    let seed: u64 = Arc::as_ptr(&shared) as usize as u64;

    // Backoff schedule lives outside the reconnect loop and PERSISTS across cycles for this
    // client. `reset()` snaps `next_delay` back to `initial` only when the previous socket
    // survived past `cfg.drop_grace` — i.e. when the previous reconnect was stable. This
    // stops the "broker accepts handshake then drops in <drop_grace, backoff snaps to
    // initial" storm that ADR-0028's anti-thrash detector escalates against as the second
    // line of defence. Lazy-init from the in-loop cfg snapshot so dynamic config edits to
    // `initial_backoff` / `max_backoff` / `mandatory_stop` (future work) still take effect
    // before the supervisor has had to redial once.
    let mut backoff: Option<magnetar_proto::Backoff> = None;

    // Give-up budget counter (ADR-0061). Hoisted
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
    // definition.
    let mut give_up_attempts: u32 = 0;

    // First pass uses the current socket. The inner-loop result is what we propagate to the
    // caller if we exit without a supervisor reconnect. `socket_alive_since` lets us decide,
    // once `driver_loop_inner` returns, whether the previous socket lived long enough to
    // count as a stable reconnect (-> `backoff.reset()`) or died inside `drop_grace`
    // (-> keep growing). `flush_after_write` short-circuits the post-`write_all` `flush()`
    // syscall on plaintext TCP (the kernel buffer already pushes bytes onto the wire);
    // TLS keeps the flush because `tokio_rustls` buffers plaintext until `flush()` actually
    // emits an encrypted record.
    let mut socket_alive_since = Instant::now();
    let mut flush_after_write = transport_needs_flush(&socket);
    let mut last_inner_result = driver_loop_inner(&shared, &mut socket, flush_after_write).await;

    loop {
        // User-requested close beats reconnect — the state machine is in `Closing` /
        // `Closed`, so we propagate the inner result (Ok or Err) as-is. `Failed`
        // (transport drop, `mark_disconnected`) deliberately does NOT count: the
        // supervisor's whole purpose is to reconnect after that, so `is_user_closed()`
        // (which excludes `Failed`) is the right gate here.
        if shared.inner.lock().is_user_closed() {
            return last_inner_result;
        }

        // Snapshot the supervisor config + max-attempts on every iteration so dynamic updates
        // to it (future work) take effect before the next reconnect.
        let supervisor_cfg = shared.inner.lock().supervisor_config().cloned();
        let Some(cfg) = supervisor_cfg else {
            return last_inner_result;
        };

        // ADR-0028: the inner loop just exited because the socket closed (or
        // errored). If the transport closed inside the supervisor's
        // `drop_grace` of the most-recent successful re-attach, feed the drop
        // into the anti-thrash detector. This is the engine-side attribution
        // step — the per-pair `drop_within` knob on the threshold is the
        // strict policy gate that actually decides whether the paired entry
        // counts towards tripping cooldown.
        if cfg.anti_thrash_threshold.is_some() {
            let now = std::time::Instant::now();
            let should_record = {
                let conn = shared.inner.lock();
                conn.anti_thrash_state()
                    .last_reattach_at()
                    .is_some_and(|t| now.saturating_duration_since(t) <= cfg.drop_grace)
            };
            if should_record {
                shared.inner.lock().record_reattach_outcome(
                    now,
                    // Diagnostic handle — the detector cares only about the
                    // timestamp, so use any producer-handle marker. The real
                    // pairing happens inside `AntiThrashState::record`.
                    magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
                    magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
                );
            }
        }

        // ADR-0028: if the anti-thrash detector has armed a cooldown, sleep
        // until it expires before the next redial. This stacks above the
        // per-handle backoff (the inner backoff loop below still runs after).
        let cooldown_until = {
            let conn = shared.inner.lock();
            match conn.anti_thrash_tick(std::time::Instant::now()) {
                magnetar_proto::AntiThrashDisposition::Cooldown { until } => Some(until),
                magnetar_proto::AntiThrashDisposition::Normal => None,
            }
        };
        if let Some(until) = cooldown_until {
            let now = std::time::Instant::now();
            if until > now {
                let dur = until.saturating_duration_since(now);
                tracing::warn!(
                    cooldown_ms = u64::try_from(dur.as_millis()).unwrap_or(u64::MAX),
                    "supervisor: anti-thrash cooldown engaged; sleeping before next redial"
                );
                tokio::time::sleep(dur).await;
            }
            // Clear the cooldown so the next disconnect can re-arm it.
            shared.inner.lock().anti_thrash_state_mut().clear_cooldown();
        }

        // Backoff persistence policy (ADR-0028 alignment): lazy-init on the first redial,
        // then reuse across cycles. `reset()` is gated on the previous socket surviving past
        // `cfg.drop_grace` — sockets that died inside that window count as thrashes, so the
        // schedule keeps growing and successive ProducerReady-then-drop cycles slow down
        // geometrically up to `max_backoff`.
        //
        // ADR-0061: the give-up budget counter (`give_up_attempts`, hoisted
        // above) shares this SAME stability gate — a socket that survived
        // `drop_grace` resets BOTH the backoff schedule and the give-up budget,
        // so the two share one definition of "the last reconnect counted as
        // stable". A socket that died inside `drop_grace` (or never handshaked
        // at all, behind a TCP-accepting proxy) resets neither.
        let backoff = backoff.get_or_insert_with(|| cfg.build_backoff(seed));
        if cfg.should_reset_backoff(socket_alive_since.elapsed()) {
            backoff.reset();
            give_up_attempts = 0;
        }

        // Reconnect loop — keep trying until we land a fresh socket + handshake OR exhaust
        // `max_attempts`. The give-up counter spans the full dial+handshake
        // cycle (ADR-0061): each pass through this loop is one dial attempt; a
        // pass that dials successfully but whose post-handshake
        // `driver_loop_inner` later returns (handshake / session failure)
        // re-enters the outer loop without resetting the counter, so the next
        // dial increments from where this one left off.
        let new_socket = loop {
            let delay = backoff.next();
            tokio::time::sleep(delay).await;

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
            // gate as the outer loop — `Failed` from `mark_disconnected` must NOT abort
            // the reconnect.
            if shared.inner.lock().is_user_closed() {
                return last_inner_result;
            }

            // PIP-121 cluster failover — re-resolve the broker URL via the provider on every
            // attempt before dialling. The provider is sync + cheap by contract (see
            // `magnetar_proto::ServiceUrlProvider` doc); a provider that wants to do I/O must
            // park the work on a separate task and stamp its result into shared state. If no
            // provider is configured, fall back to the cached URL captured at start time.
            let target_url: std::borrow::Cow<'_, ParsedUrl> =
                match reconnect_ctx.service_url_provider.as_ref() {
                    Some(provider) => {
                        let raw = provider.get_service_url();
                        match ParsedUrl::parse(&raw) {
                            Ok(parsed) => std::borrow::Cow::Owned(parsed),
                            Err(err) => {
                                tracing::warn!(
                                    attempt,
                                    error = %err,
                                    "supervisor: service-url provider returned an unparseable \
                                     URL; falling back to the cached URL"
                                );
                                std::borrow::Cow::Borrowed(&reconnect_ctx.url)
                            }
                        }
                    }
                    None => std::borrow::Cow::Borrowed(&reconnect_ctx.url),
                };

            let resolver = reconnect_ctx.dns_resolver.as_deref();
            // Each reconnect dial inherits the `connect_timeout` chokepoint so a
            // hung re-dial is abandoned instead of stalling the supervisor loop
            // (ADR-0052). Snapshot per-iteration so a future dynamic config
            // update takes effect on the next attempt.
            let connect_timeout = shared.inner.lock().connect_timeout();
            match Transport::connect_with_resolver(
                &target_url,
                reconnect_ctx.tls_config.clone(),
                resolver,
                connect_timeout,
            )
            .await
            {
                Ok(t) => {
                    let (host, port) = target_url.socket_addr();
                    // ADR-0061: this is a TCP-connect, NOT a confirmed reconnect
                    // — behind a TCP-accepting proxy the dial succeeds while the
                    // backend (and hence the Pulsar handshake) is down. The
                    // TRUE reconnect-success info log fires AFTER the handshake
                    // completes (the `ProducerReady` / `SubscribeAcked` path);
                    // mislabelling a TCP accept as a reconnect would tell
                    // operators the broker is back when it is not.
                    tracing::info!(
                        attempt,
                        host = %host,
                        port,
                        "supervisor: TCP connected; handshaking"
                    );
                    break t;
                }
                Err(err) => {
                    let (host, port) = target_url.socket_addr();
                    tracing::warn!(
                        attempt,
                        host = %host,
                        port,
                        error = %err,
                        "supervisor: reconnect attempt failed; will retry"
                    );
                    // Loop and back off again.
                }
            }
        };

        // Got a new transport. Reset the state machine + kick off CONNECT. Stage 3: arm the
        // rebuild flag so the inner loop replays every still-open producer / consumer once the
        // new socket's handshake completes.
        {
            let mut conn = shared.inner.lock();
            conn.reset();
            if let Err(err) = conn.begin_handshake() {
                // Should never happen — reset() snaps state back to Uninitialized — but if it
                // does, surface it.
                tracing::error!(error = %err, "supervisor: begin_handshake after reset failed");
                return Err(err.into());
            }
        }
        shared
            .pending_rebuild
            .store(true, std::sync::atomic::Ordering::SeqCst);
        notify_retry_generation_replaced(&shared);

        socket = new_socket;
        socket_alive_since = Instant::now();
        flush_after_write = transport_needs_flush(&socket);
        last_inner_result = driver_loop_inner(&shared, &mut socket, flush_after_write).await;
    }
}

/// Whether the inner driver loop should issue an explicit `flush()` after
/// every `write_all`. Plaintext TCP doesn't need it — the kernel-buffered
/// `write_all` already pushes bytes to the socket and there's no user-space
/// buffer to drain. TLS does need it — `tokio_rustls::TlsStream::flush()` is
/// what actually emits the encrypted record onto the wire.
fn transport_needs_flush(transport: &Transport) -> bool {
    match transport {
        Transport::Plain(_) => false,
        Transport::Tls(_) => true,
    }
}

/// Write every byte of every segment to `stream`, advancing through
/// the segment list as the kernel reports progress (ADR-0040 wave 2).
///
/// Equivalent to `AsyncWriteExt::write_all` for a contiguous buffer,
/// but lets the kernel concatenate disjoint segments via `writev(2)` —
/// skipping the user-space memcpy that the legacy contiguous-coalesce
/// path performs at
/// `magnetar_proto::frame::encode_payload`'s `dst.extend_from_slice(payload)`.
///
/// Implementation notes:
///
/// - **Partial writes**. `AsyncWriteExt::write_vectored` returns the number of bytes the kernel
///   accepted from the *front* of the slice list; not necessarily all of them. We advance
///   per-segment offsets and re-issue `write_vectored` until every byte has been accepted.
/// - **WriteZero**. A successful `write_vectored` returning `0` when the IoSlice array is non-empty
///   is treated the same as `AsyncWriteExt::write_all` does — an `io::ErrorKind::WriteZero` so the
///   driver doesn't spin.
/// - **Vectored support detection**. We do not check `AsyncWrite::is_write_vectored`; the default
///   `poll_write_vectored` impl falls back to a single-buffer `poll_write` with the first non-empty
///   slice, which still makes progress (just without the syscall reduction). The fall-back loop is
///   correct on every `AsyncWrite + Unpin`.
#[cfg(test)]
async fn write_all_vectored<S>(stream: &mut S, segs: &[bytes::Bytes]) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut offsets: Vec<usize> = vec![0; segs.len()];
    loop {
        let slices: Vec<IoSlice<'_>> = segs
            .iter()
            .zip(offsets.iter())
            .filter_map(|(seg, &off)| {
                let rest = &seg[off..];
                if rest.is_empty() {
                    None
                } else {
                    Some(IoSlice::new(rest))
                }
            })
            .collect();
        if slices.is_empty() {
            return Ok(());
        }
        let n = stream.write_vectored(&slices).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0 with non-empty IoSlice array",
            ));
        }
        let mut remaining = n;
        for (seg, off) in segs.iter().zip(offsets.iter_mut()) {
            let avail = seg.len().saturating_sub(*off);
            if avail == 0 {
                continue;
            }
            if remaining >= avail {
                *off = seg.len();
                remaining -= avail;
            } else {
                *off += remaining;
                remaining = 0;
                break;
            }
        }
        debug_assert_eq!(remaining, 0, "kernel reported more bytes than queued");
    }
}

struct PendingDriverWrite {
    segments: VecDeque<bytes::Bytes>,
    front_offset: usize,
}

impl PendingDriverWrite {
    fn new() -> Self {
        Self {
            segments: VecDeque::new(),
            front_offset: 0,
        }
    }

    #[cfg(test)]
    fn from_transmit(transmit: magnetar_proto::TransmitOwned) -> Self {
        let mut pending = Self::new();
        pending.push_transmit(transmit);
        pending
    }

    fn push_transmit(&mut self, transmit: magnetar_proto::TransmitOwned) {
        debug_assert!(
            self.is_empty(),
            "new transmit must only be pulled after the pending write queue drains"
        );
        match transmit {
            magnetar_proto::TransmitOwned::Contiguous(buf) => {
                if !buf.is_empty() {
                    self.segments.push_back(buf);
                }
            }
            magnetar_proto::TransmitOwned::Vectored(segs) => {
                self.segments
                    .extend(segs.into_iter().filter(|s| !s.is_empty()));
            }
        }
        self.front_offset = 0;
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    async fn write_budgeted<S>(&mut self, stream: &mut S, budget: usize) -> std::io::Result<usize>
    where
        S: AsyncWrite + Unpin,
    {
        let mut written = 0usize;
        let mut remaining = budget;
        while remaining > 0 {
            let Some(front) = self.segments.front() else {
                break;
            };
            let available = front.len().saturating_sub(self.front_offset);
            if available == 0 {
                let _ = self.segments.pop_front();
                self.front_offset = 0;
                continue;
            }
            let n = available.min(remaining);
            stream
                .write_all(&front[self.front_offset..self.front_offset + n])
                .await?;
            self.front_offset += n;
            written += n;
            remaining -= n;
            if self.front_offset == front.len() {
                let _ = self.segments.pop_front();
                self.front_offset = 0;
            }
        }
        Ok(written)
    }
}

/// The per-socket driver loop.
///
/// Implementation notes:
///
/// - **Lock discipline**: every interaction with `magnetar_proto::Connection` happens inside a
///   `parking_lot::Mutex::lock()` critical section. Critical sections are short — they never
///   `.await`.
/// - **Write path**: we drain outbound bytes from the state machine into an owned queue, release
///   the lock, then write a bounded slice before giving reads a chance to run. The unwritten tail
///   stays in the driver and is flushed by later iterations.
/// - **Read path**: we read directly into a `BytesMut` then hand its slice to the state machine.
///   The state machine handles framing — partial frames stay in its internal `inbound` buffer.
/// - **Timeout**: `Connection::poll_timeout` returns the next deadline if any. We `tokio::select!`
///   against `tokio::time::sleep_until(deadline)`. If no deadline is set, that arm is disabled.
pub(crate) async fn driver_loop_inner<S>(
    shared: &Arc<ConnectionShared>,
    socket: &mut S,
    flush_after_write: bool,
) -> Result<(), ClientError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);
    let mut pending_write = PendingDriverWrite::new();
    let mut close_after_write = false;

    loop {
        // Drain outbound bytes + check if the state machine wants us to terminate.
        // `poll_transmit` already calls `Connection::drain_producer_outbound`
        // internally to merge per-slot staged frames (queued by
        // `Producer::send` without taking the global lock — ADR-0038 Phase 3)
        // into the connection-wide outbound buffer before returning the byte
        // slice for the driver to flush.
        let (write_data, deadline, should_close) = if pending_write.is_empty() {
            let mut conn = shared.inner.lock();
            // ADR-0040 wave 2: take the owned `TransmitOwned` so we can
            // drop the lock before awaiting on the socket. The contiguous
            // arm carries the same `Bytes` the legacy `poll_transmit`
            // returned (O(1) ownership transfer via `BytesMut::split()`);
            // the vectored arm carries the producer batch's
            // `[head, payload]` segment list — dispatched below via
            // `write_vectored` to skip the user-space coalesce memcpy.
            let out = conn.poll_transmit_owned();
            let dl = conn.poll_timeout();
            let closing = matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Closing
                    | magnetar_proto::HandshakeState::Closed
                    | magnetar_proto::HandshakeState::Failed
            );
            (out, dl, closing)
        } else {
            let conn = shared.inner.lock();
            let dl = conn.poll_timeout();
            let closing = matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Closing
                    | magnetar_proto::HandshakeState::Closed
                    | magnetar_proto::HandshakeState::Failed
            );
            (
                magnetar_proto::TransmitOwned::Contiguous(bytes::Bytes::new()),
                dl,
                closing,
            )
        };
        close_after_write |= should_close;
        if pending_write.is_empty() {
            pending_write.push_transmit(write_data);
        }

        // Flush whatever the state machine produced. This happens *outside* the lock so user
        // futures can keep enqueuing while we hold the network handle.
        //
        // `flush_after_write` is `true` for TLS transports (the rustls layer
        // buffers plaintext until `flush()` actually emits the record) and for
        // unknown / generic-socket spawn paths (conservative default). For
        // plaintext TCP it's `false` — kernel-buffered `write_all` already
        // pushes the bytes to the socket and there's no user-space buffer to
        // drain, so the extra `flush()` is wasted syscall overhead.
        if !pending_write.is_empty() {
            match pending_write
                .write_budgeted(socket, DRIVER_WRITE_BUDGET_BYTES)
                .await
            {
                Ok(bytes) => tracing::trace!(bytes, "writing outbound bytes"),
                Err(err) => {
                    shared.inner.lock().mark_disconnected();
                    return Err(err.into());
                }
            }
            if flush_after_write {
                if let Err(err) = socket.flush().await {
                    shared.inner.lock().mark_disconnected();
                    return Err(err.into());
                }
            }
        }

        if pending_write.is_empty() && close_after_write {
            // Connection is winding down; give the peer a chance to see the EOF and exit.
            let _ = socket.shutdown().await;
            return Ok(());
        }

        // Park until something interesting happens.
        let sleep = match deadline {
            Some(t) => {
                let now = Instant::now();
                let dur = t.saturating_duration_since(now);
                Some(tokio::time::sleep(dur))
            }
            None => None,
        };

        tokio::select! {
            // ADR-0070 read-fairness (issue #303): under a sustained `send`
            // burst every `Producer::send` pulses `driver_waker.notify_one()`,
            // so a waker permit is almost always pending on loop entry. With
            // `biased;` the FIRST arm wins whenever it is ready, so polling the
            // `driver_waker` arm first would let writes starve the inbound path
            // — already-arrived `CommandSendReceipt` bytes would not be read
            // that iteration and the matching `SendFut`s would resolve late.
            //
            // The outbound path is NOT starved by giving reads priority here:
            // `poll_transmit` + `write_all` already run at the TOP of every loop
            // iteration (above), so each tick flushes pending sends regardless
            // of which `select!` arm wins. Draining the socket first therefore
            // keeps BOTH directions live. `biased;` is retained so the arm
            // order is deterministic — the moonpool engine's
            // bit-for-bit-reproducible simulation depends on it, and a
            // non-biased `select!` would pick arms via an uncontrolled
            // thread-local RNG (ADR-0024 determinism constraint). The read arm
            // is cancel-safe: bytes land in the persistent `read_buf` and are
            // only consumed via `split()` AFTER this arm wins, so reordering
            // drops nothing. The full read/write task split is the deferred
            // follow-up (TLS streams cannot `into_split`; supervisor reconnect
            // rework) — this localized reorder is the minimal fix.
            biased;

            // Inbound bytes (polled first for receipt fairness — see above).
            r = socket.read_buf(&mut read_buf) => {
                let n = match r {
                    Ok(n) => n,
                    Err(err) => {
                        shared.inner.lock().mark_disconnected();
                        return Err(err.into());
                    }
                };
                if n == 0 {
                    // Peer closed cleanly. Mark the state machine as disconnected so user
                    // futures see is_connected() flip and the disconnect timestamp records.
                    // State-consistency postcondition (asserted on the *same* guard — no
                    // re-lock, so no race with concurrent user futures; ADR-0038): once
                    // `mark_disconnected()` runs the connection must report
                    // `!is_connected()` (state snaps to `Failed`). A regression that left it
                    // `Connected` would leak a dead socket into user-facing `is_connected()`.
                    {
                        let mut conn = shared.inner.lock();
                        conn.mark_disconnected();
                        debug_assert!(
                            !conn.is_connected(),
                            "mark_disconnected() must clear is_connected() (ADR-0038)"
                        );
                    }
                    return Err(ClientError::PeerClosed);
                }
                // ADR-0040 wave 3 (read-path ownership pass-through):
                // hand the freshly-read `BytesMut` chunk to the state
                // machine via `handle_bytes_owned`. When the proto's
                // internal `inbound` buffer is empty (the common case
                // after a full-frame decode), the chunk is *swapped*
                // into place with zero memcpy. Mid-frame fall-back
                // re-uses the legacy `extend_from_slice` path. The
                // local `read_buf` keeps a fresh empty
                // `BytesMut::with_capacity(READ_BUFFER_CAPACITY)` for
                // the next iteration (via `split()`'s O(1) move).
                let chunk = read_buf.split();
                // Read-buffer postcondition: `read_buf` is drained via `split()` on every
                // inbound-arm iteration and never appended to elsewhere, so it is empty when
                // `read_buf()` runs — the freshly split chunk therefore carries exactly the
                // `n` bytes just read. A mismatch would mean stale bytes leaked across loop
                // iterations into the byte stream fed to `handle_bytes_owned`.
                debug_assert_eq!(
                    chunk.len(),
                    n,
                    "read chunk length must equal the byte count just read"
                );
                let now = Instant::now();
                // ADR-0038: the `shared.inner` guard returned by `lock()` is a
                // *temporary* in the `if let` scrutinee, which lives until the
                // end of the consequent block. Re-locking `shared.inner` inside
                // the error branch would re-enter the non-reentrant
                // `parking_lot::Mutex` and self-deadlock the driver task. Bind
                // the result to a `let` first: the guard drops at the `;`,
                // before the branch body takes the lock again. (Latent twin of
                // the moonpool-engine deadlock surfaced by sim_chaos
                // swizzle-clog seeds 0x56201ccaba82dbc1 / 0xdc638c565234d23f.)
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
                        let (n_p, n_c) = {
                            let mut conn = shared.inner.lock();
                            let producers = conn.rebuild_producers();
                            let consumers = conn.rebuild_consumers();
                            (producers.len(), consumers.len())
                        };
                        notify_retry_generation_replaced(shared);
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
                // After handling bytes, drain only the driver-actionable subset of
                // semantic events (AuthChallenge / TopicListChanged / TopicMigrated).
                // Per-future Wakers registered via [`Connection::register_waker`] are
                // already woken inline by the sans-io layer; event-stream-watching
                // futures (`EventWaitFut` for ProducerReady / SubscribeAcked) get
                // pulsed via the dedicated event waker below so they re-poll and
                // observe the freshly-pushed event without competing with the
                // driver for outbound-work permits.
                handle_pending_events(shared)?;
                shared.event_waker.notify_waiters();
                shared.driver_waker.notify_waiters();
            }

            // Driver wake-up from user-facing futures (e.g. a freshly-enqueued
            // send). Polled AFTER the inbound arm (see the read-fairness note at
            // the top of this `select!`): when both are ready the socket is
            // drained first so receipts are not deferred; the enqueued frames
            // are still flushed by the top-of-loop `poll_transmit` on the next
            // iteration.
            () = shared.driver_waker.notified() => {
                // Loop: poll_transmit will drain whatever the future enqueued.
            }

            () = async {
                if pending_write.is_empty() {
                    std::future::pending::<()>().await;
                }
            } => {
                // Pending outbound bytes remain. Yield the read arm once, then
                // continue with the next bounded write slice.
            }

            // Timer fired.
            () = async {
                match sleep {
                    Some(s) => s.await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                shared.inner.lock().handle_timeout(Instant::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! ADR-0040 wave 2 — `driver::write_all_vectored` over a real
    //! `tokio::net::TcpStream`. 1:1 mirror of
    //! `magnetar-runtime-moonpool/src/transport.rs`'s `write_all_vectored`
    //! Plain-arm tests (ADR-0024 layer (b) + the strict runtime-test-parity
    //! count). The tokio engine writes byte-identical output to the moonpool
    //! engine; real TCP coalesces, so these assert the *reassembled stream*
    //! rather than per-segment delivery boundaries (which only the sim
    //! `SimTcpStream` preserves).

    use std::future::Future as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use bytes::{Bytes, BytesMut};
    use magnetar_proto::types::CompressionKind;
    use magnetar_proto::{
        ConnectionConfig, CreateProducerRequest, OpOutcome, OperationRetryConfig, PendingOpKey,
        SubscribeRequest, decode_one, encode_command, pb,
    };
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::{
        DRIVER_WRITE_BUDGET_BYTES, PendingDriverWrite, RetryRequest, lookup_then,
        notify_retry_generation_replaced, spawn_retry_leg, terminalize_retry_request,
        write_all_vectored,
    };
    use crate::ConnectionShared;
    use crate::producer::Producer;

    #[test]
    fn permanent_reattachment_errors_wake_established_operation_waiters() {
        struct CountingWake(AtomicUsize);
        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let shared = ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
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
        conn.handle_bytes(Instant::now(), &frame)
            .expect("complete handshake");
        while conn.poll_event().is_some() {}

        let producer_request_id = conn.peek_next_request_id_for_test();
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/permanent-reattach-producer".to_owned(),
            ..Default::default()
        });
        let producer_success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: producer_request_id,
                producer_name: "producer".to_owned(),
                last_sequence_id: Some(-1),
                producer_ready: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &producer_success).expect("encode ProducerSuccess");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("establish producer");
        while conn.poll_event().is_some() {}

        let consumer_request_id = conn.peek_next_request_id_for_test();
        let consumer = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/permanent-reattach-consumer".to_owned(),
            subscription: "permanent-reattach".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        });
        let subscribe_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: consumer_request_id,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &subscribe_success).expect("encode CommandSuccess");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("establish consumer");
        while conn.poll_event().is_some() {}

        let snapshot_sequence_id = conn
            .send(
                producer,
                magnetar_proto::producer::OutgoingMessage {
                    payload: Bytes::from_static(b"before-reset"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 12,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send before reset");
        let reset_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let reset_waker: Waker = Arc::clone(&reset_counter).into();
        conn.register_waker(
            PendingOpKey::Send(producer, snapshot_sequence_id),
            reset_waker,
        );
        conn.reset();
        assert_eq!(reset_counter.0.load(Ordering::SeqCst), 1);
        let snapshot_terminal_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let snapshot_terminal_waker: Waker = Arc::clone(&snapshot_terminal_counter).into();
        conn.register_waker(
            PendingOpKey::Send(producer, snapshot_sequence_id),
            snapshot_terminal_waker,
        );
        conn.begin_handshake().expect("re-handshake");
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &connected).expect("encode CommandConnected");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("complete reconnect handshake");
        while conn.poll_event().is_some() {}
        let producer_retry = conn.rebuild_producers()[0];
        let consumer_retry = conn.rebuild_consumers()[0];

        let sequence_id = conn
            .send(
                producer,
                magnetar_proto::producer::OutgoingMessage {
                    payload: Bytes::from_static(b"pending"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 7,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                Instant::now(),
            )
            .expect("queue send during producer reattachment");
        let send_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let send_waker: Waker = Arc::clone(&send_counter).into();
        conn.register_waker(PendingOpKey::Send(producer, sequence_id), send_waker);
        let receive_counter = Arc::new(CountingWake(AtomicUsize::new(0)));
        let receive_waker: Waker = Arc::clone(&receive_counter).into();
        conn.register_consumer_receive_waker(consumer, receive_waker)
            .expect("register receive waker during consumer reattachment");

        for request_id in [producer_retry, consumer_retry] {
            let error = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: request_id.0,
                    error: pb::ServerError::TopicNotFound as i32,
                    message: "topic was deleted".to_owned(),
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &error).expect("encode CommandError");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle terminal reattachment error");
        }

        assert_eq!(send_counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            conn.take_outcome(PendingOpKey::Send(producer, sequence_id)),
            Some(OpOutcome::Terminal { .. })
        ));
        assert_eq!(snapshot_terminal_counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            conn.take_outcome(PendingOpKey::Send(producer, snapshot_sequence_id)),
            Some(OpOutcome::Terminal { .. })
        ));
        assert_eq!(receive_counter.0.load(Ordering::SeqCst), 1);
        assert!(conn.consumer(consumer).is_some());
        assert!(conn.consumer_handle_is_terminal(consumer));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_established_handle_cancels_blackholed_retry_lookup() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, slot, request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let request_id = magnetar_proto::RequestId(conn.peek_next_request_id_for_test());
            let handle = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/blackholed-retry-drop".to_owned(),
                ..Default::default()
            });
            let slot = conn.producer(handle).expect("producer slot").clone();
            (handle, slot, request_id)
        };
        let producer =
            Producer::assemble(shared.clone(), handle, slot, CompressionKind::None, None);
        let mut lookup = Box::pin(lookup_then(
            &shared,
            "persistent://public/default/blackholed-retry-drop",
            RetryRequest::Producer(handle, request_id),
        ));
        std::future::poll_fn(|cx| {
            assert!(lookup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        drop(producer);

        let completed = tokio::time::timeout(Duration::from_millis(100), lookup)
            .await
            .expect("dropping the handle must wake the blackholed lookup");
        assert!(!completed, "a closed handle must cancel its retry lookup");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_established_handle_cancels_initial_retry_backoff() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, slot, failed_request_id) = {
            let mut conn = shared.inner.lock();
            conn.set_operation_retry_config(OperationRetryConfig {
                initial_backoff: Duration::from_secs(30),
                max_backoff: Duration::from_secs(30),
                max_retries: Some(1),
            });
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let producer_request_id = conn.peek_next_request_id_for_test();
            let handle = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/retry-backoff-drop".to_owned(),
                ..Default::default()
            });
            let slot = conn.producer(handle).expect("producer slot").clone();
            let producer_success = pb::BaseCommand {
                r#type: pb::base_command::Type::ProducerSuccess as i32,
                producer_success: Some(pb::CommandProducerSuccess {
                    request_id: producer_request_id,
                    producer_name: "producer".to_owned(),
                    last_sequence_id: Some(-1),
                    producer_ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &producer_success).expect("encode ProducerSuccess");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("establish producer");
            while conn.poll_event().is_some() {}
            conn.reset();
            conn.begin_handshake().expect("restart handshake");
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &connected).expect("encode CommandConnected");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete reconnect handshake");
            while conn.poll_event().is_some() {}
            let failed_request_id = conn.rebuild_producers()[0];
            let transient_error = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: failed_request_id.0,
                    error: pb::ServerError::ProducerBusy as i32,
                    message: "retry later".to_owned(),
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &transient_error).expect("encode transient error");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("schedule established retry");
            (handle, slot, failed_request_id)
        };
        let producer =
            Producer::assemble(shared.clone(), handle, slot, CompressionKind::None, None);
        let weak = Arc::downgrade(&shared);
        spawn_retry_leg(&shared, RetryRequest::Producer(handle, failed_request_id));

        drop(producer);
        drop(shared);

        tokio::time::timeout(Duration::from_millis(100), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the handle must cancel the initial retry backoff");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_lookup_does_not_emit_before_reconnect_handshake_completes() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let request_id = magnetar_proto::RequestId(conn.peek_next_request_id_for_test());
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/retry-before-reconnect-handshake".to_owned(),
                subscription: "retry-before-reconnect-handshake".to_owned(),
                ..Default::default()
            });
            conn.reset();
            conn.begin_handshake().expect("restart handshake");
            (handle, request_id)
        };

        let completed = tokio::time::timeout(
            Duration::from_millis(100),
            lookup_then(
                &shared,
                "persistent://public/default/retry-before-reconnect-handshake",
                RetryRequest::Consumer(handle, request_id),
            ),
        )
        .await
        .expect("a pre-handshake retry lookup must cancel instead of parking");
        assert!(!completed);

        let mut staged = shared.inner.lock().poll_transmit();
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            assert_ne!(
                frame.command.r#type,
                pb::base_command::Type::Lookup as i32,
                "no data-plane lookup may precede CommandConnected"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn superseding_consumer_generation_cancels_blackholed_retry_lookup() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let request_id = magnetar_proto::RequestId(conn.peek_next_request_id_for_test());
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/superseded-blackholed-retry".to_owned(),
                subscription: "superseded-blackholed-retry".to_owned(),
                ..Default::default()
            });
            (handle, request_id)
        };
        let lookup_request_id =
            magnetar_proto::RequestId(shared.inner.lock().peek_next_request_id_for_test());
        let mut lookup = Box::pin(lookup_then(
            &shared,
            "persistent://public/default/superseded-blackholed-retry",
            RetryRequest::Consumer(handle, request_id),
        ));
        std::future::poll_fn(|cx| {
            assert!(lookup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            shared
                .inner
                .lock()
                .has_pending_request_for_test(lookup_request_id)
        );

        shared
            .inner
            .lock()
            .resubscribe_consumer_after_seek(handle)
            .expect("replacement subscribe generation");
        notify_retry_generation_replaced(&shared);

        let completed = tokio::time::timeout(Duration::from_millis(100), lookup)
            .await
            .expect("generation replacement must wake the blackholed lookup");
        assert!(!completed);
        assert!(
            !shared
                .inner
                .lock()
                .has_pending_request_for_test(lookup_request_id),
            "cancelled retry lookup must unregister its pending request"
        );
    }

    #[test]
    fn superseded_consumer_retry_lookup_cannot_terminalize_current_generation() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, superseded_request_id, current_request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");

            let initial_request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/superseded-retry-lookup".to_owned(),
                subscription: "superseded-retry-lookup".to_owned(),
                ..Default::default()
            });
            let success = pb::BaseCommand {
                r#type: pb::base_command::Type::Success as i32,
                success: Some(pb::CommandSuccess {
                    request_id: initial_request_id,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &success).expect("encode initial success");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("establish consumer");

            let superseded_request_id = conn
                .resubscribe_consumer_after_seek(handle)
                .expect("superseded subscribe generation");
            let current_request_id = conn
                .resubscribe_consumer_after_seek(handle)
                .expect("current subscribe generation");
            (handle, superseded_request_id, current_request_id)
        };

        terminalize_retry_request(
            &shared,
            RetryRequest::Consumer(handle, superseded_request_id),
            pb::ServerError::AuthorizationError as i32,
            "stale retry lookup denied",
        );

        let mut conn = shared.inner.lock();
        assert!(
            !conn.consumer_handle_is_terminal(handle),
            "a terminal lookup from the superseded retry must not kill the current generation"
        );
        assert!(
            conn.retry_consumer_subscribe_if_current(handle, current_request_id)
                .is_some(),
            "the current generation must remain retryable"
        );
    }

    #[test]
    fn superseded_producer_retry_lookup_cannot_terminalize_current_generation() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, superseded_request_id, current_request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/superseded-producer-retry".to_owned(),
                ..Default::default()
            });
            let success = pb::BaseCommand {
                r#type: pb::base_command::Type::ProducerSuccess as i32,
                producer_success: Some(pb::CommandProducerSuccess {
                    request_id,
                    producer_name: "producer".to_owned(),
                    last_sequence_id: Some(-1),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &success).expect("encode ProducerSuccess");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("establish producer");
            let superseded_request_id = conn
                .rebuild_producers()
                .into_iter()
                .next()
                .expect("superseded producer generation");
            let current_request_id = conn
                .rebuild_producers()
                .into_iter()
                .next()
                .expect("current producer generation");
            (handle, superseded_request_id, current_request_id)
        };

        terminalize_retry_request(
            &shared,
            RetryRequest::Producer(handle, superseded_request_id),
            pb::ServerError::AuthorizationError as i32,
            "stale producer retry lookup denied",
        );

        let mut conn = shared.inner.lock();
        assert!(
            !conn.producer_is_closed(handle),
            "a terminal lookup from the superseded retry must not kill the current producer generation"
        );
        assert!(
            conn.retry_producer_open_if_current(handle, current_request_id)
                .is_some(),
            "the current producer generation must remain retryable"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn superseding_producer_generation_cancels_blackholed_retry_lookup() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("begin handshake");
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
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete handshake");
            let request_id = magnetar_proto::RequestId(conn.peek_next_request_id_for_test());
            let handle = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/superseded-producer-blackhole".to_owned(),
                ..Default::default()
            });
            (handle, request_id)
        };
        let lookup_request_id =
            magnetar_proto::RequestId(shared.inner.lock().peek_next_request_id_for_test());
        let mut lookup = Box::pin(lookup_then(
            &shared,
            "persistent://public/default/superseded-producer-blackhole",
            RetryRequest::Producer(handle, request_id),
        ));
        std::future::poll_fn(|cx| {
            assert!(lookup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        shared.inner.lock().rebuild_producers();
        notify_retry_generation_replaced(&shared);

        let completed = tokio::time::timeout(Duration::from_millis(100), lookup)
            .await
            .expect("producer generation replacement must wake the blackholed lookup");
        assert!(!completed);
        assert!(
            !shared
                .inner
                .lock()
                .has_pending_request_for_test(lookup_request_id),
            "cancelled producer retry lookup must unregister its pending request"
        );
    }

    /// A small multi-segment vectored write reassembles, in order, to the
    /// concatenation of its segments on the peer.
    #[tokio::test(flavor = "current_thread")]
    async fn write_all_vectored_delivers_segments_in_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let (mut server, _peer) = listener.accept().await.expect("accept");

        let segs = vec![
            Bytes::from_static(b"AAAA"),
            Bytes::from_static(b"BBBBBB"),
            Bytes::from_static(b"CC"),
        ];
        let mut expected: Vec<u8> = Vec::new();
        for s in &segs {
            expected.extend_from_slice(s);
        }

        write_all_vectored(&mut client, &segs)
            .await
            .expect("vectored write");
        drop(client); // clean EOF so the read loop terminates

        let mut received = Vec::new();
        server
            .read_to_end(&mut received)
            .await
            .expect("read_to_end");
        assert_eq!(
            received, expected,
            "reassembled stream must equal the segment concatenation, in order",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn driver_write_budget_leaves_tail_for_next_tick() {
        let mut pending =
            PendingDriverWrite::from_transmit(magnetar_proto::TransmitOwned::Contiguous(
                Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"),
            ));
        let (mut client, mut server) = tokio::io::duplex(64);

        let written = pending
            .write_budgeted(&mut client, 8)
            .await
            .expect("budgeted write");
        assert_eq!(written, 8);
        assert!(
            !pending.is_empty(),
            "the driver must keep unwritten bytes so it can read before continuing writes"
        );

        let mut observed = vec![0; 8];
        server
            .read_exact(&mut observed)
            .await
            .expect("read first budget");
        assert_eq!(&observed, b"abcdefgh");

        let rest = pending
            .write_budgeted(&mut client, DRIVER_WRITE_BUDGET_BYTES)
            .await
            .expect("drain tail");
        assert_eq!(rest, 18);
        assert!(pending.is_empty());
    }

    /// Segments whose combined length far exceeds the socket send buffer
    /// force at least one short `write_vectored`. The offset-tracking loop
    /// must re-issue the writev for the unflushed tail until every byte
    /// lands; the peer's reassembled stream must be byte-identical to the
    /// concatenation. The reader drains concurrently so the writer's
    /// backpressure clears.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_all_vectored_handles_partial_accept() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let (mut server, _peer) = listener.accept().await.expect("accept");

        // 4 MiB per segment, 3 segments = 12 MiB — comfortably larger than
        // any default loopback socket buffer, guaranteeing partial accepts.
        let seg_len = 4 * 1024 * 1024;
        let segs = vec![
            Bytes::from(vec![1u8; seg_len]),
            Bytes::from(vec![2u8; seg_len]),
            Bytes::from(vec![3u8; seg_len]),
        ];
        let mut expected: Vec<u8> = Vec::with_capacity(seg_len * 3);
        for s in &segs {
            expected.extend_from_slice(s);
        }
        let total = expected.len();

        let writer = tokio::spawn(async move {
            write_all_vectored(&mut client, &segs)
                .await
                .expect("vectored write (partial-accept)");
            // Drop closes the socket → reader sees EOF after the last byte.
            drop(client);
        });

        let mut received: Vec<u8> = Vec::with_capacity(total);
        server
            .read_to_end(&mut received)
            .await
            .expect("read_to_end");
        writer.await.expect("writer task joined");

        assert_eq!(
            received.len(),
            total,
            "partial-accept loop must flush every byte",
        );
        assert_eq!(
            received, expected,
            "reassembled stream must equal the segment concatenation",
        );
    }
}
