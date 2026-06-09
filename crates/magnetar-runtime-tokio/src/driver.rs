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

use std::io::IoSlice;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::ConnectionEvent;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::ConnectionShared;
use crate::dns::DnsResolver;
use crate::error::ClientError;
use crate::transport::Transport;
use crate::url_parse::ParsedUrl;

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
            ConnectionEvent::ProducerOpenFailedTransient {
                handle,
                code,
                message,
            } => {
                // Broker bounced the `CommandProducer` with a transient code
                // (`ServiceNotReady`, `MetadataError`, `TopicNotFound`) — typical
                // post-`docker restart` window where the namespace bundle hasn't
                // been re-acquired yet. Pulsar's recommended recovery is "Please
                // redo the lookup": a fresh `CommandLookupTopic` triggers the
                // broker to (re)acquire bundle ownership, after which the
                // `CommandProducer` retry actually succeeds. Mirrors Java's
                // `ProducerImpl.connectionOpened` → `lookupRequest` flow.
                //
                // `warn!` per ADR-0054 §2.1: degraded-but-recovering background
                // retry, not surfaced as `Err` to any caller while it retries.
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "producer-open transient error; scheduling lookup + retry"
                );
                let shared_for_retry = shared.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    let topic = shared_for_retry
                        .inner
                        .lock()
                        .producer_topic(handle)
                        .map(str::to_owned);
                    let Some(topic) = topic else { return };
                    if !lookup_then(&shared_for_retry, &topic).await {
                        return;
                    }
                    let request_id = {
                        let mut conn = shared_for_retry.inner.lock();
                        conn.retry_producer_open(handle)
                    };
                    if request_id.is_some() {
                        shared_for_retry.driver_waker.notify_one();
                    }
                });
            }
            ConnectionEvent::SubscribeFailedTransient {
                handle,
                code,
                message,
            } => {
                // `warn!` per ADR-0054 §2.1 — same level rule as the
                // producer-open transient arm above.
                tracing::warn!(
                    ?handle,
                    code,
                    message = crate::log_fields::truncate_broker_str(&message),
                    "consumer-subscribe transient error; scheduling lookup + retry"
                );
                let shared_for_retry = shared.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    let topic = shared_for_retry
                        .inner
                        .lock()
                        .consumer_topic(handle)
                        .map(str::to_owned);
                    let Some(topic) = topic else { return };
                    if !lookup_then(&shared_for_retry, &topic).await {
                        return;
                    }
                    let request_id = {
                        let mut conn = shared_for_retry.inner.lock();
                        conn.retry_consumer_subscribe(handle)
                    };
                    if request_id.is_some() {
                        shared_for_retry.driver_waker.notify_one();
                    }
                });
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

/// Issue a `CommandLookupTopic` and await the broker's `CommandLookupTopicResponse` /
/// `CommandError`. Returns `true` when the lookup landed any outcome (the actual
/// broker disposition is logged but ignored — the caller's next step is a
/// `retry_*` that will re-fail if the bundle is still not served). Used by the
/// transient-error retry path (see #71 + #72) to force the broker to (re)acquire
/// namespace bundle ownership before we re-attach the producer / consumer.
async fn lookup_then(shared: &Arc<ConnectionShared>, topic: &str) -> bool {
    use magnetar_proto::OpOutcome;

    use crate::client::RequestFut;

    let request_id = {
        let mut conn = shared.inner.lock();
        conn.lookup(topic, false)
    };
    shared.driver_waker.notify_one();
    let outcome = RequestFut {
        shared: shared.clone(),
        request_id,
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

/// Default size of the per-connection read buffer. Reads are non-blocking and append-style, so
/// this is just the high-water mark before allocation grows.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

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
            let reason = match &outcome {
                Ok(()) => "connection closed".to_owned(),
                Err(err) => err.to_string(),
            };
            shared.inner.lock().fail_all_pending(&reason);
        }
        // ADR-0059 / follow-ups §4.1: the plain driver is gone for good — latch
        // the no-driver signal so a NEW op issued after this point fast-fails
        // synchronously with `PeerClosed` at the entry-point guards instead of
        // registering a doomed pending op no driver is left to resolve. Set it
        // AFTER `fail_all_pending` so the slot `closed` flags + terminal
        // outcomes are already in place when a fresh op observes the latch.
        shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on `driver_waker` rather than the waker slab.
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
        // reconnect never reaches this point. New ops issued after this fast
        // fail at the entry-point guards. Set AFTER `fail_all_pending`.
        driver_shared.mark_no_driver();
        // Wake event-stream waiters (ProducerReadyFut / SubscribeAckedFut) that
        // park on `driver_waker` rather than the waker slab.
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
        // geometrically up to `max_backoff`. The attempt counter is per-cycle (it gates
        // `max_attempts` give-up, not the cadence).
        let backoff = backoff.get_or_insert_with(|| cfg.build_backoff(seed));
        if cfg.should_reset_backoff(socket_alive_since.elapsed()) {
            backoff.reset();
        }
        let mut attempt: u32 = 0;

        // Reconnect loop — keep trying until we land a fresh socket + handshake OR exhaust
        // `max_attempts`.
        let new_socket = loop {
            let delay = backoff.next();
            tokio::time::sleep(delay).await;

            attempt = attempt.saturating_add(1);
            if let Some(max) = cfg.max_attempts {
                if attempt > max {
                    tracing::warn!(
                        attempt,
                        max_attempts = max,
                        "supervisor: gave up; reconnect attempt budget exhausted"
                    );
                    return last_inner_result;
                }
            }

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
                    tracing::info!(
                        attempt,
                        host = %host,
                        port,
                        "supervisor: reconnected to broker"
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
        shared.driver_waker.notify_one();

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

/// The per-socket driver loop.
///
/// Implementation notes:
///
/// - **Lock discipline**: every interaction with `magnetar_proto::Connection` happens inside a
///   `parking_lot::Mutex::lock()` critical section. Critical sections are short — they never
///   `.await`.
/// - **Write path**: we drain outbound bytes from the state machine into an owned `Vec<u8>`,
///   release the lock, then `write_all` the entire buffer to the socket. The state machine queues
///   additional frames as user futures call `send`/`ack`/etc.; the driver picks them up on the next
///   loop iteration after the `driver_waker` notification.
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

    loop {
        // Drain outbound bytes + check if the state machine wants us to terminate.
        // `poll_transmit` already calls `Connection::drain_producer_outbound`
        // internally to merge per-slot staged frames (queued by
        // `Producer::send` without taking the global lock — ADR-0038 Phase 3)
        // into the connection-wide outbound buffer before returning the byte
        // slice for the driver to flush.
        let (write_data, deadline, should_close) = {
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
        };

        // Flush whatever the state machine produced. This happens *outside* the lock so user
        // futures can keep enqueuing while we hold the network handle.
        //
        // `flush_after_write` is `true` for TLS transports (the rustls layer
        // buffers plaintext until `flush()` actually emits the record) and for
        // unknown / generic-socket spawn paths (conservative default). For
        // plaintext TCP it's `false` — kernel-buffered `write_all` already
        // pushes the bytes to the socket and there's no user-space buffer to
        // drain, so the extra `flush()` is wasted syscall overhead.
        if !write_data.is_empty() {
            let write_result = match &write_data {
                magnetar_proto::TransmitOwned::Contiguous(buf) => {
                    tracing::trace!(bytes = buf.len(), "writing outbound bytes (contiguous)");
                    socket.write_all(buf).await
                }
                magnetar_proto::TransmitOwned::Vectored(segs) => {
                    let total: usize = segs.iter().map(bytes::Bytes::len).sum();
                    tracing::trace!(bytes = total, "writing outbound bytes (vectored)");
                    write_all_vectored(socket, segs).await
                }
            };
            if let Err(err) = write_result {
                shared.inner.lock().mark_disconnected();
                return Err(err.into());
            }
            if flush_after_write {
                if let Err(err) = socket.flush().await {
                    shared.inner.lock().mark_disconnected();
                    return Err(err.into());
                }
            }
        }

        if should_close {
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
            biased;
            // Driver wake-up from user-facing futures (e.g. a freshly-enqueued send).
            () = shared.driver_waker.notified() => {
                // Loop: poll_transmit will drain whatever the future enqueued.
            }

            // Inbound bytes.
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
                        let (n_p, n_c) = {
                            let mut conn = shared.inner.lock();
                            let producers = conn.rebuild_producers();
                            let consumers = conn.rebuild_consumers();
                            (producers.len(), consumers.len())
                        };
                        tracing::info!(
                            producers = n_p,
                            consumers = n_c,
                            "supervisor: replayed producer + consumer state on reconnect"
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
                // pulsed via `driver_waker.notify_waiters()` below so they re-poll
                // and observe the freshly-pushed event.
                handle_pending_events(shared)?;
                shared.driver_waker.notify_waiters();
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

    use bytes::Bytes;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::write_all_vectored;

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
