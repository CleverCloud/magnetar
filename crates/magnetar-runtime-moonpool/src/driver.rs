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
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::ConnectionEvent;
use moonpool_core::{Providers, TaskProvider, TimeProvider};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::dns::DnsResolver;
use crate::transport::Transport;
use crate::{ConnectionShared, EngineError, ObservedReplicatedSubscriptionMarker, TopicListChange};

/// Default size of the per-connection read buffer. Reads are non-blocking
/// and append-style, so this is just the high-water mark before allocation
/// grows.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

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
fn handle_pending_events(shared: &Arc<ConnectionShared>) -> Result<(), EngineError> {
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
                    | ConnectionEvent::ReplicatedSubscriptionMarkerObserved { .. }
            )
        });
        let Some(event) = event else {
            return Ok(());
        };
        match event {
            ConnectionEvent::AuthChallenge {
                method: _,
                challenge,
            } => {
                let Some(provider) = shared.auth_provider.clone() else {
                    tracing::warn!(
                        "broker requested in-band auth refresh but no AuthProvider configured; \
                         the connection will be reset"
                    );
                    return Err(EngineError::Config(
                        "broker requested AUTH_CHALLENGE but client has no auth provider"
                            .to_owned(),
                    ));
                };
                let bytes = challenge.unwrap_or_default();
                let refreshed = provider
                    .respond_to_challenge(&bytes)
                    .map_err(|err| EngineError::Config(format!("auth refresh failed: {err}")))?;
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
                    new_url = broker_service_url.as_deref(),
                    new_url_tls = broker_service_url_tls.as_deref(),
                    "broker requested PIP-188 topic migration; supervised reconnect will fire"
                );
                return Err(EngineError::Config(
                    "PIP-188: broker requested topic migration; resetting connection".to_owned(),
                ));
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
            _ => {}
        }
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
/// [`DriverHandle::join`] to wait for it.
///
/// Joining is implemented over [`tokio::sync::Notify`] rather than the
/// tokio `JoinHandle` of the spawned task because moonpool's
/// [`TaskProvider::spawn_task`] returns `JoinHandle<()>`. We surface the
/// terminal `Result<(), EngineError>` via a shared slot instead.
pub struct DriverHandle {
    join: JoinHandle<()>,
    result: Arc<DriverResult>,
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

    /// Abort the driver task. The result slot is populated with a
    /// `Config("aborted")` error so callers awaiting [`Self::join`] don't
    /// hang.
    pub fn abort(&self) {
        self.join.abort();
        // Populate the result slot so any pending `join().await` wakes up.
        {
            let mut slot = self.result.result.lock();
            if slot.is_none() {
                *slot = Some(Err(EngineError::Config("driver aborted".to_owned())));
            }
        }
        self.result.done.notify_waiters();
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
    let join = task.spawn_task("magnetar-moonpool-driver", async move {
        let outcome = driver_loop_inner::<P>(shared, transport, time).await;
        *result_for_task.result.lock() = Some(outcome);
        result_for_task.done.notify_waiters();
    });
    DriverHandle { join, result }
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
    let time = providers.time().clone();
    let task = providers.task().clone();
    let join = task.spawn_task("magnetar-moonpool-driver-supervised", async move {
        let outcome =
            supervised_driver_loop::<P>(shared, transport, reconnect_ctx, providers, time).await;
        *result_for_task.result.lock() = Some(outcome);
        result_for_task.done.notify_waiters();
    });
    DriverHandle { join, result }
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

    // `socket_alive_since` lets us decide, once `driver_loop_inner` returns, whether the
    // previous socket lived long enough to count as a stable reconnect (-> `backoff.reset()`)
    // or died inside `drop_grace` (-> keep growing). Uses host-clock `Instant::now()` to
    // pair with the same clock the inner loop already stamps on `handle_bytes` /
    // `ProducerReady`. Determinism for the schedule itself is preserved via `time.sleep()`
    // on the redial path.
    let mut socket_alive_since = Instant::now();
    let mut last_inner_result =
        driver_loop_inner::<P>(shared.clone(), transport, time.clone()).await;

    loop {
        // User-requested close beats reconnect. `Failed` (transport drop, from
        // `mark_disconnected`) deliberately does NOT count here — the supervisor exists
        // precisely to retry after that, so the gate is `is_user_closed()` (Closing /
        // Closed only), mirroring the tokio runtime.
        if shared.inner.lock().is_user_closed() {
            return last_inner_result;
        }

        let supervisor_cfg = shared.inner.lock().supervisor_config().cloned();
        let Some(cfg) = supervisor_cfg else {
            return last_inner_result;
        };

        // ADR-0028: feed a TCP-drop signal into the anti-thrash detector if
        // the socket closed within the supervisor's `drop_grace` of the
        // most-recent successful re-attach. Mirror of the tokio runtime
        // (`crates/magnetar-runtime-tokio/src/driver.rs`). `Instant::now()` is
        // used here for the per-host clock that pairs with the `Instant` the
        // sans-io state machine already stamps on `ProducerReady` /
        // `SubscribeAcked` (driver inner loop, ~`Instant::now()` on
        // `handle_bytes`). The moonpool engine keeps deterministic sleep
        // scheduling via `time.sleep(duration)` below.
        if cfg.anti_thrash_threshold.is_some() {
            let now = Instant::now();
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
            match conn.anti_thrash_tick(Instant::now()) {
                magnetar_proto::AntiThrashDisposition::Cooldown { until } => Some(until),
                magnetar_proto::AntiThrashDisposition::Normal => None,
            }
        };
        if let Some(until) = cooldown_until {
            let now = Instant::now();
            if until > now {
                let dur = until.saturating_duration_since(now);
                tracing::warn!(
                    "supervisor: anti-thrash cooldown engaged; sleeping {dur:?} before next redial"
                );
                let _ = time.sleep(dur).await;
            }
            shared.inner.lock().anti_thrash_state_mut().clear_cooldown();
        }

        // Backoff persistence policy (ADR-0028 alignment): lazy-init on the first redial,
        // then reuse across cycles. `reset()` is gated on the previous socket surviving past
        // `cfg.drop_grace` — sockets that died inside that window count as thrashes, so the
        // schedule keeps growing and successive ProducerReady-then-drop cycles slow down
        // geometrically up to `max_backoff`. The attempt counter is per-cycle (it gates
        // `max_attempts` give-up, not the cadence). Mirror of the tokio runtime.
        let backoff = backoff.get_or_insert_with(|| cfg.build_backoff(seed));
        if cfg.should_reset_backoff(socket_alive_since.elapsed()) {
            backoff.reset();
        }
        let mut attempt: u32 = 0;

        // Reconnect loop — keep trying until we land a fresh socket + handshake OR
        // exhaust `max_attempts`.
        let new_transport = loop {
            let delay = backoff.next();
            // Use the moonpool TimeProvider so sim runs stay deterministic.
            let _ = time.sleep(delay).await;

            attempt = attempt.saturating_add(1);
            if let Some(max) = cfg.max_attempts {
                if attempt > max {
                    tracing::warn!(
                        "supervisor: gave up after {attempt} reconnect attempt(s) \
                         (max_attempts={max})"
                    );
                    return last_inner_result;
                }
            }

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
                            "supervisor: service-url provider returned an unparseable URL \
                         on attempt {attempt}; falling back to the cached host:port"
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
            )
            .await
            {
                Ok(t) => break t,
                Err(err) => {
                    tracing::warn!(
                        "supervisor: reconnect attempt {attempt} failed \
                         (target={target_host_port}): {err}; will retry"
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
                tracing::error!("supervisor: begin_handshake after reset failed: {err}");
                return Err(EngineError::Protocol(err));
            }
        }
        shared
            .pending_rebuild
            .store(true, std::sync::atomic::Ordering::SeqCst);
        shared.driver_waker.notify_one();

        transport = new_transport;
        socket_alive_since = Instant::now();
        last_inner_result = driver_loop_inner::<P>(shared.clone(), transport, time.clone()).await;
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
        //    See the moonpool wall-clock bridge entry in `docs/follow-ups.md`.
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
        //    moonpool's `Transport` does not yet expose a vectored
        //    write primitive (the underlying
        //    `moonpool_core::NetworkProvider::TcpStream` lacks a
        //    chaos-pack-aware `write_vectored`); for now the vectored
        //    arm coalesces locally and falls through to the same
        //    contiguous `write_all`. Once moonpool-core adds vectored
        //    support the chaos pack gains segment-granular drops
        //    (ADR-0040 wave 2's chaos-pack note). The tokio engine
        //    already dispatches `Vectored` via real `writev(2)` —
        //    coalesce-here on moonpool means the *bytes* on the wire
        //    are byte-identical to tokio, only the chaos-pack
        //    fidelity differs.
        let (write_buf, deadline, should_close) = {
            let mut conn = shared.inner.lock();
            let owned = conn.poll_transmit_owned();
            let dl = conn.poll_timeout();
            let closing = matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Closing
                    | magnetar_proto::HandshakeState::Closed
                    | magnetar_proto::HandshakeState::Failed
            );
            let write_buf = match owned {
                magnetar_proto::TransmitOwned::Contiguous(buf) => buf,
                magnetar_proto::TransmitOwned::Vectored(segs) => {
                    let total: usize = segs.iter().map(bytes::Bytes::len).sum();
                    let mut coalesced = bytes::BytesMut::with_capacity(total);
                    for seg in segs {
                        coalesced.extend_from_slice(&seg);
                    }
                    coalesced.freeze()
                }
            };
            (write_buf, dl, closing)
        };

        // 2. Flush whatever the state machine produced. This happens outside the lock so user
        //    futures can keep enqueuing.
        if !write_buf.is_empty() {
            if let Err(err) = transport.write_all(&write_buf).await {
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
        //    `TimeProvider::sleep` takes a `Duration`, not an `Instant`.
        let sleep_dur = deadline.map(|t| t.saturating_duration_since(Instant::now()));

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
                    shared.inner.lock().mark_disconnected();
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
                let now = Instant::now();
                if let Err(err) = shared.inner.lock().handle_bytes_owned(now, chunk) {
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
                handle_pending_events(&shared)?;
                // Wake event-stream-watching futures (e.g. `ProducerReadyFut`)
                // that parked on `driver_waker.notified()` so they re-poll and
                // observe the freshly-pushed event.
                shared.driver_waker.notify_waiters();
            }

            // Timer fired. `sleep_or_pending` only returns once the duration
            // elapses or the time provider shuts down; both are treated as
            // a tick.
            () = sleep_or_pending::<P>(&time, sleep_dur) => {
                shared.inner.lock().handle_timeout(Instant::now());
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
        let err = handle_pending_events(&shared).expect_err("migration must error");
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
