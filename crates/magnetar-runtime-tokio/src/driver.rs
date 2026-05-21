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
//! 4. reconnects via [`crate::transport::Transport::connect`], calls
//!    [`magnetar_proto::Connection::reset`] (which fails every in-flight op with
//!    [`magnetar_proto::OpOutcome::SessionLost`]), restarts the handshake, and resumes step 1.
//!
//! Stage 3 (producer / consumer state replay) wires in here too: after the new socket completes
//! its handshake, the inner loop calls [`magnetar_proto::Connection::rebuild_producers`] and
//! [`magnetar_proto::Connection::rebuild_consumers`], which re-emit every still-open producer's
//! `CommandProducer` (with a bumped `epoch`) and every still-open consumer's `CommandSubscribe`
//! plus `CommandFlow` (resuming from `last_acked_message_id` when known). In-flight publishes
//! severed by the reset still surface `SessionLost` — full at-least-once replay is follow-up
//! work.
//!
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::ConnectionEvent;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::ConnectionShared;
use crate::error::ClientError;
use crate::transport::Transport;
use crate::url_parse::ParsedUrl;

/// Drain the connection's semantic event queue and react to events that need
/// runtime-layer work (currently only `AuthChallenge` — every other event is
/// handled inline by the sans-io layer's per-future Waker dispatch).
fn handle_pending_events(shared: &Arc<ConnectionShared>) -> Result<(), ClientError> {
    loop {
        let event = shared.inner.lock().poll_event();
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
                    return Err(ClientError::Other(
                        "broker requested AUTH_CHALLENGE but client has no auth provider"
                            .to_owned(),
                    ));
                };
                let bytes = challenge.unwrap_or_default();
                let refreshed = provider
                    .respond_to_challenge(&bytes)
                    .map_err(|err| ClientError::Other(format!("auth refresh failed: {err}")))?;
                let method = provider.method().to_owned();
                shared
                    .inner
                    .lock()
                    .submit_auth_response(refreshed.to_vec(), Some(method));
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
            _ => {}
        }
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
    let join = tokio::spawn(async move {
        let mut socket = socket;
        driver_loop_inner(&shared, &mut socket).await
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
    let join = tokio::spawn(supervised_driver_loop(shared, socket, reconnect_ctx));
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

    // First pass uses the current socket. The inner-loop result is what we propagate to the
    // caller if we exit without a supervisor reconnect.
    let mut last_inner_result = driver_loop_inner(&shared, &mut socket).await;

    loop {
        // User-requested close beats reconnect — the state machine is already in `Closing`
        // or `Closed`, so we propagate the inner result (Ok or Err) as-is.
        if shared.inner.lock().is_closed() {
            return last_inner_result;
        }

        // Snapshot the supervisor config + max-attempts on every iteration so dynamic updates
        // to it (future work) take effect before the next reconnect.
        let supervisor_cfg = shared.inner.lock().supervisor_config().cloned();
        let Some(cfg) = supervisor_cfg else {
            return last_inner_result;
        };

        // Fresh Backoff per disconnect: Java resets the schedule on a successful reconnect, so
        // we reset on a *successful* handshake too. The attempt counter is the only piece of
        // state that survives across reconnect attempts here.
        let mut backoff = cfg.build_backoff(seed);
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
                        "supervisor: gave up after {attempt} reconnect attempt(s) \
                         (max_attempts={max})"
                    );
                    return last_inner_result;
                }
            }

            // Did the user request close while we were sleeping?
            if shared.inner.lock().is_closed() {
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
                                    "supervisor: service-url provider returned an unparseable URL \
                                 {raw:?} on attempt {attempt}: {err}; falling back to the \
                                 cached URL"
                                );
                                std::borrow::Cow::Borrowed(&reconnect_ctx.url)
                            }
                        }
                    }
                    None => std::borrow::Cow::Borrowed(&reconnect_ctx.url),
                };

            match Transport::connect(&target_url, reconnect_ctx.tls_config.clone()).await {
                Ok(t) => break t,
                Err(err) => {
                    let (host, port) = target_url.socket_addr();
                    tracing::warn!(
                        "supervisor: reconnect attempt {attempt} failed (url={host}:{port}): \
                         {err}; will retry"
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
                tracing::error!("supervisor: begin_handshake after reset failed: {err}");
                return Err(err.into());
            }
        }
        shared
            .pending_rebuild
            .store(true, std::sync::atomic::Ordering::SeqCst);
        shared.driver_waker.notify_one();

        socket = new_socket;
        last_inner_result = driver_loop_inner(&shared, &mut socket).await;
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
) -> Result<(), ClientError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);
    let mut write_buf: Vec<u8> = Vec::with_capacity(READ_BUFFER_CAPACITY);

    loop {
        // Drain outbound bytes + check if the state machine wants us to terminate.
        let (deadline, should_close) = {
            let mut conn = shared.inner.lock();
            write_buf.clear();
            let _ = conn.poll_transmit(&mut write_buf);
            let dl = conn.poll_timeout();
            let closing = matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Closing
                    | magnetar_proto::HandshakeState::Closed
                    | magnetar_proto::HandshakeState::Failed
            );
            (dl, closing)
        };

        // Flush whatever the state machine produced. This happens *outside* the lock so user
        // futures can keep enqueuing while we hold the network handle.
        if !write_buf.is_empty() {
            if let Err(err) = socket.write_all(&write_buf).await {
                shared.inner.lock().mark_disconnected();
                return Err(err.into());
            }
            if let Err(err) = socket.flush().await {
                shared.inner.lock().mark_disconnected();
                return Err(err.into());
            }
            write_buf.clear();
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
                    shared.inner.lock().mark_disconnected();
                    return Err(ClientError::PeerClosed);
                }
                let bytes = read_buf.split().freeze();
                let now = Instant::now();
                if let Err(err) = shared.inner.lock().handle_bytes(now, &bytes) {
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
                // After handling bytes, drain semantic events. Most go to per-future Wakers
                // already (the sans-io layer wakes them inline), but `AuthChallenge` requires
                // the runtime to invoke the configured AuthProvider and submit the response.
                // PIP-30 / PIP-292.
                handle_pending_events(shared)?;
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
