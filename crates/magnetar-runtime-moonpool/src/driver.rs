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
//! 5. dispatch semantic events that need runtime-layer work (`AuthChallenge`, `TopicListChanged`).
//!
//! The driver does **not** wake user-facing futures itself — the sans-io
//! layer does that when an `OpOutcome` lands. See
//! [GUIDELINES.md] §"No-channels rule".
//!
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md
//! [`Transport`]: crate::transport::Transport

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::ConnectionEvent;
use moonpool_core::{Providers, TaskProvider, TimeProvider};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::transport::Transport;
use crate::{ConnectionShared, EngineError, TopicListChange};

/// Default size of the per-connection read buffer. Reads are non-blocking
/// and append-style, so this is just the high-water mark before allocation
/// grows.
const READ_BUFFER_CAPACITY: usize = 64 * 1024;

/// Drain the connection's semantic event queue and react to events that need
/// runtime-layer work. Most events (`Connected`, `SendReceipt`, `Message`,
/// …) are routed back to user-facing futures by the sans-io layer itself
/// through `Waker` slabs; only `AuthChallenge` and `TopicListChanged`
/// require the driver to do anything.
fn handle_pending_events(shared: &Arc<ConnectionShared>) -> Result<(), EngineError> {
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
                    .submit_auth_response(refreshed.to_vec(), Some(method));
                shared.driver_waker.notify_one();
            }
            ConnectionEvent::TopicListChanged { added, removed } => {
                shared
                    .topic_list_changes
                    .lock()
                    .push_back(TopicListChange { added, removed });
                shared.topic_list_notify.notify_waiters();
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
        let outcome = driver_loop::<P>(shared, transport, time).await;
        *result_for_task.result.lock() = Some(outcome);
        result_for_task.done.notify_waiters();
    });
    DriverHandle { join, result }
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
pub(crate) async fn driver_loop<P>(
    shared: Arc<ConnectionShared>,
    mut transport: Transport<P>,
    time: P::Time,
) -> Result<(), EngineError>
where
    P: Providers,
{
    let mut read_buf = BytesMut::with_capacity(READ_BUFFER_CAPACITY);
    let mut write_buf: Vec<u8> = Vec::with_capacity(READ_BUFFER_CAPACITY);

    loop {
        // 1. Drain outbound bytes + check if the state machine wants us to terminate.
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
            write_buf.clear();
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
                let bytes = read_buf.split().freeze();
                let now = Instant::now();
                if let Err(err) = shared.inner.lock().handle_bytes(now, &bytes) {
                    shared.inner.lock().mark_disconnected();
                    return Err(err.into());
                }
                handle_pending_events(&shared)?;
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
