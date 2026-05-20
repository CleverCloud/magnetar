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
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::ConnectionEvent;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::ConnectionShared;
use crate::error::ClientError;

/// Drain the connection's semantic event queue and react to events that need
/// runtime-layer work (currently only `AuthChallenge` — every other event is
/// handled inline by the sans-io layer's per-future Waker dispatch).
fn handle_pending_events(shared: &Arc<ConnectionShared>) -> Result<(), ClientError> {
    loop {
        let event = shared.inner.lock().poll_event();
        let Some(event) = event else {
            return Ok(());
        };
        if let ConnectionEvent::AuthChallenge {
            method: _,
            challenge,
        } = event
        {
            let Some(provider) = shared.auth_provider.clone() else {
                tracing::warn!(
                    "broker requested in-band auth refresh but no AuthProvider configured; \
                     the connection will be reset"
                );
                return Err(ClientError::Other(
                    "broker requested AUTH_CHALLENGE but client has no auth provider".to_owned(),
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

/// Spawn the driver loop on the current tokio runtime.
pub(crate) fn spawn<S>(shared: Arc<ConnectionShared>, socket: S) -> DriverHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let join = tokio::spawn(driver_loop(shared, socket));
    DriverHandle { join }
}

/// The driver loop.
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
pub(crate) async fn driver_loop<S>(
    shared: Arc<ConnectionShared>,
    mut socket: S,
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
            socket.write_all(&write_buf).await?;
            socket.flush().await?;
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
                let n = r?;
                if n == 0 {
                    // Peer closed cleanly. Surface as a typed error so the producer/consumer
                    // futures wake up with something actionable.
                    return Err(ClientError::PeerClosed);
                }
                let bytes = read_buf.split().freeze();
                let now = Instant::now();
                shared.inner.lock().handle_bytes(now, &bytes)?;
                // After handling bytes, drain semantic events. Most go to per-future Wakers
                // already (the sans-io layer wakes them inline), but `AuthChallenge` requires
                // the runtime to invoke the configured AuthProvider and submit the response.
                // PIP-30 / PIP-292.
                handle_pending_events(&shared)?;
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
