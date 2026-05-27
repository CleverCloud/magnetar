// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: the broker accepts our TCP dial, lets us write
//! `CommandConnect`, then *partitions* — the server-side never replies and
//! never closes. The supervised connection's handshake driver must surface a
//! recoverable error so the runtime supervisor can retry against a fresh
//! transport.
//!
//! Why this is moonpool territory and not a `testcontainers` test: a
//! containerised broker cannot half-drop the handshake mid-way without
//! injecting an OS-level network blocker (`iptables`, `tc netem`). The
//! sans-io state machine + a synthetic transport substitute it deterministically.
//!
//! ## Shape
//!
//! 1. Drive the [`Connection`](magnetar_proto::Connection) state machine to `Uninitialized →
//!    AwaitingConnected` by calling [`Connection::begin_handshake`].
//! 2. Drain the resulting outbound `CommandConnect` bytes via [`Connection::poll_transmit`]; the
//!    moonpool driver loop would have written these to the wire.
//! 3. *Never feed back* a `CommandConnected` — that's the partition. Confirm:
//!    - [`Connection::state`] stays in the awaiting-connected state,
//!    - [`Connection::is_connected`] is `false`,
//!    - the driver would surface this to the supervisor as
//!      [`magnetar_runtime_moonpool::EngineError::PeerClosed`] once the transport's `read_buf`
//!      returns 0 (the recoverable "peer closed before CONNECTED" envelope already used by
//!      [`MoonpoolEngine::connect_plain`]).
//! 4. Simulate the supervisor's "retry on fresh socket" path: call [`Connection::reset`] (the same
//!    hook the runtime `supervised_driver_loop` uses between disconnects) and re-run the handshake.
//!    This time feed back a `CommandConnected` — the connection must reach `Connected`.
//!
//! Pins the contract that the sans-io layer does not get wedged after a
//! mid-handshake partition: the supervisor's `reset + begin_handshake +
//! handle_bytes(Connected)` loop is the recovery path, and any future
//! refactor that hides one of those hooks surfaces here too.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, HandshakeState};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

#[test]
fn mid_handshake_partition_keeps_state_machine_recoverable() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let t0 = Instant::now();

    // 1. Kick the handshake. The state machine queues a `CommandConnect` in its outbound buffer.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("begin_handshake");
        assert!(
            !conn.is_connected(),
            "freshly-started handshake must not be Connected"
        );
        assert_ne!(conn.state(), HandshakeState::Connected);
    }

    // 2. Drain the outbound bytes — what the engine would have shipped over `Transport::write_all`.
    //    We discard them: in a partition the broker never received them anyway.
    {
        let mut conn = shared.inner.lock();
        let n = conn.poll_transmit().len();
        assert!(
            n > 0,
            "begin_handshake must have queued a CommandConnect (got {n} bytes)"
        );
    }

    // 3. Partition: pretend the broker neither replies nor closes. Time advances; nothing happens.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + Duration::from_secs(30));
        assert!(
            !conn.is_connected(),
            "Connection must not advance to Connected without a CommandConnected frame"
        );
    }

    // 4. Supervisor's recovery: `reset` clears the half-handshake state, `begin_handshake`
    //    re-issues `CommandConnect`, and a synthetic `CommandConnected` flips us to Connected. This
    //    is the exact flow `supervised_driver_loop` runs between disconnects, modulo the real
    //    socket.
    {
        let mut conn = shared.inner.lock();
        let epoch_before = conn.session_epoch();
        conn.reset();
        assert_eq!(
            conn.session_epoch(),
            epoch_before.wrapping_add(1),
            "reset must bump the session epoch so callers can detect the new session",
        );
        // After reset the state machine is back at `Uninitialized` and a
        // fresh `begin_handshake` is required.
        conn.begin_handshake().expect("re-handshake");

        // Drain the second CommandConnect — the driver would have written
        // this over the freshly-dialed socket.
        let n = conn.poll_transmit().len();
        assert!(n > 0, "re-handshake must queue a fresh CommandConnect");

        // The broker on the new socket replies. We feed the Connected frame
        // back and confirm the state machine completes.
        let connected: BytesMut = handshake_response_bytes();
        conn.handle_bytes(t0 + Duration::from_secs(31), &connected)
            .expect("handle Connected on retry");
        assert!(
            conn.is_connected(),
            "supervisor's reset + re-handshake must reach Connected on the new transport"
        );
    }
}
