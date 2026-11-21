// SPDX-License-Identifier: Apache-2.0

//! A user-requested `Connection::close()` is sticky: a later transport
//! failure must not turn it back into a redialable transport drop.
//!
//! `Connection::close()` records the user's intent only as the handshake
//! state `Closing`. The supervised driver loop decides "exit cleanly" versus
//! "redial" from `is_user_closed()`, which is true for `Closing` / `Closed`
//! and false for `Failed`. Every transport error arm of the runtime driver
//! loops (read error, peer EOF, decode error, write-deadline expiry) calls
//! `mark_disconnected()`, and a close that is still draining its write queue
//! races exactly those arms. If `mark_disconnected()` demotes `Closing` to
//! `Failed`, the supervisor redials, `reset()`s and re-attaches every
//! producer and consumer, and `Client::close().await` never returns.
//!
//! Layer (a) of the ADR-0024 four-layer test policy for the user-close
//! stickiness defect.

use std::time::Instant;

use magnetar_proto::{Connection, ConnectionConfig, HandshakeState, pb};

fn new_connection() -> Connection {
    Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    )
}

/// A connection that has completed the CONNECT / CONNECTED handshake.
fn connected(now: Instant) -> Connection {
    let mut conn = new_connection();
    conn.begin_handshake().expect("begin_handshake");
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
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &connected).expect("encode connected");
    conn.handle_bytes(now, &buf).expect("apply connected");
    let _ = conn.poll_transmit();
    assert_eq!(conn.state(), HandshakeState::Connected);
    conn
}

/// The transport dies while the user's close is still draining (the driver
/// has not yet written everything it owes the socket): the socket error arm
/// calls `mark_disconnected()`. The user's close must survive it, because
/// the supervisor's only stop signal is `is_user_closed()`.
#[test]
fn transport_drop_after_close_keeps_the_user_close() {
    let mut conn = connected(Instant::now());
    conn.close();
    assert!(conn.is_user_closed(), "close() records the user's intent");

    conn.mark_disconnected();

    assert!(
        conn.is_user_closed(),
        "a transport failure after close() must not demote the user close to a \
         redialable `Failed` (state is now {:?}); the supervised driver would \
         redial and Client::close() would never return",
        conn.state(),
    );
}

/// The user closes while the handshake is still in flight (CONNECT sent, no
/// CONNECTED yet), and the broker's CONNECTED is already on the wire. A late
/// CONNECTED must not resurrect a connection the user has closed: the
/// supervised driver and every `is_closed()` guard read the state.
#[test]
fn late_connected_after_close_keeps_the_user_close() {
    let mut conn = new_connection();
    conn.begin_handshake().expect("begin_handshake");
    let _ = conn.poll_transmit();
    assert_eq!(conn.state(), HandshakeState::ConnectSent);

    conn.close();
    assert!(conn.is_user_closed(), "close() records the user's intent");

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
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &connected).expect("encode connected");
    // Whether the late frame is applied, ignored or refused is the fix's
    // choice; only the user close it must not undo is asserted.
    let _ = conn.handle_bytes(Instant::now(), &buf);

    assert!(
        conn.is_user_closed(),
        "a CONNECTED that arrives after close() must not reopen the connection \
         (state is now {:?})",
        conn.state(),
    );
}
