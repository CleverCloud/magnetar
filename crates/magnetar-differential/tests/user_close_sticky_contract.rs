// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O contracts for a user close against the broker's late handshake frames,
//! executed in the simulation coverage domain.
//!
//! Both runtimes leave the handshake state to the shared `Connection`, and a user close is
//! sticky there (ADR-0134): a `CommandConnected` or a `CommandAuthChallenge` that arrives after
//! `close()` answers a handshake nobody waits for, so it must neither reopen the connection nor
//! surface an event.
//! Both runtime drivers stop reading once a close has nothing left to write, so no runtime
//! scenario delivers such a frame deterministically; these contracts feed it to the `Connection`
//! through its public API, the way the broker would.

use std::time::{Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, HandshakeState, encode_command, pb,
};

fn feed(conn: &mut Connection, command: &pb::BaseCommand) {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode broker command");
    conn.handle_bytes(Instant::now(), &bytes)
        .expect("handle broker command");
}

fn connected() -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    }
}

fn auth_challenge() -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::AuthChallenge as i32,
        auth_challenge: Some(pb::CommandAuthChallenge {
            challenge: Some(pb::AuthData {
                auth_method_name: Some("token".to_owned()),
                auth_data: Some(Bytes::from_static(b"challenge")),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A connection that completed its handshake and that the user then closed.
fn closed_connection() -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    feed(&mut conn, &connected());
    assert_eq!(conn.state(), HandshakeState::Connected);
    while conn.poll_event().is_some() {}
    conn.close();
    let _ = conn.poll_transmit();
    while conn.poll_event().is_some() {}
    assert!(
        conn.is_user_closed(),
        "precondition: the user closed the connection"
    );
    conn
}

/// The events a frame left behind, by kind.
fn handshake_events(conn: &mut Connection) -> (usize, usize) {
    let (mut connected, mut challenges) = (0, 0);
    while let Some(event) = conn.poll_event() {
        match event {
            ConnectionEvent::Connected { .. } => connected += 1,
            ConnectionEvent::AuthChallenge { .. } => challenges += 1,
            _ => {}
        }
    }
    (connected, challenges)
}

/// A `CommandConnected` after `close()` leaves the connection closed and surfaces no `Connected`
/// event; the feature flags it carries are not taken either.
#[test]
fn late_connected_after_close_is_dropped_whole() {
    let mut conn = closed_connection();
    let mut late = connected();
    if let Some(connected) = late.connected.as_mut() {
        connected.feature_flags = Some(pb::FeatureFlags {
            supports_topic_watchers: Some(true),
            ..Default::default()
        });
    }
    feed(&mut conn, &late);
    assert!(
        conn.is_user_closed(),
        "a CONNECTED after close() must not reopen the connection (state is now {:?})",
        conn.state()
    );
    assert_eq!(
        handshake_events(&mut conn),
        (0, 0),
        "no event for a refused CONNECTED"
    );
    assert_eq!(
        conn.feature_flags(),
        &pb::FeatureFlags::default(),
        "a refused CONNECTED must not replace the negotiated feature flags"
    );
}

/// A `CommandAuthChallenge` after `close()` leaves the connection closed and surfaces no
/// `AuthChallenge` event, so nothing answers it back into `Connected`.
#[test]
fn late_auth_challenge_after_close_is_dropped_whole() {
    let mut conn = closed_connection();
    feed(&mut conn, &auth_challenge());
    assert!(
        conn.is_user_closed(),
        "a challenge after close() must not reopen the connection (state is now {:?})",
        conn.state()
    );
    assert_eq!(
        handshake_events(&mut conn),
        (0, 0),
        "no event for a refused challenge"
    );
}
