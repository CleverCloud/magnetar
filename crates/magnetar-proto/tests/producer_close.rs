// SPDX-License-Identifier: Apache-2.0

//! `Connection::close_producer` slot-state contract.
//!
//! The runtime engines' last-clone drop guard (`ProducerCloseGuard`)
//! relies on two synchronous properties of `close_producer`:
//!
//! 1. the per-slot `closed` flag flips **before** the call returns, so a later guard run observes
//!    it and skips the duplicate close;
//! 2. a `CommandCloseProducer` frame is staged on the connection buffer, so waking the driver is
//!    enough to push it to the broker.
//!
//! Layer (a) of the ADR-0024 four-layer test policy for the
//! producer-drop close (issue #241).

use std::time::Instant;

use magnetar_proto::{Connection, ConnectionConfig, CreateProducerRequest, decode_one, pb};

/// Construct a `Connection` whose state machine has cleared the
/// handshake so `create_producer` runs cleanly.
fn handshake_complete(now: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
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
    // Drain the bytes the connection produced during handshake.
    let _ = conn.poll_transmit();
    conn
}

/// Decode every staged frame and collect the command types.
fn drain_command_types(conn: &mut Connection) -> Vec<i32> {
    let mut bytes = conn.poll_transmit();
    let mut kinds = Vec::new();
    while !bytes.is_empty() {
        let frame = decode_one(&mut bytes).expect("staged frame must decode");
        kinds.push(frame.command.r#type);
    }
    kinds
}

/// `close_producer` flips the per-slot `closed` flag synchronously —
/// before the call returns, not on the broker ack. The runtime drop
/// guard reads this flag to decide whether a best-effort close is still
/// needed; were the flip deferred to the ack, an explicit
/// `close().await` followed by the last-clone drop would enqueue a
/// duplicate `CloseProducer`.
#[test]
fn close_producer_marks_slot_closed_synchronously() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/close-sync".to_owned(),
        ..Default::default()
    });
    let slot = conn.producer(handle).expect("slot exists").clone();
    assert!(!slot.state.lock().closed, "fresh producer must be open");

    let _request_id = conn.close_producer(handle);

    assert!(
        slot.state.lock().closed,
        "closed flag must flip synchronously inside close_producer"
    );
}

/// `close_producer` stages a `CommandCloseProducer` frame on the
/// connection buffer synchronously — the drop guard only has to wake
/// the driver for the frame to reach the broker.
#[test]
fn close_producer_stages_close_frame() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/close-frame".to_owned(),
        ..Default::default()
    });
    // Drain the staged CommandProducer open frame first.
    let _ = conn.poll_transmit();

    let _request_id = conn.close_producer(handle);

    let kinds = drain_command_types(&mut conn);
    assert_eq!(
        kinds,
        vec![pb::base_command::Type::CloseProducer as i32],
        "close_producer must stage exactly one CloseProducer frame"
    );
}
