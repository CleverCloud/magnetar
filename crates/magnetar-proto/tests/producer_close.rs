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

use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, PendingOpKey, decode_one, pb,
};

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

/// Feed a `CommandSuccess` correlating to `request_id` into the
/// connection — the broker's close ack.
fn ack_success(conn: &mut Connection, request_id: u64, now: Instant) {
    let ack = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &ack).expect("encode Success");
    conn.handle_bytes(now, &buf).expect("apply Success");
}

/// Feed a `CommandError` correlating to `request_id` into the
/// connection — the broker rejecting the close.
fn ack_error(conn: &mut Connection, request_id: u64, now: Instant) {
    let err = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::ServiceNotReady as i32,
            message: "synthetic close rejection".to_owned(),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &err).expect("encode Error");
    conn.handle_bytes(now, &buf).expect("apply Error");
}

/// `close_producer_forget` (the drop guard's entry) must NOT record an
/// `OpOutcome` when the broker acks the close: no waiter will ever drain
/// it, so a recorded outcome would leak one permanent `outcomes` entry
/// per dropped producer on a long-lived connection — unbounded growth
/// under issue #241's continuous LRU-eviction workload.
#[test]
fn close_producer_forget_records_no_outcome_on_success() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/forget-success".to_owned(),
        ..Default::default()
    });
    let slot = conn.producer(handle).expect("slot exists").clone();

    let request_id = conn.close_producer_forget(handle);
    assert!(
        slot.state.lock().closed,
        "forget variant must still flip the closed flag synchronously"
    );

    ack_success(&mut conn, request_id.0, now);

    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none(),
        "fire-and-forget close ack must be consumed in-place, not recorded"
    );
}

/// A broker *rejection* of the fire-and-forget close must not record an
/// `OpOutcome` either (same leak), and is surfaced as a `warn!` instead
/// of being silently swallowed.
#[test]
fn close_producer_forget_records_no_outcome_on_broker_error() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/forget-error".to_owned(),
        ..Default::default()
    });

    let request_id = conn.close_producer_forget(handle);
    ack_error(&mut conn, request_id.0, now);

    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none(),
        "rejected fire-and-forget close must not leak an OpOutcome entry"
    );
}

/// Contrast: the awaited `close_producer` path must keep recording its
/// outcome — that entry is exactly what the engines' `RequestFut`
/// drains via `take_outcome`.
#[test]
fn close_producer_awaited_still_records_outcome() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/awaited-close".to_owned(),
        ..Default::default()
    });

    let request_id = conn.close_producer(handle);
    ack_success(&mut conn, request_id.0, now);

    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_some(),
        "awaited close must record the outcome its RequestFut drains"
    );
}
