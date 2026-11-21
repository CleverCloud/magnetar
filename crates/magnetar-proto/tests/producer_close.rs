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

use std::time::{Duration, Instant};

use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, decode_one, pb,
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

#[test]
fn close_producer_forget_records_no_outcome_on_reset() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/forget-reset".to_owned(),
        ..Default::default()
    });

    let request_id = conn.close_producer_forget(handle);
    conn.reset();

    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none(),
        "reset must not materialize an outcome for a forgotten close"
    );
}

#[test]
fn close_producer_forget_records_no_outcome_on_fail_all_pending() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/forget-fail-all".to_owned(),
        ..Default::default()
    });

    let request_id = conn.close_producer_forget(handle);
    conn.fail_all_pending("synthetic terminal drop");

    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none(),
        "fail_all_pending must not materialize an outcome for a forgotten close"
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

/// Which close entry point the producer used.
#[derive(Clone, Copy, Debug)]
enum Close {
    /// `Producer::close()`: the awaited `CommandCloseProducer`.
    Awaited,
    /// The last-clone drop guard: the fire-and-forget close.
    Forgotten,
}

/// Where the producer close lands relative to the transport loss.
#[derive(Clone, Copy, Debug)]
enum CloseAt {
    /// The close is unacknowledged when the session resets, so the broker's ack can never arrive:
    /// the reset itself resolves the send, and the outcome is read right after it.
    BeforeReset,
    /// The close is issued on the new session, after the handshake and before the producers are
    /// re-attached.
    BetweenResetAndRebuild,
}

/// A fresh `CONNECTED` frame, for the handshake that follows `reset()`.
fn connected_frame() -> bytes::BytesMut {
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
    buf
}

/// The broker's `CommandProducerSuccess` for `request_id`.
fn ack_producer_success(conn: &mut Connection, request_id: u64, now: Instant) {
    let ack = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "magnetar-test-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &ack).expect("encode ProducerSuccess");
    conn.handle_bytes(now, &buf).expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
}

/// One unbatched send is in flight, its producer is closed, and the transport drops. Returns the
/// outcome the send's future would find once the supervisor has re-handshaked, re-attached the
/// producers and the producer's `send_timeout` (30 s) has long elapsed.
fn send_outcome_after_close_and_reset(close: Close, at: CloseAt) -> Option<OpOutcome> {
    let t0 = Instant::now();
    let mut conn = handshake_complete(t0);
    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/close-then-reset".to_owned(),
        send_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    ack_producer_success(&mut conn, create_rid, t0);

    let seq = conn
        .send(
            producer,
            magnetar_proto::producer::OutgoingMessage {
                payload: bytes::Bytes::from_static(b"in flight at close"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 18,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send");
    let _ = conn.poll_transmit();

    let close_producer = |conn: &mut Connection| match close {
        Close::Awaited => conn.close_producer(producer),
        Close::Forgotten => conn.close_producer_forget(producer),
    };
    if matches!(at, CloseAt::BeforeReset) {
        close_producer(&mut conn);
    }

    // The transport drops before the broker acknowledged the close: the supervisor resets the
    // connection, redials and re-handshakes.
    conn.reset();
    // A close the reset took is resolved by the reset itself: read the outcome there, before the
    // rebuild below could resolve it another way.
    if matches!(at, CloseAt::BeforeReset) {
        return conn.take_outcome(PendingOpKey::Send(producer, seq));
    }
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("apply reconnect CONNECTED");
    let _ = conn.poll_transmit();

    if matches!(at, CloseAt::BetweenResetAndRebuild) {
        close_producer(&mut conn);
    }

    // The driver re-attaches every producer once the handshake completes. The closed one is not
    // re-attached, and no `CommandCloseProducer` ack will ever come for its in-flight send.
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();

    // Even with `send_timeout` configured and long elapsed, the send must resolve.
    conn.handle_timeout(t0 + Duration::from_secs(3600));
    conn.take_outcome(PendingOpKey::Send(producer, seq))
}

/// Whether `outcome` resolves a send's future with a failure (the producer is gone, so no receipt
/// can ever arrive).
fn resolves_the_send_with_a_failure(outcome: Option<&OpOutcome>) -> bool {
    matches!(
        outcome,
        Some(OpOutcome::Terminal { .. } | OpOutcome::SendError { .. })
    )
}

/// A send that is in flight when its producer is closed must not hang when the session resets
/// before the broker acknowledges the close.
///
/// `reset()` takes the pending close (the ack can no longer arrive), so it fails the send itself:
/// it runs `fail_sends_of_closed_producer` for every close request it takes. This case and its
/// forgotten-close twin guard that path; a close issued after the reset is
/// `send_relocated_by_reset_resolves_when_the_producer_is_closed_before_the_rebuild`.
#[test]
fn send_in_flight_at_awaited_close_resolves_when_the_session_resets_before_the_close_ack() {
    let outcome = send_outcome_after_close_and_reset(Close::Awaited, CloseAt::BeforeReset);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send in flight at an awaited producer close must resolve with Terminal or SendError \
         at the reset that takes the close, before the rebuild \
         (got {outcome:?})"
    );
}

/// Same defect through the engines' last-clone drop guard, which uses the fire-and-forget close.
#[test]
fn send_in_flight_at_forgotten_close_resolves_when_the_session_resets_before_the_close_ack() {
    let outcome = send_outcome_after_close_and_reset(Close::Forgotten, CloseAt::BeforeReset);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send in flight at a forgotten producer close must resolve with Terminal or SendError \
         at the reset that takes the close, before the rebuild \
         (got {outcome:?})"
    );
}

/// Variant: the close is issued after `reset()` and before the producers are re-attached, so the
/// reset snapshots of the now-closed producer are discarded by the rebuild before the close ack can
/// drain them.
#[test]
fn send_relocated_by_reset_resolves_when_the_producer_is_closed_before_the_rebuild() {
    let outcome =
        send_outcome_after_close_and_reset(Close::Awaited, CloseAt::BetweenResetAndRebuild);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send relocated by reset() must resolve with Terminal or SendError when its producer is \
         closed before the rebuild, even once send_timeout elapsed (got {outcome:?})"
    );
}
