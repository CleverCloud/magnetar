// SPDX-License-Identifier: Apache-2.0

//! An outcome nobody can take is not recorded, identically on both engines (ADR-0024 layer (d)).
//!
//! `Connection::outcomes` holds the outcome of a request-id-correlated operation until a future
//! takes it, and `reset()` does not clear it. `reset()` itself records `SessionLost` for a pending
//! producer open and consumer subscribe, which the runtimes await through events and never through
//! the outcome slab, and the `CommandGetLastMessageIdResponse` arm records its outcome for a
//! request `cancel_request` already dropped. Both engines wrap the same `magnetar-proto` state
//! machine, so they must agree, and neither may keep an entry no caller can ever take.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, RequestId,
    SubscribeRequest, encode_command, pb,
};

/// What the connection holds, as `(open, subscribe)`, for the requests a reset retired.
type RetainedAfterReset = (bool, bool);

/// What the connection holds for a live and a cancelled last-message-id request, as
/// `(live outcome's last ledger id, cancelled request recorded something)`.
type RetainedAfterReply = (Option<u64>, bool);

fn connected_frame() -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    frame
}

fn tokio_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn handshake(conn: &mut Connection, now: Instant) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();
}

fn last_message_id_response(request_id: RequestId) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::GetLastMessageIdResponse as i32,
            get_last_message_id_response: Some(pb::CommandGetLastMessageIdResponse {
                last_message_id: pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: 11,
                    ..Default::default()
                },
                request_id: request_id.0,
                consumer_mark_delete_position: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode GetLastMessageIdResponse");
    frame
}

/// Resets the session with one producer open and one subscribe in flight, and reports whether the
/// connection kept an outcome for either.
fn retained_after_reset(conn: &mut Connection, now: Instant) -> RetainedAfterReset {
    handshake(conn, now);

    let open = RequestId(conn.peek_next_request_id_for_test());
    let _producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reset-pending-open".to_owned(),
        ..Default::default()
    });
    let subscribe = RequestId(conn.peek_next_request_id_for_test());
    let _consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/reset-pending-subscribe".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let lookup = conn.lookup("persistent://public/default/reset-pending-lookup", false);
    assert!(
        [open, subscribe, lookup]
            .into_iter()
            .all(|id| conn.has_pending_request_for_test(id)),
        "precondition: the open, the subscribe and the lookup must be pending at the reset"
    );

    conn.reset();

    // Control: a lookup is awaited through its outcome (the engine re-issues it on
    // `SessionLost`), so the result below is about the open and the subscribe and not about
    // reset() publishing nothing.
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(lookup)),
            Some(OpOutcome::SessionLost { .. })
        ),
        "reset() must still fail a pending lookup with SessionLost"
    );

    (
        conn.take_outcome(PendingOpKey::Request(open)).is_some(),
        conn.take_outcome(PendingOpKey::Request(subscribe))
            .is_some(),
    )
}

/// Answers one live and one cancelled last-message-id request, and reports what the connection
/// recorded for each, in that order.
fn retained_after_reply(conn: &mut Connection, now: Instant) -> RetainedAfterReply {
    handshake(conn, now);
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/cancelled-last-message-id".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();

    let live = conn.get_last_message_id(consumer);
    conn.handle_bytes(now, &last_message_id_response(live))
        .expect("answer the live request");
    let live_ledger = match conn.take_outcome(PendingOpKey::Request(live)) {
        Some(OpOutcome::LastMessageId {
            last_message_id, ..
        }) => Some(last_message_id.ledger_id),
        None => None,
        Some(other) => panic!("unexpected outcome for a last-message-id request: {other:?}"),
    };

    let cancelled = conn.get_last_message_id(consumer);
    assert!(
        conn.has_pending_request_for_test(cancelled),
        "precondition: the request must be pending before it is cancelled"
    );
    conn.cancel_request(cancelled);
    conn.handle_bytes(now, &last_message_id_response(cancelled))
        .expect("answer the cancelled request");
    let cancelled_recorded = conn
        .take_outcome(PendingOpKey::Request(cancelled))
        .is_some();

    (live_ledger, cancelled_recorded)
}

#[test]
fn engines_record_no_outcome_for_a_producer_open_and_subscribe_a_reset_retired() {
    let tokio = tokio_projection(retained_after_reset);
    let moonpool = moonpool_projection(retained_after_reset);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what a reset leaves behind for a pending open and subscribe"
    );
    assert_eq!(
        tokio,
        (false, false),
        "reset() recorded an outcome for the pending (open, subscribe); neither is awaited \
         through the outcome slab, so nothing can take it"
    );
}

#[test]
fn engines_ignore_a_last_message_id_reply_after_cancel() {
    let tokio = tokio_projection(retained_after_reply);
    let moonpool = moonpool_projection(retained_after_reply);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what a live and a cancelled last-message-id reply leave behind"
    );

    let (live, cancelled) = tokio;
    assert_eq!(
        live,
        Some(7),
        "a reply to a live last-message-id request must still resolve it"
    );
    assert!(
        !cancelled,
        "a reply to a cancelled last-message-id request must be ignored, but the connection kept \
         an outcome nobody can take"
    );
}
