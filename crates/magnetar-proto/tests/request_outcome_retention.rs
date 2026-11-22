// SPDX-License-Identifier: Apache-2.0

//! An outcome nobody can take must not be recorded for the life of the connection.
//!
//! `OpOutcome`s for request-id-correlated operations live in `Connection::outcomes` until a
//! future calls `take_outcome`, and `reset()` does not clear them. Two recording sites park one
//! that no caller can ever take:
//!
//! - `reset()` publishes `SessionLost` for every pending request it fails, including a producer
//!   open and a consumer subscribe. Neither is awaited through the outcome slab: the runtimes wait
//!   for the `ProducerReady` / `SubscribeAcked` (or `*Failed`) event, and the driver discards the
//!   request ids `rebuild_producers` / `rebuild_consumers` return. Each reset with an open or a
//!   subscribe in flight therefore costs two permanent entries.
//! - The `CommandGetLastMessageIdResponse` arm inserts its outcome whether or not the request is
//!   still pending, so a reply to a request that `cancel_request` dropped (its documented contract:
//!   "a late broker reply is ignored") is recorded all the same.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, RequestId,
    SubscribeRequest, encode_command, pb,
};

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

fn connected(now: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    feed(
        &mut conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    );
    let _ = conn.poll_transmit();
    conn
}

fn last_message_id_response(request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
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
    }
}

#[test]
fn reset_records_no_outcome_for_a_pending_producer_open() {
    let now = Instant::now();
    let mut conn = connected(now);

    let open = RequestId(conn.peek_next_request_id_for_test());
    let _producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reset-pending-open".to_owned(),
        ..Default::default()
    });
    assert!(
        conn.has_pending_request_for_test(open),
        "precondition: the producer open must be pending when the session resets"
    );

    conn.reset();

    assert!(
        !conn.has_pending_request_for_test(open),
        "reset() must retire the pending producer open"
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(open)).is_none(),
        "reset() recorded an outcome for the pending producer open; the runtime waits for the \
         ProducerReady / ProducerOpenFailed event and the driver discards the rebuild's request \
         id, so nothing can take it"
    );
}

#[test]
fn reset_records_no_outcome_for_a_pending_consumer_subscribe() {
    let now = Instant::now();
    let mut conn = connected(now);

    let subscribe = RequestId(conn.peek_next_request_id_for_test());
    let _consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/reset-pending-subscribe".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    assert!(
        conn.has_pending_request_for_test(subscribe),
        "precondition: the subscribe must be pending when the session resets"
    );

    conn.reset();

    assert!(
        !conn.has_pending_request_for_test(subscribe),
        "reset() must retire the pending subscribe"
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(subscribe))
            .is_none(),
        "reset() recorded an outcome for the pending subscribe; the runtime waits for the \
         SubscribeAcked / SubscribeFailed event and the driver discards the rebuild's request \
         id, so nothing can take it"
    );
}

/// Control for the two tests above: a pending lookup is awaited through its outcome (the engine
/// re-issues it on `SessionLost`), so `reset()` must keep publishing it.
#[test]
fn reset_still_fails_a_pending_lookup_with_session_lost() {
    let now = Instant::now();
    let mut conn = connected(now);

    let lookup = conn.lookup("persistent://public/default/reset-pending-lookup", false);
    assert!(
        conn.has_pending_request_for_test(lookup),
        "precondition: the lookup must be pending when the session resets"
    );

    conn.reset();

    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(lookup)),
            Some(OpOutcome::SessionLost { .. })
        ),
        "reset() must fail a pending lookup with SessionLost"
    );
}

#[test]
fn a_last_message_id_reply_after_cancel_request_records_no_outcome() {
    let now = Instant::now();
    let mut conn = connected(now);
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/cancelled-last-message-id".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();

    let request = conn.get_last_message_id(consumer);
    assert!(
        conn.has_pending_request_for_test(request),
        "precondition: the last-message-id request must be pending before it is cancelled"
    );
    conn.cancel_request(request);
    assert!(
        !conn.has_pending_request_for_test(request),
        "cancel_request must retire the pending request"
    );

    feed(&mut conn, now, &last_message_id_response(request));

    assert!(
        conn.take_outcome(PendingOpKey::Request(request)).is_none(),
        "a reply to a cancelled last-message-id request must be ignored, but the connection \
         recorded an outcome nobody is left to take"
    );
}
