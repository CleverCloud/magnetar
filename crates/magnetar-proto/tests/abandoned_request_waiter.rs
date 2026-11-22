// SPDX-License-Identifier: Apache-2.0

//! A request whose awaiting future was dropped records no outcome when the broker answers it.
//!
//! The runtimes' producer and consumer `RequestFut`s call
//! [`Connection::abandon_request_waiter`] from `Drop`. A reply to a request nobody awaits any more
//! would otherwise park an `OpOutcome` in `Connection::outcomes` for the life of the connection:
//! one entry per cancelled `Producer::close()`, `get_schema()`, `Consumer::last_message_id()` or
//! `ack()`. The hand-off is by request kind, never by waker presence:
//!
//! - a producer close is retagged, not removed, because its `Success` still has to fail the
//!   producer's pending sends;
//! - an ack is marked forgotten;
//! - a schema lookup and a last-message-id request are retired;
//! - a seek is left to its own drop guard.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, CreateProducerRequest, MessageId, OpOutcome,
    PendingOpKey, RequestId, SeekTarget, SubscribeRequest, encode_command, pb,
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

fn success(request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: request_id.0,
            schema: None,
        }),
        ..Default::default()
    }
}

fn error(request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id: request_id.0,
            error: pb::ServerError::UnknownError as i32,
            message: "rejected".to_owned(),
        }),
        ..Default::default()
    }
}

fn ack_response(consumer_id: u64, request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id,
            request_id: Some(request_id.0),
            error: None,
            message: None,
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    }
}

fn get_schema_response(request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::GetSchemaResponse as i32,
        get_schema_response: Some(pb::CommandGetSchemaResponse {
            request_id: request_id.0,
            schema: Some(pb::Schema {
                name: "abandoned".to_owned(),
                schema_data: bytes::Bytes::from_static(b"{}"),
                r#type: pb::schema::Type::Json as i32,
                properties: Vec::new(),
            }),
            schema_version: Some(bytes::Bytes::from_static(b"v1")),
            error_code: None,
            error_message: None,
        }),
        ..Default::default()
    }
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

fn producer_success(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
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
    }
}

fn outcome_of(conn: &mut Connection, request_id: RequestId) -> Option<OpOutcome> {
    conn.take_outcome(PendingOpKey::Request(request_id))
}

fn message_id() -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id: 0,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}

fn subscribed(conn: &mut Connection, name: &str) -> magnetar_proto::ConsumerHandle {
    let consumer = conn.subscribe(SubscribeRequest {
        topic: format!("persistent://public/default/{name}"),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    consumer
}

/// How the broker answers the close an abandoned `Producer::close()` issued.
#[derive(Clone, Copy, Debug)]
enum CloseReply {
    Ack,
    Reject,
}

/// A producer holds one unacknowledged send when its `close()` is issued; the close's future is
/// dropped before the broker answers. Returns what the connection holds for the close request and
/// for the send once the broker has answered the close.
fn abandoned_close(reply: CloseReply) -> (Option<OpOutcome>, Option<OpOutcome>) {
    let now = Instant::now();
    let mut conn = connected(now);
    let open = RequestId(conn.peek_next_request_id_for_test());
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/abandoned-close".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    feed(&mut conn, now, &producer_success(open.0));
    while conn.poll_event().is_some() {}
    let sequence = conn
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
            now,
        )
        .expect("queue send");
    let _ = conn.poll_transmit();

    let close = conn.close_producer(producer);
    assert!(
        conn.has_pending_request_for_test(close),
        "precondition: the close must be pending when its future is dropped"
    );
    conn.abandon_request_waiter(close);
    assert!(
        conn.has_pending_request_for_test(close),
        "an abandoned close must stay pending: its answer still has to fail the producer's sends"
    );

    feed(
        &mut conn,
        now,
        &match reply {
            CloseReply::Ack => success(close),
            CloseReply::Reject => error(close),
        },
    );
    assert!(
        !conn.has_pending_request_for_test(close),
        "the broker's answer must retire the pending close"
    );
    (
        outcome_of(&mut conn, close),
        conn.take_outcome(PendingOpKey::Send(producer, sequence)),
    )
}

#[test]
fn an_abandoned_close_records_no_outcome_and_still_fails_the_pending_send_on_ack() {
    let (close, send) = abandoned_close(CloseReply::Ack);
    assert!(
        close.is_none(),
        "the ack of a close nobody awaits recorded an outcome nobody can take: {close:?}"
    );
    assert!(
        matches!(send, Some(OpOutcome::Terminal { .. })),
        "the ack of an abandoned close must still fail the producer's pending send (got {send:?})"
    );
}

#[test]
fn an_abandoned_close_records_no_outcome_and_still_fails_the_pending_send_on_rejection() {
    let (close, send) = abandoned_close(CloseReply::Reject);
    assert!(
        close.is_none(),
        "the rejection of a close nobody awaits recorded an outcome nobody can take: {close:?}"
    );
    // A rejected close fails the producer's pending sends exactly as an acknowledged one does,
    // and exactly as for an awaited close: the producer slot is released either way.
    assert!(
        matches!(send, Some(OpOutcome::Terminal { .. })),
        "the rejection of an abandoned close must still fail the producer's pending send (got {send:?})"
    );
}

/// Control: the awaited close records its outcome and fails the send the same way, so the
/// abandoned one differs only in recording nothing.
#[test]
fn a_close_that_is_not_abandoned_still_records_its_outcome() {
    let now = Instant::now();
    let mut conn = connected(now);
    let open = RequestId(conn.peek_next_request_id_for_test());
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/awaited-close".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    feed(&mut conn, now, &producer_success(open.0));
    while conn.poll_event().is_some() {}

    let close = conn.close_producer(producer);
    feed(&mut conn, now, &success(close));

    assert!(
        matches!(
            outcome_of(&mut conn, close),
            Some(OpOutcome::Success { .. })
        ),
        "an awaited close must record the outcome its future drains"
    );
}

#[test]
fn an_abandoned_ack_records_no_outcome_while_a_live_one_does() {
    let now = Instant::now();
    let mut conn = connected(now);
    let consumer = subscribed(&mut conn, "abandoned-ack");
    let ack = |conn: &mut Connection| {
        conn.ack(
            consumer,
            AckRequest {
                message_ids: vec![message_id()],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            now,
        )
    };

    let abandoned = ack(&mut conn);
    let live = ack(&mut conn);
    conn.abandon_request_waiter(abandoned);
    assert!(
        conn.has_pending_request_for_test(abandoned),
        "an abandoned ack stays pending until the broker answers (or the ack timeout reaps it)"
    );

    feed(&mut conn, now, &ack_response(consumer.0, abandoned));
    feed(&mut conn, now, &ack_response(consumer.0, live));

    assert!(
        outcome_of(&mut conn, abandoned).is_none(),
        "the reply to an ack nobody awaits recorded an outcome nobody can take"
    );
    assert!(
        matches!(outcome_of(&mut conn, live), Some(OpOutcome::Success { .. })),
        "the reply to an ack that is still awaited must keep resolving it"
    );
}

#[test]
fn an_abandoned_get_schema_is_retired_and_its_late_reply_ignored() {
    let now = Instant::now();
    let mut conn = connected(now);
    let live = conn.get_schema("persistent://public/default/live-schema", None);
    let abandoned = conn.get_schema("persistent://public/default/abandoned-schema", None);

    conn.abandon_request_waiter(abandoned);
    assert!(
        !conn.has_pending_request_for_test(abandoned),
        "an abandoned schema lookup must be retired"
    );

    feed(&mut conn, now, &get_schema_response(abandoned));
    feed(&mut conn, now, &get_schema_response(live));

    assert!(
        outcome_of(&mut conn, abandoned).is_none(),
        "the reply to a schema lookup nobody awaits recorded an outcome nobody can take"
    );
    assert!(
        matches!(
            outcome_of(&mut conn, live),
            Some(OpOutcome::GetSchemaResponse { .. })
        ),
        "the reply to a schema lookup that is still awaited must keep resolving it"
    );
}

#[test]
fn an_abandoned_last_message_id_is_retired_and_its_late_reply_ignored() {
    let now = Instant::now();
    let mut conn = connected(now);
    let consumer = subscribed(&mut conn, "abandoned-last-message-id");
    let live = conn.get_last_message_id(consumer);
    let abandoned = conn.get_last_message_id(consumer);

    conn.abandon_request_waiter(abandoned);
    assert!(
        !conn.has_pending_request_for_test(abandoned),
        "an abandoned last-message-id request must be retired"
    );

    feed(&mut conn, now, &last_message_id_response(abandoned));
    feed(&mut conn, now, &last_message_id_response(live));

    assert!(
        outcome_of(&mut conn, abandoned).is_none(),
        "the reply to a last-message-id request nobody awaits recorded an outcome nobody can take"
    );
    assert!(
        matches!(
            outcome_of(&mut conn, live),
            Some(OpOutcome::LastMessageId { .. })
        ),
        "the reply to a last-message-id request that is still awaited must keep resolving it"
    );
}

/// The seek's own drop guard owns its hand-off (it carries the re-attach), and it runs after the
/// request future's drop: the generic hand-off must leave the seek pending for it.
#[test]
fn abandoning_a_seek_request_leaves_it_to_the_seek_guard() {
    let now = Instant::now();
    let mut conn = connected(now);
    let consumer = subscribed(&mut conn, "abandoned-seek");
    let seek = conn.seek(consumer, SeekTarget::MessageId(MessageId::EARLIEST));

    conn.abandon_request_waiter(seek);

    assert!(
        conn.has_pending_request_for_test(seek),
        "the generic hand-off must not retire a seek"
    );
    assert!(
        conn.abandon_consumer_seek_waiter(consumer, seek),
        "the seek guard must still find the seek pending and take it over"
    );
    feed(&mut conn, now, &success(seek));
    assert!(
        outcome_of(&mut conn, seek).is_none(),
        "the reply to a seek the guard took over records no outcome"
    );
}

#[test]
fn abandoning_discards_an_outcome_that_landed_first_and_is_idempotent() {
    let now = Instant::now();
    let mut conn = connected(now);
    let schema = conn.get_schema("persistent://public/default/landed-schema", None);
    // The reply lands and wakes the parked task, but the future is dropped before the task
    // polls it again, so nothing would ever take the outcome.
    feed(&mut conn, now, &get_schema_response(schema));

    conn.abandon_request_waiter(schema);
    assert!(
        outcome_of(&mut conn, schema).is_none(),
        "an outcome that landed before the drop must be discarded"
    );

    // A request already answered is no longer pending; abandoning it again, or abandoning an id
    // this connection never issued, changes nothing.
    conn.abandon_request_waiter(schema);
    conn.abandon_request_waiter(RequestId(u64::MAX));
    assert!(outcome_of(&mut conn, schema).is_none());
}

#[test]
fn a_reset_records_no_outcome_for_an_abandoned_close() {
    let now = Instant::now();
    let mut conn = connected(now);
    let open = RequestId(conn.peek_next_request_id_for_test());
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/abandoned-close-reset".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    feed(&mut conn, now, &producer_success(open.0));
    while conn.poll_event().is_some() {}

    let close = conn.close_producer(producer);
    conn.abandon_request_waiter(close);
    conn.reset();

    assert!(
        outcome_of(&mut conn, close).is_none(),
        "a reset recorded SessionLost for a close nobody awaits"
    );
}
