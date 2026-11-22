// SPDX-License-Identifier: Apache-2.0

//! A request whose awaiting future was dropped records no outcome, identically on both engines
//! (ADR-0024 layer (d)).
//!
//! Both engines' producer and consumer `RequestFut`s call `Connection::abandon_request_waiter`
//! from `Drop`. The broker's answer to an abandoned `Producer::close()`, `ack()`, `get_schema()`
//! or `Consumer::last_message_id()` must leave nothing in `Connection::outcomes`, while the answer
//! to the same request kind that is still awaited keeps resolving it, and an abandoned close still
//! fails the sends its producer had in flight. Both engines wrap the same `magnetar-proto` state
//! machine, so they must agree.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, CreateProducerRequest, MessageId, OpOutcome,
    PendingOpKey, RequestId, SubscribeRequest, encode_command, pb,
};

/// What the connection holds once the broker has answered one abandoned and one live request of
/// each kind, as `(what, whether it was recorded or took effect)`: the abandoned requests must
/// record nothing, the live ones must resolve, and the abandoned close must still fail the send.
type Retained = Vec<(&'static str, bool)>;

fn frame(command: &pb::BaseCommand) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(&mut frame, command).expect("encode command");
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
    conn.handle_bytes(
        now,
        &frame(&pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        }),
    )
    .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();
}

fn ack_command(consumer: magnetar_proto::ConsumerHandle, request_id: RequestId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id: consumer.0,
            request_id: Some(request_id.0),
            error: None,
            message: None,
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    }
}

fn schema_command(request_id: RequestId) -> pb::BaseCommand {
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

fn last_message_id_command(request_id: RequestId) -> pb::BaseCommand {
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

fn recorded(conn: &mut Connection, request_id: RequestId) -> bool {
    conn.take_outcome(PendingOpKey::Request(request_id))
        .is_some()
}

/// A producer with one send in flight whose close future is dropped before the broker's ack.
/// Returns the producer, the send's sequence id and the close's request id.
fn producer_with_abandoned_close(
    conn: &mut Connection,
    now: Instant,
) -> (
    magnetar_proto::ProducerHandle,
    magnetar_proto::SequenceId,
    RequestId,
) {
    let open = RequestId(conn.peek_next_request_id_for_test());
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/abandoned-close".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        now,
        &frame(&pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: open.0,
                producer_name: "differential-producer".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        }),
    )
    .expect("ProducerSuccess");
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
    conn.abandon_request_waiter(close);
    (producer, sequence, close)
}

/// Issues one abandoned and one live request of each kind, answers all of them, and reports what
/// the connection recorded.
fn retained_after_answers(conn: &mut Connection, now: Instant) -> Retained {
    handshake(conn, now);
    let (producer, sequence, close) = producer_with_abandoned_close(conn, now);

    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/abandoned-requests".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    let ack = |conn: &mut Connection| {
        conn.ack(
            consumer,
            AckRequest {
                message_ids: vec![MessageId {
                    ledger_id: 1,
                    entry_id: 0,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                }],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            now,
        )
    };
    let (ack_abandoned, ack_live) = (ack(conn), ack(conn));
    let (schema_abandoned, schema_live) = (
        conn.get_schema("persistent://public/default/abandoned-schema", None),
        conn.get_schema("persistent://public/default/live-schema", None),
    );
    let (last_abandoned, last_live) = (
        conn.get_last_message_id(consumer),
        conn.get_last_message_id(consumer),
    );
    for abandoned in [ack_abandoned, schema_abandoned, last_abandoned] {
        assert!(
            conn.has_pending_request_for_test(abandoned),
            "precondition: {abandoned:?} must be pending when its future is dropped"
        );
        conn.abandon_request_waiter(abandoned);
    }

    conn.handle_bytes(
        now,
        &frame(&pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: close.0,
                schema: None,
            }),
            ..Default::default()
        }),
    )
    .expect("answer the close");
    let answers = vec![
        ack_command(consumer, ack_abandoned),
        ack_command(consumer, ack_live),
        schema_command(schema_abandoned),
        schema_command(schema_live),
        last_message_id_command(last_abandoned),
        last_message_id_command(last_live),
    ];
    for command in &answers {
        conn.handle_bytes(now, &frame(command))
            .expect("answer the request");
    }

    vec![
        ("abandoned close recorded an outcome", recorded(conn, close)),
        (
            "abandoned close failed the pending send",
            matches!(
                conn.take_outcome(PendingOpKey::Send(producer, sequence)),
                Some(OpOutcome::Terminal { .. })
            ),
        ),
        (
            "abandoned ack recorded an outcome",
            recorded(conn, ack_abandoned),
        ),
        ("live ack resolved", recorded(conn, ack_live)),
        (
            "abandoned schema lookup recorded an outcome",
            recorded(conn, schema_abandoned),
        ),
        ("live schema lookup resolved", recorded(conn, schema_live)),
        (
            "abandoned last-message-id recorded an outcome",
            recorded(conn, last_abandoned),
        ),
        ("live last-message-id resolved", recorded(conn, last_live)),
    ]
}

#[test]
fn engines_record_no_outcome_for_the_requests_whose_futures_were_dropped() {
    let tokio = tokio_projection(retained_after_answers);
    let moonpool = moonpool_projection(retained_after_answers);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what the broker's answers leave behind for abandoned requests"
    );
    assert_eq!(
        tokio,
        vec![
            ("abandoned close recorded an outcome", false),
            ("abandoned close failed the pending send", true),
            ("abandoned ack recorded an outcome", false),
            ("live ack resolved", true),
            ("abandoned schema lookup recorded an outcome", false),
            ("live schema lookup resolved", true),
            ("abandoned last-message-id recorded an outcome", false),
            ("live last-message-id resolved", true),
        ],
        "an abandoned request must record nothing and keep its side effects, while a live one \
         must keep resolving"
    );
}
