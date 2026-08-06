// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O stream command contracts executed by the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_proto::consumer::BatchAckEntry;
use magnetar_proto::{
    AckRequest, AssignmentError, Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle,
    ControllerIncarnation, KeyRange, MessageId, SeekTarget, StreamPositionError, SubscribeRequest,
    TxnAction, TxnId, canonical_segment_topic, decode_one, encode_command, encode_payload, pb,
};

const TOPIC: &str = "topic://public/default/scaled";

fn connection() -> Connection {
    Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now))
}

fn encode(command: pb::BaseCommand) -> BytesMut {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, &command).expect("encode broker command");
    bytes
}

fn drain_commands(connection: &mut Connection) -> Vec<pb::BaseCommand> {
    let mut bytes = connection.poll_transmit();
    let mut commands = Vec::new();
    while !bytes.is_empty() {
        commands.push(
            decode_one(&mut bytes)
                .expect("decode outbound command")
                .command,
        );
    }
    commands
}

fn handshaked_scalable_connection() -> Connection {
    let mut connection = connection();
    connection.begin_handshake().expect("begin handshake");
    let _ = drain_commands(&mut connection);
    let mut feature_flags = pb::FeatureFlags::default();
    feature_flags.supports_scalable_topics = Some(true);
    connection
        .handle_bytes(
            Instant::now(),
            &encode(pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "contract-broker".to_owned(),
                    protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(feature_flags),
                }),
                ..Default::default()
            }),
        )
        .expect("accept connected response");
    assert!(matches!(
        connection.poll_event(),
        Some(ConnectionEvent::Connected { .. })
    ));
    connection
}

fn assigned(id: u64, start: u32, end: u32) -> pb::ScalableAssignedSegment {
    let range = KeyRange::new(start, end).expect("valid range");
    pb::ScalableAssignedSegment {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        segment_topic: canonical_segment_topic(TOPIC, range, magnetar_proto::SegmentId(id))
            .expect("canonical segment"),
    }
}

fn assignment(
    epoch: u64,
    segments: Vec<pb::ScalableAssignedSegment>,
) -> pb::ScalableConsumerAssignment {
    pb::ScalableConsumerAssignment {
        layout_epoch: epoch,
        segments,
    }
}

fn complete_id(
    ledger_id: u64,
    entry_id: u64,
    batch_index: Option<i32>,
    batch_size: Option<i32>,
    ack_set: Vec<i64>,
) -> pb::MessageIdData {
    pb::MessageIdData {
        ledger_id,
        entry_id,
        partition: Some(0),
        batch_index,
        ack_set,
        batch_size,
        first_chunk_message_id: None,
    }
}

fn ack_request(data: &pb::MessageIdData, ack_type: pb::command_ack::AckType) -> AckRequest {
    AckRequest {
        message_ids: vec![MessageId::from_pb(data)],
        ack_type,
        properties: Vec::new(),
        txn_id: None,
    }
}

#[test]
fn complete_ids_drive_partial_ack_nack_seek_and_manual_flow() {
    let mut connection = connection();
    let unknown = ConsumerHandle(u64::MAX);
    let handle = connection.subscribe(SubscribeRequest {
        topic: "persistent://public/default/segment".to_owned(),
        subscription: "stream-contract".to_owned(),
        receiver_queue_size: 0,
        negative_ack_redelivery_delay: Some(Duration::from_secs(1)),
        ack_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    });
    let _ = drain_commands(&mut connection);

    connection.flow(handle, 3);
    let flow = drain_commands(&mut connection);
    assert!(matches!(
        flow.as_slice(),
        [pb::BaseCommand {
            flow: Some(pb::CommandFlow {
                message_permits: 3,
                ..
            }),
            ..
        }]
    ));
    connection.flow(unknown, 1);
    assert!(matches!(
        drain_commands(&mut connection).as_slice(),
        [pb::BaseCommand {
            flow: Some(pb::CommandFlow {
                consumer_id: u64::MAX,
                message_permits: 1,
            }),
            ..
        }]
    ));

    let individual = complete_id(7, 11, Some(1), Some(4), vec![0b1111]);
    let _ = connection.ack_with_message_id_data(
        handle,
        ack_request(&individual, pb::command_ack::AckType::Individual),
        vec![individual.clone()],
        Instant::now(),
    );
    let commands = drain_commands(&mut connection);
    let ack = commands[0].ack.as_ref().expect("individual ack command");
    assert_eq!(ack.message_id[0].ack_set, vec![0b1101]);

    let cumulative = complete_id(8, 12, Some(3), Some(4), vec![0b1111]);
    let _ = connection.ack_with_message_id_data(
        handle,
        ack_request(&cumulative, pb::command_ack::AckType::Cumulative),
        vec![cumulative],
        Instant::now(),
    );
    let commands = drain_commands(&mut connection);
    assert!(
        commands[0]
            .ack
            .as_ref()
            .expect("cumulative ack command")
            .message_id[0]
            .ack_set
            .is_empty()
    );

    let ordinary = complete_id(9, 13, None, None, Vec::new());
    let _ = connection.ack_with_message_id_data(
        unknown,
        ack_request(&ordinary, pb::command_ack::AckType::Individual),
        vec![ordinary.clone()],
        Instant::now(),
    );
    let _ = connection.ack_with_message_id_data(
        unknown,
        ack_request(&ordinary, pb::command_ack::AckType::Cumulative),
        vec![ordinary.clone()],
        Instant::now(),
    );
    assert_eq!(drain_commands(&mut connection).len(), 2);

    connection.negative_ack_with_message_id_data(handle, vec![individual.clone()], Instant::now());
    assert!(connection.poll_transmit().is_empty());
    connection.negative_ack_with_message_id_data(unknown, vec![ordinary.clone()], Instant::now());
    connection.negative_ack_with_message_id_data(handle, Vec::new(), Instant::now());
    let redeliver = drain_commands(&mut connection);
    assert_eq!(redeliver.len(), 2);
    assert!(redeliver.iter().all(|command| {
        command.r#type == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32
    }));
    connection.negative_ack_with_delay(
        handle,
        MessageId::from_pb(&individual),
        Duration::from_millis(1),
        Instant::now(),
    );

    let chunked = pb::MessageIdData {
        ledger_id: 10,
        entry_id: 20,
        partition: Some(0),
        batch_index: None,
        ack_set: Vec::new(),
        batch_size: None,
        first_chunk_message_id: Some(Box::new(pb::MessageIdData {
            ledger_id: 10,
            entry_id: 18,
            partition: Some(0),
            batch_index: None,
            ack_set: Vec::new(),
            batch_size: None,
            first_chunk_message_id: None,
        })),
    };
    let _ = connection.seek(handle, SeekTarget::MessageIdData(chunked));
    let commands = drain_commands(&mut connection);
    assert_eq!(
        commands[0].seek.as_ref().expect("chunk seek").message_id,
        Some(complete_id(10, 18, None, None, Vec::new()))
    );

    let batch = complete_id(11, 21, Some(2), Some(4), vec![0b1110]);
    let _ = connection.seek(handle, SeekTarget::MessageIdData(batch));
    let commands = drain_commands(&mut connection);
    assert_eq!(
        commands[0]
            .seek
            .as_ref()
            .expect("batch seek")
            .message_id
            .as_ref()
            .expect("message-id seek")
            .ack_set,
        vec![0b1100]
    );
}

#[test]
fn batch_masks_and_manual_flow_account_for_broker_selected_members() {
    let boundary = BatchAckEntry::from_ack_set(64, &[-1]);
    assert!(boundary.is_unacked(0));
    assert!(boundary.is_unacked(63));
    assert!(!boundary.is_unacked(64));
    let empty = BatchAckEntry::from_ack_set(0, &[-1]);
    assert!(empty.is_fully_acked());
    assert!(!empty.is_unacked(0));

    let mut single = BatchAckEntry::fresh(1);
    assert!(!single.ack_position(-1));
    assert!(single.is_unacked(0));

    let mut connection = connection();
    let handle = connection.subscribe(SubscribeRequest {
        topic: "persistent://public/default/manual-flow".to_owned(),
        subscription: "manual-flow-contract".to_owned(),
        receiver_queue_size: 0,
        ..Default::default()
    });
    let _ = drain_commands(&mut connection);
    connection.flow(handle, 1);
    let _ = drain_commands(&mut connection);

    let ordinary_command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: complete_id(20, 1, None, None, Vec::new()),
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let ordinary_metadata = pb::MessageMetadata {
        producer_name: "manual-flow-contract".to_owned(),
        sequence_id: 1,
        publish_time: 0,
        ..Default::default()
    };
    let mut ordinary_frame = BytesMut::new();
    encode_payload(
        &mut ordinary_frame,
        &ordinary_command,
        &ordinary_metadata,
        b"one",
    )
    .expect("encode ordinary frame");
    connection
        .handle_bytes(Instant::now(), &ordinary_frame)
        .expect("deliver ordinary frame");
    assert!(connection.pop_message(handle, Instant::now()).is_some());

    let batch_command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: complete_id(20, 2, None, None, Vec::new()),
            redelivery_count: Some(0),
            ack_set: vec![0],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let batch_metadata = pb::MessageMetadata {
        producer_name: "manual-flow-contract".to_owned(),
        sequence_id: 2,
        publish_time: 0,
        num_messages_in_batch: Some(2),
        ..Default::default()
    };
    let mut batch_body = BytesMut::new();
    for _ in 0..2 {
        let metadata = pb::SingleMessageMetadata {
            payload_size: 1,
            ..Default::default()
        };
        let metadata_len = prost::Message::encoded_len(&metadata);
        batch_body.extend_from_slice(&(metadata_len as u32).to_be_bytes());
        prost::Message::encode(&metadata, &mut batch_body).expect("encode batch member metadata");
        batch_body.extend_from_slice(b"x");
    }
    let mut batch_frame = BytesMut::new();
    encode_payload(
        &mut batch_frame,
        &batch_command,
        &batch_metadata,
        &batch_body,
    )
    .expect("encode broker-selected batch frame");
    connection
        .handle_bytes(Instant::now(), &batch_frame)
        .expect("accept empty broker-selected batch");
    assert_eq!(connection.consumer_queue_len(handle), 0);
}

#[test]
fn cancelling_end_transaction_retires_all_correlation_state() {
    let mut connection = connection();
    let transaction = TxnId::new(7, 11);
    let request = connection
        .end_txn(transaction, TxnAction::Commit)
        .expect("start transaction completion");
    connection.cancel_request(request);
    connection.release_end_txn_waiter(transaction, TxnAction::Commit);
    let replacement = connection
        .end_txn(transaction, TxnAction::Commit)
        .expect("cancellation permits a fresh completion request");
    assert_ne!(replacement, request);
}

#[test]
fn scalable_subscribe_replays_prebaseline_pushes_and_rejects_identity_drift() {
    let mut connection = handshaked_scalable_connection();
    let _first_request = connection
        .scalable_topic_subscribe(
            TOPIC,
            "sub",
            "member",
            7,
            magnetar_proto::ScalableConsumerType::Stream,
            ControllerIncarnation(1),
        )
        .expect("open scalable registration");
    let request = connection
        .scalable_topic_subscribe(
            TOPIC,
            "sub",
            "member",
            7,
            magnetar_proto::ScalableConsumerType::Stream,
            ControllerIncarnation(2),
        )
        .expect("replace the same registration with a new incarnation");
    assert!(matches!(
        connection.scalable_topic_subscribe(
            TOPIC,
            "other-sub",
            "member",
            7,
            magnetar_proto::ScalableConsumerType::Stream,
            ControllerIncarnation(3),
        ),
        Err(magnetar_proto::ScalableTopicError::Assignment(
            AssignmentError::RegistrationMismatch { consumer_id: 7 }
        ))
    ));
    let _ = drain_commands(&mut connection);

    connection
        .handle_bytes(
            Instant::now(),
            &encode(pb::BaseCommand {
                r#type: pb::base_command::Type::ScalableTopicAssignmentUpdate as i32,
                scalable_topic_assignment_update: Some(pb::CommandScalableTopicAssignmentUpdate {
                    consumer_id: 7,
                    assignment: assignment(2, vec![assigned(2, 0, 65_535)]),
                }),
                ..Default::default()
            }),
        )
        .expect("buffer assignment push");
    connection
        .handle_bytes(
            Instant::now(),
            &encode(pb::BaseCommand {
                r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
                scalable_topic_subscribe_response: Some(
                    pb::CommandScalableTopicSubscribeResponse {
                        request_id: request.0,
                        error: None,
                        message: None,
                        assignment: Some(assignment(1, vec![assigned(1, 0, 65_535)])),
                    },
                ),
                ..Default::default()
            }),
        )
        .expect("apply response baseline");

    assert!(matches!(
        connection.poll_event(),
        Some(ConnectionEvent::ScalableConsumerAssigned { consumer_id: 7, .. })
    ));
    assert!(matches!(
        connection.poll_event(),
        Some(ConnectionEvent::ScalableAssignmentChanged {
            consumer_id: 7,
            delta,
            ..
        }) if delta.layout_epoch == 2
    ));
}

// Keep error exports instantiated in the same public-contract target so future
// representation changes cannot silently remove their trait bounds.
#[test]
fn stream_error_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StreamPositionError>();
    assert_send_sync::<Bytes>();
}
