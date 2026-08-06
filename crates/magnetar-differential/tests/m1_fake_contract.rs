// SPDX-License-Identifier: Apache-2.0

//! Public stateful-M1 fake contracts executed by the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use magnetar_fakes::m1::{
    AncestryProof, AuthAttempt, BrokerFailure, ConnectionId, DrainEligibility, Endpoint,
    EndpointAuthorities, FakeTransactionState, FullAssignment, M1AdapterError, M1ConnectionAdapter,
    M1FakeCluster, M1FakeConfig, M1FakeError, M1Segment, MemberId, OperationKind,
    PendingCompletion, PendingOperationId, ScriptedBehavior, TransportSecurity,
};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, CreateProducerRequest, Frame, MAX_FRAME_SIZE,
    decode_one, encode_command, encode_payload, pb,
};

const TOPIC: &str = "topic://public/default/scaled";
const TRANSACTION_COORDINATOR_TOPIC: &str =
    "persistent://pulsar/system/transaction_coordinator_assign-partition-0";

fn command_bytes(command: &pb::BaseCommand) -> Bytes {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode command");
    bytes.freeze()
}

fn send(
    cluster: &mut M1FakeCluster,
    connection: ConnectionId,
    command: &pb::BaseCommand,
) -> Result<(), M1FakeError> {
    let mut bytes = command_bytes(command);
    cluster.handle_bytes(connection, &mut bytes)
}

fn send_with_payload(
    cluster: &mut M1FakeCluster,
    connection: ConnectionId,
    command: &pb::BaseCommand,
) -> Result<(), M1FakeError> {
    let metadata = pb::MessageMetadata {
        producer_name: "hostile-contract".to_owned(),
        sequence_id: 1,
        publish_time: 1,
        ..Default::default()
    };
    let mut bytes = BytesMut::new();
    encode_payload(
        &mut bytes,
        command,
        &metadata,
        &Bytes::from_static(b"unexpected"),
    )
    .expect("encode payload-bearing command");
    let mut bytes = bytes.freeze();
    cluster.handle_bytes(connection, &mut bytes)
}

fn take_frames(cluster: &mut M1FakeCluster, connection: ConnectionId) -> Vec<Frame> {
    cluster
        .take_output(connection)
        .expect("connection remains open")
        .into_iter()
        .map(|mut bytes| decode_one(&mut bytes).expect("decode fake output"))
        .collect()
}

fn connect(cluster: &mut M1FakeCluster, endpoint: Endpoint) -> ConnectionId {
    let connection = cluster
        .open_connection(endpoint)
        .expect("open known fake endpoint");
    send(
        cluster,
        connection,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(pb::CommandConnect {
                client_version: "m1-public-contract".to_owned(),
                feature_flags: Some(pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .expect("complete fake handshake");
    assert!(
        take_frames(cluster, connection)[0]
            .command
            .connected
            .is_some()
    );
    connection
}

fn register_member(
    cluster: &mut M1FakeCluster,
    controller: ConnectionId,
    subscription: &str,
    consumer_name: &str,
    consumer_id: u64,
    request_id: u64,
) -> MemberId {
    send(
        cluster,
        controller,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ScalableTopicSubscribe as i32,
            scalable_topic_subscribe: Some(pb::CommandScalableTopicSubscribe {
                request_id,
                topic: cluster.topic().to_owned(),
                subscription: subscription.to_owned(),
                consumer_name: consumer_name.to_owned(),
                consumer_id,
                consumer_type: pb::ScalableConsumerType::Stream as i32,
            }),
            ..Default::default()
        },
    )
    .expect("register controller member");
    assert!(
        take_frames(cluster, controller)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .is_some_and(|response| response.error.is_none())
    );
    MemberId::new(controller, consumer_id)
}

fn split_layout(epoch: u64) -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(epoch),
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), epoch).with_parents([1]),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), epoch).with_parents([1]),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

fn garbage_collected_split_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

fn double_split_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2)
            .with_parents([1])
            .with_children([5, 6])
            .sealed_at(3),
        M1Segment::active(5, 0, 8_191, Endpoint::Segment(1), 3).with_parents([3]),
        M1Segment::active(6, 8_192, 16_383, Endpoint::Segment(1), 3).with_parents([3]),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2).with_parents([1]),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

fn lookup_command(topic: &str, request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Lookup as i32,
        lookup_topic: Some(pb::CommandLookupTopic {
            topic: topic.to_owned(),
            request_id,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn get_schema_command(topic: &str, request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::GetSchema as i32,
        get_schema: Some(pb::CommandGetSchema {
            request_id,
            topic: topic.to_owned(),
            schema_version: Some(Bytes::from_static(b"v1")),
        }),
        ..Default::default()
    }
}

fn scalable_lookup_command(session_id: u64, topic: &str) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicLookup as i32,
        scalable_topic_lookup: Some(pb::CommandScalableTopicLookup {
            session_id,
            topic: topic.to_owned(),
        }),
        ..Default::default()
    }
}

fn scalable_close_command(session_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicClose as i32,
        scalable_topic_close: Some(pb::CommandScalableTopicClose { session_id }),
        ..Default::default()
    }
}

fn scalable_subscribe_command(
    topic: &str,
    subscription: &str,
    consumer_name: &str,
    consumer_id: u64,
    request_id: u64,
    consumer_type: i32,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicSubscribe as i32,
        scalable_topic_subscribe: Some(pb::CommandScalableTopicSubscribe {
            request_id,
            topic: topic.to_owned(),
            subscription: subscription.to_owned(),
            consumer_name: consumer_name.to_owned(),
            consumer_id,
            consumer_type,
        }),
        ..Default::default()
    }
}

fn segment_subscribe_command(
    topic: &str,
    subscription: &str,
    controller_consumer_name: &str,
    segment_id: u64,
    consumer_id: u64,
    request_id: u64,
    sub_type: pb::command_subscribe::SubType,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Subscribe as i32,
        subscribe: Some(pb::CommandSubscribe {
            topic: topic.to_owned(),
            subscription: subscription.to_owned(),
            sub_type: sub_type as i32,
            consumer_id,
            request_id,
            consumer_name: Some(format!("{controller_consumer_name}-seg-{segment_id}")),
            initial_position: Some(pb::command_subscribe::InitialPosition::Earliest as i32),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn flow_command(consumer_id: u64, permits: u32) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Flow as i32,
        flow: Some(pb::CommandFlow {
            consumer_id,
            message_permits: permits,
        }),
        ..Default::default()
    }
}

fn ack_command(
    consumer_id: u64,
    request_id: u64,
    message_id: pb::MessageIdData,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Ack as i32,
        ack: Some(pb::CommandAck {
            consumer_id,
            ack_type: pb::command_ack::AckType::Individual as i32,
            message_id: vec![message_id],
            request_id: Some(request_id),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn redeliver_command(consumer_id: u64, message_id: pb::MessageIdData) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
        redeliver_unacknowledged_messages: Some(pb::CommandRedeliverUnacknowledgedMessages {
            consumer_id,
            message_ids: vec![message_id],
            consumer_epoch: None,
        }),
        ..Default::default()
    }
}

fn seek_command(
    consumer_id: u64,
    request_id: u64,
    message_id: pb::MessageIdData,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Seek as i32,
        seek: Some(pb::CommandSeek {
            consumer_id,
            request_id,
            message_id: Some(message_id),
            message_publish_time: None,
        }),
        ..Default::default()
    }
}

fn close_command(consumer_id: u64, request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id,
            request_id,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    }
}

fn tc_connect_command(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::TcClientConnectRequest as i32,
        tc_client_connect_request: Some(pb::CommandTcClientConnectRequest {
            request_id,
            tc_id: 0,
            scalable: None,
        }),
        ..Default::default()
    }
}

fn new_transaction_command(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::NewTxn as i32,
        new_txn: Some(pb::CommandNewTxn {
            request_id,
            txn_ttl_millis: Some(30_000),
            tc_id: Some(0),
            scalable: None,
        }),
        ..Default::default()
    }
}

fn add_subscription_to_transaction_command(
    request_id: u64,
    txn_id: magnetar_proto::TxnId,
    topic: &str,
    subscription: &str,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::AddSubscriptionToTxn as i32,
        add_subscription_to_txn: Some(pb::CommandAddSubscriptionToTxn {
            request_id,
            txnid_least_bits: Some(txn_id.least_sig_bits),
            txnid_most_bits: Some(txn_id.most_sig_bits),
            subscription: vec![pb::Subscription {
                topic: topic.to_owned(),
                subscription: subscription.to_owned(),
            }],
            scalable: None,
        }),
        ..Default::default()
    }
}

fn transactional_ack_command(
    consumer_id: u64,
    request_id: u64,
    txn_id: magnetar_proto::TxnId,
    message_id: pb::MessageIdData,
) -> pb::BaseCommand {
    let mut command = ack_command(consumer_id, request_id, message_id);
    let ack = command.ack.as_mut().expect("CommandAck body");
    ack.txnid_least_bits = Some(txn_id.least_sig_bits);
    ack.txnid_most_bits = Some(txn_id.most_sig_bits);
    command
}

fn end_transaction_command(
    request_id: u64,
    txn_id: magnetar_proto::TxnId,
    action: pb::TxnAction,
) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::EndTxn as i32,
        end_txn: Some(pb::CommandEndTxn {
            request_id,
            txnid_least_bits: Some(txn_id.least_sig_bits),
            txnid_most_bits: Some(txn_id.most_sig_bits),
            txn_action: Some(action as i32),
            scalable: None,
        }),
        ..Default::default()
    }
}

fn opened_transaction(frames: &[Frame]) -> magnetar_proto::TxnId {
    let opened = frames
        .iter()
        .find_map(|frame| frame.command.new_txn_response.as_ref())
        .expect("broker allocated a transaction");
    magnetar_proto::TxnId::new(
        opened.txnid_most_bits.expect("transaction most bits"),
        opened.txnid_least_bits.expect("transaction least bits"),
    )
}

fn message_id(frames: &[Frame]) -> pb::MessageIdData {
    frames
        .iter()
        .find_map(|frame| frame.command.message.as_ref())
        .expect("broker emitted a message")
        .message_id
        .clone()
}

fn root_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

fn assert_invalid_layout(segments: Vec<M1Segment>) {
    let mut cluster = M1FakeCluster::default();
    assert!(matches!(
        cluster.advance_layout(2, segments),
        Err(M1FakeError::InvalidLayout(_))
    ));
    assert_eq!(cluster.layout_epoch(), 1);
}

#[test]
fn fake_configuration_and_debug_output_validate_without_exposing_authentication() {
    let attempt = AuthAttempt {
        endpoint: Endpoint::Controller,
        transport: TransportSecurity::Tls,
        method: Some("token"),
        data: Some(b"secret-authentication-material"),
    };
    let debug = format!("{attempt:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("token"));
    assert!(!debug.contains("secret-authentication-material"));

    let config = M1FakeConfig::new(TOPIC)
        .expect("valid M1 topic")
        .with_endpoint_authorities(
            Endpoint::Controller,
            EndpointAuthorities::new(
                "pulsar://custom-controller.test:6650",
                "pulsar+ssl://custom-controller.test:6651",
            ),
        )
        .with_auth_validator(|_| true);
    let config_debug = format!("{config:?}");
    assert!(config_debug.contains("<redacted validator>"));
    assert!(!config_debug.contains("secret-authentication-material"));

    let cluster = M1FakeCluster::from_config(config).expect("valid custom fake config");
    let cluster_debug = format!("{cluster:?}");
    assert!(cluster_debug.contains("M1FakeCluster"));
    assert!(cluster_debug.contains("<redacted validator>"));
    assert_eq!(cluster.topic(), TOPIC);
    assert_eq!(cluster.layout_epoch(), 1);
    assert_eq!(
        cluster.endpoint_url(Endpoint::Controller),
        Some("pulsar://custom-controller.test:6650")
    );
    assert_eq!(
        cluster.endpoint_url_for(Endpoint::Controller, TransportSecurity::Tls),
        Some("pulsar+ssl://custom-controller.test:6651")
    );
    assert_eq!(cluster.endpoint_url(Endpoint::Segment(99)), None);

    assert!(matches!(
        M1FakeConfig::new("persistent://public/default/scaled"),
        Err(M1FakeError::InvalidLayout(_))
    ));
    assert!(matches!(
        M1FakeConfig::new("topic://public/scaled"),
        Err(M1FakeError::InvalidLayout(_))
    ));
    let wrong_scheme = M1FakeConfig::new(TOPIC)
        .expect("base config")
        .with_endpoint_authorities(
            Endpoint::Controller,
            EndpointAuthorities::new(
                "pulsar+ssl://controller.test:6650",
                "pulsar+ssl://controller.test:6651",
            ),
        );
    assert!(matches!(
        M1FakeCluster::from_config(wrong_scheme),
        Err(M1FakeError::InvalidLayout(_))
    ));
    let malformed = M1FakeConfig::new(TOPIC)
        .expect("base config")
        .with_endpoint_authorities(
            Endpoint::Controller,
            EndpointAuthorities::new(
                "pulsar://user@controller.test:6650",
                "pulsar+ssl://controller.test:6651",
            ),
        );
    assert!(matches!(
        M1FakeCluster::from_config(malformed),
        Err(M1FakeError::InvalidLayout(_))
    ));
}

#[test]
fn fake_projects_layout_and_assignment_and_fences_physical_connections() {
    let default_cluster = M1FakeCluster::default();
    assert_eq!(default_cluster.topic(), TOPIC);
    assert_eq!(default_cluster.layout_session_ids(), Vec::new());
    assert_eq!(default_cluster.member("missing", "missing"), None);
    assert_eq!(
        default_cluster.segment_endpoint(1),
        Some(Endpoint::Segment(1))
    );
    assert_eq!(default_cluster.segment_endpoint(99), None);
    assert!(default_cluster.segment_topic(1).is_some());
    assert_eq!(default_cluster.segment_topic(99), None);
    assert_eq!(
        default_cluster
            .dag_snapshot()
            .expect("generated DAG")
            .epoch(),
        1
    );
    let assignment = default_cluster
        .consumer_assignment(1, [1, 2])
        .expect("generated assignment");
    assert_eq!(assignment.layout_epoch(), 1);
    assert_eq!(assignment.segments().len(), 2);
    assert!(matches!(
        default_cluster.consumer_assignment(1, [99]),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        default_cluster.consumer_assignment(99, [1]),
        Err(M1FakeError::InvalidAssignment(_))
    ));

    let custom =
        M1FakeCluster::for_topic("topic://tenant/namespace/other").expect("valid alternate topic");
    assert_eq!(custom.topic(), "topic://tenant/namespace/other");
    assert!(matches!(
        M1FakeCluster::for_topic("topic://invalid"),
        Err(M1FakeError::InvalidLayout(_))
    ));

    let mut cluster = M1FakeCluster::two_segment();
    assert!(matches!(
        cluster.open_connection(Endpoint::Segment(99)),
        Err(M1FakeError::UnknownEndpoint(Endpoint::Segment(99)))
    ));
    let plaintext = cluster
        .open_connection(Endpoint::Controller)
        .expect("open plaintext controller connection");
    let tls = cluster
        .open_connection_with_transport(Endpoint::Segment(1), TransportSecurity::Tls)
        .expect("open TLS segment connection");
    assert!(
        cluster
            .take_output(plaintext)
            .expect("empty output")
            .is_empty()
    );
    assert_eq!(cluster.resource_counts().connections, 2);

    let mut ping = command_bytes(&pb::BaseCommand {
        r#type: pb::base_command::Type::Ping as i32,
        ping: Some(pb::CommandPing {}),
        ..Default::default()
    });
    assert!(matches!(
        cluster.handle_bytes(plaintext, &mut ping),
        Err(M1FakeError::HandshakeRequired { .. })
    ));
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        )
        .expect("script known endpoint");
    assert!(matches!(
        cluster.script_next(
            Endpoint::Segment(99),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        ),
        Err(M1FakeError::UnknownEndpoint(Endpoint::Segment(99)))
    ));
    assert!(matches!(
        cluster.complete_pending(PendingOperationId(99), PendingCompletion::Succeed),
        Err(M1FakeError::UnknownPending(PendingOperationId(99)))
    ));

    cluster
        .disconnect_connection(plaintext)
        .expect("disconnect controller");
    assert!(matches!(
        cluster.disconnect_connection(plaintext),
        Err(M1FakeError::Disconnected(id)) if id == plaintext
    ));
    assert!(matches!(
        cluster.disconnect_connection(ConnectionId(99)),
        Err(M1FakeError::UnknownConnection(ConnectionId(99)))
    ));
    assert!(matches!(
        cluster.take_output(plaintext),
        Err(M1FakeError::Disconnected(id)) if id == plaintext
    ));
    assert_eq!(
        cluster
            .disconnect_endpoint(Endpoint::Segment(1))
            .expect("disconnect TLS segment endpoint"),
        1
    );
    assert_eq!(
        cluster
            .disconnect_endpoint(Endpoint::Segment(1))
            .expect("already disconnected endpoint"),
        0
    );
    assert!(matches!(
        cluster.disconnect_endpoint(Endpoint::Segment(99)),
        Err(M1FakeError::UnknownEndpoint(Endpoint::Segment(99)))
    ));
    assert_eq!(cluster.resource_counts().connections, 0);
    assert_ne!(plaintext, tls);
}

#[test]
fn production_connection_adapter_round_trips_real_framing() {
    let mut cluster = M1FakeCluster::default();
    let connection = cluster
        .open_connection(Endpoint::Controller)
        .expect("open controller connection");
    let adapter = M1ConnectionAdapter::new(connection);
    let mut client = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    client
        .begin_handshake()
        .expect("begin production handshake");

    let exchange = adapter
        .exchange(&mut cluster, &mut client, Instant::now())
        .expect("exchange production handshake");
    assert!(exchange.client_bytes > 0);
    assert_eq!(exchange.broker_frames, 1);
    assert!(matches!(
        client.poll_event(),
        Some(ConnectionEvent::Connected { .. })
    ));
    let idle = adapter
        .exchange(&mut cluster, &mut client, Instant::now())
        .expect("idle adapter exchange");
    assert_eq!(idle.client_bytes, 0);
    assert_eq!(idle.broker_frames, 0);
    assert!(!cluster.routes().is_empty());
    assert!(!cluster.broker_frames().is_empty());
    cluster.clear_routes();
    cluster.clear_broker_frames();
    assert!(cluster.routes().is_empty());
    assert!(cluster.broker_frames().is_empty());

    let unknown_adapter = M1ConnectionAdapter::new(ConnectionId(99));
    let mut unknown_client =
        Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    unknown_client
        .begin_handshake()
        .expect("stage another handshake");
    assert!(
        unknown_adapter
            .exchange(&mut cluster, &mut unknown_client, Instant::now())
            .is_err()
    );

    let producer_request_id = client.peek_next_request_id_for_test();
    let producer = client.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/adapter-producer".to_owned(),
        ..Default::default()
    });
    let _ = client.poll_transmit_owned();
    let producer_success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id: producer_request_id,
            producer_name: "adapter-producer".to_owned(),
            last_sequence_id: Some(-1),
            producer_ready: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    client
        .handle_bytes(Instant::now(), &command_bytes(&producer_success))
        .expect("mark adapter producer ready");
    let _ = client.poll_event();
    client
        .producer(producer)
        .cloned()
        .expect("adapter producer slot")
        .queue_send(
            OutgoingMessage {
                payload: Bytes::from_static(b"vectored-adapter-payload"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 24,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            Instant::now() + Duration::from_millis(1),
        )
        .expect("queue vectored adapter send");
    assert!(matches!(
        adapter.exchange(&mut cluster, &mut client, Instant::now()),
        Err(M1AdapterError::Fake(M1FakeError::UnsupportedCommand(
            pb::base_command::Type::Send
        )))
    ));
}

#[test]
fn assignment_plans_are_complete_historical_and_ancestry_aware() {
    let mut cluster = M1FakeCluster::default();
    let controller_a = connect(&mut cluster, Endpoint::Controller);
    let controller_b = connect(&mut cluster, Endpoint::Controller);
    let member_a = register_member(&mut cluster, controller_a, "group", "member-a", 7, 1);
    let member_b = register_member(&mut cluster, controller_b, "group", "member-b", 8, 2);

    assert!(matches!(
        cluster.publish_assignment_plan(1, Vec::new()),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            2,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![FullAssignment::new(MemberId::new(controller_a, 99), [1, 2])]
        ),
        Err(M1FakeError::UnknownMember(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_a, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(1, vec![FullAssignment::new(member_a, [1, 2])]),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1, 1]),
                FullAssignment::new(member_b, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1, 99]),
                FullAssignment::new(member_b, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [1, 2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, []),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));

    cluster
        .publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        )
        .expect("install complete initial assignment");
    assert_eq!(cluster.assigned_owner("group", 1), Some(member_a));
    assert_eq!(cluster.assigned_owner("group", 2), Some(member_b));
    assert!(!take_frames(&mut cluster, controller_a).is_empty());
    assert!(!take_frames(&mut cluster, controller_b).is_empty());
    cluster
        .resend_assignment(member_a)
        .expect("resend current assignment");
    assert!(!take_frames(&mut cluster, controller_a).is_empty());
    assert!(matches!(
        cluster.resend_assignment(MemberId::new(controller_a, 99)),
        Err(M1FakeError::UnknownMember(_))
    ));
    assert!(matches!(
        cluster.ancestry_proof(member_a, 1),
        Err(M1FakeError::InvalidLayout(_))
    ));
    assert_eq!(
        cluster
            .drain_eligibility(member_a, 1)
            .expect("a root has no predecessor barrier"),
        DrainEligibility::Eligible
    );
    assert!(matches!(
        cluster.drain_eligibility(member_a, 2),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.drain_eligibility(MemberId::new(controller_a, 99), 1),
        Err(M1FakeError::UnknownMember(_))
    ));

    assert!(matches!(
        cluster.advance_layout(1, split_layout(2)),
        Err(M1FakeError::InvalidLayout(_))
    ));
    assert!(matches!(
        cluster.publish_early_descendant_assignment_plan(
            2,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    cluster
        .advance_layout(2, split_layout(2))
        .expect("advance to a reciprocal split");
    cluster
        .publish_assignment_plan(
            2,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        )
        .expect("retain parent frontier while it drains");
    let _ = take_frames(&mut cluster, controller_a);
    let _ = take_frames(&mut cluster, controller_b);

    assert!(matches!(
        cluster.push_stale_assignment(member_a, 2, [1]),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.push_stale_assignment(member_a, 1, [1, 1]),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    assert!(matches!(
        cluster.push_stale_assignment(member_a, 1, [99]),
        Err(M1FakeError::UnknownSegment(99))
    ));
    cluster
        .push_stale_assignment(member_a, 1, [1])
        .expect("push retained historical share");
    assert!(!take_frames(&mut cluster, controller_a).is_empty());
    cluster
        .publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        )
        .expect("publish retained complete historical plan");
    assert!(matches!(
        cluster.publish_assignment_plan(
            0,
            vec![
                FullAssignment::new(member_a, [1]),
                FullAssignment::new(member_b, [2]),
            ],
        ),
        Err(M1FakeError::InvalidAssignment(_))
    ));

    let controller_c = connect(&mut cluster, Endpoint::Controller);
    let member_c = register_member(&mut cluster, controller_c, "group", "member-c", 9, 3);
    assert!(matches!(
        cluster.publish_assignment_plan(1, vec![FullAssignment::new(member_c, [1])]),
        Err(M1FakeError::InvalidAssignment(_))
    ));
    cluster
        .publish_early_descendant_assignment_plan(
            2,
            vec![
                FullAssignment::new(member_a, [1, 4]),
                FullAssignment::new(member_b, [2, 3]),
                FullAssignment::new(member_c, []),
            ],
        )
        .expect("assign split descendants before parent completion");
    assert_eq!(
        cluster
            .ancestry_proof(member_a, 4)
            .expect("same-member parent evidence"),
        AncestryProof::LocallyProvable {
            member: member_a,
            parent_ids: vec![1],
        }
    );
    assert!(matches!(
        cluster
            .ancestry_proof(member_b, 3)
            .expect("cross-member parent evidence"),
        AncestryProof::CrossMemberUnprovable { parent_ids, .. } if parent_ids == vec![1]
    ));
    assert_eq!(
        cluster
            .drain_eligibility(member_a, 4)
            .expect("local parent remains incomplete"),
        DrainEligibility::ParentBlocked {
            segment_ids: vec![1],
        }
    );
    assert_eq!(
        cluster
            .drain_eligibility(member_b, 3)
            .expect("cross-member completion is unprovable"),
        DrainEligibility::CrossMemberUnprovable {
            segment_ids: vec![1],
        }
    );
    assert!(matches!(
        cluster.ancestry_proof(member_a, 3),
        Err(M1FakeError::InvalidAssignment(_))
    ));

    let mut unknown = M1FakeCluster::default();
    let registered_controller = connect(&mut unknown, Endpoint::Controller);
    let registered = register_member(
        &mut unknown,
        registered_controller,
        "unknown",
        "registered",
        1,
        1,
    );
    unknown
        .advance_layout(2, split_layout(2))
        .expect("split before publishing an assignment to a pending member");
    unknown
        .advance_layout(3, double_split_layout())
        .expect("split an as-yet-unassigned descendant");
    let pending_controller = connect(&mut unknown, Endpoint::Controller);
    unknown
        .script_next(
            Endpoint::Controller,
            OperationKind::ScalableOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay a second controller member");
    send(
        &mut unknown,
        pending_controller,
        &scalable_subscribe_command(
            TOPIC,
            "unknown",
            "pending",
            2,
            2,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("hold the second member before registration");
    let pending = unknown
        .member("unknown", "pending")
        .expect("pending member is addressable for a pushed assignment");
    unknown
        .publish_early_descendant_assignment_plan(
            3,
            vec![
                FullAssignment::new(registered, [1, 2, 4, 5, 6]),
                FullAssignment::new(pending, [3]),
            ],
        )
        .expect("publish a complete plan while parent ownership is pending");
    assert_eq!(
        unknown
            .ancestry_proof(registered, 5)
            .expect("classify missing parent ownership"),
        AncestryProof::Unknown {
            child_member: registered,
            missing_parent_ids: vec![3],
        }
    );
    assert_eq!(
        unknown
            .drain_eligibility(registered, 5)
            .expect("block a descendant with unknown ancestry"),
        DrainEligibility::UnknownAncestry {
            segment_ids: vec![3],
        }
    );
    let pending_child = connect(&mut unknown, Endpoint::Segment(1));
    let pending_topic = unknown.segment_topic(3).expect("pending parent topic");
    assert!(matches!(
        send(
            &mut unknown,
            pending_child,
            &segment_subscribe_command(
                &pending_topic,
                "unknown",
                "pending",
                3,
                20,
                20,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
}

#[test]
fn complete_layout_validation_is_atomic_and_independently_bounded() {
    assert_invalid_layout(Vec::new());

    let oversized = (0..=4096)
        .map(|offset| M1Segment::active(10_000 + offset, 0, 0, Endpoint::Segment(1), 2))
        .collect();
    assert_invalid_layout(oversized);

    let duplicate = M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0);
    assert_invalid_layout(vec![duplicate.clone(), duplicate]);
    assert_invalid_layout(vec![M1Segment::active(
        2,
        32_768,
        65_535,
        Endpoint::Segment(2),
        0,
    )]);

    let mut changed_identity = root_layout();
    changed_identity[0].hash_end = 32_766;
    assert_invalid_layout(changed_identity);

    let mut changed_active_children = root_layout();
    changed_active_children[0].child_ids.push(3);
    changed_active_children
        .push(M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 2).with_parents([1]));
    assert_invalid_layout(changed_active_children);

    let mut invalid_range = root_layout();
    invalid_range.push(M1Segment::active(3, 2, 1, Endpoint::Segment(1), 2));
    assert_invalid_layout(invalid_range);

    let mut zero_created = root_layout();
    zero_created.push(M1Segment::active(3, 0, 0, Endpoint::Segment(1), 0));
    assert_invalid_layout(zero_created);

    let mut backdated_root = root_layout();
    backdated_root.push(M1Segment::active(3, 0, 0, Endpoint::Segment(1), 1));
    assert_invalid_layout(backdated_root);

    let mut active_with_seal = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.sealed_at_epoch = Some(2);
    active_with_seal.push(malformed);
    assert_invalid_layout(active_with_seal);

    let mut sealed_without_epoch = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.state = pb::SegmentState::Sealed;
    malformed.sealed_at_epoch = None;
    sealed_without_epoch.push(malformed);
    assert_invalid_layout(sealed_without_epoch);

    let mut sealed_in_future = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.state = pb::SegmentState::Sealed;
    malformed.sealed_at_epoch = Some(3);
    sealed_in_future.push(malformed);
    assert_invalid_layout(sealed_in_future);

    let mut missing_placement = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.endpoint = None;
    missing_placement.push(malformed);
    assert_invalid_layout(missing_placement);

    let mut wrong_placement = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.endpoint = Some(Endpoint::Controller);
    wrong_placement.push(malformed);
    assert_invalid_layout(wrong_placement);

    let mut too_many_edges = root_layout();
    let mut malformed = M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2);
    malformed.parent_ids = vec![1; 16_385];
    too_many_edges.push(malformed);
    assert_invalid_layout(too_many_edges);

    let mut repeated_edge = split_layout(2);
    repeated_edge[1].parent_ids.push(1);
    assert_invalid_layout(repeated_edge);

    let mut missing_parent = root_layout();
    missing_parent[0] = missing_parent[0].clone().sealed_at(2);
    missing_parent
        .push(M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 2).with_parents([99]));
    assert_invalid_layout(missing_parent);

    let mut non_reciprocal_parent = root_layout();
    non_reciprocal_parent[0] = non_reciprocal_parent[0].clone().sealed_at(2);
    non_reciprocal_parent
        .push(M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 2).with_parents([1]));
    assert_invalid_layout(non_reciprocal_parent);

    let mut inconsistent_epochs = root_layout();
    inconsistent_epochs[0] = inconsistent_epochs[0]
        .clone()
        .with_children([3])
        .sealed_at(2);
    inconsistent_epochs
        .push(M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 1).with_parents([1]));
    assert_invalid_layout(inconsistent_epochs);

    let mut missing_child = root_layout();
    missing_child[0] = missing_child[0].clone().with_children([99]).sealed_at(2);
    assert_invalid_layout(missing_child);

    let mut non_reciprocal_child = root_layout();
    non_reciprocal_child[0] = non_reciprocal_child[0]
        .clone()
        .with_children([3])
        .sealed_at(2);
    non_reciprocal_child.push(M1Segment::active(3, 0, 32_767, Endpoint::Segment(1), 2));
    assert_invalid_layout(non_reciprocal_child);

    let cycle = vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
        M1Segment::active(3, 0, 0, Endpoint::Segment(1), 2)
            .with_parents([4])
            .with_children([4])
            .sealed_at(2),
        M1Segment::active(4, 0, 0, Endpoint::Segment(1), 2)
            .with_parents([3])
            .with_children([3])
            .sealed_at(2),
    ];
    assert_invalid_layout(cycle);

    let mut deep = Vec::new();
    deep.push(
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3])
            .sealed_at(2),
    );
    deep.push(M1Segment::active(
        2,
        32_768,
        65_535,
        Endpoint::Segment(2),
        0,
    ));
    for offset in 0..=256_u64 {
        let id = 3 + offset;
        let parent = if offset == 0 { 1 } else { id - 1 };
        let mut segment =
            M1Segment::active(id, 0, 32_767, Endpoint::Segment(1), 2).with_parents([parent]);
        if offset < 256 {
            segment = segment.with_children([id + 1]).sealed_at(2);
        }
        deep.push(segment);
    }
    assert_invalid_layout(deep);

    let mut missing_coverage = root_layout();
    missing_coverage[0] = missing_coverage[0].clone().sealed_at(2);
    assert_invalid_layout(missing_coverage);

    let mut gap = root_layout();
    gap[0] = gap[0].clone().sealed_at(2);
    gap.push(M1Segment::active(3, 0, 16_000, Endpoint::Segment(1), 2));
    assert_invalid_layout(gap);

    let mut overlap = root_layout();
    overlap.push(M1Segment::active(3, 0, 1, Endpoint::Segment(1), 2));
    assert_invalid_layout(overlap);

    let split_conflict = vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0)
            .with_children([3, 5])
            .sealed_at(2),
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2).with_parents([1, 2]),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(1), 2).with_parents([1]),
        M1Segment::active(5, 32_768, 65_535, Endpoint::Segment(2), 2).with_parents([2]),
    ];
    assert_invalid_layout(split_conflict);

    let one_to_one_range_change = vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
        M1Segment::active(3, 0, 16_000, Endpoint::Segment(1), 2).with_parents([1]),
        M1Segment::active(4, 16_001, 32_767, Endpoint::Segment(1), 2),
    ];
    assert_invalid_layout(one_to_one_range_change);

    let mut cluster = M1FakeCluster::default();
    cluster
        .advance_layout(2, split_layout(2))
        .expect("valid split still succeeds after hostile matrix");
    assert_eq!(cluster.layout_epoch(), 2);
    assert!(matches!(
        cluster.enqueue_message(1, Bytes::from_static(b"sealed")),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    let mut rewritten = split_layout(2);
    rewritten[1].parent_ids.clear();
    assert!(matches!(
        cluster.advance_layout(3, rewritten),
        Err(M1FakeError::InvalidLayout(_))
    ));
    let mut rewritten = split_layout(2);
    rewritten[0].sealed_at_epoch = Some(3);
    assert!(matches!(
        cluster.advance_layout(3, rewritten),
        Err(M1FakeError::InvalidLayout(_))
    ));

    let mut undrained = M1FakeCluster::default();
    let controller = connect(&mut undrained, Endpoint::Controller);
    let _member = register_member(&mut undrained, controller, "gc", "member", 1, 1);
    undrained
        .advance_layout(2, split_layout(2))
        .expect("split before an undrained GC attempt");
    assert!(matches!(
        undrained.advance_layout(3, garbage_collected_split_layout()),
        Err(M1FakeError::InvalidLayout(_))
    ));

    let mut reintroduced = M1FakeCluster::default();
    reintroduced
        .advance_layout(2, split_layout(2))
        .expect("split before GC");
    reintroduced
        .advance_layout(3, garbage_collected_split_layout())
        .expect("GC a sealed segment without durable groups");
    let mut resurrected = garbage_collected_split_layout();
    resurrected.push(
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
    );
    let reintroduction = reintroduced
        .advance_layout(4, resurrected)
        .expect_err("garbage-collected segment cannot be reintroduced");
    assert!(
        matches!(&reintroduction, M1FakeError::InvalidLayout(reason) if reason.contains("reintroduced")),
        "unexpected reintroduction error: {reintroduction:?}"
    );

    let split_gap = vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
        M1Segment::active(3, 0, 10_000, Endpoint::Segment(1), 2).with_parents([1]),
        M1Segment::active(5, 10_001, 19_999, Endpoint::Segment(1), 2),
        M1Segment::active(4, 20_000, 32_767, Endpoint::Segment(1), 2).with_parents([1]),
    ];
    assert_invalid_layout(split_gap);

    let merge_mismatch = vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0)
            .with_children([3])
            .sealed_at(2),
        M1Segment::active(3, 0, 60_000, Endpoint::Segment(1), 2).with_parents([1, 2]),
        M1Segment::active(4, 60_001, 65_535, Endpoint::Segment(2), 2),
    ];
    assert_invalid_layout(merge_mismatch);

    let huge_topic = format!("topic://public/default/{}", "x".repeat(MAX_FRAME_SIZE));
    let mut huge = M1FakeCluster::for_topic(huge_topic).expect("valid oversized topic name");
    assert!(matches!(
        huge.advance_layout(2, split_layout(2)),
        Err(M1FakeError::InvalidLayout(_))
    ));
}

#[test]
fn hostile_wire_commands_never_reroute_or_leak_controller_state() {
    let mut cluster = M1FakeCluster::default();
    let payload_connect = cluster
        .open_connection(Endpoint::Controller)
        .expect("open payload-bearing CONNECT endpoint");
    assert!(matches!(
        send_with_payload(
            &mut cluster,
            payload_connect,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Connect as i32,
                connect: Some(pb::CommandConnect {
                    feature_flags: Some(pb::FeatureFlags {
                        supports_scalable_topics: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let unconnected = cluster
        .open_connection(Endpoint::Segment(1))
        .expect("open unconnected child endpoint");
    assert!(matches!(
        send(&mut cluster, unconnected, &flow_command(1, 1)),
        Err(M1FakeError::HandshakeRequired { .. })
    ));

    let child = connect(&mut cluster, Endpoint::Segment(1));
    let topic_one = cluster.segment_topic(1).expect("segment one topic");
    let topic_two = cluster.segment_topic(2).expect("segment two topic");
    for controller_command in vec![
        scalable_lookup_command(90, TOPIC),
        scalable_close_command(90),
        scalable_subscribe_command(
            TOPIC,
            "wrong-endpoint",
            "wrong-endpoint",
            90,
            90,
            pb::ScalableConsumerType::Stream as i32,
        ),
        tc_connect_command(90),
        new_transaction_command(90),
        add_subscription_to_transaction_command(
            90,
            magnetar_proto::TxnId::new(0, 90),
            &topic_one,
            "wrong-endpoint",
        ),
        end_transaction_command(90, magnetar_proto::TxnId::new(0, 90), pb::TxnAction::Abort),
    ]
    .into_boxed_slice()
    {
        assert!(matches!(
            send(&mut cluster, child, &controller_command),
            Err(M1FakeError::WrongEndpoint { .. })
        ));
    }
    assert!(matches!(
        send(&mut cluster, child, &lookup_command(&topic_one, 1)),
        Err(M1FakeError::WrongEndpoint {
            expected: Endpoint::Controller,
            actual: Endpoint::Segment(1),
            ..
        })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic_two,
                "sub",
                "member",
                2,
                2,
                2,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::WrongEndpoint {
            expected: Endpoint::Segment(2),
            actual: Endpoint::Segment(1),
            ..
        })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic_one,
                "sub",
                "member",
                1,
                3,
                3,
                pb::command_subscribe::SubType::Shared,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            },
        ),
        Err(M1FakeError::UnsupportedCommand(
            pb::base_command::Type::Pong
        ))
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &pb::BaseCommand {
                r#type: i32::MAX,
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    assert!(matches!(
        send(
            &mut cluster,
            child,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Connect as i32,
                connect: Some(pb::CommandConnect::default()),
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Ping as i32,
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(
        &mut cluster,
        child,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Ping as i32,
            ping: Some(pb::CommandPing {}),
            ..Default::default()
        },
    )
    .expect("valid PING");
    assert!(take_frames(&mut cluster, child)[0].command.pong.is_some());

    let controller_without_features = cluster
        .open_connection(Endpoint::Controller)
        .expect("open controller without features");
    assert!(matches!(
        send(
            &mut cluster,
            controller_without_features,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Connect as i32,
                connect: Some(pb::CommandConnect::default()),
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let proxied = cluster
        .open_connection(Endpoint::Segment(1))
        .expect("open direct endpoint");
    assert!(matches!(
        send(
            &mut cluster,
            proxied,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Connect as i32,
                connect: Some(pb::CommandConnect {
                    proxy_to_broker_url: Some("pulsar://other:6650".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    let rejecting_config = M1FakeConfig::new(TOPIC)
        .expect("valid auth config")
        .with_auth_validator(|_| false);
    let mut rejecting = M1FakeCluster::from_config(rejecting_config).expect("valid auth fixture");
    let rejected = rejecting
        .open_connection_with_transport(Endpoint::Segment(1), TransportSecurity::Tls)
        .expect("open auth-rejected endpoint");
    assert!(matches!(
        send(
            &mut rejecting,
            rejected,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::Connect as i32,
                connect: Some(pb::CommandConnect {
                    auth_method_name: Some("token".to_owned()),
                    auth_data: Some(Bytes::from_static(b"redacted")),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        Err(M1FakeError::AuthenticationRejected {
            endpoint: Endpoint::Segment(1)
        })
    ));

    let controller = connect(&mut cluster, Endpoint::Controller);
    send(
        &mut cluster,
        controller,
        &scalable_lookup_command(40, "topic://public/default/other"),
    )
    .expect("unknown scalable lookup receives a response");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .scalable_topic_update
            .as_ref()
            .is_some_and(|update| update.error.is_some())
    );
    send(
        &mut cluster,
        controller,
        &scalable_lookup_command(41, TOPIC),
    )
    .expect("open layout session");
    assert_eq!(take_frames(&mut cluster, controller).len(), 2);
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &scalable_lookup_command(41, TOPIC),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    cluster
        .resend_layout(controller, 41)
        .expect("resend known layout session");
    assert!(!take_frames(&mut cluster, controller).is_empty());
    assert!(matches!(
        cluster.resend_layout(child, 41),
        Err(M1FakeError::WrongEndpoint { .. })
    ));
    assert!(matches!(
        cluster.push_stale_layout(controller, 41, 1),
        Err(M1FakeError::InvalidLayout(_))
    ));
    cluster
        .advance_layout(2, split_layout(2))
        .expect("advance an observed layout session");
    assert!(matches!(
        cluster.push_stale_layout(controller, 41, 0),
        Err(M1FakeError::InvalidLayout(_))
    ));
    cluster
        .push_stale_layout(controller, 41, 1)
        .expect("push a retained historical layout");
    let _ = take_frames(&mut cluster, controller);
    send(&mut cluster, controller, &scalable_close_command(41)).expect("close layout session");
    send(&mut cluster, controller, &scalable_close_command(41))
        .expect("layout close is idempotent");
    assert!(cluster.layout_session_ids().is_empty());
    assert!(cluster.resend_layout(controller, 41).is_err());

    send(
        &mut cluster,
        controller,
        &scalable_subscribe_command(
            "topic://public/default/other",
            "sub",
            "unknown-topic",
            100,
            100,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("unknown topic returns scalable failure");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(TOPIC, "sub", "bad-type", 101, 101, i32::MAX),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &scalable_subscribe_command(
                TOPIC,
                "sub",
                "checkpoint",
                102,
                102,
                pb::ScalableConsumerType::Checkpoint as i32,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let _member = register_member(&mut cluster, controller, "sub", "member", 103, 103);
    send(
        &mut cluster,
        controller,
        &scalable_subscribe_command(
            TOPIC,
            "sub",
            "member",
            104,
            104,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("duplicate member name returns ConsumerBusy");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .is_some_and(|response| response.error == Some(pb::ServerError::ConsumerBusy as i32))
    );
}

#[test]
fn malformed_transaction_and_child_commands_fail_before_mutating_fake_state() {
    let mut cluster = M1FakeCluster::default();
    let controller = connect(&mut cluster, Endpoint::Controller);
    let child = connect(&mut cluster, Endpoint::Segment(1));

    assert!(matches!(
        send_with_payload(&mut cluster, controller, &tc_connect_command(90)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send_with_payload(&mut cluster, controller, &new_transaction_command(91)),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    for command_type in [
        pb::base_command::Type::ScalableTopicLookup,
        pb::base_command::Type::ScalableTopicClose,
        pb::base_command::Type::ScalableTopicSubscribe,
        pb::base_command::Type::Lookup,
        pb::base_command::Type::TcClientConnectRequest,
        pb::base_command::Type::NewTxn,
        pb::base_command::Type::AddSubscriptionToTxn,
        pb::base_command::Type::EndTxn,
    ] {
        assert!(matches!(
            send(
                &mut cluster,
                controller,
                &pb::BaseCommand {
                    r#type: command_type as i32,
                    ..Default::default()
                },
            ),
            Err(M1FakeError::InvalidCommand { .. })
        ));
    }
    for command_type in [
        pb::base_command::Type::Subscribe,
        pb::base_command::Type::GetSchema,
        pb::base_command::Type::Flow,
        pb::base_command::Type::Ack,
        pb::base_command::Type::RedeliverUnacknowledgedMessages,
        pb::base_command::Type::Seek,
        pb::base_command::Type::CloseConsumer,
    ] {
        assert!(matches!(
            send(
                &mut cluster,
                child,
                &pb::BaseCommand {
                    r#type: command_type as i32,
                    ..Default::default()
                },
            ),
            Err(M1FakeError::InvalidCommand { .. })
        ));
    }

    send(
        &mut cluster,
        controller,
        &lookup_command("persistent://public/default/not-active", 1),
    )
    .expect("unknown ordinary lookup returns failure");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .lookup_topic_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );

    let mut invalid_tc = tc_connect_command(2);
    invalid_tc
        .tc_client_connect_request
        .as_mut()
        .expect("TC connect body")
        .tc_id = 1;
    assert!(matches!(
        send(&mut cluster, controller, &invalid_tc),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(&mut cluster, controller, &tc_connect_command(3)).expect("valid TC connect");
    let _ = take_frames(&mut cluster, controller);
    let mut missing_ttl = new_transaction_command(4);
    missing_ttl
        .new_txn
        .as_mut()
        .expect("new transaction body")
        .txn_ttl_millis = None;
    assert!(matches!(
        send(&mut cluster, controller, &missing_ttl),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(&mut cluster, controller, &new_transaction_command(5)).expect("allocate wire txn");
    let txn = opened_transaction(&take_frames(&mut cluster, controller));

    let member = register_member(&mut cluster, controller, "wire-sub", "wire-member", 10, 10);
    let topic = cluster.segment_topic(1).expect("segment topic");
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &get_schema_command("persistent://public/default/not-a-segment", 9),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    for malformed_attachment in [
        "segment://public/default/scaled/0000-7fff",
        "segment://public/default/scaled/0000",
        "segment://public/default/scaled/zzzz-7fff-1",
        "segment://public/default/scaled/0000-zzzz-1",
        "segment://public/default/scaled/0000-7fff-nope",
        "segment://public/default/scaled/0000-7fff-not-a-number",
        "segment://public/default/scaled/0000-7fff-1-extra",
        "segment://public/default/scaled/0-7fff-1",
    ] {
        assert!(matches!(
            send(
                &mut cluster,
                child,
                &get_schema_command(malformed_attachment, 9),
            ),
            Err(M1FakeError::InvalidCommand { .. })
        ));
    }
    assert!(matches!(
        send(&mut cluster, controller, &get_schema_command(&topic, 10)),
        Err(M1FakeError::WrongEndpoint { .. })
    ));
    send(&mut cluster, child, &get_schema_command(&topic, 11)).expect("resolve segment schema");
    let schema = take_frames(&mut cluster, child)
        .into_iter()
        .find_map(|frame| frame.command.get_schema_response)
        .expect("schema response");
    assert_eq!(schema.request_id, 11);
    assert_eq!(schema.schema_version.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(
        schema.schema.expect("resolved schema").r#type,
        pb::schema::Type::None as i32
    );
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                "persistent://public/default/not-a-segment",
                "wire-sub",
                "wire-member",
                1,
                11,
                11,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "unassigned-sub",
                "wire-member",
                1,
                11,
                12,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "wire-sub",
                "wrong-name",
                1,
                11,
                13,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "wire-sub",
            "wire-member",
            1,
            11,
            14,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open child for wire validation");
    let _ = take_frames(&mut cluster, child);
    assert!(matches!(
        send_with_payload(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(92, txn, &topic, "wire-sub"),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send_with_payload(
            &mut cluster,
            controller,
            &end_transaction_command(93, txn, pb::TxnAction::Abort),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut empty_registration =
        add_subscription_to_transaction_command(94, txn, &topic, "wire-sub");
    empty_registration
        .add_subscription_to_txn
        .as_mut()
        .expect("registration body")
        .subscription
        .clear();
    assert!(matches!(
        send(&mut cluster, controller, &empty_registration),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let topic_two = cluster.segment_topic(2).expect("segment two topic");
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(95, txn, &topic_two, "wire-sub"),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut missing_action = end_transaction_command(96, txn, pb::TxnAction::Abort);
    missing_action
        .end_txn
        .as_mut()
        .expect("EndTxn body")
        .txn_action = None;
    assert!(matches!(
        send(&mut cluster, controller, &missing_action),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut partial_end = end_transaction_command(196, txn, pb::TxnAction::Abort);
    partial_end
        .end_txn
        .as_mut()
        .expect("EndTxn body")
        .txnid_least_bits = None;
    assert!(matches!(
        send(&mut cluster, controller, &partial_end),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &end_transaction_command(97, magnetar_proto::TxnId::new(99, 99), pb::TxnAction::Abort,),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(&mut cluster, child, &{
            let mut command = segment_subscribe_command(
                &topic,
                "wire-sub",
                "wire-member",
                1,
                11,
                113,
                pb::command_subscribe::SubType::Exclusive,
            );
            command
                .subscribe
                .as_mut()
                .expect("Subscribe body")
                .consumer_name = None;
            command
        },),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &segment_subscribe_command(
                &topic,
                "wire-sub",
                "wire-member",
                1,
                11,
                15,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(&mut cluster, child, &flow_command(999, 1)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    for unknown_child_command in vec![
        ack_command(999, 114, pb::MessageIdData::default()),
        redeliver_command(999, pb::MessageIdData::default()),
        seek_command(999, 115, pb::MessageIdData::default()),
        close_command(999, 116),
    ]
    .into_boxed_slice()
    {
        assert!(matches!(
            send(&mut cluster, child, &unknown_child_command),
            Err(M1FakeError::InvalidCommand { .. })
        ));
    }

    let competing = connect(&mut cluster, Endpoint::Segment(1));
    send(
        &mut cluster,
        competing,
        &segment_subscribe_command(
            &topic,
            "wire-sub",
            "wire-member",
            1,
            12,
            16,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("exclusive conflict returns broker error");
    assert!(
        take_frames(&mut cluster, competing)[0]
            .command
            .error
            .as_ref()
            .is_some_and(|error| error.error == pb::ServerError::ConsumerBusy as i32)
    );

    for payload in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ] {
        cluster
            .enqueue_message(1, Bytes::copy_from_slice(payload))
            .expect("enqueue wire message");
    }
    send(&mut cluster, child, &flow_command(11, 3)).expect("deliver wire messages");
    let delivered = take_frames(&mut cluster, child)
        .into_iter()
        .filter_map(|frame| frame.command.message.map(|message| message.message_id))
        .collect::<Vec<_>>();
    assert_eq!(delivered.len(), 3);

    let mut publish_time_seek = seek_command(11, 20, delivered[0].clone());
    publish_time_seek
        .seek
        .as_mut()
        .expect("seek body")
        .message_publish_time = Some(1);
    assert!(matches!(
        send(&mut cluster, child, &publish_time_seek),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut wrong_segment_seek = delivered[0].clone();
    wrong_segment_seek.ledger_id = 2;
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &seek_command(11, 21, wrong_segment_seek)
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut beyond_seek = delivered[0].clone();
    beyond_seek.entry_id = 99;
    assert!(matches!(
        send(&mut cluster, child, &seek_command(11, 22, beyond_seek)),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    let mut partial_txn = ack_command(11, 30, delivered[0].clone());
    partial_txn.ack.as_mut().expect("ACK body").txnid_most_bits = Some(0);
    assert!(matches!(
        send(&mut cluster, child, &partial_txn),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut no_request = transactional_ack_command(11, 31, txn, delivered[0].clone());
    no_request.ack.as_mut().expect("ACK body").request_id = None;
    assert!(matches!(
        send(&mut cluster, child, &no_request),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &transactional_ack_command(11, 32, txn, delivered[0].clone()),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut unknown_type = ack_command(11, 33, delivered[0].clone());
    unknown_type.ack.as_mut().expect("ACK body").ack_type = i32::MAX;
    assert!(matches!(
        send(&mut cluster, child, &unknown_type),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut empty_ack = ack_command(11, 34, delivered[0].clone());
    empty_ack.ack.as_mut().expect("ACK body").message_id.clear();
    assert!(matches!(
        send(&mut cluster, child, &empty_ack),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut cumulative = ack_command(11, 35, delivered[0].clone());
    let cumulative_body = cumulative.ack.as_mut().expect("ACK body");
    cumulative_body.ack_type = pb::command_ack::AckType::Cumulative as i32;
    cumulative_body.message_id.push(delivered[1].clone());
    assert!(matches!(
        send(&mut cluster, child, &cumulative),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut wrong_ledger = ack_command(11, 36, delivered[0].clone());
    wrong_ledger.ack.as_mut().expect("ACK body").message_id[0].ledger_id = 2;
    assert!(matches!(
        send(&mut cluster, child, &wrong_ledger),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut wrong_identity = ack_command(11, 37, delivered[0].clone());
    wrong_identity.ack.as_mut().expect("ACK body").message_id[0].partition = Some(1);
    assert!(matches!(
        send(&mut cluster, child, &wrong_identity),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut duplicate = ack_command(11, 38, delivered[0].clone());
    duplicate
        .ack
        .as_mut()
        .expect("ACK body")
        .message_id
        .push(delivered[0].clone());
    assert!(matches!(
        send(&mut cluster, child, &duplicate),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut wrong_first_chunk = delivered[1].clone();
    wrong_first_chunk.first_chunk_message_id = Some(Box::new(pb::MessageIdData {
        ledger_id: 2,
        entry_id: delivered[0].entry_id,
        partition: Some(-1),
        ..Default::default()
    }));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &ack_command(11, 138, wrong_first_chunk)
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let mut reversed_chunk_range = delivered[1].clone();
    reversed_chunk_range.first_chunk_message_id = Some(Box::new(pb::MessageIdData {
        ledger_id: 1,
        entry_id: delivered[2].entry_id,
        partition: Some(-1),
        ..Default::default()
    }));
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &ack_command(11, 139, reversed_chunk_range)
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    let mut silent_ack = ack_command(11, 39, delivered[0].clone());
    silent_ack.ack.as_mut().expect("ACK body").request_id = None;
    send(&mut cluster, child, &silent_ack).expect("silent individual ACK");
    assert!(take_frames(&mut cluster, child).is_empty());
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &ack_command(11, 40, delivered[0].clone()),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    let mut redeliver_all = redeliver_command(11, delivered[1].clone());
    let mut wrong_redelivery = redeliver_command(11, delivered[1].clone());
    wrong_redelivery
        .redeliver_unacknowledged_messages
        .as_mut()
        .expect("redelivery body")
        .message_ids[0]
        .ledger_id = 2;
    assert!(matches!(
        send(&mut cluster, child, &wrong_redelivery),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    redeliver_all
        .redeliver_unacknowledged_messages
        .as_mut()
        .expect("redelivery body")
        .message_ids
        .clear();
    send(&mut cluster, child, &redeliver_all).expect("redeliver all unacked messages");

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Close,
            ScriptedBehavior::Delay,
        )
        .expect("delay close for fencing");
    send(&mut cluster, child, &close_command(11, 41)).expect("hold close");
    assert!(matches!(
        send(&mut cluster, child, &flow_command(11, 1)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(&mut cluster, child, &close_command(11, 42)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    cluster
        .disconnect_connection(child)
        .expect("disconnect cancels delayed close");
    assert!(cluster.pending_operations().is_empty());
    assert!(matches!(
        send(&mut cluster, child, &flow_command(11, 1)),
        Err(M1FakeError::Disconnected(_))
    ));
    assert!(matches!(
        cluster.disconnect_connection(child),
        Err(M1FakeError::Disconnected(_))
    ));

    let pending_controller = connect(&mut cluster, Endpoint::Controller);
    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::ScalableOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay disconnecting scalable open");
    send(
        &mut cluster,
        pending_controller,
        &scalable_subscribe_command(
            TOPIC,
            "disconnect-sub",
            "disconnect-member",
            50,
            50,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("hold scalable open until disconnect");
    assert_eq!(cluster.pending_operations().len(), 1);
    cluster
        .disconnect_connection(pending_controller)
        .expect("disconnect cancels scalable open");
    assert!(cluster.pending_operations().is_empty());
    assert!(
        cluster
            .member("disconnect-sub", "disconnect-member")
            .is_none()
    );
    assert_eq!(cluster.assigned_owner("wire-sub", 1), Some(member));

    let mut unplaced = M1FakeCluster::default();
    let mut layout = split_layout(2);
    layout[0].endpoint = None;
    unplaced
        .advance_layout(2, layout)
        .expect("sealed segment may omit its old serving placement");
    let unplaced_child = connect(&mut unplaced, Endpoint::Segment(1));
    let sealed_topic = unplaced.segment_topic(1).expect("sealed segment topic");
    assert!(matches!(
        send(
            &mut unplaced,
            unplaced_child,
            &get_schema_command(&sealed_topic, 98),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert!(matches!(
        send(
            &mut unplaced,
            unplaced_child,
            &segment_subscribe_command(
                &sealed_topic,
                "unplaced-sub",
                "unplaced-member",
                1,
                98,
                98,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
}

#[test]
fn delayed_child_operations_commit_only_after_explicit_completion() {
    let mut cluster = M1FakeCluster::default();
    let controller = connect(&mut cluster, Endpoint::Controller);

    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::ScalableOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay scalable open");
    send(
        &mut cluster,
        controller,
        &scalable_subscribe_command(
            TOPIC,
            "delayed-sub",
            "failed-member",
            1,
            1,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("hold scalable open");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            delayed,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::ServiceNotReady,
                "scripted scalable failure",
            )),
        )
        .expect("fail delayed scalable open");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .scalable_topic_subscribe_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );

    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::ScalableOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay successful scalable open");
    send(
        &mut cluster,
        controller,
        &scalable_subscribe_command(
            TOPIC,
            "delayed-sub",
            "member",
            2,
            2,
            pb::ScalableConsumerType::Stream as i32,
        ),
    )
    .expect("hold replacement scalable open");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(delayed, PendingCompletion::Succeed)
        .expect("complete scalable open");
    let _ = take_frames(&mut cluster, controller);

    let child = connect(&mut cluster, Endpoint::Segment(1));
    let topic = cluster.segment_topic(1).expect("segment topic");
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::SegmentOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay child open");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "delayed-sub",
            "member",
            1,
            10,
            10,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("hold child open");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            delayed,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::ServiceNotReady,
                "scripted child-open failure",
            )),
        )
        .expect("fail child open");
    assert!(take_frames(&mut cluster, child)[0].command.error.is_some());

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::SegmentOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay child-open retry");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "delayed-sub",
            "member",
            1,
            11,
            11,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("hold child-open retry");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(delayed, PendingCompletion::Succeed)
        .expect("complete child open");
    let _ = take_frames(&mut cluster, child);

    cluster
        .enqueue_message(1, Bytes::from_static(b"delayed-operations"))
        .expect("enqueue child message");
    send(&mut cluster, child, &flow_command(11, 1)).expect("grant one permit");
    let delivered = take_frames(&mut cluster, child);
    let delivered_id = message_id(&delivered);

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        )
        .expect("delay acknowledgement");
    send(
        &mut cluster,
        child,
        &ack_command(11, 12, delivered_id.clone()),
    )
    .expect("hold acknowledgement");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            delayed,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "scripted acknowledgement failure",
            )),
        )
        .expect("fail acknowledgement");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .ack_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        )
        .expect("delay acknowledgement retry");
    send(
        &mut cluster,
        child,
        &ack_command(11, 13, delivered_id.clone()),
    )
    .expect("hold acknowledgement retry");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(delayed, PendingCompletion::Succeed)
        .expect("complete acknowledgement retry");
    let _ = take_frames(&mut cluster, child);

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Seek,
            ScriptedBehavior::Delay,
        )
        .expect("delay seek");
    send(
        &mut cluster,
        child,
        &seek_command(11, 14, delivered_id.clone()),
    )
    .expect("hold seek");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            delayed,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "scripted seek failure",
            )),
        )
        .expect("fail seek");
    assert!(take_frames(&mut cluster, child)[0].command.error.is_some());

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Seek,
            ScriptedBehavior::Delay,
        )
        .expect("delay successful seek");
    send(&mut cluster, child, &seek_command(11, 15, delivered_id)).expect("hold successful seek");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(delayed, PendingCompletion::Succeed)
        .expect("complete seek");
    let _ = take_frames(&mut cluster, child);

    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "delayed-sub",
            "member",
            1,
            12,
            16,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("reopen after seek");
    let _ = take_frames(&mut cluster, child);
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Close,
            ScriptedBehavior::Delay,
        )
        .expect("delay close");
    send(&mut cluster, child, &close_command(12, 17)).expect("hold close");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            delayed,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::ServiceNotReady,
                "scripted close failure",
            )),
        )
        .expect("fail close");
    assert!(take_frames(&mut cluster, child)[0].command.error.is_some());
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Close,
            ScriptedBehavior::Delay,
        )
        .expect("delay close retry");
    send(&mut cluster, child, &close_command(12, 18)).expect("hold close retry");
    let delayed = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(delayed, PendingCompletion::Succeed)
        .expect("complete close retry");
    let _ = take_frames(&mut cluster, child);
    assert_eq!(cluster.resource_counts().pending_operations, 0);
    assert_eq!(cluster.resource_counts().child_consumers, 0);

    let mut duplicate = M1FakeCluster::default();
    let mut colocated = root_layout();
    colocated[1].endpoint = Some(Endpoint::Segment(1));
    duplicate
        .advance_layout(2, colocated)
        .expect("co-locate child endpoints for an id-collision scenario");
    let controller = connect(&mut duplicate, Endpoint::Controller);
    let _member = register_member(&mut duplicate, controller, "duplicate", "member", 1, 1);
    let child = connect(&mut duplicate, Endpoint::Segment(1));
    let topic_one = duplicate.segment_topic(1).expect("segment one topic");
    let topic_two = duplicate.segment_topic(2).expect("segment two topic");
    duplicate
        .script_next(
            Endpoint::Segment(1),
            OperationKind::SegmentOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay the first use of a child id");
    send(
        &mut duplicate,
        child,
        &segment_subscribe_command(
            &topic_one,
            "duplicate",
            "member",
            1,
            77,
            2,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("reserve the child id");
    assert!(matches!(
        send(
            &mut duplicate,
            child,
            &segment_subscribe_command(
                &topic_two,
                "duplicate",
                "member",
                2,
                77,
                3,
                pb::command_subscribe::SubType::Exclusive,
            ),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
}

#[test]
fn stale_child_operations_are_fenced_across_assignment_and_controller_changes() {
    let mut retry_cluster = M1FakeCluster::default();
    let retry_controller = connect(&mut retry_cluster, Endpoint::Controller);
    let _retry_member = register_member(
        &mut retry_cluster,
        retry_controller,
        "retry-sub",
        "retry-member",
        1,
        1,
    );
    retry_cluster
        .disconnect_connection(retry_controller)
        .expect("disconnect assigned retry member");
    let retry_child = connect(&mut retry_cluster, Endpoint::Segment(1));
    let retry_topic = retry_cluster.segment_topic(1).expect("retry segment topic");
    send(
        &mut retry_cluster,
        retry_child,
        &segment_subscribe_command(
            &retry_topic,
            "retry-sub",
            "retry-member",
            1,
            11,
            2,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("reconnecting former owner receives ConsumerBusy");
    assert!(
        take_frames(&mut retry_cluster, retry_child)[0]
            .command
            .error
            .as_ref()
            .is_some_and(|error| error.error == pb::ServerError::ConsumerBusy as i32)
    );

    let mut cluster = M1FakeCluster::default();
    let controller_a = connect(&mut cluster, Endpoint::Controller);
    let controller_b = connect(&mut cluster, Endpoint::Controller);
    let member_a = register_member(&mut cluster, controller_a, "fence-sub", "member-a", 1, 1);
    let member_b = register_member(&mut cluster, controller_b, "fence-sub", "member-b", 2, 2);
    cluster
        .publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1, 2]),
                FullAssignment::new(member_b, []),
            ],
        )
        .expect("assign initial fencing owner");
    let _ = take_frames(&mut cluster, controller_a);
    let _ = take_frames(&mut cluster, controller_b);

    let child = connect(&mut cluster, Endpoint::Segment(1));
    let topic = cluster.segment_topic(1).expect("fencing segment topic");
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::SegmentOpen,
            ScriptedBehavior::Delay,
        )
        .expect("delay child open before reassignment");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "fence-sub",
            "member-a",
            1,
            11,
            3,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("hold child open");
    let stale_open = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::SegmentOpen)
        .expect("delayed child open")
        .id;
    cluster
        .publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [2]),
                FullAssignment::new(member_b, [1]),
            ],
        )
        .expect("move segment while child open is pending");
    assert!(matches!(
        cluster.complete_pending(stale_open, PendingCompletion::Succeed),
        Err(M1FakeError::StalePending(id)) if id == stale_open
    ));

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::SegmentOpen,
            ScriptedBehavior::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "scripted segment open failure",
            )),
        )
        .expect("fail one current-owner child open");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "fence-sub",
            "member-b",
            1,
            12,
            4,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("return scripted segment-open failure");
    assert!(take_frames(&mut cluster, child)[0].command.error.is_some());
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "fence-sub",
            "member-b",
            1,
            12,
            5,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open current-owner child");
    let _ = take_frames(&mut cluster, child);
    cluster
        .enqueue_message(1, Bytes::from_static(b"stale-operations"))
        .expect("enqueue stale-operation delivery");
    send(&mut cluster, child, &flow_command(12, 1)).expect("FLOW stale-operation delivery");
    let delivered = message_id(&take_frames(&mut cluster, child));

    cluster
        .publish_assignment_plan(
            1,
            vec![
                FullAssignment::new(member_a, [1, 2]),
                FullAssignment::new(member_b, []),
            ],
        )
        .expect("move segment away from active child");
    assert!(matches!(
        send(&mut cluster, child, &flow_command(12, 1)),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        )
        .expect("delay stale acknowledgement");
    send(&mut cluster, child, &ack_command(12, 6, delivered.clone()))
        .expect("hold stale acknowledgement");
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Close,
            ScriptedBehavior::Delay,
        )
        .expect("delay stale close");
    send(&mut cluster, child, &close_command(12, 7)).expect("hold stale close");
    assert!(matches!(
        send(&mut cluster, child, &seek_command(12, 8, delivered)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let pending_ack = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::Ack)
        .expect("delayed stale acknowledgement")
        .id;
    let pending_close = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::Close)
        .expect("delayed stale close")
        .id;
    cluster
        .disconnect_connection(controller_b)
        .expect("disconnect stale child controller");
    assert!(matches!(
        cluster.complete_pending(pending_ack, PendingCompletion::Succeed),
        Err(M1FakeError::StalePending(id)) if id == pending_ack
    ));
    assert!(matches!(
        cluster.complete_pending(pending_close, PendingCompletion::Succeed),
        Err(M1FakeError::StalePending(id)) if id == pending_close
    ));
    assert!(matches!(
        send(&mut cluster, child, &flow_command(12, 1)),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(&mut cluster, child, &close_command(12, 9)).expect("close stale child after fencing");
    let _ = take_frames(&mut cluster, child);

    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "fence-sub",
            "member-a",
            1,
            13,
            10,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open replacement child");
    let _ = take_frames(&mut cluster, child);
    let mut rewind = seek_command(13, 11, pb::MessageIdData::default());
    rewind.seek.as_mut().expect("seek body").message_id = None;
    send(&mut cluster, child, &rewind).expect("seek replacement child to earliest");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .success
            .is_some()
    );
}

#[test]
fn message_delivery_redelivery_seek_terminal_and_close_conserve_resources() {
    let mut cluster = M1FakeCluster::default();
    assert!(matches!(
        cluster.enqueue_message(99, Bytes::new()),
        Err(M1FakeError::UnknownSegment(99))
    ));
    assert!(matches!(
        cluster.terminate_segment(99),
        Err(M1FakeError::UnknownSegment(99))
    ));
    assert!(matches!(
        cluster.enqueue_message(1, Bytes::from(vec![0; MAX_FRAME_SIZE])),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let controller = connect(&mut cluster, Endpoint::Controller);
    let _member = register_member(&mut cluster, controller, "lifecycle", "member", 100, 1);
    let child = connect(&mut cluster, Endpoint::Segment(1));
    let topic = cluster.segment_topic(1).expect("segment topic");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "lifecycle",
            "member",
            1,
            7,
            1,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open ordinary child");
    let _ = take_frames(&mut cluster, child);
    cluster
        .enqueue_message(1, Bytes::from_static(b"first"))
        .expect("enqueue first");
    cluster
        .enqueue_message(1, Bytes::from_static(b"second"))
        .expect("enqueue second");
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW first delivery");
    let first = take_frames(&mut cluster, child);
    let first_id = message_id(&first);
    send(&mut cluster, child, &redeliver_command(7, first_id.clone())).expect("request redelivery");
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW redelivery");
    let replay = take_frames(&mut cluster, child);
    assert_eq!(
        replay
            .iter()
            .find_map(|frame| frame.command.message.as_ref())
            .and_then(|message| message.redelivery_count),
        Some(1)
    );
    send(&mut cluster, child, &ack_command(7, 2, first_id.clone()))
        .expect("acknowledge first entry");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .ack_response
            .is_some()
    );
    send(&mut cluster, child, &seek_command(7, 3, first_id)).expect("seek child cursor");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .success
            .is_some()
    );
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "lifecycle",
            "member",
            1,
            7,
            4,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("reopen after seek");
    let _ = take_frames(&mut cluster, child);
    send(&mut cluster, child, &flow_command(7, 2)).expect("FLOW replayed entries");
    assert_eq!(
        take_frames(&mut cluster, child)
            .iter()
            .filter(|frame| frame.command.message.is_some())
            .count(),
        2
    );
    cluster.terminate_segment(1).expect("terminate segment");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .reached_end_of_topic
            .is_some()
    );
    assert!(matches!(
        cluster.enqueue_message(1, Bytes::from_static(b"late")),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(&mut cluster, child, &close_command(7, 5)).expect("close child");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .success
            .is_some()
    );
    send(&mut cluster, child, &close_command(7, 6)).expect("retry confirmed child close");
    assert!(
        take_frames(&mut cluster, child)[0]
            .command
            .success
            .is_some()
    );
    let counts = cluster.resource_counts();
    assert_eq!(counts.child_consumers, 0);
    assert_eq!(counts.permits, 0);
    assert_eq!(counts.unacked_messages, 0);

    let mut sparse = M1FakeCluster::default();
    let controller = connect(&mut sparse, Endpoint::Controller);
    let _member = register_member(&mut sparse, controller, "sparse", "member", 1, 1);
    let child = connect(&mut sparse, Endpoint::Segment(1));
    let topic = sparse.segment_topic(1).expect("sparse segment topic");
    send(
        &mut sparse,
        child,
        &segment_subscribe_command(
            &topic,
            "sparse",
            "member",
            1,
            1,
            1,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open sparse-ack child");
    let _ = take_frames(&mut sparse, child);
    sparse
        .enqueue_message(1, Bytes::from_static(b"zero"))
        .expect("enqueue sparse zero");
    sparse
        .enqueue_message(1, Bytes::from_static(b"one"))
        .expect("enqueue sparse one");
    send(&mut sparse, child, &flow_command(1, 2)).expect("deliver sparse messages");
    let delivered: Vec<_> = take_frames(&mut sparse, child)
        .into_iter()
        .filter_map(|frame| frame.command.message.map(|message| message.message_id))
        .collect();
    send(&mut sparse, child, &ack_command(1, 2, delivered[1].clone()))
        .expect("acknowledge the sparse second entry");
    let _ = take_frames(&mut sparse, child);
    send(&mut sparse, child, &close_command(1, 3)).expect("close sparse child");
    let _ = take_frames(&mut sparse, child);
    send(
        &mut sparse,
        child,
        &segment_subscribe_command(
            &topic,
            "sparse",
            "member",
            1,
            2,
            4,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("reopen sparse-ack child");
    let _ = take_frames(&mut sparse, child);
    send(&mut sparse, child, &flow_command(2, 2)).expect("skip sparse acknowledged entry");
    assert_eq!(
        take_frames(&mut sparse, child)
            .iter()
            .filter(|frame| frame.command.message.is_some())
            .count(),
        1
    );
}

#[test]
fn transaction_coordinator_stages_commit_abort_and_fences_pending_work() {
    let mut cluster = M1FakeCluster::default();
    let controller = connect(&mut cluster, Endpoint::Controller);
    let _member = register_member(&mut cluster, controller, "txn-sub", "txn-member", 100, 1);
    let child = connect(&mut cluster, Endpoint::Segment(1));
    let topic = cluster.segment_topic(1).expect("segment topic");
    send(
        &mut cluster,
        child,
        &segment_subscribe_command(
            &topic,
            "txn-sub",
            "txn-member",
            1,
            7,
            2,
            pb::command_subscribe::SubType::Exclusive,
        ),
    )
    .expect("open transaction child");
    let _ = take_frames(&mut cluster, child);

    send(
        &mut cluster,
        controller,
        &lookup_command(TRANSACTION_COORDINATOR_TOPIC, 10),
    )
    .expect("lookup transaction coordinator");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .lookup_topic_response
            .is_some()
    );
    send(&mut cluster, controller, &tc_connect_command(11)).expect("connect transaction client");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .tc_client_connect_response
            .is_some()
    );

    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::NewTxn as i32,
                new_txn: Some(pb::CommandNewTxn {
                    request_id: 12,
                    txn_ttl_millis: Some(0),
                    tc_id: Some(0),
                    scalable: None,
                }),
                ..Default::default()
            },
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(&mut cluster, controller, &new_transaction_command(13)).expect("allocate commit txn");
    let commit_txn = opened_transaction(&take_frames(&mut cluster, controller));

    let mut partial_id = add_subscription_to_transaction_command(14, commit_txn, &topic, "txn-sub");
    partial_id
        .add_subscription_to_txn
        .as_mut()
        .expect("registration body")
        .txnid_least_bits = None;
    assert!(matches!(
        send(&mut cluster, controller, &partial_id),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let unknown_topic = "persistent://public/default/not-a-segment";
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &add_subscription_to_transaction_command(15, commit_txn, unknown_topic, "txn-sub"),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    send(
        &mut cluster,
        controller,
        &add_subscription_to_transaction_command(16, commit_txn, &topic, "txn-sub"),
    )
    .expect("register commit subscription");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .add_subscription_to_txn_response
            .is_some()
    );

    cluster
        .enqueue_message(1, Bytes::from_static(b"commit"))
        .expect("enqueue commit message");
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW commit message");
    let commit_message = message_id(&take_frames(&mut cluster, child));
    send(
        &mut cluster,
        child,
        &transactional_ack_command(7, 17, commit_txn, commit_message.clone()),
    )
    .expect("stage commit acknowledgement");
    let _ = take_frames(&mut cluster, child);
    let repeated_commit_message = cluster
        .transaction_observation(commit_txn)
        .expect("staged commit transaction");
    assert_eq!(repeated_commit_message.staged_acknowledgements, 1);
    assert!(matches!(
        send(
            &mut cluster,
            child,
            &transactional_ack_command(7, 117, commit_txn, commit_message),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    assert_eq!(cluster.durable_cursor("txn-sub", 1), Some(0));
    assert_eq!(
        cluster
            .transaction_observation(commit_txn)
            .expect("open commit transaction")
            .staged_acknowledgements,
        1
    );
    send(
        &mut cluster,
        controller,
        &end_transaction_command(18, commit_txn, pb::TxnAction::Commit),
    )
    .expect("commit staged acknowledgement");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .end_txn_response
            .is_some()
    );
    assert_eq!(cluster.durable_cursor("txn-sub", 1), Some(1));
    assert_eq!(
        cluster
            .transaction_observation(commit_txn)
            .expect("committed transaction")
            .state,
        FakeTransactionState::Committed
    );
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &end_transaction_command(19, commit_txn, pb::TxnAction::Commit),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));

    send(&mut cluster, controller, &new_transaction_command(20)).expect("allocate abort txn");
    let abort_txn = opened_transaction(&take_frames(&mut cluster, controller));
    send(
        &mut cluster,
        controller,
        &add_subscription_to_transaction_command(21, abort_txn, &topic, "txn-sub"),
    )
    .expect("register abort subscription");
    let _ = take_frames(&mut cluster, controller);
    cluster
        .enqueue_message(1, Bytes::from_static(b"abort"))
        .expect("enqueue abort message");
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW abort message");
    let abort_message = message_id(&take_frames(&mut cluster, child));
    send(
        &mut cluster,
        child,
        &transactional_ack_command(7, 22, abort_txn, abort_message.clone()),
    )
    .expect("stage abort acknowledgement");
    let _ = take_frames(&mut cluster, child);
    send(&mut cluster, child, &flow_command(7, 1)).expect("retain redelivery permit");
    send(
        &mut cluster,
        controller,
        &end_transaction_command(23, abort_txn, pb::TxnAction::Abort),
    )
    .expect("abort staged acknowledgement");
    let _ = take_frames(&mut cluster, controller);
    let replay = take_frames(&mut cluster, child);
    assert_eq!(message_id(&replay), abort_message);
    assert_eq!(
        replay
            .iter()
            .find_map(|frame| frame.command.message.as_ref())
            .and_then(|message| message.redelivery_count),
        Some(1)
    );
    assert_eq!(
        cluster
            .transaction_observation(abort_txn)
            .expect("aborted transaction")
            .state,
        FakeTransactionState::Aborted
    );

    send(&mut cluster, controller, &new_transaction_command(24)).expect("allocate pending txn");
    let pending_txn = opened_transaction(&take_frames(&mut cluster, controller));
    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::TransactionRegistration,
            ScriptedBehavior::Delay,
        )
        .expect("delay transaction registration");
    send(
        &mut cluster,
        controller,
        &add_subscription_to_transaction_command(25, pending_txn, &topic, "txn-sub"),
    )
    .expect("hold transaction registration");
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &end_transaction_command(26, pending_txn, pb::TxnAction::Commit),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let pending = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(
            pending,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "registration failed",
            )),
        )
        .expect("fail delayed transaction registration");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .add_subscription_to_txn_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );
    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::TransactionRegistration,
            ScriptedBehavior::Delay,
        )
        .expect("delay registration retry");
    send(
        &mut cluster,
        controller,
        &add_subscription_to_transaction_command(27, pending_txn, &topic, "txn-sub"),
    )
    .expect("hold registration retry");
    let pending = cluster.pending_operations()[0].id;
    cluster
        .complete_pending(pending, PendingCompletion::Succeed)
        .expect("complete registration retry");
    let _ = take_frames(&mut cluster, controller);
    cluster
        .enqueue_message(1, Bytes::from_static(b"pending-ack"))
        .expect("enqueue pending transaction acknowledgement");
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW pending transaction message");
    let pending_message = message_id(&take_frames(&mut cluster, child));
    cluster
        .script_next(
            Endpoint::Segment(1),
            OperationKind::Ack,
            ScriptedBehavior::Delay,
        )
        .expect("delay transaction acknowledgement");
    send(
        &mut cluster,
        child,
        &transactional_ack_command(7, 127, pending_txn, pending_message),
    )
    .expect("hold transaction acknowledgement");
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &end_transaction_command(128, pending_txn, pb::TxnAction::Abort),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
    let pending_ack = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::Ack)
        .expect("pending transaction acknowledgement")
        .id;
    cluster
        .complete_pending(pending_ack, PendingCompletion::Succeed)
        .expect("complete pending transaction acknowledgement");
    let _ = take_frames(&mut cluster, child);
    send(
        &mut cluster,
        controller,
        &end_transaction_command(28, pending_txn, pb::TxnAction::Abort),
    )
    .expect("abort settled pending transaction");
    let _ = take_frames(&mut cluster, controller);

    send(&mut cluster, controller, &new_transaction_command(29))
        .expect("allocate scripted EndTxn transaction");
    let scripted_txn = opened_transaction(&take_frames(&mut cluster, controller));
    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::EndTransaction,
            ScriptedBehavior::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "EndTxn failed",
            )),
        )
        .expect("script immediate EndTxn failure");
    send(
        &mut cluster,
        controller,
        &end_transaction_command(30, scripted_txn, pb::TxnAction::Commit),
    )
    .expect("return scripted EndTxn failure");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .end_txn_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );
    assert_eq!(
        cluster
            .transaction_observation(scripted_txn)
            .expect("failed EndTxn leaves transaction open")
            .state,
        FakeTransactionState::Open
    );

    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::EndTransaction,
            ScriptedBehavior::Delay,
        )
        .expect("delay EndTxn failure retry");
    send(
        &mut cluster,
        controller,
        &end_transaction_command(31, scripted_txn, pb::TxnAction::Commit),
    )
    .expect("hold EndTxn failure retry");
    let delayed_end = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::EndTransaction)
        .expect("delayed EndTxn operation")
        .id;
    cluster
        .complete_pending(
            delayed_end,
            PendingCompletion::Fail(BrokerFailure::new(
                pb::ServerError::PersistenceError,
                "delayed EndTxn failed",
            )),
        )
        .expect("fail delayed EndTxn");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .end_txn_response
            .as_ref()
            .is_some_and(|response| response.error.is_some())
    );

    cluster
        .script_next(
            Endpoint::Controller,
            OperationKind::EndTransaction,
            ScriptedBehavior::Delay,
        )
        .expect("delay successful EndTxn retry");
    send(
        &mut cluster,
        controller,
        &end_transaction_command(32, scripted_txn, pb::TxnAction::Commit),
    )
    .expect("hold successful EndTxn retry");
    let delayed_end = cluster
        .pending_operations()
        .into_iter()
        .find(|pending| pending.kind == OperationKind::EndTransaction)
        .expect("successful delayed EndTxn operation")
        .id;
    cluster
        .complete_pending(delayed_end, PendingCompletion::Succeed)
        .expect("complete delayed EndTxn");
    assert!(
        take_frames(&mut cluster, controller)[0]
            .command
            .end_txn_response
            .as_ref()
            .is_some_and(|response| response.error.is_none())
    );
    assert_eq!(
        cluster
            .transaction_observation(scripted_txn)
            .expect("completed delayed EndTxn")
            .state,
        FakeTransactionState::Committed
    );

    send(&mut cluster, controller, &new_transaction_command(200))
        .expect("allocate stale-child transaction");
    let stale_child_txn = opened_transaction(&take_frames(&mut cluster, controller));
    send(
        &mut cluster,
        controller,
        &add_subscription_to_transaction_command(201, stale_child_txn, &topic, "txn-sub"),
    )
    .expect("register stale-child transaction");
    let _ = take_frames(&mut cluster, controller);
    send(&mut cluster, child, &flow_command(7, 1)).expect("FLOW stale-child transaction message");
    let stale_child_message = message_id(&take_frames(&mut cluster, child));
    send(
        &mut cluster,
        child,
        &transactional_ack_command(7, 202, stale_child_txn, stale_child_message),
    )
    .expect("stage stale-child transaction acknowledgement");
    let _ = take_frames(&mut cluster, child);
    cluster
        .disconnect_connection(child)
        .expect("replace staged transaction child");
    assert!(matches!(
        send(
            &mut cluster,
            controller,
            &end_transaction_command(203, stale_child_txn, pb::TxnAction::Commit),
        ),
        Err(M1FakeError::InvalidCommand { .. })
    ));
}
