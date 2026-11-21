// SPDX-License-Identifier: Apache-2.0

//! A scalable-topic watch the client itself refuses is closed at the broker.
//!
//! A DAG session, a namespace scalable-topics watch and a TC-assignment watch
//! each live at the broker until the client sends the matching close command
//! (`CommandScalableTopicClose`, `CommandWatchScalableTopicsClose`,
//! `CommandWatchTcAssignmentsClose`): the broker keeps pushing on the session
//! otherwise (ADR-0093, `dag_watch` module documentation). When the client
//! rejects an update because the update is unusable (it carries neither a
//! layout/event/snapshot nor an error, or its layout fails whole-snapshot
//! validation), `Connection` drops the watch from its own registry and queues
//! the `*WatchClosed` event, but encodes no close frame. Every later close is a
//! no-op because the watch is already gone, so the broker-side watch is
//! orphaned for the life of the TCP connection.
//!
//! A failure the broker itself reported is the control: the broker ended that
//! watch, so there is nothing to close.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

#![cfg(feature = "scalable-topics")]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::pb::base_command::Type;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, decode_one, encode_command, pb,
};

const TOPIC: &str = "topic://public/default/scaled";
const NAMESPACE: &str = "public/default";

fn new_connection() -> Connection {
    Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    )
}

/// A connection whose peer advertised both scalable-topic capabilities.
fn connected() -> Connection {
    let mut conn = new_connection();
    conn.begin_handshake().expect("handshake");
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    supports_tc_metadata_discovery: Some(true),
                    ..pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    conn.handle_bytes(Instant::now(), &buf).expect("Connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    conn
}

/// Decode every frame staged on the connection's outbound buffer.
fn drain_commands(conn: &mut Connection) -> Vec<pb::BaseCommand> {
    let mut bytes = conn.poll_transmit();
    let mut commands = Vec::new();
    while !bytes.is_empty() {
        commands.push(
            decode_one(&mut bytes)
                .expect("staged frame must decode")
                .command,
        );
    }
    commands
}

/// Every watch-close frame among `commands`, as `(command type, watch or session id)`.
fn closes(commands: &[pb::BaseCommand]) -> Vec<(Type, u64)> {
    commands
        .iter()
        .filter_map(|command| {
            if let Some(close) = command.scalable_topic_close.as_ref() {
                Some((Type::ScalableTopicClose, close.session_id))
            } else if let Some(close) = command.watch_scalable_topics_close.as_ref() {
                Some((Type::WatchScalableTopicsClose, close.watch_id))
            } else {
                command
                    .watch_tc_assignments_close
                    .as_ref()
                    .map(|close| (Type::WatchTcAssignmentsClose, close.watch_id))
            }
        })
        .collect()
}

fn command_bytes(command: &pb::BaseCommand) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode test command");
    buf
}

/// A `CommandScalableTopicUpdate` carrying neither a layout nor an error.
fn bodyless_dag_update(session_id: u64) -> BytesMut {
    dag_update(session_id, None, None)
}

/// A `CommandScalableTopicUpdate` carrying a broker error and no layout.
fn failed_dag_update(session_id: u64) -> BytesMut {
    dag_update(
        session_id,
        None,
        Some(pb::ServerError::TopicNotFound as i32),
    )
}

fn dag_update(session_id: u64, dag: Option<pb::ScalableTopicDag>, error: Option<i32>) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag,
            error,
            message: error.map(|_| "broker says no".to_owned()),
            resolved_topic_name: None,
        }),
        ..Default::default()
    })
}

/// A forward-epoch layout whose single active segment stops short of the end of the hash space.
///
/// Whole-snapshot validation refuses it (`ActiveCoverageEnd`), after the update was well-formed.
fn short_coverage_dag_update(session_id: u64) -> BytesMut {
    dag_update(
        session_id,
        Some(pb::ScalableTopicDag {
            epoch: 0,
            segments: vec![pb::SegmentInfoProto {
                segment_id: 1,
                hash_start: 0,
                hash_end: 100,
                state: pb::SegmentState::Active as i32,
                parent_ids: Vec::new(),
                child_ids: Vec::new(),
                created_at_epoch: 0,
                sealed_at_epoch: None,
                created_at_ms: 0,
                sealed_at_ms: None,
                legacy_topic_name: None,
                entry_bucket_splits: Vec::new(),
            }],
            segment_brokers: vec![pb::SegmentBrokerAddress {
                segment_id: 1,
                broker_url: "pulsar://seg1:6650".to_owned(),
                broker_url_tls: None,
            }],
            controller_broker_url: Some("pulsar://controller:6650".to_owned()),
            controller_broker_url_tls: None,
        }),
        None,
    )
}

/// A `CommandWatchScalableTopicsUpdate` carrying neither an event nor an error.
fn bodyless_topics_update(watch_id: u64) -> BytesMut {
    topics_update(watch_id, None)
}

/// A `CommandWatchScalableTopicsUpdate` carrying a broker error and no event.
fn failed_topics_update(watch_id: u64) -> BytesMut {
    topics_update(watch_id, Some(pb::ServerError::TopicNotFound as i32))
}

fn topics_update(watch_id: u64, error: Option<i32>) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::WatchScalableTopicsUpdate as i32,
        watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
            watch_id,
            event: None,
            error,
            message: error.map(|_| "broker says no".to_owned()),
        }),
        ..Default::default()
    })
}

/// A `CommandWatchTcAssignmentsUpdate` carrying neither a snapshot nor an error.
fn bodyless_tc_update(watch_id: u64) -> BytesMut {
    tc_update(watch_id, None)
}

/// A `CommandWatchTcAssignmentsUpdate` carrying a broker error and no snapshot.
fn failed_tc_update(watch_id: u64) -> BytesMut {
    tc_update(watch_id, Some(pb::ServerError::TopicNotFound as i32))
}

fn tc_update(watch_id: u64, error: Option<i32>) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::WatchTcAssignmentsUpdate as i32,
        watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
            watch_id,
            snapshot: None,
            error,
            message: error.map(|_| "broker says no".to_owned()),
        }),
        ..Default::default()
    })
}

/// Open a DAG session and drain its `CommandScalableTopicLookup`, proving the frame helpers decode
/// what the connection writes.
fn open_dag_session(conn: &mut Connection) -> u64 {
    let session_id = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    let opened = drain_commands(conn);
    assert_eq!(
        opened
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>(),
        vec![Type::ScalableTopicLookup as i32],
        "opening a session writes exactly the lookup"
    );
    session_id
}

fn open_topics_watch(conn: &mut Connection) -> u64 {
    let watch_id = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("broker supports scalable topics");
    let opened = drain_commands(conn);
    assert_eq!(
        opened
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>(),
        vec![Type::WatchScalableTopics as i32],
        "opening a watch writes exactly the subscribe"
    );
    watch_id
}

fn open_tc_watch(conn: &mut Connection) -> u64 {
    let watch_id = conn
        .watch_tc_assignments()
        .expect("broker supports TC metadata discovery");
    let opened = drain_commands(conn);
    assert_eq!(
        opened
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>(),
        vec![Type::WatchTcAssignments as i32],
        "opening a watch writes exactly the subscribe"
    );
    watch_id
}

#[test]
fn bodyless_dag_update_closes_the_session_at_the_broker() {
    let mut conn = connected();
    let session_id = open_dag_session(&mut conn);

    conn.handle_bytes(Instant::now(), &bodyless_dag_update(session_id))
        .expect("bodyless update");
    assert!(
        matches!(
            conn.poll_event(),
            Some(ConnectionEvent::DagWatchClosed { session_id: id, .. }) if id == session_id
        ),
        "the client refuses the update and drops the session"
    );
    assert!(
        conn.dag_snapshot(session_id).is_none(),
        "the session is gone locally"
    );

    assert_eq!(
        closes(&drain_commands(&mut conn)),
        vec![(Type::ScalableTopicClose, session_id)],
        "the client dropped the session, so it must tell the broker to stop pushing on it"
    );
}

#[test]
fn invalid_dag_layout_closes_the_session_at_the_broker() {
    let mut conn = connected();
    let session_id = open_dag_session(&mut conn);

    conn.handle_bytes(Instant::now(), &short_coverage_dag_update(session_id))
        .expect("invalid layout update");
    assert!(
        matches!(
            conn.poll_event(),
            Some(ConnectionEvent::DagWatchClosed { session_id: id, .. }) if id == session_id
        ),
        "the client refuses the layout and drops the session"
    );

    assert_eq!(
        closes(&drain_commands(&mut conn)),
        vec![(Type::ScalableTopicClose, session_id)],
        "the client dropped the session, so it must tell the broker to stop pushing on it"
    );
}

#[test]
fn bodyless_topics_update_closes_the_watch_at_the_broker() {
    let mut conn = connected();
    let watch_id = open_topics_watch(&mut conn);

    conn.handle_bytes(Instant::now(), &bodyless_topics_update(watch_id))
        .expect("bodyless update");
    assert!(
        matches!(
            conn.poll_event(),
            Some(ConnectionEvent::ScalableTopicsWatchClosed { watch_id: id, .. }) if id == watch_id
        ),
        "the client refuses the update and drops the watch"
    );
    assert!(
        conn.scalable_topics_snapshot(watch_id).is_none(),
        "the watch is gone locally"
    );

    assert_eq!(
        closes(&drain_commands(&mut conn)),
        vec![(Type::WatchScalableTopicsClose, watch_id)],
        "the client dropped the watch, so it must tell the broker to stop pushing on it"
    );
}

#[test]
fn bodyless_tc_update_closes_the_watch_at_the_broker() {
    let mut conn = connected();
    let watch_id = open_tc_watch(&mut conn);

    conn.handle_bytes(Instant::now(), &bodyless_tc_update(watch_id))
        .expect("bodyless update");
    assert!(
        matches!(
            conn.poll_event(),
            Some(ConnectionEvent::TcAssignmentsWatchClosed { watch_id: id, .. }) if id == watch_id
        ),
        "the client refuses the update and drops the watch"
    );

    assert_eq!(
        closes(&drain_commands(&mut conn)),
        vec![(Type::WatchTcAssignmentsClose, watch_id)],
        "the client dropped the watch, so it must tell the broker to stop pushing on it"
    );
}

/// Control: a failure the broker itself reported ended that watch at the broker, so the client has
/// nothing to close. This must stay green before and after the fix.
#[test]
fn broker_reported_failure_is_not_closed_again() {
    let mut conn = connected();
    let session_id = open_dag_session(&mut conn);
    let topics_watch = open_topics_watch(&mut conn);
    let tc_watch = open_tc_watch(&mut conn);

    conn.handle_bytes(Instant::now(), &failed_dag_update(session_id))
        .expect("failed DAG update");
    conn.handle_bytes(Instant::now(), &failed_topics_update(topics_watch))
        .expect("failed topics update");
    conn.handle_bytes(Instant::now(), &failed_tc_update(tc_watch))
        .expect("failed TC update");

    let mut dropped = 0;
    while let Some(event) = conn.poll_event() {
        if matches!(
            event,
            ConnectionEvent::DagWatchClosed { .. }
                | ConnectionEvent::ScalableTopicsWatchClosed { .. }
                | ConnectionEvent::TcAssignmentsWatchClosed { .. }
        ) {
            dropped += 1;
        }
    }
    assert_eq!(dropped, 3, "each broker-reported failure drops its watch");
    assert_eq!(
        closes(&drain_commands(&mut conn)),
        Vec::new(),
        "the broker already ended these watches"
    );
}
