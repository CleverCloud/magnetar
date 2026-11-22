// SPDX-License-Identifier: Apache-2.0

//! A transport reset forgets the raw scalable-topic watches that lived on the dead session,
//! exercised through the tokio engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-moonpool` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/scalable_reset_registries.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! A DAG session, a namespace scalable-topics watch and a TC-assignment watch live at the broker
//! only as long as the TCP connection that carried their subscribe command, and nothing in the
//! client writes the subscribe again. The supervisor calls `Connection::reset()` after a drop, but
//! `reset()` kept the three raw registries registered: their accessors then reported the topic
//! set and the layout the broker last pushed on the dead session as if the watch were live, and a
//! later close wrote a close frame for an id the new session never opened.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

#![cfg(feature = "scalable-topics")]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::pb::base_command::Type;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

const TOPIC: &str = "topic://public/default/scaled";
const NAMESPACE: &str = "public/default";
const WATCHED_TOPIC: &str = "topic://public/default/watched";

fn command_bytes(command: &pb::BaseCommand) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode test command");
    buf
}

/// Run the handshake against a peer that advertises both scalable-topic capabilities.
fn handshake(conn: &mut Connection) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(
        Instant::now(),
        &command_bytes(&pb::BaseCommand {
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
        }),
    )
    .expect("Connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

/// The transport drops: the supervisor resets the connection and handshakes again.
fn reconnect(conn: &mut Connection) {
    conn.reset();
    handshake(conn);
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

/// A layout whose single active segment covers the whole hash space, so it validates.
fn layout_frame(session_id: u64) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch: 0,
                segments: vec![pb::SegmentInfoProto {
                    segment_id: 1,
                    hash_start: 0,
                    hash_end: 65_535,
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
            error: None,
            message: None,
            resolved_topic_name: Some(TOPIC.to_owned()),
        }),
        ..Default::default()
    })
}

/// A namespace-watch snapshot naming one matching topic.
fn topics_snapshot_frame(watch_id: u64) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::WatchScalableTopicsUpdate as i32,
        watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
            watch_id,
            event: Some(pb::command_watch_scalable_topics_update::Event::Snapshot(
                pb::ScalableTopicsSnapshot {
                    topics: vec![WATCHED_TOPIC.to_owned()],
                },
            )),
            error: None,
            message: None,
        }),
        ..Default::default()
    })
}

/// A TC-assignment snapshot naming one coordinator.
fn tc_snapshot_frame(watch_id: u64) -> BytesMut {
    command_bytes(&pb::BaseCommand {
        r#type: Type::WatchTcAssignmentsUpdate as i32,
        watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
            watch_id,
            snapshot: Some(pb::TcAssignmentsSnapshot {
                parallelism: 1,
                assignments: vec![pb::TcAssignment {
                    tc_id: 0,
                    broker_service_url: Some("pulsar://tc0:6650".to_owned()),
                    broker_service_url_tls: None,
                }],
            }),
            error: None,
            message: None,
        }),
        ..Default::default()
    })
}

/// Open a DAG session and let the broker push its layout.
fn open_dag_session(conn: &mut Connection) -> u64 {
    let session_id = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    conn.handle_bytes(Instant::now(), &layout_frame(session_id))
        .expect("layout");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    session_id
}

/// Open a namespace watch and let the broker push its snapshot.
fn open_topics_watch(conn: &mut Connection) -> u64 {
    let watch_id = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("broker supports scalable topics");
    conn.handle_bytes(Instant::now(), &topics_snapshot_frame(watch_id))
        .expect("topics snapshot");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    watch_id
}

/// Open a TC-assignment watch and let the broker push its snapshot.
fn open_tc_watch(conn: &mut Connection) -> u64 {
    let watch_id = conn
        .watch_tc_assignments()
        .expect("broker supports TC metadata discovery");
    conn.handle_bytes(Instant::now(), &tc_snapshot_frame(watch_id))
        .expect("TC snapshot");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    watch_id
}

#[test]
fn reset_forgets_every_raw_scalable_registration() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn);
    let session_id = open_dag_session(&mut conn);
    let topics_watch = open_topics_watch(&mut conn);
    let tc_watch = open_tc_watch(&mut conn);
    assert_eq!(
        conn.dag_snapshot(session_id).map(|segments| segments.len()),
        Some(1),
        "the session is live and holds the broker's layout before the drop"
    );
    assert_eq!(
        conn.scalable_topics_snapshot(topics_watch),
        Some(vec![WATCHED_TOPIC.to_owned()]),
        "the watch is live and holds the broker's snapshot before the drop"
    );

    reconnect(&mut conn);

    assert_eq!(
        conn.dag_snapshot(session_id),
        None,
        "the broker reaped the session with the dead connection"
    );
    assert_eq!(conn.scalable_resolved_topic_name(session_id), None);
    assert_eq!(
        conn.scalable_topics_snapshot(topics_watch),
        None,
        "the broker reaped the watch with the dead connection"
    );
    // The TC watch has no accessor: an update for an id the connection tracks becomes an event.
    conn.handle_bytes(Instant::now(), &tc_snapshot_frame(tc_watch))
        .expect("TC snapshot for the old id");
    assert!(
        !matches!(
            conn.poll_event(),
            Some(ConnectionEvent::TcAssignmentsChanged { watch_id: id, .. }) if id == tc_watch
        ),
        "the new session never opened this TC watch, so an update for its id has no owner"
    );

    conn.close_scalable_topic_session(session_id);
    conn.close_scalable_topics_watch(topics_watch);
    conn.close_tc_assignments_watch(tc_watch);
    assert_eq!(
        closes(&drain_commands(&mut conn)),
        Vec::new(),
        "the new session never opened these ids, so closing them must write nothing"
    );
}

#[test]
fn a_watch_opened_while_the_connection_is_failed_does_not_survive_the_reset() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn);
    conn.mark_disconnected();

    // The feature flags outlive `mark_disconnected`, so the open succeeds, but its frame goes to
    // an outbound buffer that `reset()` wipes: the watch exists only locally from birth.
    let watch_id = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("capability survives the disconnect");
    assert_eq!(
        conn.scalable_topics_snapshot(watch_id),
        Some(Vec::new()),
        "the watch is registered locally in the failed window"
    );

    reconnect(&mut conn);

    assert_eq!(
        conn.scalable_topics_snapshot(watch_id),
        None,
        "the subscribe never reached a broker, so the watch must not outlive the reset"
    );
}

/// Control: a registration opened after the reset is tracked normally, under an id the dead
/// session never used. This must stay green before and after the fix.
#[test]
fn registrations_opened_after_a_reset_are_tracked_under_fresh_ids() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn);
    let old_session = open_dag_session(&mut conn);
    let old_topics = open_topics_watch(&mut conn);
    let old_tc = open_tc_watch(&mut conn);

    reconnect(&mut conn);

    let session_id = open_dag_session(&mut conn);
    let topics_watch = open_topics_watch(&mut conn);
    let tc_watch = open_tc_watch(&mut conn);
    assert_ne!(session_id, old_session, "a session id is never reused");
    assert_ne!(topics_watch, old_topics, "a watch id is never reused");
    assert_ne!(tc_watch, old_tc, "a watch id is never reused");

    assert_eq!(conn.dag_snapshot(session_id).map(|s| s.len()), Some(1));
    assert_eq!(
        conn.scalable_topics_snapshot(topics_watch),
        Some(vec![WATCHED_TOPIC.to_owned()])
    );

    conn.close_scalable_topic_session(session_id);
    conn.close_scalable_topics_watch(topics_watch);
    conn.close_tc_assignments_watch(tc_watch);
    assert_eq!(
        closes(&drain_commands(&mut conn)),
        vec![
            (Type::ScalableTopicClose, session_id),
            (Type::WatchScalableTopicsClose, topics_watch),
            (Type::WatchTcAssignmentsClose, tc_watch),
        ],
        "a registration opened on the new session is closed at the broker"
    );
}
