// SPDX-License-Identifier: Apache-2.0

//! A transport reset forgets the raw scalable-topic watches that lived on the dead session,
//! identically on both engines (ADR-0024 layer (d)).
//!
//! A DAG session, a namespace scalable-topics watch and a TC-assignment watch live at the broker
//! only as long as the TCP connection that carried their subscribe command, and nothing in the
//! client writes the subscribe again. The supervisor calls `Connection::reset()` after a drop, but
//! `reset()` kept the three raw registries registered: their accessors then reported the topic
//! set and the layout the broker last pushed on the dead session as if the watch were live, and a
//! later close wrote a close frame for an id the new session never opened. Both engines wrap the
//! same `magnetar-proto` state machine, so they must agree on what the accessors report and on
//! the frames a close leaves on the wire, before and after the reset.
//!
//! The transcript opens one registration of each kind plus a namespace watch in the failed window
//! (the capability outlives `mark_disconnected`, so the open succeeds and its frame is wiped by
//! the reset), resets, and then reads every accessor, replays a TC update for the old id and
//! closes every old id. Registrations opened after the reset are the control.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::pb::base_command::Type;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, decode_one, encode_command, pb,
};

const TOPIC: &str = "topic://public/default/scaled";
const NAMESPACE: &str = "public/default";
const WATCHED_TOPIC: &str = "topic://public/default/watched";

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

fn command_bytes(command: &pb::BaseCommand) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode test command");
    buf
}

fn handshake(conn: &mut Connection, at: Instant) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(
        at,
        &command_bytes(&pb::BaseCommand {
            r#type: Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
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

/// Every watch-close frame the connection staged, as `Type(id)`; every other frame is skipped.
fn staged_closes(conn: &mut Connection) -> Vec<String> {
    let mut bytes = conn.poll_transmit();
    let mut closes = Vec::new();
    while !bytes.is_empty() {
        let command = decode_one(&mut bytes)
            .expect("staged frame must decode")
            .command;
        if let Some(close) = command.scalable_topic_close.as_ref() {
            closes.push(format!("ScalableTopicClose({})", close.session_id));
        } else if let Some(close) = command.watch_scalable_topics_close.as_ref() {
            closes.push(format!("WatchScalableTopicsClose({})", close.watch_id));
        } else if let Some(close) = command.watch_tc_assignments_close.as_ref() {
            closes.push(format!("WatchTcAssignmentsClose({})", close.watch_id));
        }
    }
    closes
}

/// Every TC-assignment event the connection queued, as `TcAssignmentsChanged(id)`.
fn tc_events(conn: &mut Connection) -> Vec<String> {
    let mut changed = Vec::new();
    while let Some(event) = conn.poll_event() {
        if let ConnectionEvent::TcAssignmentsChanged { watch_id, .. } = event {
            changed.push(format!("TcAssignmentsChanged({watch_id})"));
        }
    }
    changed
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

/// Open one registration of each kind and let the broker push its first update on each.
fn open_all(conn: &mut Connection, at: Instant) -> (u64, u64, u64) {
    let session = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    let topics = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("broker supports scalable topics");
    let tc = conn
        .watch_tc_assignments()
        .expect("broker supports TC metadata discovery");
    conn.handle_bytes(at, &layout_frame(session))
        .expect("layout");
    conn.handle_bytes(at, &topics_snapshot_frame(topics))
        .expect("topics snapshot");
    conn.handle_bytes(at, &tc_snapshot_frame(tc))
        .expect("TC snapshot");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    (session, topics, tc)
}

/// What the accessors report for a session and a namespace watch.
fn observe(conn: &Connection, session: u64, topics: u64) -> String {
    format!(
        "dag_segments={:?} resolved={:?} topics={:?}",
        conn.dag_snapshot(session).map(|segments| segments.len()),
        conn.scalable_resolved_topic_name(session),
        conn.scalable_topics_snapshot(topics),
    )
}

/// Open every kind of registration, drop the transport, and report what the connection says about
/// the dead session's ids and about the ones opened on the new session.
fn reset_transcript(conn: &mut Connection, at: Instant) -> Vec<String> {
    handshake(conn, at);
    let (session, topics, tc) = open_all(conn, at);
    let mut transcript = vec![format!(
        "before the drop: {}",
        observe(conn, session, topics)
    )];

    conn.mark_disconnected();
    let failed_window = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("capability survives the disconnect");
    transcript.push(format!(
        "opened in the failed window: topics={:?}",
        conn.scalable_topics_snapshot(failed_window)
    ));

    conn.reset();
    handshake(conn, at);
    transcript.push(format!(
        "after the reset: {} failed_window={:?}",
        observe(conn, session, topics),
        conn.scalable_topics_snapshot(failed_window)
    ));

    conn.handle_bytes(at, &tc_snapshot_frame(tc))
        .expect("TC snapshot for the old id");
    transcript.push(format!("TC update for the old id: {:?}", tc_events(conn)));

    conn.close_scalable_topic_session(session);
    conn.close_scalable_topics_watch(topics);
    conn.close_tc_assignments_watch(tc);
    conn.close_scalable_topics_watch(failed_window);
    transcript.push(format!("closing the old ids: {:?}", staged_closes(conn)));

    let (new_session, new_topics, new_tc) = open_all(conn, at);
    transcript.push(format!(
        "opened on the new session: ids=({new_session}, {new_topics}, {new_tc}) {}",
        observe(conn, new_session, new_topics)
    ));
    conn.close_scalable_topic_session(new_session);
    conn.close_scalable_topics_watch(new_topics);
    conn.close_tc_assignments_watch(new_tc);
    transcript.push(format!("closing the new ids: {:?}", staged_closes(conn)));
    transcript
}

#[test]
fn engines_forget_the_raw_scalable_registrations_of_a_dead_session() {
    let tokio = tokio_projection(reset_transcript);
    let moonpool = moonpool_projection(reset_transcript);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the accessors and the close frames around a reset"
    );

    assert_eq!(
        tokio,
        vec![
            "before the drop: dag_segments=Some(1) resolved=Some(\"topic://public/default/scaled\") \
             topics=Some([\"topic://public/default/watched\"])"
                .to_owned(),
            "opened in the failed window: topics=Some([])".to_owned(),
            "after the reset: dag_segments=None resolved=None topics=None failed_window=None"
                .to_owned(),
            "TC update for the old id: []".to_owned(),
            "closing the old ids: []".to_owned(),
            "opened on the new session: ids=(2, 4, 5) dag_segments=Some(1) \
             resolved=Some(\"topic://public/default/scaled\") \
             topics=Some([\"topic://public/default/watched\"])"
                .to_owned(),
            "closing the new ids: [\"ScalableTopicClose(2)\", \"WatchScalableTopicsClose(4)\", \
             \"WatchTcAssignmentsClose(5)\"]"
                .to_owned(),
        ],
        "a reset must forget the dead session's registrations and leave the new ones alone"
    );
}
