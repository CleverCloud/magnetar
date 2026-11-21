// SPDX-License-Identifier: Apache-2.0

//! A scalable-topic watch the client itself refuses is closed at the broker, identically on both
//! engines (ADR-0024 layer (d)).
//!
//! A DAG session, a namespace scalable-topics watch and a TC-assignment watch each live at the
//! broker until the client sends the matching close command. When the client rejects an update
//! because the update is unusable, `Connection` drops the watch locally and queues the
//! `*WatchClosed` event; without a close frame the broker-side watch is orphaned for the life of
//! the TCP connection. A failure the broker itself reported ended that watch at the broker, so it
//! needs no close. Both engines wrap the same `magnetar-proto` state machine, so they must agree
//! on the events and on the frames each rejection leaves on the wire.
//!
//! The transcript runs four client-side rejections (a bodyless DAG update, a DAG layout that
//! fails whole-snapshot validation, a bodyless namespace-watch update and a bodyless TC-watch
//! update) and the three broker-reported failures that serve as the control.

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

/// Every watch-closed event the connection queued, as `Event(id)`.
fn closed_events(conn: &mut Connection) -> Vec<String> {
    let mut closed = Vec::new();
    while let Some(event) = conn.poll_event() {
        match event {
            ConnectionEvent::DagWatchClosed { session_id, .. } => {
                closed.push(format!("DagWatchClosed({session_id})"));
            }
            ConnectionEvent::ScalableTopicsWatchClosed { watch_id, .. } => {
                closed.push(format!("ScalableTopicsWatchClosed({watch_id})"));
            }
            ConnectionEvent::TcAssignmentsWatchClosed { watch_id, .. } => {
                closed.push(format!("TcAssignmentsWatchClosed({watch_id})"));
            }
            _ => {}
        }
    }
    closed
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

/// A forward-epoch layout whose only active segment stops short of the end of the hash space.
fn short_coverage_dag() -> pb::ScalableTopicDag {
    pb::ScalableTopicDag {
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
    }
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

/// Feed one refusal and record what the connection reported and wrote.
fn step(conn: &mut Connection, at: Instant, label: &str, update: &BytesMut) -> String {
    conn.handle_bytes(at, update).expect(label);
    format!(
        "{label}: events={:?} closes={:?}",
        closed_events(conn),
        staged_closes(conn)
    )
}

/// Open every kind of watch on one connection, refuse each update, and report the transcript.
fn rejection_transcript(conn: &mut Connection, at: Instant) -> Vec<String> {
    handshake(conn, at);
    let not_found = Some(pb::ServerError::TopicNotFound as i32);

    let bodyless_dag = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    let invalid_dag = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    let bodyless_topics = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("broker supports scalable topics");
    let bodyless_tc = conn
        .watch_tc_assignments()
        .expect("broker supports TC metadata discovery");
    let failed_dag = conn
        .open_scalable_topic_session(TOPIC, true)
        .expect("broker supports scalable topics");
    let failed_topics = conn
        .watch_scalable_topics(NAMESPACE, Vec::new())
        .expect("broker supports scalable topics");
    let failed_tc = conn
        .watch_tc_assignments()
        .expect("broker supports TC metadata discovery");
    // The open frames are not under test: drop them so each step reports only its own reply.
    let _ = conn.poll_transmit();

    vec![
        step(
            conn,
            at,
            "client rejects a bodyless DAG update",
            &dag_update(bodyless_dag, None, None),
        ),
        step(
            conn,
            at,
            "client rejects an invalid DAG layout",
            &dag_update(invalid_dag, Some(short_coverage_dag()), None),
        ),
        step(
            conn,
            at,
            "client rejects a bodyless namespace-watch update",
            &topics_update(bodyless_topics, None),
        ),
        step(
            conn,
            at,
            "client rejects a bodyless TC-watch update",
            &tc_update(bodyless_tc, None),
        ),
        step(
            conn,
            at,
            "broker fails a DAG session",
            &dag_update(failed_dag, None, not_found),
        ),
        step(
            conn,
            at,
            "broker fails a namespace watch",
            &topics_update(failed_topics, not_found),
        ),
        step(
            conn,
            at,
            "broker fails a TC watch",
            &tc_update(failed_tc, not_found),
        ),
    ]
}

#[test]
fn engines_close_a_client_rejected_watch_at_the_broker_and_only_that() {
    let tokio = tokio_projection(rejection_transcript);
    let moonpool = moonpool_projection(rejection_transcript);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the events and frames of every rejection"
    );

    assert_eq!(
        tokio,
        vec![
            "client rejects a bodyless DAG update: events=[\"DagWatchClosed(1)\"] \
             closes=[\"ScalableTopicClose(1)\"]"
                .to_owned(),
            "client rejects an invalid DAG layout: events=[\"DagWatchClosed(2)\"] \
             closes=[\"ScalableTopicClose(2)\"]"
                .to_owned(),
            "client rejects a bodyless namespace-watch update: \
             events=[\"ScalableTopicsWatchClosed(1)\"] closes=[\"WatchScalableTopicsClose(1)\"]"
                .to_owned(),
            "client rejects a bodyless TC-watch update: events=[\"TcAssignmentsWatchClosed(2)\"] \
             closes=[\"WatchTcAssignmentsClose(2)\"]"
                .to_owned(),
            "broker fails a DAG session: events=[\"DagWatchClosed(3)\"] closes=[]".to_owned(),
            "broker fails a namespace watch: events=[\"ScalableTopicsWatchClosed(3)\"] closes=[]"
                .to_owned(),
            "broker fails a TC watch: events=[\"TcAssignmentsWatchClosed(4)\"] closes=[]"
                .to_owned(),
        ],
        "a client-rejected watch must be closed at the broker; a broker-failed one must not"
    );
}
