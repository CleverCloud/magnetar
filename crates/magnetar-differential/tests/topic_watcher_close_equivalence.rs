// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher the connection stops tracking must not stay registered at the
//! broker, and a delta for a watcher the connection does not track must not be queued,
//! identically on both engines (ADR-0024 layer (d)).
//!
//! Nothing ever sends `CommandWatchTopicListClose`, so a snapshot the caller gave up on (its future
//! was dropped, or the operation deadline fired, both of which call `Connection::cancel_request`)
//! leaves the broker holding a watcher for the registration already on the wire, and the
//! `CommandWatchTopicUpdate` arm queues a `TopicListChanged` for any watcher id, tracked or not,
//! onto a per-client queue that has no bound.
//!
//! The acknowledged-watcher half (a closed or dropped `PatternConsumer`, and
//! `PulsarClient::topic_list_snapshot` after its reply) is proved on the façade in
//! `pattern_watcher_close_equivalence.rs`.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, RequestId, decode_one, encode_command, pb,
};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// Everything the scenario observes, compared field by field across the engines. Watchers are
/// named by role, not by id, so a difference reads as a difference in behaviour.
#[derive(Debug, PartialEq, Eq)]
struct Projection {
    /// The roles of the watchers a `CommandWatchTopicListClose` frame named after the cancel.
    closed_at_the_broker: Vec<&'static str>,
    /// Whether the delta of an acknowledged watcher surfaced as a `TopicListChanged`.
    tracked_delta_surfaced: bool,
    /// The roles of the watchers whose delta was queued although the connection tracks none.
    untracked_deltas_queued: Vec<&'static str>,
}

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

fn tokio_projection() -> Projection {
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    scenario(&mut conn, Instant::now())
}

fn moonpool_projection() -> Projection {
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    scenario(&mut conn, Instant::now())
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

fn handshake(conn: &mut Connection, now: Instant) {
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("apply Connected");
    let _ = conn.poll_transmit();
    while conn.poll_event().is_some() {}
}

/// Decode every frame staged on the connection's outbound buffer.
fn drain_commands(conn: &mut Connection) -> Vec<pb::BaseCommand> {
    let mut wire = conn.poll_transmit();
    let mut commands = Vec::new();
    while !wire.is_empty() {
        commands.push(
            decode_one(&mut wire)
                .expect("staged frame must decode")
                .command,
        );
    }
    commands
}

/// Starts a watcher whose registration is on the wire, and returns its request id and the watcher
/// id that registration names.
fn watch(conn: &mut Connection) -> (RequestId, u64) {
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let registrations: Vec<_> = drain_commands(conn)
        .into_iter()
        .filter_map(|command| command.watch_topic_list)
        .collect();
    assert_eq!(
        registrations.len(),
        1,
        "one registration: {registrations:?}"
    );
    (request_id, registrations[0].watcher_id)
}

fn update(watcher_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicUpdate as i32,
        watch_topic_update: Some(pb::CommandWatchTopicUpdate {
            watcher_id,
            new_topics: vec!["persistent://public/default/orders-9".to_owned()],
            deleted_topics: Vec::new(),
            topics_hash: "hash-2".to_owned(),
        }),
        ..Default::default()
    }
}

fn scenario(conn: &mut Connection, now: Instant) -> Projection {
    handshake(conn, now);

    // A watcher the broker acknowledged: the control, whose delta must keep surfacing.
    let (tracked, tracked_watcher) = watch(conn);
    feed(
        conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
            watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                request_id: tracked.0,
                watcher_id: tracked_watcher,
                topic: Vec::new(),
                topics_hash: "hash-1".to_owned(),
            }),
            ..Default::default()
        },
    );
    while conn.poll_event().is_some() {}
    feed(conn, now, &update(tracked_watcher));
    let tracked_delta_surfaced = matches!(
        conn.poll_event(),
        Some(ConnectionEvent::TopicListChanged { .. })
    );
    while conn.poll_event().is_some() {}

    // Two watchers whose registration is on the wire; the caller gives up on one of them.
    let (cancelled, cancelled_watcher) = watch(conn);
    let (_kept, kept_watcher) = watch(conn);
    conn.cancel_request(cancelled);
    let closes: Vec<u64> = drain_commands(conn)
        .iter()
        .filter_map(|command| command.watch_topic_list_close.as_ref())
        .map(|close| close.watcher_id)
        .collect();

    // The broker keeps pushing for the watcher it was never told to forget, and a watcher id the
    // connection never registered shows up too.
    let mut untracked_deltas_queued = Vec::new();
    feed(conn, now, &update(cancelled_watcher));
    if conn.poll_event().is_some() {
        untracked_deltas_queued.push("cancelled");
    }
    while conn.poll_event().is_some() {}
    feed(conn, now, &update(9_999));
    if conn.poll_event().is_some() {
        untracked_deltas_queued.push("unknown");
    }

    let role = |watcher_id: u64| {
        if watcher_id == cancelled_watcher {
            "cancelled"
        } else if watcher_id == kept_watcher {
            "kept"
        } else {
            "other"
        }
    };
    Projection {
        closed_at_the_broker: closes.into_iter().map(role).collect(),
        tracked_delta_surfaced,
        untracked_deltas_queued,
    }
}

#[test]
fn engines_agree_on_closing_and_ignoring_watchers_the_connection_stops_tracking() {
    let tokio = tokio_projection();
    let moonpool = moonpool_projection();
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what happens to a cancelled topic-list watcher"
    );

    assert!(
        tokio.tracked_delta_surfaced,
        "control: an acknowledged watcher's delta surfaces: {tokio:?}"
    );
    assert_eq!(
        tokio.closed_at_the_broker,
        vec!["cancelled"],
        "cancelling a watch whose registration is on the wire closes exactly that watcher at the \
         broker: {tokio:?}"
    );
    assert!(
        tokio.untracked_deltas_queued.is_empty(),
        "a delta for a watcher the connection does not track must not be queued: {tokio:?}"
    );
}
