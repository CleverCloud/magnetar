// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher the connection stops tracking must not stay registered at the
//! broker, and a delta for a watcher the connection does not track must not be queued.
//!
//! `Connection::watch_topic_list` writes `CommandWatchTopicList`, which makes the broker hold a
//! watcher for the life of the connection and push `CommandWatchTopicUpdate` for every matching
//! topic change. Nothing ever sends `CommandWatchTopicListClose`, so the broker's watcher outlives
//! the registry entry the client drops:
//!
//! - a snapshot the caller gave up on (its future was dropped, or the operation deadline fired)
//!   goes through `Connection::cancel_request`, which removes the registry entry but leaves the
//!   registration the connection already put on the wire standing at the broker;
//! - the `CommandWatchTopicUpdate` arm then queues a `TopicListChanged` for any watcher id, tracked
//!   or not, and both runtimes append every one to a per-client queue without a bound, so a watcher
//!   nobody holds keeps growing it at the rate of the namespace's topic churn.
//!
//! The acknowledged-watcher half of the leak (a `PatternConsumer` that is closed or dropped, and
//! `PulsarClient::topic_list_snapshot` after its reply) has no `Connection` entry point to call
//! today, so it is proved one layer up, in
//! `crates/magnetar-differential/tests/pattern_watcher_close_equivalence.rs`.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, RequestId, decode_one, encode_command, pb,
};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

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
    while conn.poll_event().is_some() {}
    conn
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
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
    assert_eq!(registrations[0].request_id, request_id.0);
    (request_id, registrations[0].watcher_id)
}

fn success(request_id: RequestId, watcher_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
            request_id: request_id.0,
            watcher_id,
            topic: vec!["persistent://public/default/orders-0".to_owned()],
            topics_hash: "hash-1".to_owned(),
        }),
        ..Default::default()
    }
}

fn update(watcher_id: u64, added: &str) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicUpdate as i32,
        watch_topic_update: Some(pb::CommandWatchTopicUpdate {
            watcher_id,
            new_topics: vec![added.to_owned()],
            deleted_topics: Vec::new(),
            topics_hash: "hash-2".to_owned(),
        }),
        ..Default::default()
    }
}

/// The watcher ids of the `CommandWatchTopicListClose` frames among `commands`.
fn closed_watchers(commands: &[pb::BaseCommand]) -> Vec<u64> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list_close.as_ref())
        .map(|close| close.watcher_id)
        .collect()
}

/// A snapshot the caller gave up on while its registration was in flight (the future was dropped
/// or the operation deadline fired, both of which call `cancel_request`) must be closed at the
/// broker: the registration is already on the wire, so the broker holds a watcher for it that
/// pushes an update for every matching topic change until the connection ends.
#[test]
fn cancelled_pending_watch_is_closed_at_the_broker() {
    let now = Instant::now();
    let mut conn = connected(now);

    let (cancelled, cancelled_watcher) = watch(&mut conn);
    // A second watcher that stays pending: it must be left alone, so the assertion below is about
    // the cancelled one and not about every watcher being closed.
    let (_kept, kept_watcher) = watch(&mut conn);
    assert_ne!(cancelled_watcher, kept_watcher);

    conn.cancel_request(cancelled);

    let after_cancel = drain_commands(&mut conn);
    assert_eq!(
        closed_watchers(&after_cancel),
        vec![cancelled_watcher],
        "cancelling a watch whose registration is on the wire must stage exactly one \
         CommandWatchTopicListClose for it; the frames staged were {:?}",
        after_cancel
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>()
    );
}

/// Control: a delta for a watcher the connection tracks still surfaces, so the two tests below
/// are about watchers the connection does not track and not about deltas never surfacing.
#[test]
fn delta_for_an_acknowledged_watcher_surfaces() {
    let now = Instant::now();
    let mut conn = connected(now);

    let (request_id, watcher_id) = watch(&mut conn);
    feed(&mut conn, now, &success(request_id, watcher_id));
    while conn.poll_event().is_some() {}

    feed(
        &mut conn,
        now,
        &update(watcher_id, "persistent://public/default/orders-1"),
    );
    match conn.poll_event() {
        Some(ConnectionEvent::TopicListChanged { added, .. }) => {
            assert_eq!(added, vec!["persistent://public/default/orders-1"]);
        }
        other => panic!("expected the tracked watcher's delta, got {other:?}"),
    }
}

/// The broker keeps pushing for a watcher the client cancelled until it sees a close (and for the
/// frames already in flight when it does). The connection tracks no such watcher, so the delta has
/// no owner: queueing it grows the per-client queue for as long as the watcher lives.
#[test]
fn delta_for_a_cancelled_watcher_queues_nothing() {
    let now = Instant::now();
    let mut conn = connected(now);

    let (cancelled, cancelled_watcher) = watch(&mut conn);
    conn.cancel_request(cancelled);
    let _ = drain_commands(&mut conn);

    feed(
        &mut conn,
        now,
        &update(cancelled_watcher, "persistent://public/default/orders-9"),
    );
    let queued = conn.poll_event();
    assert!(
        queued.is_none(),
        "a delta for a watcher the connection no longer tracks must not be queued, but it \
         produced {queued:?}"
    );
}

/// The same for a watcher id the connection never registered.
#[test]
fn delta_for_an_unknown_watcher_queues_nothing() {
    let now = Instant::now();
    let mut conn = connected(now);

    feed(
        &mut conn,
        now,
        &update(9_999, "persistent://public/default/orders-9"),
    );
    let queued = conn.poll_event();
    assert!(
        queued.is_none(),
        "a delta for a watcher id the connection never registered must not be queued, but it \
         produced {queued:?}"
    );
}
