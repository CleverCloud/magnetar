// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher the connection stops tracking must not stay registered at the
//! broker, and a delta for a watcher the connection does not track must not be queued, exercised
//! through the tokio engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-moonpool` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/topic_watcher_close.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! Nothing ever sends `CommandWatchTopicListClose`, so a snapshot the caller gave up on (its future
//! was dropped, or the operation deadline fired, both of which call `Connection::cancel_request`)
//! leaves the broker holding a watcher for the registration the connection already put on the
//! wire, and the `CommandWatchTopicUpdate` arm queues a `TopicListChanged` for any watcher id,
//! tracked or not, onto a per-client queue that has no bound.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, RequestId, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

fn handshake(conn: &mut Connection, now: Instant) {
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("apply CONNECTED");
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

#[test]
fn cancelled_pending_watch_is_closed_at_the_broker() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);

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

/// Control: a delta for a watcher the connection tracks still surfaces.
#[test]
fn delta_for_an_acknowledged_watcher_surfaces() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);

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

#[test]
fn delta_for_a_cancelled_watcher_queues_nothing() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);

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

#[test]
fn delta_for_an_unknown_watcher_queues_nothing() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);

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
