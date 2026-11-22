// SPDX-License-Identifier: Apache-2.0

//! `Connection::close_topic_watcher` and the other ways a PIP-145 topic-list watcher stops being
//! tracked (ADR-0156). `topic_watcher_close.rs` proves a *pending* watch the caller cancels is
//! closed at the broker and that a delta for an untracked watcher queues nothing; this file proves
//! the entry point that closes an acknowledged watcher, in every state the registry can hold it:
//!
//! - an acknowledged watcher is closed at the broker once, and a second close, the broker's reply
//!   to the close and a delta that was in flight when the close was staged all leave no trace;
//! - a watcher a reset took down with its session is only forgotten: the broker holds nothing for
//!   it on the new session, so the close stages no frame and the rebuild does not register it
//!   again;
//! - a watcher whose re-registration is on the wire is closed, and the reply to that
//!   re-registration is ignored;
//! - a watch the broker refused holds nothing at the broker, so its registry entry goes without a
//!   frame;
//! - closing on a connection that was shut down only forgets the watcher.
//!
//! Exercised through the moonpool engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-tokio` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/topic_watcher_close_lifecycle.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, OpOutcome, PendingOpKey, RequestId, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

fn handshake(conn: &mut Connection, now: Instant) {
    conn.begin_handshake().expect("begin_handshake");
    feed(
        conn,
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

fn registrations(commands: &[pb::BaseCommand]) -> Vec<&pb::CommandWatchTopicList> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list.as_ref())
        .collect()
}

fn closes(commands: &[pb::BaseCommand]) -> Vec<&pb::CommandWatchTopicListClose> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list_close.as_ref())
        .collect()
}

fn success(request_id: u64, watcher_id: u64, topics_hash: &str) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
            request_id,
            watcher_id,
            topic: vec!["persistent://public/default/orders-1".to_owned()],
            topics_hash: topics_hash.to_owned(),
        }),
        ..Default::default()
    }
}

fn update(watcher_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicUpdate as i32,
        watch_topic_update: Some(pb::CommandWatchTopicUpdate {
            watcher_id,
            new_topics: vec!["persistent://public/default/orders-2".to_owned()],
            deleted_topics: Vec::new(),
            topics_hash: "hash-update".to_owned(),
        }),
        ..Default::default()
    }
}

fn close_reply(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    }
}

fn error_reply(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::AuthorizationError as i32,
            message: "synthetic watch refusal".to_owned(),
        }),
        ..Default::default()
    }
}

/// Register a watcher and let the broker acknowledge it with `hash-1`. Returns its id.
fn acknowledged_watcher(conn: &mut Connection, now: Instant) -> u64 {
    let (request_id, watcher_id) = conn.watch_topic_list_with_watcher_id(NAMESPACE, PATTERN);
    let staged = drain_commands(conn);
    assert_eq!(registrations(&staged)[0].watcher_id, watcher_id);
    feed(conn, now, &success(request_id.0, watcher_id, "hash-1"));
    while conn.poll_event().is_some() {}
    watcher_id
}

/// The transport drops and the supervisor redials: reset, handshake, then re-attach. Returns the
/// commands the new session carries.
fn reconnect(conn: &mut Connection, now: Instant) -> Vec<pb::BaseCommand> {
    conn.reset();
    handshake(conn, now);
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    drain_commands(conn)
}

/// Closing an acknowledged watcher tells the broker once. A second close, the broker's reply to
/// the close (success or error), and a delta that was in flight when the close was staged leave no
/// frame, no outcome and no event: the connection tracks nothing for the watcher any more.
#[test]
fn closing_an_acknowledged_watcher_closes_it_once_and_forgets_it() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    conn.close_topic_watcher(watcher_id);
    let staged = drain_commands(&mut conn);
    let staged_closes = closes(&staged);
    assert_eq!(
        staged_closes.len(),
        1,
        "exactly one close frame: {staged:?}"
    );
    assert_eq!(staged_closes[0].watcher_id, watcher_id);
    let close_request = staged_closes[0].request_id;

    conn.close_topic_watcher(watcher_id);
    assert!(
        drain_commands(&mut conn).is_empty(),
        "a watcher closes once"
    );

    feed(&mut conn, now, &update(watcher_id));
    assert!(
        conn.poll_event().is_none(),
        "a delta in flight when the close was staged queues nothing"
    );

    feed(&mut conn, now, &close_reply(close_request));
    feed(&mut conn, now, &error_reply(close_request));
    assert!(conn.poll_event().is_none(), "the replies record no event");
    assert!(
        conn.take_outcome(PendingOpKey::Request(RequestId(close_request)))
            .is_none(),
        "the replies record no outcome nobody would take"
    );
}

/// A close for an id the connection never registered stages nothing.
#[test]
fn closing_an_unknown_watcher_stages_nothing() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);

    conn.close_topic_watcher(9_999);

    let staged = drain_commands(&mut conn);
    assert!(
        staged.is_empty(),
        "closing an id nobody registered stages no frame: {staged:?}"
    );
}

/// A reset takes the registration down with the session, so closing the watcher afterwards needs
/// no frame, and the rebuild must not register it again.
#[test]
fn closing_a_watcher_a_reset_took_down_stages_nothing_and_it_is_not_registered_again() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    conn.reset();
    handshake(&mut conn, now);
    conn.close_topic_watcher(watcher_id);
    let _ = conn.rebuild_consumers();

    let staged = drain_commands(&mut conn);
    assert!(
        closes(&staged).is_empty(),
        "the new session holds no watcher to close: {staged:?}"
    );
    assert!(
        registrations(&staged).is_empty(),
        "a closed watcher is not registered again: {staged:?}"
    );
}

/// A re-registration is on the wire, so the broker holds a watcher for it on the new session:
/// closing stages the frame, and the reply to the re-registration is then ignored.
#[test]
fn closing_a_watcher_whose_reattach_is_on_the_wire_closes_it_and_ignores_the_late_reply() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    let second = reconnect(&mut conn, now);
    let reattach = registrations(&second)[0].clone();
    assert_eq!(reattach.watcher_id, watcher_id);

    conn.close_topic_watcher(watcher_id);
    let staged = drain_commands(&mut conn);
    let staged_closes = closes(&staged);
    assert_eq!(staged_closes.len(), 1, "one close frame: {staged:?}");
    assert_eq!(staged_closes[0].watcher_id, watcher_id);

    feed(
        &mut conn,
        now,
        &success(reattach.request_id, watcher_id, "hash-2"),
    );
    assert!(
        conn.poll_event().is_none(),
        "the late reply records no event"
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(RequestId(reattach.request_id)))
            .is_none(),
        "the late reply records no outcome"
    );
    feed(&mut conn, now, &update(watcher_id));
    assert!(
        conn.poll_event().is_none(),
        "the watcher is not tracked again by its late reply"
    );
}

/// The broker refused the watch, so it holds nothing: the registry entry goes with the refusal
/// and closing the id afterwards stages no frame. The refusal itself still resolves the awaiting
/// caller with an error.
#[test]
fn a_refused_watch_is_forgotten_without_a_close_frame() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let (request_id, watcher_id) = conn.watch_topic_list_with_watcher_id(NAMESPACE, PATTERN);
    let _ = drain_commands(&mut conn);

    feed(&mut conn, now, &error_reply(request_id.0));
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(request_id)),
            Some(OpOutcome::Error { .. })
        ),
        "the refusal resolves the watch with an error"
    );

    feed(&mut conn, now, &update(watcher_id));
    assert!(
        conn.poll_event().is_none(),
        "a refused watch is not tracked, so a delta naming it queues nothing"
    );
    conn.close_topic_watcher(watcher_id);
    conn.cancel_request(request_id);
    assert!(
        drain_commands(&mut conn).is_empty(),
        "nothing is held at the broker for a refused watch"
    );
}

/// A close on a connection that was shut down only forgets the watcher: it must not panic or
/// resurrect the entry, and the frame it stages is never written.
#[test]
fn closing_a_watcher_on_a_closed_connection_only_forgets_it() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    conn.close();
    conn.fail_all_pending("test shutdown");
    conn.close_topic_watcher(watcher_id);

    assert!(conn.is_closed());
    feed(&mut conn, now, &update(watcher_id));
    let queued: Vec<_> = std::iter::from_fn(|| conn.poll_event())
        .filter(|event| matches!(event, ConnectionEvent::TopicListChanged { .. }))
        .collect();
    assert!(
        queued.is_empty(),
        "a closed watcher queues no delta: {queued:?}"
    );
}
