// SPDX-License-Identifier: Apache-2.0

//! What happens to a PIP-145 topic-list watcher around the re-registration a reconnect owes it
//! (ADR-0136), exercised through the tokio engine's [`ConnectionShared`] state. Mirrors the
//! matching `magnetar-runtime-moonpool` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/topic_watcher_reattach.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! `topic_watcher_reconnect.rs` proves the registration goes out again; this file proves the rest
//! of the contract:
//!
//! - the broker's reply to that registration has no future behind it, so it leaves no outcome and
//!   no event, and the watcher keeps receiving updates afterwards;
//! - a refusal leaves no outcome either, and the next rebuild tries again;
//! - a watcher still awaiting its first reply when the session resets is not registered again,
//!   because its caller was told the session was lost;
//! - a registration the broker never answered is owed again after another reset, and a rebuild is
//!   idempotent within one session.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, OpOutcome, PendingOpKey, RequestId, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
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

/// The `CommandWatchTopicList` frames staged on the connection's outbound buffer.
fn drain_registrations(conn: &mut Connection) -> Vec<pb::CommandWatchTopicList> {
    let mut bytes = conn.poll_transmit();
    let mut registrations = Vec::new();
    while !bytes.is_empty() {
        let command = decode_one(&mut bytes)
            .expect("staged frame must decode")
            .command;
        registrations.extend(command.watch_topic_list);
    }
    registrations
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

/// Register a watcher and let the broker acknowledge it with `hash-1`. Returns its id.
fn acknowledged_watcher(conn: &mut Connection, now: Instant) -> u64 {
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let registration = drain_registrations(conn).remove(0);
    feed(
        conn,
        now,
        &success(request_id.0, registration.watcher_id, "hash-1"),
    );
    while conn.poll_event().is_some() {}
    registration.watcher_id
}

/// The transport drops and the supervisor redials: reset, handshake, then re-attach. Returns the
/// watcher registrations the new session carries.
fn reconnect(conn: &mut Connection, now: Instant) -> Vec<pb::CommandWatchTopicList> {
    conn.reset();
    handshake(conn, now);
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    drain_registrations(conn)
}

/// The reply to a re-registration is consumed in place: it records no outcome nobody would take,
/// queues no event, and the watcher keeps receiving updates (and keeps its hash) afterwards.
#[test]
fn reattach_reply_is_consumed_in_place_and_the_watcher_keeps_receiving_updates() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    let second = reconnect(&mut conn, now);
    assert_eq!(second.len(), 1, "one registration per watcher: {second:?}");
    let reattach = &second[0];
    assert_eq!(reattach.watcher_id, watcher_id, "the watcher keeps its id");
    assert_eq!(
        reattach.topics_hash.as_deref(),
        Some("hash-1"),
        "the registration carries the hash the broker last reported"
    );

    // A rebuild is idempotent within one session: the registration is awaiting its reply.
    let _ = conn.rebuild_consumers();
    assert!(
        drain_registrations(&mut conn).is_empty(),
        "an unanswered registration is not sent twice on one session"
    );

    feed(
        &mut conn,
        now,
        &success(reattach.request_id, watcher_id, "hash-2"),
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(RequestId(reattach.request_id)))
            .is_none(),
        "no future awaits the re-registration, so no outcome may be recorded for it"
    );
    assert!(
        conn.poll_event().is_none(),
        "the re-registration reply queues no event"
    );

    // The watcher is live again: a delta for it surfaces, and its hash is the one the next
    // reconnect sends.
    feed(
        &mut conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicUpdate as i32,
            watch_topic_update: Some(pb::CommandWatchTopicUpdate {
                watcher_id,
                new_topics: vec!["persistent://public/default/orders-2".to_owned()],
                deleted_topics: Vec::new(),
                topics_hash: "hash-3".to_owned(),
            }),
            ..Default::default()
        },
    );
    match conn.poll_event() {
        Some(ConnectionEvent::TopicListChanged {
            watcher_id: addressed_to,
            added,
            removed,
        }) => {
            assert_eq!(
                addressed_to, watcher_id,
                "the delta is addressed to the watcher registered again"
            );
            assert_eq!(added, vec!["persistent://public/default/orders-2"]);
            assert_eq!(removed, Vec::<String>::new());
        }
        other => panic!("expected the watcher's delta, got {other:?}"),
    }
    let third = reconnect(&mut conn, now);
    assert_eq!(third.len(), 1, "the watcher survives every reconnect");
    assert_eq!(third[0].watcher_id, watcher_id);
    assert_eq!(third[0].topics_hash.as_deref(), Some("hash-3"));
}

/// A broker that refuses the re-registration leaves no outcome behind (no future awaits it), and
/// the watcher is registered again by the next rebuild instead of being lost.
#[test]
fn rejected_reattach_leaves_no_outcome_and_the_next_rebuild_retries_it() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    let second = reconnect(&mut conn, now);
    assert_eq!(second.len(), 1);
    let refused = second[0].request_id;
    feed(
        &mut conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: refused,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "synthetic watcher refusal".to_owned(),
            }),
            ..Default::default()
        },
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(RequestId(refused)))
            .is_none(),
        "no future awaits the re-registration, so no outcome may be recorded for its refusal"
    );

    let _ = conn.rebuild_consumers();
    let retry = drain_registrations(&mut conn);
    assert_eq!(retry.len(), 1, "a refused registration is owed again");
    assert_eq!(retry[0].watcher_id, watcher_id);
    assert_ne!(retry[0].request_id, refused, "under a fresh request id");
}

/// A watcher whose first reply never came is not registered again: its request resolves with
/// `SessionLost`, so its caller holds nothing to keep alive.
#[test]
fn watcher_awaiting_its_first_reply_is_not_registered_again() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    assert_eq!(drain_registrations(&mut conn).len(), 1);

    let second = reconnect(&mut conn, now);
    assert!(
        second.is_empty(),
        "an unacknowledged watcher is not registered again: {second:?}"
    );
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(request_id)),
            Some(OpOutcome::SessionLost { .. })
        ),
        "the caller of the unacknowledged watcher is told the session was lost"
    );
}

/// A registration the broker never answered is owed again after the next reset, however many
/// resets come before a rebuild.
#[test]
fn unanswered_reattach_is_owed_again_after_another_reset() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn, now);
    let watcher_id = acknowledged_watcher(&mut conn, now);

    let second = reconnect(&mut conn, now);
    assert_eq!(second.len(), 1);

    // The link flaps again before the broker answered, and once more before any rebuild.
    conn.reset();
    conn.reset();
    handshake(&mut conn, now);
    let _ = conn.rebuild_consumers();
    let third = drain_registrations(&mut conn);
    assert_eq!(third.len(), 1, "owed once, however many resets: {third:?}");
    assert_eq!(third[0].watcher_id, watcher_id);
    assert_ne!(third[0].request_id, second[0].request_id);
}
