// SPDX-License-Identifier: Apache-2.0

//! A topic-list snapshot reply that lands after its request was cancelled must not be recorded.
//!
//! `Connection::cancel_request` is what a dropped or timed-out `topic_list_snapshot` future calls:
//! it removes the waker, the landed outcome, the pending-request discriminator and the watcher,
//! and documents that "a late broker reply is ignored". The `CommandWatchTopicListSuccess` arm
//! nevertheless records a `TopicListSnapshot` outcome unconditionally, holding the broker's full
//! topic list. Nobody is left to `take_outcome` it and `reset()` does not clear it, so it lives as
//! long as the connection.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, decode_one, encode_command,
    pb,
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
    conn
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

/// Starts a watcher and returns its request id and the watcher id the registration frame names.
fn watch(conn: &mut Connection) -> (RequestId, u64) {
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let mut wire = conn.poll_transmit();
    let mut watcher_id = None;
    while !wire.is_empty() {
        let command = decode_one(&mut wire)
            .expect("staged frame must decode")
            .command;
        if let Some(watch) = command.watch_topic_list {
            assert_eq!(watch.request_id, request_id.0);
            watcher_id = Some(watch.watcher_id);
        }
    }
    (
        request_id,
        watcher_id.expect("the registration frame names a watcher id"),
    )
}

fn success(request_id: RequestId, watcher_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
            request_id: request_id.0,
            watcher_id,
            topic: (0..3)
                .map(|n| format!("persistent://public/default/orders-{n}"))
                .collect(),
            topics_hash: "hash-1".to_owned(),
        }),
        ..Default::default()
    }
}

/// The broker answers the snapshot after its caller gave up (the future was dropped, or the
/// operation deadline fired and cancelled it). The reply has no waiter and no registered watcher
/// left to attach to, so it must leave nothing behind for a `take_outcome` that never comes.
#[test]
fn snapshot_reply_after_cancel_leaves_no_outcome() {
    let now = Instant::now();
    let mut conn = connected(now);

    // Control: a reply to a live request is recorded, so the assertion below is about the cancel
    // and not about snapshots never producing an outcome.
    let (live, live_watcher) = watch(&mut conn);
    feed(&mut conn, now, &success(live, live_watcher));
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(live)),
            Some(OpOutcome::TopicListSnapshot { .. })
        ),
        "a reply to a live snapshot request must still resolve it"
    );

    let (cancelled, cancelled_watcher) = watch(&mut conn);
    conn.cancel_request(cancelled);
    feed(&mut conn, now, &success(cancelled, cancelled_watcher));

    let leaked = conn.take_outcome(PendingOpKey::Request(cancelled));
    assert!(
        leaked.is_none(),
        "a snapshot reply for a cancelled request must be ignored, but the connection kept an \
         outcome nobody can take: {leaked:?}"
    );
}

/// What a cancelled snapshot leaves behind: no outcome and no local watcher, while the broker's
/// watcher of it stays open. A snapshot asked for afterwards on the same connection is a request of
/// its own: it resolves on its own reply, under the watcher id this connection gave it.
#[test]
fn a_snapshot_asked_for_after_a_cancelled_one_resolves_on_its_own_reply() {
    let now = Instant::now();
    let mut conn = connected(now);
    let (cancelled, cancelled_watcher) = watch(&mut conn);
    conn.cancel_request(cancelled);
    feed(&mut conn, now, &success(cancelled, cancelled_watcher));
    assert!(
        conn.take_outcome(PendingOpKey::Request(cancelled))
            .is_none()
    );

    let (next, next_watcher) = watch(&mut conn);
    assert_ne!(next, cancelled);
    assert_ne!(next_watcher, cancelled_watcher);
    feed(&mut conn, now, &success(next, next_watcher));
    match conn.take_outcome(PendingOpKey::Request(next)) {
        Some(OpOutcome::TopicListSnapshot {
            request_id,
            watcher_id,
            topics,
        }) => {
            assert_eq!(request_id, next);
            assert_eq!(watcher_id, next_watcher);
            assert_eq!(topics.len(), 3);
        }
        other => panic!("the next snapshot must resolve on its own reply, got {other:?}"),
    }
}
