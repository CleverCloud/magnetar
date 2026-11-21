// SPDX-License-Identifier: Apache-2.0

//! A topic-list snapshot reply that lands after its request was cancelled must not be recorded,
//! exercised through the moonpool engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-tokio` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/topic_list_snapshot_late_success.rs`) and the
//! `magnetar-differential` equivalence scenario.
//!
//! A dropped or timed-out `topic_list_snapshot` future calls `Connection::cancel_request`, which
//! documents that "a late broker reply is ignored". The `CommandWatchTopicListSuccess` arm records
//! a `TopicListSnapshot` outcome holding the full topic list regardless, and nothing ever takes it.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, decode_one, encode_command,
    pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
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

#[test]
fn snapshot_reply_after_cancel_leaves_no_outcome() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

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
