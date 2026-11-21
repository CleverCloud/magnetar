// SPDX-License-Identifier: Apache-2.0

//! A topic-list snapshot reply that lands after its request was cancelled is ignored, identically
//! on both engines (ADR-0024 layer (d)).
//!
//! A dropped or timed-out `topic_list_snapshot` future calls `Connection::cancel_request`, which
//! documents that "a late broker reply is ignored". The `CommandWatchTopicListSuccess` arm records
//! a `TopicListSnapshot` outcome holding the full topic list regardless, and nothing ever takes it.
//! Both engines wrap the same `magnetar-proto` state machine, so they must agree, and the reply
//! must leave no outcome behind.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, decode_one, encode_command,
    pb,
};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// What the connection holds for a request after it was answered, as `(topics, request id)` of a
/// recorded `TopicListSnapshot` outcome, or `None` when nothing was recorded.
type Recorded = Option<(Vec<String>, u64)>;

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

fn recorded(outcome: Option<OpOutcome>) -> Recorded {
    match outcome {
        None => None,
        Some(OpOutcome::TopicListSnapshot {
            request_id, topics, ..
        }) => Some((topics, request_id.0)),
        Some(other) => panic!("unexpected outcome for a topic-list snapshot: {other:?}"),
    }
}

/// Answers one live and one cancelled snapshot request, and returns what the connection recorded
/// for each, in that order.
fn recorded_per_request(conn: &mut Connection, now: Instant) -> (Recorded, Recorded) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    let (live, live_watcher) = watch(conn);
    feed(conn, now, &success(live, live_watcher));
    let live_outcome = recorded(conn.take_outcome(PendingOpKey::Request(live)));

    let (cancelled, cancelled_watcher) = watch(conn);
    conn.cancel_request(cancelled);
    feed(conn, now, &success(cancelled, cancelled_watcher));
    let cancelled_outcome = recorded(conn.take_outcome(PendingOpKey::Request(cancelled)));

    (live_outcome, cancelled_outcome)
}

#[test]
fn engines_ignore_a_snapshot_reply_after_cancel() {
    let tokio = tokio_projection(recorded_per_request);
    let moonpool = moonpool_projection(recorded_per_request);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what a live and a cancelled snapshot reply leave behind"
    );

    let (live, cancelled) = tokio;
    assert!(
        live.is_some(),
        "a reply to a live snapshot request must still resolve it"
    );
    assert_eq!(
        cancelled, None,
        "a snapshot reply for a cancelled request must be ignored, but the connection kept an \
         outcome nobody can take"
    );
}
