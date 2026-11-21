// SPDX-License-Identifier: Apache-2.0

//! What happens to a PIP-145 topic-list watcher around the re-registration a reconnect owes it
//! (ADR-0136), identically on both engines (ADR-0024 layer (d)).
//!
//! `topic_watcher_reconnect_equivalence.rs` proves the registration goes out again. This file
//! proves the rest of the contract on the same two `ConnectionShared` states: the broker's reply to
//! the re-registration leaves no outcome and no event and the watcher keeps receiving updates; a
//! refusal leaves no outcome and is retried by the next rebuild; a watcher still awaiting its
//! first reply when the session resets is not registered again.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, OpOutcome, PendingOpKey, RequestId, decode_one,
    encode_command, pb,
};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// Everything the scenario observes, compared field by field across the engines.
#[derive(Debug, PartialEq, Eq)]
struct Projection {
    /// `(namespace, pattern, topics_hash)` of each registration the first reconnect carries.
    first_reconnect: Vec<(String, String, Option<String>)>,
    /// Whether the reply to that registration left an outcome behind.
    reply_left_an_outcome: bool,
    /// Whether the reply to that registration queued an event.
    reply_queued_an_event: bool,
    /// The topics the watcher's next delta reports as added.
    delta_added: Vec<String>,
    /// The hash the second reconnect sends, which is the delta's.
    second_reconnect_hash: Option<String>,
    /// Whether the refusal of a registration left an outcome behind.
    refusal_left_an_outcome: bool,
    /// Registrations the rebuild after a refusal carries.
    retry_registrations: usize,
    /// Registrations a reconnect carries for a watcher that never got its first reply.
    unacknowledged_registrations: usize,
    /// How that watcher's own request resolved.
    unacknowledged_outcome: &'static str,
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

/// The transport drops and the supervisor redials: reset, handshake, then re-attach.
fn reconnect(conn: &mut Connection, now: Instant) -> Vec<pb::CommandWatchTopicList> {
    conn.reset();
    handshake(conn, now);
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    drain_registrations(conn)
}

fn scenario(conn: &mut Connection, now: Instant) -> Projection {
    handshake(conn, now);

    // An acknowledged watcher, then the first reconnect and the broker's reply to its
    // re-registration.
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let registration = drain_registrations(conn).remove(0);
    let watcher_id = registration.watcher_id;
    feed(conn, now, &success(request_id.0, watcher_id, "hash-1"));
    while conn.poll_event().is_some() {}

    let first = reconnect(conn, now);
    let first_reconnect = first
        .iter()
        .map(|watch| {
            (
                watch.namespace.clone(),
                watch.topics_pattern.clone(),
                watch.topics_hash.clone(),
            )
        })
        .collect();
    let reattach_id = first[0].request_id;
    feed(conn, now, &success(reattach_id, watcher_id, "hash-2"));
    let reply_left_an_outcome = conn
        .take_outcome(PendingOpKey::Request(RequestId(reattach_id)))
        .is_some();
    let reply_queued_an_event = conn.poll_event().is_some();

    // The watcher is live again: its next delta surfaces and its hash is the next reconnect's.
    feed(
        conn,
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
    let delta_added = match conn.poll_event() {
        Some(ConnectionEvent::TopicListChanged { added, .. }) => added,
        _ => Vec::new(),
    };
    let second = reconnect(conn, now);
    let second_reconnect_hash = second[0].topics_hash.clone();

    // The broker refuses the re-registration: no outcome, and the next rebuild retries it.
    feed(
        conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: second[0].request_id,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "synthetic watcher refusal".to_owned(),
            }),
            ..Default::default()
        },
    );
    let refusal_left_an_outcome = conn
        .take_outcome(PendingOpKey::Request(RequestId(second[0].request_id)))
        .is_some();
    let _ = conn.rebuild_consumers();
    let retry_registrations = drain_registrations(conn).len();

    // A second watcher that never got its first reply is not registered again, and its caller is
    // told the session was lost. The acknowledged one is registered again alone.
    let unacknowledged = conn.watch_topic_list(NAMESPACE, "persistent://public/default/never-.*");
    let _ = drain_registrations(conn);
    let after = reconnect(conn, now);
    let unacknowledged_registrations = after
        .iter()
        .filter(|watch| watch.topics_pattern.ends_with("never-.*"))
        .count();
    let unacknowledged_outcome = match conn.take_outcome(PendingOpKey::Request(unacknowledged)) {
        Some(OpOutcome::SessionLost { .. }) => "session-lost",
        Some(_) => "other",
        None => "unresolved",
    };

    Projection {
        first_reconnect,
        reply_left_an_outcome,
        reply_queued_an_event,
        delta_added,
        second_reconnect_hash,
        refusal_left_an_outcome,
        retry_registrations,
        unacknowledged_registrations,
        unacknowledged_outcome,
    }
}

#[test]
fn engines_agree_on_the_reattach_contract() {
    let tokio = tokio_projection();
    let moonpool = moonpool_projection();
    assert_eq!(
        tokio, moonpool,
        "engines must agree on how a re-registered topic-list watcher behaves"
    );

    assert_eq!(
        tokio.first_reconnect,
        vec![(
            NAMESPACE.to_owned(),
            PATTERN.to_owned(),
            Some("hash-1".to_owned())
        )],
        "the reconnect registers the acknowledged watcher again with its last hash"
    );
    assert!(
        !tokio.reply_left_an_outcome && !tokio.reply_queued_an_event,
        "the reply to a re-registration has no future behind it: {tokio:?}"
    );
    assert_eq!(
        tokio.delta_added,
        vec!["persistent://public/default/orders-2"]
    );
    assert_eq!(tokio.second_reconnect_hash.as_deref(), Some("hash-3"));
    assert!(
        !tokio.refusal_left_an_outcome,
        "a refused re-registration records no outcome: {tokio:?}"
    );
    assert_eq!(tokio.retry_registrations, 1, "a refusal is retried");
    assert_eq!(tokio.unacknowledged_registrations, 0);
    assert_eq!(tokio.unacknowledged_outcome, "session-lost");
}
