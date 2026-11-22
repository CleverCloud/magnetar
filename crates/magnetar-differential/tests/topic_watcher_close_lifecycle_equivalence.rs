// SPDX-License-Identifier: Apache-2.0

//! `Connection::close_topic_watcher` and the other ways a PIP-145 topic-list watcher stops being
//! tracked behave identically on both engines (ADR-0024 layer (d), ADR-0156).
//!
//! `topic_watcher_close_equivalence.rs` covers a pending watch the caller cancels and the deltas
//! the connection no longer queues. This file covers the entry point that closes an acknowledged
//! watcher, in every state the registry can hold it: acknowledged on the live session, taken down
//! by a reset and not registered again, re-registered and unanswered, and refused by the broker.
//! The façade-level half (a closed or dropped `PatternConsumer`, and `topic_list_snapshot`) is
//! `pattern_watcher_close_equivalence.rs`.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, decode_one, encode_command,
    pb,
};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// What an acknowledged watcher leaves after the connection closes it.
#[derive(Debug, PartialEq, Eq)]
struct Acknowledged {
    /// `CommandWatchTopicListClose` frames the first close staged.
    close_frames: usize,
    /// Frames a second close of the same id staged.
    second_close_frames: usize,
    /// Frames a close of an id that was never registered staged.
    unknown_close_frames: usize,
    /// Whether a delta naming the closed watcher queued an event.
    delta_queued: bool,
    /// Whether the broker's replies to the close recorded an event or an outcome.
    replies_left_a_trace: bool,
}

/// What a watcher a reset took down leaves after the connection closes it.
#[derive(Debug, PartialEq, Eq)]
struct Reset {
    close_frames: usize,
    registrations_after_rebuild: usize,
}

/// What a watcher with a re-registration on the wire leaves after the connection closes it.
#[derive(Debug, PartialEq, Eq)]
struct Reattached {
    close_frames: usize,
    late_reply_left_a_trace: bool,
    delta_queued_after_late_reply: bool,
}

/// What a watch the broker refused leaves.
#[derive(Debug, PartialEq, Eq)]
struct Refused {
    outcome_is_error: bool,
    delta_queued: bool,
    frames_after_close: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct Projection {
    acknowledged: Acknowledged,
    reset: Reset,
    reattached: Reattached,
    refused: Refused,
    /// Whether a delta queued after a close on a connection that was shut down.
    closed_connection_delta_queued: bool,
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
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
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
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

fn close_frames(commands: &[pb::BaseCommand]) -> Vec<pb::CommandWatchTopicListClose> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list_close)
        .collect()
}

fn registrations(commands: &[pb::BaseCommand]) -> Vec<pb::CommandWatchTopicList> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list.clone())
        .collect()
}

fn success(request_id: u64, watcher_id: u64, topics_hash: &str) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
            request_id,
            watcher_id,
            topic: Vec::new(),
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
            new_topics: vec!["persistent://public/default/orders-9".to_owned()],
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

fn acknowledged_watcher(conn: &mut Connection, now: Instant) -> u64 {
    let (request_id, watcher_id) = conn.watch_topic_list_with_watcher_id(NAMESPACE, PATTERN);
    let _ = drain_commands(conn);
    feed(conn, now, &success(request_id.0, watcher_id, "hash-1"));
    while conn.poll_event().is_some() {}
    watcher_id
}

fn delta_queued(conn: &mut Connection, now: Instant, watcher_id: u64) -> bool {
    feed(conn, now, &update(watcher_id));
    let queued = conn.poll_event().is_some();
    while conn.poll_event().is_some() {}
    queued
}

fn acknowledged(conn: &mut Connection, now: Instant) -> Acknowledged {
    let watcher_id = acknowledged_watcher(conn, now);

    conn.close_topic_watcher(watcher_id);
    let first = close_frames(&drain_commands(conn));
    conn.close_topic_watcher(watcher_id);
    let second = close_frames(&drain_commands(conn));
    conn.close_topic_watcher(9_999);
    let unknown = close_frames(&drain_commands(conn));

    let delta_queued = delta_queued(conn, now, watcher_id);
    let close_request = first.first().map_or(0, |close| close.request_id);
    feed(conn, now, &close_reply(close_request));
    feed(conn, now, &error_reply(close_request));
    let replies_left_a_trace = conn.poll_event().is_some()
        || conn
            .take_outcome(PendingOpKey::Request(RequestId(close_request)))
            .is_some();

    Acknowledged {
        close_frames: first.len(),
        second_close_frames: second.len(),
        unknown_close_frames: unknown.len(),
        delta_queued,
        replies_left_a_trace,
    }
}

fn reset(conn: &mut Connection, now: Instant) -> Reset {
    let watcher_id = acknowledged_watcher(conn, now);

    conn.reset();
    handshake(conn, now);
    conn.close_topic_watcher(watcher_id);
    let _ = conn.rebuild_consumers();
    let staged = drain_commands(conn);

    Reset {
        close_frames: close_frames(&staged).len(),
        registrations_after_rebuild: registrations(&staged).len(),
    }
}

fn reattached(conn: &mut Connection, now: Instant) -> Reattached {
    let watcher_id = acknowledged_watcher(conn, now);

    conn.reset();
    handshake(conn, now);
    let _ = conn.rebuild_consumers();
    let reattach = registrations(&drain_commands(conn)).remove(0);
    assert_eq!(reattach.watcher_id, watcher_id, "the watcher keeps its id");

    conn.close_topic_watcher(watcher_id);
    let close_frames = close_frames(&drain_commands(conn)).len();
    feed(
        conn,
        now,
        &success(reattach.request_id, watcher_id, "hash-2"),
    );
    let late_reply_left_a_trace = conn.poll_event().is_some()
        || conn
            .take_outcome(PendingOpKey::Request(RequestId(reattach.request_id)))
            .is_some();
    Reattached {
        close_frames,
        late_reply_left_a_trace,
        delta_queued_after_late_reply: delta_queued(conn, now, watcher_id),
    }
}

fn refused(conn: &mut Connection, now: Instant) -> Refused {
    let (request_id, watcher_id) = conn.watch_topic_list_with_watcher_id(NAMESPACE, PATTERN);
    let _ = drain_commands(conn);

    feed(conn, now, &error_reply(request_id.0));
    let outcome_is_error = matches!(
        conn.take_outcome(PendingOpKey::Request(request_id)),
        Some(OpOutcome::Error { .. })
    );
    let delta_queued = delta_queued(conn, now, watcher_id);
    conn.close_topic_watcher(watcher_id);
    conn.cancel_request(request_id);

    Refused {
        outcome_is_error,
        delta_queued,
        frames_after_close: drain_commands(conn).len(),
    }
}

fn closed_connection(conn: &mut Connection, now: Instant) -> bool {
    let watcher_id = acknowledged_watcher(conn, now);

    conn.close();
    conn.fail_all_pending("differential shutdown");
    conn.close_topic_watcher(watcher_id);
    // Shutting the connection down queues its own `Closed` event, which is not a delta.
    while conn.poll_event().is_some() {}
    delta_queued(conn, now, watcher_id)
}

/// Runs `scenario` on a fresh handshaken connection of the given engine's `ConnectionShared`.
macro_rules! on_fresh_connection {
    ($shared:ty, $scenario:ident) => {{
        let shared = <$shared>::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        let now = Instant::now();
        handshake(&mut conn, now);
        $scenario(&mut conn, now)
    }};
}

macro_rules! projection {
    ($shared:ty) => {
        Projection {
            acknowledged: on_fresh_connection!($shared, acknowledged),
            reset: on_fresh_connection!($shared, reset),
            reattached: on_fresh_connection!($shared, reattached),
            refused: on_fresh_connection!($shared, refused),
            closed_connection_delta_queued: on_fresh_connection!($shared, closed_connection),
        }
    };
}

#[test]
fn engines_agree_on_closing_an_acknowledged_watcher_in_every_state() {
    let tokio = projection!(magnetar_runtime_tokio::ConnectionShared);
    let moonpool = projection!(magnetar_runtime_moonpool::ConnectionShared);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on how a topic-list watcher is closed"
    );

    assert_eq!(
        tokio.acknowledged,
        Acknowledged {
            close_frames: 1,
            second_close_frames: 0,
            unknown_close_frames: 0,
            delta_queued: false,
            replies_left_a_trace: false,
        },
        "an acknowledged watcher is closed at the broker once and then forgotten"
    );
    assert_eq!(
        tokio.reset,
        Reset {
            close_frames: 0,
            registrations_after_rebuild: 0,
        },
        "a watcher a reset took down is only forgotten, and not registered again"
    );
    assert_eq!(
        tokio.reattached,
        Reattached {
            close_frames: 1,
            late_reply_left_a_trace: false,
            delta_queued_after_late_reply: false,
        },
        "a watcher whose re-registration is on the wire is closed, and its late reply ignored"
    );
    assert_eq!(
        tokio.refused,
        Refused {
            outcome_is_error: true,
            delta_queued: false,
            frames_after_close: 0,
        },
        "a refused watch is forgotten without a close frame"
    );
    assert!(
        !tokio.closed_connection_delta_queued,
        "a watcher closed on a shut-down connection is forgotten"
    );
}
