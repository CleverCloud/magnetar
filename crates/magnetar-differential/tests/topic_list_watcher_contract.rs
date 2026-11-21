// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O PIP-145 contracts executed by the simulation runner.
//!
//! A topic-list snapshot outcome names the watcher every later delta is keyed on
//! ([ADR-0132](../../../specs/adr/0132-address-pip-145-topic-list-deltas-to-their-watcher.md)).
//! That is the id the connection allocated for the request, whatever id the
//! broker echoes. A snapshot answering a request its caller has already
//! cancelled has no reader and no allocated id left to name, so it records no
//! outcome at all.

#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, decode_one, encode_command,
    pb,
};

const NAMESPACE: &str = "public/default";

fn feed(conn: &mut Connection, command: &pb::BaseCommand) {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode broker command");
    conn.handle_bytes(Instant::now(), &bytes)
        .expect("handle broker command");
}

fn handshaked_connection() -> Connection {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("begin handshake");
    let _ = conn.poll_transmit();
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "topic-list-watcher-contract".to_owned(),
                protocol_version: Some(21),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    conn
}

/// Open one watcher and return its request and the watcher id the connection allocated.
fn watch(conn: &mut Connection, pattern: &str) -> (RequestId, u64) {
    let request = conn.watch_topic_list(NAMESPACE, pattern);
    let mut sent = conn.poll_transmit();
    while !sent.is_empty() {
        let command = decode_one(&mut sent)
            .expect("decode outbound command")
            .command;
        if let Some(watch) = command.watch_topic_list {
            return (request, watch.watcher_id);
        }
    }
    panic!("no CommandWatchTopicList went out for {pattern}");
}

fn feed_snapshot(conn: &mut Connection, request: RequestId, echoed_watcher_id: u64) {
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
            watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                request_id: request.0,
                watcher_id: echoed_watcher_id,
                topic: vec!["persistent://public/default/watched".to_owned()],
                topics_hash: "watched".to_owned(),
            }),
            ..Default::default()
        },
    );
}

fn snapshot_watcher_id(conn: &mut Connection, request: RequestId) -> u64 {
    match conn.take_outcome(PendingOpKey::Request(request)) {
        Some(OpOutcome::TopicListSnapshot { watcher_id, .. }) => watcher_id,
        other => panic!("expected a topic-list snapshot for {request:?}, got {other:?}"),
    }
}

#[test]
fn a_snapshot_names_the_watcher_its_request_allocated() {
    let mut conn = handshaked_connection();
    let (request, allocated) = watch(&mut conn, "live-.*");
    feed_snapshot(&mut conn, request, allocated.wrapping_add(1_000));
    assert_eq!(
        snapshot_watcher_id(&mut conn, request),
        allocated,
        "a live request's snapshot names the id the connection allocated, not the echoed one"
    );
}

#[test]
fn a_snapshot_answering_a_cancelled_watch_records_no_outcome() {
    let mut conn = handshaked_connection();
    let (request, allocated) = watch(&mut conn, "cancelled-.*");
    conn.cancel_request(request);
    feed_snapshot(&mut conn, request, allocated.wrapping_add(1_000));
    assert!(
        conn.take_outcome(PendingOpKey::Request(request)).is_none(),
        "a cancelled request has no reader: the late reply must not leave its topic list behind"
    );
}
