// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O contract for the record a refused topic-list re-registration leaves,
//! executed in the simulation coverage domain.
//!
//! A supervised reconnect registers every acknowledged PIP-145 watcher again (ADR-0136). Nobody
//! awaits that registration, so a broker that refuses it has no future to tell: the shared
//! `Connection` logs one `warn!` (ADR-0054: structured fields, the broker's message bounded) and
//! leaves the watcher owed for the next rebuild. This contract drives that `Connection` through
//! its public API and reads the record back.
//!
//! The capturing subscriber is global, so this file holds a single test and shares its binary
//! with no other test.

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, decode_one, encode_command, pb};
use parking_lot::Mutex;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";
const REFUSAL: &str = "broker rejected re-registering a topic-list watcher after a reconnect";

/// Shared in-memory sink for the global fmt subscriber.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn feed(conn: &mut Connection, command: &pb::BaseCommand) {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode broker command");
    conn.handle_bytes(Instant::now(), &bytes)
        .expect("handle broker command");
}

fn handshake(conn: &mut Connection) {
    conn.begin_handshake().expect("begin_handshake");
    feed(
        conn,
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

/// A refused re-registration is logged once at `warn!`, with the request id, the broker's error
/// code and its message, and the watcher is registered again by the next rebuild.
#[test]
fn refused_reattach_is_logged_with_its_request_code_and_bounded_message() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::WARN)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    handshake(&mut conn);
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let registration = drain_registrations(&mut conn).remove(0);
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
            watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                request_id: request_id.0,
                watcher_id: registration.watcher_id,
                topic: vec!["persistent://public/default/orders-1".to_owned()],
                topics_hash: "hash-1".to_owned(),
            }),
            ..Default::default()
        },
    );
    while conn.poll_event().is_some() {}

    // The transport drops and the supervisor redials: reset, handshake, re-attach.
    conn.reset();
    handshake(&mut conn);
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    let reattach = drain_registrations(&mut conn);
    assert_eq!(
        reattach.len(),
        1,
        "the watcher is registered again: {reattach:?}"
    );
    let reattach_id = reattach[0].request_id;
    assert!(
        !sink.contents().contains(REFUSAL),
        "nothing is logged before the broker answers"
    );

    let broker_message = "x".repeat(4096);
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: reattach_id,
                error: pb::ServerError::ServiceNotReady as i32,
                message: broker_message.clone(),
            }),
            ..Default::default()
        },
    );

    let logged = sink.contents();
    let records: Vec<&str> = logged
        .lines()
        .filter(|line| line.contains(REFUSAL))
        .collect();
    assert_eq!(records.len(), 1, "one record per refusal: {logged}");
    let record = records[0];
    assert!(
        record.contains("WARN"),
        "the refusal is a warning: {record}"
    );
    assert!(
        record.contains(&format!("request_id=RequestId({reattach_id})")),
        "the record names the refused request: {record}"
    );
    assert!(
        record.contains(&format!("code={}", pb::ServerError::ServiceNotReady as i32)),
        "the record carries the broker's error code: {record}"
    );
    // The fmt subscriber prints a field named `message` without its name, after the others.
    let echoed = record
        .split(|c: char| c != 'x')
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(
        echoed > 0 && echoed < broker_message.len(),
        "the record carries the broker's message, bounded ({echoed} of {} bytes): {record}",
        broker_message.len()
    );

    // The watcher is owed again: the next rebuild registers it once more.
    let _ = conn.rebuild_consumers();
    let retried = drain_registrations(&mut conn);
    assert_eq!(
        retried.len(),
        1,
        "the refused watcher is registered again: {retried:?}"
    );
    assert_eq!(retried[0].watcher_id, registration.watcher_id);
}
