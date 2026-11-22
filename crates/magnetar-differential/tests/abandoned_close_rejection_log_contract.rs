// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O contract for the record a producer close nobody awaits leaves when the broker
//! rejects it, executed in the simulation coverage domain.
//!
//! A close can lose its waiter in two ways: the producer's last clone dropped, so the drop guard
//! fired `Connection::close_producer_forget`, or the future awaiting `Producer::close()` was
//! dropped, so its `RequestFut` called `Connection::abandon_request_waiter` (ADR-0152). Either
//! way no future is left to tell, so a rejection is recorded nowhere but the one `warn!` the shared
//! `Connection` writes (ADR-0054: structured fields, the broker's message bounded) and leaves no
//! `OpOutcome` nobody could take. This contract drives that `Connection` through its public API
//! for both ways and reads the record back.
//!
//! The capturing subscriber is global, so this file holds a single test and shares its binary
//! with no other test.

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, PendingOpKey, ProducerHandle, RequestId,
    encode_command, pb,
};
use parking_lot::Mutex;

const REJECTION: &str = "broker rejected a producer close nobody awaits";
const BOTH_WAYS: &str =
    "producer dropped without explicit close, or the future awaiting close() was dropped";

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

/// A producer whose open the broker acknowledged.
fn open_producer(conn: &mut Connection, topic: &str) -> ProducerHandle {
    let open = RequestId(conn.peek_next_request_id_for_test());
    let producer = conn.create_producer(CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: open.0,
                producer_name: "log-contract-producer".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    );
    while conn.poll_event().is_some() {}
    producer
}

/// The broker refuses the close `request_id` with `message`.
fn reject(conn: &mut Connection, request_id: RequestId, message: String) {
    assert!(
        conn.has_pending_request_for_test(request_id),
        "precondition: the close {request_id:?} must still be pending when the broker rejects it"
    );
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message,
            }),
            ..Default::default()
        },
    );
}

/// The records of `logged` that carry the rejection.
fn rejection_records(logged: &str) -> Vec<&str> {
    logged
        .lines()
        .filter(|line| line.contains(REJECTION))
        .collect()
}

/// A rejected close nobody awaits is logged once at `warn!`, with the request id, the broker's
/// error code, its message bounded and the text that names both ways to lose the waiter, and
/// leaves no outcome behind.
#[test]
fn a_rejected_close_nobody_awaits_is_logged_once_and_records_no_outcome() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::WARN)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    handshake(&mut conn);
    let broker_message = "x".repeat(4096);

    // The future awaiting `close()` is dropped before the broker answers.
    let producer = open_producer(
        &mut conn,
        "persistent://public/default/dropped-close-future",
    );
    let close = conn.close_producer(producer);
    let _ = conn.poll_transmit();
    conn.abandon_request_waiter(close);
    assert!(
        rejection_records(&sink.contents()).is_empty(),
        "nothing is logged before the broker answers"
    );
    reject(&mut conn, close, broker_message.clone());
    assert!(
        conn.take_outcome(PendingOpKey::Request(close)).is_none(),
        "a rejected close nobody awaits must record no outcome"
    );

    // The producer's last clone is dropped: the drop guard's fire-and-forget close.
    let dropped = open_producer(&mut conn, "persistent://public/default/dropped-producer");
    let forgotten = conn.close_producer_forget(dropped);
    let _ = conn.poll_transmit();
    reject(&mut conn, forgotten, broker_message.clone());
    assert!(
        conn.take_outcome(PendingOpKey::Request(forgotten))
            .is_none(),
        "a rejected fire-and-forget close must record no outcome"
    );

    let logged = sink.contents();
    let records = rejection_records(&logged);
    assert_eq!(
        records.len(),
        2,
        "one record per rejected close, none for the acknowledged opens: {logged}"
    );
    for (record, request_id, handle) in [
        (records[0], close, producer),
        (records[1], forgotten, dropped),
    ] {
        assert!(
            record.contains("WARN"),
            "the rejection is a warning: {record}"
        );
        assert!(
            record.contains(&format!("request_id=RequestId({})", request_id.0)),
            "the record names the rejected request: {record}"
        );
        assert!(
            record.contains(&format!("handle=ProducerHandle({})", handle.0)),
            "the record names the producer: {record}"
        );
        assert!(
            record.contains(&format!("code={}", pb::ServerError::ServiceNotReady as i32)),
            "the record carries the broker's error code: {record}"
        );
        assert!(
            record.contains(BOTH_WAYS),
            "the record says a dropped close() future is covered as well as a dropped producer: {record}"
        );
        // The fmt subscriber prints a field named `message` without its name.
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
    }
}
