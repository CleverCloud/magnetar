// SPDX-License-Identifier: Apache-2.0

//! Twin of `crates/magnetar/tests/reader_seek_cursor.rs`, run from `magnetar-differential` so the
//! facade code it drives is executed inside the Moonpool coverage domain (ADR-0120, ADR-0121): that
//! domain runs only the `magnetar-runtime-moonpool` and `magnetar-differential` test binaries,
//! so the facade crate's own test targets never count towards its patch coverage. The
//! scenarios and assertions are the facade file's, unchanged.
//!
//! `Reader::has_message_available` must answer for the position the reader is at, and a seek moves
//! that position.
//!
//! The defect these tests pin: `Reader` answered `has_message_available` by comparing the broker's
//! last message id with `last_received`, the id of the last message `read_next` handed out. The
//! four seek wrappers (`seek_to_earliest`, `seek_to_latest`, `seek_to_message`,
//! `seek_to_timestamp`) forwarded to the consumer and wrote nothing to that cursor, so after a seek
//! the reader kept answering for the position it left:
//!
//! * a topic read to its end and rewound with `seek_to_earliest` still answered `false`, so the
//!   usual `while reader.has_message_available().await? { reader.read_next().await? }` loop exited
//!   at once and the replay was silently skipped;
//! * a topic read part-way and fast-forwarded with `seek_to_latest` still answered `true`, so the
//!   same loop called `read_next` and waited for a message that may never come.
//!
//! The scenarios run against a real `PulsarClient` and the real tokio runtime, speaking to a
//! scripted loopback broker (the same fake-broker pattern as `wrapper_pull_receive_fairness.rs`),
//! so no live Pulsar is needed. The broker holds a fixed three-entry log, answers
//! `CommandGetLastMessageId` with its last entry, and honours `CommandSeek` by moving its delivery
//! position, then delivers from that position when the reader's consumer re-subscribes and flows,
//! exactly as the runtime's seek path expects.
//!
//! The assertions state the correct answer and nothing about how a fix is shaped (reset the
//! cursor, ask the broker at seek time, track a position marker). Each proving test is paired with
//! controls through the same broker that pass with and without the fix: the pre-seek answer is
//! right, the broker really replays (or has nothing to replay) after the seek, and a fresh reader
//! answers `true`, so a failure is attributable to the cursor and not to the harness.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::InitialPosition;
use magnetar::{PulsarClient, Reader};
use magnetar_proto::{FrameError, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const TOPIC: &str = "persistent://public/default/reader-seek-cursor";
const LEDGER: u64 = 7;
/// Entries the broker holds, `LEDGER:0 ..= LEDGER:LOG_LEN-1`.
const LOG_LEN: u64 = 3;
const DEADLINE: Duration = Duration::from_secs(3);
/// How long a read that must come back empty waits before it is declared empty.
const QUIET: Duration = Duration::from_millis(300);

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` of the reader's subscription, set by `CommandSubscribe`.
    subscribed: Option<(u64, u64)>,
    /// Entry the broker delivers next; a seek moves it, delivery advances it to `LOG_LEN`.
    next: u64,
    /// Seeks the broker has honoured, as the entry each one moved the position to.
    seeks: Vec<u64>,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
}

struct FakeBroker {
    addr: String,
    state: Arc<Mutex<State>>,
}

impl FakeBroker {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr").to_string();
        let state = Arc::new(Mutex::new(State::default()));
        let accept_state = state.clone();
        tokio::spawn(async move {
            let mut next_conn = 0u64;
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let conn = next_conn;
                next_conn += 1;
                tokio::spawn(serve(stream, conn, accept_state.clone()));
            }
        });
        Self { addr, state }
    }

    async fn client(&self) -> PulsarClient {
        tokio::time::timeout(
            DEADLINE,
            PulsarClient::builder()
                .service_url(format!("pulsar://{}", self.addr))
                .connections_per_broker(1)
                .build(),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok")
    }

    /// The entry the broker delivers next.
    fn position(&self) -> u64 {
        self.state.lock().next
    }

    /// Entries each honoured seek moved the broker's position to.
    fn seeks(&self) -> Vec<u64> {
        self.state.lock().seeks.clone()
    }
}

fn base(ty: pb::base_command::Type) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: ty as i32,
        ..Default::default()
    }
}

fn command_frame(cmd: &pb::BaseCommand) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, cmd).expect("encode command");
    buf
}

fn entry_id(entry: u64) -> pb::MessageIdData {
    pb::MessageIdData {
        ledger_id: LEDGER,
        entry_id: entry,
        partition: None,
        batch_index: None,
        ack_set: vec![],
        batch_size: None,
        first_chunk_message_id: None,
    }
}

fn message_frame(consumer_id: u64, entry: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: entry_id(entry),
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..base(pb::base_command::Type::Message)
    };
    let metadata = pb::MessageMetadata {
        producer_name: "reader-seek-broker".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, b"plain").expect("encode message frame");
    frame
}

fn success(request_id: u64) -> BytesMut {
    command_frame(&pb::BaseCommand {
        success: Some(pb::CommandSuccess {
            request_id,
            ..Default::default()
        }),
        ..base(pb::base_command::Type::Success)
    })
}

/// Where a `CommandSeek` target moves the broker's delivery position.
fn seek_position(target: &pb::MessageIdData) -> u64 {
    if target.ledger_id == u64::MAX {
        // `MessageId::EARLIEST`.
        0
    } else if target.ledger_id == i64::MAX as u64 {
        // `MessageId::LATEST`.
        LOG_LEN
    } else {
        target.entry_id
    }
}

/// Handle one decoded client command, queueing replies in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    let mut guard = state.lock();
    let ty = cmd.r#type;
    let reply: Option<BytesMut> = if ty == pb::base_command::Type::Connect as i32 {
        Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "reader-seek-fake".to_owned(),
                protocol_version: Some(21),
                ..Default::default()
            }),
            ..base(pb::base_command::Type::Connected)
        }))
    } else if ty == pb::base_command::Type::Lookup as i32 {
        // Single-broker contract: every lookup resolves to "use the current connection".
        cmd.lookup_topic.as_ref().map(|lookup| {
            command_frame(&pb::BaseCommand {
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
                    request_id: lookup.request_id,
                    authoritative: Some(true),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::LookupResponse)
            })
        })
    } else if ty == pb::base_command::Type::Subscribe as i32 {
        // The first subscribe and the re-subscribe the runtime issues after a seek both land
        // here; the delivery position is only moved by a seek and by delivery itself.
        cmd.subscribe.as_ref().map(|subscribe| {
            guard.subscribed = Some((conn, subscribe.consumer_id));
            success(subscribe.request_id)
        })
    } else if ty == pb::base_command::Type::Flow as i32 {
        if let Some(flow) = cmd.flow.as_ref()
            && guard.subscribed == Some((conn, flow.consumer_id))
        {
            let from = guard.next;
            let out = guard.outbox.entry(conn).or_default();
            for entry in from..LOG_LEN {
                out.extend_from_slice(&message_frame(flow.consumer_id, entry));
            }
            guard.next = guard.next.max(LOG_LEN);
        }
        None
    } else if ty == pb::base_command::Type::GetLastMessageId as i32 {
        cmd.get_last_message_id.as_ref().map(|get| {
            command_frame(&pb::BaseCommand {
                get_last_message_id_response: Some(pb::CommandGetLastMessageIdResponse {
                    last_message_id: entry_id(LOG_LEN - 1),
                    request_id: get.request_id,
                    consumer_mark_delete_position: None,
                }),
                ..base(pb::base_command::Type::GetLastMessageIdResponse)
            })
        })
    } else if ty == pb::base_command::Type::Seek as i32 {
        cmd.seek.as_ref().map(|seek| {
            let to = seek.message_id.as_ref().map_or(0, seek_position);
            guard.next = to;
            guard.seeks.push(to);
            // The broker quiesces the consumer on a seek: it must subscribe and flow again.
            guard.subscribed = None;
            success(seek.request_id)
        })
    } else if ty == pb::base_command::Type::CloseConsumer as i32 {
        cmd.close_consumer.as_ref().map(|close| {
            guard.subscribed = None;
            success(close.request_id)
        })
    } else if ty == pb::base_command::Type::Ping as i32 {
        Some(command_frame(&pb::BaseCommand {
            pong: Some(pb::CommandPong {}),
            ..base(pb::base_command::Type::Pong)
        }))
    } else {
        None
    };
    if let Some(reply) = reply {
        guard
            .outbox
            .entry(conn)
            .or_default()
            .extend_from_slice(&reply);
    }
}

async fn serve(mut stream: TcpStream, conn: u64, state: Arc<Mutex<State>>) {
    let notify = Notify::new();
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut probe = [0u8; 8 * 1024];
    loop {
        loop {
            let mut framed = Bytes::copy_from_slice(&read_buf);
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let _ = read_buf.split_to(before - framed.len());
            handle_command(&frame.command, conn, &state);
        }
        let out = state.lock().outbox.remove(&conn);
        if let Some(out) = out
            && !out.is_empty()
            && stream.write_all(&out).await.is_err()
        {
            return;
        }
        tokio::select! {
            read = stream.read(&mut probe) => match read {
                Ok(0) | Err(_) => return,
                Ok(n) => read_buf.extend_from_slice(&probe[..n]),
            },
            // Nothing else wakes the loop: every reply is a response to a client command.
            () = notify.notified() => {}
        }
    }
}

async fn reader(client: &PulsarClient) -> Reader {
    client
        .reader(TOPIC)
        .start_position(InitialPosition::Earliest)
        .create()
        .await
        .expect("create reader")
}

/// Read `count` messages and return their entry ids. A read that does not come back within
/// `DEADLINE` is a harness failure (everything the scenarios wait on is already queued).
async fn read_entries(reader: &Reader, count: u64) -> Vec<u64> {
    let mut entries = Vec::new();
    for _ in 0..count {
        let message = reader
            .read_next_with_timeout(DEADLINE)
            .await
            .expect("read_next ok")
            .unwrap_or_else(|| panic!("harness: read_next timed out, read so far: {entries:?}"));
        assert_eq!(message.message_id.ledger_id, LEDGER, "harness: ledger id");
        entries.push(message.message_id.entry_id);
    }
    entries
}

/// Control (passes with and without the fix): a reader that has read nothing asks the broker and
/// the broker has entries, so something is available. This is the harness's answer for the
/// no-cursor branch.
#[tokio::test(flavor = "current_thread")]
async fn fresh_reader_of_a_non_empty_topic_has_a_message_available() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert!(
        reader
            .has_message_available()
            .await
            .expect("has_message_available"),
        "a fresh reader of a topic holding {LOG_LEN} entries must report one available"
    );
}

/// Control (passes with and without the fix): once the reader has read the log to its end, nothing
/// is available. This is the pre-seek answer the rewind scenario starts from.
#[tokio::test(flavor = "current_thread")]
async fn reader_at_the_end_of_the_log_has_no_message_available() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert_eq!(read_entries(&reader, LOG_LEN).await, [0, 1, 2]);
    assert!(
        !reader
            .has_message_available()
            .await
            .expect("has_message_available"),
        "a reader that has read all {LOG_LEN} entries must report none available"
    );
}

/// Control (passes with and without the fix): the broker really replays the whole log after
/// `seek_to_earliest`, so "a message is available" is the correct answer in the rewind scenario.
#[tokio::test(flavor = "current_thread")]
async fn seek_to_earliest_really_replays_the_log() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert_eq!(read_entries(&reader, LOG_LEN).await, [0, 1, 2]);
    reader.seek_to_earliest().await.expect("seek_to_earliest");
    assert_eq!(
        broker.seeks(),
        [0],
        "harness: the seek must reach the broker"
    );
    assert_eq!(
        read_entries(&reader, LOG_LEN).await,
        [0, 1, 2],
        "the log must be replayed from the start after seek_to_earliest"
    );
}

/// Control (passes with and without the fix): after `seek_to_latest` the broker has nothing left to
/// deliver, so "no message available" is the correct answer in the fast-forward scenario.
#[tokio::test(flavor = "current_thread")]
async fn seek_to_latest_leaves_nothing_to_read() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert_eq!(read_entries(&reader, 1).await, [0]);
    reader.seek_to_latest().await.expect("seek_to_latest");
    assert_eq!(
        broker.seeks(),
        [LOG_LEN],
        "harness: the seek must reach the broker"
    );
    assert!(
        reader
            .read_next_with_timeout(QUIET)
            .await
            .expect("read_next ok")
            .is_none(),
        "after seek_to_latest the reader must have nothing to read"
    );
}

/// Rewind: the reader has read all three entries (no message available, as the control above
/// shows) and rewinds with `seek_to_earliest`. The broker now holds three entries the reader has
/// not read, so a message is available. Fails if the cursor still names the last entry read: the
/// broker's last id equals it, the reader answers `false`, and the `while has_message_available`
/// replay loop exits without reading anything.
#[tokio::test(flavor = "current_thread")]
async fn has_message_available_after_seek_to_earliest_sees_the_replay() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert_eq!(read_entries(&reader, LOG_LEN).await, [0, 1, 2]);
    assert!(
        !reader
            .has_message_available()
            .await
            .expect("has_message_available"),
        "harness: a fully read log has nothing available before the seek"
    );

    reader.seek_to_earliest().await.expect("seek_to_earliest");
    // The broker honoured exactly one seek, to entry 0. Its live position is not asserted here:
    // the re-subscribe flow may already have redelivered the log, so it is timing-dependent.
    assert_eq!(
        broker.seeks(),
        [0],
        "harness: the seek must reach the broker"
    );

    let available = reader
        .has_message_available()
        .await
        .expect("has_message_available");
    let mut replayed = 0u64;
    while reader
        .has_message_available()
        .await
        .expect("has_message_available")
        && replayed < LOG_LEN
    {
        read_entries(&reader, 1).await;
        replayed += 1;
    }
    assert!(
        available,
        "after seek_to_earliest the broker replays {LOG_LEN} entries, so a message must be \
         available; the reader answered for the pre-seek position, and a \
         `while has_message_available {{ read_next }}` loop read {replayed} of {LOG_LEN}"
    );
}

/// Fast-forward: the reader has read one of three entries (a message is available, as the
/// assertion below shows) and jumps to the head with `seek_to_latest`. Nothing is past the head,
/// so no message is available. Fails if the cursor still names entry 0: the broker's last id is
/// entry 2, the reader answers `true`, and the replay loop calls `read_next` and waits for a
/// message that never comes.
#[tokio::test(flavor = "current_thread")]
async fn has_message_available_after_seek_to_latest_sees_nothing_left() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let reader = reader(&client).await;
    assert_eq!(read_entries(&reader, 1).await, [0]);
    assert!(
        reader
            .has_message_available()
            .await
            .expect("has_message_available"),
        "harness: two entries are unread before the seek"
    );

    reader.seek_to_latest().await.expect("seek_to_latest");
    assert_eq!(
        broker.seeks(),
        [LOG_LEN],
        "harness: the seek must reach the broker"
    );
    assert_eq!(
        broker.position(),
        LOG_LEN,
        "harness: the broker is at the head"
    );

    assert!(
        !reader
            .has_message_available()
            .await
            .expect("has_message_available"),
        "after seek_to_latest nothing is past the head, so no message must be available; the \
         reader answered for the pre-seek position (entry 0 read of {LOG_LEN})"
    );
}
