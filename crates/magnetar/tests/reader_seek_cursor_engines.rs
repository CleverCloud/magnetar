// SPDX-License-Identifier: Apache-2.0

//! The rest of `Reader`'s seek surface, on both engines: `has_message_available` answers for the
//! position a seek leaves the reader at.
//!
//! `reader_seek_cursor.rs` proves `seek_to_earliest` and `seek_to_latest` on the tokio reader.
//! This file runs the same scenarios against the moonpool reader (which had no seek wrapper at
//! all, so its callers seeked through `Reader::consumer()` and could never move the cursor), and
//! adds the targets that file leaves out:
//!
//! * `seek_to_message` with a real id replays from the sought message, so a message is available;
//! * `seek_to_message(MessageId::LATEST)` is a seek to the head, so none is;
//! * `seek_to_timestamp` names no message id, so the answer is whether the topic holds any;
//! * a reader of an empty topic has nothing available, and a seek to the head of one must not
//!   strand the reader on the empty-topic head (entry id `-1`), which compares above every real id
//!   and would answer `false` for ever. Nothing in this tree says which ledger id a broker reports
//!   beside that entry id, so both the current ledger and the `EARLIEST` ledger are run;
//! * a head lookup that fails after a successful seek surfaces as an error and leaves no stale
//!   cursor behind.
//!
//! Every scenario is written once, against the small [`SeekingReader`] trait, and runs on each
//! engine: `tokio_engine::*` always, `moonpool_engine::*` when the `moonpool` feature is on. Both
//! speak to the scripted loopback broker of `reader_seek_cursor.rs`, here with a log whose length
//! the test can change and a head lookup it can make fail. No live Pulsar is needed.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::InitialPosition;
use magnetar::{PulsarClient, PulsarError, Reader};
use magnetar_proto::{FrameError, MessageId, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "persistent://public/default/reader-seek-cursor-engines";
const LEDGER: u64 = 7;
/// Entries the broker holds unless a scenario changes it, `LEDGER:0 ..= LEDGER:LOG_LEN-1`.
const LOG_LEN: u64 = 3;
const DEADLINE: Duration = Duration::from_secs(3);
/// How long a read that must come back empty waits before it is declared empty.
const QUIET: Duration = Duration::from_millis(300);

struct State {
    /// `(connection, consumer id)` of the reader's subscription, set by `CommandSubscribe`.
    subscribed: Option<(u64, u64)>,
    /// Entries the log holds.
    len: u64,
    /// Entry the broker delivers next; a seek moves it, delivery advances it to `len`.
    next: u64,
    /// Seeks the broker has honoured, as the entry each one moved the position to.
    seeks: Vec<u64>,
    /// Answer `CommandGetLastMessageId` with an error instead of the head.
    head_fails: bool,
    /// Ledger id the head of an empty log is reported on.
    empty_head_ledger: u64,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
}

struct FakeBroker {
    addr: String,
    state: Arc<Mutex<State>>,
}

impl FakeBroker {
    async fn start(len: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr").to_string();
        let state = Arc::new(Mutex::new(State {
            subscribed: None,
            len,
            next: 0,
            seeks: Vec::new(),
            head_fails: false,
            empty_head_ledger: LEDGER,
            outbox: HashMap::new(),
        }));
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

    /// Entries each honoured seek moved the broker's position to.
    fn seeks(&self) -> Vec<u64> {
        self.state.lock().seeks.clone()
    }

    /// Append one entry to the log.
    fn publish(&self) {
        self.state.lock().len += 1;
    }

    /// Report the head of an empty log on `ledger`.
    fn set_empty_head_ledger(&self, ledger: u64) {
        self.state.lock().empty_head_ledger = ledger;
    }

    /// Make the broker answer `CommandGetLastMessageId` with an error, or stop doing so.
    fn set_head_failing(&self, failing: bool) {
        self.state.lock().head_fails = failing;
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

/// The head of an empty log: entry id `-1` (`u64::MAX` as the unsigned wire field) on `ledger`, no
/// batch index and no batch size. Whatever the ledger, read off the wire it is not equal to
/// `MessageId::EARLIEST` as a whole value.
fn empty_head(ledger: u64) -> pb::MessageIdData {
    pb::MessageIdData {
        ledger_id: ledger,
        entry_id: u64::MAX,
        partition: Some(-1),
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

/// Where a `CommandSeek` target moves the broker's delivery position. A seek by timestamp names no
/// message id, and the broker resolves it to the start of the log.
fn seek_position(target: Option<&pb::MessageIdData>, len: u64) -> u64 {
    match target {
        None => 0,
        // `MessageId::EARLIEST`.
        Some(id) if id.ledger_id == u64::MAX => 0,
        // `MessageId::LATEST`.
        Some(id) if id.ledger_id == i64::MAX as u64 => len,
        Some(id) => id.entry_id,
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
            let (from, len) = (guard.next, guard.len);
            let out = guard.outbox.entry(conn).or_default();
            for entry in from..len {
                out.extend_from_slice(&message_frame(flow.consumer_id, entry));
            }
            guard.next = guard.next.max(len);
        }
        None
    } else if ty == pb::base_command::Type::GetLastMessageId as i32 {
        let (len, fails, empty_ledger) = (guard.len, guard.head_fails, guard.empty_head_ledger);
        cmd.get_last_message_id.as_ref().map(|get| {
            if fails {
                return command_frame(&pb::BaseCommand {
                    error: Some(pb::CommandError {
                        request_id: get.request_id,
                        error: pb::ServerError::UnknownError as i32,
                        message: "head lookup refused by the fake broker".to_owned(),
                    }),
                    ..base(pb::base_command::Type::Error)
                });
            }
            let last_message_id = if len == 0 {
                empty_head(empty_ledger)
            } else {
                entry_id(len - 1)
            };
            command_frame(&pb::BaseCommand {
                get_last_message_id_response: Some(pb::CommandGetLastMessageIdResponse {
                    last_message_id,
                    request_id: get.request_id,
                    consumer_mark_delete_position: None,
                }),
                ..base(pb::base_command::Type::GetLastMessageIdResponse)
            })
        })
    } else if ty == pb::base_command::Type::Seek as i32 {
        let len = guard.len;
        cmd.seek.as_ref().map(|seek| {
            let to = seek_position(seek.message_id.as_ref(), len);
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
        // Every reply answers a client command, so the loop only needs to wake on a read.
        match stream.read(&mut probe).await {
            Ok(0) | Err(_) => return,
            Ok(n) => read_buf.extend_from_slice(&probe[..n]),
        }
    }
}

/// The seek surface of a `Reader`, which sits in two inherent `impl` blocks (one per engine).
trait SeekingReader: Sized {
    /// Open a reader on [`TOPIC`] through `broker`, from the earliest message.
    async fn open(broker: &FakeBroker) -> Self;
    /// Entry id of the next message, or `None` when none arrives within `within`.
    async fn next_entry(&self, within: Duration) -> Option<u64>;
    async fn has_message_available(&self) -> Result<bool, PulsarError>;
    async fn seek_to_earliest(&self) -> Result<(), PulsarError>;
    async fn seek_to_latest(&self) -> Result<(), PulsarError>;
    async fn seek_to_message(&self, message_id: MessageId) -> Result<(), PulsarError>;
    async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError>;

    /// Read `count` messages and return their entry ids. A read that does not come back within
    /// `DEADLINE` is a harness failure (everything the scenarios wait on is already queued).
    async fn read_entries(&self, count: u64) -> Vec<u64> {
        let mut entries = Vec::new();
        for _ in 0..count {
            let entry = self
                .next_entry(DEADLINE)
                .await
                .unwrap_or_else(|| panic!("harness: read timed out, read so far: {entries:?}"));
            entries.push(entry);
        }
        entries
    }
}

/// The reader and the client that dialled for it, which stays up as long as the reader does.
struct TokioReader {
    reader: Reader,
    _client: PulsarClient,
}

impl SeekingReader for TokioReader {
    async fn open(broker: &FakeBroker) -> Self {
        let client = tokio::time::timeout(
            DEADLINE,
            PulsarClient::builder()
                .service_url(format!("pulsar://{}", broker.addr))
                .connections_per_broker(1)
                .build(),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok");
        let reader = client
            .reader(TOPIC)
            .start_position(InitialPosition::Earliest)
            .create()
            .await
            .expect("create reader");
        Self {
            reader,
            _client: client,
        }
    }

    async fn next_entry(&self, within: Duration) -> Option<u64> {
        self.reader
            .read_next_with_timeout(within)
            .await
            .expect("read_next ok")
            .map(|message| {
                assert_eq!(message.message_id.ledger_id, LEDGER, "harness: ledger id");
                message.message_id.entry_id
            })
    }

    async fn has_message_available(&self) -> Result<bool, PulsarError> {
        self.reader.has_message_available().await
    }

    async fn seek_to_earliest(&self) -> Result<(), PulsarError> {
        self.reader.seek_to_earliest().await
    }

    async fn seek_to_latest(&self) -> Result<(), PulsarError> {
        self.reader.seek_to_latest().await
    }

    async fn seek_to_message(&self, message_id: MessageId) -> Result<(), PulsarError> {
        self.reader.seek_to_message(message_id).await
    }

    async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError> {
        self.reader.seek_to_timestamp(publish_time_ms).await
    }
}

/// The reader and the client that dialled for it, which stays up as long as the reader does.
#[cfg(feature = "moonpool")]
struct MoonpoolReader {
    reader: Reader<magnetar::runtime_moonpool::Consumer<moonpool_core::TokioProviders>>,
    _client: PulsarClient<magnetar::MoonpoolEngine<moonpool_core::TokioProviders>>,
}

#[cfg(feature = "moonpool")]
impl SeekingReader for MoonpoolReader {
    async fn open(broker: &FakeBroker) -> Self {
        let runtime_client = magnetar::runtime_moonpool::Client::connect_plain_supervised(
            &magnetar::runtime_moonpool::MoonpoolEngine::new(moonpool_core::TokioProviders::new()),
            &broker.addr,
            magnetar_proto::ConnectionConfig {
                operation_timeout: DEADLINE,
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .expect("connect ok");
        let client = PulsarClient::from_runtime_client(runtime_client);
        let reader = client
            .reader(TOPIC)
            .start_position(InitialPosition::Earliest)
            .create()
            .await
            .expect("create reader");
        Self {
            reader,
            _client: client,
        }
    }

    async fn next_entry(&self, within: Duration) -> Option<u64> {
        tokio::time::timeout(within, self.reader.read_next())
            .await
            .ok()
            .map(|message| {
                let message = message.expect("read_next ok");
                assert_eq!(message.id.ledger_id, LEDGER, "harness: ledger id");
                message.id.entry_id
            })
    }

    async fn has_message_available(&self) -> Result<bool, PulsarError> {
        self.reader.has_message_available().await
    }

    async fn seek_to_earliest(&self) -> Result<(), PulsarError> {
        self.reader.seek_to_earliest().await
    }

    async fn seek_to_latest(&self) -> Result<(), PulsarError> {
        self.reader.seek_to_latest().await
    }

    async fn seek_to_message(&self, message_id: MessageId) -> Result<(), PulsarError> {
        self.reader.seek_to_message(message_id).await
    }

    async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError> {
        self.reader.seek_to_timestamp(publish_time_ms).await
    }
}

/// The scenarios, engine-agnostic. Each takes the broker it runs against and a fresh reader on it.
mod scenario {
    use super::{FakeBroker, LEDGER, LOG_LEN, MessageId, PulsarError, QUIET, SeekingReader};

    /// A topic read to its end and rewound with `seek_to_earliest`: the replay is available.
    pub(super) async fn rewind_after_a_full_read<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2]);
        assert!(
            !reader.has_message_available().await.expect("available"),
            "harness: a fully read log has nothing available before the seek"
        );
        reader.seek_to_earliest().await.expect("seek_to_earliest");
        assert_eq!(
            broker.seeks(),
            [0],
            "harness: the seek must reach the broker"
        );
        assert!(
            reader.has_message_available().await.expect("available"),
            "after seek_to_earliest the broker replays {LOG_LEN} entries"
        );
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2], "the replay");
        assert!(
            !reader.has_message_available().await.expect("available"),
            "the replay was read to its end"
        );
    }

    /// A topic read part-way and fast-forwarded with `seek_to_latest`: nothing is left.
    pub(super) async fn fast_forward_after_a_partial_read<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(1).await, [0]);
        assert!(
            reader.has_message_available().await.expect("available"),
            "harness: two entries are unread before the seek"
        );
        reader.seek_to_latest().await.expect("seek_to_latest");
        assert_eq!(
            broker.seeks(),
            [LOG_LEN],
            "harness: the seek must reach the broker"
        );
        assert!(
            !reader.has_message_available().await.expect("available"),
            "after seek_to_latest nothing is past the head"
        );
        assert_eq!(
            reader.next_entry(QUIET).await,
            None,
            "harness: nothing to read"
        );
        // A message published after the seek is past the head the reader jumped to.
        broker.publish();
        assert!(
            reader.has_message_available().await.expect("available"),
            "a message published after the seek must be available"
        );
    }

    /// A seek to a real message id replays from that message, so a message is available.
    pub(super) async fn seek_to_a_message_id_replays_from_it<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2]);
        assert!(
            !reader.has_message_available().await.expect("available"),
            "harness: a fully read log has nothing available before the seek"
        );
        let target = MessageId {
            ledger_id: LEDGER,
            entry_id: 1,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        };
        reader
            .seek_to_message(target)
            .await
            .expect("seek_to_message");
        assert_eq!(
            broker.seeks(),
            [1],
            "harness: the seek must reach the broker"
        );
        assert!(
            reader.has_message_available().await.expect("available"),
            "after a seek to entry 1 the broker replays entries 1 and 2"
        );
        assert_eq!(reader.read_entries(2).await, [1, 2], "the replay");
        assert!(
            !reader.has_message_available().await.expect("available"),
            "the replay was read to its end"
        );
    }

    /// `seek_to_message(MessageId::LATEST)` is the seek `seek_to_latest` is.
    pub(super) async fn seek_to_the_latest_sentinel_is_a_seek_to_the_head<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(1).await, [0]);
        reader
            .seek_to_message(MessageId::LATEST)
            .await
            .expect("seek_to_message");
        assert_eq!(
            broker.seeks(),
            [LOG_LEN],
            "harness: the seek must reach the broker"
        );
        assert!(
            !reader.has_message_available().await.expect("available"),
            "after a seek to MessageId::LATEST nothing is past the head"
        );
    }

    /// A seek by timestamp names no message id: the topic holds messages, so one is available.
    pub(super) async fn seek_to_a_timestamp_has_the_replay_available<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2]);
        assert!(
            !reader.has_message_available().await.expect("available"),
            "harness: a fully read log has nothing available before the seek"
        );
        reader
            .seek_to_timestamp(0)
            .await
            .expect("seek_to_timestamp");
        assert_eq!(
            broker.seeks(),
            [0],
            "harness: the seek must reach the broker"
        );
        assert!(
            reader.has_message_available().await.expect("available"),
            "after a seek to a timestamp the topic still holds messages"
        );
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2], "the replay");
    }

    /// A reader of an empty topic has nothing available.
    pub(super) async fn a_reader_of_an_empty_topic_has_nothing_available<R: SeekingReader>(
        _broker: &FakeBroker,
        reader: &R,
    ) {
        assert!(
            !reader.has_message_available().await.expect("available"),
            "an empty topic has no message available"
        );
    }

    /// A seek to the head of an empty topic must not leave the reader on the empty-topic head,
    /// which compares above every real id: nothing is available until a message is published, and
    /// then the first one has to be.
    async fn seek_to_the_head_of_an_empty_topic<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
        head_ledger: u64,
    ) {
        broker.set_empty_head_ledger(head_ledger);
        reader.seek_to_latest().await.expect("seek_to_latest");
        assert_eq!(
            broker.seeks(),
            [0],
            "harness: the seek must reach the broker"
        );
        assert!(
            !reader.has_message_available().await.expect("available"),
            "nothing is available after a seek to the head of an empty topic"
        );
        broker.publish();
        assert!(
            reader.has_message_available().await.expect("available"),
            "the first message published after a seek to the head of an empty topic must be \
             available"
        );
    }

    /// The empty-topic head names the ledger the log is open on.
    pub(super) async fn seek_to_the_head_of_an_empty_topic_on_the_current_ledger<
        R: SeekingReader,
    >(
        broker: &FakeBroker,
        reader: &R,
    ) {
        seek_to_the_head_of_an_empty_topic(broker, reader, LEDGER).await;
    }

    /// The empty-topic head is the `EARLIEST` ledger and entry.
    pub(super) async fn seek_to_the_head_of_an_empty_topic_on_the_earliest_ledger<
        R: SeekingReader,
    >(
        broker: &FakeBroker,
        reader: &R,
    ) {
        seek_to_the_head_of_an_empty_topic(broker, reader, u64::MAX).await;
    }

    /// The seek succeeded but the head lookup behind `seek_to_latest` did not: the error says so,
    /// and the reader does not keep answering for the position it left.
    pub(super) async fn a_failed_head_lookup_is_an_error_and_clears_the_cursor<R: SeekingReader>(
        broker: &FakeBroker,
        reader: &R,
    ) {
        assert_eq!(reader.read_entries(LOG_LEN).await, [0, 1, 2]);
        assert!(
            !reader.has_message_available().await.expect("available"),
            "harness: a fully read log has nothing available before the seek"
        );
        broker.set_head_failing(true);
        let outcome = reader.seek_to_latest().await;
        broker.set_head_failing(false);
        assert!(
            matches!(outcome, Err(PulsarError::Other(_))),
            "a failed head lookup must surface as PulsarError::Other, got {outcome:?}"
        );
        assert_eq!(
            broker.seeks(),
            [LOG_LEN],
            "harness: the seek itself was honoured"
        );
        // The reader no longer knows where it is, so it answers whether the topic holds anything.
        // The stale cursor (entry 2, the last one read) would answer `false`.
        assert!(
            reader.has_message_available().await.expect("available"),
            "after a seek whose head lookup failed the stale cursor must be gone"
        );
    }
}

/// One test per scenario and engine. The tokio tests run on a current-thread runtime, as
/// `reader_seek_cursor.rs` does; the moonpool client dials through a supervised driver task and
/// is run on the multi-thread runtime its e2e tests use.
macro_rules! scenarios {
    ($( $name:ident $(, len = $len:expr)? ;)+) => {
        mod tokio_engine {
            use super::*;
            $(
                #[tokio::test(flavor = "current_thread")]
                async fn $name() {
                    let broker = FakeBroker::start(0 $(+ $len)?).await;
                    let reader = TokioReader::open(&broker).await;
                    scenario::$name(&broker, &reader).await;
                }
            )+
        }

        #[cfg(feature = "moonpool")]
        mod moonpool_engine {
            use super::*;
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $name() {
                    let broker = FakeBroker::start(0 $(+ $len)?).await;
                    let reader = MoonpoolReader::open(&broker).await;
                    scenario::$name(&broker, &reader).await;
                }
            )+
        }
    };
}

scenarios! {
    rewind_after_a_full_read, len = LOG_LEN;
    fast_forward_after_a_partial_read, len = LOG_LEN;
    seek_to_a_message_id_replays_from_it, len = LOG_LEN;
    seek_to_the_latest_sentinel_is_a_seek_to_the_head, len = LOG_LEN;
    seek_to_a_timestamp_has_the_replay_available, len = LOG_LEN;
    a_reader_of_an_empty_topic_has_nothing_available;
    seek_to_the_head_of_an_empty_topic_on_the_current_ledger;
    seek_to_the_head_of_an_empty_topic_on_the_earliest_ledger;
    a_failed_head_lookup_is_an_error_and_clears_the_cursor, len = LOG_LEN;
}
