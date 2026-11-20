// SPDX-License-Identifier: Apache-2.0

//! Twin of `crates/magnetar/tests/push_delivery_error_survival.rs`, run from
//! `magnetar-differential` so the facade code it drives is executed inside the Moonpool coverage
//! domain (ADR-0120, ADR-0121): that domain runs only the `magnetar-runtime-moonpool` and
//! `magnetar-differential` test binaries, so the facade crate's own test targets never count
//! towards its patch coverage. The scenarios and assertions are the facade file's, unchanged.
//!
//! Push delivery (`MessageListener`, wrapper `WrapperMessageListener`, `TableView` drain) must
//! outlive an error that concerns one message or one removed child.
//!
//! Every push loop in the façade reads *any* `receive()` error as "the consumer is closed" and
//! exits for good. The runtime `receive()` also resolves `Err` for a failure on a single message
//! while the consumer stays open (an encrypted message with no decryptor, an undecryptable or
//! corrupt-compressed payload under the default `CryptoFailureAction::Fail`), and a wrapper closes
//! the child it has just removed with `remove_topic`. In both cases the loop dies silently, the
//! healthy topics stay subscribed and undrained, and nothing recovers delivery.
//!
//! The pull contract is the reference: `e2e_crypto.rs` asserts that after the `Err` the consumer is
//! still usable. These tests assert the same for the push surfaces, against a real `PulsarClient`
//! and the real tokio runtime, speaking to a scripted loopback broker (the same fake-broker
//! pattern as `message_listener_builder_surface.rs`, extended to push `MESSAGE` frames).
//!
//! Each failing scenario is paired with a control that pushes only well-formed messages through the
//! same broker, so a red result is attributable to the error handling and not to the harness.

#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{
    IncomingMessage, MessageListener, PulsarClient, WrapperMessageListener,
    spawn_wrapper_message_listener,
};
use magnetar_proto::{FrameError, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const TOPIC_A: &str = "persistent://public/default/push-survival-a";
const TOPIC_B: &str = "persistent://public/default/push-survival-b";
const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/push-survival-.*";
const DEADLINE: Duration = Duration::from_secs(3);

/// One message the broker will push to the consumer subscribed to a topic.
#[derive(Clone)]
struct Scripted {
    /// Used as the entry id and the sequence id, so a test identifies a message by it.
    seq: u64,
    key: Option<&'static str>,
    payload: &'static [u8],
    /// Stamped with PIP-4 `encryption_keys`. The façade consumers under test configure no
    /// decryptor, so the runtime `receive()` resolves `Err` for it with the consumer still open.
    encrypted: bool,
}

impl Scripted {
    fn plain(seq: u64) -> Self {
        Self {
            seq,
            key: None,
            payload: b"plain",
            encrypted: false,
        }
    }

    fn encrypted(seq: u64) -> Self {
        Self {
            seq,
            key: None,
            payload: b"ciphertext",
            encrypted: true,
        }
    }

    fn keyed(seq: u64, key: &'static str, payload: &'static [u8]) -> Self {
        Self {
            seq,
            key: Some(key),
            payload,
            encrypted: false,
        }
    }
}

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> topic, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), String>,
    /// topic -> `(connection, consumer id)`, set by the consumer's first `CommandFlow`.
    flowed: HashMap<String, (u64, u64)>,
    /// Messages waiting for a flowed consumer on their topic.
    pending: HashMap<String, VecDeque<Scripted>>,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
    /// Wakes the connection task that owns the connection id.
    wake: HashMap<u64, Arc<Notify>>,
    /// Consumer ids the client has closed, in order.
    closed: Vec<(u64, u64)>,
    /// The topics every `CommandWatchTopicList` is answered with.
    watch_topics: Vec<String>,
}

impl State {
    /// Move every pending message whose topic has a flowed consumer into that connection's outbox.
    fn pump(&mut self) {
        let topics: Vec<String> = self.pending.keys().cloned().collect();
        for topic in topics {
            let Some(&(conn, consumer_id)) = self.flowed.get(&topic) else {
                continue;
            };
            let Some(queue) = self.pending.get_mut(&topic) else {
                continue;
            };
            let drained: Vec<Scripted> = queue.drain(..).collect();
            if drained.is_empty() {
                continue;
            }
            let out = self.outbox.entry(conn).or_default();
            for message in &drained {
                out.extend_from_slice(&message_frame(consumer_id, message));
            }
            if let Some(notify) = self.wake.get(&conn) {
                notify.notify_one();
            }
        }
    }
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
                let notify = Arc::new(Notify::new());
                accept_state.lock().wake.insert(conn, notify.clone());
                tokio::spawn(serve(stream, conn, accept_state.clone(), notify));
            }
        });
        Self { addr, state }
    }

    fn url(&self) -> String {
        format!("pulsar://{}", self.addr)
    }

    /// Queue `message` for the consumer of `topic`; it is pushed once that consumer has sent its
    /// first `CommandFlow`.
    fn publish(&self, topic: &str, message: Scripted) {
        let mut state = self.state.lock();
        state
            .pending
            .entry(topic.to_owned())
            .or_default()
            .push_back(message);
        state.pump();
    }

    fn closed_consumers(&self) -> usize {
        self.state.lock().closed.len()
    }

    /// Set the topic list every `CommandWatchTopicList` is answered with.
    fn watch_topics(&self, topics: &[&str]) {
        self.state.lock().watch_topics = topics.iter().map(|t| (*t).to_owned()).collect();
    }

    async fn client(&self) -> PulsarClient {
        tokio::time::timeout(
            DEADLINE,
            PulsarClient::builder().service_url(self.url()).build(),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok")
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

fn message_frame(consumer_id: u64, message: &Scripted) -> BytesMut {
    let cmd = pb::BaseCommand {
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id: message.seq,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..base(pb::base_command::Type::Message)
    };
    let mut metadata = pb::MessageMetadata {
        producer_name: "push-survival-broker".to_owned(),
        sequence_id: message.seq,
        publish_time: 0,
        partition_key: message.key.map(str::to_owned),
        ..Default::default()
    };
    if message.encrypted {
        metadata.encryption_keys.push(pb::EncryptionKeys {
            key: "push-survival".to_owned(),
            value: Bytes::from_static(b"k"),
            metadata: Vec::new(),
        });
        metadata.encryption_algo = Some("PUSH-SURVIVAL".to_owned());
        metadata.encryption_param = Some(Bytes::from_static(b"iv"));
    }
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, message.payload).expect("encode message frame");
    frame
}

/// Handle one decoded client command, queueing replies in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    let mut guard = state.lock();
    let ty = cmd.r#type;
    let reply: Option<BytesMut> = if ty == pb::base_command::Type::Connect as i32 {
        Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "push-survival-fake".to_owned(),
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
    } else if ty == pb::base_command::Type::WatchTopicList as i32 {
        cmd.watch_topic_list.as_ref().map(|watch| {
            command_frame(&pb::BaseCommand {
                watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                    request_id: watch.request_id,
                    watcher_id: watch.watcher_id,
                    topic: guard.watch_topics.clone(),
                    topics_hash: "push-survival".to_owned(),
                }),
                ..base(pb::base_command::Type::WatchTopicListSuccess)
            })
        })
    } else if ty == pb::base_command::Type::Subscribe as i32 {
        cmd.subscribe.as_ref().map(|subscribe| {
            guard
                .subscribed
                .insert((conn, subscribe.consumer_id), subscribe.topic.clone());
            command_frame(&pb::BaseCommand {
                success: Some(pb::CommandSuccess {
                    request_id: subscribe.request_id,
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::Success)
            })
        })
    } else if ty == pb::base_command::Type::Flow as i32 {
        if let Some(flow) = cmd.flow.as_ref()
            && let Some(topic) = guard.subscribed.get(&(conn, flow.consumer_id)).cloned()
        {
            guard.flowed.insert(topic, (conn, flow.consumer_id));
            guard.pump();
        }
        None
    } else if ty == pb::base_command::Type::Ack as i32 {
        // `Consumer::ack` resolves on the broker's `CommandAckResponse`.
        cmd.ack.as_ref().and_then(|ack| {
            ack.request_id.map(|request_id| {
                command_frame(&pb::BaseCommand {
                    ack_response: Some(pb::CommandAckResponse {
                        consumer_id: ack.consumer_id,
                        request_id: Some(request_id),
                        ..Default::default()
                    }),
                    ..base(pb::base_command::Type::AckResponse)
                })
            })
        })
    } else if ty == pb::base_command::Type::CloseConsumer as i32 {
        cmd.close_consumer.as_ref().map(|close| {
            if let Some(topic) = guard.subscribed.remove(&(conn, close.consumer_id)) {
                guard.flowed.remove(&topic);
            }
            guard.closed.push((conn, close.consumer_id));
            command_frame(&pb::BaseCommand {
                success: Some(pb::CommandSuccess {
                    request_id: close.request_id,
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::Success)
            })
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

async fn serve(mut stream: TcpStream, conn: u64, state: Arc<Mutex<State>>, notify: Arc<Notify>) {
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
            () = notify.notified() => {}
        }
    }
}

/// Install, once per test binary, a subscriber that enables every event and discards the output,
/// so the fields of the `debug!` event each skip logs are evaluated as they are in a deployment
/// that records them.
fn log_every_event() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .with_writer(std::io::sink)
        .try_init();
}

/// Poll `cond` every 10 ms until it holds or `DEADLINE` elapses; the final verdict is `cond()`.
async fn eventually(mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

fn single_listener(seen: &Arc<Mutex<Vec<u64>>>) -> MessageListener {
    let seen = seen.clone();
    Arc::new(move |msg: &IncomingMessage| seen.lock().push(msg.sequence_id()))
}

fn wrapper_listener(seen: &Arc<Mutex<Vec<(String, u64)>>>) -> WrapperMessageListener {
    let seen = seen.clone();
    Arc::new(move |topic: &str, msg: &IncomingMessage| {
        seen.lock().push((topic.to_owned(), msg.sequence_id()));
    })
}

/// Control for the single-topic scenario: well-formed messages reach the push listener through
/// this broker, so the red results below come from the error handling, not from the harness.
#[tokio::test(flavor = "current_thread")]
async fn single_topic_push_listener_delivers_plain_messages() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    let handle = client
        .consumer(TOPIC_A)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .message_listener(single_listener(&seen))
        .subscribe_with_listener()
        .await
        .expect("subscribe_with_listener");
    broker.publish(TOPIC_A, Scripted::plain(0));
    broker.publish(TOPIC_A, Scripted::plain(1));

    let delivered = eventually(|| seen.lock().len() == 2).await;
    let running = handle.is_running();
    handle.close().await;
    assert!(
        delivered,
        "control: both well-formed messages must reach the listener, saw {:?}",
        seen.lock()
    );
    assert!(running, "control: the poller must still be running");
    assert_eq!(*seen.lock(), vec![0, 1], "control: delivered in order");
}

/// Control for the error scenarios: in pull mode the very same broker script — one encrypted
/// message, then a well-formed one — gives `receive()` an `Err` followed by `Ok`. The consumer is
/// still open and the follow-up message is deliverable, which is the premise the push loops break.
#[tokio::test(flavor = "current_thread")]
async fn pull_receive_survives_a_per_message_receive_error() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;

    let consumer = client
        .consumer(TOPIC_A)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    broker.publish(TOPIC_A, Scripted::encrypted(0));
    broker.publish(TOPIC_A, Scripted::plain(1));

    let first = tokio::time::timeout(DEADLINE, consumer.receive())
        .await
        .expect("first receive did not time out");
    assert!(
        first.is_err(),
        "control: the encrypted message must surface as a receive() error"
    );
    assert!(
        !consumer.is_closed(),
        "control: a per-message error leaves the consumer open"
    );
    let second = tokio::time::timeout(DEADLINE, consumer.receive())
        .await
        .expect("second receive did not time out")
        .expect("control: the follow-up message is delivered");
    assert_eq!(second.metadata.sequence_id, 1);
}

/// Single-topic site (`spawn_listener_loop`, used by
/// `ConsumerBuilder::subscribe_with_listener`): one encrypted message with no decryptor makes
/// `receive()` resolve `Err` while the consumer stays open. The next message is well formed and
/// must still be delivered; the poller must not have exited.
#[tokio::test(flavor = "current_thread")]
async fn single_topic_push_listener_survives_a_per_message_receive_error() {
    log_every_event();
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    let handle = client
        .consumer(TOPIC_A)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .message_listener(single_listener(&seen))
        .subscribe_with_listener()
        .await
        .expect("subscribe_with_listener");
    broker.publish(TOPIC_A, Scripted::encrypted(0));
    broker.publish(TOPIC_A, Scripted::plain(1));

    let delivered = eventually(|| !seen.lock().is_empty()).await;
    let running = handle.is_running();
    let closed_by_poller_exit = broker.closed_consumers();
    handle.close().await;
    assert!(
        delivered,
        "the well-formed message that follows one undecryptable message was never delivered: \
         the push poller treated a per-message receive() error as 'consumer closed' and exited \
         (poller running = {running}, consumers closed on the broker = {closed_by_poller_exit})"
    );
    assert!(running, "the poller must outlive a per-message error");
    assert_eq!(*seen.lock(), vec![1]);
}

/// Wrapper site (`spawn_wrapper_message_listener`): the same per-message error on one
/// topic of a multi-topic consumer must neither end delivery for that topic nor for its sibling.
#[tokio::test(flavor = "current_thread")]
async fn multi_topic_push_listener_survives_a_per_message_receive_error() {
    log_every_event();
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    let handle = client
        .multi_topics_consumer()
        .topic(TOPIC_A)
        .topic(TOPIC_B)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .message_listener(wrapper_listener(&seen))
        .subscribe_with_listener()
        .await
        .expect("subscribe_with_listener");
    broker.publish(TOPIC_A, Scripted::encrypted(0));
    broker.publish(TOPIC_A, Scripted::plain(1));
    broker.publish(TOPIC_B, Scripted::plain(2));

    let expected = [(TOPIC_A.to_owned(), 1), (TOPIC_B.to_owned(), 2)];
    let delivered = eventually(|| {
        let seen = seen.lock();
        expected.iter().all(|pair| seen.contains(pair))
    })
    .await;
    let running = handle.is_running();
    handle.close().await;
    assert!(
        delivered,
        "well-formed messages after one undecryptable message were not all delivered: the wrapper \
         poller treated a per-message receive() error as 'consumer closed' and exited \
         (poller running = {running}, saw {:?})",
        seen.lock()
    );
    assert!(
        running,
        "the wrapper poller must outlive a per-message error"
    );
}

/// `TableView` site (`spawn_drain`): the drain exits on the first per-message error, so
/// the view freezes at its pre-error content while `is_connected()` stays true.
#[tokio::test(flavor = "current_thread")]
async fn table_view_drain_survives_a_per_message_receive_error() {
    log_every_event();
    let broker = FakeBroker::start().await;
    let client = broker.client().await;

    let view = tokio::time::timeout(
        DEADLINE,
        client
            .table_view(TOPIC_A)
            .subscription_name("push-survival")
            .create(),
    )
    .await
    .expect("table view create did not time out")
    .expect("table view create");
    broker.publish(
        TOPIC_A,
        Scripted {
            key: Some("bad"),
            ..Scripted::encrypted(0)
        },
    );
    broker.publish(TOPIC_A, Scripted::keyed(1, "good", b"v"));

    let applied = eventually(|| view.get("good").is_some()).await;
    let connected = view.is_connected();
    let snapshot = view.snapshot();
    view.close().await;
    assert!(
        applied,
        "the keyed message that follows one undecryptable message never reached the view: the \
         drain treated a per-message receive() error as 'consumer closed' and exited, leaving a \
         stale view (is_connected = {connected}, snapshot = {snapshot:?})"
    );
}

/// `remove_topic` closes the child it just removed while the poller is parked over a
/// snapshot that still holds it. That child resolves `Err(Closed)`, which must not end delivery
/// for the topics that remain. The first two messages are the control: they prove the poller is
/// live and parked over the `{a, b}` snapshot before the removal.
#[tokio::test(flavor = "current_thread")]
async fn multi_topic_push_listener_survives_remove_topic() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    let consumer = client
        .multi_topics_consumer()
        .topic(TOPIC_A)
        .topic(TOPIC_B)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    let handle = spawn_wrapper_message_listener(consumer.clone(), wrapper_listener(&seen));

    broker.publish(TOPIC_A, Scripted::plain(0));
    broker.publish(TOPIC_B, Scripted::plain(1));
    let warmed = eventually(|| seen.lock().len() == 2).await;
    assert!(
        warmed,
        "control: both topics must deliver before the removal, saw {:?}",
        seen.lock()
    );
    assert!(handle.is_running(), "control: poller live before removal");

    consumer
        .remove_topic(TOPIC_A)
        .await
        .expect("remove_topic closes the child");
    assert_eq!(consumer.len(), 1, "topic a left the set, topic b remains");
    broker.publish(TOPIC_B, Scripted::plain(2));

    let expected = (TOPIC_B.to_owned(), 2);
    let delivered = eventually(|| seen.lock().contains(&expected)).await;
    let running = handle.is_running();
    handle.close().await;
    assert!(
        delivered,
        "the surviving topic stopped delivering after remove_topic closed the other child: the \
         wrapper poller treated that child's Err(Closed) as the whole consumer closing \
         (poller running = {running}, saw {:?})",
        seen.lock()
    );
    assert!(running, "the poller must outlive the removal of one child");
}

/// Pattern pull site (`PatternConsumer::receive` over several children): a per-message error on
/// one child is returned naming that child's topic, and the sweep moves past it, so the following
/// calls deliver the sibling's message and the failing child's next one.
#[tokio::test(flavor = "current_thread")]
async fn pattern_pull_receive_survives_a_per_message_receive_error() {
    log_every_event();
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    broker.watch_topics(&[TOPIC_A, TOPIC_B]);
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("push-survival")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe pattern");
    broker.publish(TOPIC_A, Scripted::encrypted(0));
    broker.publish(TOPIC_A, Scripted::plain(1));
    broker.publish(TOPIC_B, Scripted::plain(2));

    let mut errors = Vec::new();
    let mut delivered = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(DEADLINE, consumer.receive())
            .await
            .expect("receive did not time out")
        {
            Ok(message) => delivered.push((message.topic, message.message.metadata.sequence_id)),
            Err(err) => errors.push(err.to_string()),
        }
        if delivered.len() == 2 {
            break;
        }
    }
    delivered.sort();
    assert_eq!(
        delivered,
        vec![(TOPIC_A.to_owned(), 1), (TOPIC_B.to_owned(), 2)],
        "both well-formed messages must be delivered after the undecryptable one (errors: \
         {errors:?})"
    );
    assert!(
        errors.len() == 1 && errors[0].contains(TOPIC_A),
        "the undecryptable message must surface once, naming its topic, got {errors:?}"
    );
}
