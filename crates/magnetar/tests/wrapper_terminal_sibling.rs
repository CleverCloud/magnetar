// SPDX-License-Identifier: Apache-2.0

//! A terminal child of a wrapper consumer (`MultiTopicsConsumer`, `PatternConsumer`) must neither
//! end the wrapper nor make its pull `receive()` fail while a sibling is still live.
//!
//! A child that is terminal (here, its broker connection was lost for good) resolves
//! `receive()` with `Err(Closed)` on its first poll, forever. If the wrapper sweeps it with its
//! siblings, that error wins every round in which no earlier sibling has a message ready, so:
//!
//! * a pull `receive()` over a live topic that is quiet returns the dead child's error at once
//!   instead of waiting for the live topic, and a caller that retries spins on it;
//! * the push poller (`spawn_wrapper_message_listener`), which treats "some child is terminal" as
//!   the end of the wrapper, stops delivering the live topic.
//!
//! `wrapper_pull_receive_fairness.rs` covers the case where the live topic already holds messages
//! when `receive()` is called. These scenarios cover the part it cannot: the live topic is quiet
//! when the call starts, so the call must wait for it, and the push poller must keep running over
//! it. They run against a real `PulsarClient` and the real tokio runtime, speaking to a scripted
//! loopback broker (the harness of that file, extended with per-connection drops); the wrapper
//! receive path is engine-generic façade code, so one tokio-driven harness covers both engines.
//!
//! Each scenario starts from the same precondition the controls pin down: the two children sit on
//! two connections, dropping one makes only its child terminal, and the live child still delivers.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{PulsarClient, WrapperMessageListener, spawn_wrapper_message_listener};
use magnetar_proto::{FrameError, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/wrap-term-.*";
const DEAD: &str = "persistent://public/default/wrap-term-dead";
const LIVE: &str = "persistent://public/default/wrap-term-live";
const SOLO: &str = "persistent://public/default/wrap-term-solo";
const DEADLINE: Duration = Duration::from_secs(3);
/// How long a `receive()` that must be waiting is given to resolve before it counts as waiting.
/// A defective `receive()` resolves on its first poll, so this only needs to outlast one poll.
const WAITING: Duration = Duration::from_millis(100);

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> topic, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), String>,
    /// topic -> `(connection, consumer id)`, set by the consumer's first `CommandFlow`.
    flowed: HashMap<String, (u64, u64)>,
    /// Messages waiting for a flowed consumer on their topic (the sequence id identifies one).
    pending: HashMap<String, VecDeque<u64>>,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
    /// Wakes the connection task that owns the connection id.
    wake: HashMap<u64, Arc<Notify>>,
    /// Connections the broker has been told to close for good.
    dropped: HashSet<u64>,
    /// The topics every `CommandWatchTopicList` is answered with, in this order.
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
            let drained: Vec<u64> = queue.drain(..).collect();
            if drained.is_empty() {
                continue;
            }
            let out = self.outbox.entry(conn).or_default();
            for seq in drained {
                out.extend_from_slice(&message_frame(consumer_id, seq));
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

    /// `connections_per_broker` of 1 keeps every consumer on the bootstrap connection; 2 puts the
    /// second consumer a client opens on a sibling connection to the same broker.
    async fn client(&self, connections_per_broker: usize) -> PulsarClient {
        tokio::time::timeout(
            DEADLINE,
            PulsarClient::builder()
                .service_url(self.url())
                .connections_per_broker(connections_per_broker)
                .build(),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok")
    }

    /// Set the topic list every `CommandWatchTopicList` is answered with.
    fn watch_topics(&self, topics: &[&str]) {
        self.state.lock().watch_topics = topics.iter().map(|t| (*t).to_owned()).collect();
    }

    /// Queue `seq` for the consumer of `topic`; it is pushed once that consumer has sent its first
    /// `CommandFlow`.
    fn publish(&self, topic: &str, seq: u64) {
        let mut state = self.state.lock();
        state
            .pending
            .entry(topic.to_owned())
            .or_default()
            .push_back(seq);
        state.pump();
    }

    /// The connection carrying the consumer subscribed to `topic`.
    fn connection_of(&self, topic: &str) -> Option<u64> {
        self.state
            .lock()
            .subscribed
            .iter()
            .find(|(_, subscribed)| subscribed.as_str() == topic)
            .map(|(&(conn, _), _)| conn)
    }

    fn flowed(&self, topic: &str) -> bool {
        self.state.lock().flowed.contains_key(topic)
    }

    /// Close one connection for good: the broker side closes the socket and the connection task
    /// ends, so nothing the client does reaches this connection again.
    fn drop_connection(&self, conn: u64) {
        let mut state = self.state.lock();
        state.dropped.insert(conn);
        if let Some(notify) = state.wake.get(&conn) {
            notify.notify_one();
        }
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

fn message_frame(consumer_id: u64, seq: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id: seq,
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
    let metadata = pb::MessageMetadata {
        producer_name: "wrap-term-broker".to_owned(),
        sequence_id: seq,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, b"plain").expect("encode message frame");
    frame
}

/// Handle one decoded client command, queueing replies in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    let mut guard = state.lock();
    let ty = cmd.r#type;
    let reply: Option<BytesMut> = if ty == pb::base_command::Type::Connect as i32 {
        Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "wrap-term-fake".to_owned(),
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
                    topics_hash: "wrap-term".to_owned(),
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
    } else if ty == pb::base_command::Type::CloseConsumer as i32 {
        cmd.close_consumer.as_ref().map(|close| {
            if let Some(topic) = guard.subscribed.remove(&(conn, close.consumer_id)) {
                guard.flowed.remove(&topic);
            }
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
        if state.lock().dropped.contains(&conn) {
            return;
        }
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

impl FakeBroker {
    /// Close every connection for good: nothing the client does reaches the broker again.
    fn drop_every_connection(&self) {
        let mut state = self.state.lock();
        let connections: Vec<u64> = state.wake.keys().copied().collect();
        for conn in connections {
            state.dropped.insert(conn);
            if let Some(notify) = state.wake.get(&conn) {
                notify.notify_one();
            }
        }
    }
}

/// Run `scenario` on a two-thread runtime, then drop the runtime without waiting for its tasks.
/// A poller that retries a terminal error without ever parking never yields to the scheduler, so
/// the usual runtime shutdown (and `MessageListenerHandle::close`) would wait for it forever and a
/// defect would hang the test binary instead of failing the assertion.
fn run(scenario: impl std::future::Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(scenario);
    }));
    runtime.shutdown_background();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

fn wrapper_listener(seen: &Arc<Mutex<Vec<(String, u64)>>>) -> WrapperMessageListener {
    let seen = seen.clone();
    Arc::new(move |topic: &str, msg: &magnetar::IncomingMessage| {
        seen.lock().push((topic.to_owned(), msg.sequence_id()));
    })
}

/// A multi-topics consumer over `[DEAD, LIVE]` on two connections, with the connection of `DEAD`
/// dropped for good: `DEAD` is child 0 and terminal, `LIVE` is child 1 and still open.
async fn multi_topics_with_a_terminal_first_child(
    broker: &FakeBroker,
    client: &PulsarClient,
) -> magnetar::MultiTopicsConsumer {
    let consumer = client
        .multi_topics_consumer()
        .topic(DEAD)
        .topic(LIVE)
        .subscription("wrap-term")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    assert_eq!(
        consumer.topics(),
        [DEAD, LIVE],
        "harness: the terminal topic must be child 0"
    );
    let dead_conn = broker.connection_of(DEAD).expect("dead subscribed");
    let live_conn = broker.connection_of(LIVE).expect("live subscribed");
    assert_ne!(
        dead_conn, live_conn,
        "harness: connections_per_broker(2) must put the two children on two connections"
    );
    broker.drop_connection(dead_conn);
    assert!(
        eventually(|| !consumer.is_connected()).await,
        "harness: the wrapper must notice that one child lost its connection"
    );
    assert!(broker.flowed(LIVE), "harness: live child flowed");
    consumer
}

/// The pattern twin of [`multi_topics_with_a_terminal_first_child`].
async fn pattern_with_a_terminal_first_child(
    broker: &FakeBroker,
    client: &PulsarClient,
) -> magnetar::PatternConsumer {
    broker.watch_topics(&[DEAD, LIVE]);
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("wrap-term")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    assert_eq!(
        consumer.topics(),
        [DEAD, LIVE],
        "harness: the terminal topic must be child 0"
    );
    let dead_conn = broker.connection_of(DEAD).expect("dead subscribed");
    let live_conn = broker.connection_of(LIVE).expect("live subscribed");
    assert_ne!(
        dead_conn, live_conn,
        "harness: connections_per_broker(2) must put the two children on two connections"
    );
    broker.drop_connection(dead_conn);
    assert!(
        eventually(|| !consumer.is_connected()).await,
        "harness: the wrapper must notice that one child lost its connection"
    );
    assert!(broker.flowed(LIVE), "harness: live child flowed");
    consumer
}

/// Terminal sibling, quiet live topic (multi-topics): the terminal child is first and the live
/// topic holds nothing yet, so a `receive()` that keeps the terminal child in its sweep resolves
/// with that child's error on its first poll. It must wait for the live topic instead, and deliver
/// what arrives there afterwards.
#[tokio::test(flavor = "current_thread")]
async fn multi_topics_receive_waits_for_the_live_topic_while_a_sibling_child_is_terminal() {
    let broker = FakeBroker::start().await;
    let client = broker.client(2).await;
    let consumer = multi_topics_with_a_terminal_first_child(&broker, &client).await;

    let mut receive = Box::pin(consumer.receive());
    match tokio::time::timeout(WAITING, &mut receive).await {
        Err(_) => {}
        Ok(Err(e)) => panic!(
            "MultiTopicsConsumer::receive resolved with `{e}` while its only live topic was \
             quiet: the terminal sibling's error ends every call instead of the call waiting"
        ),
        Ok(Ok(m)) => panic!(
            "harness: nothing was published, yet `{}` delivered",
            m.topic
        ),
    }
    broker.publish(LIVE, 7);
    let delivered = tokio::time::timeout(DEADLINE, receive)
        .await
        .expect("the live message must wake the parked receive")
        .expect("the live topic delivers");
    assert_eq!(delivered.topic, LIVE);
    assert_eq!(delivered.message.metadata.sequence_id, 7);
}

/// Terminal sibling, quiet live topic (pattern): same scenario through the pattern wrapper.
#[tokio::test(flavor = "current_thread")]
async fn pattern_receive_waits_for_the_live_topic_while_a_sibling_child_is_terminal() {
    let broker = FakeBroker::start().await;
    let client = broker.client(2).await;
    let consumer = pattern_with_a_terminal_first_child(&broker, &client).await;

    let mut receive = Box::pin(consumer.receive());
    match tokio::time::timeout(WAITING, &mut receive).await {
        Err(_) => {}
        Ok(Err(e)) => panic!(
            "PatternConsumer::receive resolved with `{e}` while its only live topic was quiet: \
             the terminal sibling's error ends every call instead of the call waiting"
        ),
        Ok(Ok(m)) => panic!(
            "harness: nothing was published, yet `{}` delivered",
            m.topic
        ),
    }
    broker.publish(LIVE, 7);
    let delivered = tokio::time::timeout(DEADLINE, receive)
        .await
        .expect("the live message must wake the parked receive")
        .expect("the live topic delivers");
    assert_eq!(delivered.topic, LIVE);
    assert_eq!(delivered.message.metadata.sequence_id, 7);
}

/// Once every child is terminal there is nothing left to wait for: `receive()` fails, and the
/// error says which topic it concerns (a nameless error left the caller no way to tell).
#[tokio::test(flavor = "current_thread")]
async fn receive_fails_naming_a_topic_once_every_child_is_terminal() {
    let broker = FakeBroker::start().await;
    let client = broker.client(1).await;
    let multi = client
        .multi_topics_consumer()
        .topic(DEAD)
        .topic(LIVE)
        .subscription("wrap-term")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe multi");
    broker.watch_topics(&[DEAD, LIVE]);
    let pattern = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("wrap-term-pattern")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe pattern");

    broker.drop_every_connection();
    assert!(
        eventually(|| multi.is_closed() && pattern.is_closed()).await,
        "harness: every child must be closed once the only connection is lost"
    );

    let multi_err = tokio::time::timeout(DEADLINE, multi.receive())
        .await
        .expect("a terminal wrapper must not park")
        .expect_err("every child is terminal");
    let pattern_err = tokio::time::timeout(DEADLINE, pattern.receive())
        .await
        .expect("a terminal wrapper must not park")
        .expect_err("every child is terminal");
    for (who, err) in [
        ("MultiTopicsConsumer", multi_err),
        ("PatternConsumer", pattern_err),
    ] {
        let text = err.to_string();
        assert!(
            text.contains(DEAD) || text.contains(LIVE),
            "{who}::receive must name the topic its error concerns, got `{text}`"
        );
    }
}

/// The push poller of a wrapper stops only once the wrapper can no longer deliver. A terminal
/// child with a live sibling is not that: the poller must keep running and keep delivering the
/// live topic (multi-topics).
#[test]
fn multi_topic_push_listener_outlives_a_terminal_sibling_child() {
    run(async {
        let broker = FakeBroker::start().await;
        let client = broker.client(2).await;
        let consumer = multi_topics_with_a_terminal_first_child(&broker, &client).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_wrapper_message_listener(consumer.clone(), wrapper_listener(&seen));

        broker.publish(LIVE, 1);
        let first = eventually(|| seen.lock().contains(&(LIVE.to_owned(), 1))).await;
        broker.publish(LIVE, 2);
        let second = eventually(|| seen.lock().contains(&(LIVE.to_owned(), 2))).await;
        let running = handle.is_running();
        assert!(
            first && second && running,
            "the push poller stopped delivering `{LIVE}` because its sibling `{DEAD}` is terminal \
             (poller running = {running}, saw {:?})",
            seen.lock()
        );
    });
}

/// The pattern twin of [`multi_topic_push_listener_outlives_a_terminal_sibling_child`].
#[test]
fn pattern_push_listener_outlives_a_terminal_sibling_child() {
    run(async {
        let broker = FakeBroker::start().await;
        let client = broker.client(2).await;
        let consumer = pattern_with_a_terminal_first_child(&broker, &client).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_wrapper_message_listener(consumer.clone(), wrapper_listener(&seen));

        broker.publish(LIVE, 1);
        let first = eventually(|| seen.lock().contains(&(LIVE.to_owned(), 1))).await;
        broker.publish(LIVE, 2);
        let second = eventually(|| seen.lock().contains(&(LIVE.to_owned(), 2))).await;
        let running = handle.is_running();
        assert!(
            first && second && running,
            "the push poller stopped delivering `{LIVE}` because its sibling `{DEAD}` is terminal \
             (poller running = {running}, saw {:?})",
            seen.lock()
        );
    });
}

/// Control for the two scenarios above, and the pattern site `push_delivery_terminal_exit.rs`
/// does not cover: when the one connection every child shares is lost, every child is terminal and
/// the pattern poller must end instead of retrying the error it resolves at once.
#[test]
fn pattern_push_listener_ends_when_every_child_is_terminal() {
    run(async {
        let broker = FakeBroker::start().await;
        broker.watch_topics(&[DEAD, LIVE]);
        let client = broker.client(1).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = client
            .pattern_consumer()
            .namespace(NAMESPACE)
            .pattern(PATTERN)
            .subscription("wrap-term")
            .subscription_type(SubType::Exclusive)
            .message_listener(wrapper_listener(&seen))
            .subscribe_with_listener()
            .await
            .expect("subscribe_with_listener");
        assert!(handle.is_running(), "control: poller live before the drop");

        broker.drop_every_connection();

        assert!(
            eventually(|| !handle.is_running()).await,
            "the pattern push poller kept running after every child became terminal: it retried \
             a terminal receive() error instead of stopping"
        );
    });
}

/// A terminal child leaves the sweep only once it has nothing left to hand out: messages already
/// queued on it when its connection was lost are delivered, exactly as the child's own
/// `receive()` would, and the call that finds it empty moves on to the live sibling. A sweep that
/// excluded a child as soon as it was terminal would strand them.
#[tokio::test(flavor = "current_thread")]
async fn multi_topics_receive_drains_a_terminal_child_before_leaving_it() {
    let broker = FakeBroker::start().await;
    let client = broker.client(2).await;
    let consumer = client
        .multi_topics_consumer()
        .topic(DEAD)
        .topic(LIVE)
        .subscription("wrap-term")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    let dead_conn = broker.connection_of(DEAD).expect("dead subscribed");
    assert_ne!(
        dead_conn,
        broker.connection_of(LIVE).expect("live subscribed"),
        "harness: connections_per_broker(2) must put the two children on two connections"
    );
    assert!(
        eventually(|| broker.flowed(DEAD) && broker.flowed(LIVE)).await,
        "harness: both children must have flowed before the broker pushes"
    );
    broker.publish(DEAD, 10);
    broker.publish(DEAD, 11);
    assert!(
        eventually(|| consumer.available_in_queue() == 2).await,
        "harness: both messages must be queued client-side before the connection drops, queued {}",
        consumer.available_in_queue()
    );
    broker.drop_connection(dead_conn);
    assert!(
        eventually(|| !consumer.is_connected()).await,
        "harness: the wrapper must notice that one child lost its connection"
    );
    broker.publish(LIVE, 20);
    assert!(
        eventually(|| consumer.available_in_queue() == 3).await,
        "harness: the live message must be queued client-side, queued {}",
        consumer.available_in_queue()
    );

    let mut delivered = Vec::new();
    for call in 0..3 {
        let message = tokio::time::timeout(DEADLINE, consumer.receive())
            .await
            .expect("receive did not time out")
            .unwrap_or_else(|e| panic!("receive #{call} failed with `{e}`, got {delivered:?}"));
        delivered.push((message.topic, message.message.metadata.sequence_id));
    }
    delivered.sort();
    assert_eq!(
        delivered,
        [
            (DEAD.to_owned(), 10),
            (DEAD.to_owned(), 11),
            (LIVE.to_owned(), 20)
        ],
        "every queued message must be delivered, the terminal child's included"
    );
}

/// A wrapper is terminal when every child of its set is, and "every" holds over an empty set: the
/// emptiness guard of `WrapperReceiver::is_terminal` is what keeps `remove_topic` of the last
/// topic from reading as a terminal wrapper. The poller must park on the empty set and deliver
/// the topic added afterwards.
#[test]
fn multi_topic_push_listener_survives_removing_its_last_topic() {
    run(async {
        let broker = FakeBroker::start().await;
        let client = broker.client(1).await;
        let consumer = client
            .multi_topics_consumer()
            .topic(SOLO)
            .subscription("wrap-term")
            .subscription_type(SubType::Exclusive)
            .subscribe()
            .await
            .expect("subscribe");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_wrapper_message_listener(consumer.clone(), wrapper_listener(&seen));

        assert!(
            eventually(|| broker.flowed(SOLO)).await,
            "harness: the child must have flowed before the broker pushes"
        );
        broker.publish(SOLO, 0);
        assert!(
            eventually(|| seen.lock().contains(&(SOLO.to_owned(), 0))).await,
            "control: the poller delivers before the removal, saw {:?}",
            seen.lock()
        );

        consumer
            .remove_topic(SOLO)
            .await
            .expect("remove_topic closes the child");
        assert!(consumer.is_empty(), "the only topic left the set");
        consumer
            .add_topic(&client, LIVE)
            .await
            .expect("add_topic subscribes the new child");
        assert!(
            eventually(|| broker.flowed(LIVE)).await,
            "harness: the new child must have flowed before the broker pushes"
        );
        broker.publish(LIVE, 1);

        let delivered = eventually(|| seen.lock().contains(&(LIVE.to_owned(), 1))).await;
        let running = handle.is_running();
        assert!(
            delivered && running,
            "the push poller ended when its last topic was removed, so the topic added \
             afterwards was never delivered (poller running = {running}, saw {:?})",
            seen.lock()
        );
    });
}
