// SPDX-License-Identifier: Apache-2.0

//! Pull `receive()` of the wrapper consumers (`MultiTopicsConsumer`, `PatternConsumer`) must reach
//! every child that has something to deliver.
//!
//! Two defects share one shape: a child that is always ready, or a child that is terminal, sits
//! ahead of the rest in the sweep and the rest are never reached.
//!
//! * Busy sibling: `PatternConsumer::receive` hands `select_all` the children in snapshot order
//!   with no cursor, and `select_all` returns the first ready future in input order, so while child
//!   0 has a backlog children 1..n are never polled. Their receiver queues fill, no refill credit
//!   is sent, and their topics stall until child 0 runs dry. `MultiTopicsConsumer::receive` carries
//!   the rotation that fixes it (the "F4" cursor); the pattern twin was never given it.
//! * Terminal sibling: a child that is terminal (here, its broker connection was lost for good)
//!   resolves `Err(Closed)` on its first poll, forever. `MultiTopicsConsumer::receive` advances its
//!   cursor only on `Ok`, so once the sweep starts at that child every later call returns its `Err`
//!   at once and never reaches the healthy topics; `PatternConsumer::receive` has no cursor at all,
//!   so a terminal child ahead of the healthy ones hides them. The error names no topic, so the
//!   caller cannot tell which child to remove.
//!
//! Both are asserted the way a caller observes them, with a fix-agnostic criterion: within a
//! bounded number of `receive()` calls, tolerating an `Err` per call, every message the healthy
//! topic holds is delivered (terminal sibling), or the quiet topic is served at all while the busy
//! one has a backlog (busy sibling). Nothing asserts how a fix is shaped (rotation, skipping
//! terminal children, naming the topic in the error).
//!
//! The scenarios run against a real `PulsarClient` and the real tokio runtime, speaking to a
//! scripted loopback broker (the same fake-broker pattern as `push_delivery_error_survival.rs`,
//! extended with the PIP-145 topic-list watch the pattern consumer discovers its topics through,
//! and with per-connection drops). The wrapper receive path is engine-generic façade code, so this
//! one tokio-driven harness covers it for both engines.
//!
//! Each failing scenario is paired with a control through the same broker that exercises the same
//! machinery without the defect (the multi-topics wrapper, which has the rotation, serving the
//! quiet topic; a plain consumer on each connection after one was dropped), so a red result is
//! attributable to the wrapper's receive and not to the harness.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{ConsumerApi, PulsarClient};
use magnetar_proto::{FrameError, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/wrap-fair-.*";
const BUSY: &str = "persistent://public/default/wrap-fair-busy";
const QUIET: &str = "persistent://public/default/wrap-fair-quiet";
const DEAD: &str = "persistent://public/default/wrap-fair-dead";
const LIVE: &str = "persistent://public/default/wrap-fair-live";
const DEADLINE: Duration = Duration::from_secs(3);
/// Messages queued on the always-ready topic: well above `FAIRNESS_WINDOW`, so the busy topic is
/// still ready when the window ends.
const BACKLOG: u64 = 32;
/// Receives allowed before the quiet topic must have been served at least once.
const FAIRNESS_WINDOW: usize = 8;
/// Messages the healthy topic holds in the terminal-child scenarios.
const LIVE_MESSAGES: u64 = 3;
/// Receive calls allowed for those messages: one `Err` per message is tolerated, twice over.
const TERMINAL_CALL_BUDGET: usize = 4 * LIVE_MESSAGES as usize;

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
        producer_name: "wrap-fair-broker".to_owned(),
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
                server_version: "wrap-fair-fake".to_owned(),
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
                    topics_hash: "wrap-fair".to_owned(),
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

/// What one `receive()` call returned: a message (topic and sequence id) or an error (its text).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Message { topic: String, seq: u64 },
    Error(String),
}

/// Call `receive` `calls` times, or until `stop` holds over what was collected. A call that does
/// not resolve within `DEADLINE` is a harness failure (everything the scenarios wait on is already
/// queued), not the defect under test, so it panics instead of being recorded.
async fn collect_receives<F, Fut>(
    calls: usize,
    mut receive: F,
    stop: impl Fn(&[Outcome]) -> bool,
) -> Vec<Outcome>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Outcome>,
{
    let mut collected = Vec::new();
    for call in 0..calls {
        let outcome = tokio::time::timeout(DEADLINE, receive())
            .await
            .unwrap_or_else(|_| {
                panic!("harness: receive #{call} did not resolve, collected so far: {collected:?}")
            });
        collected.push(outcome);
        if stop(&collected) {
            break;
        }
    }
    collected
}

fn messages_from<'a>(collected: &'a [Outcome], topic: &'a str) -> impl Iterator<Item = u64> + 'a {
    collected.iter().filter_map(move |outcome| match outcome {
        Outcome::Message { topic: t, seq } if t == topic => Some(*seq),
        _ => None,
    })
}

/// Publish `BACKLOG` messages on `busy` (sequence ids `0..BACKLOG`) and one on `quiet` (sequence
/// id 1000), once both consumers have flowed.
async fn publish_backlog(broker: &FakeBroker, busy: &str, quiet: &str) {
    assert!(
        eventually(|| broker.flowed(busy) && broker.flowed(quiet)).await,
        "harness: both consumers must have flowed before the broker pushes"
    );
    for seq in 0..BACKLOG {
        broker.publish(busy, seq);
    }
    broker.publish(quiet, 1000);
}

fn assert_quiet_topic_served(collected: &[Outcome], quiet: &str, who: &str) {
    assert!(
        collected
            .iter()
            .all(|o| matches!(o, Outcome::Message { .. })),
        "harness: every receive must deliver in this scenario, got {collected:?}"
    );
    assert!(
        messages_from(collected, quiet).next().is_some(),
        "{who}::receive never served `{quiet}` in {FAIRNESS_WINDOW} calls while the busy topic \
         held a backlog of {BACKLOG}: the sweep starts at the first child every time and \
         `select_all` returns the first ready future, so later children are starved. \
         Observed topics, in order: {:?}",
        collected
            .iter()
            .map(|o| match o {
                Outcome::Message { topic, .. } => topic.rsplit('-').next().unwrap_or(topic),
                Outcome::Error(_) => "<err>",
            })
            .collect::<Vec<_>>()
    );
}

/// Control for the busy-sibling case (passes with and without the fix):
/// `MultiTopicsConsumer::receive` carries the rotation, so with the same broker script and the same
/// ready-everywhere precondition the quiet topic is served inside the window. A red `pattern_*`
/// twin is therefore attributable to the pattern wrapper's missing rotation and not to the publish,
/// flow or arrival logic.
#[tokio::test(flavor = "current_thread")]
async fn multi_topics_receive_serves_the_quiet_topic_while_the_busy_topic_has_a_backlog() {
    let broker = FakeBroker::start().await;
    let client = broker.client(1).await;
    let consumer = client
        .multi_topics_consumer()
        .topic(BUSY)
        .topic(QUIET)
        .subscription("wrap-fair")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    assert_eq!(
        consumer.topics(),
        [BUSY, QUIET],
        "harness: the busy topic must be child 0, or the scenario proves nothing"
    );
    publish_backlog(&broker, BUSY, QUIET).await;
    assert!(
        eventually(|| consumer.available_in_queue() as u64 == BACKLOG + 1).await,
        "harness: every message must be queued client-side before the first receive, queued {}",
        consumer.available_in_queue()
    );

    let collected = collect_receives(
        FAIRNESS_WINDOW,
        || async {
            match consumer.receive().await {
                Ok(m) => Outcome::Message {
                    topic: m.topic,
                    seq: m.message.metadata.sequence_id,
                },
                Err(e) => Outcome::Error(e.to_string()),
            }
        },
        |collected| messages_from(collected, QUIET).next().is_some(),
    )
    .await;
    assert_quiet_topic_served(&collected, QUIET, "MultiTopicsConsumer");
}

/// Busy sibling: `PatternConsumer::receive` builds its `select_all` in snapshot order with no
/// cursor. Child 0 (`BUSY`) always has a message ready, so child 1 (`QUIET`), whose message is
/// already queued client-side, is never polled and its message is not delivered inside the window.
#[tokio::test(flavor = "current_thread")]
async fn pattern_receive_serves_the_quiet_topic_while_the_busy_topic_has_a_backlog() {
    let broker = FakeBroker::start().await;
    broker.watch_topics(&[BUSY, QUIET]);
    let client = broker.client(1).await;
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("wrap-fair")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    assert_eq!(
        consumer.topics(),
        [BUSY, QUIET],
        "harness: the busy topic must be child 0, or the scenario proves nothing"
    );
    publish_backlog(&broker, BUSY, QUIET).await;
    // The pattern wrapper exposes no queue depth. A request/response on the same connection is a
    // barrier instead: the broker wrote every message frame before it answers, the client handles
    // frames in order, so once the answer is here every message is in its child's receiver queue.
    client
        .topic_list_snapshot(NAMESPACE, PATTERN)
        .await
        .expect("barrier round trip");

    let collected = collect_receives(
        FAIRNESS_WINDOW,
        || async {
            match consumer.receive().await {
                Ok(m) => Outcome::Message {
                    topic: m.topic,
                    seq: m.message.metadata.sequence_id,
                },
                Err(e) => Outcome::Error(e.to_string()),
            }
        },
        |collected| messages_from(collected, QUIET).next().is_some(),
    )
    .await;
    assert_quiet_topic_served(&collected, QUIET, "PatternConsumer");
}

/// Control for the terminal-child scenarios (passes with and without the fix): two plain consumers
/// on the two connections of a `connections_per_broker(2)` client. Dropping the connection of the
/// first one makes only that consumer terminal; the other, on its own connection, stays live and
/// still delivers. This is the premise the wrapper scenarios build on: one terminal child, one
/// healthy child, no shared connection between them.
#[tokio::test(flavor = "current_thread")]
async fn dropping_one_connection_terminates_only_the_consumer_on_it() {
    let broker = FakeBroker::start().await;
    let client = broker.client(2).await;
    let dead = client
        .consumer(DEAD)
        .subscription("wrap-fair")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe dead");
    let live = client
        .consumer(LIVE)
        .subscription("wrap-fair")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe live");
    let dead_conn = broker.connection_of(DEAD).expect("dead subscribed");
    let live_conn = broker.connection_of(LIVE).expect("live subscribed");
    assert_ne!(
        dead_conn, live_conn,
        "harness: connections_per_broker(2) must put the two consumers on two connections"
    );

    broker.drop_connection(dead_conn);
    assert!(
        eventually(|| ConsumerApi::is_terminal(&dead)).await,
        "the consumer on the dropped connection must become terminal"
    );
    assert!(
        dead.receive().await.is_err(),
        "a terminal consumer resolves receive() with an error at once"
    );
    assert!(
        !ConsumerApi::is_terminal(&live),
        "the consumer on the other connection must not be terminal"
    );
    assert!(broker.flowed(LIVE), "harness: live consumer flowed");
    broker.publish(LIVE, 7);
    let delivered = tokio::time::timeout(DEADLINE, live.receive())
        .await
        .expect("live receive did not time out")
        .expect("live consumer still delivers");
    assert_eq!(delivered.metadata.sequence_id, 7);
}

/// Subscribe a `MultiTopicsConsumer` over `[DEAD, LIVE]` on two connections, terminalize the child
/// of `DEAD` by dropping its connection, then queue `LIVE_MESSAGES` on `LIVE` and wait for them
/// to be queued client-side.
async fn multi_topics_with_a_terminal_first_child(
    broker: &FakeBroker,
    client: &PulsarClient,
) -> magnetar::MultiTopicsConsumer {
    let consumer = client
        .multi_topics_consumer()
        .topic(DEAD)
        .topic(LIVE)
        .subscription("wrap-fair")
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
    for seq in 0..LIVE_MESSAGES {
        broker.publish(LIVE, seq);
    }
    assert!(
        eventually(|| consumer.available_in_queue() as u64 == LIVE_MESSAGES).await,
        "harness: the live messages must be queued client-side, queued {}",
        consumer.available_in_queue()
    );
    consumer
}

/// Terminal sibling (`MultiTopicsConsumer::receive`): the terminal child is first, so the sweep
/// starts there on every call, its `Err(Closed)` resolves at once, and the cursor is advanced only
/// on `Ok`: the healthy topic holds `LIVE_MESSAGES` messages that are never delivered, however
/// often the caller retries.
#[tokio::test(flavor = "current_thread")]
async fn multi_topics_receive_reaches_the_live_topic_after_a_sibling_child_is_terminal() {
    let broker = FakeBroker::start().await;
    let client = broker.client(2).await;
    let consumer = multi_topics_with_a_terminal_first_child(&broker, &client).await;

    let collected = collect_receives(
        TERMINAL_CALL_BUDGET,
        || async {
            match consumer.receive().await {
                Ok(m) => Outcome::Message {
                    topic: m.topic,
                    seq: m.message.metadata.sequence_id,
                },
                Err(e) => Outcome::Error(e.to_string()),
            }
        },
        |collected| messages_from(collected, LIVE).count() as u64 == LIVE_MESSAGES,
    )
    .await;
    let delivered: Vec<u64> = messages_from(&collected, LIVE).collect();
    assert_eq!(
        delivered,
        (0..LIVE_MESSAGES).collect::<Vec<_>>(),
        "MultiTopicsConsumer::receive never reached `{LIVE}` ({LIVE_MESSAGES} messages queued) \
         within {TERMINAL_CALL_BUDGET} calls once its first child was terminal; the terminal \
         child's error is returned every time and the cursor is not advanced past it. \
         Results, in order: {collected:?}"
    );
}

/// Terminal sibling (`PatternConsumer::receive`): same scenario through the pattern wrapper, which
/// has no cursor at all, so a terminal child ahead of the healthy ones hides every child after it.
#[tokio::test(flavor = "current_thread")]
async fn pattern_receive_reaches_the_live_topic_after_a_sibling_child_is_terminal() {
    let broker = FakeBroker::start().await;
    broker.watch_topics(&[DEAD, LIVE]);
    let client = broker.client(2).await;
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("wrap-fair")
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
    for seq in 0..LIVE_MESSAGES {
        broker.publish(LIVE, seq);
    }
    // Arrival barrier: `select_all` polls every child, so while the live child has nothing queued
    // the terminal child's `Err` wins, and on this single-threaded runtime a `receive()` that
    // resolves on its first poll never yields to the tasks that would deliver the frames. The
    // pattern wrapper exposes no queue depth, but `total_msgs_received` counts a message when it
    // enters the child's receiver queue, not when it is popped.
    assert!(
        eventually(|| consumer.aggregate_stats().total_msgs_received == LIVE_MESSAGES).await,
        "harness: the live messages must be queued client-side, received {}",
        consumer.aggregate_stats().total_msgs_received
    );

    let collected = collect_receives(
        TERMINAL_CALL_BUDGET,
        || async {
            match consumer.receive().await {
                Ok(m) => Outcome::Message {
                    topic: m.topic,
                    seq: m.message.metadata.sequence_id,
                },
                Err(e) => Outcome::Error(e.to_string()),
            }
        },
        |collected| messages_from(collected, LIVE).count() as u64 == LIVE_MESSAGES,
    )
    .await;
    let delivered: Vec<u64> = messages_from(&collected, LIVE).collect();
    assert_eq!(
        delivered,
        (0..LIVE_MESSAGES).collect::<Vec<_>>(),
        "PatternConsumer::receive never reached `{LIVE}` ({LIVE_MESSAGES} messages queued) \
         within {TERMINAL_CALL_BUDGET} calls once its first child was terminal. \
         Results, in order: {collected:?}"
    );
}
