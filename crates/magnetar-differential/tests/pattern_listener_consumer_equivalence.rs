// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d) — a pattern push consumer whose caller holds the consumer delivers a topic
//! created after subscribe, identically on both engines.
//!
//! `PatternConsumerBuilder::subscribe_with_listener` subscribes the `PatternConsumer` inside the
//! call, moves it into the push poller and returns only the `MessageListenerHandle`. Discovery
//! after the first snapshot is driven by the caller (`PatternConsumer::update`, alone or through
//! `start_auto_reconcile`), and only a holder of the consumer can drive it: a caller of that
//! terminal holds nothing to drive it with. `subscribe_with_listener_and_consumer` is the terminal
//! that hands the caller the consumer next to the handle.
//!
//! What this file pins is that the consumer it returns **is the one the listener was registered
//! on**: the topic the caller's `update` subscribes joins the child set the poller drains, so its
//! messages reach the callback. A terminal that returned any other consumer would report the same
//! reconcile and the same topic list, and deliver nothing.
//!
//! The scenario ([`observe`]) is generic over the engine and runs on `PulsarClient<TokioEngine>`
//! and on `PulsarClient<MoonpoolEngine<TokioProviders>>`, each against its own scripted loopback
//! broker. Both must produce the same [`Trace`], which is also the expected value. The Moonpool leg
//! runs on host sockets and not in a seeded simulation, because `spawn_wrapper_message_listener`
//! starts the poller with `tokio::spawn` on both engines and `SimProviders` has no Tokio runtime to
//! host it.
//!
//! * `the_pull_route_with_an_explicit_update_delivers_a_late_topic_on_both_engines` is the control:
//!   the same broker, listener and steps, with the consumer obtained from `subscribe()` and handed
//!   to `spawn_wrapper_message_listener`. It shows the harness, the delta push, the sentinel, the
//!   reconcile and the delivery are sound.
//! * The handle-only terminal is part of the trace for what it does deliver, a topic matched at
//!   subscribe, and for its unchanged return type.
//!
//! No live Pulsar is needed.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::{
    BrokerMetadataApi, Engine, IncomingMessage, MessageListenerHandle, MoonpoolEngine,
    PatternConsumer, PatternConsumerBuilder, PulsarClient, PulsarError, SubscribeApi, TokioEngine,
    WrapperMessageListener, spawn_wrapper_message_listener,
};
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    ConnectionConfig, FrameError, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{
    Client as MoonpoolClient, MoonpoolEngine as MoonpoolRuntimeEngine,
};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
const SUBSCRIPTION: &str = "plc";
/// The pattern of the consumer whose late topic is followed, and the topic its watcher is answered
/// with: matched at subscribe.
const PATTERN: &str = "persistent://public/default/plc-late-.*";
const SEED: &str = "persistent://public/default/plc-late-seed";
/// A topic created after the consumer subscribed. It matches [`PATTERN`].
const LATE: &str = "persistent://public/default/plc-late-created";
/// The pattern and the one topic of the consumer built through the handle-only terminal.
const HANDLE_ONLY_PATTERN: &str = "persistent://public/default/plc-handle-.*";
const HANDLE_ONLY_SEED: &str = "persistent://public/default/plc-handle-seed";

/// How long a step that must happen may take. Generous: a positive check returns as soon as it
/// holds, so this only bounds a failure.
const PATIENCE: Duration = Duration::from_secs(10);

/// What a caller of the pattern listener terminals observes. Identical on both engines.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Trace {
    /// `subscribe_with_listener_and_consumer` on a builder no listener was set on.
    without_listener: &'static str,
    /// What the listener of the handle-only terminal saw: the message of a topic matched at
    /// subscribe.
    handle_only_seen: Vec<(String, u64)>,
    /// What following a late topic through `subscribe_with_listener_and_consumer` gave.
    returned_consumer: LateTopic,
}

/// One consumer and its listener followed from subscribe to a topic created afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LateTopic {
    /// The consumer's topics right after subscribe.
    topics_at_subscribe: Vec<String>,
    /// `(added, removed)` reported by the caller's `update` on the consumer, once the broker
    /// had announced [`LATE`].
    reconciled: (usize, usize),
    /// The consumer's topics after that `update`.
    topics_after_update: Vec<String>,
    /// Every `(topic, entry id)` the listener was handed, in order.
    seen: Vec<(String, u64)>,
}

fn expected_late_topic() -> LateTopic {
    LateTopic {
        topics_at_subscribe: vec![SEED.to_owned()],
        reconciled: (1, 0),
        topics_after_update: vec![SEED.to_owned(), LATE.to_owned()],
        seen: vec![(SEED.to_owned(), 0), (LATE.to_owned(), 1)],
    }
}

fn expected_trace() -> Trace {
    Trace {
        without_listener: "configuration error, nothing sent to the broker",
        handle_only_seen: vec![(HANDLE_ONLY_SEED.to_owned(), 0)],
        returned_consumer: expected_late_topic(),
    }
}

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> topic, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), String>,
    /// topic -> `(connection, consumer id)`, set by the consumer's first `CommandFlow`.
    flowed: HashMap<String, (u64, u64)>,
    /// Entry ids waiting for a flowed consumer on their topic.
    pending: HashMap<String, VecDeque<u64>>,
    /// The topics a `CommandWatchTopicList` is answered with, per `topics_pattern`.
    watch_topics: HashMap<String, Vec<String>>,
    /// `(connection, watcher id)` of the topic-list watcher opened for each `topics_pattern`.
    watchers: HashMap<String, (u64, u64)>,
    /// `CommandPong`s received: the client answered a `CommandPing` of ours.
    pongs: u64,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
    /// Wakes the connection task that owns the connection id.
    wake: HashMap<u64, Arc<Notify>>,
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

    /// Queue `frame` for `conn` and wake its task.
    fn send(&mut self, conn: u64, frame: &[u8]) {
        self.outbox
            .entry(conn)
            .or_default()
            .extend_from_slice(frame);
        if let Some(notify) = self.wake.get(&conn) {
            notify.notify_one();
        }
    }
}

/// A scripted single-broker Pulsar endpoint on a loopback socket: it answers the handshake,
/// lookups, topic-list watches, subscribes and closes, pushes a queued message once its consumer
/// has sent a first `CommandFlow`, and pushes a topic-list delta or a ping when the test says so.
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

    /// The façade client over the tokio engine, one connection to this broker.
    async fn tokio_client(&self) -> PulsarClient<TokioEngine> {
        tokio::time::timeout(
            PATIENCE,
            PulsarClient::builder()
                .service_url(format!("pulsar://{}", self.addr))
                .connections_per_broker(1)
                .build(),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok")
    }

    /// The façade client over the moonpool engine on real host sockets, one connection to this
    /// broker.
    async fn moonpool_client(&self) -> PulsarClient<MoonpoolEngine<TokioProviders>> {
        let runtime_client = tokio::time::timeout(
            PATIENCE,
            MoonpoolClient::connect_plain(
                &MoonpoolRuntimeEngine::new(TokioProviders::new()),
                &self.addr,
                ConnectionConfig::default(),
            ),
        )
        .await
        .expect("connect did not time out")
        .expect("connect ok");
        PulsarClient::from_runtime_client(runtime_client)
    }

    /// Set the topic list a `CommandWatchTopicList` for `pattern` is answered with.
    fn watch_topics(&self, pattern: &str, topics: &[&str]) {
        self.state.lock().watch_topics.insert(
            pattern.to_owned(),
            topics.iter().map(|t| (*t).to_owned()).collect(),
        );
    }

    /// `(connection, watcher id)` of the watcher the client opened for `pattern`.
    fn watcher(&self, pattern: &str) -> (u64, u64) {
        let state = self.state.lock();
        *state.watchers.get(pattern).unwrap_or_else(|| {
            panic!(
                "harness: no watcher is open for {pattern}, open ones: {:?}",
                state.watchers.keys().collect::<Vec<_>>()
            )
        })
    }

    /// Push one `CommandWatchTopicUpdate` addressed to the watcher the client opened for
    /// `pattern`, as the broker does when a topic matching that pattern is created.
    fn push_update(&self, pattern: &str, new_topics: &[&str]) {
        let (conn, watcher_id) = self.watcher(pattern);
        let frame = command_frame(&pb::BaseCommand {
            watch_topic_update: Some(pb::CommandWatchTopicUpdate {
                watcher_id,
                new_topics: new_topics.iter().map(|t| (*t).to_owned()).collect(),
                deleted_topics: Vec::new(),
                topics_hash: "plc".to_owned(),
            }),
            ..base(pb::base_command::Type::WatchTopicUpdate)
        });
        self.state.lock().send(conn, &frame);
    }

    /// Queue the message with entry id `seq` for the consumer of `topic`; it is pushed once that
    /// consumer has sent its first `CommandFlow`.
    fn publish(&self, topic: &str, seq: u64) {
        let mut state = self.state.lock();
        state
            .pending
            .entry(topic.to_owned())
            .or_default()
            .push_back(seq);
        state.pump();
    }

    /// `true` while the client has opened no topic-list watcher and no subscription.
    fn nothing_was_opened(&self) -> bool {
        let state = self.state.lock();
        state.watchers.is_empty() && state.subscribed.is_empty()
    }

    /// Wait until the client has processed every frame pushed so far on the connection of
    /// `pattern`'s watcher. The client's connection task handles frames in order, buffers a
    /// topic-list delta before it writes anything, and answers a `CommandPing` by writing a
    /// `CommandPong`: the pong proves the deltas pushed before the ping are in the client's queue.
    async fn client_has_processed_everything_pushed(&self, pattern: &str) {
        let (conn, _) = self.watcher(pattern);
        let before = self.state.lock().pongs;
        let ping = command_frame(&pb::BaseCommand {
            ping: Some(pb::CommandPing {}),
            ..base(pb::base_command::Type::Ping)
        });
        self.state.lock().send(conn, &ping);
        assert!(
            eventually(|| self.state.lock().pongs > before).await,
            "harness: the client must answer the sentinel ping"
        );
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
        producer_name: "pattern-listener-broker".to_owned(),
        sequence_id: seq,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, b"plain").expect("encode message frame");
    frame
}

/// Handle one decoded client command, queueing the reply in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    use pb::base_command::Type;

    let mut guard = state.lock();
    let Ok(kind) = Type::try_from(cmd.r#type) else {
        return;
    };
    let reply: Option<BytesMut> = match kind {
        Type::Connect => Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "pattern-listener-fake".to_owned(),
                protocol_version: Some(21),
                ..Default::default()
            }),
            ..base(Type::Connected)
        })),
        // Single-broker contract: every lookup resolves to "use the current connection".
        Type::Lookup => cmd.lookup_topic.as_ref().map(|lookup| {
            command_frame(&pb::BaseCommand {
                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                    response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
                    request_id: lookup.request_id,
                    authoritative: Some(true),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                }),
                ..base(Type::LookupResponse)
            })
        }),
        Type::WatchTopicList => cmd.watch_topic_list.as_ref().map(|watch| {
            guard
                .watchers
                .insert(watch.topics_pattern.clone(), (conn, watch.watcher_id));
            let topics = guard
                .watch_topics
                .get(&watch.topics_pattern)
                .cloned()
                .unwrap_or_default();
            command_frame(&pb::BaseCommand {
                watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                    request_id: watch.request_id,
                    watcher_id: watch.watcher_id,
                    topic: topics,
                    topics_hash: "plc".to_owned(),
                }),
                ..base(Type::WatchTopicListSuccess)
            })
        }),
        Type::Subscribe => cmd.subscribe.as_ref().map(|subscribe| {
            guard
                .subscribed
                .insert((conn, subscribe.consumer_id), subscribe.topic.clone());
            success(subscribe.request_id)
        }),
        Type::Flow => {
            if let Some(flow) = cmd.flow.as_ref()
                && let Some(topic) = guard.subscribed.get(&(conn, flow.consumer_id)).cloned()
            {
                guard.flowed.insert(topic, (conn, flow.consumer_id));
                guard.pump();
            }
            None
        }
        Type::CloseConsumer => cmd.close_consumer.as_ref().map(|close| {
            if let Some(topic) = guard.subscribed.remove(&(conn, close.consumer_id)) {
                guard.flowed.remove(&topic);
            }
            success(close.request_id)
        }),
        Type::Ping => Some(command_frame(&pb::BaseCommand {
            pong: Some(pb::CommandPong {}),
            ..base(Type::Pong)
        })),
        Type::Pong => {
            guard.pongs += 1;
            None
        }
        _ => None,
    };
    if let Some(reply) = reply {
        guard.send(conn, &reply);
    }
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

/// Poll `cond` every 10 ms until it holds or [`PATIENCE`] elapses; the verdict is `cond()`.
async fn eventually(mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + PATIENCE;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

/// What a listener was handed: `(topic, entry id)` of every message, in order.
type Seen = Arc<Mutex<Vec<(String, u64)>>>;

fn listener(seen: &Seen) -> WrapperMessageListener {
    let seen = seen.clone();
    Arc::new(move |topic: &str, msg: &IncomingMessage| {
        seen.lock().push((topic.to_owned(), msg.sequence_id()));
    })
}

/// A pattern consumer builder on `pattern`, with every knob the scenarios share.
fn builder<'a, E: Engine>(
    client: &'a PulsarClient<E>,
    pattern: &str,
) -> PatternConsumerBuilder<'a, E> {
    client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(pattern)
        .subscription(SUBSCRIPTION)
        .subscription_type(pb::command_subscribe::SubType::Shared)
}

/// Follow `consumer`, whose listener records into `seen`, from its seed topic to a topic the
/// broker announces afterwards: the caller reconciles with `update`, then a message is published
/// on the new topic. Nothing here asserts what the listener must see; the caller of this function
/// does, from the [`LateTopic`] it returns.
async fn follow_a_late_topic<E>(
    client: &PulsarClient<E>,
    broker: &FakeBroker,
    consumer: &PatternConsumer<<E::ClientState as SubscribeApi>::Consumer>,
    seen: &Seen,
) -> LateTopic
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let topics_at_subscribe = consumer.topics();

    // The listener is live: a message of the initial snapshot is delivered first.
    broker.publish(SEED, 0);
    eventually(|| !seen.lock().is_empty()).await;

    // A topic matching the pattern is created. The sentinel proves the delta is in the client
    // before the caller reconciles.
    broker.push_update(PATTERN, &[LATE]);
    broker.client_has_processed_everything_pushed(PATTERN).await;
    let report = tokio::time::timeout(PATIENCE, consumer.update(client))
        .await
        .expect("harness: update did not hang")
        .expect("harness: update ok");
    let topics_after_update = consumer.topics();

    broker.publish(LATE, 1);
    eventually(|| seen.lock().contains(&(LATE.to_owned(), 1))).await;

    LateTopic {
        topics_at_subscribe,
        reconciled: (report.added, report.removed),
        topics_after_update,
        seen: seen.lock().clone(),
    }
}

/// Run the three listener terminals' steps once on `client`, a fresh client of `broker`.
async fn observe<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> Trace
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone + Send + Sync + 'static,
{
    // No listener set: refused, as the handle-only terminal refuses it, before anything is sent.
    let refused = builder(client, PATTERN)
        .subscribe_with_listener_and_consumer()
        .await;
    let without_listener = match refused {
        Err(PulsarError::Config(_)) if broker.nothing_was_opened() => {
            "configuration error, nothing sent to the broker"
        }
        Err(PulsarError::Config(_)) => "configuration error, after a watcher or a subscription",
        Err(_) => "another error",
        Ok(_) => "accepted",
    };

    // The handle-only terminal: unchanged return type, and a topic matched at subscribe is
    // delivered.
    broker.watch_topics(HANDLE_ONLY_PATTERN, &[HANDLE_ONLY_SEED]);
    let handle_only: Seen = Arc::default();
    let handle: MessageListenerHandle = builder(client, HANDLE_ONLY_PATTERN)
        .message_listener(listener(&handle_only))
        .subscribe_with_listener()
        .await
        .expect("subscribe_with_listener");
    broker.publish(HANDLE_ONLY_SEED, 0);
    eventually(|| !handle_only.lock().is_empty()).await;
    handle.close().await;
    let handle_only_seen = handle_only.lock().clone();

    // The terminal that also returns the consumer: the caller reconciles on what it was handed.
    broker.watch_topics(PATTERN, &[SEED]);
    let seen: Seen = Arc::default();
    let (consumer, handle) = builder(client, PATTERN)
        .message_listener(listener(&seen))
        .subscribe_with_listener_and_consumer()
        .await
        .expect("subscribe_with_listener_and_consumer");
    let returned_consumer = follow_a_late_topic(client, broker, &consumer, &seen).await;
    handle.close().await;

    Trace {
        without_listener,
        handle_only_seen,
        returned_consumer,
    }
}

/// The control: the consumer comes from `subscribe()` and the caller hands a clone of it to
/// `spawn_wrapper_message_listener`, the route that has always left the caller a consumer.
async fn observe_pull_route<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> LateTopic
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone + Send + Sync + 'static,
{
    broker.watch_topics(PATTERN, &[SEED]);
    let seen: Seen = Arc::default();
    let consumer = builder(client, PATTERN)
        .subscribe()
        .await
        .expect("subscribe");
    let handle = spawn_wrapper_message_listener(consumer.clone(), listener(&seen));
    let late_topic = follow_a_late_topic(client, broker, &consumer, &seen).await;
    handle.close().await;
    late_topic
}

async fn tokio_trace() -> Trace {
    let broker = FakeBroker::start().await;
    let client = broker.tokio_client().await;
    tokio::time::timeout(HANG_GUARD, observe(&client, &broker))
        .await
        .expect("the tokio scenario did not hang")
}

async fn moonpool_trace() -> Trace {
    let broker = FakeBroker::start().await;
    let client = broker.moonpool_client().await;
    tokio::time::timeout(HANG_GUARD, observe(&client, &broker))
        .await
        .expect("the moonpool scenario did not hang")
}

/// The delivery claim on one engine's trace: the caller's `update` on the returned consumer
/// subscribed the announced topic, and the message published on it reached the listener.
fn assert_the_late_topic_reached_the_listener(engine: &str, trace: &Trace) {
    let late = &trace.returned_consumer;
    assert_eq!(
        late.reconciled,
        (1, 0),
        "{engine}: `update` on the returned consumer must subscribe the one topic the broker \
         announced, trace {trace:?}"
    );
    assert!(
        late.seen.contains(&(LATE.to_owned(), 1)),
        "{engine}: the message published on {LATE}, a topic the broker announced after subscribe \
         and the caller's `update` on the returned consumer subscribed (reconciled = {:?}, topics \
         of the returned consumer = {:?}), never reached the listener: the consumer \
         `subscribe_with_listener_and_consumer` returned is not the one its listener was \
         registered on (listener saw {:?})",
        late.reconciled,
        late.topics_after_update,
        late.seen
    );
}

#[tokio::test(flavor = "current_thread")]
async fn the_pull_route_with_an_explicit_update_delivers_a_late_topic_on_both_engines() {
    let broker = FakeBroker::start().await;
    let client = broker.tokio_client().await;
    let tokio = tokio::time::timeout(HANG_GUARD, observe_pull_route(&client, &broker))
        .await
        .expect("the tokio control did not hang");

    let broker = FakeBroker::start().await;
    let client = broker.moonpool_client().await;
    let moonpool = tokio::time::timeout(HANG_GUARD, observe_pull_route(&client, &broker))
        .await
        .expect("the moonpool control did not hang");

    assert_eq!(tokio, expected_late_topic(), "control, tokio engine");
    assert_eq!(moonpool, expected_late_topic(), "control, moonpool engine");
}

#[tokio::test(flavor = "current_thread")]
async fn the_returned_consumer_delivers_a_late_topic_on_the_tokio_engine() {
    let trace = tokio_trace().await;
    assert_the_late_topic_reached_the_listener("tokio", &trace);
}

#[tokio::test(flavor = "current_thread")]
async fn the_returned_consumer_delivers_a_late_topic_on_the_moonpool_engine() {
    let trace = moonpool_trace().await;
    assert_the_late_topic_reached_the_listener("moonpool", &trace);
}

#[tokio::test(flavor = "current_thread")]
async fn the_pattern_listener_terminals_agree_across_engines() {
    let tokio = tokio_trace().await;
    let moonpool = moonpool_trace().await;

    assert_eq!(
        tokio, moonpool,
        "the tokio and the moonpool engine must observe the same trace"
    );
    assert_eq!(
        tokio,
        expected_trace(),
        "both engines agree, on a trace that is not what a caller of the pattern listener \
         terminals must observe"
    );
}
