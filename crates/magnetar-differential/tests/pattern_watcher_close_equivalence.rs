// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher the broker acknowledged must be closed at the broker once its last
//! user is gone, identically on both engines (ADR-0024 layer (d)), driven through the public
//! façade.
//!
//! `Connection::watch_topic_list` registers a watcher at the broker for the life of the
//! connection, and nothing ever sends `CommandWatchTopicListClose`. Three façade paths leave one
//! behind, each of them one the application takes in the ordinary course of using the client:
//!
//! - `PulsarClient::topic_list_snapshot`, the "discover the matching topics right now" call: it
//!   returns the topic list and hands the caller nothing that could close the watcher it made;
//! - `PatternConsumer::close`, the only handle on the watcher the pattern consumer's builder made;
//! - dropping the last `PatternConsumer` handle, which is how most consumers end.
//!
//! The broker then pushes a `CommandWatchTopicUpdate` for every matching topic change, and both
//! runtimes queue each one without a bound for as long as the connection lives (and register the
//! watcher again after every reconnect, ADR-0136).
//!
//! The façade is driven against one small in-process fake broker, defined here because the
//! scripted broker of `magnetar-differential` answers no `CommandWatchTopicList`: it completes the
//! handshake, answers a watch with an empty topic list and answers a close, and records the watcher
//! ids of the frames it saw. An empty topic list keeps the scenario about the watcher alone: the
//! pattern consumer has no child to subscribe. The connection-level half (a watch cancelled before
//! its reply, and a delta for a watcher the connection does not track) is
//! `topic_watcher_close_equivalence.rs`.
//!
//! The fake records what reached it, so an absent frame is a wait that ends: the assertions wait on
//! the recorded frames for [`HANG_GUARD`], the anti-hang backstop every differential wait for a
//! frame that must arrive uses (`docs/testing.md`), rather than sleeping a fixed time. A run
//! against code that never sends the close spends that whole minute before it fails, on both
//! engines at once, because the two scenarios of a test run concurrently.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use magnetar::{
    BrokerMetadataApi, Engine, MoonpoolEngine as FacadeMoonpoolEngine, PulsarClient, SubscribeApi,
    TokioEngine,
};
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// The watcher ids of the frames the fake broker has seen, in arrival order.
#[derive(Debug, Default)]
struct Recorded {
    registered: Vec<u64>,
    closed: Vec<u64>,
}

/// An in-process fake broker bound to loopback.
struct FakeBroker {
    addr: String,
    recorded: Arc<Mutex<Recorded>>,
    arrived: Arc<Notify>,
}

impl FakeBroker {
    /// Waits until `count` watchers have been closed or [`HANG_GUARD`] ends, and returns the
    /// watcher ids closed so far.
    async fn closes(&self, count: usize) -> Vec<u64> {
        let deadline = tokio::time::Instant::now() + HANG_GUARD;
        loop {
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            {
                let recorded = self.recorded.lock();
                if recorded.closed.len() >= count {
                    return recorded.closed.clone();
                }
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return self.recorded.lock().closed.clone();
            }
        }
    }

    fn registered(&self) -> Vec<u64> {
        self.recorded.lock().registered.clone()
    }
}

fn reply(command: &pb::BaseCommand) -> BytesMut {
    let mut out = BytesMut::new();
    encode_command(&mut out, command).expect("encode reply");
    out
}

/// Spawns the fake broker: `Connect` -> `Connected`, `Ping` -> `Pong`, `WatchTopicList` ->
/// `WatchTopicListSuccess` with no topics, `WatchTopicListClose` -> `Success`; every other frame is
/// swallowed.
async fn spawn_fake_broker() -> FakeBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let arrived = Arc::new(Notify::new());
    let (recorded_task, arrived_task) = (recorded.clone(), arrived.clone());
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let (recorded, arrived) = (recorded_task.clone(), arrived_task.clone());
            tokio::spawn(async move {
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
                        let command = frame.command;
                        let answer = match pb::base_command::Type::try_from(command.r#type) {
                            Ok(pb::base_command::Type::Connect) => Some(pb::BaseCommand {
                                r#type: pb::base_command::Type::Connected as i32,
                                connected: Some(pb::CommandConnected {
                                    server_version: "magnetar-watcher-close-fake".to_owned(),
                                    protocol_version: Some(21),
                                    max_message_size: Some(5 * 1024 * 1024),
                                    feature_flags: Some(pb::FeatureFlags::default()),
                                }),
                                ..Default::default()
                            }),
                            Ok(pb::base_command::Type::Ping) => Some(pb::BaseCommand {
                                r#type: pb::base_command::Type::Pong as i32,
                                pong: Some(pb::CommandPong {}),
                                ..Default::default()
                            }),
                            Ok(pb::base_command::Type::WatchTopicList) => {
                                command.watch_topic_list.map(|watch| {
                                    recorded.lock().registered.push(watch.watcher_id);
                                    arrived.notify_one();
                                    pb::BaseCommand {
                                        r#type: pb::base_command::Type::WatchTopicListSuccess
                                            as i32,
                                        watch_topic_list_success: Some(
                                            pb::CommandWatchTopicListSuccess {
                                                request_id: watch.request_id,
                                                watcher_id: watch.watcher_id,
                                                topic: Vec::new(),
                                                topics_hash: "fake-hash".to_owned(),
                                            },
                                        ),
                                        ..Default::default()
                                    }
                                })
                            }
                            Ok(pb::base_command::Type::WatchTopicListClose) => {
                                command.watch_topic_list_close.map(|close| {
                                    recorded.lock().closed.push(close.watcher_id);
                                    arrived.notify_one();
                                    pb::BaseCommand {
                                        r#type: pb::base_command::Type::Success as i32,
                                        success: Some(pb::CommandSuccess {
                                            request_id: close.request_id,
                                            schema: None,
                                        }),
                                        ..Default::default()
                                    }
                                })
                            }
                            _ => None,
                        };
                        if let Some(answer) = answer
                            && stream.write_all(&reply(&answer)).await.is_err()
                        {
                            return;
                        }
                    }
                    match stream.read(&mut probe).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => read_buf.extend_from_slice(&probe[..n]),
                    }
                }
            });
        }
    });
    FakeBroker {
        addr,
        recorded,
        arrived,
    }
}

async fn connect_tokio(broker: &FakeBroker) -> PulsarClient<TokioEngine> {
    tokio::time::timeout(
        HANG_GUARD,
        PulsarClient::<TokioEngine>::builder()
            .service_url(format!("pulsar://{}", broker.addr))
            .operation_timeout(HANG_GUARD)
            .build(),
    )
    .await
    .expect("tokio facade connect did not time out")
    .expect("tokio facade connect succeeded")
}

async fn connect_moonpool(
    broker: &FakeBroker,
) -> PulsarClient<FacadeMoonpoolEngine<TokioProviders>> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let runtime_client = tokio::time::timeout(
        HANG_GUARD,
        MoonpoolClient::connect_plain(&engine, &broker.addr, ConnectionConfig::default()),
    )
    .await
    .expect("moonpool facade connect did not time out")
    .expect("moonpool facade connect succeeded");
    PulsarClient::from_runtime_client(runtime_client)
}

/// What a scenario leaves at the broker, compared across the engines.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Watcher ids the broker was asked to register.
    registered: Vec<u64>,
    /// Watcher ids the broker was asked to close, after the scenario ended its last user.
    closed: Vec<u64>,
}

/// Builds a pattern consumer over the fake broker. The fake returns no topic, so the consumer has
/// no child, and the only thing it owns at the broker is its watcher.
async fn build_pattern_consumer<E>(
    client: &PulsarClient<E>,
    broker: &FakeBroker,
) -> magnetar::PatternConsumer<<E::ClientState as SubscribeApi>::Consumer>
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("watcher-close-sub")
        .subscribe()
        .await
        .expect("pattern consumer subscribe");
    assert!(consumer.is_empty(), "the fake returns no matching topic");
    // Control: the watcher is registered, so a missing close is a missing close and not a
    // registration that never happened.
    assert_eq!(
        broker.registered().len(),
        1,
        "the pattern consumer registers exactly one watcher"
    );
    consumer
}

/// `topic_list_snapshot` registers a watcher, returns the topic list and hands back nothing that
/// could close it.
async fn snapshot_scenario<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> Observed
where
    E: Engine,
    E::ClientState: BrokerMetadataApi,
{
    let topics = client
        .topic_list_snapshot(NAMESPACE, PATTERN)
        .await
        .expect("topic_list_snapshot");
    assert!(topics.is_empty(), "the fake returns no matching topic");
    assert_eq!(
        broker.registered().len(),
        1,
        "the snapshot registers exactly one watcher"
    );
    Observed {
        registered: broker.registered(),
        closed: broker.closes(1).await,
    }
}

/// `PatternConsumer::close` on the only handle ends the consumer.
async fn close_scenario<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> Observed
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let consumer = build_pattern_consumer(client, broker).await;
    consumer.close().await.expect("pattern consumer close");
    Observed {
        registered: broker.registered(),
        closed: broker.closes(1).await,
    }
}

/// Dropping the only `PatternConsumer` handle ends the consumer.
async fn drop_scenario<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> Observed
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let consumer = build_pattern_consumer(client, broker).await;
    drop(consumer);
    Observed {
        registered: broker.registered(),
        closed: broker.closes(1).await,
    }
}

/// The two engines agree, and what they agree on is the close of the one watcher that was
/// registered.
fn assert_watcher_closed(what: &str, tokio: &Observed, moonpool: &Observed) {
    assert_eq!(tokio, moonpool, "engines must agree: {what}");
    assert_eq!(
        tokio.closed, tokio.registered,
        "{what}: the broker must be told to close the one watcher it registered \
         (CommandWatchTopicListClose) within {HANG_GUARD:?}, but it saw {tokio:?}"
    );
}

/// Runs `scenario` on both engines at once, each against its own fake broker, so a run that has to
/// wait out [`HANG_GUARD`] waits it out once and not once per engine.
macro_rules! on_both_engines {
    ($scenario:ident) => {{
        let on_tokio = async {
            let broker = spawn_fake_broker().await;
            let client = connect_tokio(&broker).await;
            $scenario(&client, &broker).await
        };
        let on_moonpool = async {
            let broker = spawn_fake_broker().await;
            let client = connect_moonpool(&broker).await;
            $scenario(&client, &broker).await
        };
        tokio::join!(on_tokio, on_moonpool)
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_closes_its_watcher_on_both_engines() {
    let (tokio, moonpool) = on_both_engines!(snapshot_scenario);
    assert_watcher_closed("topic_list_snapshot", &tokio, &moonpool);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_a_pattern_consumer_closes_its_watcher_on_both_engines() {
    let (tokio, moonpool) = on_both_engines!(close_scenario);
    assert_watcher_closed("PatternConsumer::close", &tokio, &moonpool);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_pattern_consumer_closes_its_watcher_on_both_engines() {
    let (tokio, moonpool) = on_both_engines!(drop_scenario);
    assert_watcher_closed("dropping the last PatternConsumer", &tokio, &moonpool);
}

/// What a live pattern consumer leaves at the broker. The fence snapshot below is a watcher of its
/// own, which a fix may close at once, so only the consumer's watcher is projected.
#[derive(Debug, PartialEq, Eq)]
struct LiveObserved {
    /// Watchers registered: the consumer's, then the fence's.
    registrations: usize,
    /// Whether the consumer's own watcher has been closed while the consumer is held.
    consumer_watcher_closed: bool,
}

async fn live_scenario<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> LiveObserved
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let consumer = build_pattern_consumer(client, broker).await;
    let consumer_watcher = broker.registered()[0];
    // Fence: a request the fake answers after every frame the consumer staged, so a close sent
    // while the consumer is held would already have been recorded when it returns.
    client
        .topic_list_snapshot(NAMESPACE, "persistent://public/default/other-.*")
        .await
        .expect("fence snapshot");
    let observed = LiveObserved {
        registrations: broker.registered().len(),
        consumer_watcher_closed: broker.recorded.lock().closed.contains(&consumer_watcher),
    };
    // Held to here: the consumer is live while the broker is asked what it has seen.
    drop(consumer);
    observed
}

/// Control for the three tests above: the façade, the fake broker and the registration frame work,
/// and a pattern consumer that is still held has its watcher registered and not closed. It passes
/// today and keeps passing after the fix: a watcher must not be closed while its consumer is live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_pattern_consumer_keeps_its_watcher_registered_on_both_engines() {
    let (tokio, moonpool) = on_both_engines!(live_scenario);
    assert_eq!(tokio, moonpool, "engines must agree on a live consumer");
    assert_eq!(
        tokio,
        LiveObserved {
            registrations: 2,
            consumer_watcher_closed: false,
        },
        "the consumer's watcher stays registered while the consumer is held"
    );
}

/// Control for the fake broker itself: a raw peer that sends the close frame the façade is expected
/// to send is recorded and answered, so the three tests above cannot stay red because the fake
/// drops a `CommandWatchTopicListClose`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fake_broker_records_and_answers_a_watcher_close() {
    let broker = spawn_fake_broker().await;
    let mut stream = tokio::net::TcpStream::connect(&broker.addr)
        .await
        .expect("connect to the fake broker");

    let sent = vec![
        pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(pb::CommandConnect {
                client_version: "magnetar-watcher-close-control".to_owned(),
                protocol_version: Some(21),
                ..Default::default()
            }),
            ..Default::default()
        },
        pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicList as i32,
            watch_topic_list: Some(pb::CommandWatchTopicList {
                request_id: 1,
                watcher_id: 7,
                namespace: NAMESPACE.to_owned(),
                topics_pattern: PATTERN.to_owned(),
                topics_hash: None,
            }),
            ..Default::default()
        },
        pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicListClose as i32,
            watch_topic_list_close: Some(pb::CommandWatchTopicListClose {
                request_id: 2,
                watcher_id: 7,
            }),
            ..Default::default()
        },
    ];
    for frame in &sent {
        stream
            .write_all(&reply(frame))
            .await
            .expect("write a frame to the fake broker");
    }

    // Read until the `Success` for the close (request 2) arrives: Connected, the watch success and
    // the close success, in that order.
    let mut read_buf = BytesMut::new();
    let mut probe = [0u8; 8 * 1024];
    let mut answered = Vec::new();
    while answered.len() < 3 {
        loop {
            let mut pending = Bytes::copy_from_slice(&read_buf);
            let before = pending.len();
            match decode_one(&mut pending) {
                Ok(frame) => {
                    let _ = read_buf.split_to(before - pending.len());
                    answered.push(frame.command);
                }
                Err(FrameError::Incomplete { .. }) => break,
                Err(err) => panic!("the fake broker wrote an undecodable frame: {err:?}"),
            }
        }
        if answered.len() < 3 {
            let n = tokio::time::timeout(HANG_GUARD, stream.read(&mut probe))
                .await
                .expect("the fake broker answered within the window")
                .expect("read from the fake broker");
            assert_ne!(n, 0, "the fake broker closed the connection early");
            read_buf.extend_from_slice(&probe[..n]);
        }
    }
    assert_eq!(
        answered
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>(),
        vec![
            pb::base_command::Type::Connected as i32,
            pb::base_command::Type::WatchTopicListSuccess as i32,
            pb::base_command::Type::Success as i32,
        ]
    );
    assert_eq!(broker.registered(), vec![7]);
    assert_eq!(broker.closes(1).await, vec![7]);
}
