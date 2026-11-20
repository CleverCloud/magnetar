// SPDX-License-Identifier: Apache-2.0

//! Two pattern consumers on one client must each see only the PIP-145 deltas of their own
//! watcher.
//!
//! Every `PatternConsumerBuilder::subscribe` registers its own broker-side topic-list watcher
//! (its own `watcher_id`) on the client's connection. The broker pushes a
//! `CommandWatchTopicUpdate` addressed to one of them when a topic of that watcher's pattern is
//! created or deleted. The client strips the `watcher_id` when it turns the frame into a
//! `TopicListChanged` event, and the driver appends every such event to ONE queue per client.
//! `PatternConsumer::update` pops that queue empty and subscribes its own template to every topic
//! it finds there, so:
//!
//! * the consumer that calls `update` first applies a delta that belongs to the other watcher: it
//!   opens a durable subscription of its own on a topic outside its pattern;
//! * the owner of that delta never sees it: its `update` finds the queue empty, so its topic set
//!   permanently misses a topic the broker announced for it (discovery is delta-only after the
//!   first snapshot, and the broker announces a topic once).
//!
//! The scenarios run against a real `PulsarClient` speaking to a scripted loopback broker (the
//! harness of `pattern_update_keeps_deltas.rs`, extended to several watchers, each answered with
//! its own topic set and addressed by its own pattern). The reconcile path is engine-generic
//! façade code but the queue is filled by each runtime's own driver, so the same scenarios run
//! over the tokio engine and, with the `moonpool` feature, over the moonpool engine on real host
//! sockets.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
/// The pattern of the first consumer, and the topics its watcher is answered with.
const PATTERN_A: &str = "persistent://public/default/iso-a-.*";
const SEED_A: &str = "persistent://public/default/iso-a-seed";
/// The pattern of the second consumer. `NEW_B` matches it and does not match `PATTERN_A`.
const PATTERN_B: &str = "persistent://public/default/iso-b-.*";
const SEED_B: &str = "persistent://public/default/iso-b-seed";
const NEW_B: &str = "persistent://public/default/iso-b-new";
const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> `(topic, subscription name)`, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), (String, String)>,
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
    async fn tokio_client(&self) -> PulsarClient {
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

    /// The façade client over the moonpool engine on real host sockets, one connection to this
    /// broker.
    #[cfg(feature = "moonpool")]
    async fn moonpool_client(
        &self,
    ) -> PulsarClient<magnetar::MoonpoolEngine<moonpool_core::TokioProviders>> {
        use magnetar::runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
        use moonpool_core::TokioProviders;

        let runtime_client = tokio::time::timeout(
            DEADLINE,
            MoonpoolClient::connect_plain(
                &MoonpoolEngine::new(TokioProviders::new()),
                &self.addr,
                magnetar_proto::ConnectionConfig::default(),
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

    /// Push one `CommandWatchTopicUpdate` addressed to the watcher the client opened for
    /// `pattern`, as the broker does when a topic matching that pattern is created or deleted.
    fn push_update(&self, pattern: &str, new_topics: &[&str], deleted_topics: &[&str]) {
        let mut state = self.state.lock();
        let (conn, watcher_id) = *state.watchers.get(pattern).unwrap_or_else(|| {
            panic!(
                "harness: no watcher is open for {pattern}, open ones: {:?}",
                state.watchers.keys().collect::<Vec<_>>()
            )
        });
        let frame = command_frame(&pb::BaseCommand {
            watch_topic_update: Some(pb::CommandWatchTopicUpdate {
                watcher_id,
                new_topics: new_topics.iter().map(|t| (*t).to_owned()).collect(),
                deleted_topics: deleted_topics.iter().map(|t| (*t).to_owned()).collect(),
                topics_hash: "iso".to_owned(),
            }),
            ..base(pb::base_command::Type::WatchTopicUpdate)
        });
        state
            .outbox
            .entry(conn)
            .or_default()
            .extend_from_slice(&frame);
        if let Some(notify) = state.wake.get(&conn) {
            notify.notify_one();
        }
    }

    /// The subscription names the broker holds on `topic`, whichever consumer opened them.
    fn subscriptions_on(&self, topic: &str) -> Vec<String> {
        self.state
            .lock()
            .subscribed
            .values()
            .filter(|(subscribed_topic, _)| subscribed_topic == topic)
            .map(|(_, name)| name.clone())
            .collect()
    }

    /// Wait until the client has processed every frame pushed so far. The client's connection
    /// task handles frames in order, buffers a topic-list delta before it writes anything, and
    /// answers a `CommandPing` by writing a `CommandPong`: the pong proves the deltas pushed
    /// before the ping are in the client's queue.
    async fn client_has_processed_everything_pushed(&self) {
        let before = self.state.lock().pongs;
        let conn = self
            .state
            .lock()
            .watchers
            .values()
            .map(|(conn, _)| *conn)
            .next()
            .expect("harness: a watcher is open");
        {
            let mut state = self.state.lock();
            state
                .outbox
                .entry(conn)
                .or_default()
                .extend_from_slice(&command_frame(&pb::BaseCommand {
                    ping: Some(pb::CommandPing {}),
                    ..base(pb::base_command::Type::Ping)
                }));
            if let Some(notify) = state.wake.get(&conn) {
                notify.notify_one();
            }
        }
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

/// Handle one decoded client command, queueing replies in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    let mut guard = state.lock();
    let ty = cmd.r#type;
    let reply: Option<BytesMut> = if ty == pb::base_command::Type::Connect as i32 {
        Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "pattern-isolation-fake".to_owned(),
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
                    topics_hash: "iso".to_owned(),
                }),
                ..base(pb::base_command::Type::WatchTopicListSuccess)
            })
        })
    } else if ty == pb::base_command::Type::Subscribe as i32 {
        cmd.subscribe.as_ref().map(|subscribe| {
            guard.subscribed.insert(
                (conn, subscribe.consumer_id),
                (subscribe.topic.clone(), subscribe.subscription.clone()),
            );
            command_frame(&pb::BaseCommand {
                success: Some(pb::CommandSuccess {
                    request_id: subscribe.request_id,
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::Success)
            })
        })
    } else if ty == pb::base_command::Type::CloseConsumer as i32 {
        cmd.close_consumer.as_ref().map(|close| {
            guard.subscribed.remove(&(conn, close.consumer_id));
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
    } else if ty == pb::base_command::Type::Pong as i32 {
        guard.pongs += 1;
        None
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

/// The scenarios, written once and expanded over each engine. The expansion site provides
/// `connect(&FakeBroker)`, which returns a façade client of that engine on one connection.
macro_rules! scenarios {
    () => {
        /// Open a pattern consumer over `$pattern` on `$client`, under the subscription `$subscription`.
        macro_rules! open {
            ($client:expr, $pattern:expr, $subscription:expr) => {
                $client
                    .pattern_consumer()
                    .namespace(NAMESPACE)
                    .pattern($pattern)
                    .subscription($subscription)
                    .subscription_type(SubType::Shared)
                    .subscribe()
                    .await
                    .expect("subscribe")
            };
        }

        /// Control (passes with and without the fix): a consumer applies the delta of its own
        /// watcher when it is the only one; it shows that the harness, the push path, the
        /// sentinel and the reconcile are sound before the scenarios below rely on them.
        #[tokio::test(flavor = "current_thread")]
        async fn a_lone_consumer_applies_the_delta_of_its_own_watcher() {
            let broker = FakeBroker::start().await;
            let client = connect(&broker).await;
            broker.watch_topics(PATTERN_B, &[SEED_B]);
            let only = open!(client, PATTERN_B, "iso-b");
            assert_eq!(only.topics(), [SEED_B], "harness: the seed is the snapshot");

            broker.push_update(PATTERN_B, &[NEW_B], &[]);
            broker.client_has_processed_everything_pushed().await;
            let report = tokio::time::timeout(DEADLINE, only.update(&client))
                .await
                .expect("update did not hang")
                .expect("update ok");

            assert_eq!(report.added, 1, "harness: one topic was announced");
            assert!(
                only.topics().iter().any(|t| t == NEW_B),
                "topics: {:?}",
                only.topics()
            );
        }

        /// A delta the broker addressed to the watcher of `PATTERN_B` must not be applied by the
        /// consumer of `PATTERN_A`. Both consumers share the client; the delta names a topic that
        /// matches only `PATTERN_B`. Fails if the first consumer's `update` pops it from the
        /// client-wide queue and subscribes its own template (its own subscription name) to it.
        #[tokio::test(flavor = "current_thread")]
        async fn update_does_not_apply_a_delta_addressed_to_another_watcher() {
            let broker = FakeBroker::start().await;
            let client = connect(&broker).await;
            broker.watch_topics(PATTERN_A, &[SEED_A]);
            broker.watch_topics(PATTERN_B, &[SEED_B]);
            let first = open!(client, PATTERN_A, "iso-a");
            let _second = open!(client, PATTERN_B, "iso-b");
            assert_eq!(
                first.topics(),
                [SEED_A],
                "harness: the seed is the snapshot"
            );

            broker.push_update(PATTERN_B, &[NEW_B], &[]);
            broker.client_has_processed_everything_pushed().await;
            let report = tokio::time::timeout(DEADLINE, first.update(&client))
                .await
                .expect("update did not hang")
                .expect("update ok");

            assert!(
                !first.topics().iter().any(|t| t == NEW_B),
                "the consumer of {PATTERN_A} subscribed {NEW_B}, a topic the broker announced to \
                 the watcher of {PATTERN_B} only: its update popped the other watcher's delta \
                 from the client-wide queue (report {report:?}, topics {:?}, broker-side \
                 subscriptions on the topic {:?})",
                first.topics(),
                broker.subscriptions_on(NEW_B)
            );
        }

        /// The owner of a delta must receive it, whichever consumer calls `update` first. The
        /// delta is addressed to the watcher of `PATTERN_B`; the consumer of `PATTERN_A` runs its
        /// `update` first (its periodic reconcile fires before the other's), then the owner runs
        /// its own. Fails if the first call has already emptied the queue: the owner then finds
        /// nothing, and `NEW_B` is missing from its set for good, since the broker announces a
        /// topic once.
        #[tokio::test(flavor = "current_thread")]
        async fn the_owner_of_a_delta_still_receives_it_after_another_consumer_updated() {
            let broker = FakeBroker::start().await;
            let client = connect(&broker).await;
            broker.watch_topics(PATTERN_A, &[SEED_A]);
            broker.watch_topics(PATTERN_B, &[SEED_B]);
            let first = open!(client, PATTERN_A, "iso-a");
            let second = open!(client, PATTERN_B, "iso-b");

            broker.push_update(PATTERN_B, &[NEW_B], &[]);
            broker.client_has_processed_everything_pushed().await;
            let _ = tokio::time::timeout(DEADLINE, first.update(&client))
                .await
                .expect("update did not hang");
            let report = tokio::time::timeout(DEADLINE, second.update(&client))
                .await
                .expect("update did not hang")
                .expect("update ok");

            assert!(
                second.topics().iter().any(|t| t == NEW_B) && report.added == 1,
                "the consumer of {PATTERN_B} never received the delta the broker addressed to \
                 its own watcher: the other consumer's update had already popped it from the \
                 client-wide queue (report {report:?}, topics {:?})",
                second.topics()
            );
        }
    };
}

mod tokio_engine {
    use super::*;

    async fn connect(broker: &FakeBroker) -> PulsarClient {
        broker.tokio_client().await
    }

    scenarios!();
}

#[cfg(feature = "moonpool")]
mod moonpool_engine {
    use super::*;

    async fn connect(
        broker: &FakeBroker,
    ) -> PulsarClient<magnetar::MoonpoolEngine<moonpool_core::TokioProviders>> {
        broker.moonpool_client().await
    }

    scenarios!();
}
