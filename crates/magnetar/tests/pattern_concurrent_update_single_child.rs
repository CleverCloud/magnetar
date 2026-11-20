// SPDX-License-Identifier: Apache-2.0

//! Two `PatternConsumer::update` calls that overlap must register a topic once.
//!
//! `update` is `&self` and the consumer is `Clone`, and `start_auto_reconcile` runs `update` on a
//! task while the caller may run it too, so two calls can be in flight at once. A topic the
//! broker announced twice (two deltas, e.g. a delete-and-recreate inside the subscribe window)
//! can then reach the second call while the first call's subscribe for it is still awaiting the
//! broker. A step that checked the set before the awaited subscribe and pushed the new child after
//! it, with no second look, would register a second child for the same topic: two live consumers
//! on one subscription, each holding its own receiver queue, the second one unreachable by
//! `ack(topic, …)` (which resolves the first).
//!
//! `update` closes that by serialising calls: it holds one async lock from the moment it reads the
//! connection's deltas until it returns, so the second call cannot read the duplicate until the
//! first has put its child in the set, and then finds the topic already there. This file pins that
//! behaviour on both engines against a scripted loopback broker that holds the `CommandSubscribe`
//! reply of the announced topic until the test releases it.

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
const PATTERN: &str = "persistent://public/default/dup-.*";
const SEED: &str = "persistent://public/default/dup-seed";
/// The topic the broker announces twice; its subscribe is held until the test answers it.
const DUP: &str = "persistent://public/default/dup-twice";
const DEADLINE: Duration = Duration::from_secs(3);
/// How long the second `update` is given to run against the held subscribe before the test
/// answers it. A call that would register a second child sends its own `CommandSubscribe` well
/// inside this; the serialised call is parked on the lock for all of it.
const SETTLE: Duration = Duration::from_millis(300);

/// A `CommandSubscribe` the broker has received and not answered yet.
struct HeldSubscribe {
    conn: u64,
    request_id: u64,
    consumer_id: u64,
    topic: String,
    subscription: String,
}

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> `(topic, subscription name)`, set once a subscribe is
    /// answered.
    subscribed: HashMap<(u64, u64), (String, String)>,
    /// Subscribes of `DUP` that arrived while the broker holds them.
    held: Vec<HeldSubscribe>,
    /// `true` while a `CommandSubscribe` for `DUP` is held instead of answered.
    holding: bool,
    /// The topics a `CommandWatchTopicList` is answered with.
    watch_topics: Vec<String>,
    /// `(connection, watcher id)` of the topic-list watcher the client opened.
    watcher: Option<(u64, u64)>,
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

    fn watch_topics(&self, topics: &[&str]) {
        self.state.lock().watch_topics = topics.iter().map(|t| (*t).to_owned()).collect();
    }

    /// From now on a `CommandSubscribe` for `DUP` is held instead of answered.
    fn hold_dup_subscribes(&self) {
        self.state.lock().holding = true;
    }

    /// How many `CommandSubscribe`s for `DUP` the broker is holding.
    fn held_subscribes(&self) -> usize {
        self.state.lock().held.len()
    }

    /// Answer every held subscribe with success, as the broker would once the topic is loaded.
    /// The held replies are answered in arrival order.
    fn answer_held_subscribes(&self) {
        let mut state = self.state.lock();
        let held = std::mem::take(&mut state.held);
        for HeldSubscribe {
            conn,
            request_id,
            consumer_id,
            topic,
            subscription,
        } in held
        {
            state
                .subscribed
                .insert((conn, consumer_id), (topic, subscription));
            let frame = command_frame(&pb::BaseCommand {
                success: Some(pb::CommandSuccess {
                    request_id,
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::Success)
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
    }

    /// Push one `CommandWatchTopicUpdate` to the watcher the client opened.
    fn push_update(&self, new_topics: &[&str]) {
        let mut state = self.state.lock();
        let (conn, watcher_id) = state.watcher.expect("harness: a watcher is open");
        let frame = command_frame(&pb::BaseCommand {
            watch_topic_update: Some(pb::CommandWatchTopicUpdate {
                watcher_id,
                new_topics: new_topics.iter().map(|t| (*t).to_owned()).collect(),
                deleted_topics: Vec::new(),
                topics_hash: "dup".to_owned(),
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

    /// The subscription names the broker holds on `topic`, one per child consumer.
    fn subscriptions_on(&self, topic: &str) -> Vec<String> {
        self.state
            .lock()
            .subscribed
            .values()
            .filter(|(subscribed_topic, _)| subscribed_topic == topic)
            .map(|(_, name)| name.clone())
            .collect()
    }

    /// Wait until the client has processed every frame pushed so far (the client's connection
    /// task handles frames in order and buffers a delta before it answers a `CommandPing`).
    async fn client_has_processed_everything_pushed(&self) {
        let before = self.state.lock().pongs;
        let conn = self
            .state
            .lock()
            .watcher
            .expect("harness: a watcher is open")
            .0;
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
                server_version: "pattern-concurrent-update-fake".to_owned(),
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
            guard.watcher = Some((conn, watch.watcher_id));
            command_frame(&pb::BaseCommand {
                watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                    request_id: watch.request_id,
                    watcher_id: watch.watcher_id,
                    topic: guard.watch_topics.clone(),
                    topics_hash: "dup".to_owned(),
                }),
                ..base(pb::base_command::Type::WatchTopicListSuccess)
            })
        })
    } else if ty == pb::base_command::Type::Subscribe as i32 {
        cmd.subscribe.as_ref().and_then(|subscribe| {
            if guard.holding && subscribe.topic == DUP {
                guard.held.push(HeldSubscribe {
                    conn,
                    request_id: subscribe.request_id,
                    consumer_id: subscribe.consumer_id,
                    topic: subscribe.topic.clone(),
                    subscription: subscribe.subscription.clone(),
                });
                return None;
            }
            guard.subscribed.insert(
                (conn, subscribe.consumer_id),
                (subscribe.topic.clone(), subscribe.subscription.clone()),
            );
            Some(command_frame(&pb::BaseCommand {
                success: Some(pb::CommandSuccess {
                    request_id: subscribe.request_id,
                    ..Default::default()
                }),
                ..base(pb::base_command::Type::Success)
            }))
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

/// The scenario, written once and expanded over each engine. The expansion site provides
/// `connect(&FakeBroker)`, which returns a façade client of that engine on one connection.
macro_rules! scenarios {
    () => {
        /// The first `update` is parked on the broker's answer to the subscribe of `DUP`. The
        /// broker announces `DUP` again and a second `update` runs against it. It must not
        /// subscribe a second child for the topic: the broker holds one subscribe of `DUP`
        /// while the second call runs, and when both are answered the consumer holds `DUP`
        /// once, with one subscription on the broker.
        #[tokio::test(flavor = "current_thread")]
        async fn overlapping_updates_register_a_duplicate_announcement_once() {
            let broker = FakeBroker::start().await;
            let client = connect(&broker).await;
            broker.watch_topics(&[SEED]);
            let consumer = client
                .pattern_consumer()
                .namespace(NAMESPACE)
                .pattern(PATTERN)
                .subscription("dup")
                .subscription_type(SubType::Shared)
                .subscribe()
                .await
                .expect("subscribe");
            assert_eq!(
                consumer.topics(),
                [SEED],
                "harness: the seed is the snapshot"
            );

            broker.hold_dup_subscribes();
            broker.push_update(&[DUP]);
            broker.client_has_processed_everything_pushed().await;

            let first = tokio::time::timeout(DEADLINE, consumer.update(&client));
            let second = async {
                // The first call has read the first announcement and is parked on its subscribe.
                assert!(
                    eventually(|| broker.held_subscribes() == 1).await,
                    "harness: the first update must be parked on the subscribe of DUP"
                );
                broker.push_update(&[DUP]);
                broker.client_has_processed_everything_pushed().await;
                tokio::time::timeout(DEADLINE, consumer.update(&client)).await
            };
            let release = async {
                assert!(
                    eventually(|| broker.held_subscribes() >= 1).await,
                    "harness: a subscribe of DUP must reach the broker"
                );
                // Give the second call time to run against the held subscribe, then answer.
                tokio::time::sleep(SETTLE * 2).await;
                let held_while_second_ran = broker.held_subscribes();
                broker.answer_held_subscribes();
                // A call that sent a subscribe of its own after the answer is answered as well.
                tokio::time::sleep(SETTLE).await;
                broker.answer_held_subscribes();
                held_while_second_ran
            };
            let (first, second, held_while_second_ran) = tokio::join!(first, second, release);
            let first = first.expect("first update did not hang").expect("first update ok");
            let second = second.expect("second update did not hang").expect("second update ok");

            assert_eq!(
                held_while_second_ran, 1,
                "the second update sent its own subscribe of DUP while the first was still \
                 awaiting the broker's answer to its own"
            );
            assert_eq!(
                consumer.topics().iter().filter(|t| *t == DUP).count(),
                1,
                "DUP was registered more than once: {:?} (first {first:?}, second {second:?})",
                consumer.topics()
            );
            assert_eq!(
                broker.subscriptions_on(DUP).len(),
                1,
                "the broker holds more than one subscription on DUP: the second update opened a \
                 second child for a topic the first was already subscribing (first {first:?}, \
                 second {second:?})"
            );
            assert_eq!(
                first.added + second.added,
                1,
                "exactly one of the two updates added DUP (first {first:?}, second {second:?})"
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
