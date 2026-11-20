// SPDX-License-Identifier: Apache-2.0

//! `PatternConsumer::update` must apply the broker's topic-list deltas in the order the broker
//! sent them, and must not lose a delta it did not get to apply.
//!
//! `update` moves every pending `WatchTopicUpdate` delta out of the shared queue into locals
//! before it applies any of them. That has two consequences, both pinned here:
//!
//! * a subscribe that fails, or an `update` future that is dropped while a subscribe is in flight,
//!   discards every topic that was drained but not yet subscribed. The queue is empty on the next
//!   call, the discovery is delta-only after the first snapshot, and nothing ever resyncs, so those
//!   topics are lost for the life of the consumer;
//! * the deltas are merged into one added list and one removed list, with the removals applied
//!   first. A topic created and then deleted between two ticks is therefore subscribed after the
//!   broker reported it deleted.
//!
//! The scenarios run against a real `PulsarClient` on the real tokio runtime, speaking to a
//! scripted loopback broker (the harness of `wrapper_terminal_sibling.rs`, reduced to what the
//! discovery path needs and extended with refused and held subscribes and pushed
//! `CommandWatchTopicUpdate` frames). The reconcile path is engine-generic façade code, so one
//! tokio-driven harness covers both engines.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{PatternConsumer, PulsarClient};
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/upd-.*";
const SEED: &str = "persistent://public/default/upd-seed";
const BAD: &str = "persistent://public/default/upd-bad";
const GOOD: &str = "persistent://public/default/upd-good";
const HELD: &str = "persistent://public/default/upd-held";
const FLEETING: &str = "persistent://public/default/upd-fleeting";
const FRESH: &str = "persistent://public/default/upd-fresh";
const DEADLINE: Duration = Duration::from_secs(3);
/// How long an `update` parked on a subscribe the broker never answers is given before the test
/// drops it. Nothing can complete that call, so this only needs to outlast one poll.
const PARKED: Duration = Duration::from_millis(300);

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> topic, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), String>,
    /// Topics whose `CommandSubscribe` is answered with `CommandError`.
    denied: HashSet<String>,
    /// Topics whose `CommandSubscribe` is never answered.
    held: HashSet<String>,
    /// The topics every `CommandWatchTopicList` is answered with.
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

    /// Set the topic list every `CommandWatchTopicList` is answered with.
    fn watch_topics(&self, topics: &[&str]) {
        self.state.lock().watch_topics = topics.iter().map(|t| (*t).to_owned()).collect();
    }

    fn deny_subscribe(&self, topic: &str) {
        self.state.lock().denied.insert(topic.to_owned());
    }

    fn allow_subscribe(&self, topic: &str) {
        self.state.lock().denied.remove(topic);
    }

    fn hold_subscribe(&self, topic: &str) {
        self.state.lock().held.insert(topic.to_owned());
    }

    fn release_subscribe(&self, topic: &str) {
        self.state.lock().held.remove(topic);
    }

    fn queue(&self, frame: &BytesMut) {
        let mut state = self.state.lock();
        let (conn, _) = state.watcher.expect("harness: the watcher is open");
        state
            .outbox
            .entry(conn)
            .or_default()
            .extend_from_slice(frame);
        if let Some(notify) = state.wake.get(&conn) {
            notify.notify_one();
        }
    }

    /// Push one `CommandWatchTopicUpdate` delta to the client, as the broker does when a topic of
    /// the watched namespace is created or deleted.
    fn push_update(&self, new_topics: &[&str], deleted_topics: &[&str]) {
        let (_, watcher_id) = self
            .state
            .lock()
            .watcher
            .expect("harness: the watcher is open");
        self.queue(&command_frame(&pb::BaseCommand {
            watch_topic_update: Some(pb::CommandWatchTopicUpdate {
                watcher_id,
                new_topics: new_topics.iter().map(|t| (*t).to_owned()).collect(),
                deleted_topics: deleted_topics.iter().map(|t| (*t).to_owned()).collect(),
                topics_hash: "upd".to_owned(),
            }),
            ..base(pb::base_command::Type::WatchTopicUpdate)
        }));
    }

    /// Wait until the client has processed every frame pushed so far. The client's connection
    /// task handles frames in order, buffers a topic-list delta before it writes anything, and
    /// answers a `CommandPing` by writing a `CommandPong`: the pong proves the deltas pushed
    /// before the ping are in the client's queue.
    async fn client_has_processed_everything_pushed(&self) {
        let before = self.state.lock().pongs;
        self.queue(&command_frame(&pb::BaseCommand {
            ping: Some(pb::CommandPing {}),
            ..base(pb::base_command::Type::Ping)
        }));
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
                server_version: "pattern-update-fake".to_owned(),
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
                    topics_hash: "upd".to_owned(),
                }),
                ..base(pb::base_command::Type::WatchTopicListSuccess)
            })
        })
    } else if ty == pb::base_command::Type::Subscribe as i32 {
        cmd.subscribe.as_ref().and_then(|subscribe| {
            if guard.held.contains(&subscribe.topic) {
                return None;
            }
            if guard.denied.contains(&subscribe.topic) {
                return Some(command_frame(&pb::BaseCommand {
                    error: Some(pb::CommandError {
                        request_id: subscribe.request_id,
                        error: pb::ServerError::AuthorizationError as i32,
                        message: "subscribe refused by the fake broker".to_owned(),
                    }),
                    ..base(pb::base_command::Type::Error)
                }));
            }
            guard
                .subscribed
                .insert((conn, subscribe.consumer_id), subscribe.topic.clone());
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

/// A pattern consumer over `[SEED]` with its topic-list watcher open.
async fn seeded(broker: &FakeBroker, client: &PulsarClient) -> PatternConsumer {
    broker.watch_topics(&[SEED]);
    let consumer = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("upd")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await
        .expect("subscribe");
    assert_eq!(
        consumer.topics(),
        [SEED],
        "harness: the seed is the snapshot"
    );
    consumer
}

fn has_topic(consumer: &PatternConsumer, topic: &str) -> bool {
    consumer.topics().iter().any(|t| t == topic)
}

/// Control (passes with and without the fix): one plain delta is applied by one `update`; it
/// shows that the push path, the sentinel and the reconcile are sound before the scenarios below
/// rely on them.
#[tokio::test(flavor = "current_thread")]
async fn update_subscribes_a_topic_the_broker_announced() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let consumer = seeded(&broker, &client).await;

    broker.push_update(&[FRESH], &[]);
    broker.client_has_processed_everything_pushed().await;
    let report = tokio::time::timeout(DEADLINE, consumer.update(&client))
        .await
        .expect("update did not hang")
        .expect("update ok");

    assert_eq!(report.added, 1, "harness: one topic was announced");
    assert!(
        has_topic(&consumer, FRESH),
        "topics: {:?}",
        consumer.topics()
    );
}

/// A subscribe the broker refuses must not take the rest of the drained deltas with it. `BAD` is
/// announced before `GOOD` in one delta; the subscribe of `BAD` fails, so `update` returns `Err`.
/// `GOOD` was announced by the broker, is still live, and the broker will not announce it
/// again, so the next `update` (the auto-reconcile tick that follows a failed one) must still
/// reach it. Fails if the failed call empties the queue, so that `GOOD` is never subscribed.
/// `BAD` itself is kept and retried once the refusal passes, and the refusal does not stop the
/// failed call short of `GOOD`.
#[tokio::test(flavor = "current_thread")]
async fn update_after_a_refused_subscribe_still_reaches_the_other_new_topics() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let consumer = seeded(&broker, &client).await;

    broker.deny_subscribe(BAD);
    broker.push_update(&[BAD, GOOD], &[]);
    broker.client_has_processed_everything_pushed().await;
    let first = tokio::time::timeout(DEADLINE, consumer.update(&client))
        .await
        .expect("update did not hang");
    assert!(
        first.is_err(),
        "harness: the refused subscribe of BAD must fail the update, got {first:?}"
    );
    let good_after_the_refusal = has_topic(&consumer, GOOD);

    // The refusal passes (a busy exclusive subscription is released, a permission is granted)
    // and the next reconcile tick runs. A resnapshot would see the whole namespace.
    broker.allow_subscribe(BAD);
    broker.watch_topics(&[SEED, BAD, GOOD]);
    let _ = tokio::time::timeout(DEADLINE, consumer.update(&client))
        .await
        .expect("update did not hang");

    assert!(
        has_topic(&consumer, GOOD),
        "GOOD was announced by the broker in the same delta as the refused BAD and never \
         subscribed afterwards: the failed update drained it from the queue and dropped it; \
         topics: {:?}",
        consumer.topics()
    );
    assert!(
        has_topic(&consumer, BAD),
        "BAD was refused once and allowed before the next update, yet it is not subscribed: the \
         refused topic was not kept for a retry; topics: {:?}",
        consumer.topics()
    );
    assert!(
        good_after_the_refusal,
        "the update that refused BAD stopped there instead of going on to GOOD, announced behind it"
    );
}

/// An `update` future dropped while a subscribe is in flight (a timeout wrapper, or `abort()` of
/// the auto-reconcile task) must not take the rest of the drained deltas with it either. `HELD`
/// is announced before `GOOD` and its subscribe is never answered, so the call is parked there
/// and dropped; the next `update` must still reach `GOOD`, and retry `HELD` itself.
#[tokio::test(flavor = "current_thread")]
async fn update_dropped_mid_subscribe_does_not_lose_the_other_new_topics() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let consumer = seeded(&broker, &client).await;

    broker.hold_subscribe(HELD);
    broker.push_update(&[HELD, GOOD], &[]);
    broker.client_has_processed_everything_pushed().await;
    let parked = tokio::time::timeout(PARKED, consumer.update(&client)).await;
    assert!(
        parked.is_err(),
        "harness: the update must be parked on the unanswered subscribe of HELD, got {parked:?}"
    );

    broker.release_subscribe(HELD);
    broker.watch_topics(&[SEED, HELD, GOOD]);
    let _ = tokio::time::timeout(DEADLINE, consumer.update(&client))
        .await
        .expect("update did not hang");

    assert!(
        has_topic(&consumer, GOOD),
        "GOOD was announced by the broker in the same delta as HELD, whose subscribe was \
         cancelled, and never subscribed afterwards: the cancelled update drained it from the \
         queue and dropped it; topics: {:?}",
        consumer.topics()
    );
    assert!(
        has_topic(&consumer, HELD),
        "HELD, whose subscribe was cancelled with the dropped update, is not subscribed after the \
         next update: the cancelled step was not kept; topics: {:?}",
        consumer.topics()
    );
}

/// Two deltas buffered before one `update` are applied in the order the broker sent them. The
/// broker created `FLEETING` and then deleted it, so after the call it must not be subscribed.
/// Fails if the deltas are merged and the removals run first: the removal then finds nothing to
/// remove, and the addition subscribes a topic the broker reported deleted.
#[tokio::test(flavor = "current_thread")]
async fn update_applies_create_then_delete_in_arrival_order() {
    let broker = FakeBroker::start().await;
    let client = broker.client().await;
    let consumer = seeded(&broker, &client).await;

    broker.push_update(&[FLEETING], &[]);
    broker.push_update(&[], &[FLEETING]);
    broker.client_has_processed_everything_pushed().await;
    tokio::time::timeout(DEADLINE, consumer.update(&client))
        .await
        .expect("update did not hang")
        .expect("update ok");

    assert!(
        !has_topic(&consumer, FLEETING),
        "FLEETING was created and then deleted by the broker, yet the consumer is subscribed to \
         it: the deltas were merged and the removal ran before the addition; topics: {:?}",
        consumer.topics()
    );
}
