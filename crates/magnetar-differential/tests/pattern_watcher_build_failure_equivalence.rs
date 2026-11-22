// SPDX-License-Identifier: Apache-2.0

//! A pattern consumer whose build fails after its topic-list watcher was registered must not leave
//! the watcher at the broker, identically on both engines (ADR-0024 layer (d), ADR-0156).
//!
//! `PatternConsumerBuilder::subscribe` registers a watcher to read the matching topics, then
//! subscribes a child per topic. When a child cannot be opened it tears down the children it had
//! opened and returns the error: the consumer never exists, so nothing else holds the watcher and
//! it has to close with the early return. The fake broker (the one of
//! `pattern_watcher_close_equivalence.rs`, which the scripted broker of this crate cannot replace
//! because it answers no `CommandWatchTopicList`) returns one matching topic and refuses its
//! lookup with a non-retryable error, which fails the child subscribe.
//!
//! The assertions wait on the frames the fake recorded for [`HANG_GUARD`], the anti-hang backstop,
//! rather than sleeping a fixed time.

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
const TOPIC: &str = "persistent://public/default/orders-0";
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

/// The fake broker's answer to one frame: `Connect` -> `Connected`, `Ping` -> `Pong`,
/// `WatchTopicList` -> `WatchTopicListSuccess` with one matching topic, `WatchTopicListClose` ->
/// `Success`, `Lookup` -> a non-retryable refusal; every other frame is swallowed.
fn answer_for(
    command: pb::BaseCommand,
    recorded: &Mutex<Recorded>,
    arrived: &Notify,
) -> Option<pb::BaseCommand> {
    match pb::base_command::Type::try_from(command.r#type) {
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
        Ok(pb::base_command::Type::WatchTopicList) => command.watch_topic_list.map(|watch| {
            recorded.lock().registered.push(watch.watcher_id);
            arrived.notify_one();
            pb::BaseCommand {
                r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
                watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                    request_id: watch.request_id,
                    watcher_id: watch.watcher_id,
                    topic: vec![TOPIC.to_owned()],
                    topics_hash: "fake-hash".to_owned(),
                }),
                ..Default::default()
            }
        }),
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
        Ok(pb::base_command::Type::Lookup) => command.lookup_topic.map(|lookup| pb::BaseCommand {
            r#type: pb::base_command::Type::LookupResponse as i32,
            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
                request_id: lookup.request_id,
                error: Some(pb::ServerError::AuthorizationError as i32),
                message: Some("synthetic lookup refusal".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => None,
    }
}

/// Spawns the fake broker, which answers every frame through [`answer_for`].
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
                        let answer = answer_for(command, &recorded, &arrived);
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

/// What the failed build leaves at the broker, compared across the engines.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Whether the build returned an error.
    build_failed: bool,
    /// Watcher ids the broker was asked to register.
    registered: Vec<u64>,
    /// Watcher ids the broker was asked to close.
    closed: Vec<u64>,
}

async fn failing_build_scenario<E>(client: &PulsarClient<E>, broker: &FakeBroker) -> Observed
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    let built = client
        .pattern_consumer()
        .namespace(NAMESPACE)
        .pattern(PATTERN)
        .subscription("watcher-build-failure-sub")
        .subscribe()
        .await;
    Observed {
        build_failed: built.is_err(),
        registered: broker.registered(),
        closed: broker.closes(1).await,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_pattern_consumer_build_closes_its_watcher_on_both_engines() {
    let on_tokio = async {
        let broker = spawn_fake_broker().await;
        let client = connect_tokio(&broker).await;
        failing_build_scenario(&client, &broker).await
    };
    let on_moonpool = async {
        let broker = spawn_fake_broker().await;
        let client = connect_moonpool(&broker).await;
        failing_build_scenario(&client, &broker).await
    };
    let (tokio, moonpool) = tokio::join!(on_tokio, on_moonpool);

    assert_eq!(tokio, moonpool, "engines must agree on a failed build");
    assert!(
        tokio.build_failed,
        "control: the refused child lookup fails the build: {tokio:?}"
    );
    assert_eq!(
        tokio.registered.len(),
        1,
        "control: the build registered one watcher: {tokio:?}"
    );
    assert_eq!(
        tokio.closed, tokio.registered,
        "the broker must be told to close the watcher of a consumer that was never built \
         (CommandWatchTopicListClose) within {HANG_GUARD:?}, but it saw {tokio:?}"
    );
}
