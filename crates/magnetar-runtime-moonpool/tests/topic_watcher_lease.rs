// SPDX-License-Identifier: Apache-2.0

//! A topic-list watcher registered through
//! `Client::watch_topic_list_leased_with_operation_deadline` belongs to the lease it returns:
//! dropping the lease closes the watcher at the broker (ADR-0156), while the persistent
//! `watch_topic_list` keeps its watcher for the life of the connection. A watch the broker refuses
//! holds nothing there, so its lease closes nothing.
//!
//! Moonpool mirror of `crates/magnetar-runtime-tokio/tests/topic_watcher_lease.rs`; the
//! connection-level half is `topic_watcher_close_lifecycle.rs`, and the façade-level half is
//! `crates/magnetar-differential/tests/pattern_watcher_close_equivalence.rs`.
//!
//! Layer (c) of the ADR-0024 four-layer test policy. A stub broker answers the handshake, answers a
//! watch (a pattern starting with `refused-` is refused) and records the watcher ids of the
//! registrations and closes it saw; the assertions wait on those records for [`HANG_GUARD`].

#![forbid(unsafe_code)]

use std::sync::Arc;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, ClientError, MoonpoolEngine, TopicWatcherLease};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
mod common;
use common::HANG_GUARD;

const NAMESPACE: &str = "public/default";
const TOPIC: &str = "persistent://public/default/watched";

/// The watcher ids of the frames the stub broker has seen, in arrival order.
#[derive(Debug, Default)]
struct Recorded {
    registered: Vec<u64>,
    closed: Vec<u64>,
}

struct StubBroker {
    addr: String,
    recorded: Arc<Mutex<Recorded>>,
    arrived: Arc<Notify>,
}

impl StubBroker {
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

async fn spawn_stub_broker() -> StubBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let arrived = Arc::new(Notify::new());
    let (recorded_task, arrived_task) = (recorded.clone(), arrived.clone());
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        let mut out_buf = BytesMut::with_capacity(8 * 1024);
        loop {
            loop {
                let mut framed = read_buf.clone().freeze();
                let before = framed.len();
                let frame = match decode_one(&mut framed) {
                    Ok(f) => f,
                    Err(FrameError::Incomplete { .. }) => break,
                    Err(_) => return,
                };
                let _ = read_buf.split_to(before - framed.len());
                handle_frame(&frame, &mut out_buf, &recorded_task, &arrived_task);
            }

            if !out_buf.is_empty() {
                if stream.write_all(&out_buf).await.is_err() {
                    return;
                }
                if stream.flush().await.is_err() {
                    return;
                }
                out_buf.clear();
            }

            match stream.read_buf(&mut read_buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
    StubBroker {
        addr: addr.to_string(),
        recorded,
        arrived,
    }
}

fn handle_frame(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    recorded: &Mutex<Recorded>,
    arrived: &Notify,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    let answer = match kind {
        pb::base_command::Type::Connect => Some(pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-watcher-lease-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        }),
        pb::base_command::Type::Ping => Some(pb::BaseCommand {
            r#type: pb::base_command::Type::Pong as i32,
            pong: Some(pb::CommandPong {}),
            ..Default::default()
        }),
        pb::base_command::Type::WatchTopicList => {
            frame.command.watch_topic_list.as_ref().map(|watch| {
                recorded.lock().registered.push(watch.watcher_id);
                arrived.notify_one();
                if watch.topics_pattern.starts_with("refused-") {
                    pb::BaseCommand {
                        r#type: pb::base_command::Type::Error as i32,
                        error: Some(pb::CommandError {
                            request_id: watch.request_id,
                            error: pb::ServerError::AuthorizationError as i32,
                            message: "synthetic watch refusal".to_owned(),
                        }),
                        ..Default::default()
                    }
                } else {
                    pb::BaseCommand {
                        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
                        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                            request_id: watch.request_id,
                            watcher_id: watch.watcher_id,
                            topic: vec![TOPIC.to_owned()],
                            topics_hash: "stub-hash".to_owned(),
                        }),
                        ..Default::default()
                    }
                }
            })
        }
        pb::base_command::Type::WatchTopicListClose => {
            frame.command.watch_topic_list_close.as_ref().map(|close| {
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
    if let Some(answer) = answer {
        let _ = encode_command(out, &answer);
    }
}

async fn connect(broker: &StubBroker) -> Client<TokioProviders> {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain(&engine, &broker.addr, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok")
}

/// Registers a leased watch for `pattern` with the client's own operation deadline.
async fn leased(
    client: &Client<TokioProviders>,
    pattern: &str,
) -> Result<(Vec<String>, TopicWatcherLease), ClientError> {
    let mut deadline = client.operation_timer();
    let mut last_broker_error = None;
    client
        .watch_topic_list_leased_with_operation_deadline(
            NAMESPACE,
            pattern,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
        .map(|(_watcher_id, topics, lease)| (topics, lease))
}

/// A persistent watch keeps its watcher, and a leased one is closed when its lease drops. The
/// final fence closes a third watcher: the broker handles frames in order, so had the persistent
/// watcher been closed too, its close would have been recorded before the fence's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_lease_closes_the_watcher_and_a_persistent_watch_is_kept() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let broker = spawn_stub_broker().await;
            let client = connect(&broker).await;

            let persistent = client
                .watch_topic_list(NAMESPACE, "persistent-.*")
                .await
                .expect("persistent watch");
            assert_eq!(persistent, vec![TOPIC.to_owned()]);

            let (topics, lease) = leased(&client, "leased-.*").await.expect("leased watch");
            assert_eq!(topics, vec![TOPIC.to_owned()]);
            let registered = broker.registered();
            assert_eq!(
                registered.len(),
                2,
                "both watches registered: {registered:?}"
            );
            let (persistent_watcher, leased_watcher) = (registered[0], registered[1]);

            drop(lease);
            assert_eq!(
                broker.closes(1).await,
                vec![leased_watcher],
                "dropping the lease closes exactly the leased watcher"
            );

            let (_, fence) = leased(&client, "fence-.*").await.expect("fence watch");
            let fence_watcher = broker.registered()[2];
            drop(fence);
            assert_eq!(
                broker.closes(2).await,
                vec![leased_watcher, fence_watcher],
                "the persistent watcher {persistent_watcher} is never closed"
            );

            client.close().await;
        })
        .await;
}

/// The broker refused the watch, so it holds nothing: the error reaches the caller and the lease
/// the call held closes nothing at the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_leased_watch_closes_nothing_at_the_broker() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let broker = spawn_stub_broker().await;
            let client = connect(&broker).await;

            let error = leased(&client, "refused-.*")
                .await
                .expect_err("the broker refuses this watch");
            assert!(
                matches!(error, ClientError::Broker { .. }),
                "unexpected error: {error:?}"
            );

            let (_, fence) = leased(&client, "fence-.*").await.expect("fence watch");
            let fence_watcher = broker.registered()[1];
            drop(fence);
            assert_eq!(
                broker.closes(1).await,
                vec![fence_watcher],
                "no close is sent for the refused watcher"
            );

            client.close().await;
        })
        .await;
}

/// A lease dropped after the connection already stopped tracking its watcher (the watch was
/// cancelled by its deadline, which closed it) stages nothing more: the broker sees one close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_that_hits_its_deadline_is_closed_once() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let broker = spawn_stub_broker().await;
            let client = connect(&broker).await;

            // A deadline that is not ready at the readiness check before the registration is staged
            // (its first poll yields) and is ready at the first poll of the call's
            // select, which polls the deadline branch first: the registration is on the
            // wire and the call gives up on it.
            let mut deadline = Box::pin(async {
                tokio::task::yield_now().await;
            });
            let mut last_broker_error = None;
            let result = client
                .watch_topic_list_leased_with_operation_deadline(
                    NAMESPACE,
                    "deadline-.*",
                    deadline.as_mut(),
                    &mut last_broker_error,
                )
                .await;
            let error = result.expect_err("the deadline wins");
            assert!(
                matches!(error, ClientError::Other(ref message)
                    if message == "topic-list snapshot exceeded operation_timeout"),
                "unexpected error: {error:?}"
            );

            let closed = broker.closes(1).await;
            assert_eq!(
                closed,
                broker.registered(),
                "the watcher the deadline gave up on is closed"
            );
            let registered = closed;
            let (_, fence) = leased(&client, "fence-.*").await.expect("fence watch");
            let fence_watcher = broker.registered()[1];
            drop(fence);
            assert_eq!(
                broker.closes(2).await,
                vec![registered[0], fence_watcher],
                "the watcher is closed once, not once by the cancel and once by the lease"
            );

            client.close().await;
        })
        .await;
}
