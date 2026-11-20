// SPDX-License-Identifier: Apache-2.0

//! Push delivery must still end when the consumer is terminal.
//!
//! `push_delivery_error_survival.rs` pins that a push loop outlives an error about one message or
//! one removed child. These tests pin the other half of the same rule: a `receive()` error that
//! comes from a consumer that can never deliver again (here, a broker connection that dropped for
//! good with no reconnect supervisor) still ends the loop, instead of retrying a failure that
//! resolves at once, forever. The loop tells the two apart with `ConsumerApi::is_terminal`, the
//! gate `receive()` itself uses. These scenarios catch a loop that never breaks; they cannot tell
//! `is_terminal` from the consumer's own `closed` flag, because a plain terminal connection drop
//! closes the consumer slots too. The state where only `is_terminal` holds (a terminal subscribe
//! failure, which sets a marker and never `closed`) needs a broker script that exhausts a
//! subscribe retry budget, and is not covered here.
//!
//! Each scenario runs against a real `PulsarClient` on the real tokio runtime, speaking to a
//! scripted loopback broker that can be told to drop every connection. A loop that retried the
//! terminal error never yields, so it can be neither aborted nor waited for: each scenario runs on
//! its own two-thread runtime that is dropped without waiting for its tasks (`run`), which turns
//! that bug into a failed assertion instead of a hung test binary.
//!
//! The `TableView` drain applies the same rule, but its end is not observable from outside (the
//! view exposes no task state), so it has no scenario of its own here.

#![cfg(feature = "tokio")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{IncomingMessage, MessageListener, PulsarClient, WrapperMessageListener};
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const TOPIC_A: &str = "persistent://public/default/push-terminal-a";
const TOPIC_B: &str = "persistent://public/default/push-terminal-b";
const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Default)]
struct State {
    /// `(connection, consumer id)` -> topic, set by `CommandSubscribe`.
    subscribed: HashMap<(u64, u64), String>,
    /// Frames ready to be written, per connection.
    outbox: HashMap<u64, BytesMut>,
    /// Wakes the connection task that owns the connection id.
    wake: HashMap<u64, Arc<Notify>>,
}

struct FakeBroker {
    addr: String,
    state: Arc<Mutex<State>>,
    /// When set, every connection task closes its socket the next time it wakes.
    drop_connections: Arc<AtomicBool>,
}

impl FakeBroker {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr").to_string();
        let state = Arc::new(Mutex::new(State::default()));
        let drop_connections = Arc::new(AtomicBool::new(false));
        let accept_state = state.clone();
        let accept_drop = drop_connections.clone();
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
                tokio::spawn(serve(
                    stream,
                    conn,
                    accept_state.clone(),
                    notify,
                    accept_drop.clone(),
                ));
            }
        });
        Self {
            addr,
            state,
            drop_connections,
        }
    }

    fn url(&self) -> String {
        format!("pulsar://{}", self.addr)
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

    /// Close every connection for good. The client has no reconnect supervisor, so each consumer
    /// on it becomes terminal.
    fn drop_every_connection(&self) {
        self.drop_connections.store(true, Ordering::SeqCst);
        for notify in self.state.lock().wake.values() {
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

/// Handle one decoded client command, queueing replies in this connection's outbox.
fn handle_command(cmd: &pb::BaseCommand, conn: u64, state: &Mutex<State>) {
    let mut guard = state.lock();
    let ty = cmd.r#type;
    let reply: Option<BytesMut> = if ty == pb::base_command::Type::Connect as i32 {
        Some(command_frame(&pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "push-terminal-fake".to_owned(),
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

async fn serve(
    mut stream: TcpStream,
    conn: u64,
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
    drop_connections: Arc<AtomicBool>,
) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut probe = [0u8; 8 * 1024];
    loop {
        if drop_connections.load(Ordering::SeqCst) {
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

fn single_listener() -> MessageListener {
    Arc::new(|_: &IncomingMessage| {})
}

fn wrapper_listener() -> WrapperMessageListener {
    Arc::new(|_: &str, _: &IncomingMessage| {})
}

/// Run `scenario` on a two-thread runtime, then drop the runtime without waiting for its tasks.
/// A poller stuck retrying a terminal error never yields to the scheduler, so the usual runtime
/// shutdown (and `MessageListenerHandle::close`) would wait for it forever.
fn run(scenario: impl Future<Output = ()>) {
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

/// Single-topic site (`spawn_listener_loop`): the connection drops for good, `receive()` resolves
/// `Err(Closed)` at once and forever, and the poller must stop instead of retrying it.
#[test]
fn single_topic_push_listener_ends_when_the_connection_is_lost_for_good() {
    run(async {
        let broker = FakeBroker::start().await;
        let client = broker.client().await;

        let handle = client
            .consumer(TOPIC_A)
            .subscription("push-terminal")
            .subscription_type(SubType::Exclusive)
            .message_listener(single_listener())
            .subscribe_with_listener()
            .await
            .expect("subscribe_with_listener");
        assert!(handle.is_running(), "control: poller live before the drop");

        broker.drop_every_connection();

        let ended = eventually(|| !handle.is_running()).await;
        assert!(
            ended,
            "the push poller kept running after its connection was lost for good: it retried a \
             terminal receive() error instead of stopping"
        );
    });
}

/// Wrapper site (`spawn_wrapper_message_listener`): every child is terminal once the connection
/// drops, so the wrapper poller must stop as well.
#[test]
fn multi_topic_push_listener_ends_when_the_connection_is_lost_for_good() {
    run(async {
        let broker = FakeBroker::start().await;
        let client = broker.client().await;

        let handle = client
            .multi_topics_consumer()
            .topic(TOPIC_A)
            .topic(TOPIC_B)
            .subscription("push-terminal")
            .subscription_type(SubType::Exclusive)
            .message_listener(wrapper_listener())
            .subscribe_with_listener()
            .await
            .expect("subscribe_with_listener");
        assert!(handle.is_running(), "control: poller live before the drop");

        broker.drop_every_connection();

        let ended = eventually(|| !handle.is_running()).await;
        assert!(
            ended,
            "the wrapper push poller kept running after its connection was lost for good: it \
             retried a terminal receive() error instead of stopping"
        );
    });
}
