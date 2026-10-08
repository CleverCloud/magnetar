// SPDX-License-Identifier: Apache-2.0

//! `memory_limit` is ONE budget per client, shared by every physical
//! connection (issue #867, ADR-0111) — tokio engine.
//!
//! Java's `ClientBuilder#memoryLimit` is enforced by one
//! `MemoryLimitController` per `PulsarClientImpl`. Before ADR-0111 magnetar
//! kept the counter on each `ConnectionShared`, so a client with N physical
//! connections (`connections_per_broker > 1`, proxy pool entries, replacement
//! connections) admitted N times the configured bytes.
//!
//! Every scenario opens a client with `connections_per_broker(2)` against an
//! in-process broker that HOLDS each `CommandSendReceipt` until the test
//! releases it, so the bytes of a publish stay reserved for exactly as long as
//! the test wants. Producer `a` rides the bootstrap connection (round-robin
//! index 0) and producer `b` its sibling (index 1); `two_connection_client`
//! asserts the two land on distinct sessions.
//!
//! The scenarios pin:
//! - `FailImmediately`: two sends that each fit, but not together, on two connections — the second
//!   is rejected with the aggregate in `current`.
//! - `ProducerBlock`: the second send parks and a receipt on the OTHER connection wakes it.
//! - Op-scoped lifetime: dropping an in-flight send future does not free its bytes — the receipt
//!   does.
//! - Cancelling a parked send leaks no bytes and never reaches the wire.
//! - A parked send survives a partial release (the pre-ADR-0111 waiter-slab key reuse lost it).
//! - Closing a producer releases the bytes of its never-acknowledged (batched) sends.
//! - A reconnect replays a retained publish without releasing or re-charging it.
//! - A broker `SendError` and a send timeout both release the reservation.
//!
//! Twin of `crates/magnetar-runtime-moonpool/tests/client_wide_memory_limit.rs`
//! (ADR-0024 1:1 parity).

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, MemoryLimitPolicy, SupervisorConfig,
    decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError, Producer};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

mod common;
use common::{HANG_GUARD, handshake_response_bytes};

/// The client-wide budget every scenario configures.
const LIMIT: u64 = 1000;

/// Negative-assertion window: a send the budget must hold back must not reach
/// the broker within it. Only ever asserts that something does NOT happen, so a
/// slow host can make it weaker but never makes a correct client fail.
const SETTLE: Duration = Duration::from_millis(300);

/// One `CommandSend` the fake broker received, with the physical session it
/// arrived on.
#[derive(Debug, Clone, Copy)]
struct SeenSend {
    session: usize,
    producer_id: u64,
    sequence_id: u64,
    highest_sequence_id: Option<u64>,
    payload_len: usize,
}

/// Per-session write side: frames the test queued plus a close request,
/// flushed by the session task. `notify_one` stores a permit, so a frame
/// queued while the task is busy is never lost.
#[derive(Default)]
struct Outbox {
    bytes: Mutex<BytesMut>,
    close: AtomicBool,
    notify: Notify,
}

/// In-process single-broker Pulsar that answers the control plane at once
/// and holds every publish receipt until the test calls [`Broker::release`]
/// (or [`Broker::reject`]).
#[derive(Default)]
struct Broker {
    sends: Mutex<Vec<SeenSend>>,
    sends_changed: Notify,
    outboxes: Mutex<Vec<Arc<Outbox>>>,
}

impl Broker {
    async fn spawn() -> (String, Arc<Self>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
        let addr = listener.local_addr().expect("local_addr");
        let broker = Arc::new(Self::default());
        let accept = broker.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    return;
                };
                let outbox = Arc::new(Outbox::default());
                let session = {
                    let mut outboxes = accept.outboxes.lock();
                    outboxes.push(outbox.clone());
                    outboxes.len() - 1
                };
                let broker = accept.clone();
                tokio::spawn(async move {
                    let _ = broker.serve(stream, session, outbox).await;
                });
            }
        });
        (format!("pulsar://{addr}"), broker)
    }

    async fn serve(
        &self,
        stream: TcpStream,
        session: usize,
        outbox: Arc<Outbox>,
    ) -> std::io::Result<()> {
        let (mut rd, mut wr) = stream.into_split();
        let mut read_buf = BytesMut::with_capacity(64 * 1024);
        loop {
            let mut out = BytesMut::new();
            loop {
                let mut framed = read_buf.clone().freeze();
                let before = framed.len();
                let frame = match decode_one(&mut framed) {
                    Ok(f) => f,
                    Err(FrameError::Incomplete { .. }) => break,
                    Err(_) => return Ok(()),
                };
                let consumed = before - framed.len();
                let _ = read_buf.split_to(consumed);
                self.answer(session, &frame, &mut out);
            }
            out.extend_from_slice(&outbox.bytes.lock().split());
            if !out.is_empty() {
                wr.write_all(&out).await?;
            }
            if outbox.close.load(Ordering::SeqCst) {
                // Dropping both halves closes the TCP connection under the client.
                return Ok(());
            }
            tokio::select! {
                read = rd.read_buf(&mut read_buf) => {
                    if read? == 0 {
                        return Ok(());
                    }
                }
                () = outbox.notify.notified() => {}
            }
        }
    }

    fn answer(&self, session: usize, frame: &magnetar_proto::Frame, out: &mut BytesMut) {
        let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
            return;
        };
        let reply = match kind {
            pb::base_command::Type::Connect => pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-client-wide-memory-limit".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            },
            pb::base_command::Type::Ping => pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            },
            pb::base_command::Type::Lookup => {
                let Some(l) = &frame.command.lookup_topic else {
                    return;
                };
                // Single-broker shape: every producer rides the bootstrap broker.
                pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                }
            }
            pb::base_command::Type::Producer => {
                let Some(p) = &frame.command.producer else {
                    return;
                };
                pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: format!("cwml-{}", p.producer_id),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                }
            }
            pb::base_command::Type::CloseProducer => {
                let Some(c) = &frame.command.close_producer else {
                    return;
                };
                pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: c.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                }
            }
            pb::base_command::Type::Send => {
                let Some(s) = &frame.command.send else {
                    return;
                };
                self.sends.lock().push(SeenSend {
                    session,
                    producer_id: s.producer_id,
                    sequence_id: s.sequence_id,
                    highest_sequence_id: s.highest_sequence_id,
                    payload_len: frame.payload.as_ref().map_or(0, |p| p.body.len()),
                });
                self.sends_changed.notify_waiters();
                // Held: the test answers through `release` / `reject`.
                return;
            }
            _ => return,
        };
        let _ = encode_command(out, &reply);
    }

    fn sends(&self) -> Vec<SeenSend> {
        self.sends.lock().clone()
    }

    /// `true` once the broker has received at least `n` publishes, `false` if
    /// `within` elapses first.
    async fn wait_for_sends(&self, n: usize, within: Duration) -> bool {
        tokio::time::timeout(within, async {
            loop {
                let notified = self.sends_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.sends.lock().len() >= n {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    fn reply_to(&self, index: usize, reply: &pb::BaseCommand) {
        let session = self.sends.lock()[index].session;
        let outbox = self.outboxes.lock()[session].clone();
        let _ = encode_command(&mut outbox.bytes.lock(), reply);
        outbox.notify.notify_one();
    }

    /// Answer the `index`-th publish with its `CommandSendReceipt`.
    fn release(&self, index: usize) {
        let send = self.sends.lock()[index];
        self.reply_to(
            index,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::SendReceipt as i32,
                send_receipt: Some(pb::CommandSendReceipt {
                    producer_id: send.producer_id,
                    sequence_id: send.sequence_id,
                    message_id: Some(pb::MessageIdData {
                        ledger_id: 1,
                        entry_id: index as u64,
                        ..Default::default()
                    }),
                    highest_sequence_id: send.highest_sequence_id,
                }),
                ..Default::default()
            },
        );
    }

    /// Answer the `index`-th publish with a `CommandSendError`.
    fn reject(&self, index: usize) {
        let send = self.sends.lock()[index];
        self.reply_to(
            index,
            &pb::BaseCommand {
                r#type: pb::base_command::Type::SendError as i32,
                send_error: Some(pb::CommandSendError {
                    producer_id: send.producer_id,
                    sequence_id: send.sequence_id,
                    error: pb::ServerError::PersistenceError as i32,
                    message: "held publish rejected by the test".to_owned(),
                }),
                ..Default::default()
            },
        );
    }

    /// Close the TCP connection of `session` under the client.
    fn drop_session(&self, session: usize) {
        let outbox = self.outboxes.lock()[session].clone();
        outbox.close.store(true, Ordering::SeqCst);
        outbox.notify.notify_one();
    }
}

fn config(policy: MemoryLimitPolicy) -> ConnectionConfig {
    ConnectionConfig {
        memory_limit_bytes: LIMIT,
        memory_limit_policy: policy,
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    }
}

fn producer_request(topic: &str) -> CreateProducerRequest {
    CreateProducerRequest {
        topic: format!("persistent://public/default/{topic}"),
        // Held receipts must stay held for as long as the scenario wants.
        send_timeout: None,
        ..Default::default()
    }
}

fn msg(len: usize) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from(vec![0xA5; len]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(len).expect("test payload fits u32"),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Poll `fut` exactly once from the current task.
async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *fut).poll(cx))).await
}

async fn open(client: &Client, request: CreateProducerRequest) -> Producer {
    tokio::time::timeout(HANG_GUARD, client.open_producer(request))
        .await
        .expect("open_producer did not time out")
        .expect("open_producer ok")
}

/// A client with `connections_per_broker(2)` and two producers, `a` on the
/// bootstrap connection and `b` on its sibling.
async fn two_connection_client(
    policy: MemoryLimitPolicy,
) -> (Arc<Broker>, Client, Producer, Producer) {
    let (url, broker) = Broker::spawn().await;
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config(policy)))
        .await
        .expect("connect did not time out")
        .expect("connect ok")
        .with_connections_per_broker(2);
    let a = open(&client, producer_request("cwml-a")).await;
    let b = open(&client, producer_request("cwml-b")).await;
    assert_eq!(
        broker.outboxes.lock().len(),
        2,
        "connections_per_broker(2) must give the two producers two physical connections"
    );
    (broker, client, a, b)
}

fn shut_down(client: Client) {
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);
}

fn assert_rejected(outcome: Poll<Result<magnetar_proto::MessageId, ClientError>>, current: u64) {
    match outcome {
        Poll::Ready(Err(ClientError::MemoryLimitExceeded {
            current: seen,
            limit,
            requested,
        })) => {
            assert_eq!(limit, LIMIT, "the error must echo the client-wide limit");
            assert_eq!(
                seen, current,
                "`current` must be the client-wide aggregate, not one connection's share"
            );
            assert!(seen + requested > limit, "strict `next > limit` rejection");
        }
        other => panic!("expected MemoryLimitExceeded {{ current: {current} }}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_immediately_budget_spans_connections() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::FailImmediately).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(
        poll_once(&mut a1).await.is_pending(),
        "600 B fits the budget"
    );
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);

    // 600 B fits on its own, but 600 + 600 exceeds the ONE client-wide budget
    // even though `b` rides a different physical connection.
    let mut b1 = Box::pin(b.send(msg(600)));
    assert_rejected(poll_once(&mut b1).await, 600);
    assert!(
        !broker.wait_for_sends(2, SETTLE).await,
        "a rejected send never reaches the wire"
    );

    // The receipt on connection 0 frees the budget for connection 1.
    broker.release(0);
    tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves")
        .expect("a1 acknowledged");
    let mut b2 = Box::pin(b.send(msg(600)));
    assert!(poll_once(&mut b2).await.is_pending(), "budget freed by a1");
    assert!(broker.wait_for_sends(2, HANG_GUARD).await);
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 acknowledged");

    let sends = broker.sends();
    assert_ne!(
        sends[0].session, sends[1].session,
        "the two publishes must ride two distinct physical connections: {sends:?}"
    );
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_block_release_on_one_connection_wakes_the_other() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::ProducerBlock).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);

    // Driven by its own task, so only a wake can make it progress.
    let b1 = tokio::spawn(b.send(msg(600)));
    assert!(
        !broker.wait_for_sends(2, SETTLE).await,
        "ProducerBlock must hold b1 back while a1 holds the client-wide budget: {:?}",
        broker.sends()
    );

    broker.release(0);
    tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves")
        .expect("a1 acknowledged");
    assert!(
        broker.wait_for_sends(2, HANG_GUARD).await,
        "a receipt on connection 0 must wake the send parked for connection 1"
    );
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, b1)
        .await
        .expect("b1 resolves")
        .expect("b1 task")
        .expect("b1 acknowledged");

    let sends = broker.sends();
    assert_ne!(sends[0].session, sends[1].session, "{sends:?}");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_in_flight_send_keeps_its_bytes_until_the_receipt() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::FailImmediately).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);
    // Fire-and-forget: the publish is on the wire and retained for replay, so
    // its bytes stay reserved until the broker answers.
    drop(a1);

    let mut b1 = Box::pin(b.send(msg(600)));
    assert_rejected(poll_once(&mut b1).await, 600);

    broker.release(0);
    tokio::time::timeout(HANG_GUARD, async {
        while a.pending_count() > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the receipt drains a's pending queue");
    let mut b2 = Box::pin(b.send(msg(600)));
    assert!(
        poll_once(&mut b2).await.is_pending(),
        "the receipt released the dropped send's bytes"
    );
    assert!(broker.wait_for_sends(2, HANG_GUARD).await);
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 acknowledged");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_parked_send_leaks_nothing_and_never_reaches_the_wire() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::ProducerBlock).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);

    let mut b1 = Box::pin(b.send(msg(600)));
    assert!(poll_once(&mut b1).await.is_pending(), "b1 parks");
    // A second poll without an intervening release refreshes the parked waker.
    assert!(poll_once(&mut b1).await.is_pending(), "b1 stays parked");
    drop(b1);

    broker.release(0);
    tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves")
        .expect("a1 acknowledged");

    // The whole budget is available again: the cancelled send kept nothing.
    let b2 = tokio::spawn(b.send(msg(1000)));
    assert!(broker.wait_for_sends(2, HANG_GUARD).await);
    let sends = broker.sends();
    assert_eq!(
        sends.len(),
        2,
        "the cancelled send must never reach the wire: {sends:?}"
    );
    assert_eq!(sends[1].payload_len, 1000, "{sends:?}");
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 task")
        .expect("b2 acknowledged");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_send_survives_a_partial_release() {
    let (broker, client, a, _b) = two_connection_client(MemoryLimitPolicy::ProducerBlock).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    let mut a2 = Box::pin(a.send(msg(400)));
    assert!(poll_once(&mut a2).await.is_pending());
    assert!(broker.wait_for_sends(2, HANG_GUARD).await);

    // The budget is full (600 + 400); a 500 B send parks.
    let parked = tokio::spawn(a.send(msg(500)));
    tokio::time::sleep(SETTLE).await;

    // Releasing 400 B wakes it, but 600 + 500 still does not fit: it must
    // re-park and keep a live registration.
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, a2)
        .await
        .expect("a2 resolves")
        .expect("a2 acknowledged");
    assert!(
        !broker.wait_for_sends(3, SETTLE).await,
        "600 + 500 exceeds the budget; the parked send must stay parked"
    );

    // Releasing the remaining 600 B must wake it again.
    broker.release(0);
    tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves")
        .expect("a1 acknowledged");
    assert!(
        broker.wait_for_sends(3, HANG_GUARD).await,
        "the re-parked send must be woken by the next release (lost wakeup)"
    );
    broker.release(2);
    tokio::time::timeout(HANG_GUARD, parked)
        .await
        .expect("parked send resolves")
        .expect("parked task")
        .expect("parked send acknowledged");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_a_producer_releases_its_unacknowledged_sends() {
    let (broker, client, _a, b) = two_connection_client(MemoryLimitPolicy::FailImmediately).await;
    // Batching with no publish delay: the message sits in the batch container,
    // reserved, and never reaches the wire on its own.
    let batching = open(
        &client,
        CreateProducerRequest {
            enable_batching: true,
            ..producer_request("cwml-batching")
        },
    )
    .await;

    let mut queued = Box::pin(batching.send(msg(600)));
    assert!(poll_once(&mut queued).await.is_pending());
    let mut b1 = Box::pin(b.send(msg(600)));
    assert_rejected(poll_once(&mut b1).await, 600);

    tokio::time::timeout(HANG_GUARD, batching.clone().close())
        .await
        .expect("close resolves")
        .expect("close acknowledged");
    let closed = tokio::time::timeout(HANG_GUARD, queued)
        .await
        .expect("closing the producer resolves its unacknowledged send");
    assert!(
        closed.is_err(),
        "an unacknowledged send fails when its producer closes: {closed:?}"
    );

    let mut b2 = Box::pin(b.send(msg(1000)));
    assert!(
        poll_once(&mut b2).await.is_pending(),
        "close released the batched bytes"
    );
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);
    broker.release(0);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 acknowledged");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_replays_a_retained_send_without_release_or_recharge() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::FailImmediately).await;

    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);
    let a_session = broker.sends()[0].session;

    // Kill a's connection: the publish is retained for replay, so its bytes
    // stay reserved across the reconnect.
    broker.drop_session(a_session);
    let mut b1 = Box::pin(b.send(msg(600)));
    assert_rejected(poll_once(&mut b1).await, 600);

    // The supervisor redials and replays a1 on the new session.
    assert!(
        broker.wait_for_sends(2, HANG_GUARD).await,
        "the retained publish is replayed after the reconnect"
    );
    let sends = broker.sends();
    assert_ne!(sends[1].session, a_session, "replayed on a fresh session");
    assert_eq!(sends[1].sequence_id, sends[0].sequence_id, "same publish");

    // Replay neither released nor re-charged: exactly 600 B are reserved.
    let mut b2 = Box::pin(b.send(msg(400)));
    assert!(
        poll_once(&mut b2).await.is_pending(),
        "600 + 400 fits: the replay must not have charged the publish twice"
    );
    let mut b3 = Box::pin(b.send(msg(1)));
    assert_rejected(poll_once(&mut b3).await, 1000);

    broker.release(1);
    tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves")
        .expect("a1 acknowledged");
    assert!(broker.wait_for_sends(3, HANG_GUARD).await);
    broker.release(2);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 acknowledged");
    shut_down(client);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_error_and_send_timeout_release_the_reservation() {
    let (broker, client, a, b) = two_connection_client(MemoryLimitPolicy::FailImmediately).await;

    // Broker-side rejection.
    let mut a1 = Box::pin(a.send(msg(600)));
    assert!(poll_once(&mut a1).await.is_pending());
    assert!(broker.wait_for_sends(1, HANG_GUARD).await);
    let mut held = Box::pin(b.send(msg(600)));
    assert_rejected(poll_once(&mut held).await, 600);
    broker.reject(0);
    let rejected = tokio::time::timeout(HANG_GUARD, a1)
        .await
        .expect("a1 resolves");
    assert!(
        matches!(rejected, Err(ClientError::SendRejected { .. })),
        "{rejected:?}"
    );
    let mut b1 = Box::pin(b.send(msg(1000)));
    assert!(
        poll_once(&mut b1).await.is_pending(),
        "the SendError released a1's bytes"
    );
    assert!(broker.wait_for_sends(2, HANG_GUARD).await);
    broker.release(1);
    tokio::time::timeout(HANG_GUARD, b1)
        .await
        .expect("b1 resolves")
        .expect("b1 acknowledged");

    // Client-side send timeout.
    let timed = open(
        &client,
        CreateProducerRequest {
            send_timeout: Some(Duration::from_millis(200)),
            ..producer_request("cwml-timeout")
        },
    )
    .await;
    let t1 = tokio::time::timeout(HANG_GUARD, timed.send(msg(600)))
        .await
        .expect("the send timeout resolves t1");
    assert!(t1.is_err(), "{t1:?}");
    let mut b2 = Box::pin(b.send(msg(1000)));
    assert!(
        poll_once(&mut b2).await.is_pending(),
        "the send timeout released t1's bytes"
    );
    assert!(broker.wait_for_sends(4, HANG_GUARD).await);
    broker.release(3);
    tokio::time::timeout(HANG_GUARD, b2)
        .await
        .expect("b2 resolves")
        .expect("b2 acknowledged");
    shut_down(client);
}

/// Every way a publish enters the producer state machine moves its
/// reservation into the publish's op, which holds it until the op leaves the
/// client: a plain send, a batched send, a chunked send, and an oversized send
/// that still fits the batch container. The unreserved entry points reserve
/// nothing. Drives this engine's `ConnectionShared` directly, no broker.
#[test]
fn every_enqueue_path_hands_its_reservation_to_the_op() {
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig {
        memory_limit_bytes: LIMIT,
        ..ConnectionConfig::default()
    });
    let budget = shared.memory_limit.clone();
    assert_eq!(budget.limit_bytes(), LIMIT);
    let refused = budget
        .try_reserve(LIMIT + 1)
        .expect_err("over the whole budget");
    assert_eq!(
        refused.to_string(),
        format!(
            "memory limit exceeded: current=0B + requested={}B > limit={LIMIT}B",
            LIMIT + 1
        )
    );

    let at = std::time::Instant::now();
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(at, &handshake_response_bytes())
        .expect("connected");
    let plain = conn.create_producer(producer_request("cwml-enqueue-plain"));
    let batching = conn.create_producer(CreateProducerRequest {
        enable_batching: true,
        ..producer_request("cwml-enqueue-batching")
    });
    let chunking = conn.create_producer(CreateProducerRequest {
        enable_chunking: true,
        ..producer_request("cwml-enqueue-chunking")
    });
    let both = conn.create_producer(CreateProducerRequest {
        enable_batching: true,
        enable_chunking: true,
        max_batch_size_bytes: 64,
        ..producer_request("cwml-enqueue-both")
    });
    for handle in [chunking, both] {
        conn.producer(handle)
            .expect("slot")
            .state
            .lock()
            .max_message_size = 8;
    }

    for (handle, len, used) in [
        (plain, 10, 10),
        (batching, 20, 30),
        (chunking, 20, 50),
        (both, 20, 70),
    ] {
        let reservation = budget.try_reserve(len).expect("fits");
        conn.send_reserved(handle, msg(len as usize), reservation, 0, at)
            .expect("queued");
        assert_eq!(budget.used_bytes(), used, "the op holds the bytes");
        assert_eq!(
            conn.producer_pending_count(handle),
            1,
            "one op per logical message"
        );
        let slot = conn.producer(handle).expect("slot").clone();
        assert_eq!(slot.state.lock().pending[0].reserved_bytes(), len);
    }

    // The unreserved entry points queue without touching the budget.
    conn.send(plain, msg(5), 0, at).expect("queued");
    let slot = conn.producer(plain).expect("slot").clone();
    slot.queue_send(msg(5), 0, at).expect("queued");
    slot.state.lock().queue_send(msg(5), 0, at).expect("queued");
    assert_eq!(conn.producer_pending_count(plain), 4);
    assert!(
        slot.state
            .lock()
            .pending
            .iter()
            .skip(1)
            .all(|op| op.reserved_bytes() == 0),
        "unreserved enqueues hold nothing"
    );
    assert_eq!(budget.used_bytes(), 70);

    // Failing every op releases every reservation exactly once.
    conn.fail_all_pending("test teardown");
    drop(conn);
    assert_eq!(budget.used_bytes(), 0);
}
