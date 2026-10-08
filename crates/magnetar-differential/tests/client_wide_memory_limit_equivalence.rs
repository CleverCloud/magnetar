// SPDX-License-Identifier: Apache-2.0

//! Tokio ↔ moonpool differential equivalence for the client-wide
//! `memory_limit` (issue #867, ADR-0111). Layer (d) of the ADR-0024
//! four-layer test policy.
//!
//! The single-producer `Trace`/runner model cannot observe a budget shared
//! by two physical connections, so this test drives an in-process broker
//! that HOLDS every publish receipt against BOTH engines, each client opened
//! with `connections_per_broker(2)` so producers `a` and `b` ride two
//! distinct connections. One scenario — written once, expanded per engine by
//! `scenario!` — walks every reservation path: the `FailImmediately`
//! rejection across connections, a `ProducerBlock` send parked, re-polled,
//! cancelled, woken by a receipt on the other connection, and failed by the
//! state machine after it reserved, plus a send refused by a closed
//! producer. Each engine records the outcome of every step and the two
//! traces must be identical.

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
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const HANG_GUARD: Duration = Duration::from_mins(1);
const LIMIT: u64 = 1000;
/// Negative-assertion window; only ever asserts that nothing happens.
const SETTLE: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy)]
struct SeenSend {
    session: usize,
    producer_id: u64,
    sequence_id: u64,
    payload_len: usize,
}

#[derive(Default)]
struct Outbox {
    bytes: Mutex<BytesMut>,
    notify: Notify,
}

/// In-process single-broker Pulsar: answers the control plane at once and
/// holds every publish receipt until [`Broker::release`].
#[derive(Default)]
struct Broker {
    sends: Mutex<Vec<SeenSend>>,
    sends_changed: Notify,
    outboxes: Mutex<Vec<Arc<Outbox>>>,
    stop: AtomicBool,
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
        (addr.to_string(), broker)
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
            if self.stop.load(Ordering::SeqCst) {
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
                    server_version: "magnetar-cwml-equiv".to_owned(),
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
                        producer_name: format!("cwml-equiv-{}", p.producer_id),
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
                    payload_len: frame.payload.as_ref().map_or(0, |p| p.body.len()),
                });
                self.sends_changed.notify_waiters();
                return;
            }
            _ => return,
        };
        let _ = encode_command(out, &reply);
    }

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

    fn release(&self, index: usize) {
        let send = self.sends.lock()[index];
        let outbox = self.outboxes.lock()[send.session].clone();
        let receipt = pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: send.producer_id,
                sequence_id: send.sequence_id,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 1,
                    entry_id: index as u64,
                    ..Default::default()
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        };
        let _ = encode_command(&mut outbox.bytes.lock(), &receipt);
        outbox.notify.notify_one();
    }

    /// Session index and payload length of every publish, in arrival order.
    fn layout(&self) -> Vec<(usize, usize)> {
        self.sends
            .lock()
            .iter()
            .map(|s| (s.session, s.payload_len))
            .collect()
    }

    fn shut(&self) {
        self.stop.store(true, Ordering::SeqCst);
        for outbox in self.outboxes.lock().iter() {
            outbox.notify.notify_one();
        }
    }
}

fn config(policy: MemoryLimitPolicy) -> ConnectionConfig {
    ConnectionConfig {
        memory_limit_bytes: LIMIT,
        memory_limit_policy: policy,
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    }
}

fn producer_request(topic: &str) -> CreateProducerRequest {
    CreateProducerRequest {
        topic: format!("persistent://public/default/{topic}"),
        send_timeout: None,
        ..Default::default()
    }
}

fn msg(len: usize) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from(vec![0x5A; len]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(len).expect("test payload fits u32"),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *fut).poll(cx))).await
}

/// One engine-neutral line per observable step.
fn step(label: &str, outcome: &str) -> String {
    format!("{label}: {outcome}")
}

/// Expand the scenario for one engine. `$connect` builds the client for a
/// given policy; `$exceeded` / `$rejected_by_state_machine` classify the
/// engine's error shapes into the shared vocabulary.
macro_rules! scenario {
    ($connect:expr, $exceeded:expr, $rejected_by_state_machine:expr) => {{
        let mut trace: Vec<String> = Vec::new();

        // --- FailImmediately: one budget across two connections. ---
        let (broker, client) = $connect(MemoryLimitPolicy::FailImmediately).await;
        let open = |topic: &'static str| {
            let client = &client;
            async move {
                tokio::time::timeout(HANG_GUARD, client.open_producer(producer_request(topic)))
                    .await
                    .expect("open_producer did not time out")
                    .expect("open_producer ok")
            }
        };
        let a = open("cwml-equiv-fail-a").await;
        let b = open("cwml-equiv-fail-b").await;
        let mut a1 = Box::pin(a.send(msg(600)));
        trace.push(step(
            "a1 600 B",
            if poll_once(&mut a1).await.is_pending() {
                "queued"
            } else {
                "resolved"
            },
        ));
        assert!(broker.wait_for_sends(1, HANG_GUARD).await);
        let mut b1 = Box::pin(b.send(msg(600)));
        let b1_outcome = match poll_once(&mut b1).await {
            Poll::Ready(Err(err)) => $exceeded(&err).map_or_else(
                || format!("other error {err:?}"),
                |(c, l, r)| format!("rejected current={c} limit={l} requested={r}"),
            ),
            other => format!("unexpected {other:?}"),
        };
        trace.push(step("b1 600 B on the other connection", &b1_outcome));
        tokio::time::timeout(HANG_GUARD, b.clone().close())
            .await
            .expect("close resolves")
            .expect("close acknowledged");
        let mut closed_send = Box::pin(b.send(msg(10)));
        let closed_outcome = match poll_once(&mut closed_send).await {
            Poll::Ready(Err(err)) if $rejected_by_state_machine(&err) => {
                "rejected by the state machine".to_owned()
            }
            other => format!("unexpected {other:?}"),
        };
        trace.push(step("send on closed b", &closed_outcome));
        broker.release(0);
        let a1_done = tokio::time::timeout(HANG_GUARD, a1)
            .await
            .expect("a1 resolves");
        trace.push(step(
            "a1 receipt",
            if a1_done.is_ok() {
                "acknowledged"
            } else {
                "failed"
            },
        ));
        trace.push(format!("layout {:?}", broker.layout()));
        if let Some(driver) = client.take_driver() {
            driver.abort();
        }
        drop(client);
        broker.shut();

        // --- ProducerBlock: park, cancel, cross-connection wake, failure after reserve. ---
        let (broker, client) = $connect(MemoryLimitPolicy::ProducerBlock).await;
        let open = |topic: &'static str| {
            let client = &client;
            async move {
                tokio::time::timeout(HANG_GUARD, client.open_producer(producer_request(topic)))
                    .await
                    .expect("open_producer did not time out")
                    .expect("open_producer ok")
            }
        };
        let a = open("cwml-equiv-block-a").await; // connection 0
        let b = open("cwml-equiv-block-b").await; // connection 1
        let c = open("cwml-equiv-block-c").await; // connection 0
        let d = open("cwml-equiv-block-d").await; // connection 1

        let mut a1 = Box::pin(a.send(msg(600)));
        assert!(poll_once(&mut a1).await.is_pending());
        assert!(broker.wait_for_sends(1, HANG_GUARD).await);

        let mut parked = Box::pin(b.send(msg(600)));
        let first = poll_once(&mut parked).await.is_pending();
        let second = poll_once(&mut parked).await.is_pending();
        drop(parked);
        trace.push(step(
            "b parked, re-polled, cancelled",
            &format!("pending={first}/{second}"),
        ));

        let mut doomed = Box::pin(b.send(msg(600)));
        assert!(poll_once(&mut doomed).await.is_pending());
        tokio::time::timeout(HANG_GUARD, b.clone().close())
            .await
            .expect("close resolves")
            .expect("close acknowledged");
        broker.release(0);
        let a1_done = tokio::time::timeout(HANG_GUARD, a1)
            .await
            .expect("a1 resolves");
        trace.push(step(
            "a1 receipt",
            if a1_done.is_ok() {
                "acknowledged"
            } else {
                "failed"
            },
        ));
        let doomed_outcome = match poll_once(&mut doomed).await {
            Poll::Ready(Err(err)) if $rejected_by_state_machine(&err) => {
                "reserved, then rejected by the state machine".to_owned()
            }
            other => format!("unexpected {other:?}"),
        };
        trace.push(step("b parked on a closed producer", &doomed_outcome));

        // Nothing leaked: the full budget is free again.
        let mut c1 = Box::pin(c.send(msg(1000)));
        trace.push(step(
            "c1 1000 B",
            if poll_once(&mut c1).await.is_pending() {
                "queued"
            } else {
                "resolved"
            },
        ));
        assert!(broker.wait_for_sends(2, HANG_GUARD).await);

        // A release on connection 0 wakes a send parked for connection 1.
        let d1 = tokio::spawn(d.send(msg(600)));
        let held = !broker.wait_for_sends(3, SETTLE).await;
        trace.push(step(
            "d1 600 B while c1 holds the budget",
            if held { "parked" } else { "sent" },
        ));
        broker.release(1);
        let c1_done = tokio::time::timeout(HANG_GUARD, c1)
            .await
            .expect("c1 resolves");
        trace.push(step(
            "c1 receipt",
            if c1_done.is_ok() {
                "acknowledged"
            } else {
                "failed"
            },
        ));
        assert!(
            broker.wait_for_sends(3, HANG_GUARD).await,
            "d1 woken across connections"
        );
        broker.release(2);
        let d1_done = tokio::time::timeout(HANG_GUARD, d1)
            .await
            .expect("d1 resolves")
            .expect("d1 task");
        trace.push(step(
            "d1 receipt",
            if d1_done.is_ok() {
                "acknowledged"
            } else {
                "failed"
            },
        ));
        trace.push(format!("layout {:?}", broker.layout()));
        if let Some(driver) = client.take_driver() {
            driver.abort();
        }
        drop(client);
        broker.shut();
        trace
    }};
}

async fn tokio_trace() -> Vec<String> {
    use magnetar_runtime_tokio::{Client, ClientError};

    async fn connect(policy: MemoryLimitPolicy) -> (Arc<Broker>, Client) {
        let (host_port, broker) = Broker::spawn().await;
        let client = tokio::time::timeout(
            HANG_GUARD,
            Client::connect(&format!("pulsar://{host_port}"), config(policy)),
        )
        .await
        .expect("tokio connect did not time out")
        .expect("tokio connect ok")
        .with_connections_per_broker(2);
        (broker, client)
    }
    let exceeded = |err: &ClientError| match err {
        ClientError::MemoryLimitExceeded {
            current,
            limit,
            requested,
        } => Some((*current, *limit, *requested)),
        _ => None,
    };
    let rejected = |err: &ClientError| matches!(err, ClientError::Protocol(_));
    scenario!(connect, exceeded, rejected)
}

async fn moonpool_trace() -> Vec<String> {
    use magnetar_runtime_moonpool::{Client, ClientError, EngineError, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    async fn connect(policy: MemoryLimitPolicy) -> (Arc<Broker>, Client<TokioProviders>) {
        let (host_port, broker) = Broker::spawn().await;
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client = tokio::time::timeout(
            HANG_GUARD,
            Client::connect_plain_supervised(&engine, &host_port, config(policy), None, None),
        )
        .await
        .expect("moonpool connect did not time out")
        .expect("moonpool connect ok")
        .with_connections_per_broker(2);
        (broker, client)
    }
    let exceeded = |err: &ClientError| match err {
        ClientError::Engine(EngineError::MemoryLimitExceeded {
            current,
            limit,
            requested,
        }) => Some((*current, *limit, *requested)),
        _ => None,
    };
    let rejected = |err: &ClientError| matches!(err, ClientError::Other(message) if message.starts_with("send:"));
    scenario!(connect, exceeded, rejected)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_wide_memory_limit_is_engine_equivalent() {
    let tokio = tokio_trace().await;
    // Same `LocalSet` isolation as the sibling `connections_per_broker`
    // equivalence test; `TokioProviders` delegates Send tasks to `tokio::spawn`.
    let local = tokio::task::LocalSet::new();
    let moonpool = local.run_until(moonpool_trace()).await;

    let expected = vec![
        "a1 600 B: queued".to_owned(),
        "b1 600 B on the other connection: rejected current=600 limit=1000 requested=600"
            .to_owned(),
        "send on closed b: rejected by the state machine".to_owned(),
        "a1 receipt: acknowledged".to_owned(),
        "layout [(0, 600)]".to_owned(),
        "b parked, re-polled, cancelled: pending=true/true".to_owned(),
        "a1 receipt: acknowledged".to_owned(),
        "b parked on a closed producer: reserved, then rejected by the state machine".to_owned(),
        "c1 1000 B: queued".to_owned(),
        "d1 600 B while c1 holds the budget: parked".to_owned(),
        "c1 receipt: acknowledged".to_owned(),
        "d1 receipt: acknowledged".to_owned(),
        "layout [(0, 600), (0, 1000), (1, 600)]".to_owned(),
    ];
    assert_eq!(tokio, expected, "tokio trace");
    assert_eq!(
        tokio, moonpool,
        "tokio and moonpool must walk the client-wide budget identically"
    );
}
