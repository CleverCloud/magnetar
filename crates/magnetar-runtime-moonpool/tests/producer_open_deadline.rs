// SPDX-License-Identifier: Apache-2.0

//! Issue #887 — a `CommandProducer` the broker never answers must reach a deadline —
//! moonpool engine twin of
//! `crates/magnetar-runtime-tokio/tests/producer_open_deadline.rs`
//! (ADR-0024 cross-runtime test parity, 1:1 test names).
//!
//! [ADR-0106](../../../specs/adr/0106-reattach-broker-closed-producer-in-place.md) recorded
//! this residual and declined to fix it: `Connection::handle_timeout` swept
//! `PendingRequestKind::Ack` alone, so a re-attach the broker never answered parked the slot
//! at `broker_ready = false, open_request_id = Some(_)` for the life of the connection. Every
//! publish routed there resolved `code=-1 "send timeout"` forever — magnetar 1.7.2 lost its
//! publishers on 2 of 12 partitions that way — and every LATER broker close was refused an
//! in-place re-attach, because `open_request_id.is_some()` is a refusal in
//! `producer_reattach_in_place_is_eligible`. ADR-0125 gives the pending open the connection's
//! `operation_timeout` as its deadline.
//!
//! The recovery lives entirely in the sans-io `magnetar_proto::Connection`, so this twin also
//! gives the ADR-0024 `check-sim-coverage` gate its reach into the new `conn.rs` lines (only
//! the moonpool and differential binaries are measured). The two tests cover the sweep's two
//! dispositions: one more ADR-0080 retry leg while the budget allows it, and terminalization
//! once it does not.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, OperationRetryConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;
use common::HANG_GUARD;

/// Shorter than the default 30 s so the sweep fires inside a test, and shorter than the
/// default 30 s keepalive interval so only the pending-open arm can have produced the
/// deadline the driver wakes on.
const OPEN_TIMEOUT: Duration = Duration::from_secs(2);

/// What the mock broker observed, shared with the test body.
#[derive(Default)]
struct BrokerLog {
    /// `epoch` of every `CommandProducer` received, in wire order.
    producer_opens: Mutex<Vec<Option<u64>>>,
    /// How many TCP connections the broker accepted (a reconnect would be 2).
    accepts: AtomicUsize,
}

/// Per-connection broker behaviour knobs.
#[derive(Clone, Copy)]
struct BrokerBehaviour {
    /// How many re-attach `CommandProducer`s (`epoch >= 1`) to receive and never answer —
    /// neither `ProducerSuccess` nor `CommandError`. This is the #887 shape: a reply lost
    /// broker-side leaves the client holding a pending request forever.
    swallowed_reattaches: usize,
}

/// Mutable per-connection broker state.
struct BrokerSession {
    /// Producer ids the broker currently considers attached.
    live_producers: std::collections::HashSet<u64>,
    /// Whether the one-shot close after the first receipt already fired.
    closed_once: bool,
    /// How many re-attaches have been swallowed so far.
    swallowed: usize,
}

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-producer-open-deadline".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "producer-open-deadline".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(out: &mut BytesMut, producer_id: u64, sequence_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id: 5,
                entry_id: sequence_id,
                ..Default::default()
            }),
            highest_sequence_id: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// The broker-initiated close of an ATTACHED producer on a live socket — what
/// `ServerCnx.closeProducer` writes after `safelyRemoveProducer`.
fn emit_close_producer(out: &mut BytesMut, producer_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseProducer as i32,
        close_producer: Some(pb::CommandCloseProducer {
            producer_id,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn answer_frame(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    log: &BrokerLog,
    behaviour: BrokerBehaviour,
    session: &mut BrokerSession,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(out, l.request_id);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                log.producer_opens.lock().push(p.epoch);
                let is_reattach = p.epoch.is_some_and(|e| e >= 1);
                if is_reattach && session.swallowed < behaviour.swallowed_reattaches {
                    // Received, never answered. The client is now holding a pending
                    // `ProducerOpen` nothing on the wire will ever resolve.
                    session.swallowed = session.swallowed.saturating_add(1);
                } else {
                    session.live_producers.insert(p.producer_id);
                    emit_producer_success(out, p.request_id);
                }
            }
        }
        pb::base_command::Type::Send => {
            if let Some(s) = &frame.command.send {
                if !session.live_producers.contains(&s.producer_id) {
                    // Pulsar's `recentlyClosedProducers` path: a send for a just-closed
                    // producer id is silently dropped.
                    return;
                }
                emit_send_receipt(out, s.producer_id, s.sequence_id);
                if !session.closed_once {
                    // The bundle is unloaded: detach the producer id and tell the client,
                    // on the SAME live connection.
                    session.closed_once = true;
                    session.live_producers.remove(&s.producer_id);
                    emit_close_producer(out, s.producer_id);
                }
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
                session.live_producers.remove(&c.producer_id);
                emit_success(out, c.request_id);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                emit_success(out, c.request_id);
            }
        }
        _ => {}
    }
}

async fn run_broker_conn(
    stream: &mut tokio::net::TcpStream,
    log: &BrokerLog,
    behaviour: BrokerBehaviour,
) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out = BytesMut::with_capacity(8 * 1024);
    let mut session = BrokerSession {
        live_producers: std::collections::HashSet::new(),
        closed_once: false,
        swallowed: 0,
    };
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            answer_frame(&frame, &mut out, log, behaviour, &mut session);
        }
        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out.clear();
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Bind a broker and return its `host:port` (the moonpool engine's `connect_plain` takes a
/// bare address, not a `pulsar://` URL).
async fn spawn_broker(behaviour: BrokerBehaviour) -> (String, Arc<BrokerLog>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let log = Arc::new(BrokerLog::default());
    let log_task = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            log_task.accepts.fetch_add(1, Ordering::SeqCst);
            let log_conn = log_task.clone();
            tokio::spawn(async move {
                run_broker_conn(&mut stream, &log_conn, behaviour).await;
            });
        }
    });
    (addr, log)
}

fn deadline_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: OPEN_TIMEOUT,
        ..ConnectionConfig::default()
    }
}

/// REPRODUCTION (issue #887): the broker closes an attached producer on a live connection
/// and then drops the reply to the in-place re-attach. Before ADR-0125 nothing bounded that
/// pending `CommandProducer`: the slot kept its shut drain gate and `open_request_id` for
/// the life of the connection and every publish routed there resolved
/// `code=-1 "send timeout"`. The deadline must turn it into one more ADR-0080 retry leg,
/// which re-issues the open and recovers on the SAME connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_recovers_after_an_unanswered_reattach_reaches_its_deadline() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (addr, log) = spawn_broker(BrokerBehaviour {
                swallowed_reattaches: 1,
            })
            .await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain(&engine, &addr, deadline_config()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok")
            .with_operation_retry(OperationRetryConfig {
                initial_backoff: Duration::from_millis(200),
                max_backoff: Duration::from_millis(200),
                max_retries: Some(3),
            });
            let producer = tokio::time::timeout(
                HANG_GUARD,
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/open-deadline-887-moonpool".to_owned(),
                    send_timeout: Some(Duration::from_secs(120)),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            tokio::time::timeout(HANG_GUARD, producer.send(outgoing(b"first")))
                .await
                .expect("first send did not time out")
                .expect("first send ok");

            // The broker has closed the producer and swallowed the re-attach. This send
            // used to hang until its own `send_timeout` and then fail forever; it can only
            // resolve `Ok` if the unanswered open reached a deadline and was re-issued.
            tokio::time::timeout(
                Duration::from_secs(30),
                producer.send(outgoing(b"after-swallowed-reattach")),
            )
            .await
            .expect(
                "send WEDGED: the unanswered re-attach carried no deadline, so the slot kept \
                 a shut drain gate and a pending open forever (issue #887)",
            )
            .expect("post-close send must resolve Ok once the re-issued open is acked");

            assert_eq!(
                *log.producer_opens.lock(),
                vec![None, Some(1), Some(2)],
                "initial open, the swallowed re-attach at epoch 1, then the deadline's own \
                 retry leg re-issuing at epoch 2"
            );
            assert_eq!(
                log.accepts.load(Ordering::SeqCst),
                1,
                "the expiry re-issues on the SAME connection — no reconnect"
            );
            assert!(
                producer.last_disconnected_timestamp().is_none(),
                "an unanswered open is not a transport disconnect"
            );
            assert!(
                producer.is_ready(),
                "the re-issued open's ack restores readiness"
            );
            client.close().await;
        })
        .await;
}

/// #887 terminal branch: when the configured `OperationRetryConfig` refuses another
/// re-issue, the expiry terminalizes the slot through `fail_producer_open_with_broker_error`
/// instead of arming a leg nobody would honour. The parked publish resolves `Err` on the
/// open's deadline rather than hanging for its own far longer `send_timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_reattach_fails_the_send_once_the_retry_budget_is_exhausted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (addr, log) = spawn_broker(BrokerBehaviour {
                swallowed_reattaches: usize::MAX,
            })
            .await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain(&engine, &addr, deadline_config()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok")
            .with_operation_retry(OperationRetryConfig {
                initial_backoff: Duration::from_millis(200),
                max_backoff: Duration::from_millis(200),
                max_retries: Some(0),
            });
            let producer = tokio::time::timeout(
                HANG_GUARD,
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/open-deadline-887-moonpool-terminal"
                        .to_owned(),
                    // Two orders of magnitude above the open deadline: an `Err` inside the
                    // window below cannot have come from the send timeout.
                    send_timeout: Some(Duration::from_secs(120)),
                    ..Default::default()
                }),
            )
            .await
            .expect("open_producer did not time out")
            .expect("open_producer ok");

            tokio::time::timeout(HANG_GUARD, producer.send(outgoing(b"first")))
                .await
                .expect("first send did not time out")
                .expect("first send ok");

            let outcome = tokio::time::timeout(
                Duration::from_secs(30),
                producer.send(outgoing(b"after-swallowed-reattach")),
            )
            .await
            .expect(
                "send WEDGED: with no deadline on the pending open the publish waits out its \
                 own send_timeout and the slot never surfaces a disposition (issue #887)",
            );
            assert!(
                outcome.is_err(),
                "an exhausted retry budget must surface the failure, not an Ok: {outcome:?}"
            );

            assert_eq!(
                *log.producer_opens.lock(),
                vec![None, Some(1)],
                "the initial open plus exactly one re-attach: an exhausted budget arms no \
                 further leg"
            );
            assert_eq!(
                log.accepts.load(Ordering::SeqCst),
                1,
                "terminalizing one producer must not re-dial the connection"
            );
            client.close().await;
        })
        .await;
}
