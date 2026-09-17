// SPDX-License-Identifier: Apache-2.0

//! Issue #451 — a broker `CommandCloseProducer` on a connection that STAYS UP
//! must re-attach the producer in place — tokio engine, driven over a real
//! loopback broker and the production driver loop.
//!
//! `pulsar-admin topics unload` of one partition makes the owning broker write
//! `CommandCloseProducer` and detach the producer id while the TCP connection
//! keeps serving every other producer and consumer on it. No `Connection::reset`
//! ever runs, so `rebuild_producers` never fires; before ADR-0106 the slot stayed
//! at `broker_ready = false` for the life of the connection and every `send()`
//! routed to it resolved `ClientError::Broker { code: -1, message: "send timeout" }`
//! until the process restarted.
//!
//! Three scenarios:
//!
//! 1. `send_recovers_after_same_broker_close_producer_without_reconnect`: the broker closes the
//!    producer right after the first `CommandSendReceipt`; the next `send()` must resolve `Ok` on
//!    the SAME connection.
//! 2. `send_recovers_when_first_reattach_is_rejected_service_not_ready`: the broker answers the
//!    first re-attach with `CommandError { ServiceNotReady }` — the real shape while the bundle
//!    reloads — so recovery must ride the ADR-0080 operation-retry leg (delay + lookup + re-issue).
//! 3. `close_during_in_flight_open_still_fails_the_open`: the refusal branch — a close landing
//!    while the producer's first open is in flight must leave that open's parked waiter in charge
//!    and emit no second `CommandProducer`.
//!
//! Each test pairs with a same-named test on the moonpool side
//! (`crates/magnetar-runtime-moonpool/tests/producer_broker_close_reattach.rs`)
//! so `cargo xtask check-runtime-test-parity` stays balanced 1:1 (ADR-0024).
//! Layer (b) of the four-layer test policy.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, FrameError, OperationRetryConfig,
    ProducerHandle, RequestId, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ConnectionShared};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;
use common::HANG_GUARD;

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
    /// Answer the FIRST re-attach (`epoch >= 1`) with `ServiceNotReady`.
    reject_first_reattach: bool,
}

/// Mutable per-connection broker state.
struct BrokerSession {
    /// Producer ids the broker currently considers attached.
    live_producers: std::collections::HashSet<u64>,
    /// Whether the one-shot close after the first receipt already fired.
    closed_once: bool,
    /// Whether the one-shot re-attach rejection already fired.
    rejected_once: bool,
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-producer-broker-close".to_owned(),
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
            producer_name: "producer-broker-close".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_service_not_ready(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::ServiceNotReady as i32,
            message: "namespace bundle is being unloaded".to_owned(),
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
                if behaviour.reject_first_reattach && is_reattach && !session.rejected_once {
                    // The bundle is still reloading: exactly what a real broker
                    // answers a `CommandProducer` arriving mid-unload.
                    session.rejected_once = true;
                    emit_service_not_ready(out, p.request_id);
                } else {
                    session.live_producers.insert(p.producer_id);
                    emit_producer_success(out, p.request_id);
                }
            }
        }
        pb::base_command::Type::Send => {
            if let Some(s) = &frame.command.send {
                if !session.live_producers.contains(&s.producer_id) {
                    // Pulsar's `recentlyClosedProducers` path: a send for a
                    // just-closed producer id is silently dropped.
                    return;
                }
                emit_send_receipt(out, s.producer_id, s.sequence_id);
                if !session.closed_once {
                    // The bundle is unloaded: detach the producer id and tell
                    // the client, on the SAME live connection.
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
        rejected_once: false,
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

async fn spawn_broker(behaviour: BrokerBehaviour) -> (String, Arc<BrokerLog>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
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
    (format!("pulsar://{addr}"), log)
}

/// REPRODUCTION (issue #451): the broker closes an attached producer on a
/// connection that stays up. The next `send()` can only succeed if the client
/// re-attaches the producer in place. Before ADR-0106 it resolved
/// `Err(Broker { code: -1, message: "send timeout" })` after the configured
/// `send_timeout` and stayed broken for the life of the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_recovers_after_same_broker_close_producer_without_reconnect() {
    let (url, log) = spawn_broker(BrokerBehaviour {
        reject_first_reattach: false,
    })
    .await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/reattach-451-tokio".to_owned(),
            send_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    tokio::time::timeout(HANG_GUARD, producer.send_bytes(&b"first"[..]))
        .await
        .expect("first send did not time out")
        .expect("first send ok");

    // The broker has now closed the producer on the live socket. This send is
    // the one that used to fail `code=-1 "send timeout"` forever.
    tokio::time::timeout(
        Duration::from_secs(10),
        producer.send_bytes(&b"after-close"[..]),
    )
    .await
    .expect(
        "send WEDGED after a broker CommandCloseProducer on a live connection: the producer \
             was never re-attached (issue #451)",
    )
    .expect("post-close send must resolve Ok after the in-place re-attach");

    assert_eq!(
        *log.producer_opens.lock(),
        vec![None, Some(1)],
        "the broker must see the initial open (no epoch) plus exactly one in-place re-attach \
         at epoch 1"
    );
    assert_eq!(
        log.accepts.load(Ordering::SeqCst),
        1,
        "the re-attach happens on the SAME connection — no reconnect"
    );
    assert!(
        producer.last_disconnected_timestamp().is_none(),
        "a broker producer close is not a transport disconnect"
    );
    assert_eq!(
        producer.stats().total_send_failed,
        0,
        "no send may be failed by the re-attach"
    );
    client.close().await;
}

/// #451 primary real-world path: the broker fences the topic before writing the
/// close, so the immediate re-issue is answered `ServiceNotReady`. Recovery must
/// ride the ADR-0080 operation-retry leg (backoff + lookup + re-issue) and still
/// land on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_recovers_when_first_reattach_is_rejected_service_not_ready() {
    let (url, log) = spawn_broker(BrokerBehaviour {
        reject_first_reattach: true,
    })
    .await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok")
    .with_operation_retry(OperationRetryConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(50),
        max_retries: Some(3),
    });
    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/reattach-451-tokio-retry".to_owned(),
            send_timeout: Some(Duration::from_secs(10)),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    tokio::time::timeout(HANG_GUARD, producer.send_bytes(&b"first"[..]))
        .await
        .expect("first send did not time out")
        .expect("first send ok");

    tokio::time::timeout(HANG_GUARD, producer.send_bytes(&b"after-close"[..]))
        .await
        .expect(
            "send WEDGED: the ServiceNotReady-rejected re-attach never rode the operation-retry \
             leg (issue #451)",
        )
        .expect("post-close send must resolve Ok once the retried re-attach is acked");

    assert_eq!(
        *log.producer_opens.lock(),
        vec![None, Some(1), Some(2)],
        "initial open, the rejected re-attach at epoch 1, then the retry leg's re-issue at \
         epoch 2"
    );
    assert_eq!(
        log.accepts.load(Ordering::SeqCst),
        1,
        "the retry leg re-issues on the SAME connection — no reconnect"
    );
    assert!(
        producer.last_disconnected_timestamp().is_none(),
        "a rejected re-attach is not a transport disconnect"
    );
    client.close().await;
}

/// #451 refusal branch: a close landing while the producer's FIRST open is
/// still in flight must leave that open's parked waiter in charge — no second
/// `CommandProducer` (which would leave one of the two replies unmatched) and
/// the `ProducerClosedByBroker` event still surfaces for the waiter to read.
///
/// Driven directly over the locked `Connection` (the `ack_orphan_close.rs`
/// idiom): the branch is about frames and events, not about I/O.
#[test]
fn close_during_in_flight_open_still_fails_the_open() {
    let t0 = std::time::Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    let mut connected = BytesMut::new();
    emit_connected(&mut connected);
    conn.handle_bytes(t0, &connected).expect("connected");
    while conn.poll_event().is_some() {}

    let open_rid = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reattach-451-tokio-in-flight".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();

    let mut close = BytesMut::new();
    emit_close_producer(&mut close, handle.0);
    conn.handle_bytes(t0, &close)
        .expect("handle close mid-open");

    let mut out = conn.poll_transmit();
    let mut reopens = 0_usize;
    while !out.is_empty() {
        let frame = decode_one(&mut out).expect("decode outbound");
        if frame.command.r#type == pb::base_command::Type::Producer as i32 {
            reopens += 1;
        }
    }
    assert_eq!(
        reopens, 0,
        "the open already in flight owns the next ProducerSuccess: no second CommandProducer"
    );

    let mut saw_close_event = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ProducerClosedByBroker { handle: h, .. } = ev {
            if h == ProducerHandle(handle.0) {
                saw_close_event = true;
            }
        }
    }
    assert!(
        saw_close_event,
        "while an open is in flight its parked waiter owns the outcome, so the event must \
         still surface"
    );
    assert!(
        conn.producer_open_retry_is_current(handle, RequestId(open_rid)),
        "the in-flight open keeps owning the active generation"
    );
}
