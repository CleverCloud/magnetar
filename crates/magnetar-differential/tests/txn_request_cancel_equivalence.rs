// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer 4 — tokio ↔ moonpool equivalence of a cancelled transaction request.
//!
//! A transaction request (`AddPartitionToTxn` here) whose caller gives up before the broker
//! answers must leave nothing behind in the connection, on either engine:
//!
//! * no waker parked once the call is dropped, and
//! * no outcome recorded when the late reply arrives, because nobody can ever take it.
//!
//! The call goes through the façade's [`magnetar::TransactionApi`] on each runtime client, and
//! both delegate to their runtime. The moonpool façade used to keep its own copy of the request
//! future for this call, with no drop hook, so its leg also covers that delegation.
//!
//! Both engines speak to a loopback broker that answers `Connect` and `Ping` and parks the
//! `AddPartitionToTxn` reply until the test releases it. The reply is followed by a `Ping`, and
//! the client's `Pong` can only follow its handling of the reply, so the test knows the late
//! reply has been processed without sleeping. No live Pulsar is needed.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar::TransactionApi;
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    Connection, ConnectionConfig, Frame, FrameError, OpOutcome, PendingOpKey, RequestId, TxnAction,
    TxnId, decode_one, encode_command, pb,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const TOPIC: &str = "persistent://public/default/txn-request-cancel-equivalence";

/// What a cancelled-then-answered transaction request left in the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CancelTrace {
    /// Wakers parked in the connection once the call was dropped, beyond the baseline.
    wakers_parked_after_drop: usize,
    /// Whether the late reply was recorded as an outcome for the cancelled request.
    outcome_stranded_by_late_reply: bool,
}

/// Test-side view of the broker stub; the stub holds clones of the shared parts.
#[derive(Clone)]
struct DeferredTxnBroker {
    addr: String,
    /// Request id of the `CommandAddPartitionToTxn` the broker is holding.
    held_request_id: Arc<Mutex<Option<u64>>>,
    /// Signalled once the broker has read the request.
    received: Arc<Notify>,
    /// Signalled by the test to let the broker send its reply.
    release: Arc<Notify>,
    /// Signalled once the client answered the `Ping` that followed the reply.
    answered: Arc<Notify>,
}

fn command(ty: pb::base_command::Type) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: ty as i32,
        ..Default::default()
    }
}

impl DeferredTxnBroker {
    /// React to one frame; `reply_sent` flips once the held reply and its `Ping` are queued.
    async fn handle_frame(&self, frame: &Frame, out: &mut BytesMut, reply_sent: &mut bool) {
        let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
            return;
        };
        let reply = match kind {
            pb::base_command::Type::Connect => Some(pb::BaseCommand {
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-txn-cancel-equivalence".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..command(pb::base_command::Type::Connected)
            }),
            pb::base_command::Type::Ping => Some(pb::BaseCommand {
                pong: Some(pb::CommandPong {}),
                ..command(pb::base_command::Type::Pong)
            }),
            pb::base_command::Type::AddPartitionToTxn => {
                let request_id = frame
                    .command
                    .add_partition_to_txn
                    .as_ref()
                    .map(|add| add.request_id)
                    .expect("AddPartitionToTxn payload");
                *self.held_request_id.lock() = Some(request_id);
                self.received.notify_one();
                self.release.notified().await;
                encode_command(
                    out,
                    &pb::BaseCommand {
                        add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
                            request_id,
                            txnid_most_bits: None,
                            txnid_least_bits: None,
                            error: None,
                            message: None,
                        }),
                        ..command(pb::base_command::Type::AddPartitionToTxnResponse)
                    },
                )
                .expect("encode AddPartitionToTxnResponse");
                *reply_sent = true;
                // The client answers this only after it has handled the reply above.
                Some(pb::BaseCommand {
                    ping: Some(pb::CommandPing {}),
                    ..command(pb::base_command::Type::Ping)
                })
            }
            pb::base_command::Type::Pong if *reply_sent => {
                self.answered.notify_one();
                None
            }
            _ => None,
        };
        if let Some(reply) = reply {
            encode_command(out, &reply).expect("encode broker reply");
        }
    }

    /// Serve one connection until the peer goes away.
    async fn serve(self, mut stream: TcpStream) {
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        let mut out_buf = BytesMut::with_capacity(8 * 1024);
        let mut reply_sent = false;
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
                self.handle_frame(&frame, &mut out_buf, &mut reply_sent)
                    .await;
            }
            if !out_buf.is_empty() {
                if stream.write_all(&out_buf).await.is_err() || stream.flush().await.is_err() {
                    return;
                }
                out_buf.clear();
            }
            match stream.read_buf(&mut read_buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }
}

async fn spawn_deferred_txn_broker() -> DeferredTxnBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let broker = DeferredTxnBroker {
        addr: listener.local_addr().expect("local_addr").to_string(),
        held_request_id: Arc::new(Mutex::new(None)),
        received: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        answered: Arc::new(Notify::new()),
    };
    let serving = broker.clone();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            serving.serve(stream).await;
        }
    });
    broker
}

/// Issue `add_partition_to_txn` through the façade trait, drop the call once the broker holds
/// the request, let the late reply through, and report what the connection kept.
///
/// `parked_wakers` and `has_outcome` read the engine's connection.
async fn observe_cancelled_txn_request<C>(
    client: &C,
    broker: &DeferredTxnBroker,
    parked_wakers: impl Fn() -> usize,
    has_outcome: impl Fn(RequestId) -> bool,
) -> CancelTrace
where
    C: TransactionApi,
{
    let baseline = parked_wakers();
    {
        let mut call =
            Box::pin(client.add_partition_to_txn(TxnId::new(0x11, 0x22), TOPIC.to_owned()));
        tokio::select! {
            outcome = &mut call => {
                panic!("the broker holds its reply, so the call must still be pending: {outcome:?}")
            }
            received = tokio::time::timeout(HANG_GUARD, broker.received.notified()) => {
                received.expect("the broker received the AddPartitionToTxn request");
            }
        }
        // `call` is dropped here: the caller gave up before the reply.
    }
    let wakers_parked_after_drop = parked_wakers().saturating_sub(baseline);

    broker.release.notify_one();
    tokio::time::timeout(HANG_GUARD, broker.answered.notified())
        .await
        .expect("the client handled the reply and answered the broker's Ping");
    let request_id = RequestId(
        broker
            .held_request_id
            .lock()
            .expect("the broker saw the request id"),
    );
    CancelTrace {
        wakers_parked_after_drop,
        outcome_stranded_by_late_reply: has_outcome(request_id),
    }
}

async fn run_tokio_cancelled_txn_request() -> CancelTrace {
    let broker = spawn_deferred_txn_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar::runtime_tokio::Client::connect(
            &format!("pulsar://{}", broker.addr),
            ConnectionConfig::default(),
        ),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let shared = client.shared().clone();
    let trace = observe_cancelled_txn_request(
        &client,
        &broker,
        || shared.inner.lock().pending_waker_count(),
        |request_id| {
            shared
                .inner
                .lock()
                .take_outcome(PendingOpKey::Request(request_id))
                .is_some()
        },
    )
    .await;
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    trace
}

async fn run_moonpool_cancelled_txn_request() -> CancelTrace {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let broker = spawn_deferred_txn_broker().await;
            let engine = magnetar::runtime_moonpool::MoonpoolEngine::new(
                moonpool_core::TokioProviders::new(),
            );
            let client = tokio::time::timeout(
                HANG_GUARD,
                magnetar::runtime_moonpool::Client::connect_plain(
                    &engine,
                    &broker.addr,
                    ConnectionConfig::default(),
                ),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");
            let shared = client.shared().clone();
            let trace = observe_cancelled_txn_request(
                &client,
                &broker,
                || shared.inner.lock().pending_waker_count(),
                |request_id| {
                    shared
                        .inner
                        .lock()
                        .take_outcome(PendingOpKey::Request(request_id))
                        .is_some()
                },
            )
            .await;
            client.close().await;
            trace
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_txn_request_leaves_nothing_behind_on_either_engine() {
    let tokio_trace = run_tokio_cancelled_txn_request().await;
    let moonpool_trace = run_moonpool_cancelled_txn_request().await;
    let mut left_behind = Vec::new();
    for (engine, trace) in [("tokio", &tokio_trace), ("moonpool", &moonpool_trace)] {
        if trace.wakers_parked_after_drop != 0 {
            left_behind.push(format!(
                "{engine}: {} waker(s) still parked after the call was dropped",
                trace.wakers_parked_after_drop
            ));
        }
        if trace.outcome_stranded_by_late_reply {
            left_behind.push(format!(
                "{engine}: the late reply was recorded as an outcome nobody can take"
            ));
        }
    }
    assert!(
        left_behind.is_empty(),
        "a cancelled transaction request left state behind in the connection: {left_behind:?}"
    );
    assert_eq!(
        tokio_trace, moonpool_trace,
        "both engines must leave the same state behind a cancelled transaction request"
    );
}

/// The transaction requests. The reply of the first three answers exactly one caller; `EndTxn`
/// is only reset here, not cancelled: its outcome is retained across a cancelled waiter by
/// contract, so a retried commit or abort resumes the canonical request.
#[derive(Debug, Clone, Copy)]
enum TxnRequestKind {
    NewTxn,
    AddPartition,
    AddSubscription,
    EndTxn,
}

const TXN_REQUEST_KINDS: [TxnRequestKind; 4] = [
    TxnRequestKind::NewTxn,
    TxnRequestKind::AddPartition,
    TxnRequestKind::AddSubscription,
    TxnRequestKind::EndTxn,
];

/// What the connection kept behind cancelled and reset transaction requests, per kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BoundaryTrace {
    /// Kinds whose reply, landing after the caller cancelled, was recorded as an outcome.
    stranded_after_cancel: Vec<String>,
    /// Kinds whose `TxnClient` per-request entry outlived a session reset.
    retained_after_reset: Vec<String>,
    /// Kinds whose `TxnClient` per-request entry outlived a reset that followed a cancel (an
    /// operation deadline that fired during the outage).
    retained_after_cancel_and_reset: Vec<String>,
}

/// Run `f` on a freshly handshaked connection of the tokio engine.
fn on_tokio_connection(f: &mut dyn FnMut(&mut Connection)) {
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn);
    f(&mut conn);
}

/// Run `f` on a freshly handshaked connection of the moonpool engine.
fn on_moonpool_connection(f: &mut dyn FnMut(&mut Connection)) {
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    handshake(&mut conn);
    f(&mut conn);
}

fn handshake(conn: &mut Connection) {
    conn.begin_handshake().expect("begin handshake");
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-txn-cancel-equivalence".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..command(pb::base_command::Type::Connected)
        },
    )
    .expect("encode Connected");
    conn.handle_bytes(Instant::now(), &frame)
        .expect("handle Connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

fn issue(conn: &mut Connection, kind: TxnRequestKind) -> RequestId {
    let txn = TxnId::new(0x11, 0x22);
    match kind {
        TxnRequestKind::NewTxn => conn.new_txn(Duration::from_mins(1)),
        TxnRequestKind::AddPartition => conn.add_partition_to_txn(txn, TOPIC.to_owned()),
        TxnRequestKind::AddSubscription => {
            conn.add_subscription_to_txn(txn, "txn-leak-sub".to_owned(), TOPIC.to_owned())
        }
        TxnRequestKind::EndTxn => conn
            .end_txn(txn, TxnAction::Commit)
            .expect("a first EndTxn is issuable"),
    }
}

/// Feed the broker's successful reply to the request `request_id` of `kind`.
fn feed_reply(conn: &mut Connection, kind: TxnRequestKind, request_id: RequestId) {
    let reply = match kind {
        TxnRequestKind::NewTxn => pb::BaseCommand {
            new_txn_response: Some(pb::CommandNewTxnResponse {
                request_id: request_id.0,
                txnid_most_bits: Some(0x11),
                txnid_least_bits: Some(0x22),
                error: None,
                message: None,
            }),
            ..command(pb::base_command::Type::NewTxnResponse)
        },
        TxnRequestKind::AddPartition => pb::BaseCommand {
            add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
                request_id: request_id.0,
                txnid_most_bits: None,
                txnid_least_bits: None,
                error: None,
                message: None,
            }),
            ..command(pb::base_command::Type::AddPartitionToTxnResponse)
        },
        TxnRequestKind::AddSubscription => pb::BaseCommand {
            add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
                request_id: request_id.0,
                txnid_most_bits: None,
                txnid_least_bits: None,
                error: None,
                message: None,
            }),
            ..command(pb::base_command::Type::AddSubscriptionToTxnResponse)
        },
        TxnRequestKind::EndTxn => pb::BaseCommand {
            end_txn_response: Some(pb::CommandEndTxnResponse {
                request_id: request_id.0,
                txnid_least_bits: Some(0x22),
                txnid_most_bits: Some(0x11),
                error: None,
                message: None,
            }),
            ..command(pb::base_command::Type::EndTxnResponse)
        },
    };
    let mut frame = BytesMut::new();
    encode_command(&mut frame, &reply).expect("encode transaction reply");
    conn.handle_bytes(Instant::now(), &frame)
        .expect("handle transaction reply");
}

/// Whether the embedded `TxnClient` still tracks the request `request_id` of `kind`.
///
/// `TxnClient::register_*_waker` stores a waker only for a request it still tracks, so a probe
/// waker that the client kept a clone of proves the per-request entry is still there.
fn txn_client_tracks(conn: &mut Connection, kind: TxnRequestKind, request_id: RequestId) -> bool {
    #[derive(Default)]
    struct Probe(AtomicUsize);
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let probe = Arc::new(Probe::default());
    let waker = Waker::from(Arc::clone(&probe));
    let client = conn.txn_client_mut();
    match kind {
        TxnRequestKind::NewTxn => client.register_new_txn_waker(request_id, waker),
        TxnRequestKind::AddPartition => client.register_add_partition_waker(request_id, waker),
        TxnRequestKind::AddSubscription => {
            client.register_add_subscription_waker(request_id, waker);
        }
        TxnRequestKind::EndTxn => client.register_end_txn_waker(request_id, waker),
    }
    Arc::strong_count(&probe) > 1
}

/// Runs a closure on a freshly handshaked connection of one engine.
type WithConnection = fn(&mut dyn FnMut(&mut Connection));

/// Cancel every non-`EndTxn` transaction request, reset every one, and reset each non-`EndTxn`
/// one after cancelling it, on connections of one engine.
fn observe_boundary(with_connection: WithConnection) -> BoundaryTrace {
    let mut trace = BoundaryTrace::default();
    for kind in TXN_REQUEST_KINDS {
        // Controls: a reply to a waiting caller is recorded as its outcome, and the probe sees a
        // pending request but no longer an answered one.
        with_connection(&mut |conn| {
            let awaited = issue(conn, kind);
            assert!(
                txn_client_tracks(conn, kind, awaited),
                "{kind:?}: the probe sees a request awaiting its reply"
            );
            feed_reply(conn, kind, awaited);
            assert!(
                conn.take_outcome(PendingOpKey::Request(awaited)).is_some(),
                "{kind:?}: a reply to a waiting caller is recorded as its outcome"
            );
            assert!(
                !txn_client_tracks(conn, kind, awaited),
                "{kind:?}: the probe no longer sees an answered request"
            );
        });
        // A reply that lands after the caller cancelled must leave no outcome behind. No runtime
        // cancels an `EndTxn` (a retry resumes it), so it is only reset below.
        with_connection(&mut |conn| {
            if matches!(kind, TxnRequestKind::EndTxn) {
                return;
            }
            let request_id = issue(conn, kind);
            conn.cancel_request(request_id);
            feed_reply(conn, kind, request_id);
            if conn
                .take_outcome(PendingOpKey::Request(request_id))
                .is_some()
            {
                trace.stranded_after_cancel.push(format!("{kind:?}"));
            }
        });
        // A session reset resolves the waiting caller, and the dead session's reply never
        // comes, so the request's `TxnClient` entry has to go with it.
        with_connection(&mut |conn| {
            let request_id = issue(conn, kind);
            conn.reset();
            assert!(
                matches!(
                    conn.take_outcome(PendingOpKey::Request(request_id)),
                    Some(OpOutcome::SessionLost { .. })
                ),
                "{kind:?}: the reset resolves the waiting caller with SessionLost"
            );
            if txn_client_tracks(conn, kind, request_id) {
                trace.retained_after_reset.push(format!("{kind:?}"));
            }
        });
        // A request cancelled before the reset (its deadline fired during the outage) has left
        // `pending_requests`, and its reply could only come on the session the reset ends, so
        // the reset must retire its `TxnClient` entry too. No runtime cancels an `EndTxn`.
        with_connection(&mut |conn| {
            if matches!(kind, TxnRequestKind::EndTxn) {
                return;
            }
            let request_id = issue(conn, kind);
            conn.cancel_request(request_id);
            conn.reset();
            if txn_client_tracks(conn, kind, request_id) {
                trace
                    .retained_after_cancel_and_reset
                    .push(format!("{kind:?}"));
            }
        });
    }
    trace
}

#[test]
fn cancelled_and_reset_txn_requests_leave_no_connection_state_on_either_engine() {
    let tokio_trace = observe_boundary(on_tokio_connection);
    let moonpool_trace = observe_boundary(on_moonpool_connection);
    assert_eq!(
        tokio_trace, moonpool_trace,
        "both engines must keep the same state behind cancelled and reset transaction requests"
    );
    assert!(
        tokio_trace.stranded_after_cancel.is_empty()
            && tokio_trace.retained_after_reset.is_empty()
            && tokio_trace.retained_after_cancel_and_reset.is_empty(),
        "transaction requests left state in the connection: a reply landing after the caller \
         cancelled was recorded as an outcome nobody can take, for {:?}; a session reset left the \
         TxnClient tracking requests the dead session will never answer, for {:?} pending at the \
         reset and {:?} cancelled before it",
        tokio_trace.stranded_after_cancel,
        tokio_trace.retained_after_reset,
        tokio_trace.retained_after_cancel_and_reset,
    );
}
