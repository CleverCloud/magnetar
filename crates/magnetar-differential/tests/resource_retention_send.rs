// SPDX-License-Identifier: Apache-2.0

//! Dropped publish futures keep their wire work but not their outcomes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, PendingOpKey, ProducerHandle, SequenceId,
    decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

fn message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(b"retained"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 8,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn feed(conn: &mut Connection, command: &pb::BaseCommand, now: Instant) {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode broker command");
    conn.handle_bytes(now, &bytes)
        .expect("handle broker command");
}

fn connected() -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "retention-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    }
}

fn producer_success(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "retention-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    }
}

fn send_receipt(handle: ProducerHandle, seq: SequenceId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id: handle.0,
            sequence_id: seq.0,
            message_id: Some(pb::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                ..Default::default()
            }),
            highest_sequence_id: None,
        }),
        ..Default::default()
    }
}

fn send_error(handle: ProducerHandle, seq: SequenceId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::SendError as i32,
        send_error: Some(pb::CommandSendError {
            producer_id: handle.0,
            sequence_id: seq.0,
            error: pb::ServerError::PersistenceError as i32,
            message: "rejected".to_owned(),
        }),
        ..Default::default()
    }
}

fn pending_sequence(conn: &Connection, handle: ProducerHandle) -> SequenceId {
    conn.producer(handle)
        .expect("live producer")
        .state
        .lock()
        .pending
        .back()
        .expect("queued send")
        .sequence_id
}

/// A caller may abandon before a broker error or a local timeout. Both retain
/// the publish until the terminal event and release its reservation once.
fn abandoned_error<F: Future>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    future: F,
    timeout: bool,
) -> u64 {
    let seq = pending_sequence(&shared.lock(), handle);
    assert_eq!(memory.used_bytes(), 8);
    drop(future);
    assert_eq!(memory.used_bytes(), 8);
    let mut conn = shared.lock();
    if timeout {
        conn.handle_timeout(Instant::now() + Duration::from_secs(2));
    } else {
        feed(&mut conn, &send_error(handle, seq), Instant::now());
    }
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(conn.producer_pending_count(handle), 0);
    assert!(conn.take_outcome(PendingOpKey::Send(handle, seq)).is_none());
    let event = conn.poll_event_if(|event| {
        matches!(event,
        magnetar_proto::ConnectionEvent::SendError { handle: h, sequence_id, .. }
            if *h == handle && *sequence_id == seq)
    });
    assert!(event.is_some(), "the failure remains observable");
    conn.producer_stats(handle)
        .expect("producer stats")
        .total_send_failed
}

/// A receipt that wins the drop race is removed when the unpolled future is
/// dropped; a live future still receives its result before its first poll.
fn receipt_before_first_poll<F, E>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    mut future: F,
    abandon: bool,
) where
    F: Future<Output = Result<magnetar_proto::MessageId, E>> + Unpin,
    E: std::fmt::Debug,
{
    let seq = pending_sequence(&shared.lock(), handle);
    {
        let mut conn = shared.lock();
        feed(&mut conn, &send_receipt(handle, seq), Instant::now());
    }
    assert_eq!(memory.used_bytes(), 0);
    if abandon {
        drop(future);
        assert!(
            shared
                .lock()
                .take_outcome(PendingOpKey::Send(handle, seq))
                .is_none()
        );
    } else {
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Ready(Ok(_))
        ));
    }
}

fn aborted_snapshot<F: Future + Unpin>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    mut future: F,
) {
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut future).poll(&mut context),
        Poll::Pending
    ));
    let seq = {
        let mut conn = shared.lock();
        let seq = pending_sequence(&conn, handle);
        conn.reset();
        assert_eq!(conn.in_flight_publish_snapshot_len(handle), 1);
        seq
    };
    assert_eq!(counter.0.load(Ordering::SeqCst), 1, "reset wakes once");
    drop(future);
    assert_eq!(memory.used_bytes(), 8);
    let mut conn = shared.lock();
    conn.fail_all_pending("reconnect exhausted");
    assert_eq!(memory.used_bytes(), 0);
    assert!(conn.take_outcome(PendingOpKey::Send(handle, seq)).is_none());
    assert_eq!(
        counter.0.load(Ordering::SeqCst),
        1,
        "no wake after abandonment"
    );
}

fn rejected_close<S, C, E>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    send: S,
    retained: S,
    mut close: C,
) where
    S: Future,
    C: Future<Output = Result<(), E>> + Unpin,
    E: std::fmt::Debug,
{
    let retained_seq = pending_sequence(&shared.lock(), handle);
    let seq = shared
        .lock()
        .producer(handle)
        .expect("live producer")
        .state
        .lock()
        .pending
        .front()
        .expect("first queued send")
        .sequence_id;
    drop(send);
    assert_eq!(memory.used_bytes(), 16);
    let request = shared.lock().peek_next_request_id_for_test();
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Pending
    ));
    let mut conn = shared.lock();
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "close rejected".to_owned(),
            }),
            ..Default::default()
        },
        Instant::now(),
    );
    assert!(conn.producer(handle).is_none());
    assert!(conn.take_outcome(PendingOpKey::Send(handle, seq)).is_none());
    assert_eq!(memory.used_bytes(), 0);
    drop(conn);
    drop(retained);
    assert!(
        shared
            .lock()
            .take_outcome(PendingOpKey::Send(handle, retained_seq))
            .is_none(),
        "dropping after slot removal clears the landed result"
    );
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Ready(Err(_))
    ));
}

fn send_failures<F, E, C, Make>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    mut send: Make,
    close: C,
) -> (u64, u64)
where
    F: Future<Output = Result<magnetar_proto::MessageId, E>> + Unpin,
    E: std::fmt::Debug,
    C: Future<Output = Result<(), E>> + Unpin,
    Make: FnMut() -> F,
{
    receipt_before_first_poll(shared, memory, handle, send(), false);
    receipt_before_first_poll(shared, memory, handle, send(), true);
    let error = abandoned_error(shared, memory, handle, send(), false);
    let timeout = abandoned_error(shared, memory, handle, send(), true);
    rejected_close(shared, memory, handle, send(), send(), close);
    (error, timeout)
}

#[tokio::test(flavor = "current_thread")]
async fn abandoned_send_failures_and_close_match_across_engines() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let config = ConnectionConfig {
        memory_limit_bytes: 1024,
        ..Default::default()
    };
    let request = |suffix: &str| CreateProducerRequest {
        topic: format!("persistent://public/default/retention-failure-{suffix}"),
        send_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio");
    let producer = tokio
        .open_producer(request("tokio"))
        .await
        .expect("producer");
    let handle = producer.handle();
    let shared = tokio.shared();
    let (error_tokio, timeout_tokio) = send_failures(
        &shared.inner,
        &shared.memory_limit,
        handle,
        || producer.send(message()),
        Box::pin(producer.clone().close()),
    );
    drop(producer);
    tokio.close().await;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool");
    let producer = moonpool
        .open_producer(request("moonpool"))
        .await
        .expect("producer");
    let handle = producer.handle();
    let shared = moonpool.shared();
    let (error_moonpool, timeout_moonpool) = send_failures(
        &shared.inner,
        &shared.memory_limit,
        handle,
        || producer.send(message()),
        Box::pin(producer.clone().close()),
    );
    drop(producer);
    moonpool.close().await;
    assert_eq!(
        (error_tokio, timeout_tokio),
        (error_moonpool, timeout_moonpool)
    );
    assert_eq!((error_tokio, timeout_tokio), (1, 2));
}

#[tokio::test(flavor = "current_thread")]
async fn abandoned_snapshot_terminal_failure_releases_both_engines() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let config = ConnectionConfig {
        memory_limit_bytes: 1024,
        ..Default::default()
    };
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio");
    let producer = tokio
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/abandoned-terminal-tokio".to_owned(),
            ..Default::default()
        })
        .await
        .expect("producer");
    aborted_snapshot(
        &tokio.shared().inner,
        &tokio.shared().memory_limit,
        producer.handle(),
        producer.send(message()),
    );
    drop(producer);
    tokio.close().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool");
    let producer = moonpool
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/abandoned-terminal-moonpool".to_owned(),
            ..Default::default()
        })
        .await
        .expect("producer");
    aborted_snapshot(
        &moonpool.shared().inner,
        &moonpool.shared().memory_limit,
        producer.handle(),
        producer.send(message()),
    );
    drop(producer);
    moonpool.close().await;
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Drive the real runtime send future while the connection is held in one
/// synchronous turn, so reset and the simulated broker replies cannot race
/// the socket driver. Both engines run this exact trace.
fn dropped_snapshot<F: Future + Unpin>(
    shared: &parking_lot::Mutex<Connection>,
    memory: &magnetar_proto::MemoryLimitController,
    handle: ProducerHandle,
    completed: F,
    mut future: F,
) -> (u64, Vec<u8>, usize) {
    let completed_seq = {
        let mut conn = shared.lock();
        let seq = conn
            .producer(handle)
            .expect("live producer")
            .state
            .lock()
            .pending
            .front()
            .expect("first queued send")
            .sequence_id;
        feed(&mut conn, &send_receipt(handle, seq), Instant::now());
        seq
    };
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut future).poll(&mut context),
        Poll::Pending
    ));
    let now = Instant::now();
    let seq = {
        let mut conn = shared.lock();
        let seq = conn
            .producer(handle)
            .expect("live producer")
            .state
            .lock()
            .pending
            .back()
            .expect("queued")
            .sequence_id;
        let _ = conn.poll_transmit();
        conn.reset();
        assert_eq!(conn.in_flight_publish_snapshot_len(handle), 1);
        seq
    };
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(memory.used_bytes(), 8);
    drop(completed);
    assert!(
        shared
            .lock()
            .take_outcome(PendingOpKey::Send(handle, completed_seq))
            .is_none()
    );
    assert_eq!(
        shared.lock().in_flight_publish_snapshot_len(handle),
        1,
        "abandoning the completed sibling preserves the replay"
    );
    drop(future);
    assert_eq!(memory.used_bytes(), 8);
    let mut conn = shared.lock();
    conn.begin_handshake().expect("re-handshake");
    feed(&mut conn, &connected(), now);
    let _ = conn.poll_transmit();
    let opens = conn.rebuild_producers();
    assert_eq!(opens.len(), 1);
    let _ = conn.poll_transmit();
    feed(&mut conn, &producer_success(opens[0].0), now);
    let mut wire = conn.poll_transmit();
    let replay = decode_one(&mut wire).expect("replayed send");
    assert!(wire.is_empty(), "only one replay was emitted");
    let replay_seq = replay.command.send.expect("send command").sequence_id;
    let payload = replay.payload.expect("send payload").body.to_vec();
    assert_eq!(replay_seq, seq.0);
    assert_eq!(payload, b"retained");
    feed(&mut conn, &send_receipt(handle, seq), now);
    assert_eq!(memory.used_bytes(), 0);
    assert!(conn.take_outcome(PendingOpKey::Send(handle, seq)).is_none());
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    (replay_seq, payload, counter.0.load(Ordering::SeqCst))
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_send_snapshot_replays_equally_without_orphan_outcome() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let config = ConnectionConfig {
        memory_limit_bytes: 1024,
        ..Default::default()
    };
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio");
    let producer = tokio
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/snapshot-tokio".to_owned(),
            ..Default::default()
        })
        .await
        .expect("producer");
    let handle = producer.handle();
    let result_tokio = dropped_snapshot(
        &tokio.shared().inner,
        &tokio.shared().memory_limit,
        handle,
        producer.send(message()),
        producer.send(message()),
    );
    drop(producer);
    tokio.close().await;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool");
    let producer = moonpool
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/snapshot-moonpool".to_owned(),
            ..Default::default()
        })
        .await
        .expect("producer");
    let handle = producer.handle();
    let result_moonpool = dropped_snapshot(
        &moonpool.shared().inner,
        &moonpool.shared().memory_limit,
        handle,
        producer.send(message()),
        producer.send(message()),
    );
    drop(producer);
    moonpool.close().await;
    assert_eq!(result_tokio, result_moonpool);
}

/// A user close owns the slot even if an older attachment is being retried.
fn close_during_reattach<C, E>(
    shared: &parking_lot::Mutex<Connection>,
    handle: ProducerHandle,
    mut close: C,
) where
    C: Future<Output = Result<(), E>> + Unpin,
    E: std::fmt::Debug,
{
    let mut conn = shared.lock();
    let failed = magnetar_proto::RequestId(conn.peek_next_request_id_for_test());
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::CloseProducer as i32,
            close_producer: Some(pb::CommandCloseProducer {
                producer_id: handle.0,
                request_id: 0,
                assigned_broker_service_url: None,
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        },
        Instant::now(),
    );
    assert!(conn.has_pending_request_for_test(failed));
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: failed.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "reattach rejected".to_owned(),
            }),
            ..Default::default()
        },
        Instant::now(),
    );
    let seen = std::cell::Cell::new(false);
    let _ = conn.poll_event_if(|event| {
        if matches!(event, magnetar_proto::ConnectionEvent::ProducerOpenFailedTransient { handle: h, .. } if *h == handle) {
            seen.set(true);
        }
        false
    });
    assert!(seen.get(), "the retry event was queued");
    let retried = conn
        .retry_producer_open_if_current(handle, failed)
        .expect("retry current open");
    assert!(conn.has_pending_request_for_test(retried));
    let close_id = conn.peek_next_request_id_for_test();
    drop(conn);
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Pending
    ));
    let mut conn = shared.lock();
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: close_id,
                schema: None,
            }),
            ..Default::default()
        },
        Instant::now(),
    );
    assert!(conn.producer(handle).is_none());
    assert!(!conn.has_pending_request_for_test(retried));
    assert!(
        conn.poll_driver_retry().is_none(),
        "the stale retry was removed"
    );
    assert!(conn.poll_event_if(|event| matches!(event, magnetar_proto::ConnectionEvent::ProducerOpenFailedTransient { handle: h, .. } if *h == handle)).is_none());
    drop(conn);
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn close_during_reattach_reclaims_retry_state_in_both_engines() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let request = |suffix: &str| CreateProducerRequest {
        topic: format!("persistent://public/default/retention-reattach-{suffix}"),
        ..Default::default()
    };
    let config = ConnectionConfig::default();
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio");
    let producer = tokio
        .open_producer(request("tokio"))
        .await
        .expect("producer");
    close_during_reattach(
        &tokio.shared().inner,
        producer.handle(),
        Box::pin(producer.clone().close()),
    );
    drop(producer);
    tokio.close().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool");
    let producer = moonpool
        .open_producer(request("moonpool"))
        .await
        .expect("producer");
    close_during_reattach(
        &moonpool.shared().inner,
        producer.handle(),
        Box::pin(producer.clone().close()),
    );
    drop(producer);
    moonpool.close().await;
}
