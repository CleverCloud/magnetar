// SPDX-License-Identifier: Apache-2.0

//! Forgotten ACKs and rejected lookups on live Tokio and Moonpool connections.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, LookupOutcome, MessageId, OpOutcome,
    PendingOpKey, RequestId, SubscribeRequest, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

fn id(entry_id: u64) -> MessageId {
    MessageId {
        ledger_id: 7,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}

fn subscription(name: &str, grouped: bool) -> SubscribeRequest {
    SubscribeRequest {
        topic: format!("persistent://public/default/retention-{name}"),
        subscription: format!("retention-{name}"),
        ack_group_time: grouped.then_some(Duration::from_secs(60)),
        ..Default::default()
    }
}

fn generic_response(request_id: RequestId, error: bool) -> BytesMut {
    let cmd = if error {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "scripted generic ACK failure".to_owned(),
            }),
            ..Default::default()
        }
    } else {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: request_id.0,
                schema: None,
            }),
            ..Default::default()
        }
    };
    let mut frame = BytesMut::new();
    encode_command(&mut frame, &cmd).expect("encode broker response");
    frame
}

fn flush_and_reply(conn: &mut Connection, error: bool) -> RequestId {
    let request_id = RequestId(conn.peek_next_request_id_for_test());
    let now = Instant::now() + Duration::from_secs(61);
    conn.handle_timeout(now);
    assert_eq!(
        conn.peek_next_request_id_for_test(),
        request_id.0 + 1,
        "one grouped ACK emitted"
    );
    conn.handle_bytes(now, &generic_response(request_id, error))
        .expect("generic ACK response");
    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none(),
        "forgotten ACK must not retain a response"
    );
    request_id
}

macro_rules! ack_scenario {
    ($client:expr, $name:expr) => {{
        let client = $client;
        let consumer = client
            .subscribe(subscription($name, true))
            .await
            .expect("subscribe grouped consumer");
        consumer.ack_grouped_cumulative(id(11));
        {
            let shared = client.shared();
            let mut conn = shared.inner.lock();
            flush_and_reply(&mut conn, false);
        }
        consumer.ack_grouped_cumulative(id(12));
        {
            let shared = client.shared();
            let mut conn = shared.inner.lock();
            flush_and_reply(&mut conn, true);
        }
        let stats = consumer.stats();
        assert_eq!(stats.total_acks_sent, 2);
        assert_eq!(
            stats.total_acks_failed, 1,
            "generic error remains observable"
        );
        // No tracker means malformed grouped ACKs go through the immediate fallback.
        let invalid = client
            .subscribe(subscription(&format!("{}-invalid", $name), false))
            .await
            .expect("subscribe immediate consumer");
        let mut malformed = id(13);
        malformed.batch_index = 2;
        malformed.batch_size = 2;
        let request_id = RequestId(client.shared().inner.lock().peek_next_request_id_for_test());
        invalid.ack_grouped(malformed);
        assert_eq!(invalid.stats().total_acks_failed, 1);
        assert_eq!(invalid.stats().total_acks_sent, 0);
        assert!(
            client
                .shared()
                .inner
                .lock()
                .take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "invalid forgotten ACK must not retain an error"
        );
        // A round trip on this connection is a FIFO barrier for both grouped frames.
        let barrier = client
            .subscribe(subscription(&format!("{}-barrier", $name), false))
            .await
            .expect("barrier subscribe");
        barrier.close().await.expect("close barrier");
        invalid.close().await.expect("close invalid consumer");
        consumer.close().await.expect("close grouped consumer");
        client.close().await;
    }};
}

#[tokio::test(flavor = "current_thread")]
async fn grouped_ack_generic_responses_do_not_retain_outcomes() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let config = ConnectionConfig {
        keepalive_interval: Duration::from_secs(120),
        ..Default::default()
    };
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio connect");
    ack_scenario!(tokio, "tokio");
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool connect");
    ack_scenario!(moonpool, "moonpool");
    let moonpool_frames = broker.frame_log_snapshot();
    for frames in [&tokio_frames, &moonpool_frames] {
        assert_eq!(
            frames
                .iter()
                .filter(|&&kind| kind == pb::base_command::Type::Ack as i32)
                .count(),
            2,
            "only the two valid grouped ACKs reach the broker"
        );
    }
    assert_eq!(tokio_frames, moonpool_frames);
    broker.shutdown().await;
}

fn saturated_lookup(conn: &mut Connection) {
    while conn.poll_event().is_some() {}
    let before = conn.dropped_observational_events();
    let first = conn.lookup("persistent://public/default/first", false);
    let mut first_frame = conn.poll_transmit();
    let decoded = magnetar_proto::frame::decode_one(&mut first_frame).expect("first lookup frame");
    assert_eq!(
        decoded
            .command
            .lookup_topic
            .expect("lookup command")
            .request_id,
        first.0
    );
    assert!(first_frame.is_empty());
    let cap = magnetar_proto::conn::EVENT_QUEUE_OBSERVATIONAL_CAP;
    for _ in 0..=cap {
        let rejected = conn.lookup("persistent://public/default/rejected", false);
        assert!(matches!(conn.take_outcome(PendingOpKey::Request(rejected)),
            Some(OpOutcome::LookupResponse { request_id, outcome: LookupOutcome::Failed { code: 0, .. } }) if request_id == rejected));
    }
    assert!(
        conn.poll_transmit().is_empty(),
        "rejected lookups emit no wire frame"
    );
    assert_eq!(conn.dropped_observational_events() - before, 1);
    let mut observed = 0;
    while let Some(event) = conn.poll_event() {
        assert!(matches!(
            event,
            ConnectionEvent::LookupResponse {
                result: LookupOutcome::Failed { code: 0, .. },
                ..
            }
        ));
        observed += 1;
    }
    assert_eq!(
        observed, cap,
        "observational cap does not suppress takeable lookup outcomes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_lookup_outcomes_survive_observational_queue_saturation() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let config = ConnectionConfig {
        max_pending_lookups: 1,
        ..Default::default()
    };
    let tokio = TokioClient::connect(&broker.pulsar_url(), config.clone())
        .await
        .expect("tokio connect");
    saturated_lookup(&mut tokio.shared().inner.lock());
    tokio.close().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool = MoonpoolClient::connect_plain(&engine, &broker.host_port(), config)
        .await
        .expect("moonpool connect");
    saturated_lookup(&mut moonpool.shared().inner.lock());
    moonpool.close().await;
    broker.shutdown().await;
}

fn expire_first_chunk(
    conn: &mut Connection,
    handle: magnetar_proto::ConsumerHandle,
    entry: u64,
) -> RequestId {
    let now = Instant::now();
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 9,
                entry_id: entry,
                ..Default::default()
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "chunk-producer".to_owned(),
        sequence_id: 1,
        publish_time: 1,
        uuid: Some(format!("expired-chunk-{entry}")),
        num_chunks_from_msg: Some(2),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(2),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &command, &metadata, b"a").expect("encode first chunk");
    conn.handle_bytes(now, &frame).expect("deliver first chunk");
    let request_id = RequestId(conn.peek_next_request_id_for_test());
    conn.handle_timeout(now + Duration::from_secs(2));
    assert_eq!(
        conn.peek_next_request_id_for_test(),
        request_id.0 + 1,
        "expiry sends one ACK"
    );
    request_id
}

// This second expiry is inspected locally, so its ACK is not sent to the broker.
fn inspect_expired_chunk_ack(conn: &mut Connection, handle: magnetar_proto::ConsumerHandle) {
    let request_id = expire_first_chunk(conn, handle, 24);
    let mut bytes = conn.poll_transmit();
    let ack = magnetar_proto::frame::decode_one(&mut bytes)
        .expect("decode expired chunk ACK")
        .command
        .ack
        .expect("ACK command");
    assert!(bytes.is_empty(), "only one ACK is emitted");
    assert_eq!(ack.consumer_id, handle.0);
    assert_eq!(ack.request_id, Some(request_id.0));
    assert_eq!(ack.ack_type, pb::command_ack::AckType::Individual as i32);
    assert_eq!(ack.message_id.len(), 1);
    assert_eq!(
        (ack.message_id[0].ledger_id, ack.message_id[0].entry_id),
        (9, 24)
    );
    let response = pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id: handle.0,
            request_id: Some(request_id.0),
            error: None,
            message: None,
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_command(&mut frame, &response).expect("encode correlated ACK response");
    conn.handle_bytes(Instant::now(), &frame)
        .expect("correlated ACK response");
    assert!(
        conn.take_outcome(PendingOpKey::Request(request_id))
            .is_none()
    );
}

macro_rules! chunk_scenario {
    ($client:expr, $name:expr) => {{
        let client = $client;
        let mut request = subscription($name, false);
        request.auto_ack_oldest_chunked_message_on_queue_full = true;
        request.expire_time_of_incomplete_chunked_message = Some(Duration::from_secs(1));
        let consumer = client.subscribe(request).await.expect("subscribe chunk consumer");
        let request_id = expire_first_chunk(&mut client.shared().inner.lock(), consumer.handle(), 23);
        let barrier = client.subscribe(subscription(&format!("{}-barrier", $name), false)).await.expect("barrier subscribe");
        {
            let shared = client.shared();
            let mut conn = shared.inner.lock();
            assert!(matches!(conn.poll_event_if(|event| matches!(event,
                ConnectionEvent::AckResponse { request_id: Some(rid), .. } if *rid == request_id)),
                Some(ConnectionEvent::AckResponse { result: Ok(()), .. })), "broker acknowledged the expired chunk");
            assert!(conn.take_outcome(PendingOpKey::Request(request_id)).is_none(), "automatic chunk ACK has no waiter");
        }
        assert_eq!(consumer.stats().total_acks_sent, 1);
        barrier.close().await.expect("close barrier");
        inspect_expired_chunk_ack(&mut client.shared().inner.lock(), consumer.handle());
        assert_eq!(consumer.stats().total_acks_sent, 2);
        consumer.close().await.expect("close chunk consumer");
        client.close().await;
    }};
}

#[tokio::test(flavor = "current_thread")]
async fn expired_chunk_auto_ack_is_confirmed_without_orphan_outcome() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let tokio = TokioClient::connect(&broker.pulsar_url(), ConnectionConfig::default())
        .await
        .expect("tokio connect");
    chunk_scenario!(tokio, "chunk-tokio");
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool =
        MoonpoolClient::connect_plain(&engine, &broker.host_port(), ConnectionConfig::default())
            .await
            .expect("moonpool connect");
    chunk_scenario!(moonpool, "chunk-moonpool");
    let moonpool_frames = broker.frame_log_snapshot();
    for frames in [&tokio_frames, &moonpool_frames] {
        assert_eq!(
            frames
                .iter()
                .filter(|&&kind| kind == pb::base_command::Type::Ack as i32)
                .count(),
            1
        );
    }
    assert_eq!(tokio_frames, moonpool_frames);
    broker.shutdown().await;
}
