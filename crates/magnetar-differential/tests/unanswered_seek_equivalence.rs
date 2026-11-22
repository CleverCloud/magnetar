// SPDX-License-Identifier: Apache-2.0

//! A seek the broker never answers is failed and thaws the consumer, identically on both engines
//! (ADR-0024 layer (d)).
//!
//! `Connection::seek` freezes the consumer (`pending_seek`) until the broker's reply lifts it.
//! `Connection::handle_timeout` bounds a pending ack and a pending producer open by
//! `operation_timeout`, so a seek the broker never answers must be failed to its awaiting future
//! and must not hold the consumer frozen for the life of the connection, whether the seek's
//! future is still awaiting or was dropped. Both engines wrap the same `magnetar-proto` state
//! machine, so they must agree.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, MessageId, PendingOpKey, RequestId, SeekTarget,
    SubscribeRequest, encode_command, pb,
};

/// What the connection holds once `operation_timeout` has elapsed without a reply to a seek, as
/// `(what, whether it holds)`.
type Observed = Vec<(&'static str, bool)>;

fn frame(command: &pb::BaseCommand) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(&mut frame, command).expect("encode command");
    frame
}

fn tokio_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn success(request_id: u64) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    }
}

/// A handshaked connection with one acknowledged, flowed consumer.
fn established_consumer(conn: &mut Connection, now: Instant) -> ConsumerHandle {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(
        now,
        &frame(&pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        }),
    )
    .expect("Connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    let subscribe = RequestId(conn.peek_next_request_id_for_test());
    let consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/unanswered-seek".to_owned(),
        subscription: "sub".to_owned(),
        receiver_queue_size: 8,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &frame(&success(subscribe.0)))
        .expect("subscribe Success");
    assert!(conn.consume_initial_consumer_subscribe_completion(consumer));
    while conn.poll_event().is_some() {}
    let _ = conn.initial_flow(consumer, now);
    let _ = conn.poll_transmit();
    consumer
}

fn frozen(conn: &Connection, consumer: ConsumerHandle) -> bool {
    conn.consumer(consumer)
        .expect("consumer slot")
        .state
        .lock()
        .pending_seek
        .is_some()
}

/// Sweeps the timers one second past `operation_timeout` and reports whether the connection
/// survived the sweep, so a thaw cannot be a reset's doing.
fn sweep_past_operation_timeout(conn: &mut Connection, now: Instant) -> bool {
    let session = conn.session_epoch();
    conn.handle_timeout(now + conn.operation_timeout() + Duration::from_secs(1));
    conn.is_connected() && conn.session_epoch() == session
}

/// One seek whose future still awaits and one whose future was dropped, each on its own
/// consumer's connection, swept past `operation_timeout` without any reply.
fn observed_after_silence(conn: &mut Connection, now: Instant) -> Observed {
    let awaited = established_consumer(conn, now);
    let seek = conn.seek(awaited, SeekTarget::MessageId(MessageId::EARLIEST));
    conn.register_waker(
        PendingOpKey::Request(seek),
        std::task::Waker::noop().clone(),
    );
    let frozen_by_the_seek = frozen(conn, awaited);
    let connected = sweep_past_operation_timeout(conn, now);
    let awaited_failed = conn.take_outcome(PendingOpKey::Request(seek)).is_some();
    let awaited_thawed = !frozen(conn, awaited);

    vec![
        ("the seek froze the consumer", frozen_by_the_seek),
        ("the sweep kept the connection up", connected),
        ("the awaited seek was failed", awaited_failed),
        ("the awaited seek thawed the consumer", awaited_thawed),
    ]
}

fn abandoned_observed_after_silence(conn: &mut Connection, now: Instant) -> Observed {
    let consumer = established_consumer(conn, now);
    let seek = conn.seek(consumer, SeekTarget::MessageId(MessageId::EARLIEST));
    assert!(conn.abandon_consumer_seek_waiter(consumer, seek));
    let frozen_by_the_seek = frozen(conn, consumer);
    let connected = sweep_past_operation_timeout(conn, now);
    let thawed = !frozen(conn, consumer);

    vec![
        ("the abandoned seek froze the consumer", frozen_by_the_seek),
        ("the sweep kept the connection up", connected),
        ("the abandoned seek thawed the consumer", thawed),
    ]
}

#[test]
fn engines_fail_an_awaited_seek_the_broker_never_answers_and_thaw_the_consumer() {
    let tokio = tokio_projection(observed_after_silence);
    let moonpool = moonpool_projection(observed_after_silence);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what operation_timeout does to an unanswered seek"
    );
    assert_eq!(
        tokio,
        vec![
            ("the seek froze the consumer", true),
            ("the sweep kept the connection up", true),
            ("the awaited seek was failed", true),
            ("the awaited seek thawed the consumer", true),
        ],
        "an unanswered seek must be failed to its future and must not freeze the consumer for \
         the life of the connection"
    );
}

#[test]
fn engines_thaw_the_consumer_of_an_abandoned_seek_the_broker_never_answers() {
    let tokio = tokio_projection(abandoned_observed_after_silence);
    let moonpool = moonpool_projection(abandoned_observed_after_silence);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what operation_timeout does to an abandoned unanswered seek"
    );
    assert_eq!(
        tokio,
        vec![
            ("the abandoned seek froze the consumer", true),
            ("the sweep kept the connection up", true),
            ("the abandoned seek thawed the consumer", true),
        ],
        "an abandoned seek the broker never answers must not freeze the consumer for the life \
         of the connection"
    );
}
