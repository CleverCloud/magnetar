// SPDX-License-Identifier: Apache-2.0

//! A seek's `operation_timeout` deadline is measured from the clock the engine hands in with
//! it, wakes the driver at that deadline, and re-attaches a consumer the broker tore down,
//! identically on both engines (ADR-0024 layer (d), ADR-0153).
//!
//! `unanswered_seek_equivalence` pins the sweep itself. This file pins what the engines add to
//! it: the instant the runtimes stamp through `Connection::seek_at`, the wake `poll_timeout`
//! arms for it (so a driver sleeps until the deadline instead of sweeping on an unrelated
//! tick), and the recovery when the broker had already closed the consumer. Both engines wrap
//! the same `magnetar-proto` state machine, so they must agree.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, MessageId, PendingOpKey, RequestId, SeekTarget,
    SubscribeRequest, decode_one, encode_command, pb,
};

/// What a connection holds at each step, as `(what, whether it holds)`.
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

fn broker_close(consumer: ConsumerHandle) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id: consumer.0,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
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
        topic: "persistent://public/default/seek-operation-timeout".to_owned(),
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

/// The `CommandSubscribe` request ids and the `CommandFlow` grants staged for `consumer`.
fn drain_subscribes_and_grants(
    conn: &mut Connection,
    consumer: ConsumerHandle,
) -> (Vec<u64>, Vec<u32>) {
    let mut staged = conn.poll_transmit();
    let (mut subscribes, mut grants) = (Vec::new(), Vec::new());
    while !staged.is_empty() {
        let command = decode_one(&mut staged)
            .expect("decode staged frame")
            .command;
        if let Some(subscribe) = command.subscribe
            && subscribe.consumer_id == consumer.0
        {
            subscribes.push(subscribe.request_id);
        }
        if let Some(flow) = command.flow
            && flow.consumer_id == consumer.0
        {
            grants.push(flow.message_permits);
        }
    }
    (subscribes, grants)
}

/// A seek issued twenty seconds after the last frame, as the runtimes issue it: with their own
/// clock. A sweep 31 s after the last frame finds the seek only 11 s old and must leave it; a
/// frame decoded later moves the keepalive wake out of the way, so the earliest wake is the
/// seek's.
fn observed_with_the_callers_clock(conn: &mut Connection, now: Instant) -> Observed {
    let consumer = established_consumer(conn, now);
    let issued = now + Duration::from_secs(20);
    let seek = conn.seek_at(consumer, SeekTarget::MessageId(MessageId::EARLIEST), issued);
    conn.register_waker(
        PendingOpKey::Request(seek),
        std::task::Waker::noop().clone(),
    );
    let deadline = issued + conn.operation_timeout();

    conn.handle_timeout(now + Duration::from_secs(31));
    let early_sweep_left_it =
        frozen(conn, consumer) && conn.take_outcome(PendingOpKey::Request(seek)).is_none();
    conn.handle_bytes(now + Duration::from_secs(35), &frame(&success(9_999)))
        .expect("an unrelated reply");
    let woken_at_the_deadline = conn.poll_timeout() == Some(deadline);

    let session = conn.session_epoch();
    conn.handle_timeout(deadline + Duration::from_secs(1));
    let kept_up = conn.is_connected() && conn.session_epoch() == session;
    let failed = conn.take_outcome(PendingOpKey::Request(seek)).is_some();
    let thawed = !frozen(conn, consumer);

    vec![
        (
            "a sweep inside the deadline left the seek",
            early_sweep_left_it,
        ),
        ("the wake is the seek's own deadline", woken_at_the_deadline),
        ("the sweep kept the connection up", kept_up),
        ("the seek failed at its deadline", failed),
        ("the seek thawed the consumer", thawed),
    ]
}

/// The broker closes the consumer while applying the seek and never answers it.
fn observed_after_a_close_and_silence(conn: &mut Connection, now: Instant) -> Observed {
    let consumer = established_consumer(conn, now);
    let seek = conn.seek_at(consumer, SeekTarget::MessageId(MessageId::EARLIEST), now);
    let _ = conn.poll_transmit();
    conn.handle_bytes(now, &frame(&broker_close(consumer)))
        .expect("CloseConsumer");
    let (subscribes_mid_seek, _) = drain_subscribes_and_grants(conn, consumer);

    let expiry = now + conn.operation_timeout() + Duration::from_secs(1);
    conn.handle_timeout(expiry);
    let failed = conn.take_outcome(PendingOpKey::Request(seek)).is_some();
    let thawed = !frozen(conn, consumer);
    let (subscribes, grants) = drain_subscribes_and_grants(conn, consumer);
    let one_subscribe = subscribes.len() == 1;
    let no_grant_before_the_ack = grants.is_empty();
    let mut released_one_window = false;
    if let Some(&request_id) = subscribes.first() {
        conn.handle_bytes(expiry, &frame(&success(request_id)))
            .expect("re-subscribe Success");
        let (again, grants) = drain_subscribes_and_grants(conn, consumer);
        released_one_window = again.is_empty() && grants == [8];
    }

    vec![
        (
            "the close arm did not re-attach mid-seek",
            subscribes_mid_seek.is_empty(),
        ),
        ("the unanswered seek failed", failed),
        ("the unanswered seek thawed the consumer", thawed),
        ("exactly one re-subscribe was emitted", one_subscribe),
        (
            "no flow before the re-subscribe was acknowledged",
            no_grant_before_the_ack,
        ),
        (
            "one full window followed the acknowledgement",
            released_one_window,
        ),
    ]
}

#[test]
fn engines_measure_a_seeks_deadline_from_the_callers_clock_and_wake_for_it() {
    let tokio = tokio_projection(observed_with_the_callers_clock);
    let moonpool = moonpool_projection(observed_with_the_callers_clock);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the deadline of a seek issued with the caller's clock"
    );
    assert_eq!(
        tokio,
        vec![
            ("a sweep inside the deadline left the seek", true),
            ("the wake is the seek's own deadline", true),
            ("the sweep kept the connection up", true),
            ("the seek failed at its deadline", true),
            ("the seek thawed the consumer", true),
        ],
        "the deadline must run from the seek, not from the last frame"
    );
}

#[test]
fn engines_re_attach_a_consumer_the_broker_closed_before_an_unanswered_seek_expired() {
    let tokio = tokio_projection(observed_after_a_close_and_silence);
    let moonpool = moonpool_projection(observed_after_a_close_and_silence);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on how an unanswered seek re-attaches a closed consumer"
    );
    assert_eq!(
        tokio,
        vec![
            ("the close arm did not re-attach mid-seek", true),
            ("the unanswered seek failed", true),
            ("the unanswered seek thawed the consumer", true),
            ("exactly one re-subscribe was emitted", true),
            ("no flow before the re-subscribe was acknowledged", true),
            ("one full window followed the acknowledgement", true),
        ],
        "an unanswered seek must not leave a consumer the broker closed detached"
    );
}
