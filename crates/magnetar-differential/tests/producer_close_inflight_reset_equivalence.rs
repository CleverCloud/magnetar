// SPDX-License-Identifier: Apache-2.0

//! A send in flight at a producer close must resolve when the session resets before the broker
//! acknowledges the close, identically on both engines (ADR-0024 layer (d)).
//!
//! A close still unacknowledged when the session resets is failed by `Connection::reset()`
//! itself, which runs `fail_sends_of_closed_producer` for every close request it takes; the first
//! two scenarios guard that path. A close issued after the reset relocated the producer's
//! replayable sends into the reset snapshots, and before the supervisor's `rebuild_producers`,
//! leaves the producer out of the rebuild, which discarded its snapshots: nothing resolved the
//! send's future afterwards, so it hung until the client shut down, even with `send_timeout`
//! configured. Both engines wrap the same `magnetar-proto` state machine, so they must agree on the
//! outcome, and the outcome must be a failure that resolves the send.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, encode_command,
    pb,
};

const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Which close entry point the producer used.
#[derive(Clone, Copy, Debug)]
enum Close {
    /// `Producer::close()`: the awaited `CommandCloseProducer`.
    Awaited,
    /// The last-clone drop guard: the fire-and-forget close.
    Forgotten,
}

/// Where the producer close lands relative to the transport loss.
#[derive(Clone, Copy, Debug)]
enum CloseAt {
    /// The close is unacknowledged when the session resets, so the broker's ack can never arrive:
    /// the reset itself resolves the send, and the outcome is read right after it.
    BeforeReset,
    /// The close is issued on the new session, after the handshake and before the producers are
    /// re-attached.
    BetweenResetAndRebuild,
}

fn connected_frame() -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    frame
}

fn producer_success_frame(request_id: u64) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "close-inflight-reset".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode ProducerSuccess");
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

/// One unbatched send is in flight, its producer is closed, and the transport drops. Returns the
/// kind of outcome the send's future finds once the supervisor has re-handshaked, re-attached the
/// producers and the producer's `send_timeout` has long elapsed.
fn send_outcome_kind(
    conn: &mut Connection,
    t0: Instant,
    close: Close,
    at: CloseAt,
) -> &'static str {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/close-then-reset".to_owned(),
        send_timeout: Some(SEND_TIMEOUT),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_frame(create_rid))
        .expect("ProducerSuccess");
    while conn.poll_event().is_some() {}

    let seq = conn
        .send(
            producer,
            OutgoingMessage {
                payload: Bytes::from_static(b"in flight at close"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 18,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send");
    let _ = conn.poll_transmit();

    let close_producer = |conn: &mut Connection| match close {
        Close::Awaited => conn.close_producer(producer),
        Close::Forgotten => conn.close_producer_forget(producer),
    };
    if matches!(at, CloseAt::BeforeReset) {
        close_producer(conn);
    }

    conn.reset();
    // A close the reset took is resolved by the reset itself: read the outcome there, before the
    // rebuild below could resolve it another way.
    if matches!(at, CloseAt::BeforeReset) {
        return outcome_kind(
            conn.take_outcome(PendingOpKey::Send(producer, seq))
                .as_ref(),
        );
    }
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("reconnect Connected");
    let _ = conn.poll_transmit();

    if matches!(at, CloseAt::BetweenResetAndRebuild) {
        close_producer(conn);
    }

    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    conn.handle_timeout(t0 + SEND_TIMEOUT + Duration::from_secs(3600));

    outcome_kind(
        conn.take_outcome(PendingOpKey::Send(producer, seq))
            .as_ref(),
    )
}

fn outcome_kind(outcome: Option<&OpOutcome>) -> &'static str {
    match outcome {
        None => "unresolved",
        Some(OpOutcome::Terminal { .. }) => "terminal",
        Some(OpOutcome::SendError { .. }) => "send-error",
        Some(_) => "other",
    }
}

fn assert_engines_resolve_the_send(close: Close, at: CloseAt) {
    let tokio = tokio_projection(|conn, t0| send_outcome_kind(conn, t0, close, at));
    let moonpool = moonpool_projection(|conn, t0| send_outcome_kind(conn, t0, close, at));
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the outcome of a send in flight at a producer close \
         ({close:?}, {at:?})"
    );
    assert!(
        matches!(tokio, "terminal" | "send-error"),
        "a send in flight at a producer close must resolve with Terminal or SendError after the \
         session resets ({close:?}, {at:?}); both engines report {tokio:?}"
    );
}

#[test]
fn engines_resolve_a_send_in_flight_at_an_awaited_close_when_the_session_resets() {
    assert_engines_resolve_the_send(Close::Awaited, CloseAt::BeforeReset);
}

#[test]
fn engines_resolve_a_send_in_flight_at_a_forgotten_close_when_the_session_resets() {
    assert_engines_resolve_the_send(Close::Forgotten, CloseAt::BeforeReset);
}

#[test]
fn engines_resolve_a_send_relocated_by_reset_when_the_producer_is_closed_before_the_rebuild() {
    assert_engines_resolve_the_send(Close::Awaited, CloseAt::BetweenResetAndRebuild);
}
