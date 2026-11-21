// SPDX-License-Identifier: Apache-2.0

//! A send that is in flight when its producer is closed must not hang when the session resets
//! before the broker acknowledges the close, exercised through the tokio engine's
//! [`ConnectionShared`] state. Mirrors the matching `magnetar-runtime-moonpool` test, its
//! `magnetar-proto` unit test (`crates/magnetar-proto/tests/producer_close.rs`) and the
//! `magnetar-differential` equivalence scenario.
//!
//! A close issued after `Connection::reset()` relocated the producer's replayable sends into the
//! reset snapshots, and before the supervisor's `rebuild_producers`, leaves the producer out of the
//! rebuild, which discarded its snapshots. Nothing resolved the send's future afterwards: it was in
//! no live queue, no snapshot bucket and no pending request, so even the producer's `send_timeout`
//! sweep could not reach it, and it stayed pending until the whole client shut down. The `SendFut`
//! holds only the shared state and the producer handle, so it outlives `Producer::close(self)` and
//! the last-clone drop guard.
//!
//! A close still unacknowledged when the session resets is failed by `reset()` itself, which runs
//! `fail_sends_of_closed_producer` for every close request it takes: the two cases here that close
//! before the transport drops guard that path, and the third is the one the rebuild must fail.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle, SequenceId, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

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

fn producer_success(request_id: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "magnetar-test-producer".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandProducerSuccess");
    buf
}

/// One unbatched send is in flight, its producer is closed, and the transport drops. Returns the
/// outcome the send's future would find once the supervisor has re-handshaked, re-attached the
/// producers and the producer's `send_timeout` has long elapsed.
fn send_outcome_after_close_and_reset(close: Close, at: CloseAt) -> Option<OpOutcome> {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(magnetar_proto::ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let create_rid = conn.peek_next_request_id_for_test();
    let producer: ProducerHandle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/close-then-reset".to_owned(),
        send_timeout: Some(SEND_TIMEOUT),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success(create_rid))
        .expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}

    let seq: SequenceId = conn
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

    let close_producer = |conn: &mut magnetar_proto::Connection| match close {
        Close::Awaited => conn.close_producer(producer),
        Close::Forgotten => conn.close_producer_forget(producer),
    };
    if matches!(at, CloseAt::BeforeReset) {
        close_producer(&mut conn);
    }

    // The transport drops before the broker acknowledged the close: the supervisor resets the
    // connection, redials and re-handshakes.
    conn.reset();
    // A close the reset took is resolved by the reset itself: read the outcome there, before the
    // rebuild below could resolve it another way.
    if matches!(at, CloseAt::BeforeReset) {
        return conn.take_outcome(PendingOpKey::Send(producer, seq));
    }
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("reconnect Connected");
    let _ = conn.poll_transmit();

    if matches!(at, CloseAt::BetweenResetAndRebuild) {
        close_producer(&mut conn);
    }

    // The driver re-attaches every producer once the handshake completes. The closed one is not
    // re-attached, and no `CommandCloseProducer` ack will ever come for its in-flight send.
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();

    // Even with `send_timeout` configured and long elapsed, the send must resolve.
    conn.handle_timeout(t0 + SEND_TIMEOUT + Duration::from_secs(3600));
    conn.take_outcome(PendingOpKey::Send(producer, seq))
}

fn resolves_the_send_with_a_failure(outcome: Option<&OpOutcome>) -> bool {
    matches!(
        outcome,
        Some(OpOutcome::Terminal { .. } | OpOutcome::SendError { .. })
    )
}

#[test]
fn send_in_flight_at_awaited_close_resolves_when_the_session_resets_before_the_close_ack() {
    let outcome = send_outcome_after_close_and_reset(Close::Awaited, CloseAt::BeforeReset);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send in flight at an awaited producer close must resolve with Terminal or SendError \
         at the reset that takes the close, before the rebuild \
         (got {outcome:?})"
    );
}

#[test]
fn send_in_flight_at_forgotten_close_resolves_when_the_session_resets_before_the_close_ack() {
    let outcome = send_outcome_after_close_and_reset(Close::Forgotten, CloseAt::BeforeReset);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send in flight at a forgotten producer close must resolve with Terminal or SendError \
         at the reset that takes the close, before the rebuild \
         (got {outcome:?})"
    );
}

#[test]
fn send_relocated_by_reset_resolves_when_the_producer_is_closed_before_the_rebuild() {
    let outcome =
        send_outcome_after_close_and_reset(Close::Awaited, CloseAt::BetweenResetAndRebuild);
    assert!(
        resolves_the_send_with_a_failure(outcome.as_ref()),
        "a send relocated by reset() must resolve with Terminal or SendError when its producer is \
         closed before the rebuild, even once send_timeout elapsed (got {outcome:?})"
    );
}
