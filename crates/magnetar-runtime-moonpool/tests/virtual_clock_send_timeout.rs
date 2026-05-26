// SPDX-License-Identifier: Apache-2.0

// Chaos scenarios value a single readable step-by-step `fn`. Splitting
// these into sub-helpers would obscure the synthetic frame sequence the
// test pins. We accept the line count.
#![allow(clippy::too_many_lines)]

//! Chaos scenario: `send_timeout` must fire at exactly the configured
//! deadline relative to the *virtual* clock — not the host wall-clock.
//!
//! Why this is moonpool territory: a `testcontainers` test would have to
//! sleep for the real timeout duration on the host wall-clock — slow,
//! flaky, and not actually a determinism check. The sans-io state machine
//! exposes [`magnetar_proto::Connection::handle_timeout`] with a caller-
//! supplied [`std::time::Instant`], so we can advance the virtual clock by
//! exactly the right amount and pin the boundary condition.
//!
//! ## Shape
//!
//! 1. Configure a producer with `send_timeout = 10s`.
//! 2. Enqueue one send. No `CommandSendReceipt` ever arrives — the broker is stuck (mid-handshake
//!    partition, slow IO, etc).
//! 3. Advance the virtual clock to `t0 + 9.9s` and tick `handle_timeout`. The send must still be
//!    pending; no `SendError` outcome yet.
//! 4. Advance to `t0 + 10.1s` and tick again. The send must now resolve with `OpOutcome::SendError
//!    { code: -1, message = "send timeout" }` — the synthetic timeout envelope the state machine
//!    surfaces (Pulsar's wire `ServerError` enum has no `TimeoutError`, so the proto layer uses
//!    `-1` as the timeout sentinel).

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, SequenceId, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::{handshake_response_bytes, send_receipt_bytes};

const SEND_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn send_timeout_fires_at_virtual_deadline() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    // Walk the handshake at virtual t0.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        let connected = handshake_response_bytes();
        conn.handle_bytes(t0, &connected).expect("Connected");
        let _ = conn.poll_event();
    }

    // Open a producer with the timeout knob set. We do NOT feed a
    // `CommandProducerSuccess` — the chaos contract is that the send
    // enters the pending-publish slab even before the open round-trip
    // completes (the proto layer queues sends optimistically). For
    // strictness, ack the open first so the producer is "ready" by the
    // time we enqueue.
    let req = CreateProducerRequest {
        topic: "persistent://public/default/send-timeout".to_owned(),
        send_timeout: Some(SEND_TIMEOUT),
        ..Default::default()
    };
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, request_id)
    };
    {
        let mut conn = shared.inner.lock();
        let ok = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "magnetar-test-send-timeout".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut buf, &ok).expect("encode ProducerSuccess");
        conn.handle_bytes(t0, &buf).expect("ProducerSuccess");
        let _ = conn.poll_event();
    }

    // Enqueue one send. The proto layer stamps `enqueued_at = t0`.
    let seq = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"will-time-out"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 13,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send")
    };
    assert_eq!(seq, SequenceId(0));
    assert_eq!(shared.inner.lock().producer_pending_count(handle), 1);

    // Tick at t0 + 9.9s — strictly before the deadline. The send must
    // still be pending; no synthetic SendError outcome yet.
    let t_before = t0 + Duration::from_millis(9_900);
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t_before);
    }
    assert!(
        shared
            .inner
            .lock()
            .take_outcome(PendingOpKey::Send(handle, seq))
            .is_none(),
        "send must still be in-flight at t0 + 9.9s (timeout = 10s)"
    );
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        1,
        "pending count must not decrement before the virtual deadline",
    );

    // Tick at t0 + 10.1s — strictly after the deadline. The state machine
    // must surface a synthetic `SendError(-1, "send timeout")`.
    let t_after = t0 + Duration::from_millis(10_100);
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t_after);
    }
    let outcome = shared
        .inner
        .lock()
        .take_outcome(PendingOpKey::Send(handle, seq));
    match outcome {
        Some(OpOutcome::SendError {
            sequence_id,
            code,
            message,
        }) => {
            assert_eq!(sequence_id, seq);
            assert_eq!(code, -1, "Pulsar timeout sentinel is -1");
            assert!(
                message.contains("timeout"),
                "expected timeout message, got {message:?}"
            );
        }
        other => panic!("expected SendError(send timeout), got {other:?}"),
    }

    // Sanity: a late receipt for the timed-out sequence is a no-op (the
    // pending slot is already gone). The state machine must not panic and
    // must not double-resolve.
    {
        let mut conn = shared.inner.lock();
        let late = send_receipt_bytes(handle, seq, 1, 1);
        conn.handle_bytes(t_after, &late)
            .expect("late receipt accepted gracefully");
    }
    assert!(
        shared
            .inner
            .lock()
            .take_outcome(PendingOpKey::Send(handle, seq))
            .is_none(),
        "late receipt for a timed-out send must not produce a second outcome"
    );
}
