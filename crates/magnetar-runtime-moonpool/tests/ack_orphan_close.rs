// SPDX-License-Identifier: Apache-2.0

//! Issue #346 — ack orphaned by same-broker `CloseConsumer` + no deadline —
//! moonpool engine twin of
//! `crates/magnetar-runtime-tokio/tests/ack_orphan_close.rs`.
//!
//! Both scenarios lock [`ConnectionShared::inner`] directly and drive
//! `handle_bytes` / `handle_timeout` with injected [`Instant`]s instead of a
//! real driver task + TCP loopback — the same "no driver task, no TCP
//! listener" idiom `virtual_clock_send_timeout.rs` and
//! `virtual_clock_ack_timeout.rs` use (see `tests/common/mod.rs`'s module
//! doc). Keeps `cargo xtask check-runtime-test-parity` 1:1 (ADR-0024)
//! without a real host-clock wait on this side — the deadline scenario in
//! particular advances a synthetic clock for free, which is the whole point
//! of the moonpool engine existing.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, ConnectionConfig, ConsumerHandle, MessageId, OpOutcome, PendingOpKey,
    SubscribeRequest, encode_command, pb,
};

use crate::common::{handshake_complete_shared, handshake_complete_shared_with_config};

fn ack_message_id() -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id: 1,
        partition: -1,
        batch_index: -1,
        batch_size: -1,
    }
}

/// Broker-initiated same-broker `CommandCloseConsumer`
/// (`assigned_broker_service_url = None`) for `handle`. Mirrors the helper of
/// the same name in `magnetar-proto`'s `conn_state_tests` and the
/// differential `broker_close_resubscribe_equivalence.rs`.
fn close_consumer_frame(handle: ConsumerHandle) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id: handle.0,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandCloseConsumer");
    buf
}

/// Primary sweep (fast path): a same-broker `CloseConsumer` orphans a
/// pending ack — the close-handler sweep must fail it immediately.
#[test]
fn ack_orphaned_by_same_broker_close_fails_fast() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let handle = {
        let mut conn = shared.inner.lock();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-orphan-close".to_owned(),
            subscription: "ack-orphan-close".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        });
        let _ = conn.poll_transmit();
        handle
    };

    let rid = {
        let mut conn = shared.inner.lock();
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![ack_message_id()],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();
        rid
    };

    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &close_consumer_frame(handle))
            .expect("handle broker close");
    }

    let key = PendingOpKey::Request(rid);
    let outcome = shared.inner.lock().take_outcome(key);
    match outcome {
        Some(OpOutcome::Error {
            request_id,
            code,
            message,
        }) => {
            assert_eq!(request_id, rid);
            assert_eq!(code, -1, "orphaned-ack uses the -1 sentinel");
            assert_eq!(message, "ack orphaned by broker consumer close");
        }
        other => panic!("expected an orphaned-ack Error outcome, got {other:?}"),
    }
    assert!(
        !shared.inner.lock().has_pending_request_for_test(rid),
        "the orphaned ack must drain out of pending_requests"
    );
}

/// Backstop deadline: an ack whose `CommandAckResponse` never arrives fires
/// at exactly the configured deadline relative to the *virtual* clock — not
/// the host wall-clock. Mirrors `virtual_clock_send_timeout.rs`'s shape.
#[test]
fn ack_response_timeout_fires_at_virtual_deadline() {
    const ACK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

    let t0 = Instant::now();
    let shared = handshake_complete_shared_with_config(
        t0,
        ConnectionConfig {
            ack_response_timeout: Some(ACK_RESPONSE_TIMEOUT),
            ..ConnectionConfig::default()
        },
    );

    let handle = {
        let mut conn = shared.inner.lock();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-response-timeout".to_owned(),
            subscription: "ack-response-timeout".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        });
        let _ = conn.poll_transmit();
        handle
    };

    // Enqueue one ack. The proto layer stamps `enqueued_at = t0`. The broker
    // never responds — no CommandAckResponse is ever fed back.
    let rid = {
        let mut conn = shared.inner.lock();
        let rid = conn.ack(
            handle,
            AckRequest {
                message_ids: vec![ack_message_id()],
                ack_type: pb::command_ack::AckType::Individual,
                properties: Vec::new(),
                txn_id: None,
            },
            t0,
        );
        let _ = conn.poll_transmit();
        rid
    };
    let key = PendingOpKey::Request(rid);

    // Tick at t0 + 9.9s — strictly before the deadline. Still pending.
    let t_before = t0 + Duration::from_millis(9_900);
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t_before);
    }
    assert!(
        shared.inner.lock().take_outcome(key).is_none(),
        "ack must still be in-flight at t0 + 9.9s (timeout = 10s)"
    );
    assert!(
        shared.inner.lock().has_pending_request_for_test(rid),
        "pending entry must not drain before the virtual deadline",
    );

    // Tick at t0 + 10.1s — strictly after the deadline. The state machine
    // must surface a synthetic `Error(-1, "ack timeout")`.
    let t_after = t0 + Duration::from_millis(10_100);
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t_after);
    }
    let outcome = shared.inner.lock().take_outcome(key);
    match outcome {
        Some(OpOutcome::Error {
            request_id,
            code,
            message,
        }) => {
            assert_eq!(request_id, rid);
            assert_eq!(code, -1, "Pulsar timeout sentinel is -1");
            assert_eq!(message, "ack timeout");
        }
        other => panic!("expected an ack-timeout Error outcome, got {other:?}"),
    }
    assert!(
        !shared.inner.lock().has_pending_request_for_test(rid),
        "the timed-out ack must drain out of pending_requests"
    );
}
