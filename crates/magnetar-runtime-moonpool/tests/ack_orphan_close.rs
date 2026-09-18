// SPDX-License-Identifier: Apache-2.0

//! Issue #346 — ack orphaned by same-broker `CloseConsumer` + no deadline —
//! moonpool engine twin of
//! `crates/magnetar-runtime-tokio/tests/ack_orphan_close.rs`.
//!
//! Every scenario locks [`ConnectionShared::inner`] directly and drives
//! `handle_bytes` / `handle_timeout` with injected [`Instant`]s instead of a
//! real driver task + TCP loopback — the same "no driver task, no TCP
//! listener" idiom `virtual_clock_send_timeout.rs` and
//! `virtual_clock_ack_timeout.rs` use (see `tests/common/mod.rs`'s module
//! doc). Keeps `cargo xtask check-runtime-test-parity` 1:1 (ADR-0024)
//! without a real host-clock wait on this side — the deadline scenario in
//! particular advances a synthetic clock for free, which is the whole point
//! of the moonpool engine existing.
//!
//! Two further scenarios cover the `AckResponse` command arm's own issue
//! #241 guard, one per branch:
//!
//! 3. `ack_response_broker_rejection_for_a_live_waiter_is_recorded_and_wakes_it` — the *record*
//!    branch. Unrelated to the sweeps above: a broker that outright rejects a still-pending,
//!    live-waiter ack must still have its error recorded and its caller woken.
//! 4. `a_late_ack_response_for_an_already_resolved_ack_records_nothing` — the *skip* branch, the
//!    one the guard exists for. A sweep already resolved the ack and its waiter already drained the
//!    outcome; the broker's real reply, still in flight, must record no second, undrainable entry.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, ConnectionConfig, ConsumerHandle, MessageId, OpOutcome, PendingOpKey, RequestId,
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
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
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
        // A real `ack().await` parks a waker on its first poll; simulate that
        // so the sweep's issue #241 waiter guard sees a live caller and still
        // records the synthetic error for it to consume (the guard's whole
        // point is skipping this record for a dropped, never-polled future).
        conn.register_waker(PendingOpKey::Request(rid), std::task::Waker::noop().clone());
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
        // A real `ack().await` parks a waker on its first poll; simulate that
        // so the reap sweep's issue #241 waiter guard sees a live caller and
        // still records the synthetic timeout error for it to consume.
        conn.register_waker(PendingOpKey::Request(rid), std::task::Waker::noop().clone());
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

/// Encode a broker `CommandAckResponse` rejecting `request_id` with `code` /
/// `message` — the shape a real broker error (not a synthetic sweep) takes.
fn ack_response_error_frame(
    handle: ConsumerHandle,
    request_id: RequestId,
    code: pb::ServerError,
    message: &str,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id: handle.0,
            request_id: Some(request_id.0),
            error: Some(code as i32),
            message: Some(message.to_owned()),
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAckResponse");
    buf
}

/// The `AckResponse` command arm's own issue #241 guard has two branches:
/// `Ok(()) => OpOutcome::Success` (already exercised without this test, by the
/// plain ack round-trip in `receiver_queue_auto_growth.rs`'s
/// `auto_adjust_schedule_survives_continuous_ack_response_traffic`) and
/// `Err(msg) => OpOutcome::Error`, exercised here for the first time.
///
/// A broker that outright rejects a still-pending, live-waiter ack —
/// `pending_requests.remove` finds the entry, so `kind.is_some()` is true —
/// must still record the `Error` outcome and wake the caller. The guard only
/// ever skips recording when `pending_requests.remove` found NOTHING (the
/// #346 sweep above or the `ack_response_timeout` backstop already resolved
/// it), never for an ordinary negative reply to a still-tracked ack.
#[test]
fn ack_response_broker_rejection_for_a_live_waiter_is_recorded_and_wakes_it() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let handle = {
        let mut conn = shared.inner.lock();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-response-rejected".to_owned(),
            subscription: "ack-response-rejected".to_owned(),
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
        // A real `ack().await` parks a waker on its first poll; simulate that
        // so the assertion below proves the broker's rejection actually wakes
        // a live caller, not just that an outcome landed in the slab.
        conn.register_waker(PendingOpKey::Request(rid), std::task::Waker::noop().clone());
        rid
    };

    {
        let mut conn = shared.inner.lock();
        let frame = ack_response_error_frame(
            handle,
            rid,
            pb::ServerError::AuthorizationError,
            "not authorized",
        );
        conn.handle_bytes(t0, &frame).expect("handle AckResponse");
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
            assert_eq!(
                code,
                pb::ServerError::AuthorizationError as i32,
                "the broker's error code must pass through unchanged"
            );
            assert_eq!(message, "not authorized");
        }
        other => panic!("expected the broker's rejection as an Error outcome, got {other:?}"),
    }
    assert!(
        !shared.inner.lock().has_pending_request_for_test(rid),
        "a resolved ack — success or rejection — must drain out of pending_requests"
    );
}

/// Encode a broker `CommandAckResponse` accepting `request_id` — no `error`,
/// no `message`, which is what the `AckResponse` arm reads as `Ok(())`.
fn ack_response_success_frame(handle: ConsumerHandle, request_id: RequestId) -> BytesMut {
    let cmd = pb::BaseCommand {
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
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAckResponse");
    buf
}

/// The guard's *skip* branch — the branch the fix exists for.
///
/// The #346 sweep above already resolved this ack and its waiter already
/// drained the outcome. The broker knows nothing about that: its real
/// `CommandAckResponse` for the same request id is still in flight and
/// arrives afterwards. `pending_requests.remove` now finds nothing, so
/// `kind` is `None` — there is no future left to `take_outcome` a second
/// record, and `outcomes` is pruned only by `take_outcome` or
/// `cancel_request` (`Connection::cancel_request`), never wholesale. Writing
/// the late reply into `outcomes` would therefore leak one entry
/// permanently, which is the issue #241 shape the `Success` arm's own
/// `None => {}` case has always avoided.
///
/// Same-broker `CloseConsumer` is only the cheapest way to reach that state
/// here; the `ack_response_timeout` backstop and a caller that simply
/// dropped its `ack()` future reach it identically.
#[test]
fn a_late_ack_response_for_an_already_resolved_ack_records_nothing() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let handle = {
        let mut conn = shared.inner.lock();
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-late-response".to_owned(),
            subscription: "ack-late-response".to_owned(),
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
        conn.register_waker(PendingOpKey::Request(rid), std::task::Waker::noop().clone());
        rid
    };

    // The sweep resolves the ack, and the caller's future consumes it — the
    // state a returning `ack().await` leaves behind.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &close_consumer_frame(handle))
            .expect("handle broker close");
    }
    let key = PendingOpKey::Request(rid);
    assert!(
        shared.inner.lock().take_outcome(key).is_some(),
        "precondition: the sweep resolved the ack for its live waiter"
    );

    // The broker's own reply lands afterwards, for a request nothing is
    // tracking any more.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &ack_response_success_frame(handle, rid))
            .expect("handle late AckResponse");
    }

    assert!(
        shared.inner.lock().take_outcome(key).is_none(),
        "a late broker reply for an already-resolved ack must record no second, \
         undrainable outcome (issue #241 leak shape)"
    );
}
