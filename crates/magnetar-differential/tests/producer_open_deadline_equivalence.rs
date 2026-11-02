// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool parity for the `operation_timeout` deadline on a
//! pending `CommandProducer` (issue #887 root-cause fix, ADR-0125).
//!
//! [ADR-0106](../../../specs/adr/0106-reattach-broker-closed-producer-in-place.md) re-attaches
//! a broker-closed producer in place but recorded this residual without fixing it:
//! `Connection::handle_timeout` swept `PendingRequestKind::Ack` alone, so a re-attach the
//! broker never answered parked the slot at `broker_ready = false,
//! open_request_id = Some(_)` for the life of the connection. Every publish routed there
//! resolved `code=-1 "send timeout"` forever, and every later broker close was refused an
//! in-place re-attach because `open_request_id.is_some()` is a refusal.
//!
//! The fix adds a new deadline to `poll_timeout` and two new dispositions to
//! `handle_timeout`, both of which change the user-observable `EventStream`
//! (`ProducerOpenFailedTransient` while the ADR-0080 budget allows another re-issue,
//! `ProducerOpenFailed` once it does not), so per GUIDELINES §Cross-runtime test and
//! CLAUDE.md invariant 9 both engines must react identically: same armed deadline, same
//! swept request, same attempt accounting, same event, same recovery.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, CreateProducerRequest, OperationRetryConfig,
    ProducerHandle, RequestId, decode_one, encode_command, pb,
};

/// Shorter than the 30 s default keepalive interval, so a deadline `poll_timeout` surfaces at
/// `t0 + OPEN_TIMEOUT` can only have come from the pending-open arm.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// The pending request's own lifecycle around its deadline. Grouped, like the sibling
/// re-attach suite's `AttachmentReadiness`, so no struct here carries more than three bools.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingOpenLifecycle {
    /// `poll_timeout` surfaced exactly the pending open's deadline while it was in flight.
    armed_deadline: bool,
    /// The re-attach request was pending before its deadline.
    pending_before: bool,
    /// Still pending after the sweep? (Must be `false`, or `poll_timeout` would keep
    /// returning an elapsed instant and spin the driver loop.)
    pending_after: bool,
}

/// What the sweep left of the producer slot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SlotDisposition {
    /// Producer slot still registered after the sweep.
    present: bool,
    /// Send-drain gate open after the sweep? (Must be `false` either way: Pulsar closes the
    /// whole connection on a `CommandSend` for a producer that is not ready.)
    gate_open: bool,
    /// Does the expired generation still own the producer's open, so the ADR-0080 retry leg
    /// may re-emit (`producer_open_retry_is_current`)?
    retry_is_current: bool,
}

/// The observable reaction the two engines must agree on for one unanswered re-attach.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reaction {
    /// The pending request's lifecycle around its deadline.
    lifecycle: PendingOpenLifecycle,
    /// What the sweep left of the producer slot.
    slot: SlotDisposition,
    /// `transient_open_attempts` after the sweep.
    attempts_after: u32,
    /// `(variant, code, message)` of every producer-open event the sweep surfaced for this
    /// handle, in queue order.
    events_after: Vec<(&'static str, i32, String)>,
    /// `CommandProducer.epoch` of the re-issue the retry leg managed to emit, if any.
    reissue_epoch: Option<u64>,
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

/// Broker-initiated `CommandCloseProducer` for `handle` — what
/// `ServerCnx.closeProducer` writes after `safelyRemoveProducer` on a connection that keeps
/// serving.
fn close_producer_frame(handle: ProducerHandle) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseProducer as i32,
        close_producer: Some(pb::CommandCloseProducer {
            producer_id: handle.0,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandCloseProducer");
    buf
}

/// Feed a broker `CommandProducerSuccess` for `request_id` (acks a (re-)attach).
fn feed_producer_success(conn: &mut Connection, request_id: u64, t0: Instant) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "p-open-deadline-equiv".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode ProducerSuccess");
    conn.handle_bytes(t0, &buf).expect("handle ProducerSuccess");
}

/// Drain the outbound buffer once, returning the `(request_id, epoch)` of every
/// `CommandProducer` naming `handle` (`poll_transmit` empties the buffer).
fn drain_producer_opens(conn: &mut Connection, handle: ProducerHandle) -> Vec<(u64, Option<u64>)> {
    let mut out = conn.poll_transmit();
    let mut opens = Vec::new();
    while !out.is_empty() {
        let frame = decode_one(&mut out).expect("decode outbound");
        if frame.command.r#type == pb::base_command::Type::Producer as i32 {
            if let Some(p) = frame.command.producer {
                if p.producer_id == handle.0 {
                    opens.push((p.request_id, p.epoch));
                }
            }
        }
    }
    opens
}

/// Drain the event queue, keeping the producer-open dispositions for `handle`.
fn drain_open_events(
    conn: &mut Connection,
    handle: ProducerHandle,
) -> Vec<(&'static str, i32, String)> {
    let mut seen = Vec::new();
    while let Some(event) = conn.poll_event() {
        match event {
            ConnectionEvent::ProducerOpenFailedTransient {
                handle: h,
                code,
                message,
            } if h == handle => seen.push(("ProducerOpenFailedTransient", code, message)),
            ConnectionEvent::ProducerOpenFailed {
                handle: h,
                code,
                message,
            } if h == handle => seen.push(("ProducerOpenFailed", code, message)),
            _ => {}
        }
    }
    seen
}

/// Handshake, attach a producer, let the broker close it on the live socket, then let the
/// in-place re-attach go unanswered until its deadline elapses. Capture the reaction.
fn lock_and_run(conn: &mut Connection, t0: Instant, max_retries: Option<u32>) -> Reaction {
    conn.set_operation_retry_config(OperationRetryConfig {
        max_retries,
        ..OperationRetryConfig::default()
    });
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");

    let open_rid = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/open-deadline-887-equiv".to_owned(),
        ..Default::default()
    });
    let _ = drain_producer_opens(conn, handle);
    feed_producer_success(conn, open_rid, t0);
    let _ = drain_open_events(conn, handle);

    // The broker detaches the producer id on the live socket. ADR-0106 re-attaches in place;
    // this broker never answers that `CommandProducer`.
    let reattach_rid = conn.peek_next_request_id_for_test();
    conn.handle_bytes(t0, &close_producer_frame(handle))
        .expect("handle CommandCloseProducer");
    let _ = drain_producer_opens(conn, handle);
    let _ = drain_open_events(conn, handle);

    let armed_open_deadline = conn.poll_timeout() == Some(t0 + OPEN_TIMEOUT);
    let pending_before_deadline = conn.has_pending_request_for_test(RequestId(reattach_rid));

    conn.handle_timeout(t0 + OPEN_TIMEOUT);

    let pending_after_deadline = conn.has_pending_request_for_test(RequestId(reattach_rid));
    let attempts_after = conn.producer_transient_open_attempts(handle);
    let slot_present_after = conn.producer(handle).is_some();
    let gate_open_after = conn
        .producer(handle)
        .is_some_and(|slot| slot.state.lock().broker_ready);
    let events_after = drain_open_events(conn, handle);
    let retry_is_current_after =
        conn.producer_open_retry_is_current(handle, RequestId(reattach_rid));
    let reissue_epoch = conn
        .retry_producer_open_if_current(handle, RequestId(reattach_rid))
        .and_then(|rid| {
            drain_producer_opens(conn, handle)
                .into_iter()
                .find(|(emitted, _)| *emitted == rid.0)
                .map(|(_, epoch)| epoch.unwrap_or(0))
        });

    Reaction {
        lifecycle: PendingOpenLifecycle {
            armed_deadline: armed_open_deadline,
            pending_before: pending_before_deadline,
            pending_after: pending_after_deadline,
        },
        slot: SlotDisposition {
            present: slot_present_after,
            gate_open: gate_open_after,
            retry_is_current: retry_is_current_after,
        },
        attempts_after,
        events_after,
        reissue_epoch,
    }
}

fn run_both(max_retries: Option<u32>) -> (Reaction, Reaction) {
    let t0 = Instant::now();
    let config = ConnectionConfig {
        operation_timeout: OPEN_TIMEOUT,
        ..ConnectionConfig::default()
    };
    let tokio = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(config.clone());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0, max_retries)
    };
    let moonpool = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(config);
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0, max_retries)
    };
    (tokio, moonpool)
}

/// Budget remaining: the deadline turns the permanent wedge into one more ADR-0080 retry
/// leg. The slot survives with a shut gate, the expired generation stays the leg's liveness
/// token, and the re-issue lands at the next epoch.
#[test]
fn unanswered_producer_open_deadline_event_streams_agree() {
    let (tokio_reaction, moonpool_reaction) = run_both(Some(3));

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the expiry of an unanswered CommandProducer"
    );
    assert_eq!(
        tokio_reaction,
        Reaction {
            lifecycle: PendingOpenLifecycle {
                armed_deadline: true,
                pending_before: true,
                pending_after: false,
            },
            slot: SlotDisposition {
                present: true,
                gate_open: false,
                retry_is_current: true,
            },
            attempts_after: 1,
            events_after: vec![(
                "ProducerOpenFailedTransient",
                -1,
                "producer open timeout".to_owned()
            )],
            reissue_epoch: Some(2),
        },
        "a budget-remaining expiry must sweep the request, consume one attempt, keep the \
         gate shut and let the retry leg re-issue at the next epoch, got {tokio_reaction:?}"
    );
}

/// Budget exhausted: the expiry terminalizes the slot through
/// `fail_producer_open_with_broker_error` instead of arming a leg nobody would honour, so a
/// parked publish resolves `Err` on the open's deadline rather than waiting out its own
/// `send_timeout`.
#[test]
fn exhausted_producer_open_budget_terminalization_event_streams_agree() {
    let (tokio_reaction, moonpool_reaction) = run_both(Some(0));

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the terminal disposition of an unanswered \
         CommandProducer"
    );
    assert_eq!(
        tokio_reaction,
        Reaction {
            lifecycle: PendingOpenLifecycle {
                armed_deadline: true,
                pending_before: true,
                pending_after: false,
            },
            slot: SlotDisposition {
                present: false,
                gate_open: false,
                retry_is_current: false,
            },
            attempts_after: 0,
            events_after: vec![("ProducerOpenFailed", -1, "producer open timeout".to_owned())],
            reissue_epoch: None,
        },
        "an exhausted budget must drop the slot and surface the terminal disposition, got \
         {tokio_reaction:?}"
    );
}
