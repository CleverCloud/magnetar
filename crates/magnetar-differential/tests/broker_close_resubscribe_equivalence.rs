// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for the same-broker
//! `CommandCloseConsumer{assigned_broker_service_url: None}` → silent in-place
//! re-subscribe path (issue #307 root-cause fix, PR #318).
//!
//! A `code=6` bundle reassignment makes the broker close a *running* consumer on
//! a LIVE socket. The sans-io [`magnetar_proto::Connection`] handles this by
//! re-subscribing the consumer IN PLACE (re-emit `CommandSubscribe`, reset the
//! permit mirror to 0, defer the initial `CommandFlow` to the re-subscribe
//! `Success`) and — crucially — by **NOT** surfacing a `ConsumerClosedByBroker`
//! event, so the re-attach is transparent to the runtime.
//!
//! That suppression is a concrete, user-observable change to the event stream
//! (`failover_active_reflow_equivalence.rs` only covers the
//! `ActiveConsumerChange` promotion path), so per GUIDELINES §Cross-runtime test
//! and CLAUDE.md invariant 9 it needs a `magnetar-differential` test asserting both
//! engines react identically: same suppressed close event, same fresh
//! `CommandSubscribe`, same zeroed permits, same deferred-then-re-armed flow.
//!
//! For contrast the test also drives the `Some(url)` (PIP-188 topic migration)
//! case, which MUST keep surfacing `ConsumerClosedByBroker` and must NOT
//! re-subscribe in place — both engines, identically.
//!
//! Issue #346 extension: an ack issued just before the close is now part of
//! the observable reaction. In the same-broker (`url = None`) case the close
//! handler's orphan sweep must fail that ack immediately
//! (`code=-1, "ack orphaned by broker consumer close"`) on both engines
//! identically; in the PIP-188 migration case (`url = Some`) the ack is left
//! pending (the generic `ack_response_timeout` backstop, not this sweep,
//! eventually reaps it) — also identically on both engines.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, MessageId,
    OpOutcome, PendingOpKey, SubscribeRequest, decode_one, encode_command, pb,
};

const RQ: usize = 8;

/// The observable reaction the two engines must agree on for one
/// `CommandCloseConsumer`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reaction {
    permits_before_close: u32,
    /// `ConsumerClosedByBroker` surfaced for this handle?
    saw_close_event: bool,
    /// A fresh `CommandSubscribe` was re-emitted on the same socket?
    resubscribed: bool,
    /// Permit mirror right after the close (before any re-subscribe ack).
    permits_after_close: u32,
    /// Flow grants emitted between the close and the re-subscribe ack (must be
    /// empty — pre-ack flow is dropped broker-side, so it is deferred).
    grants_before_ack: Vec<u32>,
    /// Flow grants emitted after the re-subscribe `Success`.
    grants_after_ack: Vec<u32>,
    /// Permit mirror after the re-subscribe ack re-arms flow.
    permits_after_ack: u32,
    /// Consumer still open (re-attach must not close it)?
    open_after: bool,
    /// Issue #346: outcome of an ack issued immediately before the close,
    /// read right after the close is handled (before the re-subscribe ack).
    /// `Some((code, message))` when the close's orphan sweep resolved it
    /// synchronously; `None` when it is still pending (the PIP-188 migration
    /// branch, which the sweep does not touch).
    ack_outcome_after_close: Option<(i32, String)>,
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

/// Broker-initiated `CommandCloseConsumer` for `handle`. `url = None` is a
/// same-broker bundle reassignment; `Some(_)` is a PIP-188 topic migration.
fn close_consumer_frame(handle: ConsumerHandle, url: Option<String>) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id: handle.0,
            request_id: 0,
            assigned_broker_service_url: url,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandCloseConsumer");
    buf
}

/// Feed a broker `CommandSuccess` for `request_id` (acks a (re-)subscribe).
fn feed_success(conn: &mut Connection, request_id: u64, t0: Instant) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &buf).expect("handle Success");
}

/// Drain the outbound buffer ONCE, bucketing `CommandSubscribe` request ids and
/// `CommandFlow` grants for `handle` (`poll_transmit` empties the buffer, so a
/// second call would see nothing — classify in one pass).
fn drain_outbound(conn: &mut Connection, handle: ConsumerHandle) -> (Vec<u64>, Vec<u32>) {
    let mut out = conn.poll_transmit();
    let (mut subs, mut grants) = (Vec::new(), Vec::new());
    while !out.is_empty() {
        let frame = decode_one(&mut out).expect("decode outbound");
        if frame.command.r#type == pb::base_command::Type::Subscribe as i32 {
            if let Some(sub) = frame.command.subscribe {
                if sub.consumer_id == handle.0 {
                    subs.push(sub.request_id);
                }
            }
        } else if frame.command.r#type == pb::base_command::Type::Flow as i32 {
            if let Some(flow) = frame.command.flow {
                if flow.consumer_id == handle.0 {
                    grants.push(flow.message_permits);
                }
            }
        }
    }
    (subs, grants)
}

/// Drain + report whether a `ConsumerClosedByBroker` event surfaced for `handle`.
fn drain_close_event(conn: &mut Connection, handle: ConsumerHandle) -> bool {
    let mut saw = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ConsumerClosedByBroker { handle: h, .. } = ev {
            if h == handle {
                saw = true;
            }
        }
    }
    saw
}

/// Drive handshake + subscribe-Failover + ack + initial flow over one engine's
/// locked `Connection`, then inject one broker close (`url`) and capture the
/// reaction.
fn lock_and_run(conn: &mut Connection, t0: Instant, url: Option<String>) -> Reaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/broker-close-equiv".to_owned(),
        subscription: "sub-broker-close-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        receiver_queue_size: RQ,
        ..Default::default()
    };
    let subscribe_rid = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(req);
    feed_success(conn, subscribe_rid, t0);
    let _ = conn.poll_event();
    assert!(
        conn.consume_initial_consumer_subscribe_completion(handle),
        "initial subscribe waiter completion must be consumed before exercising transparent re-attachment"
    );

    // Arm the initial flow so the consumer holds permits — the running, active
    // state the broker close hits in production.
    let _ = conn.initial_flow(handle, t0);
    let _ = drain_outbound(conn, handle); // discard subscribe + initial flow frames
    let permits_before_close = conn.consumer_available_permits(handle);

    // Issue #346: an ack in flight when the close lands — issued (and its
    // CommandAck frame drained off the wire) BEFORE peeking `resub_rid` below
    // so the peek still predicts the re-subscribe's request id, not this
    // ack's.
    let ack_rid = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 1,
                entry_id: 1,
                partition: -1,
                batch_index: -1,
                batch_size: -1,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        t0,
    );
    // A real `ack().await` parks a waker on its first poll; simulate that on
    // both engines so the close sweep's issue #241 waiter guard sees a live
    // caller and still records the synthetic error this test asserts on.
    conn.register_waker(
        PendingOpKey::Request(ack_rid),
        std::task::Waker::noop().clone(),
    );
    let _ = drain_outbound(conn, handle); // discard the CommandAck frame

    // The re-subscribe (if any) will allocate this request id.
    let resub_rid = conn.peek_next_request_id_for_test();

    // Broker close.
    conn.handle_bytes(t0, &close_consumer_frame(handle, url))
        .expect("handle close");
    let saw_close_event = drain_close_event(conn, handle);
    let (subs, grants_before_ack) = drain_outbound(conn, handle);
    let resubscribed = subs.contains(&resub_rid);
    let permits_after_close = conn.consumer_available_permits(handle);
    let ack_outcome_after_close =
        conn.take_outcome(PendingOpKey::Request(ack_rid))
            .map(|outcome| match outcome {
                OpOutcome::Error { code, message, .. } => (code, message),
                other => panic!("unexpected ack outcome shape: {other:?}"),
            });

    // Ack the re-subscribe (only meaningful when one was emitted; harmless
    // otherwise — an unmatched Success is ignored).
    feed_success(conn, resub_rid, t0);
    let _ = drain_close_event(conn, handle);
    let (_subs2, grants_after_ack) = drain_outbound(conn, handle);
    let permits_after_ack = conn.consumer_available_permits(handle);

    Reaction {
        permits_before_close,
        saw_close_event,
        resubscribed,
        permits_after_close,
        grants_before_ack,
        grants_after_ack,
        permits_after_ack,
        open_after: !conn.consumer_is_closed(handle),
        ack_outcome_after_close,
    }
}

fn run_both(url: Option<String>) -> (Reaction, Reaction) {
    let t0 = Instant::now();
    let tokio = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0, url.clone())
    };
    let moonpool = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0, url)
    };
    (tokio, moonpool)
}

#[test]
fn same_broker_close_resubscribe_event_streams_agree() {
    let (tokio_reaction, moonpool_reaction) = run_both(None);

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the same-broker close → in-place re-subscribe"
    );

    // The correct #307 root-cause behaviour on both engines: NO close event,
    // a fresh re-subscribe, permits zeroed, flow deferred to the ack then
    // re-armed to exactly RQ, consumer left open. Issue #346: the ack issued
    // just before the close is orphaned — the orphan sweep fails it
    // synchronously with the -1 sentinel, identically on both engines.
    let expected = Reaction {
        permits_before_close: RQ as u32,
        saw_close_event: false,
        resubscribed: true,
        permits_after_close: 0,
        grants_before_ack: vec![],
        grants_after_ack: vec![RQ as u32],
        permits_after_ack: RQ as u32,
        open_after: true,
        ack_outcome_after_close: Some((-1, "ack orphaned by broker consumer close".to_owned())),
    };
    assert_eq!(
        tokio_reaction, expected,
        "same-broker close must silently re-subscribe + defer-then-re-arm flow, got {tokio_reaction:?}"
    );
}

#[test]
fn topic_migration_close_event_streams_agree() {
    // PIP-188 migration (`url = Some`): the supervised reconnect path owns the
    // re-attach on the new URL, so the proto layer MUST surface
    // `ConsumerClosedByBroker` and MUST NOT re-subscribe in place — identically
    // on both engines.
    let (tokio_reaction, moonpool_reaction) = run_both(Some("pulsar://new-broker:6650".to_owned()));

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the topic-migration (url=Some) close"
    );
    assert!(
        tokio_reaction.saw_close_event,
        "migration close must surface ConsumerClosedByBroker"
    );
    assert!(
        !tokio_reaction.resubscribed,
        "migration close must NOT re-subscribe in place"
    );
    // Issue #346: the same-broker orphan sweep is scoped to the
    // `url = None` branch only (a generic all-kinds request deadline belongs
    // to #343's owner) — the PIP-188 migration branch must leave the
    // pre-close ack pending here, identically on both engines. The
    // `ack_response_timeout` backstop (not this sweep) is what eventually
    // reaps it.
    assert_eq!(
        tokio_reaction.ack_outcome_after_close, None,
        "migration close must NOT synchronously resolve the pre-close ack — only the \
         same-broker sweep does that"
    );
}

/// The refusal side of the in-place re-attach: a same-broker
/// `CommandCloseConsumer` whose handle the connection cannot re-subscribe.
///
/// `Connection::emit_in_place_consumer_resubscribe` (issue #414 factored it out
/// of this arm; issue #307 wrote it) declines two ways, and both must put
/// NOTHING on the wire and leave the connection serving: an unknown consumer id
/// has no `CommandSubscribe` to replay at all, and a consumer that is already
/// mid-re-attach has one in flight whose `Success` would otherwise be raced by a
/// second, unmatched subscribe.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Refusal {
    /// A fresh `CommandSubscribe` was emitted for the handle?
    resubscribed: bool,
    /// `ConsumerClosedByBroker` surfaced for the handle?
    saw_close_event: bool,
    /// Connection still serving (a refusal is not a protocol error)?
    connected_after: bool,
}

/// Feed a same-broker close for a consumer id that was never subscribed.
fn run_unknown_handle(conn: &mut Connection, t0: Instant) -> Refusal {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    while conn.poll_event().is_some() {}

    let ghost = ConsumerHandle(4242);
    let next_request_id = conn.peek_next_request_id_for_test();
    conn.handle_bytes(t0, &close_consumer_frame(ghost, None))
        .expect("handle close for an unknown consumer");
    let saw_close_event = drain_close_event(conn, ghost);
    let (subs, _grants) = drain_outbound(conn, ghost);
    Refusal {
        resubscribed: subs.contains(&next_request_id),
        saw_close_event,
        connected_after: conn.is_connected(),
    }
}

/// Feed a same-broker close for a consumer whose re-attach is already in flight
/// (`flow_on_subscribe_ack`, set by `rebuild_consumers` after a reset).
fn run_reattach_in_flight(conn: &mut Connection, t0: Instant) -> Refusal {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    while conn.poll_event().is_some() {}

    let subscribe_rid = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/broker-close-refusal".to_owned(),
        subscription: "sub-broker-close-refusal".to_owned(),
        receiver_queue_size: RQ,
        ..Default::default()
    });
    feed_success(conn, subscribe_rid, t0);
    assert!(conn.consume_initial_consumer_subscribe_completion(handle));
    while conn.poll_event().is_some() {}
    let _ = conn.initial_flow(handle, t0);
    let _ = drain_outbound(conn, handle);

    // Reconnect: `rebuild_consumers` re-emits `CommandSubscribe` and arms the
    // flow gate, so a re-attach is pending and unacked.
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("re-Connected");
    while conn.poll_event().is_some() {}
    let rebuilt = conn.rebuild_consumers();
    assert_eq!(rebuilt.len(), 1, "one consumer to rebuild");
    let _ = drain_outbound(conn, handle);

    // Now the broker closes it. The re-attach already in flight owns the next
    // `Success`; a second subscribe here would leave one of them unmatched.
    let next_request_id = conn.peek_next_request_id_for_test();
    conn.handle_bytes(t0, &close_consumer_frame(handle, None))
        .expect("handle close mid-re-attach");
    let saw_close_event = drain_close_event(conn, handle);
    let (subs, _grants) = drain_outbound(conn, handle);
    Refusal {
        resubscribed: subs.contains(&next_request_id),
        saw_close_event,
        connected_after: conn.is_connected(),
    }
}

fn run_refusal_both(scenario: fn(&mut Connection, Instant) -> Refusal) -> (Refusal, Refusal) {
    let t0 = Instant::now();
    let tokio = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        scenario(&mut conn, t0)
    };
    let moonpool = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        scenario(&mut conn, t0)
    };
    (tokio, moonpool)
}

#[test]
fn same_broker_close_for_an_unknown_consumer_is_refused_identically() {
    let (tokio_refusal, moonpool_refusal) = run_refusal_both(run_unknown_handle);
    assert_eq!(
        tokio_refusal, moonpool_refusal,
        "tokio and moonpool diverged on a same-broker close for an unknown consumer",
    );
    assert_eq!(
        tokio_refusal,
        Refusal {
            resubscribed: false,
            saw_close_event: false,
            connected_after: true,
        },
        "a close naming a consumer we never subscribed has nothing to replay: no \
         `CommandSubscribe`, no surfaced close event, and the connection keeps serving",
    );
}

#[test]
fn same_broker_close_during_a_pending_reattach_is_refused_identically() {
    let (tokio_refusal, moonpool_refusal) = run_refusal_both(run_reattach_in_flight);
    assert_eq!(
        tokio_refusal, moonpool_refusal,
        "tokio and moonpool diverged on a same-broker close during a pending re-attach",
    );
    assert_eq!(
        tokio_refusal,
        Refusal {
            resubscribed: false,
            saw_close_event: false,
            connected_after: true,
        },
        "the re-attach already in flight owns the next subscribe `Success`; a second \
         `CommandSubscribe` here would leave one of the two unmatched and the flow gate stuck",
    );
}

fn attached_terminal_consumer() -> (Connection, ConsumerHandle, Instant) {
    let now = Instant::now();
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_transmit();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/terminal-lifecycle".to_owned(),
        subscription: "terminal-lifecycle".to_owned(),
        ..Default::default()
    });
    let mut wire = conn.poll_transmit();
    let subscribe = decode_one(&mut wire)
        .expect("Subscribe frame")
        .command
        .subscribe
        .expect("Subscribe");
    assert_eq!(subscribe.consumer_id, handle.0);
    assert!(wire.is_empty());
    feed_success(&mut conn, subscribe.request_id, now);
    assert!(conn.consume_initial_consumer_subscribe_completion(handle));
    while conn.poll_event().is_some() {}
    (conn, handle, now)
}

#[test]
fn terminal_close_reset_preserves_completion_without_replaying_the_consumer() {
    use magnetar_proto::consumer::ConsumerCloseCompletion;

    for forgotten in [false, true] {
        let (mut conn, handle, now) = attached_terminal_consumer();
        let request = if forgotten {
            conn.close_consumer_forget(handle, now)
        } else {
            conn.close_consumer(handle, now)
        }
        .expect("close admitted");
        let mut wire = conn.poll_transmit();
        let close = decode_one(&mut wire)
            .expect("Close frame")
            .command
            .close_consumer
            .expect("CloseConsumer");
        assert_eq!((close.consumer_id, close.request_id), (handle.0, request.0));
        assert!(wire.is_empty());
        conn.reset();
        let slot = conn.consumer(handle).expect("reusable closed slot");
        assert!(slot.state.lock().closed);
        assert_eq!(
            slot.state.lock().close_completion(),
            Some(ConsumerCloseCompletion::SessionLost)
        );
        assert_eq!(conn.close_consumer(handle, now), Ok(request));
        assert!(!conn.has_pending_request_for_test(request));
        assert!(conn.take_outcome(PendingOpKey::Request(request)).is_none());
        assert!(conn.poll_transmit().is_empty());
        conn.begin_handshake().expect("new handshake");
        conn.handle_bytes(now, &handshake_response_bytes())
            .expect("new Connected");
        let _ = conn.poll_transmit();
        while conn.poll_event().is_some() {}
        assert_eq!(
            conn.rebuild_consumers(),
            [] as [magnetar_proto::RequestId; 0]
        );
        feed_success(&mut conn, request.0, now);
        assert_eq!(
            conn.consumer(handle)
                .expect("closed slot")
                .state
                .lock()
                .close_completion(),
            Some(ConsumerCloseCompletion::SessionLost)
        );
        assert!(conn.poll_event().is_none());
        assert!(conn.take_outcome(PendingOpKey::Request(request)).is_none());
        assert!(conn.poll_transmit().is_empty());
    }
}

#[test]
fn terminal_close_rejection_keeps_completion_and_bounds_forgotten_retry() {
    use magnetar_proto::consumer::ConsumerCloseCompletion;

    for forgotten in [false, true] {
        let (mut conn, handle, now) = attached_terminal_consumer();
        let request = if forgotten {
            conn.close_consumer_forget(handle, now)
        } else {
            conn.close_consumer(handle, now)
        }
        .expect("close admitted");
        let _ = conn.poll_transmit();
        let rejection = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: request.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "broker could not close the consumer".to_owned(),
            }),
            ..Default::default()
        };
        let mut wire = BytesMut::new();
        encode_command(&mut wire, &rejection).expect("encode rejection");
        conn.handle_bytes(now, &wire).expect("broker rejection");
        assert_eq!(
            conn.consumer(handle)
                .expect("closed slot")
                .state
                .lock()
                .close_completion(),
            Some(ConsumerCloseCompletion::BrokerError {
                code: pb::ServerError::ServiceNotReady as i32,
                message: "broker could not close the consumer".to_owned(),
            })
        );
        assert_eq!(conn.close_consumer(handle, now), Ok(request));
        assert!(conn.take_outcome(PendingOpKey::Request(request)).is_none());
        assert!(conn.poll_transmit().is_empty());
        let retry = conn.force_close_consumer_forget(handle, now);
        if forgotten {
            assert!(retry.is_none());
        } else {
            let retry = retry.expect("one forgotten retry of a reliable failure");
            let mut wire = conn.poll_transmit();
            let close = decode_one(&mut wire)
                .expect("retry frame")
                .command
                .close_consumer
                .expect("CloseConsumer retry");
            assert_eq!((close.consumer_id, close.request_id), (handle.0, retry.0));
            assert!(wire.is_empty());
            feed_success(&mut conn, retry.0, now);
            assert!(conn.take_outcome(PendingOpKey::Request(retry)).is_none());
        }
        assert!(conn.force_close_consumer_forget(handle, now).is_none());
        assert!(conn.poll_event().is_none());
        assert!(conn.poll_transmit().is_empty());
    }
}

#[test]
fn terminal_connection_rejects_new_raw_consumer_close_and_unsubscribe_without_allocation() {
    use magnetar_proto::consumer::{ConsumerCloseCompletion, ConsumerTerminalAdmissionError};

    for unsubscribe in [false, true] {
        let (mut conn, original, now) = attached_terminal_consumer();
        conn.fail_all_pending("connection permanently closed");
        let next_request = conn.peek_next_request_id_for_test();
        for handle in [original, ConsumerHandle(u64::MAX)] {
            let result = if unsubscribe {
                conn.unsubscribe(handle, false)
                    .map(|admission| admission.request_id)
            } else {
                conn.close_consumer(handle, now)
            };
            assert_eq!(result, Err(ConsumerTerminalAdmissionError::Terminal));
            assert_eq!(conn.peek_next_request_id_for_test(), next_request);
            assert!(conn.poll_transmit().is_empty());
        }
        assert!(
            conn.consumer(original)
                .expect("original terminal slot")
                .state
                .lock()
                .closed
        );
        let raw = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/raw-after-terminal".to_owned(),
            subscription: "raw-after-terminal".to_owned(),
            ..Default::default()
        });
        assert_ne!(raw, original);
        let slot = ToOwned::to_owned(conn.consumer(raw).expect("raw admission retained its slot"));
        assert!(!slot.state.lock().closed);
        let next_request = conn.peek_next_request_id_for_test();
        let mut wire = conn.poll_transmit();
        let subscribe = decode_one(&mut wire)
            .expect("raw Subscribe frame")
            .command
            .subscribe
            .expect("raw Subscribe");
        assert_eq!(subscribe.consumer_id, raw.0);
        assert_eq!(subscribe.request_id + 1, next_request);
        assert!(wire.is_empty());
        let result = if unsubscribe {
            conn.unsubscribe(raw, false)
                .map(|admission| admission.request_id)
        } else {
            conn.close_consumer(raw, now)
        };
        assert_eq!(result, Err(ConsumerTerminalAdmissionError::Terminal));
        assert!(slot.state.lock().closed);
        assert_eq!(
            slot.state.lock().close_completion(),
            Some(ConsumerCloseCompletion::Terminal {
                reason: "connection permanently closed".to_owned(),
            })
        );
        assert_eq!(conn.peek_next_request_id_for_test(), next_request);
        assert!(!conn.has_pending_request_for_test(magnetar_proto::RequestId(next_request)));
        assert_eq!(
            conn.rebuild_consumers(),
            [] as [magnetar_proto::RequestId; 0]
        );
        assert!(conn.poll_transmit().is_empty());
        assert!(conn.force_close_consumer_forget(raw, now).is_none());
    }
}

#[test]
fn unsubscribe_completion_after_cancelled_pending_subscribe_does_not_restore_its_slot() {
    for rejected in [false, true] {
        let (mut conn, original, now) = attached_terminal_consumer();
        let original_slot = ToOwned::to_owned(conn.consumer(original).expect("witness slot"));
        let handle = conn.subscribe(SubscribeRequest {
            topic: "persistent://public/default/cancelled-unsubscribe".to_owned(),
            subscription: "cancelled-unsubscribe".to_owned(),
            ..Default::default()
        });
        let mut wire = conn.poll_transmit();
        let subscribe = decode_one(&mut wire)
            .expect("pending Subscribe")
            .command
            .subscribe
            .expect("Subscribe body");
        assert_eq!(subscribe.consumer_id, handle.0);
        assert_eq!(
            subscribe.topic,
            "persistent://public/default/cancelled-unsubscribe"
        );
        assert_ne!(handle, original);
        assert!(wire.is_empty());
        let admission = conn
            .unsubscribe(handle, false)
            .expect("unsubscribe admitted");
        let request = admission.request_id;
        let mut wire = conn.poll_transmit();
        let unsubscribe = decode_one(&mut wire)
            .expect("Unsubscribe")
            .command
            .unsubscribe
            .expect("Unsubscribe body");
        assert_eq!(
            (unsubscribe.consumer_id, unsubscribe.request_id),
            (handle.0, request.0)
        );
        assert!(wire.is_empty());
        drop(admission);
        let close_request = conn.peek_next_request_id_for_test();
        conn.cancel_consumer_subscribe(handle);
        let mut wire = conn.poll_transmit();
        let close = decode_one(&mut wire)
            .expect("cancelled Subscribe Close")
            .command
            .close_consumer
            .expect("Close body");
        assert_eq!(
            (close.consumer_id, close.request_id),
            (handle.0, close_request)
        );
        assert!(wire.is_empty());
        assert!(conn.consumer(handle).is_none());
        assert!(std::sync::Arc::ptr_eq(
            conn.consumer(original).expect("same witness slot"),
            &original_slot,
        ));
        assert!(
            !conn.has_pending_request_for_test(magnetar_proto::RequestId(subscribe.request_id))
        );
        assert!(conn.has_pending_request_for_test(request));
        let next_request = conn.peek_next_request_id_for_test();
        assert_eq!(next_request, close_request + 1);
        feed_success(&mut conn, close_request, now);
        feed_success(&mut conn, subscribe.request_id, now);
        if rejected {
            let mut bytes = BytesMut::new();
            encode_command(
                &mut bytes,
                &pb::BaseCommand {
                    r#type: pb::base_command::Type::Error as i32,
                    error: Some(pb::CommandError {
                        request_id: request.0,
                        error: pb::ServerError::ServiceNotReady as i32,
                        message: "subscription was not removed".to_owned(),
                    }),
                    ..Default::default()
                },
            )
            .expect("encode unsubscribe rejection");
            conn.handle_bytes(now, &bytes)
                .expect("late unsubscribe rejection");
        } else {
            feed_success(&mut conn, request.0, now);
        }
        for id in [subscribe.request_id, request.0, close_request] {
            let id = magnetar_proto::RequestId(id);
            assert!(!conn.has_pending_request_for_test(id));
            assert!(conn.take_outcome(PendingOpKey::Request(id)).is_none());
        }
        assert!(conn.consumer(handle).is_none());
        let witness = conn.consumer(original).expect("unrelated live slot");
        assert!(!witness.state.lock().closed);
        assert_eq!(
            conn.consumer_topic(original),
            Some("persistent://public/default/terminal-lifecycle")
        );
        assert_eq!(conn.peek_next_request_id_for_test(), next_request);
        assert_eq!(conn.pending_waker_count(), 0);
        assert!(conn.poll_event().is_none());
        assert!(conn.poll_transmit().is_empty());
    }
}

#[test]
fn terminal_connection_wakes_a_close_observer_before_close_admission() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use magnetar_proto::consumer::ConsumerCloseCompletion;

    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (mut conn, handle, _) = attached_terminal_consumer();
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let slot = ToOwned::to_owned(conn.consumer(handle).expect("live slot"));
    slot.state
        .lock()
        .register_close_waker(Waker::from(ToOwned::to_owned(&wakes)));
    let next_request = conn.peek_next_request_id_for_test();
    conn.fail_all_pending("terminal close observer");
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(slot.state.lock().closed);
    assert!(slot.state.lock().close_waiters.is_empty());
    assert_eq!(
        slot.state.lock().close_completion(),
        Some(ConsumerCloseCompletion::Terminal {
            reason: "terminal close observer".to_owned(),
        })
    );
    conn.fail_all_pending("repeated terminal observation");
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        slot.state.lock().close_completion(),
        Some(ConsumerCloseCompletion::Terminal {
            reason: "terminal close observer".to_owned(),
        })
    );
    assert_eq!(conn.peek_next_request_id_for_test(), next_request);
    assert_eq!(conn.pending_waker_count(), 0);
    for expected in ["terminal close observer", "repeated terminal observation"] {
        assert!(
            matches!(conn.poll_event(), Some(ConnectionEvent::Closed { reason })
            if reason.as_deref() == Some(expected))
        );
    }
    assert!(conn.poll_event().is_none());
    assert!(conn.poll_transmit().is_empty());
}
