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
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            }],
            ack_type: pb::command_ack::AckType::Individual,
            properties: Vec::new(),
            txn_id: None,
        },
        t0,
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
