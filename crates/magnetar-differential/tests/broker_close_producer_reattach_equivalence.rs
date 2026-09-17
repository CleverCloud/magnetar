// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool parity for the broker-initiated
//! `CommandCloseProducer` → silent in-place re-attach path (issue #451
//! root-cause fix, ADR-0106).
//!
//! `pulsar-admin topics unload` of one partition makes the owning broker detach
//! the producer id and write `CommandCloseProducer` while the TCP connection
//! keeps serving. The sans-io [`magnetar_proto::Connection`] handles this by
//! re-attaching the producer IN PLACE (re-emit `CommandProducer` with a bumped
//! `epoch`, keep the send-drain gate shut until the fresh `ProducerSuccess`) and
//! — crucially — by **NOT** surfacing a `ProducerClosedByBroker` event, so the
//! re-attach is transparent to the runtime.
//!
//! That suppression is a concrete, user-observable change to the event stream,
//! so per GUIDELINES §Cross-runtime test and CLAUDE.md invariant 9 it needs a
//! `magnetar-differential` test asserting both engines react identically: same
//! suppressed close event, same fresh `CommandProducer` at epoch 1, same shut
//! gate, same deferred-then-flushed sends.
//!
//! Unlike the `CommandCloseConsumer` twin (issue #307, `assigned_broker_service_url`
//! splits the behaviour), the producer arm takes the SAME in-place path for
//! `Some(url)`: on an Extensible-Load-Manager cluster a plain unload attaches the
//! assigned lookup data, making `Some(url)` the default close shape, and Java
//! uses the URL only as a dial hint. Both cases are driven here.
//!
//! The refusal side (`run_refusal_both`) covers the two branches that must put
//! NOTHING on the wire: an unknown producer id, and a close arriving while the
//! producer's first open is still in flight.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, CreateProducerRequest, ProducerHandle,
    decode_one, encode_command, pb,
};

/// The observable reaction the two engines must agree on for one
/// `CommandCloseProducer`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reaction {
    /// `ProducerClosedByBroker` surfaced for this handle?
    saw_close_event: bool,
    /// A fresh `CommandProducer` was re-emitted on the same socket?
    reattached: bool,
    /// `CommandProducer.epoch` on the wire for that re-attach.
    epoch_on_wire: Option<u64>,
    /// Send-drain gate shut right after the close (before any re-attach ack)?
    gate_closed_after_close: bool,
    /// `CommandSend` frames emitted between the close and the re-attach ack
    /// (must be 0 — Pulsar closes the whole connection on a send to a producer
    /// that is not ready).
    send_frames_before_ack: usize,
    /// `CommandSend` frames emitted after the re-attach `ProducerSuccess`: the
    /// staged publish plus the replayed still-unacked pre-close publish.
    send_frames_after_ack: usize,
    /// Producer still open (a re-attach must not close it)?
    open_after: bool,
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

/// Broker-initiated `CommandCloseProducer` for `handle`. `url = None` is a
/// same-broker bundle unload; `Some(_)` is what an Extensible-Load-Manager
/// multi-phase unload puts on the wire.
fn close_producer_frame(handle: ProducerHandle, url: Option<String>) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseProducer as i32,
        close_producer: Some(pb::CommandCloseProducer {
            producer_id: handle.0,
            request_id: 0,
            assigned_broker_service_url: url,
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
            producer_name: "p-reattach-equiv".to_owned(),
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

fn outgoing(payload: &'static [u8]) -> magnetar_proto::producer::OutgoingMessage {
    magnetar_proto::producer::OutgoingMessage {
        payload: bytes::Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Drain the outbound buffer ONCE, bucketing `CommandProducer` `(request_id,
/// epoch)` pairs and counting `CommandSend` frames for `handle`
/// (`poll_transmit` empties the buffer, so classify in one pass).
fn drain_outbound(
    conn: &mut Connection,
    handle: ProducerHandle,
) -> (Vec<(u64, Option<u64>)>, usize) {
    let mut out = conn.poll_transmit();
    let (mut opens, mut sends) = (Vec::new(), 0_usize);
    while !out.is_empty() {
        let frame = decode_one(&mut out).expect("decode outbound");
        if frame.command.r#type == pb::base_command::Type::Producer as i32 {
            if let Some(p) = frame.command.producer {
                if p.producer_id == handle.0 {
                    opens.push((p.request_id, p.epoch));
                }
            }
        } else if frame.command.r#type == pb::base_command::Type::Send as i32 {
            if let Some(s) = frame.command.send {
                if s.producer_id == handle.0 {
                    sends += 1;
                }
            }
        }
    }
    (opens, sends)
}

/// Drain + report whether a `ProducerClosedByBroker` event surfaced for `handle`.
fn drain_close_event(conn: &mut Connection, handle: ProducerHandle) -> bool {
    let mut saw = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ProducerClosedByBroker { handle: h, .. } = ev {
            if h == handle {
                saw = true;
            }
        }
    }
    saw
}

/// Drive handshake + create-producer + ack + one publish over one engine's
/// locked `Connection`, then inject one broker close (`url`) and capture the
/// reaction.
fn lock_and_run(conn: &mut Connection, t0: Instant, url: Option<String>) -> Reaction {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    while conn.poll_event().is_some() {}

    let open_rid = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reattach-451-equiv".to_owned(),
        ..Default::default()
    });
    feed_producer_success(conn, open_rid, t0);
    while conn.poll_event().is_some() {}

    // One publish on the healthy attachment, drained off the wire but left
    // unacked — the broker never receipts it, so the re-attach must replay it.
    let _ = conn.send(handle, outgoing(b"before"), 0, t0).expect("send");
    let _ = drain_outbound(conn, handle);

    // The re-attach (if any) will allocate this request id.
    let reattach_rid = conn.peek_next_request_id_for_test();

    conn.handle_bytes(t0, &close_producer_frame(handle, url))
        .expect("handle close");
    let saw_close_event = drain_close_event(conn, handle);
    let gate_closed_after_close = !conn
        .producer(handle)
        .expect("producer slot")
        .state
        .lock()
        .broker_ready;

    // A publish staged behind the gate must not reach the wire.
    let _ = conn.send(handle, outgoing(b"staged"), 0, t0).expect("send");
    let (opens, send_frames_before_ack) = drain_outbound(conn, handle);
    let reattached = opens.iter().any(|(rid, _)| *rid == reattach_rid);
    let epoch_on_wire = opens
        .iter()
        .find(|(rid, _)| *rid == reattach_rid)
        .and_then(|(_, epoch)| *epoch);

    // Ack the re-attach (only meaningful when one was emitted; harmless
    // otherwise — an unmatched ProducerSuccess is ignored).
    feed_producer_success(conn, reattach_rid, t0);
    let _ = drain_close_event(conn, handle);
    let (_opens2, send_frames_after_ack) = drain_outbound(conn, handle);

    Reaction {
        saw_close_event,
        reattached,
        epoch_on_wire,
        gate_closed_after_close,
        send_frames_before_ack,
        send_frames_after_ack,
        open_after: !conn.producer_is_closed(handle),
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

/// The correct ADR-0106 behaviour on both engines: no close event, a fresh
/// re-attach at epoch 1, the gate shut until the ack, nothing on the wire
/// meanwhile, then both publishes flushed (the staged one plus the replayed
/// still-unacked pre-close publish), producer left open.
fn expected_in_place_reaction() -> Reaction {
    Reaction {
        saw_close_event: false,
        reattached: true,
        epoch_on_wire: Some(1),
        gate_closed_after_close: true,
        send_frames_before_ack: 0,
        send_frames_after_ack: 2,
        open_after: true,
    }
}

#[test]
fn same_broker_close_producer_reattach_event_streams_agree() {
    let (tokio_reaction, moonpool_reaction) = run_both(None);

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the same-broker close → in-place re-attach"
    );
    assert_eq!(
        tokio_reaction,
        expected_in_place_reaction(),
        "a same-broker close must silently re-attach at epoch 1 and defer-then-flush the \
         staged publishes, got {tokio_reaction:?}"
    );
}

#[test]
fn assigned_url_close_producer_reattach_event_streams_agree() {
    // Unlike the consumer twin, an `assigned_broker_service_url` does NOT divert
    // the producer to the supervised-reconnect path: no existing path owns
    // `ProducerClosedByBroker { Some(url) }` on a live socket, and an
    // Extensible-Load-Manager unload makes `Some(url)` the default close shape.
    let (tokio_reaction, moonpool_reaction) = run_both(Some("pulsar://new-broker:6650".to_owned()));

    assert_eq!(
        tokio_reaction, moonpool_reaction,
        "tokio and moonpool diverged on the assigned-url (url=Some) producer close"
    );
    assert_eq!(
        tokio_reaction,
        expected_in_place_reaction(),
        "an assigned-url close takes the SAME in-place path as url=None, got {tokio_reaction:?}"
    );
}

/// The refusal side of the in-place re-attach: a `CommandCloseProducer` whose
/// handle the connection must not re-attach.
///
/// `Connection::emit_in_place_producer_reattach` declines three ways. Two are
/// driven here and both must put NOTHING on the wire and leave the connection
/// serving: an unknown producer id has no `CommandProducer` to replay at all,
/// and a producer whose first open is still in flight has one pending whose
/// `ProducerSuccess` would otherwise be raced by a second, unmatched open. The
/// third (a user-closed producer) is covered by the proto unit.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Refusal {
    /// A fresh `CommandProducer` was emitted for the handle?
    reattached: bool,
    /// `ProducerClosedByBroker` surfaced for the handle?
    saw_close_event: bool,
    /// Connection still serving (a refusal is not a protocol error)?
    connected_after: bool,
}

/// Feed a close for a producer id that was never created.
fn run_unknown_handle(conn: &mut Connection, t0: Instant) -> Refusal {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    while conn.poll_event().is_some() {}

    let ghost = ProducerHandle(4242);
    let next_request_id = conn.peek_next_request_id_for_test();
    conn.handle_bytes(t0, &close_producer_frame(ghost, None))
        .expect("handle close for an unknown producer");
    let saw_close_event = drain_close_event(conn, ghost);
    let (opens, _sends) = drain_outbound(conn, ghost);
    Refusal {
        reattached: opens.iter().any(|(rid, _)| *rid == next_request_id),
        saw_close_event,
        connected_after: conn.is_connected(),
    }
}

/// Feed a close for a producer whose FIRST open is still in flight.
fn run_open_in_flight(conn: &mut Connection, t0: Instant) -> Refusal {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    while conn.poll_event().is_some() {}

    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reattach-451-equiv-in-flight".to_owned(),
        ..Default::default()
    });
    let _ = drain_outbound(conn, handle);

    // No `ProducerSuccess` fed: the open is pending and its parked waiter owns
    // the outcome.
    let next_request_id = conn.peek_next_request_id_for_test();
    conn.handle_bytes(t0, &close_producer_frame(handle, None))
        .expect("handle close mid-open");
    let saw_close_event = drain_close_event(conn, handle);
    let (opens, _sends) = drain_outbound(conn, handle);
    Refusal {
        reattached: opens.iter().any(|(rid, _)| *rid == next_request_id),
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
fn same_broker_close_for_an_unknown_producer_is_refused_identically() {
    let (tokio_refusal, moonpool_refusal) = run_refusal_both(run_unknown_handle);
    assert_eq!(
        tokio_refusal, moonpool_refusal,
        "tokio and moonpool diverged on a close for an unknown producer",
    );
    assert_eq!(
        tokio_refusal,
        Refusal {
            reattached: false,
            saw_close_event: false,
            connected_after: true,
        },
        "a close naming a producer we never created has nothing to replay and no waiter that \
         could ever read an event: no `CommandProducer`, no surfaced event, connection keeps \
         serving",
    );
}

#[test]
fn same_broker_close_during_a_pending_producer_open_is_refused_identically() {
    let (tokio_refusal, moonpool_refusal) = run_refusal_both(run_open_in_flight);
    assert_eq!(
        tokio_refusal, moonpool_refusal,
        "tokio and moonpool diverged on a close during a pending producer open",
    );
    assert_eq!(
        tokio_refusal,
        Refusal {
            reattached: false,
            saw_close_event: true,
            connected_after: true,
        },
        "the open already in flight owns the next `ProducerSuccess`; a second `CommandProducer` \
         would leave one of the two unmatched — and its parked waiter is the one reader that \
         can consume the close event",
    );
}
