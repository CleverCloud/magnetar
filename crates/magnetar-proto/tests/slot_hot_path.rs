// SPDX-License-Identifier: Apache-2.0

//! ADR-0038 (split-connection-mutex) — per-slot hot-path unit tests.
//!
//! These tests exercise the Phase-3 contract: `ProducerSlot::queue_send`
//! must:
//!
//! 1. Mutate per-producer state (sequence-id allocation, pending queue, outbound staging) without
//!    needing a `&mut Connection` reference.
//! 2. Preserve the invariant that the per-slot mutex is the ONLY lock acquired on the hot path (no
//!    global Connection access).
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use magnetar_proto::producer::{OutgoingMessage, ProducerSlot, ProducerState};
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{Connection, ConnectionConfig, CreateProducerRequest, ProducerIdentity, pb};

/// Construct a `Connection` whose state machine has cleared the
/// handshake so `create_producer` runs cleanly.
fn handshake_complete(now: Instant) -> Connection {
    let mut conn = Connection::new(ConnectionConfig::default());
    conn.begin_handshake().expect("begin_handshake");
    let connected = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &connected).expect("encode connected");
    conn.handle_bytes(now, &buf).expect("apply connected");
    // Drain the bytes the connection produced during handshake.
    let _ = conn.poll_transmit();
    conn
}

fn outgoing_message(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// `ProducerSlot::queue_send` advances the per-producer sequence-id
/// counter and stages a frame on the slot's outbound queue without ever
/// needing a `Connection` borrow. Direct callers can drive the producer
/// state machine purely through the slot — exactly the contract the
/// runtime crates rely on for the Phase-3 hot-path bypass.
#[test]
fn producer_slot_queue_send_advances_sequence_without_connection() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/slot-hotpath".to_owned(),
        ..Default::default()
    });
    let slot: Arc<ProducerSlot> = conn.producer(handle).expect("slot exists").clone();
    // Drop the Connection borrow — the slot must stand on its own.
    drop(conn);

    // First send: sequence id 0.
    let seq0 = slot
        .queue_send(outgoing_message(b"first"), 1_700_000_000_000, now)
        .expect("first send accepted");
    assert_eq!(seq0.0, 0, "first publish on a fresh producer is seq-id 0");

    // Second send: sequence id 1.
    let seq1 = slot
        .queue_send(outgoing_message(b"second"), 1_700_000_000_001, now)
        .expect("second send accepted");
    assert_eq!(seq1.0, 1, "second publish increments seq-id");

    // The per-slot state reflects two pending sends; no outbound frame is
    // visible at the Connection level until `drain_producer_outbound` is
    // invoked (which is the driver's job).
    let state = slot.state.lock();
    assert_eq!(state.pending.len(), 2, "both sends are pending");
    assert_eq!(state.last_sequence_id_pushed, 1);
}

/// `Connection::drain_producer_outbound` merges every per-slot staged
/// frame into the connection's outbound byte buffer. Producer hot-path
/// sends followed by `poll_transmit` (which calls
/// `drain_producer_outbound` as its first step) must surface bytes on
/// the wire — proving the Phase-3 producer → driver handoff is correct
/// end-to-end.
///
/// We also confirm that the frame lives ONLY on the slot's outbound
/// queue until the drain runs: that is the contract the runtime hot
/// path relies on (no global lock contention on `queue_send`).
#[test]
fn drain_producer_outbound_lifts_per_slot_frames_to_connection_buffer() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/drain".to_owned(),
        ..Default::default()
    });
    // Drain the CommandProducer bytes from the open so we only see the
    // SEND we're about to enqueue.
    let _ = conn.poll_transmit();

    let slot: Arc<ProducerSlot> = conn.producer(handle).expect("slot exists").clone();
    // Hot path: queue without `&mut conn`.
    let _seq = slot
        .queue_send(outgoing_message(b"hello"), 1_700_000_000_000, now)
        .expect("queue ok");

    // Pending count on the slot reflects the freshly-queued send.
    assert_eq!(
        slot.state.lock().pending.len(),
        1,
        "send is pending on the slot"
    );
    // The connection's outbound byte buffer is empty until the driver
    // tick runs the per-slot drain (poll_transmit's first step).
    assert_eq!(
        conn.outbound_len(),
        0,
        "frame has not yet been merged into the connection buffer"
    );

    // Driver tick: poll_transmit calls drain_producer_outbound, then
    // hands us the merged byte buffer.
    let after_drain = conn.poll_transmit();
    assert!(
        !after_drain.is_empty(),
        "poll_transmit (which drains per-slot outbound) returns the encoded frame"
    );
}

/// Lock-ordering smoke: holding `slot.state.lock()` while
/// `Connection::drain_producer_outbound` is invoked from another stack
/// frame would deadlock because the driver takes the per-slot mutex
/// from inside the global Connection borrow. This test pins the rule
/// by exercising the safe order (global -> per-slot) and verifying it
/// completes without `try_lock` contention.
///
/// The test does not actually deadlock-trigger via threads (would
/// require timing), but it asserts the invariant in code form: take
/// the global Connection mutation API first (`drain_producer_outbound`,
/// which takes `&mut Connection`), then take per-slot mutation
/// (`queue_send`), and confirm both succeed.
#[test]
fn lock_ordering_global_then_per_slot_does_not_deadlock() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/lock-order".to_owned(),
        ..Default::default()
    });
    let slot = conn.producer(handle).expect("slot exists").clone();
    // First take the global path.
    conn.drain_producer_outbound();
    // Then take the per-slot path. If we held both at once in the wrong
    // order, this would deadlock; doing them sequentially is the
    // ADR-0038 contract.
    let _ = slot
        .queue_send(outgoing_message(b"x"), 1_700_000_000_000, now)
        .expect("queue ok after global drain");
    assert_eq!(slot.state.lock().pending.len(), 1);
}

/// `ProducerSlot::new` honours the identity supplied at construction —
/// the runtime layer relies on this for the Phase-2 cold-path reads
/// (`Producer::topic`, `access_mode`) to be lock-free.
#[test]
fn producer_slot_identity_is_immutable() {
    let now = Instant::now();
    let mut conn = handshake_complete(now);
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/identity".to_owned(),
        access_mode: pb::ProducerAccessMode::Exclusive,
        ..Default::default()
    });
    let slot = conn.producer(handle).expect("slot exists").clone();
    assert_eq!(slot.identity.handle, handle);
    assert_eq!(slot.identity.topic, "persistent://public/default/identity");
    assert_eq!(slot.identity.access_mode, pb::ProducerAccessMode::Exclusive);
    // Mutating state must not bleed into identity.
    slot.state.lock().name = Some("broker-assigned".to_owned());
    assert_eq!(slot.identity.handle, handle, "identity is frozen");
}

/// Constructing a slot via the public API and feeding it sends drives
/// the per-producer state machine exactly the same way
/// `Connection::send` does — proves the slot can be freely shared with
/// per-handle hot-path code without needing a Connection reference.
#[test]
fn producer_slot_new_then_queue_send_round_trips() {
    let identity = ProducerIdentity {
        handle: magnetar_proto::ProducerHandle(42),
        topic: "persistent://public/default/standalone".to_owned(),
        access_mode: pb::ProducerAccessMode::Shared,
    };
    let state = ProducerState::new(
        identity.handle,
        identity.topic.clone(),
        CompressionKind::None,
        5 * 1024 * 1024,
    );
    let slot = ProducerSlot::new(identity, state);
    let now = Instant::now();
    let seq = slot
        .queue_send(outgoing_message(b"standalone"), 1_700_000_000_000, now)
        .expect("standalone slot accepts send");
    assert_eq!(seq.0, 0);
    let state = slot.state.lock();
    assert_eq!(state.pending.len(), 1);
}
