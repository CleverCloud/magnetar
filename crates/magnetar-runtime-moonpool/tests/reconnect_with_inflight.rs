// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: a producer has 10 in-flight publishes that the broker
//! has not yet acknowledged. The supervised driver loop trips into the
//! reconnect path (`Connection::reset` + new socket + re-handshake +
//! `rebuild_producers`). The contract — Stage 3 transparent replay
//! (mirrors Java `ProducerImpl#resendMessages`):
//!
//! 1. `Connection::reset` does **not** install
//!    [`OpOutcome::SessionLost`](magnetar_proto::OpOutcome::SessionLost) on the publish key. The
//!    user-facing `SendFut` polls, finds no outcome, re-registers its waker, and stays pending
//!    across the reconnect.
//! 2. The in-flight publishes are snapshotted on the connection
//!    ([`magnetar_proto::Connection::in_flight_publish_snapshot_len`]) and `rebuild_producers`
//!    replays them onto the new session in original FIFO order with their original sequence ids.
//! 3. The producer handle survives — the user does not need to recreate it. Future `send()` calls
//!    allocate from the post-replay `last_sequence_id_pushed` so monotonicity is preserved.
//! 4. The session epoch is bumped by exactly one, so callers that snapshot the epoch before issuing
//!    an op can still detect the reset on completion.
//!
//! Why this is moonpool territory: `testcontainers` cannot enumerate the
//! reset → rebuild sequence with N in-flight ops. The proto layer
//! exposes the hooks directly; this test pins the invariant that the
//! N pending publishes survive the reset transparently, with no
//! `SessionLost` outcome ever surfacing to the caller.

mod common;

use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{OpOutcome, PendingOpKey, SequenceId, encode_command, pb};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::{handshake_complete_shared, open_producer_ready, send_receipt_bytes};

const INFLIGHT_COUNT: u64 = 10;

/// Feed the broker's `CommandProducerSuccess` for `request_id` — the ack
/// that opens the producer-not-ready drain gate and triggers the snapshot
/// replay (Java `handleProducerSuccess` parity). Every rebuild/retry leg in
/// these tests needs this step before replayed SEND frames may reach the
/// wire.
fn ack_producer_open(shared: &Arc<ConnectionShared>, request_id: u64, at: Instant) {
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "magnetar-test-reattach".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
}

// End-to-end snapshot-and-replay scenario. Linear by design — wire-trace
// readability beats artificial sharding. Silence the per-function line cap.
#[allow(clippy::too_many_lines)]
#[test]
fn reset_snapshots_inflight_publishes_for_transparent_replay() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/inflight", t0);

    // Enqueue 10 sends. No CommandSendReceipt arrives — they all sit in
    // the pending-publish slab.
    let mut seqs: Vec<SequenceId> = Vec::with_capacity(INFLIGHT_COUNT as usize);
    {
        let mut conn = shared.inner.lock();
        for i in 0..INFLIGHT_COUNT {
            let payload = Bytes::from(format!("in-flight-{i}"));
            let len = payload.len() as u32;
            let seq = conn
                .send(
                    handle,
                    OutgoingMessage {
                        payload,
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: len,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    t0,
                )
                .expect("queue send");
            seqs.push(seq);
        }
    }
    assert_eq!(seqs.len(), INFLIGHT_COUNT as usize);
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        INFLIGHT_COUNT as usize,
    );

    // Snapshot the session epoch so we can confirm `reset` bumps it.
    let epoch_before = shared.inner.lock().session_epoch();

    // Drain outbound bytes so the test isolates the post-reset wire
    // activity from the pre-reset CommandSend frames.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    // === Supervised reset.
    {
        shared.inner.lock().reset();
    }

    // Stage 3 transparent replay: NO SessionLost outcome lands on the
    // publish key. The user-facing SendFut would re-poll, find the slot
    // empty, re-register its waker, and stay pending until the replayed
    // receipt arrives on the new session.
    for seq in &seqs {
        let key = PendingOpKey::Send(handle, *seq);
        let outcome = shared.inner.lock().take_outcome(key);
        assert!(
            outcome.is_none(),
            "transparent replay must not install SessionLost on the publish key (got {outcome:?})"
        );
    }

    // The producer's pending queue is empty (every in-flight was drained
    // into the snapshot), and the snapshot now holds all N publishes
    // ready for `rebuild_producers` to re-issue.
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.producer_pending_count(handle),
            0,
            "reset must drain every in-flight publish into the snapshot"
        );
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            INFLIGHT_COUNT as usize,
            "the snapshot must preserve every in-flight publish until rebuild consumes it"
        );
    }

    // Session epoch bumped by exactly one.
    let epoch_after = shared.inner.lock().session_epoch();
    assert_eq!(
        epoch_after,
        epoch_before.wrapping_add(1),
        "reset must bump session_epoch exactly once"
    );

    // Walk through a synthetic re-handshake and rebuild.
    let rebuild_rid = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        let frame = common::handshake_response_bytes();
        conn.handle_bytes(t0, &frame).expect("Connected on retry");
        let _ = conn.poll_event();
        let rebuilt = conn.rebuild_producers();
        assert_eq!(
            rebuilt.len(),
            1,
            "the surviving producer must be rebuilt on the new session"
        );
        rebuilt[0]
    };

    // Producer-not-ready gate: until the broker acks the re-attachment, the
    // snapshots stay parked and no SEND may reach the wire — only the
    // rebuild's CommandProducer goes out.
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            INFLIGHT_COUNT as usize,
            "snapshots stay parked until the broker acks the re-attachment"
        );
        assert_eq!(conn.producer_pending_count(handle), 0);
    }
    ack_producer_open(&shared, rebuild_rid.0, t0);

    // Post-ack: the snapshot is drained into the producer's `pending` queue, and
    // the wire-frame queue has been re-emitted into the connection's outbound buffer.
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            0,
            "the re-attach ack consumes the snapshot"
        );
        assert_eq!(
            conn.producer_pending_count(handle),
            INFLIGHT_COUNT as usize,
            "the ack reinstalls every snapshotted OpSend into pending"
        );
    }

    // A fresh post-reset `send` continues monotonically from where the producer
    // left off (the replay does not bump `last_sequence_id_pushed`).
    let next_seq = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"after-reset"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 11,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send after reset")
    };
    assert_eq!(
        next_seq,
        SequenceId(INFLIGHT_COUNT),
        "sequence ids must continue monotonically across the reset"
    );
}

/// Replayed publishes still resolve their user-facing futures when the broker's
/// `CommandSendReceipt` arrives on the new session. This is the second half of the
/// transparent-replay contract: not only must the publish data survive the reconnect,
/// the eventual receipt must flow through `apply_receipt` and land an
/// `OpOutcome::SendReceipt` on the connection's outcome slab keyed by the original
/// `(producer, sequence_id)` — i.e. the user's `SendFut` resolves exactly as if the
/// original session had simply lasted longer. Mirrors Java
/// `ProducerImpl#ackReceived` against a `resendMessages`-replayed `OpSendMsg`.
#[test]
fn replayed_send_resolves_when_receipt_arrives_on_new_session() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/replay-ok", t0);

    // Single in-flight publish, no receipt yet.
    let seq = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"survive-me"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 10,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send")
    };

    // Drain the pre-reset wire frame.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    // Supervised reset: the snapshot is taken, no outcome lands on the publish key.
    shared.inner.lock().reset();
    let key = PendingOpKey::Send(handle, seq);
    assert!(
        shared.inner.lock().take_outcome(key).is_none(),
        "transparent replay: no SessionLost outcome installed"
    );

    // Re-handshake + rebuild on the new session.
    let rebuild_rids = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &common::handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        conn.rebuild_producers()
    };
    // The replay materialises on the broker's re-attach ack
    // (producer-not-ready gate).
    ack_producer_open(&shared, rebuild_rids[0].0, t0);

    // Drain the post-rebuild wire frames so the publish is "on the wire" of the new
    // session.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    // Feed the broker's CommandSendReceipt — the replayed OpSend resolves.
    {
        let mut conn = shared.inner.lock();
        let receipt = send_receipt_bytes(handle, seq, 99, seq.0);
        conn.handle_bytes(t0, &receipt).expect("apply receipt");
    }

    // The outcome lands keyed by the original (producer, sequence_id), and the
    // producer's pending queue is now empty.
    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SendReceipt {
            sequence_id,
            message_id,
        }) => {
            assert_eq!(sequence_id, seq);
            assert_eq!(message_id.ledger_id, 99);
            assert_eq!(message_id.entry_id, seq.0);
        }
        other => panic!("expected SendReceipt for replayed send, got {other:?}"),
    }
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        0,
        "the replayed OpSend drains on receipt"
    );
}

/// FIFO ordering invariant — three publishes in a row, reset mid-flight, rebuild
/// must replay them onto the new session in original order with their original
/// sequence ids. The Java client documents (and the tests in
/// `ProducerImplTest#testRecreateProducerOnReconnect` assert) that publish ordering is
/// preserved across the reconnect; this test mirrors that guarantee on the moonpool
/// engine surface.
#[test]
fn replay_preserves_fifo_ordering_across_rebuild() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/replay-fifo", t0);

    let payloads: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut seqs: Vec<SequenceId> = Vec::with_capacity(3);
    {
        let mut conn = shared.inner.lock();
        for p in &payloads {
            let seq = conn
                .send(
                    handle,
                    OutgoingMessage {
                        payload: Bytes::from(p.to_vec()),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: p.len() as u32,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    t0,
                )
                .expect("queue");
            seqs.push(seq);
        }
        // Drain pre-reset wire frames so the post-rebuild drain is isolated.
        let _ = conn.poll_transmit();
    }

    // Reset + rebuild.
    shared.inner.lock().reset();
    let rebuild_rids = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &common::handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        conn.rebuild_producers()
    };
    // The replay materialises on the broker's re-attach ack
    // (producer-not-ready gate).
    ack_producer_open(&shared, rebuild_rids[0].0, t0);

    // Drain post-rebuild wire frames and inspect the CommandSend ordering.
    let mut cursor = {
        let mut conn = shared.inner.lock();
        conn.poll_transmit()
    };
    let mut send_seqs: Vec<u64> = Vec::new();
    let mut send_payloads: Vec<Vec<u8>> = Vec::new();
    while !cursor.is_empty() {
        let frame = magnetar_proto::frame::decode_one(&mut cursor).expect("decode frame");
        if frame.command.r#type == pb::base_command::Type::Send as i32 {
            if let Some(s) = frame.command.send.as_ref() {
                send_seqs.push(s.sequence_id);
            }
            if let Some(body) = frame.payload.as_ref() {
                send_payloads.push(body.body.to_vec());
            }
        }
    }
    assert_eq!(
        send_seqs,
        seqs.iter().map(|s| s.0).collect::<Vec<u64>>(),
        "rebuild must replay the OpSends in their original sequence-id order"
    );
    let expected_payloads: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
    assert_eq!(
        send_payloads, expected_payloads,
        "rebuild must replay the OpSends in their original payload order"
    );
}

#[test]
fn reset_fails_flushed_batch_instead_of_orphaning_sends() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(magnetar_proto::CreateProducerRequest {
            topic: "persistent://public/default/batch-reset".to_owned(),
            enable_batching: true,
            max_batch_size_bytes: 4096,
            max_messages_in_batch: 100,
            ..Default::default()
        });
        let _ = conn.poll_transmit();
        (handle, request_id)
    };
    ack_producer_open(&shared, request_id, t0);

    let sequences = {
        let mut conn = shared.inner.lock();
        let mut sequences = Vec::new();
        for payload in [b"a".as_slice(), b"b".as_slice()] {
            sequences.push(
                conn.send(
                    handle,
                    OutgoingMessage {
                        payload: Bytes::copy_from_slice(payload),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: 1,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    t0,
                )
                .expect("queue batch member"),
            );
        }
        assert_eq!(conn.flush_producer(handle, 0, t0), 1);
        let _ = conn.poll_transmit();
        sequences
    };

    shared.inner.lock().reset();
    let mut conn = shared.inner.lock();
    for sequence_id in sequences {
        match conn.take_outcome(PendingOpKey::Send(handle, sequence_id)) {
            Some(OpOutcome::SendError { code, message, .. }) => {
                assert_eq!(code, -1);
                assert_eq!(
                    message,
                    "batched send cannot be replayed after connection reset"
                );
            }
            other => panic!("expected terminal batch reset error, got {other:?}"),
        }
    }
}

/// `session_epoch` monotonicity across a double reset → rebuild cycle. Mirrors Java
/// `ClientCnx#getEpoch` which is bumped exactly once per `connectionClosed`. The
/// transparent-replay path must not double-bump or skip the counter, since callers
/// snapshot the epoch before issuing an op and compare on completion.
#[test]
fn session_epoch_bumps_exactly_once_per_reset_in_replay_cycle() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/replay-epoch", t0);

    // One publish, two reset+rebuild cycles, expect epoch == 2 at the end.
    {
        let mut conn = shared.inner.lock();
        let _ = conn
            .send(
                handle,
                OutgoingMessage {
                    payload: Bytes::from_static(b"epoch-test"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 10,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue");
        let _ = conn.poll_transmit();
    }
    let epoch_before = shared.inner.lock().session_epoch();

    for _ in 0..2 {
        shared.inner.lock().reset();
        let rebuild_rids = {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("re-handshake");
            conn.handle_bytes(t0, &common::handshake_response_bytes())
                .expect("Connected on retry");
            let _ = conn.poll_event();
            conn.rebuild_producers()
        };
        // Ack each cycle's re-attachment (producer-not-ready gate) so the
        // snapshot drains back into `pending` before the next reset
        // re-snapshots it.
        ack_producer_open(&shared, rebuild_rids[0].0, t0);
        // Drain the wire frames so the next reset's snapshot is the only OpSend in flight.
        let _ = shared.inner.lock().poll_transmit();
    }

    let epoch_after = shared.inner.lock().session_epoch();
    assert_eq!(
        epoch_after,
        epoch_before.wrapping_add(2),
        "session_epoch must bump by exactly 1 per reset across two reset+rebuild cycles"
    );
    // The OpSend survives both cycles — it's still in the producer's pending queue,
    // ready to resolve when the broker's receipt finally arrives.
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        1,
        "the OpSend survives both reset+rebuild cycles"
    );
    assert_eq!(
        shared.inner.lock().in_flight_publish_snapshot_len(handle),
        0,
        "the snapshot bucket is empty after both acked rebuild cycles"
    );
}

/// Issue #369: a publish relocated into `in_flight_publish_snapshots` by
/// `reset()` still surfaces its configured `send_timeout` at exactly the
/// original virtual deadline — it must not hang for the whole reconnect
/// budget, and it must not fire early either. Mirrors
/// `virtual_clock_send_timeout.rs::send_timeout_fires_at_virtual_deadline`'s
/// deterministic-virtual-clock shape, with a `reset()` inserted between
/// enqueue and the deadline ticks so the timeout must be enforced from the
/// snapshot bucket rather than the live pending queue.
#[test]
fn send_timeout_fires_at_virtual_deadline_for_publish_relocated_across_reset() {
    // `open_producer_ready` opens with `CreateProducerRequest::default()`,
    // which carries the Java-parity default `send_timeout = Some(30s)`
    // (ADR-0072) — match it here so the deadline math below is exact.
    const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(
        &shared,
        "persistent://public/default/issue-369-virtual-clock",
        t0,
    );

    {
        let mut conn = shared.inner.lock();
        let seq = conn
            .send(
                handle,
                OutgoingMessage {
                    payload: Bytes::from_static(b"relocated-virtual-clock"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 24,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                t0,
            )
            .expect("queue send");
        drop(conn);

        // Relocate: reset() moves the op out of `pending` into the snapshot
        // bucket. No new session is built — the point is to exercise the
        // relocated-send timeout sweep in isolation, exactly like the sibling
        // proto-level unit tests in `magnetar_proto::conn::conn_state_tests`.
        shared.inner.lock().reset();
        assert_eq!(
            shared.inner.lock().in_flight_publish_snapshot_len(handle),
            1,
            "the relocated send must land in the snapshot bucket"
        );
        assert_eq!(shared.inner.lock().producer_pending_count(handle), 0);

        let key = PendingOpKey::Send(handle, seq);
        assert!(
            shared.inner.lock().take_outcome(key).is_none(),
            "reset installs no outcome on the Send key — transparent replay contract"
        );

        // Just BEFORE the deadline (measured from the ORIGINAL t0, not from
        // the reset): still pending, snapshot survives.
        let t_before = t0 + SEND_TIMEOUT.saturating_sub(std::time::Duration::from_millis(100));
        shared.inner.lock().handle_timeout(t_before);
        assert!(
            shared.inner.lock().take_outcome(key).is_none(),
            "no send-timeout outcome before the deadline for a relocated send"
        );
        assert_eq!(
            shared.inner.lock().in_flight_publish_snapshot_len(handle),
            1,
            "the snapshot must survive an early handle_timeout tick"
        );

        // Just AFTER the deadline: the sweep resolves the send with the
        // configured timeout error and drains the snapshot.
        let t_after = t0 + SEND_TIMEOUT + std::time::Duration::from_millis(100);
        shared.inner.lock().handle_timeout(t_after);
        match shared.inner.lock().take_outcome(key) {
            Some(OpOutcome::SendError {
                sequence_id,
                code,
                message,
            }) => {
                assert_eq!(sequence_id, seq);
                assert_eq!(code, -1, "send-timeout SendError uses the -1 sentinel");
                assert_eq!(message, "send timeout");
            }
            other => panic!("expected a send-timeout SendError, got {other:?}"),
        }
        assert_eq!(
            shared.inner.lock().in_flight_publish_snapshot_len(handle),
            0,
            "the timed-out relocated send must drain out of the snapshot bucket"
        );
    }
}
