// SPDX-License-Identifier: Apache-2.0

//! Chaos scenario: a producer has 10 in-flight publishes that the broker
//! has not yet acknowledged. The supervised driver loop trips into the
//! reconnect path (`Connection::reset` + new socket + re-handshake +
//! `rebuild_producers`). The contract:
//!
//! 1. Every in-flight publish must surface
//!    [`OpOutcome::SessionLost`](magnetar_proto::OpOutcome::SessionLost) so the caller's `SendFut`
//!    resolves with a typed signal that the publish was *not* observed by the broker. The caller is
//!    then free to retry (PIP-31 idempotent retry, at-most-once vs. at-least-once semantics belong
//!    to the user).
//! 2. The producer handle survives — the user does not need to recreate it. `rebuild_producers`
//!    reissues `CommandProducer` on the new session and returns the originating request id. The
//!    user's next `send()` lands on the rebuilt session.
//! 3. The session epoch is bumped by exactly one, so callers that snapshot the epoch before issuing
//!    an op can detect the reset on completion.
//!
//! Why this is moonpool territory: `testcontainers` cannot enumerate the
//! reset → rebuild sequence with N in-flight ops. The proto layer
//! exposes the hooks directly; this test pins the invariant that all N
//! pending publishes get a typed `SessionLost` outcome, not silently
//! dropped, on every supervised reset.

mod common;

use std::time::Instant;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{OpOutcome, PendingOpKey, SequenceId, pb};

use crate::common::{handshake_complete_shared, open_producer_ready};

const INFLIGHT_COUNT: u64 = 10;

#[test]
fn reset_surfaces_session_lost_for_every_inflight_publish() {
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
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
    }

    // === Supervised reset.
    {
        shared.inner.lock().reset();
    }

    // Every pending publish must now carry a SessionLost outcome keyed by
    // its sequence id. The pending-publish slab is emptied; the producer
    // handle survives.
    for seq in &seqs {
        let key = PendingOpKey::Send(handle, *seq);
        let outcome = shared.inner.lock().take_outcome(key);
        match outcome {
            Some(OpOutcome::SessionLost { key: returned_key }) => {
                assert_eq!(returned_key, key, "SessionLost must echo back the op key");
            }
            other => panic!("expected SessionLost for {seq:?}, got {other:?}"),
        }
    }

    // The producer's pending queue is empty (every in-flight was drained
    // by `reset`), but the handle itself is still registered with the
    // connection.
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.producer_pending_count(handle),
            0,
            "reset must drain every in-flight publish"
        );
    }

    // Session epoch bumped by exactly one.
    let epoch_after = shared.inner.lock().session_epoch();
    assert_eq!(
        epoch_after,
        epoch_before.wrapping_add(1),
        "reset must bump session_epoch exactly once"
    );

    // The producer survives — issuing rebuild_producers reissues
    // `CommandProducer` on the new session. The handle is still valid for
    // the next `send()`. This is the Stage 3 contract: user-facing
    // `Producer` handles do not need to be recreated across reconnects.
    {
        let mut conn = shared.inner.lock();
        // We need to re-handshake first; rebuild_producers operates on a
        // freshly-connected session. Walk a synthetic Connected through.
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
    }

    // The producer is once again usable for a fresh send. Sequence ids
    // resume from where the producer left off — the slab's
    // `last_sequence_id_pushed` is preserved across the reset, so a fresh
    // `send` allocates `INFLIGHT_COUNT` (the next available id).
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
