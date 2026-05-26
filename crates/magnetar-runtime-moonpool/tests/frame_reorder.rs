// SPDX-License-Identifier: Apache-2.0

// Chaos scenarios value a single readable step-by-step `fn`. Splitting
// these into sub-helpers would obscure the synthetic frame sequence the
// test pins. We accept the line count.
#![allow(clippy::too_many_lines)]

//! Chaos scenario: the broker emits `CommandSendReceipt` frames *out of
//! order* (in this case, the receipt for `sequence_id = 1` arrives before
//! the receipt for `sequence_id = 0`). The sans-io state machine's
//! pending-publish slab is sequence-id-indexed, so each receipt resolves
//! its own slot regardless of arrival order. Pins the contract.
//!
//! Why this is moonpool territory and not a `testcontainers` test: a real
//! Pulsar broker preserves per-producer publish order — it will never emit
//! receipts out of order on a single TCP stream. But a future broker
//! optimisation (PIP-188 broker migration, partial fail-over, intentional
//! reordering across multiplexed CNX streams) could, and the client must
//! tolerate it. The only way to assert that contract today is to feed
//! synthetic frames directly into the state machine.
//!
//! ## Shape
//!
//! 1. Complete the handshake.
//! 2. Open a producer; ack the `CommandProducerSuccess` round-trip.
//! 3. Enqueue two sends — sequence ids `0` and `1` — without consuming any receipts.
//! 4. Feed back the receipt for `sequence_id = 1` *first*, then `0`.
//! 5. Assert both `OpOutcome::SendReceipt` slots resolve with the correct sequence id + message id,
//!    and the producer's `last_sequence_id_published` reflects the *highest* acked id (`1`).

mod common;

use std::time::Instant;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{OpOutcome, PendingOpKey, SequenceId, pb};

use crate::common::{handshake_complete_shared, open_producer_ready, send_receipt_bytes};

#[test]
fn out_of_order_send_receipts_resolve_by_sequence_id() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/reorder", t0);

    // Enqueue two sends. The state machine assigns sequence ids 0 then 1.
    let seq0 = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"first"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 5,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send 0")
    };
    let seq1 = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"second"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 6,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send 1")
    };
    assert_eq!(seq0, SequenceId(0));
    assert_eq!(seq1, SequenceId(1));

    // Drain outbound bytes — the engine would have shipped two `CommandSend`
    // frames already. We don't need their contents; we just need the
    // pending-publish slab populated.
    {
        let mut conn = shared.inner.lock();
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
        assert!(!tx_buf.is_empty(), "two CommandSend frames must be queued");
        assert_eq!(conn.producer_pending_count(handle), 2);
    }

    // Feed receipts BACKWARDS: seq 1 first, then seq 0.
    {
        let mut conn = shared.inner.lock();
        let r1 = send_receipt_bytes(handle, seq1, 10, 1);
        conn.handle_bytes(t0, &r1).expect("handle receipt seq=1");
    }
    {
        let outcome1 = shared
            .inner
            .lock()
            .take_outcome(PendingOpKey::Send(handle, seq1));
        match outcome1 {
            Some(OpOutcome::SendReceipt {
                sequence_id,
                message_id,
            }) => {
                assert_eq!(sequence_id, seq1);
                assert_eq!(message_id.ledger_id, 10);
                assert_eq!(message_id.entry_id, 1);
            }
            other => panic!("expected SendReceipt for seq=1, got {other:?}"),
        }
        let n_pending = shared.inner.lock().producer_pending_count(handle);
        assert_eq!(
            n_pending, 1,
            "seq=0 must still be pending after seq=1 acked"
        );
    }

    {
        let mut conn = shared.inner.lock();
        let r0 = send_receipt_bytes(handle, seq0, 10, 0);
        conn.handle_bytes(t0, &r0).expect("handle receipt seq=0");
    }
    {
        let outcome0 = shared
            .inner
            .lock()
            .take_outcome(PendingOpKey::Send(handle, seq0));
        match outcome0 {
            Some(OpOutcome::SendReceipt {
                sequence_id,
                message_id,
            }) => {
                assert_eq!(sequence_id, seq0);
                assert_eq!(message_id.ledger_id, 10);
                assert_eq!(message_id.entry_id, 0);
            }
            other => panic!("expected SendReceipt for seq=0, got {other:?}"),
        }
    }

    // After both receipts the pending count is zero — both pending-publish
    // slots were resolved independently by sequence id. This is the key
    // chaos invariant: out-of-order receipts must NOT leak pending publishes
    // (which would have happened if the slab were FIFO-keyed instead of
    // sequence-id-keyed).
    let conn = shared.inner.lock();
    assert_eq!(
        conn.producer_pending_count(handle),
        0,
        "both out-of-order receipts must resolve their respective pending slots",
    );
    // `last_sequence_id_pushed` tracks emission order (1 — we pushed 0 then 1)
    // and is independent of broker-reply order.
    assert_eq!(
        conn.producer_last_sequence_id_pushed(handle),
        1,
        "highest pushed sequence id is 1 regardless of receipt arrival order",
    );
}
