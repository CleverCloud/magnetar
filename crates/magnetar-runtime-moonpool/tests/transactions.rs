// SPDX-License-Identifier: Apache-2.0

//! PIP-31 transactions — moonpool engine.
//!
//! Drives `Connection::{new_txn, add_partition_to_txn,
//! add_subscription_to_txn, end_txn}` through a synthetic broker that
//! feeds back the matching `*_response` frames. The driver loop is not
//! involved; tests poke `ConnectionShared::inner.lock()` directly.
//! Mirrors the differential broker's txn dispatch without any I/O.

#![allow(clippy::expect_used)]

mod common;

use std::time::{Duration, Instant};

use common::{
    add_partition_to_txn_response_bytes, add_subscription_to_txn_response_bytes,
    end_txn_response_bytes, handshake_complete_shared, new_txn_response_bytes,
};
use magnetar_proto::{OpOutcome, PendingOpKey, TxnAction, TxnId, TxnState};

/// `Connection::new_txn` round-trips a `TxnId` through the proto layer.
#[tokio::test(flavor = "current_thread")]
async fn new_txn_returns_broker_assigned_txn_id() {
    let at = Instant::now();
    let shared = handshake_complete_shared(at);

    let request_id = {
        let mut conn = shared.inner.lock();
        conn.new_txn(Duration::from_secs(60))
    };
    // Synthesize the broker's CommandNewTxnResponse and feed it back.
    let frame = new_txn_response_bytes(request_id.0, 0x11, 0x22);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame).expect("apply NewTxnResponse");
    }
    let outcome = {
        let mut conn = shared.inner.lock();
        conn.take_outcome(PendingOpKey::Request(request_id))
            .expect("outcome ready")
    };
    match outcome {
        OpOutcome::NewTxn {
            request_id: rid,
            result,
            ..
        } => {
            assert_eq!(rid, request_id);
            let id = result.expect("ok new_txn");
            assert_eq!(
                id,
                TxnId {
                    most_sig_bits: 0x11,
                    least_sig_bits: 0x22
                }
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

/// `Connection::add_partition_to_txn` round-trips an Ok response.
#[tokio::test(flavor = "current_thread")]
async fn add_partition_to_txn_returns_ok() {
    let at = Instant::now();
    let shared = handshake_complete_shared(at);
    let txn = TxnId {
        most_sig_bits: 0x33,
        least_sig_bits: 0x44,
    };

    let request_id = {
        let mut conn = shared.inner.lock();
        conn.add_partition_to_txn(txn, "persistent://public/default/t".to_owned())
    };
    let frame = add_partition_to_txn_response_bytes(request_id.0);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame)
            .expect("apply AddPartitionToTxnResponse");
    }
    let outcome = {
        let mut conn = shared.inner.lock();
        conn.take_outcome(PendingOpKey::Request(request_id))
            .expect("outcome ready")
    };
    match outcome {
        OpOutcome::AddPartitionToTxn { result, .. } => result.expect("ok add_partition"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

/// `Connection::add_subscription_to_txn` round-trips an Ok response.
#[tokio::test(flavor = "current_thread")]
async fn add_subscription_to_txn_returns_ok() {
    let at = Instant::now();
    let shared = handshake_complete_shared(at);
    let txn = TxnId {
        most_sig_bits: 0x55,
        least_sig_bits: 0x66,
    };

    let request_id = {
        let mut conn = shared.inner.lock();
        conn.add_subscription_to_txn(
            txn,
            "worker".to_owned(),
            "persistent://public/default/t".to_owned(),
        )
    };
    let frame = add_subscription_to_txn_response_bytes(request_id.0);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame)
            .expect("apply AddSubscriptionToTxnResponse");
    }
    let outcome = {
        let mut conn = shared.inner.lock();
        conn.take_outcome(PendingOpKey::Request(request_id))
            .expect("outcome ready")
    };
    match outcome {
        OpOutcome::AddSubscriptionToTxn { result, .. } => result.expect("ok add_subscription"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

/// `Connection::end_txn` returns `Committed` for `TxnAction::Commit`.
#[tokio::test(flavor = "current_thread")]
async fn end_txn_commit_resolves_to_committed() {
    let at = Instant::now();
    let shared = handshake_complete_shared(at);
    let txn = TxnId {
        most_sig_bits: 0x77,
        least_sig_bits: 0x88,
    };

    let request_id = {
        let mut conn = shared.inner.lock();
        conn.end_txn(txn, TxnAction::Commit)
    };
    let frame = end_txn_response_bytes(request_id.0);
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame).expect("apply EndTxnResponse");
    }
    let outcome = {
        let mut conn = shared.inner.lock();
        conn.take_outcome(PendingOpKey::Request(request_id))
            .expect("outcome ready")
    };
    match outcome {
        OpOutcome::EndTxn { result, .. } => {
            let state = result.expect("ok end_txn");
            assert_eq!(state, TxnState::Committed);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}
