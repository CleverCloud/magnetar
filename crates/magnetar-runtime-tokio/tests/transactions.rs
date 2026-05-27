// SPDX-License-Identifier: Apache-2.0

//! PIP-31 transactions — tokio engine.
//!
//! Mirror of `magnetar-runtime-moonpool/tests/transactions.rs`. Drives
//! `magnetar_proto::Connection::{new_txn, add_partition_to_txn,
//! add_subscription_to_txn, end_txn}` with synthetic broker responses;
//! the tokio engine's `Client::new_txn` is a delegate over the same
//! proto methods, so proving them here covers both layers without
//! spinning up a TCP listener.
//!
//! Parity required by ADR-0024 — count of tests must match the
//! moonpool side 1:1.

#![allow(clippy::expect_used)]

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnId, TxnState,
    encode_command, pb,
};

fn connected_frame() -> BytesMut {
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

fn handshake_complete(at: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    let frame = connected_frame();
    conn.handle_bytes(at, &frame).expect("connected");
    let _ = conn.poll_event();
    conn
}

fn new_txn_response_bytes(request_id: u64, txn_most: u64, txn_least: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::NewTxnResponse as i32,
        new_txn_response: Some(pb::CommandNewTxnResponse {
            request_id,
            txnid_most_bits: Some(txn_most),
            txnid_least_bits: Some(txn_least),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandNewTxnResponse");
    buf
}

fn add_partition_to_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddPartitionToTxnResponse as i32,
        add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAddPartitionToTxnResponse");
    buf
}

fn add_subscription_to_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddSubscriptionToTxnResponse as i32,
        add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAddSubscriptionToTxnResponse");
    buf
}

fn end_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::EndTxnResponse as i32,
        end_txn_response: Some(pb::CommandEndTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandEndTxnResponse");
    buf
}

#[tokio::test(flavor = "current_thread")]
async fn new_txn_returns_broker_assigned_txn_id() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);

    let request_id = conn.new_txn(Duration::from_secs(60));
    let frame = new_txn_response_bytes(request_id.0, 0x11, 0x22);
    conn.handle_bytes(at, &frame).expect("apply NewTxnResponse");
    let outcome = conn
        .take_outcome(PendingOpKey::Request(request_id))
        .expect("outcome ready");
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

#[tokio::test(flavor = "current_thread")]
async fn add_partition_to_txn_returns_ok() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let txn = TxnId {
        most_sig_bits: 0x33,
        least_sig_bits: 0x44,
    };

    let request_id = conn.add_partition_to_txn(txn, "persistent://public/default/t".to_owned());
    let frame = add_partition_to_txn_response_bytes(request_id.0);
    conn.handle_bytes(at, &frame)
        .expect("apply AddPartitionToTxnResponse");
    let outcome = conn
        .take_outcome(PendingOpKey::Request(request_id))
        .expect("outcome ready");
    match outcome {
        OpOutcome::AddPartitionToTxn { result, .. } => result.expect("ok add_partition"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn add_subscription_to_txn_returns_ok() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let txn = TxnId {
        most_sig_bits: 0x55,
        least_sig_bits: 0x66,
    };

    let request_id = conn.add_subscription_to_txn(
        txn,
        "worker".to_owned(),
        "persistent://public/default/t".to_owned(),
    );
    let frame = add_subscription_to_txn_response_bytes(request_id.0);
    conn.handle_bytes(at, &frame)
        .expect("apply AddSubscriptionToTxnResponse");
    let outcome = conn
        .take_outcome(PendingOpKey::Request(request_id))
        .expect("outcome ready");
    match outcome {
        OpOutcome::AddSubscriptionToTxn { result, .. } => result.expect("ok add_subscription"),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn end_txn_commit_resolves_to_committed() {
    let at = Instant::now();
    let mut conn = handshake_complete(at);
    let txn = TxnId {
        most_sig_bits: 0x77,
        least_sig_bits: 0x88,
    };

    let request_id = conn.end_txn(txn, TxnAction::Commit);
    let frame = end_txn_response_bytes(request_id.0);
    conn.handle_bytes(at, &frame).expect("apply EndTxnResponse");
    let outcome = conn
        .take_outcome(PendingOpKey::Request(request_id))
        .expect("outcome ready");
    match outcome {
        OpOutcome::EndTxn { result, .. } => {
            let state = result.expect("ok end_txn");
            assert_eq!(state, TxnState::Committed);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}
