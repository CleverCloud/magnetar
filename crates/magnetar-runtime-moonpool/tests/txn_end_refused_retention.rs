// SPDX-License-Identifier: Apache-2.0

//! The connection-owned transaction client must not keep a record of a transaction whose
//! `CommandEndTxn` the broker refused, exercised through the moonpool engine's
//! [`ConnectionShared`] state. Mirrors the matching `magnetar-runtime-tokio` test, the
//! `magnetar-proto` unit test (`end_txn_refused_by_the_broker_leaves_no_record_of_the_transaction`
//! in `crates/magnetar-proto/src/conn.rs`) and the `magnetar-differential` equivalence scenario.
//!
//! A refusal is as terminal for the record as a commit or an abort: the caller receives the
//! broker's error through its outcome and nothing reads the record again, but
//! `TxnClient::handle_end_txn_response` only rewrote its state to `Errored`. The engine builds its
//! `Connection` once and reuses it across reconnects, so each refused end would otherwise keep one
//! record for as long as the client runs.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnError, TxnId,
    encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// How many transactions the scenario runs; the retained count grows by one per refused end.
const TRANSACTIONS: u64 = 4;

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

/// Opens one transaction and has the broker refuse its end, asserting every step, then returns how
/// many transactions the connection's transaction client tracked while it was open and after the
/// refusal.
fn run_one_refused_transaction(
    conn: &mut Connection,
    now: Instant,
    ordinal: u64,
    action: TxnAction,
) -> (usize, usize) {
    let new_request = conn.new_txn(Duration::from_secs(60));
    feed(
        conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxnResponse as i32,
            new_txn_response: Some(pb::CommandNewTxnResponse {
                request_id: new_request.0,
                txnid_most_bits: Some(0x11),
                txnid_least_bits: Some(ordinal),
                error: None,
                message: None,
            }),
            ..Default::default()
        },
    );
    let txn = match conn.take_outcome(PendingOpKey::Request(new_request)) {
        Some(OpOutcome::NewTxn {
            result: Ok(txn), ..
        }) => txn,
        other => panic!("NewTxn must resolve to a transaction id, got {other:?}"),
    };
    assert_eq!(txn, TxnId::new(0x11, ordinal));
    let tracked_while_open = conn.txn_client().len();

    let end_request = conn.end_txn(txn, action).expect("claim the EndTxn waiter");
    feed(
        conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::EndTxnResponse as i32,
            end_txn_response: Some(pb::CommandEndTxnResponse {
                request_id: end_request.0,
                txnid_most_bits: Some(txn.most_sig_bits),
                txnid_least_bits: Some(txn.least_sig_bits),
                error: Some(pb::ServerError::TransactionConflict as i32),
                message: Some("conflict".to_owned()),
            }),
            ..Default::default()
        },
    );
    match conn.take_outcome(PendingOpKey::Request(end_request)) {
        Some(OpOutcome::EndTxn {
            result: Err(TxnError::Conflict),
            ..
        }) => {}
        other => panic!("EndTxn must resolve to the broker's refusal, got {other:?}"),
    }
    conn.release_end_txn_waiter(txn, action);

    (tracked_while_open, conn.txn_client().len())
}

#[test]
fn refused_transaction_ends_leave_no_record_in_the_transaction_client() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    let tracked: Vec<(usize, usize)> = (0..TRANSACTIONS)
        .map(|ordinal| {
            let action = if ordinal % 2 == 0 {
                TxnAction::Commit
            } else {
                TxnAction::Abort
            };
            run_one_refused_transaction(&mut conn, now, ordinal, action)
        })
        .collect();

    assert_eq!(
        tracked,
        vec![(1, 0); TRANSACTIONS as usize],
        "per transaction: tracked while open, then after the broker refused its end; a count \
         that stays above zero grows by one per refused transaction for the life of the \
         connection"
    );
    assert!(
        conn.txn_client().is_empty(),
        "no transaction is open, so none may be tracked"
    );
}
