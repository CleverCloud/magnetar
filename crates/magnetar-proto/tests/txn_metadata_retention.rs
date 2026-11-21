// SPDX-License-Identifier: Apache-2.0

//! The connection-owned transaction client must not keep a record of a transaction that has ended.
//!
//! `TxnClient::handle_new_txn_response` registers one `TransactionMetadata` per transaction in
//! `TxnClient::transactions`. A `CommandEndTxnResponse` only rewrites the state of that record to
//! `Committed` / `Aborted`; the only remover, `TxnClient::forget`, has no production caller. The
//! `Connection` outlives every reconnect (`reset()` reuses it), so a long-lived exactly-once client
//! gains one record per transaction for as long as it runs, and nothing ever reads a record of an
//! ended transaction again.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::{Duration, Instant};

use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnId, TxnState,
    encode_command, pb,
};

/// How many transactions the scenario runs; the retained count grows by one per transaction.
const TRANSACTIONS: u64 = 4;

fn connected(now: Instant) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    feed(
        &mut conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    );
    let _ = conn.poll_transmit();
    conn
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

/// Opens one transaction, asserting the broker's `CommandNewTxnResponse` resolved it, and returns
/// its id; the transaction stays open.
fn open_one_transaction(conn: &mut Connection, now: Instant, ordinal: u64) -> TxnId {
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
    txn
}

/// A `CommandEndTxnResponse` for a request the connection never issued, naming `txn`.
fn stale_end_response(txn: TxnId) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: pb::base_command::Type::EndTxnResponse as i32,
        end_txn_response: Some(pb::CommandEndTxnResponse {
            request_id: u64::MAX,
            txnid_most_bits: Some(txn.most_sig_bits),
            txnid_least_bits: Some(txn.least_sig_bits),
            error: None,
            message: None,
        }),
        ..Default::default()
    }
}

/// Opens one transaction and ends it, asserting every step succeeded, then returns how many
/// transactions the connection's transaction client still tracks.
fn run_one_transaction(
    conn: &mut Connection,
    now: Instant,
    ordinal: u64,
    action: TxnAction,
) -> usize {
    let txn = open_one_transaction(conn, now, ordinal);

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
                error: None,
                message: None,
            }),
            ..Default::default()
        },
    );
    let expected = match action {
        TxnAction::Commit => TxnState::Committed,
        TxnAction::Abort => TxnState::Aborted,
    };
    match conn.take_outcome(PendingOpKey::Request(end_request)) {
        Some(OpOutcome::EndTxn {
            result: Ok(state), ..
        }) => assert_eq!(state, expected),
        other => panic!("EndTxn must resolve to {expected:?}, got {other:?}"),
    }
    conn.release_end_txn_waiter(txn, action);

    conn.txn_client().len()
}

/// A committed or aborted transaction has no further use for its client-side record: the broker's
/// transaction coordinator owns the outcome, and the caller already holds it. After each
/// transaction has ended and both of its outcomes have been taken, the client must track nothing.
#[test]
fn ended_transactions_leave_no_record_in_the_transaction_client() {
    let now = Instant::now();
    let mut conn = connected(now);

    let tracked_after_each: Vec<usize> = (0..TRANSACTIONS)
        .map(|ordinal| {
            let action = if ordinal % 2 == 0 {
                TxnAction::Commit
            } else {
                TxnAction::Abort
            };
            run_one_transaction(&mut conn, now, ordinal, action)
        })
        .collect();

    assert_eq!(
        tracked_after_each,
        vec![0; TRANSACTIONS as usize],
        "the number of transactions the client still tracks after each of {TRANSACTIONS} \
         committed/aborted transactions; a count that grows by one per transaction is a leak that \
         lasts as long as the connection"
    );
    assert!(
        conn.txn_client().is_empty(),
        "no transaction is open, so none may be tracked"
    );
}

/// The record is forgotten through the request a `CommandEndTxnResponse` answers. A stale response
/// (a duplicate, or an answer to a request this connection never issued) can still name a
/// transaction that is open, and ends nothing: that transaction must stay tracked.
#[test]
fn a_stale_end_response_forgets_no_open_transaction() {
    let now = Instant::now();
    let mut conn = connected(now);
    let open = open_one_transaction(&mut conn, now, 0);
    assert_eq!(
        conn.txn_client().len(),
        1,
        "the opened transaction is tracked"
    );

    feed(&mut conn, now, &stale_end_response(open));

    assert_eq!(
        conn.txn_client().len(),
        1,
        "a stale response ends no transaction, so none may be forgotten"
    );
    assert!(
        conn.txn_client().transaction(open).is_some(),
        "the open transaction must still be tracked after a stale response"
    );
}
