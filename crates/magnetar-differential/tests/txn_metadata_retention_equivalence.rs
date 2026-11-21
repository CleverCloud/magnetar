// SPDX-License-Identifier: Apache-2.0

//! The connection-owned transaction client keeps no record of an ended transaction, identically on
//! both engines (ADR-0024 layer (d)).
//!
//! `TxnClient::handle_new_txn_response` registers one `TransactionMetadata` per transaction and
//! nothing removes it when `CommandEndTxnResponse` ends the transaction. Both engines wrap the same
//! `magnetar-proto` state machine and reuse one `Connection` across reconnects, so they must agree,
//! and a long-lived exactly-once client must not gain one record per transaction.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnId, TxnState,
    encode_command, pb,
};

/// How many transactions the scenario runs; the retained count grows by one per transaction.
const TRANSACTIONS: u64 = 4;

fn connected_frame() -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    frame
}

fn tokio_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
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

/// The number of transactions the client still tracks after each of [`TRANSACTIONS`] ended ones.
fn tracked_after_each_transaction(conn: &mut Connection, now: Instant) -> Vec<usize> {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    (0..TRANSACTIONS)
        .map(|ordinal| {
            let action = if ordinal % 2 == 0 {
                TxnAction::Commit
            } else {
                TxnAction::Abort
            };
            run_one_transaction(conn, now, ordinal, action)
        })
        .collect()
}

#[test]
fn engines_keep_no_record_of_an_ended_transaction() {
    let tokio = tokio_projection(tracked_after_each_transaction);
    let moonpool = moonpool_projection(tracked_after_each_transaction);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on how many transactions the client tracks after each one ends"
    );
    assert_eq!(
        tokio,
        vec![0; TRANSACTIONS as usize],
        "the number of transactions the client still tracks after each of {TRANSACTIONS} \
         committed/aborted transactions; a count that grows by one per transaction is a leak that \
         lasts as long as the connection"
    );
}

/// How many transactions the client tracks, and whether it still tracks the open one, after a
/// stale `CommandEndTxnResponse` names the one transaction that is open.
fn tracked_after_a_stale_end_response(conn: &mut Connection, now: Instant) -> (usize, bool) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    let open = open_one_transaction(conn, now, 0);
    feed(conn, now, &stale_end_response(open));
    (
        conn.txn_client().len(),
        conn.txn_client().transaction(open).is_some(),
    )
}

#[test]
fn engines_keep_an_open_transaction_through_a_stale_end_response() {
    let tokio = tokio_projection(tracked_after_a_stale_end_response);
    let moonpool = moonpool_projection(tracked_after_a_stale_end_response);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on what a stale end response leaves tracked"
    );
    assert_eq!(
        tokio,
        (1, true),
        "a stale response ends no transaction, so the open one must stay tracked"
    );
}
