// SPDX-License-Identifier: Apache-2.0

//! The connection-owned transaction client keeps no record of a transaction whose `CommandEndTxn`
//! the broker refused, identically on both engines (ADR-0024 layer (d)).
//!
//! A refusal is as terminal for the record as a commit or an abort: the caller receives the
//! broker's error through its outcome and nothing reads the record again, but
//! `TxnClient::handle_end_txn_response` only rewrote its state to `Errored`. Both engines wrap the
//! same `magnetar-proto` state machine and reuse one `Connection` across reconnects, so they must
//! agree, and a long-lived exactly-once client must not gain one record per refused end.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnError, TxnId,
    encode_command, pb,
};

/// How many transactions the scenario runs; the retained count grows by one per refused end.
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

/// Per refused transaction, how many the client tracked while it was open and after the refusal.
fn tracked_per_refused_transaction(conn: &mut Connection, now: Instant) -> Vec<(usize, usize)> {
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
            run_one_refused_transaction(conn, now, ordinal, action)
        })
        .collect()
}

#[test]
fn engines_keep_no_record_of_a_transaction_whose_end_was_refused() {
    let tokio = tokio_projection(tracked_per_refused_transaction);
    let moonpool = moonpool_projection(tracked_per_refused_transaction);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on how many transactions the client tracks around a refused end"
    );
    assert_eq!(
        tokio,
        vec![(1, 0); TRANSACTIONS as usize],
        "per transaction: tracked while open, then after the broker refused its end; a count \
         that stays above zero grows by one per refused transaction for the life of the \
         connection"
    );
}
