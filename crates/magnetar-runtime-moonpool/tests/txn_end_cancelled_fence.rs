// SPDX-License-Identifier: Apache-2.0

//! A commit whose caller was dropped still has its `CommandEndTxn` in flight, so the opposite end
//! must keep being refused, exercised through the moonpool engine's [`ConnectionShared`] state.
//! Mirrors the matching `magnetar-runtime-tokio` test, the `magnetar-proto` unit test
//! (`end_txn_refuses_the_opposite_action_while_a_cancelled_end_is_in_flight` in
//! `crates/magnetar-proto/src/conn.rs`) and the `magnetar-differential` equivalence scenario.
//!
//! Dropping the `Client::end_txn` future releases the runtime waiter lease
//! (`Connection::release_end_txn_waiter`) but not the request, which the transaction coordinator
//! will still apply. The `AlreadyEnding` fence used to read only the lease, so an abort issued
//! after the cancelled commit passed it, overwrote the local state to `Aborting` and staged a
//! second, conflicting `CommandEndTxn` behind the first.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, TxnAction, TxnError, TxnId, TxnState,
    encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

fn feed(conn: &mut Connection, now: Instant, command: &pb::BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(&mut buf, command).expect("encode command");
    conn.handle_bytes(now, &buf).expect("apply command");
}

/// Opens one transaction the connection's transaction client tracks.
fn open_transaction(conn: &mut Connection, now: Instant) -> TxnId {
    let new_request = conn.new_txn(Duration::from_secs(60));
    feed(
        conn,
        now,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxnResponse as i32,
            new_txn_response: Some(pb::CommandNewTxnResponse {
                request_id: new_request.0,
                txnid_most_bits: Some(0x11),
                txnid_least_bits: Some(0x22),
                error: None,
                message: None,
            }),
            ..Default::default()
        },
    );
    match conn.take_outcome(PendingOpKey::Request(new_request)) {
        Some(OpOutcome::NewTxn {
            result: Ok(txn), ..
        }) => txn,
        other => panic!("NewTxn must resolve to a transaction id, got {other:?}"),
    }
}

/// How many `CommandEndTxn` frames the connection has put on the wire since the last drain.
fn end_txn_frames_on_the_wire(conn: &mut Connection) -> usize {
    let mut wire = conn.poll_transmit();
    let mut end_commands = 0;
    while !wire.is_empty() {
        let frame = magnetar_proto::decode_one(&mut wire).expect("decode command");
        end_commands += usize::from(frame.command.r#type == pb::base_command::Type::EndTxn as i32);
    }
    end_commands
}

#[test]
fn abort_after_a_cancelled_commit_is_refused_while_the_commit_is_in_flight() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();
    let txn = open_transaction(&mut conn, now);

    let commit = conn
        .end_txn(txn, TxnAction::Commit)
        .expect("the commit claims the EndTxn waiter");
    // The commit's caller is dropped before the broker answers: the lease is released, the
    // request stays on the wire.
    conn.release_end_txn_waiter(txn, TxnAction::Commit);

    assert!(
        matches!(
            conn.end_txn(txn, TxnAction::Abort),
            Err(TxnError::AlreadyEnding {
                txn_id,
                action: TxnAction::Commit,
            }) if txn_id == txn
        ),
        "an abort issued while the cancelled commit is still unresolved must be refused"
    );
    assert_eq!(
        end_txn_frames_on_the_wire(&mut conn),
        1,
        "only the commit's CommandEndTxn may reach the wire"
    );
    assert_eq!(
        conn.txn_client()
            .transaction(txn)
            .map(|metadata| metadata.state),
        Some(TxnState::Committing),
        "a refused abort must leave the local state at the commit in flight"
    );
    assert_eq!(
        conn.end_txn(txn, TxnAction::Commit)
            .expect("the refused abort leaves the commit resumable"),
        commit
    );
}
