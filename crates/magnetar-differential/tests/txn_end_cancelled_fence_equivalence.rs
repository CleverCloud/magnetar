// SPDX-License-Identifier: Apache-2.0

//! A commit whose caller was dropped still has its `CommandEndTxn` in flight, so the opposite end
//! is refused identically on both engines (ADR-0024 layer (d)).
//!
//! Dropping the `Client::end_txn` future releases the runtime waiter lease but not the request,
//! which the transaction coordinator will still apply. The `AlreadyEnding` fence used to read only
//! the lease, so an abort issued after the cancelled commit passed it, overwrote the local state
//! to `Aborting` and staged a second, conflicting `CommandEndTxn` behind the first. Both engines
//! wrap the same `magnetar-proto` state machine, so they must agree on what the second end gets
//! back, on how many `CommandEndTxn` frames reach the wire and on the transaction's local state.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, OpOutcome, PendingOpKey, RequestId, TxnAction, TxnError, TxnId,
    TxnState, encode_command, pb,
};

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

/// What a caller and the wire observe after an end is requested behind a cancelled opposite end.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// The second end's answer: the refusal's transaction and the action it names, or the request
    /// id it was staged under.
    second_end: Result<RequestId, (TxnId, TxnAction)>,
    /// `CommandEndTxn` frames that reached the wire, with the action each carries.
    end_txn_on_the_wire: Vec<i32>,
    /// The transaction's local state once the second end was answered.
    state: Option<TxnState>,
}

/// Opens a transaction, ends it with `first`, drops that end's caller, then asks for `second`.
fn second_end_behind_a_cancelled_first(
    conn: &mut Connection,
    now: Instant,
    first: TxnAction,
    second: TxnAction,
) -> Observed {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

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
    let txn = match conn.take_outcome(PendingOpKey::Request(new_request)) {
        Some(OpOutcome::NewTxn {
            result: Ok(txn), ..
        }) => txn,
        other => panic!("NewTxn must resolve to a transaction id, got {other:?}"),
    };

    conn.end_txn(txn, first)
        .expect("the first end claims the EndTxn waiter");
    conn.release_end_txn_waiter(txn, first);

    let second_end = match conn.end_txn(txn, second) {
        Ok(request_id) => Ok(request_id),
        Err(TxnError::AlreadyEnding { txn_id, action }) => Err((txn_id, action)),
        Err(other) => panic!("the second end must be staged or fenced, got {other:?}"),
    };

    let mut wire = conn.poll_transmit();
    let mut end_txn_on_the_wire = Vec::new();
    while !wire.is_empty() {
        let frame = magnetar_proto::decode_one(&mut wire).expect("decode command");
        if frame.command.r#type == pb::base_command::Type::EndTxn as i32 {
            end_txn_on_the_wire.push(
                frame
                    .command
                    .end_txn
                    .expect("an EndTxn command carries its body")
                    .txn_action
                    .expect("an EndTxn command names its action"),
            );
        }
    }

    Observed {
        second_end,
        end_txn_on_the_wire,
        state: conn
            .txn_client()
            .transaction(txn)
            .map(|metadata| metadata.state),
    }
}

#[test]
fn engines_refuse_the_opposite_end_while_a_cancelled_end_is_in_flight() {
    let scenarios = [
        (TxnAction::Commit, TxnAction::Abort),
        (TxnAction::Abort, TxnAction::Commit),
    ];
    for (first, second) in scenarios {
        let tokio = tokio_projection(|conn, now| {
            second_end_behind_a_cancelled_first(conn, now, first, second)
        });
        let moonpool = moonpool_projection(|conn, now| {
            second_end_behind_a_cancelled_first(conn, now, first, second)
        });
        assert_eq!(
            tokio, moonpool,
            "engines must agree on a {second:?} issued behind a cancelled {first:?}"
        );
        let in_flight_state = match first {
            TxnAction::Commit => TxnState::Committing,
            TxnAction::Abort => TxnState::Aborting,
        };
        assert!(
            matches!(&tokio.second_end, Err((_, action)) if *action == first),
            "a {second:?} behind a cancelled {first:?} that is still in flight must be refused \
             with the action that is, got {tokio:?}"
        );
        assert_eq!(
            tokio.end_txn_on_the_wire,
            vec![first.to_pb() as i32],
            "only the cancelled {first:?}'s CommandEndTxn may reach the wire, got {tokio:?}"
        );
        assert_eq!(
            tokio.state,
            Some(in_flight_state),
            "a refused {second:?} must leave the local state at the {first:?} in flight, got \
             {tokio:?}"
        );
    }
}
