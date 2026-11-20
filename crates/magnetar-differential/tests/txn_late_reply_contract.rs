// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O PIP-31 contracts executed by the simulation runner.
//!
//! Both runtimes cancel a transaction request whose operation deadline expires
//! ([ADR-0129](../../../specs/adr/0129-bound-and-fast-fail-pip-31-transaction-requests.md)):
//! dropping the `RequestFut` calls `Connection::cancel_request`. The broker can
//! still answer afterwards. That late reply must be ignored by the shared
//! `Connection`, not parked in its outcomes for a future nobody holds.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, PendingOpKey, RequestId, TxnId, encode_command, pb,
};

const TOPIC: &str = "persistent://public/default/txn-late-reply";
const SUBSCRIPTION: &str = "txn-late-reply";

/// The successful reply the broker sends for each kind of request, addressed to `request_id`.
fn late_reply(kind: &str, request_id: RequestId, txn: TxnId) -> pb::BaseCommand {
    let request_id = request_id.0;
    let txnid_most_bits = Some(txn.most_sig_bits);
    let txnid_least_bits = Some(txn.least_sig_bits);
    match kind {
        "tc_client_connect" => pb::BaseCommand {
            r#type: pb::base_command::Type::TcClientConnectResponse as i32,
            tc_client_connect_response: Some(pb::CommandTcClientConnectResponse {
                request_id,
                error: None,
                message: None,
            }),
            ..Default::default()
        },
        "new_txn" => pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxnResponse as i32,
            new_txn_response: Some(pb::CommandNewTxnResponse {
                request_id,
                txnid_most_bits,
                txnid_least_bits,
                error: None,
                message: None,
            }),
            ..Default::default()
        },
        "add_partition_to_txn" => pb::BaseCommand {
            r#type: pb::base_command::Type::AddPartitionToTxnResponse as i32,
            add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
                request_id,
                txnid_most_bits,
                txnid_least_bits,
                error: None,
                message: None,
            }),
            ..Default::default()
        },
        "add_subscription_to_txn" => pb::BaseCommand {
            r#type: pb::base_command::Type::AddSubscriptionToTxnResponse as i32,
            add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
                request_id,
                txnid_most_bits,
                txnid_least_bits,
                error: None,
                message: None,
            }),
            ..Default::default()
        },
        other => panic!("no late reply scripted for {other}"),
    }
}

/// A request cancelled by its caller stays cancelled: the broker's late reply leaves no outcome,
/// for every request kind a runtime cancels on expiry. (`end_txn` is excluded on purpose: its
/// canonical request survives a released waiter so that a retry resumes it.)
#[test]
fn late_reply_to_a_cancelled_txn_request_leaves_no_outcome() {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    let txn = TxnId::new(7, 11);
    let requests = [
        ("tc_client_connect", conn.tc_client_connect(0)),
        ("new_txn", conn.new_txn(Duration::from_secs(30))),
        (
            "add_partition_to_txn",
            conn.add_partition_to_txn(txn, TOPIC.to_owned()),
        ),
        (
            "add_subscription_to_txn",
            conn.add_subscription_to_txn(txn, SUBSCRIPTION.to_owned(), TOPIC.to_owned()),
        ),
    ];
    for (kind, request_id) in requests {
        conn.cancel_request(request_id);
        let mut encoded = BytesMut::new();
        encode_command(&mut encoded, &late_reply(kind, request_id, txn))
            .expect("encode late reply");
        conn.handle_bytes(Instant::now(), &encoded)
            .expect("a late reply is not a protocol error");
        assert!(
            conn.take_outcome(PendingOpKey::Request(request_id))
                .is_none(),
            "{kind}: a late reply to a cancelled request must not park an outcome",
        );
    }
}
