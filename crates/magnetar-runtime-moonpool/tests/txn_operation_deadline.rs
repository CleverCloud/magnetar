// SPDX-License-Identifier: Apache-2.0

//! PIP-31 transaction requests honour the operation deadline — moonpool engine,
//! real loopback broker. 1:1 twin of
//! `crates/magnetar-runtime-tokio/tests/txn_operation_deadline.rs`
//! (ADR-0024 runtime-test-parity).
//!
//! Every other caller-visible setup operation (`open_producer`, `subscribe`,
//! `lookup_topic`, `partitioned_topic_metadata`, `watch_topic_list`) is raced
//! against `Client::operation_timer()` and surfaces `ClientError::Timeout`
//! when `ConnectionConfig::operation_timeout` elapses with no answer. The
//! PIP-31 request family (`new_txn` and its coordinator bootstrap,
//! `add_partition_to_txn`, `add_subscription_to_txn`, `end_txn`) awaits its
//! `RequestFut` bare, and `TxnError::Timeout` ("the driver layer surfaces it")
//! has no producer: a commit issued while the coordinator does not answer
//! (a supervised outage, a reconnect in progress) parks for as long as the
//! silence lasts.
//!
//! Script: a broker that completes the handshake and answers `Lookup` and
//! `Ping` but never answers any transaction-coordinator frame, and a
//! connection that stays up and healthy (so neither the keepalive watchdog nor
//! the terminal-exit path can resolve the request behind the test's back). The
//! client is built with a short `operation_timeout`; each transaction request
//! must resolve with an error within several multiples of it. The error
//! variant is deliberately not pinned: only "bounded by the operation
//! deadline" is the contract under test.

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, Frame, FrameError, TxnAction, TxnId, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, ClientError, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
mod common;
use common::HANG_GUARD;

/// The `operation_timeout` the client under test is built with. It also bounds
/// the handshake, so it is not shrunk below what a loaded CI runner can serve.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a transaction request may take before it counts as unbounded:
/// several multiples of [`OPERATION_TIMEOUT`]. A request honouring the
/// operation deadline answers within about one multiple; one that does not
/// parks forever, so this bound firing IS the defect.
const DEADLINE_BOUND: Duration = Duration::from_secs(6);

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "txn-deadline-broker/0".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Scripted broker with a silent transaction coordinator: answers the
/// handshake, `Lookup` and `Ping`, and ignores every other frame — in
/// particular `TcClientConnectRequest`, `NewTxn`, `AddPartitionToTxn`,
/// `AddSubscriptionToTxn` and `EndTxn`. The socket stays open for the whole
/// test.
async fn run_silent_coordinator_broker(listener: TcpListener) {
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame: Frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => emit_connected(&mut out_buf),
                Ok(pb::base_command::Type::Lookup) => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                Ok(pb::base_command::Type::Ping) => emit_pong(&mut out_buf),
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            let _ = stream.flush().await;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// `None` when the request resolved with an error inside [`DEADLINE_BOUND`],
/// otherwise a line naming the entry point and what it did instead.
fn not_bounded<T: std::fmt::Debug>(
    entry_point: &str,
    res: Result<Result<T, ClientError>, tokio::time::error::Elapsed>,
) -> Option<String> {
    match res {
        Ok(Err(_)) => None,
        Ok(Ok(value)) => Some(format!(
            "{entry_point}: resolved Ok({value:?}) from a coordinator that never answered"
        )),
        Err(_) => Some(format!(
            "{entry_point}: still pending after {DEADLINE_BOUND:?} \
             (operation_timeout is {OPERATION_TIMEOUT:?})"
        )),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_requests_honour_operation_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(run_silent_coordinator_broker(listener));

    let config = ConnectionConfig {
        operation_timeout: OPERATION_TIMEOUT,
        ..ConnectionConfig::default()
    };
    assert!(
        config.supervisor.is_none() && config.operation_timeout == OPERATION_TIMEOUT,
        "this test pins a live plain connection with a short operation_timeout",
    );

    let engine = MoonpoolEngine::new(TokioProviders::new());

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain(&engine, &addr.to_string(), config),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let txn = TxnId::new(0xAA, 0xBB);
    let topic = "persistent://public/default/txn-operation-deadline";
    let (new_txn_res, add_partition_res, add_subscription_res, end_txn_res) = tokio::join!(
        // Un-bootstrapped: the lookup is answered, then the `TcClientConnect`
        // handshake waits on a coordinator that stays silent.
        tokio::time::timeout(DEADLINE_BOUND, client.new_txn(Duration::from_mins(1))),
        tokio::time::timeout(DEADLINE_BOUND, client.add_partition_to_txn(txn, topic)),
        tokio::time::timeout(
            DEADLINE_BOUND,
            client.add_subscription_to_txn(txn, topic, "sub-txn-deadline"),
        ),
        tokio::time::timeout(DEADLINE_BOUND, client.end_txn(txn, TxnAction::Commit)),
    );
    assert!(
        client.is_connected(),
        "the connection must have stayed up: only the coordinator was silent",
    );
    let failures: Vec<String> = [
        not_bounded("new_txn (coordinator bootstrap)", new_txn_res),
        not_bounded("add_partition_to_txn", add_partition_res),
        not_bounded("add_subscription_to_txn", add_subscription_res),
        not_bounded("end_txn", end_txn_res),
    ]
    .into_iter()
    .flatten()
    .collect();
    assert!(
        failures.is_empty(),
        "every PIP-31 request must be bounded by operation_timeout and resolve with an error, \
         but: {failures:#?}",
    );
}
