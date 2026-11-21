// SPDX-License-Identifier: Apache-2.0

//! A transaction request whose operation deadline fires during a supervised
//! outage must not leave its `TxnClient` entry behind once the supervisor has
//! redialled — moonpool engine over `TokioProviders`, real loopback broker.
//! 1:1 twin of `crates/magnetar-runtime-tokio/tests/txn_request_redial.rs`
//! (ADR-0024 runtime-test-parity).
//!
//! The request is cancelled when its deadline fires (`RequestFut::cancellable`,
//! ADR-0129), so it has left the connection's pending requests by the time the
//! supervisor dials the new socket and calls `Connection::reset`. Its reply
//! could only have come on the session the reset ends, so the reset must
//! retire the entry the embedded `TxnClient` keeps for it (ADR-0138).
//!
//! Script: the first session completes the handshake and closes the socket as
//! soon as it reads the `AddPartitionToTxn` frame, without answering it. The
//! supervisor's first redial is held back for longer than the
//! `operation_timeout`, so the request times out during the outage. Every later
//! session completes the handshake and answers `Ping`. Once the client is
//! connected again, the test asks the `TxnClient` whether it still tracks the
//! timed-out request.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, Frame, FrameError, RequestId, SupervisorConfig, TxnId, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
mod common;
use common::HANG_GUARD;

/// The `operation_timeout` the client under test is built with. It also bounds
/// the handshake, so it is not shrunk below what a loaded CI runner can serve.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(1);

/// How long the supervisor waits before its redial: longer than
/// [`OPERATION_TIMEOUT`], so the request times out while the socket is gone.
const REDIAL_BACKOFF: Duration = Duration::from_secs(3);

fn emit(out: &mut BytesMut, ty: pb::base_command::Type, fill: impl FnOnce(&mut pb::BaseCommand)) {
    let mut cmd = pb::BaseCommand {
        r#type: ty as i32,
        ..Default::default()
    };
    fill(&mut cmd);
    let _ = encode_command(out, &cmd);
}

/// Serve one session. The first one (`drop_on_txn_request`) closes the socket
/// on the first `AddPartitionToTxn` frame; the others only answer the
/// handshake and `Ping`. `connected` is notified on every `Connect` frame.
async fn serve(mut stream: TcpStream, drop_on_txn_request: bool, connected: Arc<Notify>) {
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
                Ok(pb::base_command::Type::Connect) => {
                    emit(&mut out_buf, pb::base_command::Type::Connected, |cmd| {
                        cmd.connected = Some(pb::CommandConnected {
                            server_version: "txn-redial-broker/0".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        });
                    });
                    connected.notify_one();
                }
                Ok(pb::base_command::Type::Ping) => {
                    emit(&mut out_buf, pb::base_command::Type::Pong, |cmd| {
                        cmd.pong = Some(pb::CommandPong {});
                    });
                }
                Ok(pb::base_command::Type::AddPartitionToTxn) if drop_on_txn_request => return,
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

async fn run_broker(listener: TcpListener, connected: Arc<Notify>) {
    let mut first = true;
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(serve(stream, first, connected.clone()));
        first = false;
    }
}

/// Whether the client's `TxnClient` still tracks the `AddPartitionToTxn`
/// request `request_id`: `register_add_partition_waker` keeps a waker only for
/// a request it still tracks, so a probe waker it kept a clone of proves the
/// entry is there.
fn txn_client_tracks_add_partition<P: moonpool_core::Providers>(
    client: &Client<P>,
    request_id: RequestId,
) -> bool {
    #[derive(Default)]
    struct Probe(AtomicUsize);
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let probe = Arc::new(Probe::default());
    client
        .shared()
        .inner
        .lock()
        .txn_client_mut()
        .register_add_partition_waker(request_id, Waker::from(Arc::clone(&probe)));
    Arc::strong_count(&probe) > 1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_request_timed_out_during_an_outage_leaves_no_txn_client_entry_after_the_redial() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let connected = Arc::new(Notify::new());
    tokio::spawn(run_broker(listener, connected.clone()));

    let config = ConnectionConfig {
        operation_timeout: OPERATION_TIMEOUT,
        supervisor: Some(SupervisorConfig {
            initial_backoff: REDIAL_BACKOFF,
            max_backoff: REDIAL_BACKOFF,
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    };
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr.to_string(), config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");
    tokio::time::timeout(HANG_GUARD, connected.notified())
        .await
        .expect("the first session completed its handshake");

    let request_id = RequestId(client.shared().inner.lock().peek_next_request_id_for_test());
    let topic = "persistent://public/default/txn-request-redial";
    let result = tokio::time::timeout(
        HANG_GUARD,
        client.add_partition_to_txn(TxnId::new(0x51, 0x52), topic),
    )
    .await
    .expect("the request is bounded by its operation deadline");
    assert!(
        result.is_err(),
        "the broker dropped the session without answering, so the request must fail: {result:?}"
    );
    assert!(
        !client.is_connected(),
        "the request must have timed out during the outage, before the redial"
    );
    assert!(
        txn_client_tracks_add_partition(&client, request_id),
        "control: the timed-out request is still tracked while the session it was sent on is \
         the current one"
    );

    tokio::time::timeout(HANG_GUARD, connected.notified())
        .await
        .expect("the supervisor redialled and the new session completed its handshake");
    tokio::time::timeout(HANG_GUARD, async {
        while !client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the client is connected again after the redial");

    assert!(
        !txn_client_tracks_add_partition(&client, request_id),
        "the redial's reset kept the TxnClient entry of a request that timed out during the \
         outage: its reply could only have come on the session the reset ended"
    );

    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
}
