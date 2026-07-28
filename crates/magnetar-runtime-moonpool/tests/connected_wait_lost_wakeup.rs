// SPDX-License-Identifier: Apache-2.0

//! Handshake-wait lost-wakeup regression — moonpool engine twin of the
//! `client::tests::wait_connected_registers_before_the_driver_pulse_can_race_it`
//! unit test in `magnetar-runtime-tokio` (ADR-0024 cross-runtime test parity,
//! 1:1 test names). The tokio twin is an in-crate unit test because
//! `ConnectedFut` / `wait_connected` are `pub(crate)`.
//!
//! The tokio engine dials, spawns the driver task, and then *parks* until the
//! driver reports `CONNECTED`. The driver announces that with
//! `notify_waiters()`, which stores **no permit**, so a waiter that enrolls
//! after the pulse misses it outright — and a freshly dialled connection is
//! idle once `CONNECTED` lands, so nothing pulses again. The wait then burns
//! the whole `operation_timeout` and the caller surfaces
//! `producer target resolution exceeded operation_timeout`. The fix arms an
//! owned `Notified` *before* inspecting connection state — the same
//! enroll-before-drain idiom as `ConnectionShared::await_reconnect_or_terminal`
//! and the PIP-33 marker accessor (`marker_lost_wakeup.rs`).
//!
//! The moonpool engine is structurally immune: `build_entry` / `connect_plain`
//! complete the handshake **inline** via `handshake_plain` *before* the driver
//! task is spawned, so there is no park between "driver reports CONNECTED" and
//! "caller observes CONNECTED" for a pulse to fall into. This test pins that
//! invariant — a refactor that moved the moonpool handshake behind a
//! notification park would have to re-derive the tokio-side ordering rule, and
//! this assertion is what would catch it.

#![forbid(unsafe_code)]

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

/// Minimal broker: answers `CONNECT` with `CONNECTED` and nothing else. The
/// connection is deliberately silent afterwards — that silence is exactly what
/// turns a single missed handshake pulse into a full `operation_timeout` hang
/// on the tokio side.
async fn serve_connect_only_broker(mut stream: TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            if pb::base_command::Type::try_from(frame.command.r#type)
                == Ok(pb::base_command::Type::Connect)
            {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Connected as i32,
                    connected: Some(pb::CommandConnected {
                        server_version: "connect-only-broker".to_owned(),
                        protocol_version: Some(21),
                        max_message_size: Some(5 * 1024 * 1024),
                        feature_flags: Some(pb::FeatureFlags::default()),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(&mut out, &cmd);
            }
        }
        if !out.is_empty() && stream.write_all(&out.split()).await.is_err() {
            return;
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => read_buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// `Client::connect_plain` must return a connection that is **already**
/// handshaked, without depending on a post-dial notification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_connected_registers_before_the_driver_pulse_can_race_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
            let addr = listener.local_addr().expect("local_addr").to_string();
            tokio::spawn(async move {
                if let Ok((stream, _peer)) = listener.accept().await {
                    serve_connect_only_broker(stream).await;
                }
            });

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out (a lost handshake pulse would hang here)")
            .expect("connect ok");

            // The handshake is completed inline by `handshake_plain` before the
            // driver task is spawned, so the connection is live the instant
            // `connect_plain` returns — there is no window in which a driver
            // `notify_waiters()` could strand the caller.
            assert!(
                client.is_connected(),
                "moonpool must return an already-handshaked connection: the inline handshake is \
                 what removes the notification park the tokio engine has to get right"
            );

            client.close().await;
        })
        .await;
}
