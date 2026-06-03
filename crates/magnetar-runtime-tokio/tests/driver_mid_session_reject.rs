// SPDX-License-Identifier: Apache-2.0

//! Layer (b) of the ADR-0024 four-layer policy for the driver
//! re-entrant-mutex deadlock fix (ADR-0038): the tokio integration mirror
//! of `magnetar-runtime-moonpool/tests/driver_mid_session_reject.rs`.
//!
//! ## What this pins
//!
//! The deadlock lived in the engines' driver read loop: the `shared.inner`
//! `parking_lot::Mutex` guard returned by `lock()` in the
//! `if let Err(_) = lock().handle_bytes_owned(..)` scrutinee outlived the
//! consequent block, so the error arm's `shared.inner.lock()` re-entered
//! the same non-reentrant mutex and self-deadlocked the driver task. The
//! tokio engine carried the identical latent bug; this test pins its fix.
//!
//! A loopback broker completes the handshake (`CONNECT` → `CONNECTED`) and
//! then pushes one **malformed** frame — a 4-byte big-endian
//! `total_size = 0` prefix, which `peek_full_frame_len` rejects with
//! `FrameError::BadLength(0)` (layer (a) pins that proto contract). The
//! non-supervised driver (`Client::from_socket`) must drive that reject
//! down its error arm, `mark_disconnected()`, and **terminate** the task
//! with `ClientError::Protocol` — not self-deadlock.
//!
//! The `tokio::time::timeout` around `DriverHandle::join` is the
//! regression guard: pre-fix the driver task parks forever on the
//! re-entrant lock and the join never resolves, so the timeout elapsing
//! turns a deadlock into a clean test failure rather than a wedged suite.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): one `#[tokio::test]` here mirrors the
//! moonpool file's one `#[test]`.

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, ProtocolError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Beat the broker waits after acking the handshake before injecting the
/// malformed frame. Comfortably longer than the (sub-millisecond) handshake
/// processing, so `from_socket` has returned `Connected` and the reject
/// lands strictly mid-session — never during the handshake (which would
/// fail `from_socket` instead).
const SETTLE_DELAY: Duration = Duration::from_millis(300);

/// Serve one session on the accepted stream: reply `CONNECTED` to the
/// handshake, settle, push a single malformed frame, then hold the socket
/// open (draining reads) so the client observes the *reject*, not a clean
/// EOF.
async fn serve_handshake_then_malformed(mut stream: TcpStream) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut connected = false;
    let mut malformed_sent = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            if let Ok(pb::base_command::Type::Connect) =
                pb::base_command::Type::try_from(frame.command.r#type)
            {
                encode_connected(&mut out_buf);
                connected = true;
            }
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        // Handshake acked + flushed → settle, then inject exactly one
        // malformed frame (4-byte big-endian `total_size = 0`, which the
        // client's `peek_full_frame_len` rejects with `BadLength(0)`).
        if connected && !malformed_sent {
            malformed_sent = true;
            tokio::time::sleep(SETTLE_DELAY).await;
            stream.write_all(&[0u8; 4]).await?;
            stream.flush().await?;
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn encode_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test-broker".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Drive the real tokio driver loop against a broker that injects a
/// malformed frame mid-session; assert the driver terminates with the
/// framing reject instead of self-deadlocking on the re-entrant
/// `shared.inner` lock (ADR-0038).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_malformed_mid_session_frame_terminates_driver_not_deadlock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();

    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            let _ = serve_handshake_then_malformed(stream).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    // Non-supervised: the driver exits on the first failure rather than
    // re-dialling, so the malformed-frame reject is directly observable as
    // the driver's terminal error.
    let client = Client::from_socket(stream, ConnectionConfig::default())
        .await
        .expect("handshake completes before the malformed frame");

    let driver = client.take_driver().expect("driver handle");

    // The crux: pre-fix `join()` never resolves (the driver task parked
    // forever on the re-entrant `shared.inner` lock), so the timeout would
    // elapse and `expect` would fail loudly instead of the suite hanging.
    let terminal = tokio::time::timeout(Duration::from_secs(5), driver.join())
        .await
        .expect("driver must TERMINATE on a malformed mid-session frame, not self-deadlock");

    match terminal {
        Err(ClientError::Protocol(ProtocolError::Frame(FrameError::BadLength(0)))) => {}
        other => panic!("driver must terminate with the BadLength framing reject, got {other:?}"),
    }

    broker.abort();
}
