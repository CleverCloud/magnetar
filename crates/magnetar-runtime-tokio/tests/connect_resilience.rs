// SPDX-License-Identifier: Apache-2.0

//! Layer (b) of the ADR-0024 four-layer policy for the dual-cap
//! initial-dial retry (ADR-0052): the tokio integration mirror of
//! `magnetar-runtime-moonpool/tests/connect_resilience.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): two `#[tokio::test]` functions here
//! mirror the moonpool file's two `#[test]` functions.
//!
//! ## What this pins
//!
//! The tokio engine consumes the same
//! [`ConnectionConfig::connect_max_retries`] /
//! [`ConnectionConfig::operation_timeout`] dual cap as the moonpool
//! engine, on real wall time (the elapsed half is a
//! [`std::time::Instant`] comparison). Two shapes:
//!
//! 1. **Retry-then-resolve** — the broker port is initially closed (the dial gets
//!    `ConnectionRefused`, a transient `Io` error). A delayed task binds the port and serves the
//!    handshake; a later retry attempt connects and `Client::connect` resolves to a live client.
//!    This is the production analogue of the moonpool "connect-hang recovered" arm.
//! 2. **Fail-fast** — `connect_max_retries = 0` means a single dial attempt with no retry, so a
//!    closed port surfaces a bounded `ClientError::Io` immediately. This is the count-cap edge the
//!    proto unit test pins as a config and the moonpool sweep exercises as a bounded error.

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Reserve a loopback port by binding then dropping the listener. The
/// port is *very* likely still free when the delayed broker re-binds it
/// (loopback, no `SO_REUSEADDR` contention in-test); the connect-retry
/// loop absorbs the rare race by simply re-dialling.
async fn reserve_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve bind");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

/// Serve exactly one handshake on `127.0.0.1:{port}`: read until the
/// inbound `CommandConnect`, reply with `CommandConnected`, then keep the
/// socket open briefly so the client observes `Connected`.
async fn serve_one_handshake(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let (stream, _peer) = listener.accept().await?;
    handle_handshake_session(stream).await
}

/// Per-session script: read until `CommandConnect`, reply `CommandConnected`,
/// then service `PING` → `PONG` until the peer closes.
async fn handle_handshake_session(mut stream: TcpStream) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
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
            reply_to_frame(&frame, &mut out_buf);
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn reply_to_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
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
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        _ => {}
    }
}

/// Mirror of `moonpool_connect_hang_is_bounded_smoke`'s *recovered* arm.
/// The first dial(s) hit a closed port (`ConnectionRefused`, transient
/// `Io`); the dual-cap retry loop re-dials, and once the delayed broker
/// binds the port a later attempt connects + handshakes. `Client::connect`
/// must resolve to a live client rather than propagate the early refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_connect_retries_until_broker_listens() {
    let port = reserve_loopback_port().await;
    let url = format!("pulsar://127.0.0.1:{port}");

    // Bind the broker only after a short delay so the first dial attempt(s)
    // get ConnectionRefused. 150 ms comfortably spans the first couple of
    // 50 ms-doubling backoff steps without exhausting the 8-retry / 30 s
    // dual cap.
    let broker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = serve_one_handshake(port).await;
    });

    let client = tokio::time::timeout(
        Duration::from_secs(10),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not exceed the test timeout")
    .expect("connect must succeed once the broker starts listening (retry path)");

    assert!(
        client.is_connected(),
        "client must reach Connected after the retried dial handshakes",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    broker.abort();
}

/// Mirror of the moonpool sweep's *bounded-error* arm. With
/// `connect_max_retries = 0` the dial is attempted exactly once; a closed
/// port surfaces a bounded `ClientError::Io` immediately instead of
/// retrying. Proves the count cap is honoured (fail-fast), the dual-cap
/// counterpart to the recovered path above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_connect_zero_retries_fails_fast() {
    // A reserved-then-dropped port — nothing is listening, so the single
    // dial attempt gets ConnectionRefused.
    let port = reserve_loopback_port().await;
    let url = format!("pulsar://127.0.0.1:{port}");

    let cfg = ConnectionConfig {
        connect_max_retries: 0,
        ..ConnectionConfig::default()
    };

    let result = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, cfg))
        .await
        .expect("fail-fast connect must not approach the connect_timeout / test bound");

    let err =
        result.expect_err("connect with connect_max_retries=0 against a closed port must fail");
    assert!(
        matches!(err, ClientError::Io(_)),
        "fail-fast refusal must surface as a bounded ClientError::Io, got {err:?}",
    );
}
