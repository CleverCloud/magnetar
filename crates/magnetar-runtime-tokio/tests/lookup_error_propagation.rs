// SPDX-License-Identifier: Apache-2.0

//! Lookup-error propagation — tokio engine, real loopback.
//!
//! Mirror of `magnetar-runtime-moonpool/tests/lookup_error_propagation.rs`
//! (deterministic simulation). Maintains the tokio ↔ moonpool 1:1 test count
//! required by ADR-0024 (`check-runtime-test-parity`): two `#[tokio::test]`
//! functions here mirror the moonpool file's two `#[test]` functions.
//!
//! ## Coverage gap this pins
//!
//! The existing `lookup_redirect_chain.rs` pair covers a redirect chain that
//! *settles* and the redirect-cap diagnostic. What was *not* covered is the
//! two ways a `CommandLookupTopic` round-trip terminates in a **bounded
//! `ClientError::Broker`** rather than a hang:
//!
//! 1. **Broker-originated `Failed`** — the broker answers the LOOKUP with `LookupType::Failed`
//!    carrying an explicit `ServerError` code + message. `lookup_topic` (driven here through the
//!    public `open_producer` surface, since tokio's `lookup_topic` is private) must surface
//!    [`ClientError::Broker`] with the broker's verbatim code + message — not park the
//!    producer-open future forever.
//! 2. **Unbounded redirect loop** — the broker answers *every* LOOKUP with `LookupType::Redirect`.
//!    The proto layer chases the chain up to [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops,
//!    then short-circuits to a bounded `ClientError::Broker` carrying the "redirect cap exceeded"
//!    diagnostic.
//!
//! The termination proof is that `open_producer` *resolves* under the
//! per-call `tokio::time::timeout`: a regression that dropped the `Failed`
//! translation or the redirect cap would leave the future parked and the
//! timeout would trip the `expect`.

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Topic the producer targets. The broker answers by frame kind, not by
/// topic; a realistic name keeps logs readable.
const TOPIC: &str = "persistent://public/default/lookup-error-propagation";

/// Broker-side `ServerError` code echoed on the `Failed` lookup response.
/// `TopicNotFound` is the canonical "this lookup cannot resolve" answer.
const FAILED_CODE: i32 = pb::ServerError::TopicNotFound as i32;

/// Broker-side message echoed on the `Failed` lookup response — must
/// round-trip verbatim into the engine-surfaced `ClientError::Broker`.
const FAILED_MESSAGE: &str = "topic does not exist";

/// How the broker should answer `CommandLookupTopic` frames.
#[derive(Clone, Copy)]
enum LookupBehavior {
    /// Answer the LOOKUP with `LookupType::Failed { error, message }`.
    Failed,
    /// Answer *every* LOOKUP with `LookupType::Redirect`, never resolving —
    /// drives the proto redirect cap.
    AlwaysRedirect,
}

/// Spawn a loopback broker that completes the handshake and answers LOOKUPs
/// per `behavior`. Returns the dialable `pulsar://` URL. The accept loop and
/// each session run on detached tasks so the broker keeps servicing the
/// client until the test drops the connection.
async fn spawn_lookup_broker(behavior: LookupBehavior) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("pulsar://{addr}");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = handle_session(stream, behavior).await;
            });
        }
    });
    url
}

/// Per-session script: complete the handshake, then answer LOOKUPs per
/// `behavior`. Service `PING` → `PONG` so the connection stays live.
async fn handle_session(mut stream: TcpStream, behavior: LookupBehavior) -> std::io::Result<()> {
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
            handle_frame(&frame, &mut out_buf, behavior);
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

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, behavior: LookupBehavior) {
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
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                let response = match behavior {
                    LookupBehavior::Failed => pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Failed as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: Some(FAILED_CODE),
                        message: Some(FAILED_MESSAGE.to_owned()),
                        proxy_through_service_url: Some(false),
                    },
                    LookupBehavior::AlwaysRedirect => pb::CommandLookupTopicResponse {
                        broker_service_url: Some("pulsar://hostile-redirect:6650".to_owned()),
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Redirect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    },
                };
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(response),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        _ => {}
    }
}

/// A broker-originated `LookupType::Failed` response must surface as a
/// bounded [`ClientError::Broker`] carrying the broker's `ServerError` code
/// AND verbatim message — `open_producer` resolves with an error instead of
/// parking forever waiting for a `Connect`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_failed_response_surfaces_bounded_broker_error() {
    let url = spawn_lookup_broker(LookupBehavior::Failed).await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out — the lookup must surface a bounded error")
    .expect_err("open_producer must fail when the LOOKUP answers Failed");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    match err {
        ClientError::Broker { code, message } => {
            assert_eq!(
                code, FAILED_CODE,
                "ClientError::Broker must carry the broker ServerError code",
            );
            assert_eq!(
                message, FAILED_MESSAGE,
                "ClientError::Broker must carry the verbatim broker message",
            );
        }
        other => {
            panic!("lookup Failed must surface as a bounded ClientError::Broker, got {other:?}")
        }
    }
}

/// A broker that answers *every* LOOKUP with `Redirect` must NOT hang
/// `open_producer`. The proto state machine chases the chain up to
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops and then
/// short-circuits to a bounded [`ClientError::Broker`] carrying the
/// "redirect cap exceeded" diagnostic — the redirect-loop `DoS` is bounded
/// end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_redirect_loop_surfaces_bounded_cap_error() {
    let url = spawn_lookup_broker(LookupBehavior::AlwaysRedirect).await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out — the redirect cap must bound the lookup")
    .expect_err("open_producer must fail when the redirect chain never resolves");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    let msg = format!("{err}");
    assert!(
        msg.contains("redirect cap exceeded"),
        "expected the redirect-cap diagnostic to be surfaced to the user, got: {msg}",
    );
}
