// SPDX-License-Identifier: Apache-2.0

//! Handshake-error capture — moonpool engine, deterministic simulation.
//!
//! Pins the new `magnetar_proto::Connection::handshake_failure_reason`
//! enrichment: when the broker rejects `CommandConnect` (or
//! `CommandAuthChallenge`) with a `CommandError` and then tears the socket
//! down, the user-facing connect future must surface
//! [`EngineError::HandshakeFailed`] carrying the broker's `ServerError`
//! name + verbatim message — not the opaque `EngineError::PeerClosed`
//! that a raw transport drop would otherwise produce.
//!
//! Mirrors `crates/magnetar-runtime-tokio/tests/handshake_error_capture.rs`
//! (real loopback) to keep the tokio ↔ moonpool 1:1 test count required by
//! ADR-0024. This side is the canonical deterministic place for the
//! capture-then-drop ordering: the sim network's drop ordering is
//! reproducible from the seed, the tokio mirror only asserts the same
//! enrichment lands over a real socket.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, ClientError, EngineError, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

/// Port the broker workload binds to. The sim network gives every workload
/// its own IP; a fixed port keeps the client → broker address derivation
/// trivial.
const BROKER_PORT: u16 = 6650;

/// Broker-side message — must round-trip verbatim into the
/// engine-surfaced `EngineError::HandshakeFailed` payload.
const BROKER_MESSAGE: &str = "token expired";

/// Single-`poll_read` helper — appends what was read into `buf`, returns the
/// count (`0` on EOF).
async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

/// Per-session script: read the inbound `CommandConnect`, reply with a
/// `CommandError(AuthenticationError, "token expired")` and drop the socket.
async fn handle_reject_handshake_session<S>(mut stream: S) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut saw_connect = false;
    loop {
        // Try to decode any complete frame already buffered.
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
            if pb::base_command::Type::try_from(frame.command.r#type)
                == Ok(pb::base_command::Type::Connect)
            {
                saw_connect = true;
            }
        }

        if saw_connect {
            // Emit `CommandError(AuthenticationError, "token expired")` with
            // request_id = 0 — the broker does not correlate mid-handshake
            // CONNECT failures with any pending request, and the proto
            // layer is expected to capture the message regardless.
            let err = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: 0,
                    error: pb::ServerError::AuthenticationError as i32,
                    message: BROKER_MESSAGE.to_owned(),
                }),
                ..Default::default()
            };
            let mut out = BytesMut::new();
            let _ = encode_command(&mut out, &err);
            if stream.write_all(&out).await.is_err() {
                return Ok(());
            }
            let _ = stream.flush().await;
            // Drop the stream by returning — sim transport teardown follows.
            return Ok(());
        }

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

/// Broker workload: accepts the first connection, runs the
/// reject-and-drop script, and records whether the script ran. Cross-iteration
/// counter is the sweep-level proof the script fired.
struct RejectHandshakeBroker {
    sessions_handled: Arc<Mutex<u32>>,
}

impl RejectHandshakeBroker {
    fn new() -> Self {
        Self {
            sessions_handled: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for RejectHandshakeBroker {
    fn name(&self) -> &str {
        "broker"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let bind_addr = format!("{}:{BROKER_PORT}", ctx.my_ip());
        let listener = network
            .bind(&bind_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("broker bind: {e}")))?;

        let shutdown = ctx.shutdown().clone();
        let handled = self.sessions_handled.clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            *handled.lock() += 1;
                            let _handle = task.spawn_task(
                                "reject-handshake-session",
                                async move {
                                    let _ = handle_reject_handshake_session(stream).await;
                                },
                            );
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Client workload: dial the broker via `Client::connect_plain`, expect a
/// `HandshakeFailed` envelope carrying both the `ServerError` variant name
/// and the verbatim broker message. The sweep asserts at least one
/// iteration observed both substrings. `last_error` is kept so a
/// regression surfaces the actual error shape in the assertion message
/// instead of a generic "nothing was captured".
struct HandshakeFailureClient {
    saw_server_error: Arc<Mutex<bool>>,
    saw_broker_message: Arc<Mutex<bool>>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl HandshakeFailureClient {
    fn new() -> Self {
        Self {
            saw_server_error: Arc::new(Mutex::new(false)),
            saw_broker_message: Arc::new(Mutex::new(false)),
            last_error: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Workload for HandshakeFailureClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // Connect_plain (NOT supervised) — the handshake future surfaces the
        // error directly to the caller. A timeout here means the sim budget
        // never delivered the rejection; the sweep-level assertion is the
        // authoritative gate.
        let connect = tokio::time::timeout(
            Duration::from_secs(20),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await;
        let Ok(result) = connect else {
            return Ok(());
        };

        // The connect must fail with a `HandshakeFailed` carrying both the
        // ServerError variant name AND the verbatim broker message. The
        // workload itself never returns `Err` — the sweep-level
        // cross-iteration assertion in the `#[test]` is the authoritative
        // gate (mirrors the `supervised_redial` design). Returning `Err`
        // would mark the whole iteration as failed and zero its metrics,
        // which would hide a legitimate "rejection arrived later than
        // expected on this seed but earlier on another" pattern.
        if let Err(ref err) = result {
            *self.last_error.lock() = Some(format!("{err:?}"));
        }
        if let Err(ClientError::Engine(EngineError::HandshakeFailed(reason))) = result {
            if reason.contains("AuthenticationError") {
                *self.saw_server_error.lock() = true;
            }
            if reason.contains(BROKER_MESSAGE) {
                *self.saw_broker_message.lock() = true;
            }
        }
        Ok(())
    }
}

/// 4-seed sweep: under deterministic simulation a broker `CommandError`
/// arriving while the connection is in `ConnectSent` must be captured by
/// the proto layer and re-emitted by the moonpool engine as
/// [`EngineError::HandshakeFailed`] — carrying both the broker's
/// `ServerError` variant name AND the verbatim broker message — instead
/// of the opaque `EngineError::PeerClosed` a raw drop would produce.
#[test]
fn connect_plain_surfaces_handshake_failure_reason_from_broker_command_error() {
    let broker = RejectHandshakeBroker::new();
    let sessions_handled = broker.sessions_handled.clone();
    let client = HandshakeFailureClient::new();
    let saw_server_error = client.saw_server_error.clone();
    let saw_broker_message = client.saw_broker_message.clone();
    let last_error = client.last_error.clone();
    let report = SimulationBuilder::new()
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let handled = *sessions_handled.lock();
    assert!(
        handled >= 1,
        "broker must have handled at least one inbound handshake \
         (sessions_handled={handled}, report={report:?})",
    );
    let last = last_error.lock().clone();
    assert!(
        *saw_server_error.lock(),
        "HandshakeFailed reason must mention the ServerError variant \
         (\"AuthenticationError\") on at least one iteration \
         (last_error={last:?}, report={report:?})",
    );
    assert!(
        *saw_broker_message.lock(),
        "HandshakeFailed reason must carry the verbatim broker message \
         (\"{BROKER_MESSAGE}\") on at least one iteration \
         (last_error={last:?}, report={report:?})",
    );
}

// Confirm the trait bounds compose — `MoonpoolEngine<SimProviders>` must be a
// valid construction site. Compile-time-only.
#[allow(dead_code)]
fn _engine_sim_providers_compiles(providers: SimProviders) {
    let _engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
}
