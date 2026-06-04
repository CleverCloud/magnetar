// SPDX-License-Identifier: Apache-2.0

//! Lookup-error propagation — moonpool engine, deterministic simulation.
//!
//! ## Coverage gap this pins
//!
//! The existing `lookup_redirect_chain.rs` pair covers a redirect chain that
//! *settles* (terminal `Connect`) and the redirect-cap diagnostic via
//! `open_producer`. What was *not* covered anywhere is the two ways a
//! `CommandLookupTopic` round-trip can terminate in a **bounded
//! `ClientError`** rather than a hang, observed directly on the public
//! [`Client::lookup_topic`] surface:
//!
//! 1. **Broker-originated `Failed`** — the broker answers the LOOKUP with `LookupType::Failed`
//!    carrying an explicit `ServerError` code + message. The proto state machine translates this to
//!    `LookupOutcome::Failed { code, message }`, and the moonpool engine must re-emit it verbatim
//!    as [`magnetar_runtime_moonpool::ClientError::Broker`] — not park the lookup future forever
//!    waiting for a `Connect` that never comes.
//! 2. **Unbounded redirect loop** — the broker answers *every* LOOKUP with `LookupType::Redirect`,
//!    never resolving. The proto layer chases the chain internally up to
//!    [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops, then short-circuits to
//!    `LookupOutcome::Failed { code: 0, message: "lookup redirect cap exceeded (..)" }`. The engine
//!    surfaces a bounded `ClientError::Broker` carrying the cap diagnostic — the proof the
//!    redirect-loop `DoS` is bounded end-to-end on the lookup surface itself.
//!
//! The termination proof in both cases is that the in-sim
//! [`Client::lookup_topic`] future *resolves* under the per-run time
//! budget: a regression that dropped the `Failed` translation or the
//! redirect cap would leave the future parked, the sweep-level capture
//! would stay `false`, and the assertion would fire.
//!
//! Mirrors `crates/magnetar-runtime-tokio/tests/lookup_error_propagation.rs`
//! (real loopback) to keep the tokio ↔ moonpool 1:1 test count required by
//! ADR-0024. The tokio side drives the same two shapes over the public
//! `open_producer` surface (tokio's `lookup_topic` is private); both engines
//! surface an identically-shaped bounded `ClientError::Broker`.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

/// Port the in-sim broker binds to. The sim network hands every workload its
/// own IP, so a fixed port keeps the client → broker derivation trivial.
const BROKER_PORT: u16 = 6650;

/// Topic the client looks up. Value is irrelevant — the broker answers by
/// frame kind, not by topic — but a realistic name keeps logs readable.
const TOPIC: &str = "persistent://public/default/lookup-error-propagation";

/// Broker-side `ServerError` code echoed on the `Failed` lookup response.
/// `TopicNotFound` is the canonical "this lookup cannot resolve" answer.
const FAILED_CODE: i32 = pb::ServerError::TopicNotFound as i32;

/// Broker-side message echoed on the `Failed` lookup response — must
/// round-trip verbatim into the engine-surfaced `ClientError::Broker`.
const FAILED_MESSAGE: &str = "topic does not exist";

/// Per-run virtual-time budget. Comfortably above the legitimate lookup
/// ceiling (handshake + one LOOKUP round-trip for the `Failed` case, or up
/// to `MAX_LOOKUP_REDIRECTS` round-trips for the redirect-loop case) yet
/// tight enough that any runaway lookup-park trips the orchestrator's
/// no-progress detector instead of burning a wall-clock core. Pure function
/// of the simulated schedule → never perturbs replay determinism
/// (ADR-0011, ADR-0036).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// How the broker should answer `CommandLookupTopic` frames.
#[derive(Clone, Copy)]
enum LookupBehavior {
    /// Answer the first LOOKUP with `LookupType::Failed { error, message }`.
    Failed,
    /// Answer *every* LOOKUP with `LookupType::Redirect`, never resolving —
    /// drives the proto redirect cap.
    AlwaysRedirect,
}

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

/// Drive one broker session: complete the handshake, then answer LOOKUPs per
/// `behavior`. Returns when the peer closes.
async fn handle_session<S>(mut stream: S, behavior: LookupBehavior) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
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
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
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
                    server_version: "magnetar-sim-broker".to_owned(),
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

/// In-sim broker that completes the handshake and answers LOOKUPs per
/// `behavior`. Accepts every inbound connection so the supervised /
/// non-supervised client gets a clean handshake before its lookup.
struct LookupBroker {
    behavior: LookupBehavior,
}

#[async_trait]
impl Workload for LookupBroker {
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
        let task = ctx.providers().task().clone();
        let behavior = self.behavior;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = handle_session(stream, behavior).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Client workload: dial the broker via `Client::connect_plain`, issue one
/// `lookup_topic`, and record the *bounded* `ClientError::Broker` it
/// surfaces. The workload itself never returns `Err` — the sweep-level
/// assertion in the `#[test]` is the authoritative gate (mirrors
/// `handshake_error_capture.rs`).
struct LookupErrorClient {
    /// Stringified `ClientError` from the lookup, captured cross-iteration so
    /// a regression surfaces the actual error shape rather than a generic
    /// "nothing was captured".
    captured_error: Arc<Mutex<Option<String>>>,
}

impl LookupErrorClient {
    fn new() -> Self {
        Self {
            captured_error: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Workload for LookupErrorClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // A timeout here means the sim budget never delivered the lookup
        // resolution; the sweep-level assertion is the authoritative gate.
        // No `tokio::time::timeout` wrapper on the lookup itself — the whole
        // point is that the proto layer bounds it (Failed translation /
        // redirect cap). Wrapping it would mask a regression where the bound
        // stopped firing.
        let connect = tokio::time::timeout(
            Duration::from_secs(20),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await;
        let Ok(Ok(client)) = connect else {
            return Ok(());
        };

        let outcome = client.lookup_topic(TOPIC, false).await;
        if let Err(ref err) = outcome {
            *self.captured_error.lock() = Some(format!("{err:?}"));
        }
        client.close().await;
        Ok(())
    }
}

/// 4-seed sweep: a broker-originated `LookupType::Failed` response must
/// surface as a bounded [`magnetar_runtime_moonpool::ClientError::Broker`]
/// carrying the broker's `ServerError` code AND verbatim message — the lookup
/// future resolves with an error instead of parking forever waiting for a
/// `Connect`.
#[test]
fn lookup_failed_response_surfaces_bounded_broker_error() {
    let client = LookupErrorClient::new();
    let captured = client.captured_error.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker {
            behavior: LookupBehavior::Failed,
        })
        .workload(client)
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let err = captured.lock().clone();
    let err = err.expect(
        "lookup against a Failed response must resolve to a bounded ClientError — \
         the future parked instead of surfacing the broker error",
    );
    // The broker's `ServerError` code and verbatim message must both ride
    // the surfaced `ClientError::Broker { code, message }`. `Debug` for the
    // `Broker` variant renders both fields, so substring checks are stable.
    assert!(
        err.contains(&FAILED_CODE.to_string()),
        "ClientError must carry the broker ServerError code {FAILED_CODE} (got {err:?}, \
         report={report:?})",
    );
    assert!(
        err.contains(FAILED_MESSAGE),
        "ClientError must carry the verbatim broker message \"{FAILED_MESSAGE}\" \
         (got {err:?}, report={report:?})",
    );
}

/// 4-seed sweep: a broker that answers *every* LOOKUP with `Redirect` must
/// NOT hang the lookup. The proto state machine chases the chain up to
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops and then
/// short-circuits to a bounded
/// [`magnetar_runtime_moonpool::ClientError::Broker`] carrying the
/// "redirect cap exceeded" diagnostic — proving the redirect-loop `DoS` is
/// bounded end-to-end on the public lookup surface.
#[test]
fn lookup_redirect_loop_surfaces_bounded_cap_error() {
    let client = LookupErrorClient::new();
    let captured = client.captured_error.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker {
            behavior: LookupBehavior::AlwaysRedirect,
        })
        .workload(client)
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let err = captured.lock().clone();
    let err = err.expect(
        "an unbounded redirect loop must resolve to a bounded ClientError — \
         the redirect cap did not fire and the lookup parked",
    );
    assert!(
        err.contains("redirect cap exceeded"),
        "ClientError must carry the redirect-cap diagnostic (got {err:?}, report={report:?})",
    );
}
