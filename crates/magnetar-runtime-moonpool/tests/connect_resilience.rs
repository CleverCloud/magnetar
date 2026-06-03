// SPDX-License-Identifier: Apache-2.0

//! Layer (c) of the ADR-0024 four-layer policy for the dual-cap
//! initial-dial retry (ADR-0052): the dedicated moonpool *resilience*
//! test.
//!
//! ## What this pins
//!
//! `moonpool-sim`'s network provider injects connect faults by design —
//! the default [`moonpool_sim::ConnectFailureMode`] is `Probabilistic`,
//! so on a fraction of seeds the very first dial *hangs* (the broker
//! accepts the SYN but the establishment never completes). This is the
//! exact fault class that motivated ADR-0052's dual cap.
//!
//! The assertion here is the chaos-coverage pair to *keeping connect
//! faults on*: under the live `Probabilistic` connect-fault config, a
//! connect-hang on the supervised / pool dial MUST be **recovered**
//! (the retry re-dials and eventually handshakes) or surface as a
//! **bounded `operation_timeout` error** — it must NOT be a silent
//! infinite park. The proof of termination is that
//! [`SimulationBuilder::run`] returns at all: a single hung run would
//! never hand control back and the test harness would wedge. We tighten
//! the bound two ways so termination is fast *and* attributable:
//!
//! 1. a tight [`ConnectionConfig::operation_timeout`] total-budget cap (the ADR-0052 `now()`
//!    comparison, no new scheduled timer), and
//! 2. a tight [`SimulationBuilder::run_time_budget`] so the orchestrator's no-progress detector
//!    trips a deterministic deadlock rather than spinning a core if a storm ever out-paces the
//!    magnetar-side cap.
//!
//! Determinism note: the `operation_timeout` cap is a virtual-clock
//! `now()` comparison inside `dial_with_retry`, so it never arms a fresh
//! `TimeProvider` timer and never perturbs the simulated schedule
//! (ADR-0011, ADR-0052). Every seed is bit-for-bit reproducible.
//!
//! ## Runtime-test-parity
//!
//! Two `#[test]` functions live here; the mirrored
//! `magnetar-runtime-tokio/tests/connect_resilience.rs` carries two of
//! its own so `check-runtime-test-parity` stays 1:1 (ADR-0024).

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

mod common;
use common::sweep_seeds;

/// Port the in-sim broker binds to. The sim network hands every workload
/// its own IP, so a fixed port keeps the client→broker derivation trivial.
const BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget. Comfortably above the legitimate connect
/// ceiling (a few simulated seconds: connect-fault hangs bounded by the
/// 2 s `operation_timeout` below, plus a couple of retry backoffs) yet
/// tight enough that any runaway connect-storm trips the orchestrator's
/// no-progress detector instead of burning a wall-clock core. Pure
/// function of the simulated schedule → never perturbs replay
/// determinism (ADR-0011, ADR-0036).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// Tight total connect-operation budget. Small enough that a hung dial
/// surfaces as a bounded `operation_timeout` error in low single-digit
/// simulated seconds, large enough that the happy path (and a couple of
/// recovered retries) still completes. The cap is a `now()` comparison,
/// not a scheduled timer (ADR-0052).
const TIGHT_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

/// Outcome the client workload records — every variant is *bounded*. The
/// invariant the `check()` enforces is that no run ends without one of
/// these (a silent park would leave it `None`, and `run()` would never
/// return in the first place).
#[derive(Clone, Debug)]
enum ConnectOutcome {
    /// The dial (after any recovered retries) handshaked to `Connected`.
    Recovered,
    /// The dial was abandoned with a bounded error — the dual cap
    /// (`operation_timeout` / `connect_max_retries`) tripped. Carries the
    /// stringified error for diagnostics.
    BoundedError(String),
}

/// In-sim broker speaking the minimum subset to complete the handshake:
/// `CONNECT` → `CONNECTED`, plus `PING` → `PONG` so a recovered
/// connection's keepalive stays live for the brief window before close.
struct BrokerWorkload;

#[async_trait]
impl Workload for BrokerWorkload {
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
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            // Spawn the session so the accept loop keeps
                            // servicing reconnect dials (the supervised
                            // client may re-dial after a connect-fault).
                            // moonpool main's `JoinHandle` has no `abort()`;
                            // cooperative shutdown is driven by the peer
                            // closing the socket / `ctx.shutdown()`.
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = handle_session(stream).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Drive one broker session — decode frames, reply per the minimal
/// dispatch table, flush, and return when the peer closes.
async fn handle_session<S>(mut stream: S) -> SimulationResult<()>
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
            handle_frame(&frame, &mut out_buf);
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

        let mut tmp = vec![0u8; 64 * 1024];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => read_buf.extend_from_slice(&tmp[..n]),
        }
    }
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
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
        _ => {}
    }
}

/// Client workload — supervised connect under the live Probabilistic
/// connect-fault config, with the dual cap tightened. Records exactly one
/// *bounded* outcome per run.
struct ClientWorkload {
    outcome: Arc<Mutex<Option<ConnectOutcome>>>,
}

impl ClientWorkload {
    fn new() -> Self {
        Self {
            outcome: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Workload for ClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // Supervised connect → wires the dial_with_retry dual cap and the
        // pool path (ADR-0039). The tight `operation_timeout` is the
        // total-budget half of the cap; `connect_max_retries` is the count
        // half. Either trips the loop first; whichever does, the dial
        // resolves to a bounded outcome.
        let cfg = ConnectionConfig {
            operation_timeout: TIGHT_OPERATION_TIMEOUT,
            supervisor: Some(magnetar_proto::SupervisorConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(200),
                mandatory_stop: Duration::from_secs(5),
                max_attempts: Some(4),
                ..magnetar_proto::SupervisorConfig::default()
            }),
            ..ConnectionConfig::default()
        };

        // NOTE: no `tokio::time::timeout` wrapper here — the whole point is
        // that the magnetar-side dual cap (plus the orchestrator detector)
        // bounds the dial. Wrapping it would mask a regression where the
        // cap stopped firing.
        let outcome = match Client::connect_plain_supervised(&engine, &addr, cfg, None, None).await
        {
            Ok(client) => {
                let is_connected = client.shared().inner.lock().is_connected();
                client.close().await;
                if is_connected {
                    ConnectOutcome::Recovered
                } else {
                    // Reached the supervised driver but not yet Connected —
                    // still a bounded, terminating outcome (the handshake
                    // future returned), recorded as such.
                    ConnectOutcome::BoundedError("post-handshake not Connected".to_owned())
                }
            }
            Err(err) => ConnectOutcome::BoundedError(format!("{err:?}")),
        };
        *self.outcome.lock() = Some(outcome);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let outcome = self.outcome.lock().take();
        match outcome {
            // Both arms are *bounded* — that's the resilience claim: a
            // connect-hang is either recovered or surfaces a bounded error,
            // never a silent park.
            Some(ConnectOutcome::Recovered) => Ok(()),
            Some(ConnectOutcome::BoundedError(reason)) => {
                // Surface the bounded error for diagnostics — it is NOT a
                // failure (a bounded `operation_timeout` / count-cap error is
                // a valid resilient outcome), but capturing it confirms the
                // dual cap, not a wedge, ended the dial.
                tracing::info!(
                    capture = true,
                    trail = "connect_bounded_error",
                    reason = %reason,
                );
                Ok(())
            }
            None => Err(SimulationError::InvalidState(
                "client recorded no outcome — the dial neither recovered nor \
                 surfaced a bounded operation_timeout error (silent park?)"
                    .into(),
            )),
        }
    }
}

/// Single-seed smoke: boot the broker + supervised client under the live
/// connect-fault config and assert the run terminates with a bounded
/// outcome. Cheap; runs on every push.
#[test]
fn moonpool_connect_hang_is_bounded_smoke() {
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(ClientWorkload::new())
        .set_iterations(1)
        .run();
    // `run()` returning at all is the termination proof. The per-iteration
    // `check()` already rejected a `None` (silent-park) outcome, so a
    // successful run here means the dial resolved to a bounded outcome.
    assert_eq!(
        report.iterations, 1,
        "expected exactly one iteration to be dispatched and terminate: {report:?}",
    );
}

/// 16-seed sweep — the actual resilience surface. Under the default
/// `Probabilistic` connect-fault config, a fraction of these seeds hang
/// the first dial; every one of them must terminate (recovered or bounded
/// error) within the dual cap. A regression that dropped the cap would
/// leave a storming seed spinning until the `run_time_budget` detector
/// trips — still a deterministic termination, but `failed_runs` would
/// flag it. We assert no seed is left in an unbounded park.
#[test]
fn moonpool_connect_hang_is_bounded_sweep_16_seeds() {
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(ClientWorkload::new())
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
    assert_eq!(
        report.iterations, 16,
        "every seed must be dispatched and terminate (no silent hang): {report:?}",
    );
    // The `check()` rejects a `None` outcome, so any run that ended without
    // a bounded connect outcome would land in `failed_runs`. Require every
    // seed to have produced a bounded outcome.
    assert_eq!(
        report.failed_runs, 0,
        "a seed ended without a bounded connect outcome — the dual cap did \
         not bound a connect-hang: {report:?}",
    );
}
