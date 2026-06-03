// SPDX-License-Identifier: Apache-2.0

//! Layer (c) of the ADR-0024 four-layer policy for the driver
//! re-entrant-mutex deadlock fix (ADR-0038): the moonpool integration
//! mirror of `magnetar-runtime-tokio/tests/driver_mid_session_reject.rs`.
//!
//! ## What this pins
//!
//! The deadlock lived in the engines' driver read loop: the `shared.inner`
//! `parking_lot::Mutex` guard returned by `lock()` in the
//! `if let Err(_) = lock().handle_bytes_owned(..)` scrutinee outlived the
//! consequent block, so the error arm's `shared.inner.lock()` re-entered
//! the same non-reentrant mutex and self-deadlocked the driver task. The
//! only trigger is a frame the proto state machine *rejects* mid-session
//! (`handle_bytes_owned` → `Err`) — exactly what swizzle-clog seeds
//! 0x56201ccaba82dbc1 (#65) / 0xdc638c565234d23f (#136) reorder into.
//!
//! This test drives the real driver loop deterministically: the in-sim
//! broker completes the handshake (`CONNECT` → `CONNECTED`), waits a beat
//! of **virtual** time so the client has fully settled into `Connected`
//! (so the reject lands strictly mid-session, never during the handshake),
//! then pushes one **malformed** frame — a 4-byte big-endian
//! `total_size = 0` prefix, which `peek_full_frame_len` rejects with
//! `FrameError::BadLength(0)` (layer (a) pins that proto contract). The
//! non-supervised driver (`Client::connect_plain`) must drive that reject
//! down its error arm, `mark_disconnected()`, and **terminate** the task
//! with `EngineError::Protocol` — not self-deadlock.
//!
//! Under `moonpool-sim` a self-deadlock parks the single simulator thread
//! inside `parking_lot::RawMutex::lock_slow`, so a regression would wedge
//! the run and `SimulationBuilder::run` would never return (the test
//! process hangs — caught in CI). With the fix, `DriverHandle::join`
//! resolves with the bounded protocol error and the run terminates.
//!
//! ## Runtime-test-parity
//!
//! One `#[test]` here, mirrored 1:1 by the single `#[tokio::test]` in
//! `magnetar-runtime-tokio/tests/driver_mid_session_reject.rs`, so
//! `check-runtime-test-parity` stays balanced (ADR-0024).

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{ConnectionConfig, FrameError, ProtocolError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, EngineError, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

/// Port the in-sim broker binds to (the sim hands every workload its own IP).
const BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget. The legitimate path here is a handshake, a
/// short settle delay, and one rejected frame — well under a simulated
/// second — so a generous 30 s ceiling still trips the orchestrator's
/// no-progress detector on a runaway, while a `parking_lot` self-deadlock
/// (which blocks the sim thread outright) wedges the process regardless.
/// Pure function of the simulated schedule → never perturbs replay
/// determinism (ADR-0011).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// Virtual-time beat the broker waits after acking the handshake before
/// injecting the malformed frame. Long enough that `connect_plain` has
/// returned `Connected` (the `CONNECTED` bytes are flushed *before* this
/// sleep), so the reject is unambiguously mid-session.
const SETTLE_DELAY: Duration = Duration::from_millis(300);

/// What the client workload observed for the driver after the broker
/// pushed the malformed frame. The `check()` rejects a `None` (the driver
/// neither terminated nor surfaced an error — i.e. it self-deadlocked and
/// only the wedge / `run()` never returning would have shown it).
#[derive(Clone, Debug)]
enum DriverOutcome {
    /// The driver task terminated with the expected protocol reject.
    RejectedAndTerminated,
    /// The driver terminated with some *other* error — still bounded, but
    /// flagged so a future regression that changes the reject mapping is
    /// visible rather than silently green.
    OtherError(String),
    /// The driver terminated cleanly (`Ok`) — unexpected for a malformed
    /// frame; recorded so the `check()` can fail loudly.
    CleanExit,
}

/// In-sim broker: handshake, settle, then inject exactly one malformed frame.
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
        let time = ctx.providers().time().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let time = time.clone();
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = handle_session(stream, time).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Drive one broker session: reply `CONNECTED`, wait `SETTLE_DELAY` of
/// virtual time so the client is firmly `Connected`, push a single
/// malformed frame, then keep the socket open (draining reads) so the
/// client observes the *reject*, not a clean EOF.
async fn handle_session<S, T>(mut stream: S, time: T) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
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
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        // Handshake acked + flushed → settle, then inject exactly one
        // malformed frame (4-byte big-endian `total_size = 0`, which the
        // client's `peek_full_frame_len` rejects with `BadLength(0)`).
        if connected && !malformed_sent {
            malformed_sent = true;
            let _ = time.sleep(SETTLE_DELAY).await;
            if stream.write_all(&[0u8; 4]).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
        }

        let mut tmp = vec![0u8; 64 * 1024];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => read_buf.extend_from_slice(&tmp[..n]),
        }
    }
}

fn encode_connected(out: &mut BytesMut) {
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

/// Client workload: connect (non-supervised), then join the driver and
/// record how it terminated. With the deadlock present, `join()` would
/// never resolve — the sim thread would be parked in the re-entrant lock —
/// so reaching `check()` at all already proves termination.
struct ClientWorkload {
    outcome: Arc<Mutex<Option<DriverOutcome>>>,
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

        // Non-supervised: the driver exits on the first failure rather than
        // re-dialling, so the malformed-frame reject is directly observable
        // as the driver's terminal error.
        let client = Client::connect_plain(&engine, &addr, ConnectionConfig::default())
            .await
            .map_err(|e| SimulationError::InvalidState(format!("connect_plain: {e:?}")))?;

        let driver = client
            .take_driver()
            .ok_or_else(|| SimulationError::InvalidState("driver handle already taken".into()))?;

        // This await is the crux: pre-fix it would park forever (the driver
        // task self-deadlocked on the re-entrant `shared.inner` lock).
        let outcome = match driver.join().await {
            Err(EngineError::Protocol(ProtocolError::Frame(FrameError::BadLength(0)))) => {
                DriverOutcome::RejectedAndTerminated
            }
            Err(other) => DriverOutcome::OtherError(format!("{other:?}")),
            Ok(()) => DriverOutcome::CleanExit,
        };
        *self.outcome.lock() = Some(outcome);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        match self.outcome.lock().take() {
            Some(DriverOutcome::RejectedAndTerminated) => Ok(()),
            Some(DriverOutcome::OtherError(reason)) => Err(SimulationError::InvalidState(format!(
                "driver terminated, but with an unexpected error (the malformed frame must surface \
                 as a BadLength protocol reject): {reason}"
            ))),
            Some(DriverOutcome::CleanExit) => Err(SimulationError::InvalidState(
                "driver exited cleanly on a malformed mid-session frame — it must surface the \
                 framing reject"
                    .into(),
            )),
            None => Err(SimulationError::InvalidState(
                "client recorded no driver outcome — the driver neither terminated nor surfaced \
                 the reject (re-entrant-mutex self-deadlock?)"
                    .into(),
            )),
        }
    }
}

/// Drive the real moonpool driver loop against a broker that injects a
/// malformed frame mid-session; assert the driver terminates with the
/// framing reject instead of self-deadlocking on the re-entrant
/// `shared.inner` lock (ADR-0038).
#[test]
fn moonpool_malformed_mid_session_frame_terminates_driver_not_deadlock() {
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(ClientWorkload::new())
        .set_iterations(1)
        .run();
    // `run()` returning at all is the termination proof: a re-entrant-lock
    // deadlock would have wedged the sim thread. `check()` additionally
    // pins that the driver surfaced the BadLength reject.
    assert_eq!(
        report.iterations, 1,
        "the run must dispatch and terminate (no self-deadlock): {report:?}",
    );
    assert_eq!(
        report.failed_runs, 0,
        "the driver must surface the malformed-frame reject and terminate: {report:?}",
    );
}
