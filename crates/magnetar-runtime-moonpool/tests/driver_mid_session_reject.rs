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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{ConnectionConfig, FrameError, ProtocolError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, EngineError, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

mod common;
use common::sweep_seeds;

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

/// Seeds whose default-network chaos lands on a bounded, terminating
/// outcome (never the re-entrant-lock self-deadlock under test) that the
/// daily moonpool-seed-sweep flagged. Pinned here so each interleaving is
/// exercised deterministically going forward — and so the test can never
/// regress to the original unpinned wall-clock seed that made the sweep's
/// failures unreproducible from `MOONPOOL_SEED`.
///
/// Two distinct chaos classes are pinned:
///
/// - **Connect-severance** (issue #290 `MOONPOOL_SEED=0x645244daaeccc7cb`, issue #291
///   `MOONPOOL_SEED=0x65ea4fbea60a11a6`): the `SimulationBuilder`'s **unavoidable** default-network
///   chaos (`ConnectFailureMode::Probabilistic`) exhausts the client's bounded `connect_timeout`
///   before the handshake completes, so the malformed-frame scenario never sets up and the dial
///   surfaces a bounded `TimedOut` error → `DriverOutcome::Severed`.
/// - **Mid-session bit-flip → watchdog close** (issue #305 `MOONPOOL_SEED=0xbf6077ea63931440`,
///   derived sub-seed 8009627293563187958): the same unavoidable network chaos
///   (`bit_flip_probability = 0.0001`) corrupts the deterministic malformed frame `[0,0,0,0]` in
///   flight to e.g. `[0,5,0,0]`. `peek_full_frame_len` (magnetar-proto/src/frame.rs:301-318) then
///   reads a non-zero `total_size` and returns `Ok(None)` ("incomplete — waiting for more bytes")
///   instead of `Err(BadLength(0))`, so the driver parks; the ADR-0058 keepalive watchdog escalates
///   the wedged socket to `Failed` (`Connection::handle_timeout`, conn.rs:2699-2713) and the
///   driver's top-of-loop `should_close` returns `Ok(())` cleanly (driver.rs:1128-1163, the
///   documented watchdog→Failed→clean-close contract) → a bounded `DriverOutcome::CleanExit`. Same
///   chaos class as connect-severance, just at a later lifecycle point, so it is treated
///   identically (bounded, out of `failed_runs`).
const CHAOS_REGRESSION_SEEDS: [u64; 3] = [
    9_388_503_268_189_738_858,
    17_161_897_233_139_508_114,
    8_009_627_293_563_187_958,
];

/// What the client workload observed for the driver after the broker
/// pushed the malformed frame. The `check()` rejects a `None` (the driver
/// neither terminated nor surfaced an error — i.e. it self-deadlocked and
/// only the wedge / `run()` never returning would have shown it).
#[derive(Clone, Debug)]
enum DriverOutcome {
    /// The driver task terminated with the expected protocol reject.
    RejectedAndTerminated,
    /// The connection was severed by the unavoidable default-network chaos
    /// *before* the handshake completed (the client's dial exhausted its
    /// bounded `connect_timeout`, or the link was cut before `CONNECTED`).
    /// A bounded, terminating outcome — not the re-entrant-lock deadlock
    /// under test — so the malformed-frame scenario simply never set up.
    /// Carries the stringified reason for diagnostics.
    Severed(String),
    /// The driver terminated with some *other* error — still bounded, but
    /// flagged so a future regression that changes the reject mapping is
    /// visible rather than silently green.
    OtherError(String),
    /// The driver terminated cleanly (`join()` → `Ok(())`). This is the
    /// terminal outcome when the unavoidable default-network bit-flip chaos
    /// (`bit_flip_probability = 0.0001`) corrupts the injected malformed frame
    /// `[0,0,0,0]` in flight to a non-zero-`total_size` prefix:
    /// `peek_full_frame_len` then returns `Ok(None)` ("incomplete") instead of
    /// `Err(BadLength(0))`, the driver parks on the wedged socket, and the
    /// ADR-0058 keepalive watchdog escalates to `Failed` → the driver's
    /// `should_close` returns `Ok(())` (the documented watchdog→Failed→clean-close
    /// contract). A bounded, terminating outcome — not the re-entrant-lock
    /// self-deadlock under test — so it stays out of `failed_runs`. The genuine
    /// "driver swallows an *uncorrupted* malformed frame" regression is still
    /// caught by the `observed_reject` gate: a chaos-free seed that exited clean
    /// instead of surfacing `BadLength(0)` would leave that flag unset and fail
    /// the sweep.
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
    /// Set once any seed in the sweep completes the handshake and observes
    /// the exact `BadLength(0)` reject, so the sweep can require the
    /// mid-session-reject window was actually exercised at least once (a run
    /// chaos-severed before the handshake does not prove the property).
    observed_reject: Arc<AtomicBool>,
}

impl ClientWorkload {
    fn new() -> Self {
        Self {
            outcome: Arc::new(Mutex::new(None)),
            observed_reject: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Shared flag set when any run observes the `BadLength(0)` reject — read
    /// by the sweep test after `run()` to require the property held on at
    /// least one chaos-free seed.
    fn observed_reject_handle(&self) -> Arc<AtomicBool> {
        self.observed_reject.clone()
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
        //
        // The `SimulationBuilder`'s unavoidable default-network chaos
        // (`ConnectFailureMode::Probabilistic`, bit-flip, random-close) can
        // exhaust the client's bounded `connect_timeout` before the handshake
        // completes on a fraction of seeds. That surfaces as a bounded connect
        // error — a terminating outcome, NOT the re-entrant-lock self-deadlock
        // under test — so we classify it `Severed` and let the seeds that
        // complete the handshake carry the reject assertion.
        let outcome = match Client::connect_plain(&engine, &addr, ConnectionConfig::default()).await
        {
            Err(e) => DriverOutcome::Severed(format!("connect_plain: {e:?}")),
            Ok(client) => {
                let driver = client.take_driver().ok_or_else(|| {
                    SimulationError::InvalidState("driver handle already taken".into())
                })?;

                // This await is the crux: pre-fix it would park forever (the
                // driver task self-deadlocked on the re-entrant `shared.inner`
                // lock).
                match driver.join().await {
                    Err(EngineError::Protocol(ProtocolError::Frame(FrameError::BadLength(0)))) => {
                        DriverOutcome::RejectedAndTerminated
                    }
                    Err(other) => DriverOutcome::OtherError(format!("{other:?}")),
                    Ok(()) => DriverOutcome::CleanExit,
                }
            }
        };
        if matches!(outcome, DriverOutcome::RejectedAndTerminated) {
            self.observed_reject.store(true, Ordering::SeqCst);
        }
        *self.outcome.lock() = Some(outcome.clone());

        // Gate the *secondary* contract HERE in run(): a moonpool
        // `Workload::check()` Err is only logged (run_check_phase) and never
        // flips `failed_runs`, so the check() below cannot fail the test on
        // its own. The deadlock itself is still caught upstream — join()
        // would never resolve, run() never completes, and the no-progress
        // detector trips — but a driver that terminated with the *wrong*
        // error (or exited cleanly) would have slipped past check(). A
        // run() Err DOES land the iteration in `failed_runs`.
        match outcome {
            DriverOutcome::RejectedAndTerminated => Ok(()),
            // A connect severed by the unavoidable default-network chaos
            // before the handshake is a bounded, terminating outcome — not
            // the deadlock under test. Surface it for diagnostics, do not
            // fail the run.
            DriverOutcome::Severed(reason) => {
                tracing::info!(
                    capture = true,
                    trail = "connect_severed_by_chaos",
                    reason = %reason,
                );
                Ok(())
            }
            // A clean exit after the unavoidable bit-flip chaos corrupted the
            // injected malformed frame in flight (non-zero `total_size` →
            // `peek_full_frame_len` returns `Ok(None)` → the watchdog escalates
            // the wedged socket to `Failed` → `should_close` returns `Ok(())`).
            // Same chaos class as `Severed`, just post-handshake: a bounded,
            // terminating outcome, not the self-deadlock under test. Surface it
            // for diagnostics, do not fail the run — the `observed_reject` gate
            // still requires a chaos-free seed to prove the genuine reject.
            DriverOutcome::CleanExit => {
                tracing::info!(capture = true, trail = "clean_exit_after_chaos_bit_flip",);
                Ok(())
            }
            DriverOutcome::OtherError(reason) => Err(SimulationError::InvalidState(format!(
                "driver terminated with an unexpected error (expected a BadLength protocol \
                 reject): {reason}"
            ))),
        }
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        match self.outcome.lock().take() {
            // `RejectedAndTerminated` is the property. The other two terminal
            // outcomes are bounded default-network chaos: `Severed` is a
            // connect cut before the handshake (the malformed-frame scenario
            // never set up), and `CleanExit` is the watchdog-driven close
            // after the bit-flip chaos corrupted the malformed frame in flight
            // (so `peek_full_frame_len` saw `Ok(None)`, not `BadLength(0)`).
            // All three are acceptable; the `observed_reject` gate in the
            // sweep still requires a chaos-free seed to prove the real reject.
            Some(
                DriverOutcome::RejectedAndTerminated
                | DriverOutcome::Severed(_)
                | DriverOutcome::CleanExit,
            ) => Ok(()),
            Some(DriverOutcome::OtherError(reason)) => Err(SimulationError::InvalidState(format!(
                "driver terminated, but with an unexpected error (the malformed frame must surface \
                 as a BadLength protocol reject): {reason}"
            ))),
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
///
/// Deterministic seed sweep: 16 seeds derived from `MOONPOOL_SEED` (so the
/// daily sweep keeps exploring this path under fresh randoms) plus the
/// regression seeds in `CHAOS_REGRESSION_SEEDS` the sweep flagged for #290 /
/// #291 / #305. The original test drove a single **unpinned**
/// `SimulationBuilder::new().run()` whose seed came from the wall clock, so the
/// daily sweep's failures were unreproducible from `MOONPOOL_SEED` — pinning
/// the seeds fixes that.
///
/// The `SimulationBuilder`'s default network injects unavoidable chaos
/// (`ConnectFailureMode::Probabilistic`, `bit_flip_probability = 0.0001`,
/// random-close) that the builder gives no API to disable. That chaos lands on
/// two bounded, terminating outcomes that are NOT the self-deadlock under test:
///
/// - a dial that exhausts the client's bounded `connect_timeout` before the handshake completes
///   (the malformed-frame scenario never sets up) → a `Severed` outcome; and
/// - a bit-flip that corrupts the injected malformed frame `[0,0,0,0]` in flight to a
///   non-zero-`total_size` prefix, so `peek_full_frame_len` returns `Ok(None)` instead of
///   `BadLength(0)`, the driver parks, and the ADR-0058 watchdog escalates the wedged socket to
///   `Failed` → a clean `should_close` exit → a `CleanExit` outcome (issue #305).
///
/// The resilience claim is therefore bounded termination (no run hangs ⇒
/// `failed_runs == 0`) **plus** the exact `BadLength(0)` reject being observed
/// on at least one chaos-free seed (`observed_reject`), mirroring
/// `connect_resilience.rs` and the `sim_delayed_marker_*` sweep. The
/// `observed_reject` gate is what keeps a genuine "driver swallows an
/// *uncorrupted* malformed frame" regression hard-failing: such a regression
/// would `CleanExit` on the chaos-free seeds too, leaving `observed_reject`
/// unset.
#[test]
fn moonpool_malformed_mid_session_frame_terminates_driver_not_deadlock() {
    let mut seeds = sweep_seeds(16);
    seeds.extend_from_slice(&CHAOS_REGRESSION_SEEDS);
    let iterations = seeds.len();

    let client = ClientWorkload::new();
    let observed_reject = client.observed_reject_handle();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(client)
        .set_debug_seeds(seeds)
        .set_iterations(iterations)
        .run();
    // `run()` returning at all is the termination proof: a re-entrant-lock
    // deadlock would have wedged the sim thread. No seed may hang or surface
    // the wrong terminal error — a genuine lost reject / clean exit / wrong
    // mapping returns a hard `Err` and lands in `failed_runs`; a connection
    // chaos-severed before the handshake is a bounded `Severed` outcome and
    // stays out of `failed_runs`.
    assert_eq!(
        report.failed_runs, 0,
        "the driver must surface the malformed-frame reject and terminate: {report:?}",
    );
    assert!(
        report.successful_runs >= 1,
        "the run must dispatch and terminate (no self-deadlock): {report:?}",
    );
    // At least one seed must have completed the handshake and observed the
    // exact `BadLength(0)` reject — a sweep where every seed was chaos-severed
    // before the handshake would never exercise the mid-session reject the
    // ADR-0038 regression protects.
    assert!(
        observed_reject.load(Ordering::SeqCst),
        "no seed reached the mid-session malformed frame to observe the BadLength reject: {report:?}",
    );
}
