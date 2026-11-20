// SPDX-License-Identifier: Apache-2.0

//! Supervised-redial coverage — moonpool engine, deterministic simulation
//! (closes the former moonpool supervised-loop coverage gap; see ADR-0024
//! for the cross-runtime test + coverage policy).
//!
//! Closes the `supervised_driver_loop` reconnect-body coverage gap. The
//! pre-existing `sim_chaos.rs::DropsTcpAfterCreate` + `AntiThrashClientWorkload`
//! pair drives the *anti-thrash detector* through the engine's shared state,
//! but its client connects via `Client::connect_plain` — the NON-supervised
//! `driver::spawn` path. That path exits the driver on the first socket
//! failure and never enters `supervised_driver_loop`'s reconnect body
//! (anti-thrash cooldown sleep, persisted-backoff reset gate, the
//! multi-attempt redial loop, the `reset()` + `begin_handshake()` + resume
//! tail). Those lines sat 0-hit in the moonpool patch-coverage view.
//!
//! This fixture drives that body end to end: a broker that accepts →
//! CONNECT/CONNECTED → LOOKUP → `PRODUCER_SUCCESS` → drops the socket (a few
//! ms later, inside `drop_grace`), then **re-accepts the next connection**
//! for several cycles (drop → accept → drop → accept). Paired with a client
//! that uses `MoonpoolEngine::connect_plain_supervised`, every short-lived
//! socket trips `should_reset_backoff` (schedule keeps growing), arms the
//! anti-thrash cooldown after enough pairs (the `time.sleep(cooldown)` arm),
//! and every re-accept lands a fresh socket through the redial loop's
//! `Ok(t) => break t` arm followed by the `reset()` + `begin_handshake()` +
//! `pending_rebuild` resume tail.
//!
//! Pairs 1:1 with `crates/magnetar-runtime-tokio/tests/supervised_redial.rs`
//! (which drives the same production reconnect path over a real loopback
//! socket) to keep the `xtask check-runtime-test-parity` gate balanced
//! (ADR-0024). The moonpool engine is the canonical place for the
//! deterministic timing of the schedule (virtual `time.sleep`); the tokio
//! mirror asserts the same reconnect body runs against a real socket.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig,
    decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, DnsResolveFuture, DnsResolver, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

/// Port the broker workload binds to. The sim network gives every workload
/// its own IP; a fixed port keeps the client→broker address derivation
/// trivial.
const BROKER_PORT: u16 = 6650;

/// Single-`poll_read` helper — the in-sim broker reads off a `futures::io`
/// stream (moonpool main dropped raw tokio-io), where `AsyncReadExt::read`
/// returns `0` on EOF. Appends what was read into `buf`, returns the count.
async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-supervised-redial-sim".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "supervised-redial-sim".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// One session: answer CONNECT / PING / LOOKUP, ack the first
/// `CommandProducer` with `ProducerSuccess`, then drop the socket after a
/// short delay (inside the supervisor's `drop_grace`).
async fn handle_drop_after_create_session<S, T>(
    mut stream: S,
    delay: Duration,
    time: T,
    drops_performed: Arc<Mutex<u32>>,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_producer_success = false;
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
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        sent_producer_success = true;
                    }
                }
                _ => {}
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

        // Canonical ADR-0028 thrash: once we've acked the producer, sleep
        // briefly and tear the socket down. The supervisor observes the drop
        // and redials against the listener, which re-accepts.
        if sent_producer_success {
            let _ = time.sleep(delay).await;
            *drops_performed.lock() += 1;
            return Ok(());
        }

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

/// Broker workload that accepts connections, runs the canonical
/// create-then-drop session on each, and counts how many it accepted. The
/// accept loop keeps running (re-accepting after each per-session drop) so
/// the client supervisor's redial lands a fresh socket every cycle.
struct DropAcceptCycleBroker {
    /// Milliseconds between `ProducerSuccess` and the drop. Kept inside the
    /// supervisor's `drop_grace` so each socket counts as a thrash and the
    /// persisted backoff keeps growing.
    delay_ms: u64,
    /// Connections accepted across the iteration. `>= 2` proves the client
    /// supervisor redialed at least once (the redial loop body ran).
    sessions_accepted: Arc<Mutex<u32>>,
    /// Create-then-drop teardowns performed. `>= 2` proves the multi-cycle
    /// drop → accept → drop pattern actually fired.
    drops_performed: Arc<Mutex<u32>>,
}

impl DropAcceptCycleBroker {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            sessions_accepted: Arc::new(Mutex::new(0)),
            drops_performed: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for DropAcceptCycleBroker {
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
        let delay = Duration::from_millis(self.delay_ms);
        let drops = self.drops_performed.clone();
        let accepted = self.sessions_accepted.clone();
        let providers = ctx.providers().clone();
        let task = ctx.providers().task().clone();
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            *accepted.lock() += 1;
                            let drops_for_session = drops.clone();
                            let time = providers.time().clone();
                            let session_delay = delay;
                            let _handle = task.spawn_task("drop-accept-cycle-session", async move {
                                let _ = handle_drop_after_create_session(
                                    stream,
                                    session_delay,
                                    time,
                                    drops_for_session,
                                )
                                .await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Client paired with [`DropAcceptCycleBroker`]. Connects via the supervised
/// driver (`connect_plain_supervised`) with an opt-in anti-thrash threshold,
/// then drives a sequence of `open_producer` attempts. Every successful open
/// is followed by the broker dropping the socket, so the supervisor redials
/// against the re-accepting broker on the next iteration — exercising the
/// full reconnect body of `supervised_driver_loop`.
///
/// `check()` is intentionally a no-op: the authoritative gate is the
/// sweep-level `assert!(accepts >= 2)` / `assert!(drops >= 2)` in the
/// `#[test]` function, computed across all iterations on the broker-side
/// counters. A per-iteration in-workload assertion would either need a
/// lenient fallback (defeating the rigor) or would over-constrain individual
/// seeds whose redial cadence is bounded by the simulation budget.
struct SupervisedRedialClientWorkload;

impl SupervisedRedialClientWorkload {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Workload for SupervisedRedialClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        let cfg = ConnectionConfig {
            supervisor: Some(SupervisorConfig {
                // Tiny schedule so the simulation budget can observe several
                // redial cycles plus the anti-thrash cooldown sleep.
                initial_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(40),
                mandatory_stop: Duration::from_mins(1),
                // Bounded so the redial loop's `max_attempts` give-up arm is
                // reachable on slow seeds, yet high enough that the happy
                // multi-redial path dominates.
                max_attempts: Some(64),
                anti_thrash_threshold: Some(AntiThrashThreshold {
                    successful_attaches: 3,
                    window: Duration::from_secs(5),
                    drop_within: Duration::from_millis(200),
                }),
                drop_grace: Duration::from_millis(500),
                // Short cooldown floor so the `time.sleep(cooldown)` arm
                // completes inside the budget instead of stalling the run.
                max_backoff_after_thrash: Duration::from_millis(120),
            }),
            ..ConnectionConfig::default()
        };

        // Supervised connect — THIS is the path that spawns
        // `supervised_driver_loop` (vs. `connect_plain`'s `driver::spawn`).
        // A connect timeout/failure here is NOT marked as a redial
        // observation: the sweep-level broker-counter assertion in the
        // `#[test]` is the authoritative gate. If the handshake never lands
        // on a given seed, just return cleanly and let the cross-iteration
        // totals decide.
        let time = ctx.providers().time().clone();
        let task = ctx.providers().task().clone();
        let connect_res = time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
            )
            .await;
        let Ok(Ok(client)) = connect_res else {
            return Ok(());
        };

        // Drive a sequence of opens. The broker acks each producer then drops
        // the socket; the supervisor redials and resumes, so the next open
        // rides a freshly-reconnected session. Several iterations guarantee
        // the redial loop body + reset/resume tail run repeatedly and the
        // anti-thrash cooldown arms.
        for _ in 0..12u32 {
            let _ = time
                .timeout(
                    Duration::from_millis(800),
                    client.open_producer(CreateProducerRequest {
                        topic: "persistent://public/default/sim-supervised-redial".to_owned(),
                        ..Default::default()
                    }),
                )
                .await;

            // Pump the spawned supervised-driver task so it observes the
            // broker's post-ProducerSuccess drop and walks its reconnect body
            // (sleep backoff → redial → reset/resume) before the next open.
            // A bare `time.sleep` can let the sim reach quiescence while the
            // driver task is still parked; interleaving yields keeps the
            // scheduler pumping both tasks.
            for _ in 0..64 {
                task.yield_now().await;
            }
            let _ = ctx
                .providers()
                .time()
                .sleep(Duration::from_millis(30))
                .await;
        }

        // Best-effort shutdown — the simulation budget is the safety net.
        client.close().await;
        Ok(())
    }

    // No per-iteration `check()`: the authoritative redial-proof lives at
    // the sweep level in the `#[test]` function (`accepts >= 2` /
    // `drops >= 2` across all iterations). Adding a per-iteration assertion
    // here would either need a lenient fallback (defeating the rigor — an
    // iteration could pass without the client ever observing a redial) or
    // would over-constrain individual seeds whose timing is bounded by the
    // simulation budget. The default `Workload::check` is a no-op `Ok(())`.
}

/// 8-seed sweep — drives `supervised_driver_loop`'s reconnect body end to
/// end: persisted-backoff reset gate, anti-thrash cooldown sleep, the
/// multi-attempt redial loop, and the reset + `begin_handshake` + resume
/// tail. Asserts the broker re-accepted at least twice (proving the
/// supervisor redialed) and that it dropped the socket on more than one cycle
/// (proving the drop → accept → drop → accept pattern fired).
#[test]
fn supervised_loop_redials_under_drop_accept_cycle_sweep_8_seeds() {
    let broker = DropAcceptCycleBroker::new(5);
    let sessions_accepted = broker.sessions_accepted.clone();
    let drops_performed = broker.drops_performed.clone();
    // Curated regression seeds — hand-picked so every iteration drives the
    // supervisor's redial body to completion (the cross-iteration
    // `accepts >= 2` / `drops >= 2` totals below are the authoritative
    // assertion). Unlike the `sim_chaos.rs` discovery sweeps, this is a
    // pinned-anchor test (`set_debug_seeds` semantics, not `MOONPOOL_SEED`-
    // derived); the name carries `sweep_8_seeds` for historical reasons.
    let report = SimulationBuilder::new()
        .workload(broker)
        .workload(SupervisedRedialClientWorkload::new())
        .set_debug_seeds(vec![
            4_772_263_927_792_134_539,
            1,
            2,
            3,
            7,
            42,
            12_345,
            9_999_999,
        ])
        .set_iterations(8)
        .run();

    // Cross-iteration totals: the supervised loop must have redialed (>= 2
    // accepts) and torn down more than one socket (>= 2 drops) somewhere
    // across the sweep — anything less would mean the reconnect body never
    // ran.
    let accepts = *sessions_accepted.lock();
    let drops = *drops_performed.lock();
    assert!(
        accepts >= 2,
        "supervisor must redial at least once (accepts={accepts}, report={report:?})"
    );
    assert!(
        drops >= 2,
        "multi-cycle drop pattern must fire (drops={drops}, report={report:?})"
    );
}

// -- A user close() that lands while a supervisor redial is dialling ---------

/// Every frame the broker decoded, tagged with the accept-order index of the
/// session it arrived on (0 = the bootstrap socket, 1.. = supervisor redials).
type SessionFrames = Arc<Mutex<Vec<(u32, pb::base_command::Type)>>>;

/// Virtual-time bound on `Client::close()` once the redial dial is released.
/// The correct behaviour resolves it in a few simulated milliseconds.
const CLOSE_BOUND: Duration = Duration::from_secs(30);

/// Virtual-time budget of one run (a safety net: the red path leaves the
/// supervisor re-attached and alive).
const RUN_TIME_BUDGET: Duration = Duration::from_mins(10);

#[derive(Debug, Default)]
struct GateState {
    released: bool,
    waker: Option<Waker>,
}

/// Resolver whose first lookup (the bootstrap dial) answers at once and whose
/// every later lookup (a supervisor redial) parks until [`Self::release`], so
/// the workload can land `Client::close()` while the redial is in flight and
/// then let that dial succeed.
#[derive(Debug)]
struct GatedRedialResolver {
    addr: SocketAddr,
    lookups: AtomicU32,
    /// Set the moment a redial lookup parks.
    redial_parked: AtomicBool,
    gate: Mutex<GateState>,
}

impl GatedRedialResolver {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            lookups: AtomicU32::new(0),
            redial_parked: AtomicBool::new(false),
            gate: Mutex::new(GateState::default()),
        }
    }

    fn release(&self) {
        let waker = {
            let mut gate = self.gate.lock();
            gate.released = true;
            gate.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl DnsResolver for GatedRedialResolver {
    fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
        Box::pin(async move {
            if self.lookups.fetch_add(1, Ordering::SeqCst) > 0 {
                self.redial_parked.store(true, Ordering::SeqCst);
                std::future::poll_fn(|cx| {
                    let mut gate = self.gate.lock();
                    if gate.released {
                        Poll::Ready(())
                    } else {
                        gate.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
            }
            Ok(vec![self.addr])
        })
    }
}

/// One broker session. Answers CONNECT / PING / LOOKUP / PRODUCER and records
/// every decoded frame under `session`. Session 0 drops its socket `delay`
/// after acking the producer, so the supervisor redials; every other session
/// stays open until the client hangs up.
async fn handle_recorded_session<S, T>(
    mut stream: S,
    session: u32,
    seen: SessionFrames,
    delay: Duration,
    time: T,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_producer_success = false;
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
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            seen.lock().push((session, kind));
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        sent_producer_success = true;
                    }
                }
                _ => {}
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

        if session == 0 && sent_producer_success {
            let _ = time.sleep(delay).await;
            return Ok(());
        }

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

/// Broker workload that numbers its sessions in accept order and records
/// their frames.
struct RecordedSessionBroker {
    /// Milliseconds between `ProducerSuccess` and the drop of session 0.
    delay_ms: u64,
    frames: SessionFrames,
}

impl RecordedSessionBroker {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            frames: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Workload for RecordedSessionBroker {
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
        let delay = Duration::from_millis(self.delay_ms);
        let providers = ctx.providers().clone();
        let task = ctx.providers().task().clone();
        let mut next_session: u32 = 0;
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            let session = next_session;
                            next_session += 1;
                            let frames = self.frames.clone();
                            let time = providers.time().clone();
                            let _handle = task.spawn_task("recorded-session", async move {
                                let _ = handle_recorded_session(
                                    stream, session, frames, delay, time,
                                )
                                .await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// How far the client workload got.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum CloseStage {
    /// Nothing observed yet.
    #[default]
    Started,
    /// The producer opened on the bootstrap session.
    ProducerOpened,
    /// The supervisor parked inside the redial's lookup (the dial in flight).
    RedialParked,
    /// The workload reached its verdict on `Client::close()`: `closed` is
    /// whether it resolved within `CLOSE_BOUND` of the dial's release.
    Judged { closed: bool },
}

/// Client paired with [`RecordedSessionBroker`]: opens a producer, waits for
/// the supervisor to park inside its redial dial, lands `Client::close()`,
/// then lets the dial succeed.
struct CloseDuringRedialClientWorkload {
    stage: Arc<Mutex<CloseStage>>,
}

impl CloseDuringRedialClientWorkload {
    fn new() -> Self {
        Self {
            stage: Arc::new(Mutex::new(CloseStage::default())),
        }
    }
}

#[async_trait]
impl Workload for CloseDuringRedialClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let broker_addr: SocketAddr = addr
            .parse()
            .map_err(|e| SimulationError::InvalidState(format!("broker address {addr}: {e}")))?;
        let resolver = Arc::new(GatedRedialResolver::new(broker_addr));
        let dns: Arc<dyn DnsResolver> = resolver.clone();
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let time = ctx.providers().time().clone();

        let cfg = ConnectionConfig {
            supervisor: Some(SupervisorConfig {
                initial_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(40),
                mandatory_stop: Duration::from_mins(1),
                max_attempts: Some(64),
                ..SupervisorConfig::default()
            }),
            // The parked redial lookup is inside this bound: it must outlast
            // the (virtual) time the workload needs to land the close.
            connect_timeout: Duration::from_mins(5),
            ..ConnectionConfig::default()
        };

        let Ok(Ok(client)) = time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain_supervised(&engine, &addr, cfg, None, Some(dns)),
            )
            .await
        else {
            return Ok(());
        };

        // A producer the application has open on the bootstrap session.
        let producer = time
            .timeout(
                Duration::from_secs(20),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/sim-close-during-redial".to_owned(),
                    ..Default::default()
                }),
            )
            .await;
        let Ok(Ok(_producer)) = producer else {
            return Ok(());
        };
        *self.stage.lock() = CloseStage::ProducerOpened;

        // The broker drops the bootstrap socket a few ms after acking the
        // producer; wait for the supervisor to park inside the redial lookup.
        let mut parked = false;
        for _ in 0..5_000u32 {
            if resolver.redial_parked.load(Ordering::SeqCst) {
                parked = true;
                break;
            }
            let _ = time.sleep(Duration::from_millis(1)).await;
        }
        if !parked {
            return Ok(());
        }
        *self.stage.lock() = CloseStage::RedialParked;

        // `Client::close()` runs its synchronous prefix on the first poll: the
        // connection is `Closing` from here on, whatever the future does next.
        let mut close = Box::pin(client.close());
        let mut closed =
            std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(close.as_mut(), cx)))
                .await
                .is_ready();

        // The redial dial now succeeds.
        resolver.release();
        if !closed {
            closed = time.timeout(CLOSE_BOUND, close).await.is_ok();
        }
        *self.stage.lock() = CloseStage::Judged { closed };
        Ok(())
    }
}

/// A user `close()` that lands while the supervisor is inside a redial dial
/// must survive that dial succeeding: `Client::close()` has to resolve, and
/// the producer the application had open must not be re-attached on the
/// replacement socket. A supervisor that re-checks `is_user_closed()` only
/// before the dial runs `reset()` once a dial succeeds afterwards, which
/// snaps `Closing` back to `Uninitialized` and erases the close.
#[test]
fn close_during_redial_dial_is_not_erased_by_reset() {
    let broker = RecordedSessionBroker::new(5);
    let frames = broker.frames.clone();
    let client = CloseDuringRedialClientWorkload::new();
    let stage = client.stage.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![1])
        .set_iterations(1)
        .run();

    let stage = *stage.lock();
    let CloseStage::Judged { closed } = stage else {
        panic!(
            "precondition: the producer opened, the supervisor parked inside its redial dial and \
             the close was judged (stage={stage:?}, report={report:?})"
        );
    };
    let replayed_producers = frames
        .lock()
        .iter()
        .filter(|(session, kind)| *session >= 1 && *kind == pb::base_command::Type::Producer)
        .count();
    assert!(
        closed && replayed_producers == 0,
        "a user close() issued while a supervisor redial was dialling was erased by the \
         redial's reset(): Client::close() resolved = {closed} (must be true), producers \
         re-attached on the replacement socket = {replayed_producers} (must be 0) \
         (report={report:?})"
    );
}

// -- The anti-thrash cooldown sleep is not socket lifetime -------------------

/// Reconnect budget of the storm test below (`SupervisorConfig::max_attempts`).
const STORM_MAX_ATTEMPTS: u32 = 3;

/// The bootstrap session plus one session per redial the budget allows.
const STORM_SESSION_BUDGET: u32 = STORM_MAX_ATTEMPTS + 1;

/// Virtual-time bound on the driver giving up. The correct behaviour needs a
/// few simulated seconds (one cooldown per redial); the bug never gives up.
const GIVE_UP_BOUND: Duration = Duration::from_mins(2);

/// How far the storm client got.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum StormStage {
    /// Nothing observed yet.
    #[default]
    Started,
    /// The producer opened on the bootstrap session and the driver handle was taken.
    ProducerOpened,
    /// The workload reached its verdict: whether the supervised driver
    /// terminated (gave up) before the broker saw more sessions than the
    /// budget allows, and how many sessions the broker had accepted by then.
    Judged { terminated: bool, accepted: u32 },
}

/// Client paired with [`DropAcceptCycleBroker`]: a supervisor that bounds its
/// reconnect budget and has the anti-thrash cooldown opted in, with a
/// cooldown longer than `drop_grace`. Holds one producer open so every redial
/// re-attaches it and the broker drops the socket a few ms later.
struct CooldownLifetimeClientWorkload {
    /// Broker-side count of accepted connections.
    sessions_accepted: Arc<Mutex<u32>>,
    stage: Arc<Mutex<StormStage>>,
}

impl CooldownLifetimeClientWorkload {
    fn new(sessions_accepted: Arc<Mutex<u32>>) -> Self {
        Self {
            sessions_accepted,
            stage: Arc::new(Mutex::new(StormStage::default())),
        }
    }
}

#[async_trait]
impl Workload for CooldownLifetimeClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let time = ctx.providers().time().clone();

        let cfg = ConnectionConfig {
            supervisor: Some(SupervisorConfig {
                initial_backoff: Duration::from_millis(5),
                max_backoff: Duration::from_millis(40),
                mandatory_stop: Duration::from_mins(1),
                max_attempts: Some(STORM_MAX_ATTEMPTS),
                anti_thrash_threshold: Some(AntiThrashThreshold {
                    successful_attaches: 2,
                    window: Duration::from_mins(1),
                    drop_within: Duration::from_millis(200),
                }),
                // A socket that dies within a few ms is far inside this grace;
                // the cooldown below is far outside it.
                drop_grace: Duration::from_millis(400),
                max_backoff_after_thrash: Duration::from_secs(1),
            }),
            ..ConnectionConfig::default()
        };

        let Ok(Ok(client)) = time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
            )
            .await
        else {
            return Ok(());
        };

        // The producer the supervisor re-attaches on every redial: it has to
        // stay open for the storm to continue.
        let producer = time
            .timeout(
                Duration::from_secs(20),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/sim-cooldown-is-not-lifetime".to_owned(),
                    ..Default::default()
                }),
            )
            .await;
        let Ok(Ok(_producer)) = producer else {
            return Ok(());
        };
        let Some(driver) = client.take_driver() else {
            return Ok(());
        };
        *self.stage.lock() = StormStage::ProducerOpened;

        // Wait for the driver to give up. Bail out as soon as the broker has
        // seen more sessions than the budget allows: the supervisor is then
        // redialling past `max_attempts` and waiting longer proves nothing.
        let mut join = Box::pin(driver.join());
        let mut terminated = false;
        let mut waited = Duration::ZERO;
        let slice = Duration::from_millis(100);
        while waited < GIVE_UP_BOUND && *self.sessions_accepted.lock() <= STORM_SESSION_BUDGET {
            if time.timeout(slice, &mut join).await.is_ok() {
                terminated = true;
                break;
            }
            waited += slice;
        }
        *self.stage.lock() = StormStage::Judged {
            terminated,
            accepted: *self.sessions_accepted.lock(),
        };
        Ok(())
    }
}

/// An attach-then-drop storm against a supervisor that bounds its reconnect
/// budget (`max_attempts`) and has the anti-thrash cooldown opted in, with a
/// cooldown longer than `drop_grace`.
///
/// Every socket here dies a few milliseconds after its producer is acked, so
/// none of them is "stable" (`should_reset_backoff` is false for a socket that
/// lived less than `drop_grace`): the ADR-0061 give-up budget must keep
/// counting across cycles and the driver must stop after `max_attempts`
/// redials. The cooldown the detector arms sleeps for longer than
/// `drop_grace`, though, and a supervisor that measures the dead socket's
/// lifetime AFTER that sleep reads the cooldown as socket lifetime: it resets
/// the backoff schedule and the give-up counter every cycle and redials for
/// ever.
#[test]
fn anti_thrash_cooldown_sleep_is_not_counted_as_socket_lifetime() {
    let broker = DropAcceptCycleBroker::new(5);
    let client = CooldownLifetimeClientWorkload::new(broker.sessions_accepted.clone());
    let stage = client.stage.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![1])
        .set_iterations(1)
        .run();

    let stage = *stage.lock();
    let StormStage::Judged {
        terminated,
        accepted,
    } = stage
    else {
        panic!(
            "precondition: the producer opened on the bootstrap session and the storm was judged \
             (stage={stage:?}, report={report:?})"
        );
    };
    assert!(
        terminated && accepted <= STORM_SESSION_BUDGET,
        "the supervisor kept redialling an attach-then-drop storm past max_attempts = \
         {STORM_MAX_ATTEMPTS}: driver terminated = {terminated} (must be true), broker sessions \
         accepted = {accepted} (must be <= {STORM_SESSION_BUDGET}); the anti-thrash cooldown \
         sleep was counted as socket lifetime and reset the give-up budget (report={report:?})"
    );
}

// Confirm the trait bounds compose — `MoonpoolEngine<SimProviders>` must be a
// valid construction site. Compile-time-only.
#[allow(dead_code)]
fn _engine_sim_providers_compiles(providers: SimProviders) {
    let _engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
}
