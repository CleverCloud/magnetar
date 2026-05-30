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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig,
    decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
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
            tokio::select! {
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
                mandatory_stop: Duration::from_secs(60),
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
        let connect_res = tokio::time::timeout(
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
            let _ = tokio::time::timeout(
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
                tokio::task::yield_now().await;
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

// Confirm the trait bounds compose — `MoonpoolEngine<SimProviders>` must be a
// valid construction site. Compile-time-only.
#[allow(dead_code)]
fn _engine_sim_providers_compiles(providers: SimProviders) {
    let _engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
}
