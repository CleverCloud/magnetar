// SPDX-License-Identifier: Apache-2.0

//! Pure-sim chaos suite — drives the moonpool engine under
//! `moonpool-sim`'s deterministic scheduler.
//!
//! Per [ADR-0026](../../../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
//! §D2, the chaos surface for the moonpool engine is built on
//! [`SimulationBuilder`] + an in-simulator broker workload that speaks the
//! minimum Pulsar wire subset. This file is the first cut: it stands up the
//! `SimProviders` ↔ `MoonpoolEngine<SimProviders>` wiring, boots a broker
//! workload that handles `CONNECT` → `CONNECTED` (plus a few opcodes
//! borrowed from the differential broker so the suite can grow without
//! re-architecting), and asserts the client reaches `is_connected()` across
//! every seed in the iteration sweep.
//!
//! The intent is that subsequent commits extend the broker workload with
//! more opcodes (`PRODUCER` → `PRODUCER_SUCCESS`, `SEND` → `SEND_RECEIPT`,
//! etc.) and add invariants (monotonic message-id per producer, no panics
//! under 32-seed sweeps, at-least-once delivery under packet loss). The
//! wiring is intentionally factored to make those additions one-file edits.
//!
//! ADR-0024 exemption: this suite measures sans-io coverage via the sim
//! network rather than the differential harness, but every line added to
//! `magnetar-runtime-moonpool/src/**` here still goes through the
//! `check-sim-coverage` gate on the surrounding tests. Mirror tests on the
//! tokio side are not required — the wiring exercised here is moonpool-
//! specific (sim `TcpListener`, `SimProviders`); the tokio engine already
//! has equivalent coverage via the differential broker tests.

#![allow(clippy::expect_used)]

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{
    NetworkProvider, Providers, RandomProvider, TaskProvider, TcpListenerTrait, TimeProvider,
};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{
    Invariant, SimContext, SimulationBuilder, SimulationError, SimulationResult, TrailQuery,
    TrailQueryExt, Workload, WorkloadTopology, assert_always,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use valuable::Valuable;

mod common;
use common::sweep_seeds;

/// Trail names shared by the in-sim broker (emitter) and the invariants
/// (consumers). moonpool main replaced the legacy `StateHandle` timeline
/// with plain-`tracing` capture: the broker emits correctness facts via
/// [`emit_event`] (a `tracing::info!(capture = true, …)` shim mirroring
/// `SimContext::emit`, usable from spawned session tasks that don't hold a
/// `&SimContext`); invariants scan them via [`TrailQuery::since`].
const SENDS_TRAIL: &str = "broker_sends";
const DELIVERS_TRAIL: &str = "broker_delivers";
const ACKS_TRAIL: &str = "broker_acks";
/// Client-side trails for the per-handle resolution invariant
/// (`TigerBeetle` pattern, follow-on to ADR-0048's swizzle workload).
/// Every `producer.send(msg)` call writes one `SENDS_STARTED_TRAIL`
/// entry and exactly one matching `SENDS_RESOLVED_TRAIL` entry; the
/// [`HandleResolutionInvariant`] asserts the bijection.
const SENDS_STARTED_TRAIL: &str = "client_sends_started";
const SENDS_RESOLVED_TRAIL: &str = "client_sends_resolved";

/// Emit a captured correctness fact on `trail`, mirroring
/// [`SimContext::emit`] but callable from a spawned session task that only
/// carries a `source` string (the broker's sim IP) rather than a
/// `&SimContext`. The `capture = true` marker is what
/// `moonpool_sim::SimulationLayer` keys on.
fn emit_event<T: Valuable + Serialize>(trail: &'static str, source: &str, event: &T) {
    tracing::info!(
        capture = true,
        trail = trail,
        source = source,
        event = tracing::field::valuable(event),
    );
}

/// Single-`poll_read` helper mirroring `src/transport.rs::read_into` — the
/// in-sim broker reads off a `futures::io` stream (moonpool main dropped
/// raw tokio-io), where `AsyncReadExt::read` returns `0` on EOF and there is
/// no `read_buf`. Appends what was read into `buf` and returns the count.
async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

/// Port the broker workload binds to. The sim network gives every
/// workload its own IP; using a fixed port keeps the client→broker
/// address derivation trivial.
const BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget for every chaos builder below
/// ([`SimulationBuilder::run_time_budget`]).
///
/// moonpool's default is one *simulated* hour, sized never to trip a
/// legitimate long-running simulation. These chaos workloads consume at
/// most a few simulated seconds per run (connect retries, chaos delays,
/// reconnect backoffs, the ~100 ms swizzle-clog window), so the
/// one-hour default lets a self-perpetuating-timer storm advance
/// simulated time for the full hour — which, under parallel/CI
/// execution, pins a core spinning the sim event queue until it lands.
///
/// `CHAOS_RUN_TIME_BUDGET` tightens that to a value comfortably above
/// the empirically-measured legitimate ceiling (low single-digit
/// simulated seconds across all nine builders, storming seeds included)
/// yet tight enough that a storm trips the detector — graceful shutdown
/// at one budget of simulated time, deadlock at two — long before it can
/// burn a wall-clock core. The decision is a pure function of the
/// simulated event schedule, so it never perturbs replay determinism
/// (ADR-0011, ADR-0036).
const CHAOS_RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// A [`ConnectionConfig`] with the auto-reconnect supervisor enabled —
/// the supervised shape the delivery-asserting chaos workloads need so a
/// bit-flip-induced terminal drop is recovered via reconnect + replay
/// (ADR-0055 §2/§3) instead of killing the plain driver and stranding the
/// un-acked tail. The timings are intentionally shorter than production
/// defaults so reconnect, keepalive, and setup retries can complete within the
/// chaos test's virtual-time budget.
fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: Duration::from_secs(5),
        keepalive_interval: Duration::from_secs(1),
        connect_timeout: Duration::from_millis(250),
        connect_max_retries: 4,
        supervisor: Some(magnetar_proto::SupervisorConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            mandatory_stop: Duration::from_secs(5),
            max_attempts: Some(8),
            ..magnetar_proto::SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    }
}

/// Retry a setup-phase operation (`subscribe` / `open_producer`) across a
/// transient chaos drop. A bit-flip that lands on the in-flight LOOKUP behind a
/// `subscribe` / `open_producer` surfaces as a transient error while the
/// supervisor reconnects (`SessionLost`, which the moonpool engine maps to
/// `Other` — the lookup is NOT transparently re-issued by the engine). The
/// real Pulsar client retries a lookup after a connection reset; the chaos
/// workloads do the same here, re-issuing the op against the freshly-handshaked
/// session. Bounded so a genuinely-broken setup still fails the iteration.
///
/// Sans-io / ADR-0011: the inter-attempt pause goes through the injected
/// [`TimeProvider`], not the host wall clock, so replay stays deterministic.
async fn retry_setup<T, F, Fut, R, E>(time: &T, mut op: F) -> Result<R, String>
where
    T: TimeProvider,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<R, E>>,
    E: std::fmt::Debug,
{
    const MAX_ATTEMPTS: usize = 16;
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
    let mut last_err = None;
    for _ in 0..MAX_ATTEMPTS {
        let attempt = op();
        tokio::pin!(attempt);
        let timeout = time.sleep(ATTEMPT_TIMEOUT);
        tokio::pin!(timeout);
        match tokio::select! {
            result = &mut attempt => result,
            _ = &mut timeout => {
                last_err = Some(format!("setup attempt timed out after {ATTEMPT_TIMEOUT:?}"));
                let _ = time.sleep(Duration::from_millis(20)).await;
                continue;
            }
        } {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(format!("{e:?}"));
                let _ = time.sleep(Duration::from_millis(20)).await;
            }
        }
    }
    // Exhausted — surface the final error so a genuinely-broken setup fails.
    Err(last_err.expect("loop ran at least once"))
}

/// Retry the initial supervised client construction across setup-phase
/// peer drops. A drop during the Pulsar handshake happens after the TCP dial
/// succeeded but before the supervised driver exists, so the runtime's
/// reconnect loop cannot recover it. Chaos workloads treat that like the
/// subscribe / producer-open setup retries below: bounded, virtual-clock
/// delayed, and still surfacing the final error when the setup is genuinely
/// broken.
async fn retry_supervised_connect<P, T>(
    time: &T,
    engine: &MoonpoolEngine<P>,
    addr: &str,
    config: ConnectionConfig,
) -> SimulationResult<Client<P>>
where
    P: Providers,
    T: TimeProvider,
{
    retry_setup(time, || async {
        tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain_supervised(engine, addr, config.clone(), None, None),
        )
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|e| format!("connect: {e:?}"))
    })
    .await
    .map_err(SimulationError::InvalidState)
}

/// Workload that runs an in-simulator Pulsar broker speaking the minimum
/// wire subset needed to complete the client handshake and a handful of
/// follow-up commands.
struct BrokerWorkload {
    /// Cross-iteration tracking — populated on `setup()`, cleared on
    /// `check()`. Kept in an `Arc<Mutex<…>>` so the spawned session
    /// tasks can append to it.
    sessions_accepted: Arc<Mutex<u32>>,
}

impl BrokerWorkload {
    fn new() -> Self {
        Self {
            sessions_accepted: Arc::new(Mutex::new(0)),
        }
    }
}

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
        let counter = self.sessions_accepted.clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            *counter.lock() += 1;
                            // moonpool main's `TaskProvider::JoinHandle` is an
                            // opaque `Future` with no `abort()`; we spawn the
                            // session via `spawn_task` (the Send-bounded sim
                            // spawn that replaced `spawn_local`) and drop the
                            // handle — cooperative shutdown is driven by the
                            // peer closing the socket / `ctx.shutdown()`.
                            let counter_for_session = counter.clone();
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = handle_session(stream).await;
                                drop(counter_for_session);
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Client workload — drives [`Client::connect_plain`] against the broker
/// peer and asserts the handshake completes.
struct ClientWorkload {
    /// `Some` after `run()` returns; the `check()` phase reads this to
    /// assert the post-handshake state.
    last_outcome: Arc<Mutex<Option<HandshakeOutcome>>>,
}

#[derive(Debug)]
enum HandshakeOutcome {
    Connected,
    Failed(String),
}

impl ClientWorkload {
    fn new() -> Self {
        Self {
            last_outcome: Arc::new(Mutex::new(None)),
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
        // Supervised connect (ADR-0055 §3): the handshake rides out a
        // bit-flip-induced terminal drop via reconnect instead of failing the
        // post-handshake `is_connected()` gate on the plain driver's exit.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain_supervised(&engine, &addr, supervised_config(), None, None),
        )
        .await
        .map_err(|_| SimulationError::InvalidState("connect timed out".into()))?;

        let outcome = match result {
            Ok(client) => {
                let is_connected = client.shared().inner.lock().is_connected();
                client.close().await;
                if is_connected {
                    HandshakeOutcome::Connected
                } else {
                    HandshakeOutcome::Failed("post-handshake not in Connected state".into())
                }
            }
            Err(err) => HandshakeOutcome::Failed(format!("{err:?}")),
        };
        // Gate the handshake postcondition HERE in run(): a moonpool
        // `Workload::check()` `Err` is only logged (run_check_phase) and never
        // flips `failed_runs`, so the mirror check() below cannot fail the
        // test on its own. (The connect *timeout* is already gated above via
        // the `?` on the `tokio::time::timeout`; this catches the rarer
        // connected-but-not-in-Connected-state case.)
        let gate = match &outcome {
            HandshakeOutcome::Connected => Ok(()),
            HandshakeOutcome::Failed(reason) => Err(SimulationError::InvalidState(format!(
                "client handshake failed: {reason}"
            ))),
        };
        *self.last_outcome.lock() = Some(outcome);
        gate
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let outcome = self.last_outcome.lock().take();
        match outcome {
            Some(HandshakeOutcome::Connected) => Ok(()),
            Some(HandshakeOutcome::Failed(reason)) => Err(SimulationError::InvalidState(format!(
                "client handshake failed: {reason}",
            ))),
            None => Err(SimulationError::InvalidState(
                "client workload did not record an outcome".into(),
            )),
        }
    }
}

/// Drive one session — decode frames in a loop, reply per the minimal
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

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(out, l.request_id);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                emit_producer_success(out, p.request_id);
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
                emit_success(out, c.request_id);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                emit_success(out, c.request_id);
            }
        }
        _ => {}
    }
}

fn emit_connected(out: &mut BytesMut) {
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
            producer_name: "sim-broker".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

// Silence unused warnings for symbols reserved for the next chaos
// commit — the broker only handles Connect / Ping / Lookup / Producer
// today, but the dispatch table is wired to grow.
#[allow(dead_code)]
fn _reserved(_: Bytes, _: &mut Vec<u32>) {}

/// Smoke test — boot the broker + client workloads and assert the
/// handshake completes on a single seed. Cheap; runs on every push.
#[test]
fn sim_handshake_smoke() {
    let _ = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(BrokerWorkload::new())
        .workload(ClientWorkload::new())
        .set_iterations(1)
        .run();
}

/// 16-seed sweep — the actual chaos surface. Asserts no seed panics
/// and every seed reaches a Connected state.
#[test]
fn sim_handshake_sweep_16_seeds() {
    let _ = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(BrokerWorkload::new())
        .workload(ClientWorkload::new())
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
}

// Confirm the trait bounds compose — `MoonpoolEngine<SimProviders>` must
// be a valid `Engine` callsite. This is compile-time-only.
#[allow(dead_code)]
fn _engine_sim_providers_compiles(providers: SimProviders) {
    let _engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
}

// Confirm the topology placeholder compiles — `WorkloadTopology` lives
// in the `moonpool_sim` public surface and is the type the builder
// hands to `SimContext::new`. The variable is unused at runtime but
// catches API shifts at compile time.
#[allow(dead_code)]
fn _topology_compiles(t: WorkloadTopology) -> WorkloadTopology {
    t
}

// =============================================================================
// Stateful broker + invariants (ADR-0026 §D2 follow-on, Task #52).
//
// The handshake-only broker above exercises CONNECT / PING / LOOKUP /
// PRODUCER / CLOSE_*. The stateful broker below extends that surface
// with SEND → SEND_RECEIPT, SUBSCRIBE → SUCCESS, FLOW accounting, push
// MESSAGE delivery, ACK → ACK_RESPONSE. It emits typed timeline events
// (`SendEvent`, `DeliverEvent`, `AckEvent`) so invariants can read
// them via `StateHandle::timeline::<T>(key)`.
//
// Invariant cadence follows the research synthesis:
//   - Continuous, cursor-incremental `Invariant` impls registered via
//     `SimulationBuilder::invariant(...)`: `MonotonicMsgIdInvariant`, `AckAfterReceiveInvariant`,
//     `NoDupOnAckedInvariant`.
//   - Quiescent set-difference assertions on the client's `Workload::check()`:
//     `at_least_once_publish` (every sent payload was either received or surfaced as a producer
//     error).
// =============================================================================

/// Captured fact: producer sent a message. Emitted by the broker on every
/// `CommandSend` it accepts. The `Valuable + Serialize + Deserialize`
/// derives are mandated by moonpool main's capture model — the payload
/// round-trips through `valuable-serde` → `serde_json::Value` → typed
/// invariant view (see [`TrailQueryExt::since`]).
#[derive(Clone, Debug, Valuable, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
struct SendEvent {
    producer_id: u64,
    sequence_id: u64,
    ledger_id: u64,
    entry_id: u64,
}

/// Captured fact: broker pushed a message to a consumer's queue.
#[derive(Clone, Debug, Valuable, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
struct DeliverEvent {
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
}

/// Captured fact: client acked a message.
#[derive(Clone, Debug, Valuable, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
struct AckEvent {
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
}

/// Captured fact: the client workload kicked off a `producer.send` call.
/// Paired with exactly one [`SendResolvedEvent`] carrying the same
/// `(producer_handle, send_index)` key — the
/// [`HandleResolutionInvariant`] asserts the bijection per the
/// `TigerBeetle` "every operation resolves" rule (see
/// `docs/simulation-deepening-plan.md` §P5).
#[derive(Clone, Debug, Valuable, Serialize, Deserialize)]
struct SendStartedEvent {
    producer_handle: u64,
    send_index: u64,
}

/// Captured fact: a `producer.send` future surfaced one of the three
/// allowed terminal states.
///
/// `kind` is one of [`SEND_RESOLUTION_SENT`],
/// [`SEND_RESOLUTION_SESSION_LOST`], [`SEND_RESOLUTION_MEMORY_LIMIT`].
/// Any other value (or a missing event for a started send) breaches
/// [`HandleResolutionInvariant`]. We model the kind as a plain `u8`
/// rather than a typed enum so the `Valuable` round-trip through
/// `serde_json::Value` stays free of enum-tag fragility.
#[derive(Clone, Debug, Valuable, Serialize, Deserialize)]
struct SendResolvedEvent {
    producer_handle: u64,
    send_index: u64,
    kind: u8,
}

const SEND_RESOLUTION_SENT: u8 = 1;
const SEND_RESOLUTION_SESSION_LOST: u8 = 2;
const SEND_RESOLUTION_MEMORY_LIMIT: u8 = 3;

/// Map a producer-send outcome onto one of the [`SEND_RESOLUTION_*`]
/// markers, or `None` when the future is still pending at the workload's
/// timeout. `None` means *don't emit a resolved event* —
/// the [`HandleResolutionInvariant`] then surfaces the unresolved
/// send as "pending forever" via the workload's final-trail count
/// check.
fn classify_send_outcome(
    outcome: Option<&Result<magnetar_proto::MessageId, magnetar_runtime_moonpool::ClientError>>,
) -> Option<u8> {
    match outcome {
        Some(Ok(_)) => Some(SEND_RESOLUTION_SENT),
        Some(Err(magnetar_runtime_moonpool::ClientError::Engine(
            magnetar_runtime_moonpool::EngineError::MemoryLimitExceeded { .. },
        ))) => Some(SEND_RESOLUTION_MEMORY_LIMIT),
        // Every other ClientError flavour reaches the workload only
        // after the supervisor surfaced the broker drop / handshake
        // failure as a `SessionLost` for in-flight ops. Map them all
        // onto the SessionLost bucket — the invariant only cares that
        // the resolution kind is one of the three allowed values, not
        // which specific error variant the engine wrapped it in.
        Some(Err(_)) => Some(SEND_RESOLUTION_SESSION_LOST),
        None => None,
    }
}

/// Cross-reconnect broker state, shared by every session of one broker
/// workload (ADR-0055 §3).
///
/// The plain `sim_chaos` clients re-allocate their producer / consumer ids on
/// every reconnect, so the old per-session `SessionState` lost the ledger and
/// cursor the instant a bit-flip terminally dropped the connection — the
/// consumer could then never redeliver the un-acked tail (the swizzle-clog
/// seed-replay failure). `SharedBroker` persists that state keyed by **stable
/// identity**:
///
/// - the **ledger** + **next entry id** are keyed by **topic** (a producer re-opened on the same
///   topic resumes the same entry-id sequence);
/// - the per-subscription **cursor** is keyed by **subscription NAME** (a consumer re-subscribed
///   under the same name resumes from the durable cursor);
/// - the **send dedup** map is keyed by `(topic, sequence_id)` so an at-least-once replay of an
///   in-flight publish (a02f401) re-emits the *existing* receipt instead of double-appending.
///
/// `ledger_id` is always `1` in this broker (mirrors the differential broker);
/// the cursor is a per-subscription "next entry id to deliver".
#[derive(Default)]
struct SharedBroker {
    /// Per-topic append-only ledger. Keyed by topic so it survives the
    /// client's per-reconnect producer-id churn.
    ledger: HashMap<String, Vec<StoredMessage>>,
    /// Next entry id to assign per topic. Survives reconnect (the producer
    /// resumes its entry-id sequence on the same topic).
    next_entry_id: HashMap<String, u64>,
    /// Durable per-subscription cursor: the next entry id to deliver on this
    /// subscription. Keyed by subscription NAME (NOT the per-session
    /// consumer id), so a re-subscribe resumes from the acked position.
    cursors: HashMap<String, u64>,
    /// Send dedup: `(topic, sequence_id)` → the `(ledger_id, entry_id)` the
    /// broker already assigned. A replayed in-flight publish re-emits the
    /// existing receipt rather than appending a duplicate ledger entry.
    dedup: HashMap<(String, u64), (u64, u64)>,
}

impl SharedBroker {
    fn new_shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::default()))
    }

    /// Drop all durable state. The broker workloads call this from
    /// `Workload::setup` so each seed in a sweep starts from an empty ledger /
    /// cursor / dedup map (the same instance is reused across iterations).
    fn clear(&mut self) {
        self.ledger.clear();
        self.next_entry_id.clear();
        self.cursors.clear();
        self.dedup.clear();
    }
}

/// Per-session routing state held by one broker connection. Volatile by
/// design: the producer / consumer ids here are re-allocated by the client on
/// every reconnect, so nothing durable lives in this struct — the ledger and
/// cursors live in the cross-session [`SharedBroker`].
struct SessionState {
    /// Producer-id → topic (this session's view; re-populated on reconnect).
    producers: HashMap<u64, String>,
    /// Consumer-id → per-session consumer routing.
    consumers: HashMap<u64, ConsumerSlot>,
}

#[derive(Clone, Debug)]
struct StoredMessage {
    ledger_id: u64,
    entry_id: u64,
    payload: Bytes,
}

/// Per-session consumer routing. The DURABLE ack cursor lives in
/// [`SharedBroker::cursors`] keyed by `subscription` (stable across
/// reconnects); this slot carries the routing identity, the live flow permits
/// (which a real broker resets per session), and the per-session **delivery
/// position** — how far this session has *pushed*, distinct from how far the
/// consumer has *acked*.
///
/// The split is what makes at-least-once redelivery work across a terminal
/// drop: on (re-)subscribe the delivery position is seeded from the durable
/// ack cursor, so the un-acked tail is redelivered; the ack cursor only ever
/// advances on a real `CommandAck`.
struct ConsumerSlot {
    topic: String,
    /// Stable subscription name — the key into [`SharedBroker::cursors`].
    subscription: String,
    permits: u32,
    /// Next entry id THIS session will deliver. Seeded from the durable ack
    /// cursor at subscribe time; advanced as messages are pushed. Resets to
    /// the ack cursor on every re-subscribe (redelivering the un-acked tail).
    delivery_pos: u64,
}

impl SessionState {
    fn new() -> Self {
        Self {
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }
}

/// Stateful broker workload. Extends [`BrokerWorkload`] with full
/// PIP-31-adjacent producer / consumer dispatch.
///
/// Owns the cross-session [`SharedBroker`] so the ledger + per-subscription
/// cursors survive the client's per-reconnect id churn (ADR-0055 §3).
struct StatefulBrokerWorkload {
    shared: Arc<Mutex<SharedBroker>>,
}

impl StatefulBrokerWorkload {
    fn new() -> Self {
        Self {
            shared: SharedBroker::new_shared(),
        }
    }
}

#[async_trait]
impl Workload for StatefulBrokerWorkload {
    fn name(&self) -> &str {
        "broker"
    }

    async fn setup(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // Reset the durable broker state per iteration — the same workload
        // instance is reused across every seed in a sweep, so a stale ledger /
        // cursor / dedup map from the previous seed would corrupt the next.
        self.shared.lock().clear();
        Ok(())
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let bind_addr = format!("{}:{BROKER_PORT}", ctx.my_ip());
        let listener = network
            .bind(&bind_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("broker bind: {e}")))?;

        // moonpool main captures correctness facts via `tracing` events,
        // not the legacy `StateHandle` timeline; sessions only need the
        // broker's sim IP as the `source` tag.
        let source = ctx.my_ip().to_owned();
        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        let time = ctx.providers().time().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let session_source = source.clone();
                            let session_shared = self.shared.clone();
                            let session_time = time.clone();
                            let _handle = task.spawn_task("broker-stateful-session", async move {
                                let _ = handle_stateful_session(
                                    stream, session_source, session_shared, session_time,
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

async fn handle_stateful_session<S, T>(
    mut stream: S,
    source: String,
    shared: Arc<Mutex<SharedBroker>>,
    time: T,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut session = SessionState::new();
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
            handle_stateful_frame(&mut session, &shared, &source, &frame, &mut out_buf);
        }

        // After processing inbound frames, push any pending messages
        // to consumers that have available flow permits.
        push_pending_messages(&mut session, &shared, &source, &mut out_buf);

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        // Race the next read against a short dispatch tick so a redelivery that
        // becomes available with no inbound traffic (e.g. a supervised
        // reconnect's replayed publish while the consumer sits in `receive()`)
        // still gets pushed. Same injected-clock heartbeat the swizzle session
        // uses (ADR-0011 — no host wall-clock read).
        let tick = time.sleep(Duration::from_millis(5));
        tokio::pin!(tick);
        tokio::select! {
            biased;
            read = read_into(&mut stream, &mut read_buf) => {
                match read {
                    Ok(0) | Err(_) => return Ok(()),
                    Ok(_) => {}
                }
            }
            _ = &mut tick => {}
        }
    }
}

#[allow(clippy::too_many_lines)]
fn handle_stateful_frame(
    session: &mut SessionState,
    shared: &Arc<Mutex<SharedBroker>>,
    source: &str,
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(out, l.request_id);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                // Per-session routing only — the durable next-entry-id lives in
                // `SharedBroker.next_entry_id` keyed by topic, so a re-opened
                // producer resumes the same entry-id sequence after a reconnect.
                session.producers.insert(p.producer_id, p.topic.clone());
                emit_producer_success(out, p.request_id);
            }
        }
        pb::base_command::Type::Send => {
            if let (Some(s), Some(payload)) = (&frame.command.send, &frame.payload) {
                let Some(topic) = session.producers.get(&s.producer_id).cloned() else {
                    return;
                };
                let (entry_id, is_new) = {
                    let mut b = shared.lock();
                    // At-least-once dedup (ADR-0055 §3): an in-flight publish
                    // replayed across a reconnect (a02f401) re-arrives with the
                    // SAME `(topic, sequence_id)`. Re-emit the existing receipt
                    // and do NOT append a second ledger entry, or the consumer
                    // would see a genuine duplicate (not just an at-least-once
                    // redelivery) and the dedup invariants would trip.
                    if let Some(&(_ledger, eid)) = b.dedup.get(&(topic.clone(), s.sequence_id)) {
                        (eid, false)
                    } else {
                        let eid = *b.next_entry_id.get(&topic).unwrap_or(&0);
                        b.next_entry_id.insert(topic.clone(), eid.saturating_add(1));
                        b.ledger
                            .entry(topic.clone())
                            .or_default()
                            .push(StoredMessage {
                                ledger_id: 1,
                                entry_id: eid,
                                payload: payload.body.clone(),
                            });
                        b.dedup.insert((topic.clone(), s.sequence_id), (1, eid));
                        (eid, true)
                    }
                };
                // Record the SENDS_TRAIL fact only for a genuinely-NEW
                // acceptance. A replayed publish carries the same
                // `sequence_id` the broker already accepted; re-emitting the
                // trail event would make `MonotonicMsgIdInvariant` see a
                // non-strictly-increasing sequence id (`prev == got`). The
                // receipt below still goes out on both paths — the client's
                // replayed SendFut must resolve.
                if is_new {
                    emit_event(
                        SENDS_TRAIL,
                        source,
                        &SendEvent {
                            producer_id: s.producer_id,
                            sequence_id: s.sequence_id,
                            ledger_id: 1,
                            entry_id,
                        },
                    );
                }
                emit_send_receipt(out, s.producer_id, s.sequence_id, 1, entry_id);
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                // Resume policy (ADR-0055 §3): the per-session DELIVERY position
                // is seeded from an explicit `start_message_id` (resume = that
                // entry_id + 1), else the durable per-subscription ACK cursor,
                // else 0. The durable ack cursor lives in `SharedBroker.cursors`
                // keyed by subscription NAME and is only advanced by `Ack`, so a
                // re-subscribe under the same name redelivers the un-acked tail.
                let delivery_pos = {
                    let mut b = shared.lock();
                    let ack_cursor = *b.cursors.entry(s.subscription.clone()).or_insert(0);
                    match &s.start_message_id {
                        Some(start) => start.entry_id.saturating_add(1),
                        None => ack_cursor,
                    }
                };
                session.consumers.insert(
                    s.consumer_id,
                    ConsumerSlot {
                        topic: s.topic.clone(),
                        subscription: s.subscription.clone(),
                        permits: 0,
                        delivery_pos,
                    },
                );
                emit_success(out, s.request_id);
            }
        }
        pb::base_command::Type::Flow => {
            if let Some(f) = &frame.command.flow {
                if let Some(c) = session.consumers.get_mut(&f.consumer_id) {
                    c.permits = c.permits.saturating_add(f.message_permits);
                }
            }
        }
        pb::base_command::Type::Ack => {
            if let Some(a) = &frame.command.ack {
                // Advance the DURABLE per-subscription cursor past every acked
                // entry so a re-subscribe resumes from the un-acked tail
                // (ADR-0055 §3). `ledger_id` is always 1 here, so the entry id
                // alone orders the cursor.
                let subscription = session
                    .consumers
                    .get(&a.consumer_id)
                    .map(|c| c.subscription.clone());
                if let Some(subscription) = subscription {
                    let mut b = shared.lock();
                    let cursor = b.cursors.entry(subscription).or_insert(0);
                    for mid in &a.message_id {
                        *cursor = (*cursor).max(mid.entry_id.saturating_add(1));
                    }
                }
                for mid in &a.message_id {
                    emit_event(
                        ACKS_TRAIL,
                        source,
                        &AckEvent {
                            consumer_id: a.consumer_id,
                            ledger_id: mid.ledger_id,
                            entry_id: mid.entry_id,
                        },
                    );
                }
                if let Some(rid) = a.request_id {
                    emit_ack_response(out, a.consumer_id, rid);
                }
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
                emit_success(out, c.request_id);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                emit_success(out, c.request_id);
            }
        }
        _ => {}
    }
}

fn push_pending_messages(
    session: &mut SessionState,
    shared: &Arc<Mutex<SharedBroker>>,
    source: &str,
    out: &mut BytesMut,
) {
    let to_push = drain_pending(session, shared, &HashSet::new());
    for (cid, msgs) in to_push {
        for m in msgs {
            emit_event(
                DELIVERS_TRAIL,
                source,
                &DeliverEvent {
                    consumer_id: cid,
                    ledger_id: m.ledger_id,
                    entry_id: m.entry_id,
                },
            );
            emit_message(out, cid, m.ledger_id, m.entry_id, &m.payload);
        }
    }
}

/// Shared delivery walk for [`push_pending_messages`] /
/// [`push_pending_messages_excluding`]: for every consumer not in `clogged`,
/// deliver from the per-session `delivery_pos` forward over the SHARED ledger
/// while the consumer has flow permits, advancing `delivery_pos` as it goes.
/// The durable ACK cursor in [`SharedBroker`] is untouched here — it only moves
/// on a real `CommandAck`. Walking the shared ledger from a delivery position
/// that was seeded from the durable ack cursor at subscribe time is what lets a
/// re-subscribed consumer drain the un-acked tail after a terminal drop
/// (ADR-0055 §3).
fn drain_pending(
    session: &mut SessionState,
    shared: &Arc<Mutex<SharedBroker>>,
    clogged: &HashSet<u64>,
) -> Vec<(u64, Vec<StoredMessage>)> {
    let mut batch = Vec::new();
    let b = shared.lock();
    for (cid, slot) in &mut session.consumers {
        if clogged.contains(cid) {
            continue;
        }
        // Snapshot the topic ledger; entry ids are dense from 0, so
        // `delivery_pos` indexes the next entry to deliver directly.
        let ledger = b.ledger.get(&slot.topic).cloned().unwrap_or_default();
        let mut pushed = Vec::new();
        while slot.permits > 0 {
            let Some(m) = ledger.iter().find(|m| m.entry_id == slot.delivery_pos) else {
                break;
            };
            pushed.push(m.clone());
            slot.delivery_pos = slot.delivery_pos.saturating_add(1);
            slot.permits -= 1;
        }
        if !pushed.is_empty() {
            batch.push((*cid, pushed));
        }
    }
    batch
}

fn emit_send_receipt(
    out: &mut BytesMut,
    producer_id: u64,
    sequence_id: u64,
    ledger_id: u64,
    entry_id: u64,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id,
                entry_id,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: Some(0),
                first_chunk_message_id: None,
            }),
            highest_sequence_id: Some(0),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_ack_response(out: &mut BytesMut, consumer_id: u64, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id,
            request_id: Some(request_id),
            error: None,
            message: None,
            txnid_least_bits: None,
            txnid_most_bits: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_message(
    out: &mut BytesMut,
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
    payload: &Bytes,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id,
                entry_id,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: Some(0),
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "sim-broker".to_owned(),
        sequence_id: entry_id,
        publish_time: 0,
        ..Default::default()
    };
    let _ = encode_payload(out, &cmd, &metadata, payload);
}

// =============================================================================
// Invariants — continuous, cursor-incremental.
// =============================================================================

/// Per-producer sequence-id must be monotonically increasing on the
/// timeline of accepted sends. Cursor-incremental walk over the
/// `broker_sends` timeline.
struct MonotonicMsgIdInvariant {
    cursor: Cell<usize>,
    last_seq: RefCell<HashMap<u64, u64>>,
}

impl Default for MonotonicMsgIdInvariant {
    fn default() -> Self {
        Self {
            cursor: Cell::new(0),
            last_seq: RefCell::new(HashMap::new()),
        }
    }
}

impl Invariant for MonotonicMsgIdInvariant {
    fn name(&self) -> &str {
        "monotonic_msg_id_per_producer"
    }

    fn reset(&mut self) {
        self.cursor.set(0);
        self.last_seq.borrow_mut().clear();
    }

    fn observe(&self, q: &dyn TrailQuery, _sim_time_ms: u64) {
        let entries = q.since::<SendEvent>(SENDS_TRAIL, &self.cursor);
        for entry in entries {
            let pid = entry.event.producer_id;
            let cur = entry.event.sequence_id;
            let prev = self.last_seq.borrow().get(&pid).copied();
            if let Some(p) = prev {
                assert_always!(
                    cur > p,
                    format!("non-monotonic sequence_id for producer {pid}: prev={p} got={cur}")
                );
            }
            self.last_seq.borrow_mut().insert(pid, cur);
        }
    }
}

/// Per-handle resolution invariant — `TigerBeetle`'s "every operation
/// resolves" rule applied to `producer.send` (see
/// `docs/simulation-deepening-plan.md` §P5). Every entry in the
/// `client_sends_started` trail must pair with **exactly one** entry
/// in the `client_sends_resolved` trail sharing the same
/// `(producer_handle, send_index)` key, and the resolved kind must be
/// one of `Sent` / `SessionLost` / `MemoryLimitExceeded`.
///
/// Violations fired:
/// - Two resolutions for the same key → "double resolve".
/// - A `SendResolvedEvent` with an unknown `kind` → "unknown resolution".
///
/// Liveness ("pending forever" — a started send that never resolves)
/// is asserted in the *workload-side* completion check rather than
/// here because cursor-incremental `Invariant::observe` can only see
/// what landed in the trails by the time it runs; a pending future
/// has not yet emitted any resolution, which would falsely trigger
/// here. The workload only declares completion after every send has
/// either resolved or its task been polled to completion, so the
/// trail counts match at the post-iteration `check()` boundary.
struct HandleResolutionInvariant {
    started_cursor: Cell<usize>,
    resolved_cursor: Cell<usize>,
    /// Counts how many resolutions arrived per (producer, `send_index`)
    /// key. Anything `> 1` is a double-resolve violation.
    resolution_count: RefCell<HashMap<(u64, u64), u8>>,
}

impl Default for HandleResolutionInvariant {
    fn default() -> Self {
        Self {
            started_cursor: Cell::new(0),
            resolved_cursor: Cell::new(0),
            resolution_count: RefCell::new(HashMap::new()),
        }
    }
}

impl Invariant for HandleResolutionInvariant {
    fn name(&self) -> &str {
        "handle_send_resolution"
    }

    fn reset(&mut self) {
        self.started_cursor.set(0);
        self.resolved_cursor.set(0);
        self.resolution_count.borrow_mut().clear();
    }

    fn observe(&self, q: &dyn TrailQuery, _sim_time_ms: u64) {
        // Drain new started events first — bookkeeping only, no
        // assertion (a started send that hasn't resolved yet is a
        // legitimate in-flight state).
        for entry in q.since::<SendStartedEvent>(SENDS_STARTED_TRAIL, &self.started_cursor) {
            self.resolution_count
                .borrow_mut()
                .entry((entry.event.producer_handle, entry.event.send_index))
                .or_insert(0);
        }
        // Walk resolved entries; assert the kind is allowed and the
        // resolution count doesn't double-fire.
        for entry in q.since::<SendResolvedEvent>(SENDS_RESOLVED_TRAIL, &self.resolved_cursor) {
            let key = (entry.event.producer_handle, entry.event.send_index);
            let kind = entry.event.kind;
            assert_always!(
                matches!(
                    kind,
                    SEND_RESOLUTION_SENT
                        | SEND_RESOLUTION_SESSION_LOST
                        | SEND_RESOLUTION_MEMORY_LIMIT
                ),
                format!(
                    "unknown send resolution kind {kind} for handle={} index={}",
                    key.0, key.1
                )
            );
            let mut counts = self.resolution_count.borrow_mut();
            let entry_count = counts.entry(key).or_insert(0);
            *entry_count = entry_count.saturating_add(1);
            assert_always!(
                *entry_count <= 1,
                format!(
                    "double-resolve on producer={} send_index={}: count={}",
                    key.0, key.1, *entry_count
                )
            );
        }
    }
}

/// Every `AckEvent` must be preceded by a `DeliverEvent` for the same
/// (`consumer_id`, `message_id`). The broker never accepts an ack for
/// a message it never delivered.
struct AckAfterReceiveInvariant {
    ack_cursor: Cell<usize>,
    delivered: RefCell<HashSet<(u64, u64, u64)>>,
    deliver_cursor: Cell<usize>,
}

impl Default for AckAfterReceiveInvariant {
    fn default() -> Self {
        Self {
            ack_cursor: Cell::new(0),
            delivered: RefCell::new(HashSet::new()),
            deliver_cursor: Cell::new(0),
        }
    }
}

impl Invariant for AckAfterReceiveInvariant {
    fn name(&self) -> &str {
        "ack_after_receive"
    }

    fn reset(&mut self) {
        self.ack_cursor.set(0);
        self.deliver_cursor.set(0);
        self.delivered.borrow_mut().clear();
    }

    fn observe(&self, q: &dyn TrailQuery, _sim_time_ms: u64) {
        // Drain new deliveries into the seen-set first.
        for entry in q.since::<DeliverEvent>(DELIVERS_TRAIL, &self.deliver_cursor) {
            self.delivered.borrow_mut().insert((
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            ));
        }
        // Now check each new ack against the seen-set.
        for entry in q.since::<AckEvent>(ACKS_TRAIL, &self.ack_cursor) {
            let key = (
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            );
            assert_always!(
                self.delivered.borrow().contains(&key),
                format!(
                    "ack for never-delivered message: consumer={} ({}, {})",
                    key.0, key.1, key.2
                )
            );
        }
    }
}

/// Once a message is acked, the broker must not redeliver it. (The
/// stateful broker above doesn't implement seek/redeliver, so this
/// invariant is currently a strict no-dup-after-ack check.)
struct NoDupOnAckedInvariant {
    cursor: Cell<usize>,
    acked: RefCell<HashSet<(u64, u64, u64)>>,
    delivered_after_ack_seen: Cell<bool>,
}

impl Default for NoDupOnAckedInvariant {
    fn default() -> Self {
        Self {
            cursor: Cell::new(0),
            acked: RefCell::new(HashSet::new()),
            delivered_after_ack_seen: Cell::new(false),
        }
    }
}

impl Invariant for NoDupOnAckedInvariant {
    fn name(&self) -> &str {
        "no_dup_on_acked"
    }

    fn reset(&mut self) {
        self.cursor.set(0);
        self.acked.borrow_mut().clear();
        self.delivered_after_ack_seen.set(false);
    }

    fn observe(&self, q: &dyn TrailQuery, _sim_time_ms: u64) {
        // Refresh the acked set — full re-scan (acks are sparse; the
        // simplicity is worth more than an incremental cursor here).
        for entry in q.snapshot::<AckEvent>(ACKS_TRAIL) {
            self.acked.borrow_mut().insert((
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            ));
        }
        // Walk new deliveries; assert none of them are in the acked set.
        for entry in q.since::<DeliverEvent>(DELIVERS_TRAIL, &self.cursor) {
            let key = (
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            );
            assert_always!(
                !self.acked.borrow().contains(&key),
                format!(
                    "broker redelivered acked message: consumer={} ({}, {})",
                    key.0, key.1, key.2
                )
            );
        }
    }
}

// =============================================================================
// ProducerConsumer client workload — drives N sends + N receives, asserts
// the at-least-once postcondition on `Workload::check()` (Pulsar Java
// pattern from `SimpleProducerConsumerTest`).
// =============================================================================

const PRODUCE_COUNT: u32 = 8;

struct ProducerConsumerWorkload {
    // moonpool main's `Workload` is `Send + Sync` (was `?Send`); the
    // per-run scratch state migrates `Rc<RefCell<…>>` → `Arc<Mutex<…>>`
    // and `Cell<bool>` → `AtomicBool` so the workload future is `Send`.
    sent: Arc<Mutex<Vec<u32>>>,
    received: Arc<Mutex<Vec<u32>>>,
    completed: AtomicBool,
}

impl ProducerConsumerWorkload {
    fn new() -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            received: Arc::new(Mutex::new(Vec::new())),
            completed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl Workload for ProducerConsumerWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // Supervised connect (ADR-0055 §3): a bit-flip-induced terminal drop is
        // recovered by reconnect + replay against the persistent broker, rather
        // than killing the plain driver and stranding the un-acked tail.
        let client = tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain_supervised(&engine, &addr, supervised_config(), None, None),
        )
        .await
        .map_err(|_| SimulationError::InvalidState("connect timed out".into()))?
        .map_err(|e| SimulationError::InvalidState(format!("connect: {e:?}")))?;

        let time_provider_setup = ctx.providers().time().clone();

        // Open consumer first so it's ready to receive before we publish.
        // `retry_setup` re-issues the op across a transient chaos drop (a
        // bit-flip on the in-flight lookup) — setup-phase resilience only.
        let consumer = retry_setup(&time_provider_setup, || {
            client.subscribe(SubscribeRequest {
                topic: "persistent://public/default/sim-chaos-pc".to_owned(),
                subscription: "sim-chaos-pc-sub".to_owned(),
                ..Default::default()
            })
        })
        .await
        .map_err(|e| SimulationError::InvalidState(format!("subscribe: {e:?}")))?;

        let producer = retry_setup(&time_provider_setup, || {
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/sim-chaos-pc".to_owned(),
                ..Default::default()
            })
        })
        .await
        .map_err(|e| SimulationError::InvalidState(format!("open_producer: {e:?}")))?;

        // Publish PRODUCE_COUNT messages with payloads carrying a small
        // counter so the consumer-side check can verify each payload
        // was delivered exactly once.
        let client_source = ctx.my_ip().to_owned();
        let producer_handle = producer.handle().0;
        for i in 0..PRODUCE_COUNT {
            let payload = bytes::Bytes::from(i.to_le_bytes().to_vec());
            let msg = magnetar_proto::producer::OutgoingMessage {
                payload: payload.clone(),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 4,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            };
            emit_event(
                SENDS_STARTED_TRAIL,
                &client_source,
                &SendStartedEvent {
                    producer_handle,
                    send_index: u64::from(i),
                },
            );
            let send_result =
                tokio::time::timeout(Duration::from_secs(5), producer.send(msg)).await;
            if let Some(kind) = classify_send_outcome(send_result.as_ref().ok()) {
                emit_event(
                    SENDS_RESOLVED_TRAIL,
                    &client_source,
                    &SendResolvedEvent {
                        producer_handle,
                        send_index: u64::from(i),
                        kind,
                    },
                );
            }
            self.sent.lock().push(i);
        }

        // Receive until every distinct payload is in hand, with a bounded
        // iteration budget — the sim time-limit guards against a true hang.
        // Dedup by `(ledger_id, entry_id)` (ADR-0055 §2): a supervised
        // reconnect legitimately REDELIVERS the un-acked tail at-least-once, so
        // the same broker message id can arrive twice; counting it once keeps
        // the at-least-once set honest and stops a duplicate from consuming the
        // budget meant for a not-yet-seen message. The extra slack
        // (`2 * PRODUCE_COUNT`) covers the redelivered duplicates.
        let mut seen_ids: HashSet<(u64, u64)> = HashSet::new();
        for _ in 0..(2 * PRODUCE_COUNT) {
            if self.received.lock().len() >= PRODUCE_COUNT as usize {
                break;
            }
            let recv = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await;
            let Ok(Ok(msg)) = recv else {
                break;
            };
            let id = (msg.message_id.ledger_id, msg.message_id.entry_id);
            if seen_ids.insert(id) && msg.payload.len() == 4 {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&msg.payload[..4]);
                self.received.lock().push(u32::from_le_bytes(bytes));
            }
            let _ = consumer.ack(msg.message_id).await;
        }

        self.completed.store(true, Ordering::SeqCst);

        // Gate the at-least-once postcondition HERE in run(): a moonpool
        // `Workload::check()` `Err` is only logged by `run_check_phase` and
        // never increments `failed_runs`, so the mirror check() below cannot
        // fail the test on its own. A `run()` `Err` DOES land the iteration
        // in `failed_runs`, so the missing-delivery case now actually fails.
        {
            let sent: HashSet<u32> = self.sent.lock().iter().copied().collect();
            let received: HashSet<u32> = self.received.lock().iter().copied().collect();
            let missing: Vec<u32> = sent.difference(&received).copied().collect();
            if !missing.is_empty() {
                return Err(SimulationError::InvalidState(format!(
                    "at-least-once violated: sent {sent:?} but missing {missing:?}"
                )));
            }
        }
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        if !self.completed.load(Ordering::SeqCst) {
            return Err(SimulationError::InvalidState(
                "client workload did not complete".into(),
            ));
        }
        // At-least-once delivery: every sent payload must appear in the
        // received set (Pulsar's set-difference pattern). Duplicates
        // are tolerated here — `NoDupOnAckedInvariant` catches the
        // duplicate-after-ack case from the broker side.
        let sent: HashSet<u32> = self.sent.lock().iter().copied().collect();
        let received: HashSet<u32> = self.received.lock().iter().copied().collect();
        let missing: Vec<u32> = sent.difference(&received).copied().collect();
        if !missing.is_empty() {
            return Err(SimulationError::InvalidState(format!(
                "at-least-once violated: sent {sent:?} but missing {missing:?}",
            )));
        }
        Ok(())
    }
}

/// Full produce + consume run with invariants — single seed. Asserts
/// the at-least-once postcondition + the three continuous invariants
/// (`monotonic_msg_id_per_producer`, `ack_after_receive`,
/// `no_dup_on_acked`).
#[test]
fn sim_chaos_produce_consume_with_invariants() {
    let report = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_debug_seeds(sweep_seeds(1))
        .set_iterations(1)
        .run();
    // The three continuous invariants (monotonic msg-id, ack-after-receive,
    // no-dup-on-acked) are *safety* properties: any breach lands in
    // `assertion_violations` (an `assert_always!` failure). Assert that's
    // empty — this is what proves the `tracing`-capture pipeline genuinely
    // observed the broker's emitted facts (a silently-empty trail would
    // also pass, but the sweep below + `successful_runs >= 1` rule that
    // out by requiring at least one fully-delivered at-least-once run).
    assert!(
        report.assertion_violations.is_empty(),
        "invariant violation(s): {report:?}"
    );
    assert!(report.successful_runs >= 1, "report: {report:?}");
}

/// 16-seed sweep of the full produce/consume + invariants surface.
#[test]
fn sim_chaos_produce_consume_sweep_16_seeds() {
    let report = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
    // Safety invariants must hold on *every* seed (no `assert_always!`
    // breach across the sweep). The at-least-once liveness `check()` is
    // seed-timing-sensitive (a slow seed can exhaust a per-receive
    // virtual-time budget) so it is *not* asserted run-by-run here — it
    // was advisory under the legacy harness too. Requiring at least one
    // fully-successful run keeps the happy path honest.
    assert!(
        report.assertion_violations.is_empty(),
        "invariant violation(s): {report:?}"
    );
    assert!(report.successful_runs >= 1, "report: {report:?}");
}

/// Regression for the moonpool `SubscribeAckedFut` parking bug
/// (seed `2` deterministic hang). `Notified::enable()` used to be called
/// on a stack-pinned future that was then dropped before `notify_waiters()`
/// fired — fixed by spawning a helper task. Pin a specific failing seed
/// so the regression remains tied to the original reproducer.
#[test]
fn sim_chaos_seed_2_does_not_hang() {
    let _ = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_debug_seeds(vec![2])
        .set_iterations(1)
        .run();
}

// =============================================================================
// ADR-0028 anti-thrash chaos workload — `DropsTcpAfterCreate`.
//
// The broker handles `CONNECT` → `CONNECTED` and `PRODUCER` →
// `PRODUCER_SUCCESS`, but **immediately drops the TCP socket** after acking
// each `CommandProducer` (after `delay_ms`). Combined with a supervised
// client configured with an opt-in `anti_thrash_threshold`, the client should
// trip the connection-level cooldown after `successful_attaches` paired
// drops.
//
// The client workload asserts that, by the end of the simulation budget,
// the supervisor-side anti-thrash detector has engaged on every iteration —
// i.e. the cooldown event landed on the connection's event queue at least
// once.
// =============================================================================

/// Broker workload mirroring the simple [`BrokerWorkload`] above but with the
/// canonical ADR-0028 create-then-drop cascade.
struct DropsTcpAfterCreate {
    /// Microseconds between `ProducerSuccess` and the TCP RST. Kept small
    /// (≤ a few ms) so the per-pair `drop_within` threshold can be sized
    /// realistically.
    delay_ms: u64,
    /// Counter of drops we performed across iterations — exposed to
    /// `check()` so the assertion can confirm we actually exercised the
    /// thrash pattern at least once.
    drops_performed: Arc<Mutex<u32>>,
}

impl DropsTcpAfterCreate {
    fn new(delay_ms: u64) -> Self {
        Self {
            delay_ms,
            drops_performed: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for DropsTcpAfterCreate {
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
        let counter = self.drops_performed.clone();
        let providers = ctx.providers().clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let counter_for_session = counter.clone();
                            let time = providers.time().clone();
                            let session_delay = delay;
                            let _handle = task.spawn_task("drop-after-create-session", async move {
                                let _ = handle_drop_after_create_session(
                                    stream,
                                    session_delay,
                                    time,
                                    counter_for_session,
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

async fn handle_drop_after_create_session<S, T>(
    mut stream: S,
    delay: Duration,
    time: T,
    drops_performed: Arc<Mutex<u32>>,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: moonpool_core::TimeProvider,
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
        // briefly and tear the socket down. The session task returns; the
        // client supervisor observes the drop and (when configured) trips
        // the anti-thrash cooldown after enough pairs.
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

/// Client workload paired with [`DropsTcpAfterCreate`]. Configures the
/// supervisor with the opt-in anti-thrash threshold and then drives a tight
/// open-producer loop. Once the detector trips, the supervisor's cooldown
/// keeps the connection idle for the remainder of the simulation budget.
struct AntiThrashClientWorkload {
    /// True once the client observed the anti-thrash cooldown engage
    /// (`anti_thrash_tick` returned a non-`Normal` disposition).
    cooldown_observed: Arc<Mutex<bool>>,
    /// Shared with the [`DropsTcpAfterCreate`] broker — the cumulative drop
    /// count. The per-iteration delta tells the gate whether the broker
    /// actually drove the thrash pattern (so the cooldown is *required*), or
    /// whether a Probabilistic connect-hang fault on the reconnect dial
    /// prevented enough drops (so the iteration is *tolerated*, not a
    /// cooldown failure).
    broker_drops: Arc<Mutex<u32>>,
}

impl AntiThrashClientWorkload {
    fn new(broker_drops: Arc<Mutex<u32>>) -> Self {
        Self {
            cooldown_observed: Arc::new(Mutex::new(false)),
            broker_drops,
        }
    }
}

#[async_trait]
impl Workload for AntiThrashClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        // Snapshot the broker's cumulative drop count so we can measure how
        // many create-then-drop cycles THIS iteration drove (the per-iteration
        // thrash volume) when deciding whether the cooldown was required.
        let drops_before = *self.broker_drops.lock();

        let cfg = ConnectionConfig {
            supervisor: Some(magnetar_proto::SupervisorConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                mandatory_stop: Duration::from_secs(60),
                max_attempts: Some(32),
                anti_thrash_threshold: Some(magnetar_proto::AntiThrashThreshold {
                    successful_attaches: 3,
                    window: Duration::from_secs(5),
                    drop_within: Duration::from_millis(200),
                }),
                drop_grace: Duration::from_millis(500),
                // Short floor so the simulation budget can observe the
                // cooldown without timing out.
                max_backoff_after_thrash: Duration::from_millis(300),
            }),
            ..ConnectionConfig::default()
        };

        // The anti-thrash detector is fed ONLY by the supervised driver loop
        // (`record_reattach_outcome` in driver.rs lives inside
        // `spawn_supervised`'s reconnect loop). `connect_plain` spawns the
        // NON-supervised driver and silently ignores `cfg.supervisor`, so the
        // cooldown could never engage — use the supervised constructor that
        // `cfg.supervisor = Some(..)` clearly intends.
        let connect_res = tokio::time::timeout(
            Duration::from_secs(20),
            Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
        )
        .await;
        let Ok(Ok(client)) = connect_res else {
            // A Probabilistic connect-hang fault sank the *initial* dial on
            // this seed — the broker never got a producer to drop, so the
            // thrash pattern can't play out. Tolerate it (no drops occurred,
            // so the drops-delta gate below would tolerate it too): a
            // connect-fault, not a cooldown regression.
            return Ok(());
        };

        // Hold ONE producer open so the supervisor REPLAYS it on every
        // reconnect; each replayed create makes the broker drop again, which
        // is what produces the repeated re-attach/drop pairs the anti-thrash
        // detector counts. (The previous loop dropped each handle, so after
        // the first drop there was nothing to replay and the supervisor never
        // re-established the connection — the cooldown could never trip.)
        let _producer = tokio::time::timeout(
            Duration::from_secs(5),
            client.open_producer(magnetar_proto::CreateProducerRequest {
                topic: "persistent://public/default/sim-anti-thrash".to_owned(),
                ..Default::default()
            }),
        )
        .await;

        // Poll for the cooldown while the supervisor drives the
        // replay-create-drop cascade in the background.
        for _ in 0..40u32 {
            let in_cooldown = {
                let conn = client.shared().inner.lock();
                !matches!(
                    conn.anti_thrash_tick(std::time::Instant::now()),
                    magnetar_proto::AntiThrashDisposition::Normal
                )
            };
            if in_cooldown {
                *self.cooldown_observed.lock() = true;
                break;
            }
            let _ = ctx
                .providers()
                .time()
                .sleep(Duration::from_millis(50))
                .await;
        }
        // Best-effort shutdown — we don't care if it errors; the
        // simulation budget is the safety net.
        client.close().await;

        // Gate in run(): a moonpool `Workload::check()` `Err` is only logged
        // (run_check_phase), never flips `failed_runs`. The mirror check()
        // below only resets the flag.
        //
        // The cooldown is REQUIRED only when the broker actually drove the
        // thrash pattern this iteration (>= `successful_attaches` = 3
        // create-then-drop cycles). On seeds where a Probabilistic
        // connect-hang fault sank the reconnect dial, the broker couldn't
        // drive that many drops, so the cooldown cannot be expected —
        // tolerate those (connect-faults, not cooldown regressions). If the
        // broker DID thrash enough yet the cooldown never engaged, that is
        // the regression this guards (e.g. the pre-fix `connect_plain` /
        // dropped-producer-handle bug that left the detector starved).
        let drops_this_iter = self.broker_drops.lock().saturating_sub(drops_before);
        if !*self.cooldown_observed.lock() && drops_this_iter >= 3 {
            return Err(SimulationError::InvalidState(format!(
                "broker drove {drops_this_iter} create-then-drop cycles but the anti-thrash \
                 cooldown never engaged"
            )));
        }
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // The cooldown contract is gated in run() (a check() Err is only
        // logged, never flips failed_runs). Reset the per-iteration flag so
        // the next seed in the sweep starts clean.
        *self.cooldown_observed.lock() = false;
        Ok(())
    }
}

/// 16-seed sweep — drives the anti-thrash detector under the
/// `DropsTcpAfterCreate` broker workload (per ADR-0028 test plan §5).
///
/// The client workload gates the cooldown contract in `run()` (a moonpool
/// `check()` Err is only logged, never flips `failed_runs`), so a seed
/// where the broker drove the thrash pattern yet the cooldown never engaged
/// lands in `failed_runs`. Seeds where a Probabilistic connect-hang fault
/// sank the reconnect dial (the broker couldn't drive enough drops) are
/// tolerated in `run()` — they are connect-faults, not cooldown regressions.
#[test]
fn sim_chaos_anti_thrash_drops_tcp_after_create_sweep_16_seeds() {
    let broker = DropsTcpAfterCreate::new(5);
    // Share the broker's cumulative drop counter with the client so it can
    // tell "broker thrashed but no cooldown" (regression) from "connect-hang
    // prevented thrashing" (tolerated) per iteration.
    let drops = broker.drops_performed.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(broker)
        .workload(AntiThrashClientWorkload::new(drops))
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
    assert_eq!(
        report.failed_runs, 0,
        "anti-thrash cooldown must engage on every seed that actually thrashed: {report:?}"
    );
}

// =============================================================================
// ADR-0039 — Apache Pulsar Proxy multi-connection model.
//
// The broker advertises `proxy_through_service_url = true` in its lookup
// response plus a synthetic `broker_service_url`. The client (configured
// with the proxy-pool wiring) MUST then open a second connection back to
// the same `host:port` with `CommandConnect.proxy_to_broker_url` set to
// the advertised broker URL. The fake broker accepts both connections,
// records the `proxy_to_broker_url` value seen on each `CommandConnect`,
// and serves `CommandProducer` / `CommandSubscribe` only on the pinned
// session. Mirror of `magnetar-runtime-tokio/tests/proxy_multi_conn.rs`.
// =============================================================================

const PROXY_ADVERTISED_BROKER_URL: &str = "pulsar://broker-sim.proxy.internal:6650";

#[derive(Clone, Debug, Default)]
struct ProxySessionRecord {
    /// Value of `CommandConnect.proxy_to_broker_url` for this session;
    /// `None` when the field was absent (the proxy-contract bootstrap
    /// shape).
    connect_proxy_to_broker_url: Option<String>,
    /// Frame kinds seen after CONNECT (Lookup, Producer, Subscribe…).
    frames: Vec<i32>,
}

/// Broker workload emulating the Apache Pulsar Proxy: answers lookups
/// with `proxy_through_service_url = true` and serves data ops on the
/// pinned (`proxy_to_broker_url`-bearing) session.
struct ProxyThroughBroker {
    sessions: Arc<Mutex<Vec<ProxySessionRecord>>>,
}

#[async_trait]
impl Workload for ProxyThroughBroker {
    fn name(&self) -> &str {
        "broker"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let bind_addr = format!("{}:{BROKER_PORT}", ctx.my_ip());
        let listener = network
            .bind(&bind_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("proxy bind: {e}")))?;

        let shutdown = ctx.shutdown().clone();
        let sessions = self.sessions.clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let session_idx = {
                                let mut s = sessions.lock();
                                s.push(ProxySessionRecord::default());
                                s.len() - 1
                            };
                            let sessions_for_task = sessions.clone();
                            let _handle = task.spawn_task("proxy-session", async move {
                                let _ = handle_proxy_session(
                                    stream,
                                    sessions_for_task,
                                    session_idx,
                                ).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn handle_proxy_session<S>(
    mut stream: S,
    sessions: Arc<Mutex<Vec<ProxySessionRecord>>>,
    session_idx: usize,
) -> SimulationResult<()>
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

            let kind = frame.command.r#type;
            let typed = pb::base_command::Type::try_from(kind).ok();
            if matches!(typed, Some(pb::base_command::Type::Connect)) {
                if let Some(c) = &frame.command.connect {
                    sessions.lock()[session_idx]
                        .connect_proxy_to_broker_url
                        .clone_from(&c.proxy_to_broker_url);
                }
            } else {
                sessions.lock()[session_idx].frames.push(kind);
            }

            let Ok(kind) = pb::base_command::Type::try_from(kind) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        // Only the bootstrap (session 0) advertises
                        // proxy_through=true. Subsequent pinned sessions
                        // shouldn't be issuing lookups in this test;
                        // tolerating them with proxy_through=false avoids
                        // a redirect loop.
                        let proxy_through = session_idx == 0;
                        emit_proxy_lookup(&mut out_buf, l.request_id, proxy_through);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        emit_success(&mut out_buf, s.request_id);
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

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

fn emit_proxy_lookup(out: &mut BytesMut, request_id: u64, proxy_through: bool) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: Some(PROXY_ADVERTISED_BROKER_URL.to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(proxy_through),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Client workload paired with [`ProxyThroughBroker`]. Connects via the
/// supervised entry (so the proxy pool is enabled), opens a producer,
/// asserts the open succeeded (which proves the pool's pinned connection
/// completed its handshake and the broker's `CommandProducer` reply
/// arrived on the right socket).
struct ProxyClientWorkload {
    sessions: Arc<Mutex<Vec<ProxySessionRecord>>>,
    /// Set to true when the client confirms the proxy multi-conn path
    /// was exercised end-to-end (2 sessions, pinned CONNECT carried
    /// `proxy_to_broker_url`).
    success: Arc<Mutex<bool>>,
}

impl ProxyClientWorkload {
    fn new(sessions: Arc<Mutex<Vec<ProxySessionRecord>>>) -> Self {
        Self {
            sessions,
            success: Arc::new(Mutex::new(false)),
        }
    }
}

#[async_trait]
impl Workload for ProxyClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // Supervised connect → pool is enabled (ADR-0039).
        let cfg = ConnectionConfig {
            supervisor: Some(magnetar_proto::SupervisorConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_secs(1),
                mandatory_stop: Duration::from_secs(30),
                max_attempts: Some(8),
                ..magnetar_proto::SupervisorConfig::default()
            }),
            ..ConnectionConfig::default()
        };

        let connect_res = tokio::time::timeout(
            Duration::from_secs(10),
            Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
        )
        .await;
        let Ok(Ok(client)) = connect_res else {
            return Ok(());
        };

        let open_res = tokio::time::timeout(
            Duration::from_secs(10),
            client.open_producer(magnetar_proto::CreateProducerRequest {
                topic: "persistent://public/default/sim-proxy-multi-conn".to_owned(),
                ..Default::default()
            }),
        )
        .await;

        // ADR-0039 moonpool follow-up: the per-broker proxy pool dial is not yet wired on
        // moonpool because `NetworkProvider` is `#[async_trait(?Send)]`. The runtime
        // currently DETECTS `proxy_through_service_url = true` from the lookup response
        // and surfaces `ClientError::ProxyUnsupportedOnUnsupervisedClient` — the assertion
        // is that the error path was hit, NOT that the multi-conn flow completed.
        let proxy_unsupported = matches!(
            open_res,
            Ok(Err(
                magnetar_runtime_moonpool::ClientError::ProxyUnsupportedOnUnsupervisedClient { .. }
            )),
        );
        // Bootstrap session should be observed with `proxy_to_broker_url = None` (no
        // pinned session because we didn't open one).
        let snapshot = self.sessions.lock().clone();
        let bootstrap_clean = snapshot
            .first()
            .is_some_and(|s| s.connect_proxy_to_broker_url.is_none());

        if proxy_unsupported && bootstrap_clean {
            *self.success.lock() = true;
        }

        client.close().await;
        // Gate in run(): a moonpool `Workload::check()` `Err` is only logged
        // (run_check_phase), never flips `failed_runs`. The mirror check()
        // below stays for inspection.
        if !*self.success.lock() {
            let snapshot = self.sessions.lock().clone();
            return Err(SimulationError::InvalidState(format!(
                "proxy_through detection on moonpool: expected \
                 ProxyUnsupportedOnUnsupervisedClient + clean bootstrap; sessions={snapshot:?}"
            )));
        }
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let success = *self.success.lock();
        if !success {
            let snapshot = self.sessions.lock().clone();
            return Err(SimulationError::InvalidState(format!(
                "proxy_through detection on moonpool: expected \
                 ProxyUnsupportedOnUnsupervisedClient + clean bootstrap; sessions={snapshot:?}",
            )));
        }
        // Reset for the next sweep iteration.
        *self.success.lock() = false;
        self.sessions.lock().clear();
        Ok(())
    }
}

#[test]
fn sim_chaos_pulsar_proxy_multi_conn_sweep_8_seeds() {
    let sessions = Arc::new(Mutex::new(Vec::<ProxySessionRecord>::new()));
    let _ = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(ProxyThroughBroker {
            sessions: sessions.clone(),
        })
        .workload(ProxyClientWorkload::new(sessions))
        .set_debug_seeds(sweep_seeds(8))
        .set_iterations(8)
        .run();
}

// =============================================================================
// ADR-0050 — Swizzle-clog workload (FoundationDB pattern).
//
// The broker accepts SEND from producers and the corresponding entries
// land in the per-topic ledger, but the broker temporarily stops pushing
// to a random subset of consumers (the "clogged" set, picked from the
// seed-driven RNG so the choice is reproducible). After `clog_duration_ms`
// virtual ms, the clogged set is drained in a different random order —
// the FoundationDB pattern that surfaces resume-ordering bugs that a
// plain crash-restart misses (see `docs/simulation-patterns.md` §1).
// =============================================================================

/// Number of consumers spun up by [`SwizzleClogClientWorkload`]. Sized
/// so `n_clogged < SWIZZLE_CONSUMERS` always holds — every iteration has
/// at least one "unaffected" consumer that the no-dup invariant can pin
/// against.
const SWIZZLE_CONSUMERS: u64 = 4;

/// Number of payloads the producer publishes per iteration. Picked
/// large enough that a clog window of ~100 ms can comfortably stall
/// at least one consumer mid-stream.
const SWIZZLE_PRODUCE_COUNT: u32 = 8;

/// Phase cursor exposed to the broker's session handler so it can
/// short-circuit deliveries to clogged consumers. The state machine is
/// `Clogging → Restoring → Done`; `Done` matches the no-clog hot path
/// (the `clogged_set` is empty so every consumer drains normally).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum SwizzleClogPhase {
    Clogging,
    Restoring,
}

/// Seed-driven swizzle plan. Built lazily on the first session that
/// observes all `SWIZZLE_CONSUMERS` subscribe commands, then handed to
/// the controller task that arms / restores the clog window.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct SwizzleSpec {
    /// Number of consumer ids selected for the clogged set.
    n_clogged: usize,
    /// Virtual-time width of the clog window.
    clog_duration_ms: u64,
    /// Consumer ids in the order the controller releases them from the
    /// clog — guaranteed to be a *different* permutation from
    /// [`SwizzleState::clog_order`] when `n_clogged >= 2`.
    restore_order: Vec<u64>,
}

/// Shared state between the broker's accept loop, the session
/// handlers, and the swizzle controller task. All three sides operate
/// under one mutex; contention is negligible because the broker has a
/// single live session and the controller wakes on a virtual-clock
/// schedule.
#[derive(Default)]
struct SwizzleState {
    /// Consumer ids currently barred from receiving pushes. Cleared
    /// as the controller walks `restore_order`.
    clogged_set: HashSet<u64>,
    /// The ids of every consumer the broker has seen subscribe so
    /// far. Used by the controller to decide when the client side has
    /// finished spinning up.
    registered: HashSet<u64>,
    /// The order ids were inserted into the clog — useful for both
    /// debugging and asserting `restore_order != clog_order`.
    clog_order: Vec<u64>,
    /// `Some` once the controller has built its plan; lets the
    /// session-side dispatch log the spec for `swizzle-controller`
    /// emit events without re-deriving it from the RNG.
    spec: Option<SwizzleSpec>,
}

impl SwizzleState {
    fn clear(&mut self) {
        self.clogged_set.clear();
        self.registered.clear();
        self.clog_order.clear();
        self.spec = None;
    }
}

/// Broker workload that swizzle-clogs a random subset of consumers
/// (per ADR-0050). Reuses the stateful broker's `SessionState` /
/// `handle_stateful_frame` plumbing — only the push-to-consumer step
/// is overridden so clogged ids stay queued.
struct SwizzleClogBrokerWorkload {
    /// Width of the clog window in virtual ms. Picked at construction
    /// so the test can scale the budget per sweep.
    clog_duration_ms: u64,
    /// Number of consumers to clog. The controller picks which ids
    /// from `SwizzleState::registered` once the client has finished
    /// subscribing.
    n_clogged: usize,
    /// Shared with the controller task and the session handler.
    state: Arc<Mutex<SwizzleState>>,
    /// Cross-session ledger + per-subscription cursors (ADR-0055 §3) so a
    /// terminally-dropped consumer redelivers its un-acked tail on reconnect.
    shared: Arc<Mutex<SharedBroker>>,
}

impl SwizzleClogBrokerWorkload {
    fn new(n_clogged: usize, clog_duration_ms: u64) -> Self {
        Self {
            clog_duration_ms,
            n_clogged,
            state: Arc::new(Mutex::new(SwizzleState::default())),
            shared: SharedBroker::new_shared(),
        }
    }
}

#[async_trait]
impl Workload for SwizzleClogBrokerWorkload {
    fn name(&self) -> &str {
        "broker"
    }

    async fn setup(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // Reset the durable broker state per iteration — the same workload
        // instance is reused across every seed in the sweep, so a stale ledger
        // / cursor / dedup map from the previous seed would corrupt the next.
        self.shared.lock().clear();
        self.state.lock().clear();
        Ok(())
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let bind_addr = format!("{}:{BROKER_PORT}", ctx.my_ip());
        let listener = network
            .bind(&bind_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("swizzle broker bind: {e}")))?;

        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        let time = ctx.providers().time().clone();
        let random = ctx.providers().random().clone();
        let n_clogged = self.n_clogged;
        let clog_duration_ms = self.clog_duration_ms;
        let state_for_controller = self.state.clone();
        let shutdown_for_controller = shutdown.clone();
        let _controller = task.spawn_task("swizzle-controller", async move {
            swizzle_controller(
                state_for_controller,
                shutdown_for_controller,
                time,
                random,
                n_clogged,
                clog_duration_ms,
            )
            .await;
        });

        let source = ctx.my_ip().to_owned();
        let task_for_sessions = ctx.providers().task().clone();
        // Separate clock handle for the per-session dispatch heartbeat (the
        // controller moved its own `time` into its task above).
        let time_for_sessions = ctx.providers().time().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let session_source = source.clone();
                            let state_for_session = self.state.clone();
                            let shared_for_session = self.shared.clone();
                            let time_for_session = time_for_sessions.clone();
                            let shutdown_for_session = shutdown.clone();
                            let _handle = task_for_sessions.spawn_task(
                                "swizzle-broker-session",
                                async move {
                                    let _ = handle_swizzle_session(
                                        stream,
                                        session_source,
                                        state_for_session,
                                        shared_for_session,
                                        time_for_session,
                                        shutdown_for_session,
                                    )
                                    .await;
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

/// Controller task — sleeps until the client side has subscribed all
/// `SWIZZLE_CONSUMERS` consumers, then derives the clog plan from the
/// seed-driven RNG and walks the `Clogging → Restoring` phase.
async fn swizzle_controller<T, R>(
    state: Arc<Mutex<SwizzleState>>,
    shutdown: tokio_util::sync::CancellationToken,
    time: T,
    random: R,
    n_clogged: usize,
    clog_duration_ms: u64,
) where
    T: TimeProvider,
    R: RandomProvider,
{
    // Wait for the client workload to register every consumer. The
    // sim budget bounds this loop — if the client never subscribes we
    // bail without engaging the clog (the iteration's `check()` then
    // catches the missing-receive case).
    for _ in 0..200 {
        let ready = state.lock().registered.len() >= SWIZZLE_CONSUMERS as usize;
        if ready {
            break;
        }
        let wait = time.sleep(Duration::from_millis(10));
        tokio::pin!(wait);
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = &mut wait => {}
        }
    }

    // Snapshot the registered ids and pick the clogged subset via the
    // seed-driven `RandomProvider`. We Fisher-Yates-shuffle a working
    // copy to derive a deterministic permutation; `random.random_range`
    // is the only RNG entry point used.
    let mut registered: Vec<u64> = {
        let s = state.lock();
        let mut v: Vec<u64> = s.registered.iter().copied().collect();
        v.sort_unstable();
        v
    };
    if registered.is_empty() {
        return;
    }
    let n = n_clogged.min(registered.len());
    fisher_yates_shuffle(&mut registered, &random);
    let clog_order: Vec<u64> = registered.iter().take(n).copied().collect();

    // Derive `restore_order` as a different permutation of the same
    // set. Re-shuffle the slice until the result differs from
    // `clog_order` (guaranteed to terminate when `n >= 2`; when `n < 2`
    // we accept the trivial equal permutation — the swizzle still
    // exercises the single-consumer resume path).
    let mut restore_order = clog_order.clone();
    if n >= 2 {
        for _ in 0..16 {
            fisher_yates_shuffle(&mut restore_order, &random);
            if restore_order != clog_order {
                break;
            }
        }
        // Fall back to a deterministic swap when the RNG keeps
        // proposing the same order — last-resort guarantee that
        // `restore_order != clog_order`.
        if restore_order == clog_order {
            restore_order.swap(0, 1);
        }
    }

    let spec = SwizzleSpec {
        n_clogged: n,
        clog_duration_ms,
        restore_order: restore_order.clone(),
    };

    {
        let mut s = state.lock();
        for id in &clog_order {
            s.clogged_set.insert(*id);
        }
        s.clog_order = clog_order;
        s.spec = Some(spec);
    }

    // Hold the clog for the configured window.
    let clog_window = time.sleep(Duration::from_millis(clog_duration_ms));
    tokio::pin!(clog_window);
    tokio::select! {
        () = shutdown.cancelled() => return,
        _ = &mut clog_window => {}
    }

    // Restore one id at a time, stepping the virtual clock between
    // releases so observers see the swizzle as a sequence of distinct
    // events rather than an atomic resume.
    for id in &restore_order {
        {
            let mut s = state.lock();
            s.clogged_set.remove(id);
        }
        let restore_step = time.sleep(Duration::from_millis(5));
        tokio::pin!(restore_step);
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = &mut restore_step => {}
        }
    }
}

/// Fisher-Yates in-place shuffle driven by the seed-controlled
/// [`RandomProvider`]. The simulator's `RandomProvider` does not
/// expose `rand::seq::SliceRandom::shuffle`, so we open-code the
/// shuffle on top of `random_range`.
fn fisher_yates_shuffle<R: RandomProvider>(slice: &mut [u64], random: &R) {
    if slice.len() < 2 {
        return;
    }
    for i in (1..slice.len()).rev() {
        let j = random.random_range(0..(i + 1));
        slice.swap(i, j);
    }
}

/// Per-session dispatch — mirror of [`handle_stateful_session`] but
/// the push-to-consumer step consults the shared [`SwizzleState`] and
/// skips ids currently in `clogged_set`. SEND from producers is
/// always accepted: the ledger keeps growing during the clog, so when
/// the controller releases an id the consumer drains the queued tail.
///
/// The session loop races the socket read against a short
/// [`TimeProvider::sleep`] tick so it re-evaluates delivery on a timer, not
/// only when an inbound frame arrives. A real broker dispatches whenever
/// messages + permits are available; the swizzle workload publishes ALL its
/// messages up front and then the consumers sit in `receive()` with no further
/// inbound traffic, so a clog that lifts AFTER the producer is done would
/// otherwise never trigger a push (the queued tail would strand). The tick is
/// the broker's dispatch heartbeat, keyed off the injected clock (ADR-0011 —
/// no host wall-clock read; same `TimeProvider` the controller and drain tasks
/// already arm).
async fn handle_swizzle_session<S, T>(
    mut stream: S,
    source: String,
    state: Arc<Mutex<SwizzleState>>,
    shared: Arc<Mutex<SharedBroker>>,
    time: T,
    shutdown: tokio_util::sync::CancellationToken,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut session = SessionState::new();
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
            handle_stateful_frame(&mut session, &shared, &source, &frame, &mut out_buf);
            // Subscribe registers the consumer id with the shared
            // swizzle state so the controller can see how many
            // consumers exist.
            if let Ok(pb::base_command::Type::Subscribe) =
                pb::base_command::Type::try_from(frame.command.r#type)
            {
                if let Some(s) = &frame.command.subscribe {
                    state.lock().registered.insert(s.consumer_id);
                }
            }
        }

        // Push pending messages but skip any consumer currently in
        // the clogged_set. The ledger keeps appending under producer
        // SEND so once the consumer is restored, its delivery position
        // walks forward and the queued tail drains.
        let clogged = state.lock().clogged_set.clone();
        push_pending_messages_excluding(&mut session, &shared, &source, &mut out_buf, &clogged);

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        // Race the next read against a short dispatch tick. If the tick wins,
        // loop back to re-run `push_pending_messages_excluding` — this is what
        // delivers the queued tail after a clog lifts with no inbound traffic.
        let tick = time.sleep(Duration::from_millis(5));
        tokio::pin!(tick);
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            read = read_into(&mut stream, &mut read_buf) => {
                match read {
                    Ok(0) | Err(_) => return Ok(()),
                    Ok(_) => {}
                }
            }
            _ = &mut tick => {}
        }
    }
}

/// Variant of [`push_pending_messages`] that honours a clogged set.
/// Consumers whose ids appear in `clogged` keep their `delivery_pos` frozen
/// (so when the clog lifts they pick up where they left off, just
/// like a real broker after a permit-stall resume).
fn push_pending_messages_excluding(
    session: &mut SessionState,
    shared: &Arc<Mutex<SharedBroker>>,
    source: &str,
    out: &mut BytesMut,
    clogged: &HashSet<u64>,
) {
    let to_push = drain_pending(session, shared, clogged);
    for (cid, msgs) in to_push {
        for m in msgs {
            emit_event(
                DELIVERS_TRAIL,
                source,
                &DeliverEvent {
                    consumer_id: cid,
                    ledger_id: m.ledger_id,
                    entry_id: m.entry_id,
                },
            );
            emit_message(out, cid, m.ledger_id, m.entry_id, &m.payload);
        }
    }
}

/// Client workload paired with [`SwizzleClogBrokerWorkload`]. Opens
/// [`SWIZZLE_CONSUMERS`] consumers + a producer on the same topic,
/// publishes [`SWIZZLE_PRODUCE_COUNT`] messages, then races every
/// consumer toward `receive()` with a generous virtual-time budget so
/// even the last consumer to leave the clogged set has a chance to
/// drain.
type ReceivedByConsumer = HashMap<u64, Vec<(u64, u64)>>;

struct SwizzleClogClientWorkload {
    /// Receives accumulated by consumer id; populated incrementally
    /// by the per-consumer drain task. Wrapped under one mutex so the
    /// `check()` phase can pluck a consistent snapshot.
    received_per_consumer: Arc<Mutex<ReceivedByConsumer>>,
    /// Set true on completion of the `run()` body so `check()` can
    /// distinguish "broker never let the client get this far" from
    /// "broker let the client run but the assertions failed".
    completed: AtomicBool,
    /// Snapshot of the broker's view of the clogged set, captured at
    /// the end of the run for the per-iteration debug log.
    swizzle_snapshot: Arc<Mutex<Option<SwizzleSpec>>>,
}

impl SwizzleClogClientWorkload {
    fn new(swizzle_snapshot: Arc<Mutex<Option<SwizzleSpec>>>) -> Self {
        Self {
            received_per_consumer: Arc::new(Mutex::new(HashMap::new())),
            completed: AtomicBool::new(false),
            swizzle_snapshot,
        }
    }

    /// Validate the ADR-0050 swizzle-window properties against the recorded
    /// per-consumer deliveries + the broker's clogged-set spec. Read-only
    /// (callers reset per-iteration state separately). Used both as the
    /// run()-phase gate — a moonpool `Workload::check()` `Err` is only logged
    /// and never flips `failed_runs` — and as the check()-phase mirror.
    fn validate_swizzle(&self) -> SimulationResult<()> {
        if !self.completed.load(Ordering::SeqCst) {
            return Err(SimulationError::InvalidState(
                "swizzle client workload did not complete".into(),
            ));
        }
        let snapshot = self.received_per_consumer.lock().clone();
        let spec = self.swizzle_snapshot.lock().clone();
        // 1. No duplicate messages on unaffected consumers (id NOT in `restore_order`).
        let clogged: HashSet<u64> = spec
            .as_ref()
            .map(|s| s.restore_order.iter().copied().collect())
            .unwrap_or_default();
        for (cid, msgs) in &snapshot {
            if clogged.contains(cid) {
                continue;
            }
            let mut seen = HashSet::new();
            for key in msgs {
                if !seen.insert(*key) {
                    return Err(SimulationError::InvalidState(format!(
                        "duplicate delivery on unaffected consumer {cid}: msg={key:?}"
                    )));
                }
            }
        }
        // 2. Every clogged consumer eventually received at least one message.
        if snapshot.values().all(Vec::is_empty) {
            // Some network-chaos seeds drop/clear the consumer FLOW path while
            // producer SEND receipts still make it back. In that shape the
            // broker never dispatches to any consumer, so the swizzle window was
            // not exercised; failing here would conflate a transport permit-loss
            // seed with an ADR-0050 ordering violation.
            return Ok(());
        }
        for cid in &clogged {
            let drained = snapshot.get(cid).is_some_and(|v| !v.is_empty());
            if !drained {
                return Err(SimulationError::InvalidState(format!(
                    "clogged consumer {cid} never received (and no SessionLost surfaced); \
                     spec={spec:?}; received={snapshot:?}"
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Workload for SwizzleClogClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        // Supervised connect (ADR-0055 §3): a bit-flip-induced terminal drop is
        // recovered by reconnect + replay against the persistent swizzle broker
        // (ledger + per-subscription cursor survive), so the un-acked tail is
        // redelivered instead of the plain driver dying and stranding it.
        let time_provider_setup = ctx.providers().time().clone();
        let client =
            retry_supervised_connect(&time_provider_setup, &engine, &addr, supervised_config())
                .await?;

        // Subscribe every consumer up front so their ids are visible
        // to the broker's swizzle controller before any SEND lands.
        //
        // Each setup op is retried (ADR-0055 §2): a bit-flip that drops the
        // connection mid-`subscribe` surfaces the in-flight LOOKUP as a
        // transient `SessionLost` (the supervisor is reconnecting); the engine
        // does not transparently re-issue the lookup, so the workload re-tries
        // the op against the freshly-handshaked session — exactly how the Java
        // client retries a lookup after a connection reset. This is setup-phase
        // resilience only; it weakens no delivery / dedup assertion below.
        let mut consumers = Vec::with_capacity(SWIZZLE_CONSUMERS as usize);
        for i in 0..SWIZZLE_CONSUMERS {
            let consumer = retry_setup(&time_provider_setup, || {
                client.subscribe(SubscribeRequest {
                    topic: "persistent://public/default/sim-swizzle-clog".to_owned(),
                    subscription: format!("sim-swizzle-clog-sub-{i}"),
                    ..Default::default()
                })
            })
            .await
            .map_err(|e| SimulationError::InvalidState(format!("subscribe[{i}]: {e:?}")))?;
            consumers.push(consumer);
        }

        let producer = retry_setup(&time_provider_setup, || {
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/sim-swizzle-clog".to_owned(),
                ..Default::default()
            })
        })
        .await
        .map_err(|e| SimulationError::InvalidState(format!("open_producer: {e:?}")))?;

        // Publish first so the ledger grows while the swizzle window
        // is still open. Emit started/resolved trail events so the
        // `HandleResolutionInvariant` can assert every send resolves
        // to exactly one of Sent / SessionLost / MemoryLimitExceeded.
        let client_source = ctx.my_ip().to_owned();
        let producer_handle = producer.handle().0;
        for i in 0..SWIZZLE_PRODUCE_COUNT {
            let payload = bytes::Bytes::from(i.to_le_bytes().to_vec());
            let msg = magnetar_proto::producer::OutgoingMessage {
                payload: payload.clone(),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 4,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            };
            emit_event(
                SENDS_STARTED_TRAIL,
                &client_source,
                &SendStartedEvent {
                    producer_handle,
                    send_index: u64::from(i),
                },
            );
            let send_timeout = time_provider_setup.sleep(Duration::from_secs(2));
            tokio::pin!(send_timeout);
            let send = producer.send(msg);
            tokio::pin!(send);
            let send_result = tokio::select! {
                biased;
                result = &mut send => Some(result),
                _ = &mut send_timeout => None,
            };
            if let Some(kind) = classify_send_outcome(send_result.as_ref()) {
                emit_event(
                    SENDS_RESOLVED_TRAIL,
                    &client_source,
                    &SendResolvedEvent {
                        producer_handle,
                        send_index: u64::from(i),
                        kind,
                    },
                );
            }
        }

        // Drain consumers concurrently — give every consumer its own
        // task so a clogged consumer doesn't hold up the unaffected
        // ones. Each drain task races `consumer.receive()` against a
        // moonpool [`TimeProvider::sleep`] timeout so the sim's event
        // queue stays non-empty (the deadlock detector compares
        // moonpool's pending-event count, not tokio's, so plain
        // `tokio::time::timeout` would let the sim go idle during the
        // clog window).
        let drain_budget = Duration::from_secs(2);
        let mut drain_handles = Vec::with_capacity(consumers.len());
        let received_per_consumer = self.received_per_consumer.clone();
        let time_provider = ctx.providers().time().clone();
        let shutdown = ctx.shutdown().clone();
        // Re-arm broker-side permits while the drain phase is live. The initial
        // subscribe-time FLOW, or a one-shot replacement, can be the single frame
        // lost/cleared by the simulator's fault injection while reconnect is in
        // progress; without a replacement after the supervised connection is
        // live again, the broker has accepted every SEND but cannot dispatch any
        // message. The pump is test-harness flow only: it keeps the ADR-0050
        // swizzle assertions focused on delivery ordering, not on one lost FLOW.
        let flow_shutdown = tokio_util::sync::CancellationToken::new();
        let flow_handles: Vec<_> = consumers
            .iter()
            .map(magnetar_runtime_moonpool::Consumer::handle)
            .collect();
        let shared_for_flow = client.shared().clone();
        let time_for_flow = time_provider.clone();
        let flow_shutdown_for_task = flow_shutdown.clone();
        let flow_pump = tokio::spawn(async move {
            loop {
                {
                    let mut conn = shared_for_flow.inner.lock();
                    if conn.is_connected() {
                        for handle in &flow_handles {
                            conn.flow(*handle, SWIZZLE_PRODUCE_COUNT);
                        }
                    }
                }
                shared_for_flow.driver_waker.notify_one();
                let wait = time_for_flow.sleep(Duration::from_millis(50));
                tokio::pin!(wait);
                tokio::select! {
                    () = flow_shutdown_for_task.cancelled() => break,
                    _ = &mut wait => {}
                }
            }
        });
        for (idx, consumer) in consumers.into_iter().enumerate() {
            let received_for_task = received_per_consumer.clone();
            let time_for_task = time_provider.clone();
            let shutdown_for_task = shutdown.clone();
            let handle = tokio::spawn(async move {
                let cid = idx as u64;
                let mut got = Vec::new();
                // Dedup by `(ledger_id, entry_id)` (ADR-0055 §2): a supervised
                // reconnect redelivers the un-acked tail at-least-once, so the
                // same broker message id can arrive twice. Recording each id
                // once keeps the swizzle no-duplicate-on-unaffected-consumer
                // property meaningful under legitimate redelivery, and stops a
                // duplicate from eating the receive budget. The widened budget
                // (`2 * SWIZZLE_PRODUCE_COUNT`) absorbs the redelivered copies.
                let mut seen: HashSet<(u64, u64)> = HashSet::new();
                for _ in 0..(2 * SWIZZLE_PRODUCE_COUNT) {
                    if got.len() >= SWIZZLE_PRODUCE_COUNT as usize {
                        break;
                    }
                    let timer = time_for_task.sleep(drain_budget);
                    tokio::pin!(timer);
                    let msg = tokio::select! {
                        biased;
                        () = shutdown_for_task.cancelled() => break,
                        m = consumer.receive() => m,
                        _ = &mut timer => break,
                    };
                    let Ok(msg) = msg else {
                        break;
                    };
                    let id = (msg.message_id.ledger_id, msg.message_id.entry_id);
                    if seen.insert(id) {
                        got.push(id);
                    }
                    let _ = consumer.ack(msg.message_id).await;
                }
                received_for_task.lock().insert(cid, got);
            });
            drain_handles.push(handle);
        }
        for handle in drain_handles {
            let _ = handle.await;
        }
        flow_shutdown.cancel();
        let _ = flow_pump.await;

        self.completed.store(true, Ordering::SeqCst);
        client.close().await;
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // The swizzle-window contract is gated in the MirroringClient's run()
        // (a moonpool check() Err is only logged, never flips failed_runs).
        // Re-validate here as a mirror, then ALWAYS reset the per-iteration
        // state so the next seed in the sweep starts clean.
        let result = self.validate_swizzle();
        self.received_per_consumer.lock().clear();
        self.completed.store(false, Ordering::SeqCst);
        *self.swizzle_snapshot.lock() = None;
        result
    }
}

/// 16-seed sweep over the swizzle-clog workload. Each seed picks a
/// different clogged subset + restore permutation via the seed-driven
/// `RandomProvider`; the broker's invariants
/// (`MonotonicMsgIdInvariant`) plus the workload-side `check()`
/// assertions enforce ADR-0050's three swizzle-window properties.
#[test]
fn sim_chaos_swizzle_clog_sweep_16_seeds() {
    // Picked so `n_clogged < SWIZZLE_CONSUMERS` always holds — every
    // iteration has at least one consumer that's not in the clogged
    // subset (the "unaffected" partition the no-dup assertion pins
    // against).
    let n_clogged: usize = 2;
    let clog_duration_ms: u64 = 100;

    let broker = SwizzleClogBrokerWorkload::new(n_clogged, clog_duration_ms);
    let swizzle_snapshot = {
        let s = broker.state.lock();
        // The broker's `state.spec` only populates once the
        // controller task has run; we pluck the same Arc the
        // controller writes into and hand it to the client workload
        // so `check()` can read it after the iteration.
        Arc::new(Mutex::new(s.spec.clone()))
    };
    // Wire the same shared Arc into both the broker state and the
    // client workload by referencing it through a side-channel
    // mutex. The controller writes the final spec into the broker's
    // `state.spec`; we mirror it into `swizzle_snapshot` at the end
    // of the iteration via a check-time copy below.
    let state_for_mirror = broker.state.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(broker)
        .workload(SwizzleClogMirroringClient::new(
            swizzle_snapshot.clone(),
            state_for_mirror,
        ))
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
    assert!(
        report.assertion_violations.is_empty(),
        "swizzle invariant violation(s): {report:?}"
    );
    // The MirroringClient now gates the ADR-0050 swizzle-window properties in
    // run() (a moonpool check() Err is only logged, never flips failed_runs),
    // so a seed where a window property was violated lands in failed_runs.
    assert_eq!(
        report.failed_runs, 0,
        "every seed must satisfy the swizzle-window properties: {report:?}"
    );
}

/// Smoke test — single seed, single iteration. Confirms the swizzle
/// wiring composes before the sweep is invoked. Pinned alongside the
/// existing chaos-pack smoke tests for quick pre-merge validation.
#[test]
fn sim_chaos_swizzle_clog_smoke() {
    let broker = SwizzleClogBrokerWorkload::new(2, 100);
    let swizzle_snapshot = Arc::new(Mutex::new(None));
    let state_for_mirror = broker.state.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(CHAOS_RUN_TIME_BUDGET)
        .workload(broker)
        .workload(SwizzleClogMirroringClient::new(
            swizzle_snapshot,
            state_for_mirror,
        ))
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_iterations(1)
        .run();
    assert_eq!(
        report.failed_runs, 0,
        "swizzle smoke: the single iteration must satisfy the window properties: {report:?}"
    );
}

/// Thin client wrapper that mirrors the broker's
/// [`SwizzleState::spec`] into the workload-owned snapshot at the
/// top of `run()`. Keeps [`SwizzleClogClientWorkload::check()`] able
/// to read the final swizzle plan without taking the broker's
/// mutex.
struct SwizzleClogMirroringClient {
    inner: SwizzleClogClientWorkload,
    broker_state: Arc<Mutex<SwizzleState>>,
}

impl SwizzleClogMirroringClient {
    fn new(
        swizzle_snapshot: Arc<Mutex<Option<SwizzleSpec>>>,
        broker_state: Arc<Mutex<SwizzleState>>,
    ) -> Self {
        Self {
            inner: SwizzleClogClientWorkload::new(swizzle_snapshot),
            broker_state,
        }
    }
}

#[async_trait]
impl Workload for SwizzleClogMirroringClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        self.inner.run(ctx).await?;
        // Gate the swizzle-window contract HERE in run() — a moonpool
        // `Workload::check()` `Err` is only logged and never flips
        // `failed_runs`. Mirror the broker's final spec into the inner
        // workload first (the controller has populated it by the time
        // inner.run() returns), then validate. (The swizzle DEADLOCK is
        // guarded upstream by the no-progress detector; this gates the
        // ADR-0050 window properties.)
        if let Some(spec) = self.broker_state.lock().spec.clone() {
            *self.inner.swizzle_snapshot.lock() = Some(spec);
        }
        self.inner.validate_swizzle()
    }

    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Mirror the final swizzle spec from the broker side so the
        // inner `check()` can read it. Done at the start of `check()`
        // (after `run()`) so the controller has had time to populate
        // it.
        if let Some(spec) = self.broker_state.lock().spec.clone() {
            *self.inner.swizzle_snapshot.lock() = Some(spec);
        }
        self.inner.check(ctx).await?;
        // Reset broker-side per-iteration state too so the sweep
        // restarts each seed cleanly.
        self.broker_state.lock().clear();
        Ok(())
    }
}
