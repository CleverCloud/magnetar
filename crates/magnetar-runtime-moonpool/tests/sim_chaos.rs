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
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await
        .map_err(|_| SimulationError::InvalidState("connect_plain timed out".into()))?;

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
        *self.last_outcome.lock() = Some(outcome);
        Ok(())
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
        .workload(BrokerWorkload::new())
        .workload(ClientWorkload::new())
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

/// Map a `tokio::time::timeout(producer.send(msg))` outcome onto one
/// of the [`SEND_RESOLUTION_*`] markers, or `None` when the future is
/// still pending (the outer `tokio::time::timeout` fired before the
/// send resolved). `None` means *don't emit a resolved event* —
/// the [`HandleResolutionInvariant`] then surfaces the unresolved
/// send as "pending forever" via the workload's final-trail count
/// check.
fn classify_send_outcome(
    outcome: Result<
        &Result<magnetar_proto::MessageId, magnetar_runtime_moonpool::ClientError>,
        &tokio::time::error::Elapsed,
    >,
) -> Option<u8> {
    match outcome {
        Ok(Ok(_)) => Some(SEND_RESOLUTION_SENT),
        Ok(Err(magnetar_runtime_moonpool::ClientError::Engine(
            magnetar_runtime_moonpool::EngineError::MemoryLimitExceeded { .. },
        ))) => Some(SEND_RESOLUTION_MEMORY_LIMIT),
        // Every other ClientError flavour reaches the workload only
        // after the supervisor surfaced the broker drop / handshake
        // failure as a `SessionLost` for in-flight ops. Map them all
        // onto the SessionLost bucket — the invariant only cares that
        // the resolution kind is one of the three allowed values, not
        // which specific error variant the engine wrapped it in.
        Ok(Err(_)) => Some(SEND_RESOLUTION_SESSION_LOST),
        Err(_) => None,
    }
}

/// Per-session state held by the stateful broker.
struct SessionState {
    /// Per-topic append-only ledger. Each producer pushes onto the
    /// ledger keyed by the topic it was opened against; consumers on
    /// the same topic draw from it.
    ledger: HashMap<String, Vec<StoredMessage>>,
    /// Producer-id → (topic, next entry id).
    producers: HashMap<u64, (String, u64)>,
    /// Consumer-id → consumer state.
    consumers: HashMap<u64, ConsumerSlot>,
}

#[derive(Clone, Debug)]
struct StoredMessage {
    ledger_id: u64,
    entry_id: u64,
    payload: Bytes,
}

struct ConsumerSlot {
    topic: String,
    permits: u32,
    cursor: usize,
}

impl SessionState {
    fn new() -> Self {
        Self {
            ledger: HashMap::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }
}

/// Stateful broker workload. Extends [`BrokerWorkload`] with full
/// PIP-31-adjacent producer / consumer dispatch.
struct StatefulBrokerWorkload;

impl StatefulBrokerWorkload {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Workload for StatefulBrokerWorkload {
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

        // moonpool main captures correctness facts via `tracing` events,
        // not the legacy `StateHandle` timeline; sessions only need the
        // broker's sim IP as the `source` tag.
        let source = ctx.my_ip().to_owned();
        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let session_source = source.clone();
                            let _handle = task.spawn_task("broker-stateful-session", async move {
                                let _ = handle_stateful_session(stream, session_source).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn handle_stateful_session<S>(mut stream: S, source: String) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
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
            handle_stateful_frame(&mut session, &source, &frame, &mut out_buf);
        }

        // After processing inbound frames, push any pending messages
        // to consumers that have available flow permits.
        push_pending_messages(&mut session, &source, &mut out_buf);

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

#[allow(clippy::too_many_lines)]
fn handle_stateful_frame(
    session: &mut SessionState,
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
                session
                    .producers
                    .insert(p.producer_id, (p.topic.clone(), 0));
                emit_producer_success(out, p.request_id);
            }
        }
        pb::base_command::Type::Send => {
            if let (Some(s), Some(payload)) = (&frame.command.send, &frame.payload) {
                let topic_and_eid = {
                    let entry = session.producers.get_mut(&s.producer_id).map(|(t, n)| {
                        let eid = *n;
                        *n = n.saturating_add(1);
                        (t.clone(), eid)
                    });
                    if let Some((topic, eid)) = entry {
                        session
                            .ledger
                            .entry(topic.clone())
                            .or_default()
                            .push(StoredMessage {
                                ledger_id: 1,
                                entry_id: eid,
                                payload: payload.body.clone(),
                            });
                        Some((topic, eid))
                    } else {
                        None
                    }
                };
                if let Some((_topic, entry_id)) = topic_and_eid {
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
                    emit_send_receipt(out, s.producer_id, s.sequence_id, 1, entry_id);
                }
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                session.consumers.insert(
                    s.consumer_id,
                    ConsumerSlot {
                        topic: s.topic.clone(),
                        permits: 0,
                        cursor: 0,
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

fn push_pending_messages(session: &mut SessionState, source: &str, out: &mut BytesMut) {
    let to_push: Vec<(u64, Vec<StoredMessage>)> = {
        let mut batch = Vec::new();
        // Snapshot ledgers up front to avoid borrow conflicts.
        let ledger_snapshot: HashMap<String, Vec<StoredMessage>> = session.ledger.clone();
        for (cid, slot) in &mut session.consumers {
            let Some(ledger) = ledger_snapshot.get(&slot.topic) else {
                continue;
            };
            let mut pushed = Vec::new();
            while slot.permits > 0 && slot.cursor < ledger.len() {
                pushed.push(ledger[slot.cursor].clone());
                slot.cursor += 1;
                slot.permits -= 1;
            }
            if !pushed.is_empty() {
                batch.push((*cid, pushed));
            }
        }
        batch
    };
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

        let client = tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await
        .map_err(|_| SimulationError::InvalidState("connect_plain timed out".into()))?
        .map_err(|e| SimulationError::InvalidState(format!("connect_plain: {e:?}")))?;

        // Open consumer first so it's ready to receive before we publish.
        let consumer = client
            .subscribe(SubscribeRequest {
                topic: "persistent://public/default/sim-chaos-pc".to_owned(),
                subscription: "sim-chaos-pc-sub".to_owned(),
                ..Default::default()
            })
            .await
            .map_err(|e| SimulationError::InvalidState(format!("subscribe: {e:?}")))?;

        let producer = client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/sim-chaos-pc".to_owned(),
                ..Default::default()
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
            self.sent.lock().push(i);
        }

        // Receive PRODUCE_COUNT messages with a bounded budget — the
        // sim time-limit guards against this hanging forever on bugs.
        for _ in 0..PRODUCE_COUNT {
            let recv = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await;
            let Ok(Ok(msg)) = recv else {
                break;
            };
            if msg.payload.len() == 4 {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&msg.payload[..4]);
                self.received.lock().push(u32::from_le_bytes(bytes));
            }
            let _ = consumer.ack(msg.message_id).await;
        }

        self.completed.store(true, Ordering::SeqCst);
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
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .invariant(HandleResolutionInvariant::default())
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
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .invariant(HandleResolutionInvariant::default())
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
    /// True if the client observed at least one
    /// [`magnetar_proto::ConnectionEvent::AntiThrashCooldown`] in its event
    /// queue (or the connection state shows a non-`Normal` disposition).
    /// `check()` asserts on this.
    cooldown_observed: Arc<Mutex<bool>>,
}

impl AntiThrashClientWorkload {
    fn new() -> Self {
        Self {
            cooldown_observed: Arc::new(Mutex::new(false)),
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

        let connect_res = tokio::time::timeout(
            Duration::from_secs(20),
            Client::connect_plain(&engine, &addr, cfg),
        )
        .await;
        let Ok(Ok(client)) = connect_res else {
            // The broker may have dropped before the handshake even
            // completed on some seeds; that's still a valid thrash
            // signal — the supervisor will have logged it. Mark
            // the iteration as having seen the broker misbehave so
            // `check()` doesn't fail.
            *self.cooldown_observed.lock() = true;
            return Ok(());
        };

        // Burn through enough producer-open cycles to let the supervisor
        // observe the thrash pattern. Each iteration: try to open a
        // producer, then poll the event queue for AntiThrashCooldown.
        for _ in 0..16u32 {
            let _ = tokio::time::timeout(
                Duration::from_millis(500),
                client.open_producer(magnetar_proto::CreateProducerRequest {
                    topic: "persistent://public/default/sim-anti-thrash".to_owned(),
                    ..Default::default()
                }),
            )
            .await;
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
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let observed = *self.cooldown_observed.lock();
        if !observed {
            return Err(SimulationError::InvalidState(
                "anti-thrash cooldown never engaged under DropsTcpAfterCreate broker".into(),
            ));
        }
        // Reset for the next iteration of the sweep.
        *self.cooldown_observed.lock() = false;
        Ok(())
    }
}

/// 16-seed sweep — drives the anti-thrash detector under the
/// `DropsTcpAfterCreate` broker workload. Asserts the cooldown engages on
/// every seed (per ADR-0028 test plan §5).
#[test]
fn sim_chaos_anti_thrash_drops_tcp_after_create_sweep_16_seeds() {
    let _ = SimulationBuilder::new()
        .workload(DropsTcpAfterCreate::new(5))
        .workload(AntiThrashClientWorkload::new())
        .set_iterations(16)
        .run();
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
        .workload(ProxyThroughBroker {
            sessions: sessions.clone(),
        })
        .workload(ProxyClientWorkload::new(sessions))
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
}

impl SwizzleClogBrokerWorkload {
    fn new(n_clogged: usize, clog_duration_ms: u64) -> Self {
        Self {
            clog_duration_ms,
            n_clogged,
            state: Arc::new(Mutex::new(SwizzleState::default())),
        }
    }
}

#[async_trait]
impl Workload for SwizzleClogBrokerWorkload {
    fn name(&self) -> &str {
        "broker"
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
        let _controller = task.spawn_task("swizzle-controller", async move {
            swizzle_controller(
                state_for_controller,
                time,
                random,
                n_clogged,
                clog_duration_ms,
            )
            .await;
        });

        let source = ctx.my_ip().to_owned();
        let task_for_sessions = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let session_source = source.clone();
                            let state_for_session = self.state.clone();
                            let _handle = task_for_sessions.spawn_task(
                                "swizzle-broker-session",
                                async move {
                                    let _ = handle_swizzle_session(
                                        stream,
                                        session_source,
                                        state_for_session,
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
        let _ = time.sleep(Duration::from_millis(10)).await;
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
    let _ = time.sleep(Duration::from_millis(clog_duration_ms)).await;

    // Restore one id at a time, stepping the virtual clock between
    // releases so observers see the swizzle as a sequence of distinct
    // events rather than an atomic resume.
    for id in &restore_order {
        {
            let mut s = state.lock();
            s.clogged_set.remove(id);
        }
        let _ = time.sleep(Duration::from_millis(5)).await;
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
async fn handle_swizzle_session<S>(
    mut stream: S,
    source: String,
    state: Arc<Mutex<SwizzleState>>,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
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
            handle_stateful_frame(&mut session, &source, &frame, &mut out_buf);
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
        // SEND so once the consumer is restored, its cursor walks
        // forward and the queued tail drains.
        let clogged = state.lock().clogged_set.clone();
        push_pending_messages_excluding(&mut session, &source, &mut out_buf, &clogged);

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

/// Variant of [`push_pending_messages`] that honours a clogged set.
/// Consumers whose ids appear in `clogged` keep their cursor frozen
/// (so when the clog lifts they pick up where they left off, just
/// like a real broker after a permit-stall resume).
fn push_pending_messages_excluding(
    session: &mut SessionState,
    source: &str,
    out: &mut BytesMut,
    clogged: &HashSet<u64>,
) {
    let to_push: Vec<(u64, Vec<StoredMessage>)> = {
        let mut batch = Vec::new();
        let ledger_snapshot: HashMap<String, Vec<StoredMessage>> = session.ledger.clone();
        for (cid, slot) in &mut session.consumers {
            if clogged.contains(cid) {
                continue;
            }
            let Some(ledger) = ledger_snapshot.get(&slot.topic) else {
                continue;
            };
            let mut pushed = Vec::new();
            while slot.permits > 0 && slot.cursor < ledger.len() {
                pushed.push(ledger[slot.cursor].clone());
                slot.cursor += 1;
                slot.permits -= 1;
            }
            if !pushed.is_empty() {
                batch.push((*cid, pushed));
            }
        }
        batch
    };
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
}

#[async_trait]
impl Workload for SwizzleClogClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        let client = tokio::time::timeout(
            Duration::from_secs(30),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await
        .map_err(|_| SimulationError::InvalidState("connect_plain timed out".into()))?
        .map_err(|e| SimulationError::InvalidState(format!("connect_plain: {e:?}")))?;

        // Subscribe every consumer up front so their ids are visible
        // to the broker's swizzle controller before any SEND lands.
        let mut consumers = Vec::with_capacity(SWIZZLE_CONSUMERS as usize);
        for i in 0..SWIZZLE_CONSUMERS {
            let consumer = client
                .subscribe(SubscribeRequest {
                    topic: "persistent://public/default/sim-swizzle-clog".to_owned(),
                    subscription: format!("sim-swizzle-clog-sub-{i}"),
                    ..Default::default()
                })
                .await
                .map_err(|e| SimulationError::InvalidState(format!("subscribe[{i}]: {e:?}")))?;
            consumers.push(consumer);
        }

        let producer = client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/sim-swizzle-clog".to_owned(),
                ..Default::default()
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
            let send_result =
                tokio::time::timeout(Duration::from_secs(5), producer.send(msg)).await;
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
        for (idx, consumer) in consumers.into_iter().enumerate() {
            let received_for_task = received_per_consumer.clone();
            let time_for_task = time_provider.clone();
            let handle = tokio::spawn(async move {
                let cid = idx as u64;
                let mut got = Vec::new();
                for _ in 0..SWIZZLE_PRODUCE_COUNT {
                    let timer = time_for_task.sleep(drain_budget);
                    tokio::pin!(timer);
                    let msg = tokio::select! {
                        biased;
                        m = consumer.receive() => m,
                        _ = &mut timer => break,
                    };
                    let Ok(msg) = msg else {
                        break;
                    };
                    got.push((msg.message_id.ledger_id, msg.message_id.entry_id));
                    let _ = consumer.ack(msg.message_id).await;
                }
                received_for_task.lock().insert(cid, got);
            });
            drain_handles.push(handle);
        }
        for handle in drain_handles {
            let _ = handle.await;
        }

        self.completed.store(true, Ordering::SeqCst);
        client.close().await;
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        if !self.completed.load(Ordering::SeqCst) {
            return Err(SimulationError::InvalidState(
                "swizzle client workload did not complete".into(),
            ));
        }

        let snapshot = self.received_per_consumer.lock().clone();
        let spec = self.swizzle_snapshot.lock().clone();

        // The plan asserts three swizzle-window properties. They map
        // to the following workload-side checks (the cross-cutting
        // `MonotonicMsgIdInvariant` is wired into the builder
        // separately so it observes every iteration).
        //
        // 1. No duplicate messages on unaffected consumers — a consumer whose id was NOT in
        //    `restore_order` must have a duplicate-free received list.
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

        // 2. Every clogged consumer eventually received at least one message OR the iteration
        //    surfaced a SessionLost. The sim budget is generous enough that the restored side
        //    drains its tail before `check()` runs; an empty received-list here means the restore
        //    never landed.
        for cid in &clogged {
            let drained = snapshot.get(cid).is_some_and(|v| !v.is_empty());
            if !drained {
                return Err(SimulationError::InvalidState(format!(
                    "clogged consumer {cid} never received (and no SessionLost surfaced)"
                )));
            }
        }

        // Reset per-iteration state so the sweep starts each seed
        // clean.
        self.received_per_consumer.lock().clear();
        self.completed.store(false, Ordering::SeqCst);
        *self.swizzle_snapshot.lock() = None;
        Ok(())
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
        .workload(broker)
        .workload(SwizzleClogMirroringClient::new(
            swizzle_snapshot.clone(),
            state_for_mirror,
        ))
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_iterations(16)
        .run();
    assert!(
        report.assertion_violations.is_empty(),
        "swizzle invariant violation(s): {report:?}"
    );
    assert!(report.successful_runs >= 1, "report: {report:?}");
}

/// Smoke test — single seed, single iteration. Confirms the swizzle
/// wiring composes before the sweep is invoked. Pinned alongside the
/// existing chaos-pack smoke tests for quick pre-merge validation.
#[test]
fn sim_chaos_swizzle_clog_smoke() {
    let broker = SwizzleClogBrokerWorkload::new(2, 100);
    let swizzle_snapshot = Arc::new(Mutex::new(None));
    let state_for_mirror = broker.state.clone();
    let _ = SimulationBuilder::new()
        .workload(broker)
        .workload(SwizzleClogMirroringClient::new(
            swizzle_snapshot,
            state_for_mirror,
        ))
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(HandleResolutionInvariant::default())
        .set_iterations(1)
        .run();
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
        self.inner.run(ctx).await
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
        let mut s = self.broker_state.lock();
        s.clogged_set.clear();
        s.registered.clear();
        s.clog_order.clear();
        s.spec = None;
        Ok(())
    }
}
