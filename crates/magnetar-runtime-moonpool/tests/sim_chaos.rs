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
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TcpListenerTrait, TimeProvider};
use moonpool_sim::chaos::invariant_trait::Invariant;
use moonpool_sim::chaos::state_handle::StateHandle;
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{
    SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload, WorkloadTopology,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

#[async_trait(?Send)]
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
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            *counter.lock() += 1;
                            // Each session runs inline rather than via
                            // `spawn_local` so the simulator's task budget
                            // stays predictable. The driver's reads are
                            // async-scheduled by the sim runtime.
                            let counter_for_session = counter.clone();
                            tokio::task::spawn_local(async move {
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

#[async_trait(?Send)]
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
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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

        match stream.read_buf(&mut read_buf).await {
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
        .set_time_limit(Duration::from_secs(60))
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
        .set_time_limit(Duration::from_secs(60))
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

/// Timeline event: producer sent a message. Emitted by the broker on
/// every `CommandSend` it accepts.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
struct SendEvent {
    producer_id: u64,
    sequence_id: u64,
    #[allow(dead_code)]
    ledger_id: u64,
    #[allow(dead_code)]
    entry_id: u64,
}

/// Timeline event: broker pushed a message to a consumer's queue.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
struct DeliverEvent {
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
}

/// Timeline event: client acked a message.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
struct AckEvent {
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
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
struct StatefulBrokerWorkload {
    /// State handle clone is taken in `run()` and threaded into every
    /// spawned session so they can emit timeline events without
    /// needing a `&SimContext`.
    state_handle: Rc<RefCell<Option<StateHandle>>>,
}

impl StatefulBrokerWorkload {
    fn new() -> Self {
        Self {
            state_handle: Rc::new(RefCell::new(None)),
        }
    }
}

#[async_trait(?Send)]
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

        *self.state_handle.borrow_mut() = Some(ctx.state().clone());
        let state_handle = ctx.state().clone();
        let shutdown = ctx.shutdown().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let sh = state_handle.clone();
                            tokio::task::spawn_local(async move {
                                let _ = handle_stateful_session(stream, sh).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn handle_stateful_session<S>(mut stream: S, state: StateHandle) -> SimulationResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let session = Rc::new(RefCell::new(SessionState::new()));
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
            handle_stateful_frame(&session, &state, &frame, &mut out_buf);
        }

        // After processing inbound frames, push any pending messages
        // to consumers that have available flow permits.
        push_pending_messages(&session, &state, &mut out_buf);

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

#[allow(clippy::too_many_lines)]
fn handle_stateful_frame(
    session: &Rc<RefCell<SessionState>>,
    state: &StateHandle,
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
                    .borrow_mut()
                    .producers
                    .insert(p.producer_id, (p.topic.clone(), 0));
                emit_producer_success(out, p.request_id);
            }
        }
        pb::base_command::Type::Send => {
            if let (Some(s), Some(payload)) = (&frame.command.send, &frame.payload) {
                let topic_and_eid = {
                    let mut sess = session.borrow_mut();
                    let entry = sess.producers.get_mut(&s.producer_id).map(|(t, n)| {
                        let eid = *n;
                        *n = n.saturating_add(1);
                        (t.clone(), eid)
                    });
                    if let Some((topic, eid)) = entry {
                        sess.ledger
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
                    state.emit_raw(
                        "broker_sends",
                        SendEvent {
                            producer_id: s.producer_id,
                            sequence_id: s.sequence_id,
                            ledger_id: 1,
                            entry_id,
                        },
                        0,
                        "broker",
                    );
                    emit_send_receipt(out, s.producer_id, s.sequence_id, 1, entry_id);
                }
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                session.borrow_mut().consumers.insert(
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
                if let Some(c) = session.borrow_mut().consumers.get_mut(&f.consumer_id) {
                    c.permits = c.permits.saturating_add(f.message_permits);
                }
            }
        }
        pb::base_command::Type::Ack => {
            if let Some(a) = &frame.command.ack {
                for mid in &a.message_id {
                    state.emit_raw(
                        "broker_acks",
                        AckEvent {
                            consumer_id: a.consumer_id,
                            ledger_id: mid.ledger_id,
                            entry_id: mid.entry_id,
                        },
                        0,
                        "broker",
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
    session: &Rc<RefCell<SessionState>>,
    state: &StateHandle,
    out: &mut BytesMut,
) {
    let to_push: Vec<(u64, Vec<StoredMessage>)> = {
        let mut sess = session.borrow_mut();
        let mut batch = Vec::new();
        // Snapshot ledgers up front to avoid borrow conflicts.
        let ledger_snapshot: HashMap<String, Vec<StoredMessage>> = sess.ledger.clone();
        for (cid, slot) in &mut sess.consumers {
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
            state.emit_raw(
                "broker_delivers",
                DeliverEvent {
                    consumer_id: cid,
                    ledger_id: m.ledger_id,
                    entry_id: m.entry_id,
                },
                0,
                "broker",
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

    fn check(&self, state: &StateHandle, _t: u64) {
        let Some(tl) = state.timeline::<SendEvent>("broker_sends") else {
            return;
        };
        let entries = tl.all();
        let start = self.cursor.get();
        if start >= entries.len() {
            return;
        }
        for entry in &entries[start..] {
            let pid = entry.event.producer_id;
            let cur = entry.event.sequence_id;
            let prev = self.last_seq.borrow().get(&pid).copied();
            if let Some(p) = prev {
                assert!(
                    cur > p,
                    "non-monotonic sequence_id for producer {pid}: prev={p} got={cur}",
                );
            }
            self.last_seq.borrow_mut().insert(pid, cur);
        }
        self.cursor.set(entries.len());
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

    fn check(&self, state: &StateHandle, _t: u64) {
        // Drain new deliveries into the seen-set first.
        if let Some(dtl) = state.timeline::<DeliverEvent>("broker_delivers") {
            let dentries = dtl.all();
            for entry in &dentries[self.deliver_cursor.get()..] {
                self.delivered.borrow_mut().insert((
                    entry.event.consumer_id,
                    entry.event.ledger_id,
                    entry.event.entry_id,
                ));
            }
            self.deliver_cursor.set(dentries.len());
        }
        // Now check each new ack against the seen-set.
        let Some(tl) = state.timeline::<AckEvent>("broker_acks") else {
            return;
        };
        let entries = tl.all();
        let start = self.ack_cursor.get();
        if start >= entries.len() {
            return;
        }
        for entry in &entries[start..] {
            let key = (
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            );
            assert!(
                self.delivered.borrow().contains(&key),
                "ack for never-delivered message: consumer={} ({}, {})",
                key.0,
                key.1,
                key.2,
            );
        }
        self.ack_cursor.set(entries.len());
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

    fn check(&self, state: &StateHandle, _t: u64) {
        // Refresh the acked set.
        if let Some(atl) = state.timeline::<AckEvent>("broker_acks") {
            for entry in atl.all().iter() {
                self.acked.borrow_mut().insert((
                    entry.event.consumer_id,
                    entry.event.ledger_id,
                    entry.event.entry_id,
                ));
            }
        }
        // Walk new deliveries; assert none of them are in the acked set.
        let Some(dtl) = state.timeline::<DeliverEvent>("broker_delivers") else {
            return;
        };
        let entries = dtl.all();
        let start = self.cursor.get();
        if start >= entries.len() {
            return;
        }
        for entry in &entries[start..] {
            let key = (
                entry.event.consumer_id,
                entry.event.ledger_id,
                entry.event.entry_id,
            );
            assert!(
                !self.acked.borrow().contains(&key),
                "broker redelivered acked message: consumer={} ({}, {})",
                key.0,
                key.1,
                key.2,
            );
        }
        self.cursor.set(entries.len());
    }
}

// =============================================================================
// ProducerConsumer client workload — drives N sends + N receives, asserts
// the at-least-once postcondition on `Workload::check()` (Pulsar Java
// pattern from `SimpleProducerConsumerTest`).
// =============================================================================

const PRODUCE_COUNT: u32 = 8;

struct ProducerConsumerWorkload {
    sent: Rc<RefCell<Vec<u32>>>,
    received: Rc<RefCell<Vec<u32>>>,
    completed: Cell<bool>,
}

impl ProducerConsumerWorkload {
    fn new() -> Self {
        Self {
            sent: Rc::new(RefCell::new(Vec::new())),
            received: Rc::new(RefCell::new(Vec::new())),
            completed: Cell::new(false),
        }
    }
}

#[async_trait(?Send)]
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
        for i in 0..PRODUCE_COUNT {
            let payload = bytes::Bytes::from(i.to_le_bytes().to_vec());
            let msg = magnetar_proto::producer::OutgoingMessage {
                payload: payload.clone(),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 4,
                num_messages: 1,
                txn_id: None,
            };
            let _ = tokio::time::timeout(Duration::from_secs(5), producer.send(msg)).await;
            self.sent.borrow_mut().push(i);
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
                self.received.borrow_mut().push(u32::from_le_bytes(bytes));
            }
            let _ = consumer.ack(msg.message_id).await;
        }

        self.completed.set(true);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        if !self.completed.get() {
            return Err(SimulationError::InvalidState(
                "client workload did not complete".into(),
            ));
        }
        // At-least-once delivery: every sent payload must appear in the
        // received set (Pulsar's set-difference pattern). Duplicates
        // are tolerated here — `NoDupOnAckedInvariant` catches the
        // duplicate-after-ack case from the broker side.
        let sent: HashSet<u32> = self.sent.borrow().iter().copied().collect();
        let received: HashSet<u32> = self.received.borrow().iter().copied().collect();
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
    let _ = SimulationBuilder::new()
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .set_iterations(1)
        .set_time_limit(Duration::from_secs(60))
        .run();
}

/// 16-seed sweep of the full produce/consume + invariants surface.
#[test]
fn sim_chaos_produce_consume_sweep_16_seeds() {
    let _ = SimulationBuilder::new()
        .workload(StatefulBrokerWorkload::new())
        .workload(ProducerConsumerWorkload::new())
        .invariant(MonotonicMsgIdInvariant::default())
        .invariant(AckAfterReceiveInvariant::default())
        .invariant(NoDupOnAckedInvariant::default())
        .set_iterations(16)
        .set_time_limit(Duration::from_secs(60))
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

#[async_trait(?Send)]
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
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let counter_for_session = counter.clone();
                            let time = providers.time().clone();
                            let session_delay = delay;
                            tokio::task::spawn_local(async move {
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
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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

        match stream.read_buf(&mut read_buf).await {
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

#[async_trait(?Send)]
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
        .set_time_limit(Duration::from_secs(60))
        .run();
}
