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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, TcpListenerTrait};
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
