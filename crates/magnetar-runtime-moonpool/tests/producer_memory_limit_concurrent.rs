// SPDX-License-Identifier: Apache-2.0

//! Layer (c) of the ADR-0024 four-layer policy for the producer-side
//! memory-limit reservation (ADR-0017): the moonpool *deterministic
//! simulation* test for `MemoryLimitPolicy::FailImmediately`.
//!
//! ## What this pins
//!
//! With a small [`ConnectionConfig::memory_limit_bytes`] and the Java
//! default [`MemoryLimitPolicy::FailImmediately`], `Producer::send`
//! reserves the payload bytes against the global budget *before* the
//! message reaches the sans-io state machine (mirrors Java
//! `MemoryLimitController.reserveMemory`). The end-to-end contract this
//! test drives through a full `connect → open_producer → send` round-trip
//! against an in-sim broker is:
//!
//! 1. an **under-limit** send (payload ≤ limit) reserves successfully, rides the wire, and resolves
//!    `Ok(MessageId)` once the broker replies with `CommandSendReceipt`; and
//! 2. an **over-limit** send (a single payload strictly larger than the whole budget) is rejected
//!    *synchronously* — the `SendFut` resolves
//!    `Err(ClientError::Engine(EngineError::MemoryLimitExceeded { .. }))` without ever hitting the
//!    wire.
//!
//! The over-limit payload is sized to exceed the entire budget on its own
//! (`OVER_LIMIT_PAYLOAD > LIMIT_BYTES`), so the rejection is deterministic
//! regardless of how much budget the under-limit send happens to still
//! hold when the over-limit send is issued — there is no release-ordering
//! race to make the outcome seed-dependent (ADR-0011, ADR-0036).
//!
//! Determinism note: the reservation is a lock-free CAS on an `AtomicU64`
//! (`ConnectionShared::try_reserve_memory`), not a scheduled timer, so it
//! never perturbs the simulated schedule. Every seed is bit-for-bit
//! reproducible.
//!
//! ## Connect chaos is in scope — the memory-limit contract is gated on a live connection (ADR-0052)
//!
//! `SimulationBuilder::new()` runs every iteration under the default
//! `moonpool_sim` network, whose `ConnectFailureMode::Probabilistic`
//! (a `FoundationDB` `SIM_CONNECT_ERROR_MODE = 2` port) makes each initial
//! dial either fail fast or **hang forever**, by design "to test timeout
//! handling in connection retry logic" — and there is **no** moonpool API
//! to disable it from the builder or a workload (`with_network_config` is
//! read-only). On a fraction of seeds the `connect → open_producer`
//! handshake therefore never completes within magnetar's dual-cap dial
//! budget (`connect_timeout` × `connect_max_retries`, bounded by
//! `operation_timeout`; [ADR-0052](../../../specs/adr/0052-initial-connect-timeout-retry.md)).
//! That is a *bounded connect outcome*, not a memory-limit-contract
//! violation: the reservation CAS lives entirely client-side and is never
//! reached when the dial is blocked.
//!
//! So the contract this test pins is **conditional on a live connection**:
//! when the dial + `open_producer` succeed (the common case, and every seed
//! across the broader seed space), the under-limit / over-limit contract
//! above MUST hold; when the chaos network bounds the dial before a
//! producer exists, that seed records a non-failing `connect_blocked`
//! outcome (mirroring `connect_resilience.rs`, the ADR-0052 reference
//! pattern). The silent-park case — a dial that neither connects nor
//! surfaces a bounded error — is still a hard failure (it trips the
//! orchestrator's `run_time_budget` detector). This matches the
//! `sim_chaos_*` and `connect_resilience` sweeps, which assert bounded
//! termination rather than unconditional connect success under the same
//! default chaos.
//!
//! ## Runtime-test-parity
//!
//! Two `#[test]` functions live here; the mirrored
//! `magnetar-runtime-tokio/tests/producer_memory_limit_concurrent.rs`
//! carries two `#[tokio::test]` functions so `check-runtime-test-parity`
//! stays 1:1 (ADR-0024).

#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, MemoryLimitPolicy, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, EngineError, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

mod common;
use common::sweep_seeds;

/// Port the in-sim broker binds to. The sim network hands every workload
/// its own IP, so a fixed port keeps the client→broker derivation trivial.
const BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget. Comfortably above the legitimate
/// connect + `open_producer` + one round-trip send (a few simulated
/// milliseconds) yet tight enough that any runaway trips the
/// orchestrator's no-progress detector instead of burning a wall-clock
/// core. Pure function of the simulated schedule → never perturbs replay
/// determinism (ADR-0011, ADR-0036).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// Total memory budget for the connection. Small enough that a single
/// modest payload can exceed it, large enough that the under-limit send
/// fits with room to spare.
const LIMIT_BYTES: u64 = 64;

/// Under-limit payload — fits inside [`LIMIT_BYTES`] so the reservation
/// CAS succeeds and the send rides the wire.
const UNDER_LIMIT_PAYLOAD: usize = 16;

/// Over-limit payload — strictly larger than the *entire* budget, so the
/// reservation fails even against a fully-empty counter. Makes the
/// rejection independent of any release-ordering race.
const OVER_LIMIT_PAYLOAD: usize = (LIMIT_BYTES as usize) + 64;

/// Build a plain single-message [`OutgoingMessage`] of `len` zero bytes.
/// The moonpool `Producer` exposes only `send(OutgoingMessage)` (no
/// `send_bytes` convenience), so the test constructs the envelope itself.
/// `len` is the payload byte count the reservation CAS charges against the
/// budget.
fn outgoing(len: usize) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from(vec![0u8; len]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: len as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Outcome the client workload records. The memory-limit contract fields
/// (`under_limit_ok`, `over_limit_rejected`) are asserted in `check()` /
/// the in-`run()` gate **only when a connection was established**: the
/// under-limit send must have resolved `Ok`, the over-limit send must have
/// surfaced `MemoryLimitExceeded`. When `connect_blocked` is set, the chaos
/// network bounded the dial before a producer existed (ADR-0052) — a valid,
/// non-failing outcome with no memory-limit assertion to make.
#[derive(Clone, Debug, Default)]
struct SendOutcome {
    /// `Some(reason)` when `connect` / `open_producer` surfaced a bounded
    /// failure under the default `ConnectFailureMode::Probabilistic` chaos
    /// (ADR-0052), so the workload never reached the reservation path.
    /// `None` when a producer was opened and the contract below applies.
    connect_blocked: Option<String>,
    /// `Some(true)` when the under-limit send resolved `Ok(MessageId)`;
    /// `Some(false)` if it errored; `None` if the workload never reached it.
    under_limit_ok: Option<bool>,
    /// `Some(true)` when the over-limit send surfaced
    /// `MemoryLimitExceeded`; `Some(false)` for any other outcome; `None`
    /// if the workload never reached it.
    over_limit_rejected: Option<bool>,
}

/// In-sim broker speaking the subset needed to drive `open_producer`
/// (`CONNECT → CONNECTED`, `LOOKUP → LookupResponse(Connect)`,
/// `PRODUCER → PRODUCER_SUCCESS`) and one publish round-trip
/// (`SEND → SEND_RECEIPT`), plus `PING → PONG` for keepalive.
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

/// Drive one broker session — decode frames, reply per the dispatch
/// table, flush, and return when the peer closes.
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
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "mem-limit-test".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Send => {
            if let Some(s) = &frame.command.send {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::SendReceipt as i32,
                    send_receipt: Some(pb::CommandSendReceipt {
                        producer_id: s.producer_id,
                        sequence_id: s.sequence_id,
                        message_id: Some(pb::MessageIdData {
                            ledger_id: 1,
                            entry_id: s.sequence_id,
                            partition: None,
                            batch_index: None,
                            ack_set: vec![],
                            batch_size: None,
                            first_chunk_message_id: None,
                        }),
                        highest_sequence_id: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        _ => {}
    }
}

/// Client workload: connect, open a non-batching producer against a tiny
/// memory budget, then issue one under-limit and one over-limit send.
/// Records both outcomes for `check()`. Under the default
/// `ConnectFailureMode::Probabilistic` chaos (ADR-0052) some iterations
/// bound the dial before a producer exists; those record `connect_blocked`
/// and exercise no memory-limit assertion.
struct ClientWorkload {
    outcome: Arc<Mutex<SendOutcome>>,
    /// Cumulative count of iterations that reached a live connection and
    /// satisfied the memory-limit contract. Shared across the reused
    /// workload instance so the sweep test can assert non-vacuity (at least
    /// one seed actually exercised the reservation, rather than every seed
    /// having been connect-blocked by chaos).
    contract_runs: Arc<Mutex<usize>>,
}

impl ClientWorkload {
    fn new() -> Self {
        Self {
            outcome: Arc::new(Mutex::new(SendOutcome::default())),
            contract_runs: Arc::new(Mutex::new(0)),
        }
    }

    /// Handle to the cumulative contract-exercised counter for the
    /// non-vacuity assertion in the sweep test.
    fn contract_runs(&self) -> Arc<Mutex<usize>> {
        self.contract_runs.clone()
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

        // Tiny budget + the Java-default FailImmediately policy. Disable
        // batching so every send goes out as an individual `CommandSend`
        // the broker can 1:1 receipt.
        let cfg = ConnectionConfig {
            memory_limit_bytes: LIMIT_BYTES,
            memory_limit_policy: MemoryLimitPolicy::FailImmediately,
            ..ConnectionConfig::default()
        };

        // The initial dial runs under the default
        // `ConnectFailureMode::Probabilistic` chaos (ADR-0052): a fraction
        // of seeds bound the dial (fail-fast or hang-then-timeout) before a
        // connection exists. magnetar's dual cap guarantees that surfaces as
        // a bounded `Err` here — never a silent park (the silent-park case
        // would trip the orchestrator's `run_time_budget` detector instead).
        // A bounded dial failure is NOT a memory-limit violation: the
        // reservation CAS is never reached. Record it and return `Ok` so the
        // iteration does not land in `failed_runs`.
        let client = match Client::connect_plain(&engine, &addr, cfg).await {
            Ok(client) => client,
            Err(e) => {
                self.outcome.lock().connect_blocked = Some(format!("connect: {e:?}"));
                return Ok(());
            }
        };

        let producer = match client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/mem-limit".to_owned(),
                enable_batching: false,
                ..Default::default()
            })
            .await
        {
            Ok(producer) => producer,
            Err(e) => {
                self.outcome.lock().connect_blocked = Some(format!("open_producer: {e:?}"));
                client.close().await;
                return Ok(());
            }
        };

        // Connection is live — the memory-limit contract now applies.
        // (1) Under-limit send — reserves (16 B < 64 B budget, CAS succeeds),
        // rides the wire, and resolves `Ok(MessageId)` once the broker's
        // `SendReceipt` lands. But the wire round-trip runs over the default
        // chaos network (ADR-0052), which can tear the connection down
        // mid-flight: an unsupervised `connect_plain` driver then resolves
        // the pending send with `OpOutcome::Terminal` → `PeerClosed` (or
        // `Closed` on a local close race). That is a *transport* outcome, not
        // a memory-limit violation — the reservation CAS already succeeded —
        // so treat it like a connect-blocked seed: record it and return `Ok`
        // without asserting the contract. Any other error (a genuine send
        // failure on a live wire) flows through to the contract gate below.
        let under_res = producer.send(outgoing(UNDER_LIMIT_PAYLOAD)).await;
        if matches!(
            under_res,
            Err(magnetar_runtime_moonpool::ClientError::PeerClosed
                | magnetar_runtime_moonpool::ClientError::Closed)
        ) {
            self.outcome.lock().connect_blocked =
                Some(format!("under-limit send: {:?}", under_res.err()));
            client.close().await;
            return Ok(());
        }
        let under = under_res.is_ok();
        self.outcome.lock().under_limit_ok = Some(under);

        // (2) Over-limit send — a single payload larger than the whole
        // budget. Must surface MemoryLimitExceeded synchronously.
        let over = producer.send(outgoing(OVER_LIMIT_PAYLOAD)).await;
        let over_rejected = matches!(
            over,
            Err(magnetar_runtime_moonpool::ClientError::Engine(
                EngineError::MemoryLimitExceeded { .. }
            ))
        );
        self.outcome.lock().over_limit_rejected = Some(over_rejected);

        client.close().await;

        // Non-vacuity guard: on this moonpool build a `Workload::check` `Err`
        // is only logged — it never increments `failed_runs` — so enforce the
        // memory-limit contract HERE in `run()`, whose `Err` DOES land the
        // iteration in `failed_runs` and fail the test. (The `check()` below
        // stays as belt-and-suspenders documentation but is not the gate.)
        // Only reached on the live-connection path, so `under` /
        // `over_rejected` are the actual reservation outcomes.
        if !under || !over_rejected {
            return Err(SimulationError::InvalidState(format!(
                "memory-limit contract violated: under_limit_ok={under} (expected true), \
                 over_limit_rejected={over_rejected} (expected true)"
            )));
        }
        // The reservation contract was actually exercised this iteration —
        // count it for the sweep's non-vacuity guard.
        *self.contract_runs.lock() += 1;
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let outcome = self.outcome.lock().clone();
        // Chaos bounded the dial before a producer existed (ADR-0052):
        // surface it for diagnostics, but it is a valid non-failing outcome
        // with no memory-limit assertion to make.
        if let Some(reason) = outcome.connect_blocked {
            tracing::info!(
                capture = true,
                trail = "memory_limit_connect_blocked",
                reason = %reason,
            );
            return Ok(());
        }
        match (outcome.under_limit_ok, outcome.over_limit_rejected) {
            (Some(true), Some(true)) => Ok(()),
            (under, over) => Err(SimulationError::InvalidState(format!(
                "memory-limit contract violated: under_limit_ok={under:?} \
                 (expected Some(true)), over_limit_rejected={over:?} \
                 (expected Some(true))"
            ))),
        }
    }
}

/// Single-seed smoke: connect, open a producer against a 64-byte budget,
/// and assert the under-limit send succeeds while the over-limit send is
/// rejected with `MemoryLimitExceeded`. Cheap; runs on every push. The
/// default builder seed reaches a live connection, so the memory-limit
/// contract is actually exercised here (asserted via `contract_runs`).
#[test]
fn moonpool_producer_memory_limit_fail_immediately_smoke() {
    let client = ClientWorkload::new();
    let contract_runs = client.contract_runs();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(client)
        .set_iterations(1)
        .run();
    // `run()` returning, with `check()` rejecting any non-(Ok, rejected)
    // outcome, is the proof: a regression that dropped the reservation
    // (over-limit send slipping through) or wrongly rejected the
    // under-limit send would land the iteration in `failed_runs`.
    assert_eq!(
        report.iterations, 1,
        "expected exactly one iteration to be dispatched and terminate: {report:?}",
    );
    assert_eq!(
        report.failed_runs, 0,
        "the memory-limit contract must hold on the smoke seed: {report:?}",
    );
    assert_eq!(
        *contract_runs.lock(),
        1,
        "the smoke seed must reach a live connection and exercise the \
         memory-limit contract (not connect-blocked): {report:?}",
    );
}

/// 8-seed sweep — wherever the chaos network lets the connection through,
/// the reservation outcome is a deterministic function of the payload sizes,
/// so the under-limit-Ok / over-limit-rejected contract must hold; a
/// regression in the reservation CAS or the policy dispatch would flip
/// `failed_runs`. Seeds whose dial is bounded by the default
/// `ConnectFailureMode::Probabilistic` chaos before a producer exists
/// (ADR-0052) record a non-failing `connect_blocked` outcome — they exercise
/// no reservation, so they cannot fail the contract, but the non-vacuity
/// guard below requires at least one seed to have actually exercised it.
#[test]
fn moonpool_producer_memory_limit_fail_immediately_sweep_8_seeds() {
    let client = ClientWorkload::new();
    let contract_runs = client.contract_runs();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(BrokerWorkload)
        .workload(client)
        .set_debug_seeds(sweep_seeds(8))
        .set_iterations(8)
        .run();
    assert_eq!(
        report.iterations, 8,
        "every seed must be dispatched and terminate: {report:?}",
    );
    assert_eq!(
        report.failed_runs, 0,
        "no live-connection seed may violate the memory-limit contract \
         (under-limit Ok, over-limit MemoryLimitExceeded); connect-blocked \
         seeds are bounded chaos outcomes, not failures: {report:?}",
    );
    assert!(
        *contract_runs.lock() >= 1,
        "non-vacuity: at least one seed must reach a live connection and \
         exercise the memory-limit reservation — every seed was \
         connect-blocked by chaos (contract never tested): {report:?}",
    );
}
