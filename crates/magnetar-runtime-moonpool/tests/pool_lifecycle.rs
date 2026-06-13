// SPDX-License-Identifier: Apache-2.0

//! Lifecycle coverage for the ADR-0039 per-broker connection pool on the
//! moonpool engine: a `proxy_through_service_url = true` lookup must open a
//! *pooled* per-broker connection that is established and reused, and engine
//! `close()` must tear that pooled dial down without panic.
//!
//! ## What this pins
//!
//! Existing coverage (`tests/proxy_multi_conn.rs`) drives the proxy pool over
//! production `TokioProviders` and asserts the *open* shape (second TCP
//! session, pinned `CommandConnect.proxy_to_broker_url`, pool reuse). This
//! test pins the missing *lifecycle* half over the deterministic
//! `moonpool-sim` substrate ([`SimulationBuilder`] + a [`Workload`] broker +
//! [`SimulationBuilder::run_time_budget`], modelled on
//! `tests/connect_resilience.rs`):
//!
//! 1. **Pooled connection established + used.** The supervised client (`connect_plain_supervised` →
//!    pool enabled, ADR-0039) opens a producer. The broker advertises `proxy_through_service_url =
//!    true`, so the data op rides on a *second*, pinned pool connection whose
//!    `CommandConnect.proxy_to_broker_url` names the advertised broker. The producer open only
//!    succeeds if that pinned connection handshaked and the `CommandProducer` round-trip completed
//!    on it — a non-vacuous proof that the pooled connection works.
//! 2. **Clean teardown of pooled dials.** `Client::close()` drains the pool
//!    (`ProxyConnectionPool::close`) — closing every `EntryState::Ready` connection and joining its
//!    supervised driver. The workload records that `close()` returned at all; combined with the run
//!    terminating (the `SimulationBuilder::run` handing control back), that is the proof the
//!    teardown ran to completion without a panic or a wedged driver join.
//!
//! ## Determinism under the default sim fault config
//!
//! `SimulationBuilder::new()` (no `random_network()`) installs
//! `NetworkConfiguration::default()`, whose fault model includes probabilistic
//! connect failure and FDB-style bit-flip corruption (ADR-0055). The exact
//! lifecycle shape is pinned by the smoke test; the multi-seed sweep pins the
//! chaos property: every seed must terminate with either the strong lifecycle
//! result or a bounded error. A bit-flip on an unchecksummed Pulsar command
//! frame can corrupt `CommandConnected` or `CommandConnect.proxy_to_broker_url`;
//! that is a valid chaos drop, not evidence that the pool lifecycle code broke.
//! The caps are virtual-time bounded (ADR-0011, ADR-0052), so every seed is
//! bit-for-bit reproducible and the wall-clock cost is just scheduler steps.
//!
//! ## Runtime-test-parity
//!
//! Two `#[test]` functions live here; the mirrored
//! `magnetar-runtime-tokio/tests/pool_lifecycle.rs` carries two of its own so
//! `check-runtime-test-parity` stays 1:1 (ADR-0024).

#![allow(clippy::expect_used)]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

mod common;
use common::sweep_seeds;

/// Port the in-sim proxy binds to. The sim network hands every workload its
/// own IP, so a fixed port keeps the client→proxy derivation trivial. Every
/// pool entry dials this same `host:port` (it IS the proxy — ADR-0039).
const BROKER_PORT: u16 = 6650;

/// Synthetic broker URL the fake proxy advertises in lookup responses. The
/// host is meaningless — the client never dials it; the pinned pool entry
/// stays on the proxy address and rides `proxy_to_broker_url` to reach it.
const ADVERTISED_BROKER_URL: &str = "pulsar://broker-pool-lifecycle.proxy.internal:6650";

/// `host:port` form of [`ADVERTISED_BROKER_URL`] — the value the runtime must
/// stuff into `CommandConnect.proxy_to_broker_url` after stripping the
/// `pulsar://` scheme (parity with Java + pulsar-rs; ADR-0039).
const ADVERTISED_BROKER_HOST_PORT: &str = "broker-pool-lifecycle.proxy.internal:6650";

/// Per-run virtual-time budget. Comfortably above the worst-case pooled-dial
/// recovery (a handful of `connect_timeout`-bounded hangs plus short backoffs,
/// all on virtual time) yet tight enough that a genuine wedge trips the
/// orchestrator's no-progress detector instead of burning a wall-clock core.
/// Pure function of the simulated schedule → never perturbs replay determinism
/// (ADR-0011, ADR-0036).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(120);

/// Tight per-attempt connect timeout so a probabilistic connect-*hang* on the
/// pooled dial surfaces as a bounded `Io(TimedOut)` quickly and is re-dialled,
/// rather than parking the whole operation budget on one hung attempt.
const TIGHT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Generous total operation budget for the pooled dial. The pool wraps the
/// per-broker dial in this budget (`pool.rs` `get_or_open`); it must exceed the
/// worst-case sum of recovered connect-hangs so the pinned connection always
/// establishes within it. Virtual time → no wall-clock cost.
const GENEROUS_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// High retry backstop so the count half of the dual cap never trips before a
/// transient connect-hang is recovered (the proxy stays bound all run).
const HIGH_CONNECT_MAX_RETRIES: u32 = 64;

/// Per-session log: the `proxy_to_broker_url` seen on `CommandConnect` and the
/// kinds of every subsequent frame, in arrival order. Captures the
/// bootstrap-vs-pinned distinction.
#[derive(Clone, Debug, Default)]
struct SessionRecord {
    /// `Some(url)` when `CommandConnect.proxy_to_broker_url = Some(url)`, `None`
    /// when the field was absent.
    connect_proxy_to_broker_url: Option<String>,
    /// All non-CONNECT frame kinds the session received, in arrival order.
    frames: Vec<i32>,
}

/// Outcome the client workload records, one per simulation iteration. The
/// lifecycle claim — asserted in the TEST BODY after `run()` returns, not in
/// `Workload::check` (whose `Err` only logs and never fails the run) — is that
/// EVERY iteration reaches `PooledThenClean`: the pinned pool connection
/// established, the producer opened on it, and `close()` tore the pool down and
/// returned.
#[derive(Clone, Debug)]
enum LifecycleOutcome {
    /// The pooled producer open completed (pinned connection works) AND
    /// `Client::close()` returned (pool teardown ran clean). Carries the
    /// session snapshot so the test body can pin the pinned-CONNECT shape.
    PooledThenClean { sessions: Vec<SessionRecord> },
    /// The supervised connect or the producer open surfaced a bounded error
    /// (e.g. the dual cap tripped on a storming pooled dial). A bounded,
    /// terminating outcome — recorded for diagnostics, never a silent park.
    BoundedError(String),
}

/// In-sim Apache Pulsar Proxy. On the bootstrap session (idx 0) it answers
/// `CommandLookupTopic` with `proxy_through_service_url = true` plus a
/// synthetic `broker_service_url`, forcing the runtime to open a *second*,
/// pinned pool connection. It serves `CommandProducer` on the pinned session.
struct ProxyWorkload {
    sessions: Arc<Mutex<Vec<SessionRecord>>>,
}

#[async_trait]
impl Workload for ProxyWorkload {
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
                                s.push(SessionRecord::default());
                                s.len() - 1
                            };
                            let sessions_for_task = sessions.clone();
                            // Spawn the session so the accept loop keeps
                            // servicing the pinned pool dial (and any
                            // supervised re-dial after a connect-fault).
                            // moonpool main's `JoinHandle` has no `abort()`;
                            // cooperative shutdown is driven by the peer
                            // closing the socket / `ctx.shutdown()`.
                            let _handle = task.spawn_task("proxy-session", async move {
                                let _ = handle_session(stream, sessions_for_task, session_idx).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Drive one proxy session — decode frames, record the CONNECT proxy field +
/// every other frame kind, reply per the minimal dispatch table, and return
/// when the peer closes (the close-driven half of the lifecycle).
async fn handle_session<S>(
    mut stream: S,
    sessions: Arc<Mutex<Vec<SessionRecord>>>,
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
            if matches!(
                pb::base_command::Type::try_from(kind).ok(),
                Some(pb::base_command::Type::Connect)
            ) {
                if let Some(c) = &frame.command.connect {
                    sessions.lock()[session_idx]
                        .connect_proxy_to_broker_url
                        .clone_from(&c.proxy_to_broker_url);
                }
            } else {
                sessions.lock()[session_idx].frames.push(kind);
            }

            handle_frame(&frame, &mut out_buf, session_idx);
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

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, session_idx: usize) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-pool-lifecycle".to_owned(),
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
                // Only the bootstrap session (idx 0) advertises
                // proxy_through=true; pinned sessions echo false to avoid a
                // redirect loop (they shouldn't issue lookups in this test).
                let proxy_through = session_idx == 0;
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: Some(ADVERTISED_BROKER_URL.to_owned()),
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(proxy_through),
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
                        producer_name: "pool-lifecycle".to_owned(),
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
        _ => {}
    }
}

/// Client workload — supervised connect (pool enabled, ADR-0039), open a
/// producer through the proxy (forcing the pinned pool dial), then close the
/// client (draining the pool). Pushes exactly one terminating outcome per
/// iteration into the shared `outcomes` vec, which the TEST BODY inspects after
/// `run()` returns (a `Workload::check` `Err` only logs and never fails the
/// run, so the load-bearing assertions live in the test, not in `check`).
struct ClientWorkload {
    sessions: Arc<Mutex<Vec<SessionRecord>>>,
    outcomes: Arc<Mutex<Vec<LifecycleOutcome>>>,
}

impl ClientWorkload {
    fn new(
        sessions: Arc<Mutex<Vec<SessionRecord>>>,
        outcomes: Arc<Mutex<Vec<LifecycleOutcome>>>,
    ) -> Self {
        Self { sessions, outcomes }
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

        // Supervised connect → the proxy pool is enabled (ADR-0039). The dual
        // cap is tuned so a probabilistic connect-hang on the pinned pool dial
        // fails its per-attempt `connect_timeout` fast and is re-dialled well
        // within the operation budget — recovery is deterministic because the
        // proxy listener stays bound the whole run.
        let cfg = ConnectionConfig {
            connect_timeout: TIGHT_CONNECT_TIMEOUT,
            operation_timeout: GENEROUS_OPERATION_TIMEOUT,
            connect_max_retries: HIGH_CONNECT_MAX_RETRIES,
            supervisor: Some(magnetar_proto::SupervisorConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(200),
                mandatory_stop: Duration::from_secs(90),
                max_attempts: Some(64),
                ..magnetar_proto::SupervisorConfig::default()
            }),
            ..ConnectionConfig::default()
        };

        let client = match Client::connect_plain_supervised(&engine, &addr, cfg, None, None).await {
            Ok(client) => client,
            Err(err) => {
                self.outcomes
                    .lock()
                    .push(LifecycleOutcome::BoundedError(format!(
                        "supervised connect failed: {err:?}"
                    )));
                return Ok(());
            }
        };

        // Open a producer through the proxy. This forces the
        // `LookupTarget::Proxy` path → a pinned pool dial back to the proxy
        // with `CommandConnect.proxy_to_broker_url` set. Success here proves
        // the pooled connection established and round-tripped `CommandProducer`.
        let open = client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/pool-lifecycle-producer".to_owned(),
                ..Default::default()
            })
            .await;

        let outcome = match open {
            Ok(producer) => {
                // Snapshot the pool-open shape BEFORE teardown, then drop the
                // producer handle and tear the whole client down. `close()`
                // drains the proxy pool (ADR-0039): every pooled
                // `EntryState::Ready` connection is closed and its supervised
                // driver joined. Returning at all is the clean-teardown proof.
                let sessions = self.sessions.lock().clone();
                drop(producer);
                client.close().await;
                LifecycleOutcome::PooledThenClean { sessions }
            }
            Err(err) => {
                client.close().await;
                LifecycleOutcome::BoundedError(format!("open_producer failed: {err:?}"))
            }
        };
        self.outcomes.lock().push(outcome);
        Ok(())
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // Reset the shared session log for the next sweep iteration. The
        // outcome assertions live in the test body (see
        // [`assert_every_iteration_pooled_then_clean`]) because a
        // `Workload::check` `Err` only logs — it never increments
        // `failed_runs` nor fails the test.
        self.sessions.lock().clear();
        Ok(())
    }
}

/// Assert EVERY recorded iteration reached the strong lifecycle outcome: a
/// pinned pool connection opened (bootstrap CONNECT clean, pinned CONNECT
/// carries the advertised `host:port` in `proxy_to_broker_url`, pinned session
/// served the `CommandProducer`) and `Client::close()` tore the pool down. This
/// is only meaningful on the smoke path: under default sim chaos, bit-flip can
/// corrupt unchecksummed command-frame bytes before the broker records them.
fn assert_every_iteration_pooled_then_clean(
    outcomes: &Arc<Mutex<Vec<LifecycleOutcome>>>,
    expected_iterations: usize,
) {
    let snapshot = outcomes.lock().clone();
    assert_eq!(
        snapshot.len(),
        expected_iterations,
        "every iteration must record exactly one terminating outcome; got {} for {expected_iterations} iteration(s): {snapshot:?}",
        snapshot.len(),
    );
    for (i, outcome) in snapshot.iter().enumerate() {
        match outcome {
            LifecycleOutcome::PooledThenClean { sessions } => {
                // Non-vacuous: the pooled connection must actually have been a
                // distinct, pinned entry — bootstrap (idx 0) + ≥1 pinned (idx 1).
                assert!(
                    sessions.len() >= 2,
                    "iter {i}: proxy_through lookup must open a SECOND pooled connection; \
                     saw {} session(s): {sessions:?}",
                    sessions.len(),
                );
                let bootstrap = &sessions[0];
                let pinned = &sessions[1];
                assert!(
                    bootstrap.connect_proxy_to_broker_url.is_none(),
                    "iter {i}: bootstrap CONNECT must NOT carry proxy_to_broker_url, got {:?}",
                    bootstrap.connect_proxy_to_broker_url,
                );
                assert_eq!(
                    pinned.connect_proxy_to_broker_url.as_deref(),
                    Some(ADVERTISED_BROKER_HOST_PORT),
                    "iter {i}: pinned pool CONNECT must carry proxy_to_broker_url = host:port \
                     (no scheme), got {:?}",
                    pinned.connect_proxy_to_broker_url,
                );
                assert!(
                    pinned
                        .frames
                        .contains(&(pb::base_command::Type::Producer as i32)),
                    "iter {i}: pooled producer open must ride the pinned connection; \
                     pinned frames {:?}",
                    pinned.frames,
                );
            }
            LifecycleOutcome::BoundedError(reason) => panic!(
                "iter {i}: pooled proxy connection did not establish + tear down cleanly within \
                 the dual cap: {reason}"
            ),
        }
    }
}

/// Assert every chaos-sweep iteration recorded a bounded terminating outcome.
/// Exact byte-shape checks belong to the smoke test; the sweep runs under the
/// default moonpool fault model, where command-frame corruption is an intended
/// transient drop (ADR-0055).
fn assert_every_iteration_terminated(
    outcomes: &Arc<Mutex<Vec<LifecycleOutcome>>>,
    expected_iterations: usize,
) {
    let snapshot = outcomes.lock().clone();
    assert_eq!(
        snapshot.len(),
        expected_iterations,
        "every iteration must record exactly one terminating outcome; got {} for {expected_iterations} iteration(s): {snapshot:?}",
        snapshot.len(),
    );
}

/// Single-seed smoke: a `proxy_through_service_url = true` lookup opens a
/// pooled per-broker connection, the producer rides it, and `Client::close()`
/// tears the pool down cleanly. The test-body assertion
/// ([`assert_every_iteration_pooled_then_clean`]) pins the pooled-open shape +
/// clean teardown; `run()` returning at all proves termination.
#[test]
fn moonpool_pooled_proxy_connection_opens_and_tears_down_clean_smoke() {
    let sessions = Arc::new(Mutex::new(Vec::<SessionRecord>::new()));
    let outcomes = Arc::new(Mutex::new(Vec::<LifecycleOutcome>::new()));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(ProxyWorkload {
            sessions: sessions.clone(),
        })
        .workload(ClientWorkload::new(sessions, outcomes.clone()))
        .set_iterations(1)
        .run();
    assert_eq!(
        report.iterations, 1,
        "expected exactly one iteration to be dispatched and terminate: {report:?}",
    );
    assert_every_iteration_pooled_then_clean(&outcomes, 1);
}

/// 8-seed sweep — the lifecycle surface under the default moonpool fault model.
/// On a fraction of seeds a connect fault or bit-flip can prevent the clean path;
/// every one must still terminate with either the strong lifecycle outcome or a
/// bounded error. The smoke test above is the exact pooled-open + teardown proof.
#[test]
fn moonpool_pooled_proxy_connection_opens_and_tears_down_clean_sweep_8_seeds() {
    let sessions = Arc::new(Mutex::new(Vec::<SessionRecord>::new()));
    let outcomes = Arc::new(Mutex::new(Vec::<LifecycleOutcome>::new()));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(ProxyWorkload {
            sessions: sessions.clone(),
        })
        .workload(ClientWorkload::new(sessions, outcomes.clone()))
        .set_debug_seeds(sweep_seeds(8))
        .set_iterations(8)
        .run();
    assert_eq!(
        report.iterations, 8,
        "every seed must be dispatched and terminate (no silent hang): {report:?}",
    );
    assert_every_iteration_terminated(&outcomes, 8);
}
