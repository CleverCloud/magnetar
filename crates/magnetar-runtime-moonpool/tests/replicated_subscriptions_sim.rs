// SPDX-License-Identifier: Apache-2.0

//! PIP-33 (replicated subscriptions, ADR-0034) — deterministic
//! `SimProviders` / `SimulationBuilder` regression for the marker-accessor
//! lost-wakeup race.
//!
//! The real-TCP `replicated_subscriptions.rs` suite runs over
//! `TokioProviders` (multi-threaded, non-deterministic) so it never drives the
//! parked-waiter gap in `Client::next_replicated_subscription_marker`
//! reliably. This file stands up the same in-simulator broker wiring as
//! `sim_chaos.rs` (deterministic single-threaded scheduler, virtual clock) and
//! adds a **delayed-marker broker** that holds the
//! `REPLICATED_SUBSCRIPTION_SNAPSHOT` marker until the client has subscribed,
//! drained its flow permits, and parked on the marker accessor — i.e. the
//! exact window the lost-wakeup fix protects.
//!
//! The fix is the enroll-before-drain idiom (the `Notified` future is armed and
//! `enable()`d before the buffer drain + `is_closed()` re-check), mirrored 1:1
//! across both engines. With the pre-fix drain-then-`notified().await` shape a
//! marker the driver pushes via `notify_waiters()` (which stores no permit) in
//! that window is lost and the accessor hangs; this test deterministically
//! schedules the marker into that window so the accessor must survive it.
//!
//! ADR-0024: this is the moonpool deterministic regression layer (a
//! moonpool-only `SimProviders` harness, like `sim_chaos.rs`, so it is on the
//! `check-runtime-test-parity` exempt list). The cross-engine 1:1 twins are
//! `marker_lost_wakeup.rs` in both runtime crates; the differential parity test
//! `marker_filter_event_stream_parity` asserts the marker-observation
//! `EventStream` matches across engines.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    ConnectionConfig, FrameError, ReplicatedSubscriptionMarkerKind, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

mod common;
use common::sweep_seeds;

/// Port the broker workload binds to (mirrors `sim_chaos.rs`).
const BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget. The lost-wakeup hang manifests as a future
/// that never resolves; the budget is the deterministic deadline that turns a
/// hang into a recorded failure (pre-fix) instead of pinning the scheduler.
const SIM_RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// Topic + subscription the marker test drives.
const TOPIC: &str = "persistent://public/default/replicated-sim";
const SUBSCRIPTION: &str = "sub-pip-33-sim";

/// Single-`poll_read` helper mirroring `sim_chaos.rs::read_into`.
async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

/// Shared client↔broker coordination flag. The client sets `parked` once it has
/// subscribed and is about to await `next_replicated_subscription_marker`; the
/// delayed-marker broker waits for it before pushing the marker so the marker
/// deterministically lands in the parked-waiter window.
#[derive(Default)]
struct Coordination {
    /// Set by the client right before it parks on the marker accessor.
    parked: AtomicBool,
}

/// Delayed-marker broker workload. Speaks the minimal Pulsar subset
/// (Connect / Ping / Lookup / Subscribe / Flow), and — once the client signals
/// it has parked on the marker accessor — pushes a single
/// `REPLICATED_SUBSCRIPTION_SNAPSHOT` marker so the observation lands in the
/// lost-wakeup window.
struct DelayedMarkerBroker {
    coord: Arc<Coordination>,
}

impl DelayedMarkerBroker {
    fn new(coord: Arc<Coordination>) -> Self {
        Self { coord }
    }
}

#[async_trait]
impl Workload for DelayedMarkerBroker {
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
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let coord = self.coord.clone();
                            let session_time = time.clone();
                            let _handle = task.spawn_task("delayed-marker-session", async move {
                                let _ = handle_session(stream, coord, session_time).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Per-session routing: consumer-id → topic, plus whether the snapshot marker
/// has already been pushed (pushed exactly once, after the client parks).
#[derive(Default)]
struct SessionState {
    consumers: HashMap<u64, String>,
    marker_pushed: bool,
}

async fn handle_session<S, T>(
    mut stream: S,
    coord: Arc<Coordination>,
    time: T,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut session = SessionState::default();
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
            handle_frame(&mut session, &frame, &mut out_buf);
        }

        // Push the snapshot marker exactly once, only after the client has
        // signalled it is parked on the marker accessor — the lost-wakeup
        // window. The push rides the same MESSAGE frame path a real broker
        // marker takes.
        if !session.marker_pushed
            && coord.parked.load(Ordering::SeqCst)
            && !session.consumers.is_empty()
        {
            for &consumer_id in session.consumers.keys() {
                emit_snapshot_marker(&mut out_buf, consumer_id);
            }
            session.marker_pushed = true;
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

        // Race the next read against a short dispatch tick so the parked-waiter
        // marker push fires even with no further inbound traffic (the client
        // sits in the marker accessor, sending nothing). Injected clock only
        // (ADR-0011 — no host wall-clock read).
        let tick = time.sleep(Duration::from_millis(2));
        tokio::pin!(tick);
        moonpool_sim::select! {
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

fn handle_frame(session: &mut SessionState, frame: &magnetar_proto::Frame, out: &mut BytesMut) {
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
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                session.consumers.insert(s.consumer_id, s.topic.clone());
                emit_success(out, s.request_id);
            }
        }
        // Flow permits and everything else are ignored — the only payload this
        // broker ever pushes is the single delayed snapshot marker, gated on the
        // client's parked signal (below) rather than on permits.
        _ => {}
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-sim-marker-broker".to_owned(),
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

/// Emit a `REPLICATED_SUBSCRIPTION_SNAPSHOT` (`marker_type` 12) MESSAGE frame for
/// the given consumer. The proto layer filters this off the regular receive
/// stream and surfaces it as a `ReplicatedSubscriptionMarkerObserved` event,
/// which the driver drains into `replicated_subscription_markers` + fires
/// `replicated_subscription_marker_notify.notify_waiters()`.
fn emit_snapshot_marker(out: &mut BytesMut, consumer_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                partition: None,
                batch_index: None,
                ack_set: Vec::new(),
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let snapshot = pb::ReplicatedSubscriptionsSnapshot {
        snapshot_id: "sim-snap".to_owned(),
        local_message_id: Some(pb::MarkersMessageIdData {
            ledger_id: 1,
            entry_id: 1,
        }),
        clusters: vec![pb::ClusterMessageId {
            cluster: "cluster-b".to_owned(),
            message_id: pb::MarkersMessageIdData {
                ledger_id: 1,
                entry_id: 1,
            },
        }],
    };
    let mut payload = Vec::new();
    prost::Message::encode(&snapshot, &mut payload).expect("encode snapshot");
    let meta = pb::MessageMetadata {
        producer_name: "broker-marker".to_owned(),
        sequence_id: 0,
        publish_time: 0,
        marker_type: Some(ReplicatedSubscriptionMarkerKind::Snapshot.marker_type()),
        ..Default::default()
    };
    let _ = encode_payload(out, &cmd, &meta, &Bytes::from(payload));
}

/// Outcome the client workload records for one run. Every variant is
/// **bounded** — the run always terminates with one of these (a silent park
/// would leave it `None`, and `run()` would never return at all).
///
/// The split mirrors `connect_resilience.rs`'s `ConnectOutcome`: under the
/// `SimulationBuilder`'s **unavoidable** default-network chaos
/// (`NetworkConfiguration::default()` carries `bit_flip_probability: 0.0001`,
/// FDB's `BUGGIFY_WITH_PROB(0.0001)`), a fraction of seeds flip bits inside a
/// `CommandLookup` / `CommandSubscribe` frame on the wire. A control-command
/// frame carries no CRC32C (only payload frames with magic `0x0e01` do —
/// invariant #4), so the corruption surfaces as a framing `BadLength` reject
/// in the minimal in-sim broker, which then drops the connection exactly as a
/// real Pulsar broker would. The client observes that as a bounded
/// `PeerClosed` *before it ever parks on the marker accessor* — the
/// lost-wakeup window is never entered, so this is NOT the bug under test. We
/// classify it as `Severed` (acceptable) and require only that the runs which
/// *do* reach the parked-waiter window observe the marker.
#[derive(Clone, Debug)]
enum MarkerOutcome {
    /// The client subscribed, parked on the marker accessor, and the delayed
    /// snapshot marker pushed into the parked-waiter window was observed —
    /// the property this regression protects.
    Observed,
    /// The connection was severed by the default-network bit-flip chaos
    /// before the marker could be observed (corrupted `Lookup` / `Subscribe`
    /// → broker drop → bounded `PeerClosed`). A bounded, terminating outcome,
    /// not the lost-wakeup hang. Carries the stringified reason for
    /// diagnostics.
    Severed(String),
}

/// Client workload: connect, subscribe, signal "parked", then await the
/// delayed marker. The outcome is gated in `run()` (a moonpool
/// `Workload::check()` `Err` is only logged, never failing the run).
struct MarkerClientWorkload {
    coord: Arc<Coordination>,
    outcome: Arc<Mutex<Option<Result<MarkerOutcome, String>>>>,
    /// Set once any seed in a sweep observes the marker, so the sweep can
    /// assert the parked-waiter window was actually exercised at least once
    /// (a run severed by chaos before parking does not prove the property).
    observed_any: Arc<AtomicBool>,
}

impl MarkerClientWorkload {
    fn new(coord: Arc<Coordination>) -> Self {
        Self {
            coord,
            outcome: Arc::new(Mutex::new(None)),
            observed_any: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Shared flag set when any run observes the marker — read by the sweep
    /// test after `run()` to require the property held on at least one seed.
    fn observed_any(&self) -> Arc<AtomicBool> {
        self.observed_any.clone()
    }
}

#[async_trait]
impl Workload for MarkerClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn setup(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        // The same workload instance is reused across every seed in a sweep, so
        // clear the parked signal + the recorded outcome before each iteration.
        self.coord.parked.store(false, Ordering::SeqCst);
        *self.outcome.lock() = None;
        Ok(())
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let time = ctx.providers().time().clone();

        let result = self.drive(&engine, &addr, &time).await;
        // Both `Observed` and `Severed` are bounded outcomes — the run
        // terminated. Only a genuine lost-wakeup hang (or a wrong-kind marker)
        // returns `Err` and fails the run. Record which seeds actually proved
        // the property so the sweep can require it held at least once.
        if matches!(result, Ok(MarkerOutcome::Observed)) {
            self.observed_any.store(true, Ordering::SeqCst);
        }
        let gate = match &result {
            Ok(_) => Ok(()),
            Err(reason) => Err(SimulationError::InvalidState(format!(
                "marker accessor did not resolve: {reason}"
            ))),
        };
        *self.outcome.lock() = Some(result);
        gate
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        match self.outcome.lock().take() {
            // Both `Observed` and `Severed` are bounded outcomes — the run
            // terminated. The marker was observed, or the connection was
            // severed by chaos before the parked-waiter window.
            Some(Ok(MarkerOutcome::Observed)) => Ok(()),
            Some(Ok(MarkerOutcome::Severed(reason))) => {
                // Surface the severed reason for diagnostics — it is NOT a
                // failure (a bit-flip-corrupted control frame → broker drop →
                // bounded `PeerClosed` is a valid resilient outcome), but
                // capturing it confirms chaos, not the lost-wakeup hang, ended
                // the run.
                tracing::info!(
                    capture = true,
                    trail = "marker_severed_by_chaos",
                    reason = %reason,
                );
                Ok(())
            }
            Some(Err(reason)) => Err(SimulationError::InvalidState(format!(
                "marker accessor did not resolve: {reason}"
            ))),
            None => Err(SimulationError::InvalidState(
                "client workload did not record an outcome".into(),
            )),
        }
    }
}

/// Connect config tuned for the sim scheduler: a short per-dial
/// `connect_timeout` with bounded re-dials so the client's initial dial keeps
/// retrying (with backoff) until the broker workload's listener has accepted —
/// otherwise some scheduler seeds race the listener's `accept()` and the single
/// default-timeout dial gives up. Mirrors `sim_chaos.rs`'s connect resilience.
fn sim_connect_config() -> ConnectionConfig {
    ConnectionConfig {
        connect_timeout: Duration::from_millis(250),
        connect_max_retries: 64,
        operation_timeout: Duration::from_secs(20),
        ..ConnectionConfig::default()
    }
}

impl MarkerClientWorkload {
    async fn drive<P, T>(
        &self,
        engine: &MoonpoolEngine<P>,
        addr: &str,
        time: &T,
    ) -> Result<MarkerOutcome, String>
    where
        P: Providers,
        T: TimeProvider,
    {
        // Connect + subscribe can both be severed by the default-network
        // bit-flip chaos (a corrupted control-command frame → broker drop →
        // `PeerClosed`). Those are bounded `Severed` outcomes, not failures —
        // the lost-wakeup window is only entered *after* a clean subscribe.
        let connect = time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain(engine, addr, sim_connect_config()),
            )
            .await
            .map_err(|_| "connect timed out".to_owned())?;
        let client = match connect {
            Ok(client) => client,
            Err(e) => return Ok(MarkerOutcome::Severed(format!("connect: {e:?}"))),
        };

        let subscribe = time
            .timeout(
                Duration::from_secs(5),
                client.subscribe(SubscribeRequest {
                    topic: TOPIC.to_owned(),
                    subscription: SUBSCRIPTION.to_owned(),
                    receiver_queue_size: 32,
                    durable: true,
                    replicate_subscription_state: Some(true),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|_| "subscribe timed out".to_owned())?;
        if let Err(e) = subscribe {
            return Ok(MarkerOutcome::Severed(format!("subscribe: {e:?}")));
        }

        // Signal the broker we are about to park on the marker accessor. The
        // broker pushes the single snapshot marker once it observes this, so
        // the observation lands in the parked-waiter window the lost-wakeup fix
        // protects. With the pre-fix drain-then-`notified().await` accessor the
        // marker is lost and this await hangs until the sim budget records a
        // failure; the enroll-before-drain fix captures it.
        self.coord.parked.store(true, Ordering::SeqCst);

        // A *hang* here is the lost-wakeup bug — it MUST fail the run (hard
        // `Err`). A connection severed by chaos *after* parking resolves the
        // accessor to `None` (closed before the marker arrived) — still a
        // bounded `Severed` outcome, not a hang.
        let Some(observed) = time
            .timeout(
                Duration::from_secs(15),
                client.next_replicated_subscription_marker(),
            )
            .await
            .map_err(|_| "next_replicated_subscription_marker hung (lost wakeup)".to_owned())?
        else {
            return Ok(MarkerOutcome::Severed(
                "connection closed before marker arrived".to_owned(),
            ));
        };

        if observed.marker.kind != ReplicatedSubscriptionMarkerKind::Snapshot {
            return Err(format!(
                "expected Snapshot marker, got {:?}",
                observed.marker.kind
            ));
        }
        client.close().await;
        Ok(MarkerOutcome::Observed)
    }
}

/// Deterministic, chaos-free seed for the single-seed smoke — verified to
/// complete the handshake/subscribe and reach the parked-waiter window so the
/// delayed marker is observed. Fixed (not derived from `MOONPOOL_SEED`) so the
/// smoke is reproducible under the daily sweep regardless of the outer seed.
const SMOKE_HAPPY_SEED: u64 = 1;

/// Single-seed smoke: the delayed marker, pushed into the parked-waiter window,
/// is observed (post-fix). Pre-fix this hangs until the sim budget records the
/// run as failed.
#[test]
fn sim_delayed_marker_is_observed() {
    let coord = Arc::new(Coordination::default());
    let client = MarkerClientWorkload::new(coord.clone());
    let observed_any = client.observed_any();
    let report = SimulationBuilder::new()
        .run_time_budget(SIM_RUN_TIME_BUDGET)
        .workload(DelayedMarkerBroker::new(coord))
        .workload(client)
        // Pinned, chaos-free happy-path seed (verified to complete the
        // handshake/subscribe and reach the parked-waiter window). The
        // original smoke left the seed UNPINNED — a wall-clock seed — so on
        // the ~fraction of seeds the unavoidable default-network chaos
        // severs the connect/subscribe before the marker, `observed_any`
        // went false and the smoke flaked (the same class as the #290/#291
        // driver-reject seeds). Pinning a known-good seed makes it
        // deterministic.
        .set_debug_seeds(vec![SMOKE_HAPPY_SEED])
        .set_iterations(1)
        .run();
    assert!(report.successful_runs >= 1, "report: {report:?}");
    assert_eq!(report.failed_runs, 0, "report: {report:?}");
    // The single-seed smoke runs the chaos-free happy path: the one seed must
    // reach the parked-waiter window and observe the marker.
    assert!(
        observed_any.load(Ordering::SeqCst),
        "the single seed must observe the marker: {report:?}"
    );
}

/// 16-seed sweep — the delayed marker is observed across the seeds that reach
/// the parked-waiter window, and no seed hits the lost-wakeup hang.
///
/// The `SimulationBuilder`'s default network injects bit-flip corruption
/// (`bit_flip_probability: 0.0001`, FDB parity) that the builder gives no API
/// to disable. On a fraction of seeds the corruption lands inside a CRC-less
/// `CommandLookup` / `CommandSubscribe` control frame, the minimal broker
/// rejects the malformed frame and drops the connection, and the client gets a
/// bounded `PeerClosed` *before* it parks on the marker accessor — a
/// `Severed`, terminating outcome that is not the bug under test. The
/// resilience claim is therefore bounded termination (no run hangs ⇒
/// `failed_runs == 0`) **plus** the property holding on at least one
/// chaos-free seed (`observed_any`), mirroring `connect_resilience.rs`'s
/// bounded-outcome sweep.
#[test]
fn sim_delayed_marker_is_observed_sweep_16_seeds() {
    let coord = Arc::new(Coordination::default());
    let client = MarkerClientWorkload::new(coord.clone());
    let observed_any = client.observed_any();
    let report = SimulationBuilder::new()
        .run_time_budget(SIM_RUN_TIME_BUDGET)
        .workload(DelayedMarkerBroker::new(coord))
        .workload(client)
        .set_debug_seeds(sweep_seeds(16))
        .set_iterations(16)
        .run();
    // No seed may hang the marker accessor (the lost-wakeup bug returns a hard
    // `Err`, landing in `failed_runs`); chaos-severed runs resolve to a bounded
    // `Severed` outcome and stay out of `failed_runs`.
    assert_eq!(report.failed_runs, 0, "report: {report:?}");
    assert!(report.successful_runs >= 1, "report: {report:?}");
    // At least one seed must have reached the parked-waiter window and observed
    // the delayed marker — a sweep where every seed was severed by chaos would
    // never exercise the lost-wakeup window the regression protects.
    assert!(
        observed_any.load(Ordering::SeqCst),
        "no seed reached the parked-waiter window to observe the marker: {report:?}"
    );
}
