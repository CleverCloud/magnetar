// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::expect_used, clippy::too_many_lines)]

//! Issue #370 / ADR-0083 — a stalled outbound write must not starve the
//! driver's read, `driver_waker` and timer arms forever, and must be bounded
//! by `Connection::operation_timeout()` rather than run unconditionally
//! ahead of the `select!`.
//!
//! `driver_loop_inner` is `pub(crate)`, so this genuine public-API stall
//! drives the bug (and pins the fix) through [`Client::connect_plain_supervised`]
//! instead of constructing a synthetic `Transport` double (`Transport<P>` is a
//! closed enum over the provider's TCP stream — there is no seam to inject a
//! Pending-forever write at this layer the way the tokio engine's
//! `AsyncWrite` double can). The broker workload accepts the connection,
//! completes CONNECT / LOOKUP / PRODUCER, then **stops reading its socket
//! entirely** — it neither closes the stream nor drains it. moonpool-sim's
//! `SimTcpStream::poll_write` partially accepts bytes into a 64 KiB
//! per-connection send buffer and returns `Poll::Pending` (registering a
//! send-buffer waker that never fires, because nothing ever reads) once that
//! buffer fills. A client publish whose serialized frame exceeds 64 KiB
//! therefore parks mid-write — the same "peer stopped draining" scenario the
//! tokio-engine test double models, reached through the real wire.
//!
//! Pairs 1:1 with `crates/magnetar-runtime-tokio/src/driver.rs`'s
//! `driver::tests::stalled_write_is_bounded_by_operation_timeout` (ADR-0024
//! layer (b)/(c) parity — this file is the moonpool layer (c) counterpart,
//! `+1` moonpool test balancing the tokio `+1`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

const BROKER_PORT: u16 = 6650;

/// Payload comfortably larger than moonpool-sim's 64 KiB default
/// per-connection send buffer (`DEFAULT_SEND_BUFFER_CAPACITY` in
/// `moonpool-sim`), so the serialized `CommandSend` frame cannot be accepted
/// in one `poll_write` batch once the broker stops draining.
const STALLED_PAYLOAD_BYTES: usize = 200 * 1024;

async fn read_into<S: futures::io::AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    use futures::io::AsyncReadExt;
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-write-deadline-sim".to_owned(),
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
            producer_name: "write-deadline-sim".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// One broker session: answer CONNECT / PING / LOOKUP, ack the first
/// `CommandProducer`, then — the crux of the reproduction — **stop reading
/// entirely**. The stream is neither closed nor drained again: it just parks
/// forever, modelling a peer whose receive window never advances. Any
/// in-flight or subsequent client write large enough to fill the sim's send
/// buffer therefore parks.
async fn handle_session_then_stop_reading<S>(mut stream: S) -> SimulationResult<()>
where
    S: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send,
{
    use futures::io::AsyncWriteExt;

    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut producer_opened = false;
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
                        producer_opened = true;
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

        if producer_opened {
            // The stall: never read again. The socket stays open (no clean
            // EOF, no reset) so this is a one-directional stall, not a
            // peer-closed error — exactly the case `keepalive_interval`
            // (read-side liveness) cannot detect and only a write deadline
            // can.
            std::future::pending::<()>().await;
        }

        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

/// Broker workload: accepts connections in a loop (so a supervisor redial
/// after the stall lands a fresh socket) and runs
/// [`handle_session_then_stop_reading`] on each.
struct StallAfterProducerBroker {
    sessions_accepted: Arc<Mutex<u32>>,
}

impl StallAfterProducerBroker {
    fn new() -> Self {
        Self {
            sessions_accepted: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for StallAfterProducerBroker {
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
        let accepted = self.sessions_accepted.clone();
        let task = ctx.providers().task().clone();
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            *accepted.lock() += 1;
                            let _h = task.spawn_task(
                                "write-deadline-broker-session",
                                async move {
                                    let _ = handle_session_then_stop_reading(stream).await;
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

#[derive(Debug, Default)]
struct ClientObservation {
    connect_error: Option<String>,
    /// Connected right after the producer opened, before the stalled send.
    connected_before_stall: Option<bool>,
    /// `true` if `is_connected()` was observed `false` at ANY point while
    /// ticking virtual time forward after the stall began. This is sampled
    /// on every tick rather than read once at the end of the budget: with
    /// the fix in place, the supervisor keeps redialing (and each fresh
    /// connection replays the still-queued oversized publish, which stalls
    /// again against the broker's identical stop-reading behaviour), so
    /// `is_connected()` genuinely oscillates true → false → true across
    /// the run. A single end-of-budget snapshot can land inside one of the
    /// brief "just reconnected" windows and would misreport the bug as
    /// present; sampling throughout is what actually pins "the connection
    /// was correctly marked disconnected within one `operation_timeout` of
    /// the stall" without being racy against the exact sample instant.
    /// Pre-fix (issue #370) this NEVER flips `false` at all within the
    /// budget, because the write starves the timer arm forever.
    observed_disconnected_after_stall: bool,
}

struct StalledWriteClient {
    obs: Arc<Mutex<ClientObservation>>,
}

impl StalledWriteClient {
    fn new() -> Self {
        Self {
            obs: Arc::new(Mutex::new(ClientObservation::default())),
        }
    }
}

#[async_trait]
impl Workload for StalledWriteClient {
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
        let task = ctx.providers().task().clone();

        // Supervised so a post-fix write-deadline failure is observable as a
        // redial (a fresh `sessions_accepted` count), mirroring the brief's
        // "assert ... that the supervisor redials". Short backoff so the
        // redial (if it happens) lands well inside the simulation budget.
        let cfg = ConnectionConfig {
            supervisor: Some(SupervisorConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(100),
                max_attempts: Some(8),
                ..SupervisorConfig::default()
            }),
            ..ConnectionConfig::default()
        };

        let client = match time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
            )
            .await
        {
            Ok(Ok(c)) => c,
            other => {
                self.obs.lock().connect_error =
                    Some(format!("connect_plain_supervised: {other:?}"));
                return Ok(());
            }
        };

        let producer = match time
            .timeout(
                Duration::from_secs(20),
                client.open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/write-deadline-370".to_owned(),
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(Ok(p)) => p,
            other => {
                self.obs.lock().connect_error = Some(format!("open_producer: {other:?}"));
                return Ok(());
            }
        };

        self.obs.lock().connected_before_stall = Some(client.is_connected());

        // Fire the oversized publish without waiting for it — it is expected
        // to park mid-write once the broker's stall fills the sim send
        // buffer. Detached so a hung send (pre-fix) cannot itself block this
        // workload's `run()` from reaching the observation window.
        let big_payload = Bytes::from(vec![0xABu8; STALLED_PAYLOAD_BYTES]);
        let send_fut = producer.send(OutgoingMessage {
            payload: big_payload,
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: u32::try_from(STALLED_PAYLOAD_BYTES).unwrap_or(u32::MAX),
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        let _stalled_send = task.spawn_task("stalled-write-send", async move {
            let _ = send_fut.await;
        });

        // Advance virtual time in small ticks, well past one
        // `operation_timeout` (30s default), so the driver's write-deadline
        // (post-fix) or the harness itself (pre-fix, which just runs out of
        // budget) has room to act. Ticking (rather than one large sleep)
        // keeps the scheduler pumping the stalled write + driver tasks so
        // moonpool-sim's virtual-time auto-advance has real pending work to
        // resolve against at each step, AND lets us sample `is_connected()`
        // on every tick rather than once at the end — see
        // `ClientObservation::observed_disconnected_after_stall` for why a
        // single end-of-budget sample would be racy against the
        // supervisor's redial-and-restall cadence.
        for _ in 0..90 {
            if time.sleep(Duration::from_millis(500)).await.is_err() {
                break;
            }
            for _ in 0..4 {
                task.yield_now().await;
            }
            if !client.is_connected() {
                self.obs.lock().observed_disconnected_after_stall = true;
            }
        }

        Ok(())
    }
}

/// Compile-time check: `MoonpoolEngine<SimProviders>` is a valid construction
/// site (keeps the trait bounds honest if the engine surface shifts).
#[allow(dead_code)]
fn _engine_constructs(providers: SimProviders) {
    let _ = MoonpoolEngine::new(providers);
}

/// Issue #370 / ADR-0083 regression: a broker that stops draining its socket
/// after the producer opens must not be able to wedge the connection as
/// still-connected forever. Pre-fix the unbounded top-of-loop write starves
/// the timer arm so `is_connected()` never flips — this is the RED
/// assertion. Post-fix the write is bounded by `operation_timeout` and maps
/// expiry through the same `mark_disconnected()` branch every other write
/// error takes, so `is_connected()` flips `false` and the supervisor redials
/// (a second broker accept).
#[test]
fn stalled_write_is_bounded_by_operation_timeout_sim() {
    let broker = StallAfterProducerBroker::new();
    let sessions = broker.sessions_accepted.clone();
    let client = StalledWriteClient::new();
    let obs = client.obs.clone();
    let _report = SimulationBuilder::new()
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![0x0370_0370_0370_0370_u64])
        .set_iterations(1)
        .run();

    let obs = obs.lock();
    assert!(
        obs.connect_error.is_none(),
        "setup (connect + open_producer) must succeed before the stall: {obs:?}"
    );
    assert_eq!(
        obs.connected_before_stall,
        Some(true),
        "must be connected immediately after the producer opens: {obs:?}"
    );
    assert!(
        obs.observed_disconnected_after_stall,
        "issue #370: a write that never drains must be bounded by \
         operation_timeout and mark the connection disconnected — pre-fix \
         the unbounded top-of-loop write starves the timer arm and \
         is_connected() incorrectly stays true for the ENTIRE budget \
         (obs={obs:?})"
    );
    assert!(
        *sessions.lock() >= 2,
        "the supervisor must redial after the write-deadline failure, \
         producing a second broker accept (sessions_accepted={})",
        *sessions.lock()
    );
}
