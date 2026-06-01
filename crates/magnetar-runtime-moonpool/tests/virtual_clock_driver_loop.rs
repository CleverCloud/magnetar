// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::expect_used, clippy::too_many_lines)]

//! ADR-0011 sans-io clock injection — driver-loop coverage (lookup multi-agent
//! review HIGH-5).
//!
//! Pins the bug fix landed in
//! `crates/magnetar-runtime-moonpool/src/driver.rs`: every time-stamped read
//! on the supervised + non-supervised driver loops (`handle_bytes`,
//! `handle_timeout`, the deadline math for `time.sleep`,
//! `socket_alive_since` for `should_reset_backoff`, and the anti-thrash
//! detector's now) now flows through `ConnectionShared::now_instant()` —
//! which, under the moonpool engine, is wired to
//! [`moonpool_core::TimeProvider`]. Before the fix the driver called
//! `std::time::Instant::now()` directly, so under `SimProviders` the proto
//! state machine's `send_timeout` deadline was evaluated against host wall
//! time and would never fire inside a fast-running simulation budget.
//!
//! ## Shape (single-iteration sim)
//!
//! 1. Broker workload that accepts CONNECT/CONNECTED, accepts LOOKUP →
//!    [`pb::base_command::Type::LookupResponse`] (so the producer-open path succeeds), accepts
//!    CREATE PRODUCER → `ProducerSuccess`, then **deliberately ignores** every subsequent
//!    `CommandSend`. No `CommandSendReceipt` is emitted — the send hangs.
//! 2. Client workload opens a producer with [`CreateProducerRequest::send_timeout`] = 10 virtual
//!    seconds, calls `.send()`, and parks on the returned future.
//! 3. The simulator advances virtual time inside the client workload via a series of `time.sleep`
//!    ticks. After ~11 virtual seconds, the driver loop's `Connection::handle_timeout` tick (fed
//!    with the engine's `now_instant`) must observe `enqueued_at + send_timeout` <= virtual now and
//!    resolve the send future with the synthetic timeout sentinel (`code = -1`, message containing
//!    `"timeout"`).
//!
//! ## What this proves
//!
//! - Under `SimProviders` the host wall clock never advances by 10 seconds inside the test budget,
//!   but the virtual clock does. If the driver were still reading `std::time::Instant::now()`,
//!   `handle_timeout` would compare the proto's stamped `enqueued_at` (virtual, fed by user-facing
//!   `Producer::send` via `shared.now_instant()`) against a fresh host instant — and the synthetic
//!   `enqueued_at + 10s` deadline would sit ~10s in the future of host time, so the send would
//!   never time out and the test would fail. The fix routes the driver's `now` through
//!   `shared.now_instant()` too, so the comparison is consistent across virtual time and the send
//!   times out promptly.
//! - The test's host wall-clock budget (the outer `tokio::time::timeout`) is generous (30s) but
//!   bounds the run regardless of seed. Virtual time advances inside the budget via cooperative
//!   `time.sleep` ticks.
//!
//! Pairs 1:1 with the tokio mirror
//! `crates/magnetar-runtime-tokio/tests/virtual_clock_driver_loop.rs`
//! per ADR-0024 — the tokio variant uses host time and asserts the same
//! timeout envelope arrives over real loopback, keeping
//! `cargo xtask check-runtime-test-parity` balanced.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, ClientError, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

const BROKER_PORT: u16 = 6650;
const SEND_TIMEOUT_SECS: u64 = 10;

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
            server_version: "magnetar-virtual-clock-driver".to_owned(),
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
            producer_name: "magnetar-virtual-clock-driver".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Broker session: replies to CONNECT / LOOKUP / PRODUCER opens but
/// **never** responds to SEND. The send-timeout path is the gate the test
/// exercises.
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
                    }
                }
                // Deliberately ignore SEND — the test relies on the
                // client-side `send_timeout` firing via the driver loop's
                // virtual-clock-driven `handle_timeout` tick.
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

struct SendTimeoutBroker {
    sessions_handled: Arc<Mutex<u32>>,
}

impl SendTimeoutBroker {
    fn new() -> Self {
        Self {
            sessions_handled: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for SendTimeoutBroker {
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
        let handled = self.sessions_handled.clone();
        let task = ctx.providers().task().clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            *handled.lock() += 1;
                            let _h = task.spawn_task(
                                "virtual-clock-driver-broker-session",
                                async move {
                                    let _ = handle_session(stream).await;
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

/// Outcome captured by the client workload, surfaced cross-iteration so
/// the `#[test]` body can assert.
#[derive(Debug, Default)]
struct ClientObservation {
    /// `Some(true)` once the send future resolved with the synthetic
    /// timeout sentinel (broker code `-1`, message containing
    /// `"timeout"`). `Some(false)` if it resolved with a different
    /// outcome. `None` if the test budget exhausted before resolution.
    timed_out: Option<bool>,
    /// Last error seen, for assertion diagnostics on failure.
    last_error: Option<String>,
    /// Virtual-time duration the client workload observed between
    /// enqueueing the send and the future resolving.
    virtual_elapsed: Option<Duration>,
}

struct SendTimeoutClient {
    obs: Arc<Mutex<ClientObservation>>,
}

impl SendTimeoutClient {
    fn new() -> Self {
        Self {
            obs: Arc::new(Mutex::new(ClientObservation::default())),
        }
    }
}

#[async_trait]
impl Workload for SendTimeoutClient {
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

        let connect = tokio::time::timeout(
            Duration::from_secs(20),
            Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
        )
        .await;
        let Ok(Ok(client)) = connect else {
            self.obs.lock().last_error = Some(format!("connect_plain failed: {connect:?}"));
            return Ok(());
        };

        // Open the producer with a virtual send_timeout. The proto layer
        // stamps `enqueued_at = shared.now_instant()` (which under
        // SimProviders is virtual time). The fix under test routes
        // `Connection::handle_timeout`'s `now` argument through the same
        // virtual clock in the driver loop — without it, the comparison
        // `now - enqueued_at >= send_timeout` would never trigger inside
        // the host-time budget.
        let producer = match tokio::time::timeout(
            Duration::from_secs(20),
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/virtual-clock-driver".to_owned(),
                send_timeout: Some(Duration::from_secs(SEND_TIMEOUT_SECS)),
                ..Default::default()
            }),
        )
        .await
        {
            Ok(Ok(p)) => p,
            other => {
                self.obs.lock().last_error = Some(format!("open_producer failed: {other:?}"));
                return Ok(());
            }
        };

        // Capture virtual-time anchor BEFORE the send. We compare to
        // `time.now()` after the future resolves to assert the timeout
        // fired against virtual time, not host time.
        let t_before = time.now();
        let payload = Bytes::from_static(b"will-time-out");
        let payload_len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        let send_fut = producer.send(OutgoingMessage {
            payload,
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: payload_len,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });

        // Drive the simulation forward in virtual time so the driver
        // loop's `handle_timeout` tick has a chance to observe the
        // deadline. We interleave virtual sleeps with task yields so the
        // sim scheduler keeps pumping the driver task. ~12s of virtual
        // sleep covers the 10s deadline plus a margin for the timer
        // wheel resolution. The outer `tokio::time::timeout` (host) is
        // the safety budget.
        let resolved: Option<Result<magnetar_proto::MessageId, ClientError>> =
            tokio::time::timeout(Duration::from_secs(30), async {
                tokio::pin!(send_fut);
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
                for _ in 0..12 {
                    tokio::select! {
                        biased;
                        result = &mut send_fut => return Some(result),
                        slept = time.sleep(Duration::from_secs(1)) => {
                            if slept.is_err() {
                                // Time provider shut down — nothing left to
                                // do; the future must resolve in this
                                // iteration or we report a None outcome.
                                break;
                            }
                            for _ in 0..32 {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
                // Give the resolved future one final poll opportunity.
                tokio::select! {
                    biased;
                    result = &mut send_fut => Some(result),
                    () = tokio::task::yield_now() => None,
                }
            })
            .await
            .unwrap_or(None);

        let t_after = time.now();
        let mut obs = self.obs.lock();
        obs.virtual_elapsed = Some(t_after.saturating_sub(t_before));
        match resolved {
            Some(Err(ClientError::Broker { code, message })) => {
                let timed_out = code == -1 && message.to_lowercase().contains("timeout");
                obs.timed_out = Some(timed_out);
                if !timed_out {
                    obs.last_error = Some(format!(
                        "send resolved with non-timeout broker error code={code} msg={message}"
                    ));
                }
            }
            Some(Ok(_msg_id)) => {
                obs.timed_out = Some(false);
                obs.last_error = Some(
                    "send returned Ok — broker never replies; the deadline must have fired"
                        .to_owned(),
                );
            }
            Some(Err(other)) => {
                obs.timed_out = Some(false);
                obs.last_error = Some(format!("send resolved with non-broker error: {other:?}"));
            }
            None => {
                obs.timed_out = None;
                obs.last_error =
                    Some("driver pump exited before the send future resolved".to_owned());
            }
        }
        Ok(())
    }
}

/// Single-iteration sim: pin a known-good seed (kept tight to keep the
/// suite fast; the cross-runtime mirror test exercises the same envelope
/// on the tokio side under real loopback).
///
/// The assertion is: `obs.timed_out == Some(true)` AND
/// `obs.virtual_elapsed >= SEND_TIMEOUT_SECS`. Either failure indicates
/// the driver loop is still reading host time somewhere on the
/// publish/timeout path.
#[test]
fn driver_loop_send_timeout_fires_against_virtual_clock() {
    let broker = SendTimeoutBroker::new();
    let sessions = broker.sessions_handled.clone();
    let client = SendTimeoutClient::new();
    let obs = client.obs.clone();
    let _report = SimulationBuilder::new()
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![1_234_567_890_u64])
        .set_iterations(1)
        .run();

    let handled = *sessions.lock();
    assert!(
        handled >= 1,
        "broker must have accepted the client's CONNECT (sessions_handled={handled})"
    );
    let obs = obs.lock();
    assert_eq!(
        obs.timed_out,
        Some(true),
        "send must resolve with the synthetic timeout sentinel under virtual time \
         (obs={obs:?}); a `None` or `Some(false)` means the driver loop is still \
         reading host time somewhere on the timeout path (HIGH-5, ADR-0011)."
    );
    let elapsed = obs.virtual_elapsed.unwrap_or_default();
    assert!(
        elapsed >= Duration::from_secs(SEND_TIMEOUT_SECS),
        "virtual time must have advanced past the deadline before resolution \
         (elapsed={elapsed:?}, obs={obs:?})"
    );
}

/// Confirm the trait bounds compose — `MoonpoolEngine<SimProviders>` must be
/// a valid construction site. Compile-time-only.
#[allow(dead_code)]
fn _engine_sim_providers_compiles(providers: SimProviders) {
    let _engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
}
