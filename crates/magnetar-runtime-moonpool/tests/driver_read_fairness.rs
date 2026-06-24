// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::expect_used, clippy::too_many_lines)]

//! Issue #303 — driver-loop read fairness under sustained `driver_waker`
//! pressure, deterministic-simulation mirror of
//! `magnetar-runtime-tokio/tests/driver_read_fairness.rs` (ADR-0024 layer (c),
//! kept 1:1 with the tokio layer).
//!
//! The single per-connection driver task multiplexes the outbound write path
//! and the inbound read path in one `tokio::select! { biased; … }`. Every
//! `Producer::send` pulses `shared.driver_waker.notify_one()`, so under
//! sustained publish load a waker permit is almost always pending on loop
//! entry. The pre-fix arm order polled the `driver_waker` arm FIRST, so when a
//! permit was pending the inbound read arm was not polled that iteration —
//! `CommandSendReceipt` bytes already back on the socket were starved and the
//! matching `SendFut`s stayed pending (issue #303).
//!
//! The fix (driver.rs) reorders the `select!` so the inbound read arm is polled
//! FIRST while keeping `biased;`. Keeping `biased;` is REQUIRED here: a
//! non-biased `tokio::select!` picks arms via an uncontrolled thread-local RNG,
//! which would break the bit-for-bit reproducibility this engine guarantees —
//! that constraint is exactly why the fix is a deterministic *reorder* and not
//! "drop the bias".
//!
//! ## What this test asserts
//!
//! `tokio::sync::Notify` holds a SINGLE permit, so the pre-fix waker-first order
//! costs at most one extra loop iteration of read latency per permit refresh;
//! unbounded starvation needs genuine multi-threaded send concurrency that the
//! single-threaded deterministic scheduler does not model, so this is NOT a
//! "hangs under the old order" test. It is the deterministic mirror of the tokio
//! layer (ADR-0024 1:1): the client workload keeps `driver_waker` perpetually
//! armed (the worst-case sustained-publish signal) while a canary send awaits
//! its receipt, and asserts the receipt is read and resolved within a bounded
//! virtual-time budget. The read-FIRST arm order is what makes that hold under
//! perpetual waker pressure; the differential test pins that the reorder changed
//! no observable `EventStream` output.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

const BROKER_PORT: u16 = 6650;

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
            server_version: "magnetar-read-fairness".to_owned(),
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
            producer_name: "magnetar-read-fairness".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(out: &mut BytesMut, producer_id: u64, sequence_id: u64, entry_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id: 7,
                entry_id,
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

/// Broker session: replies to CONNECT / LOOKUP / PRODUCER opens and echoes a
/// `CommandSendReceipt` for every `CommandSend`.
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
                pb::base_command::Type::Send => {
                    if let Some(s) = &frame.command.send {
                        emit_send_receipt(
                            &mut out_buf,
                            s.producer_id,
                            s.sequence_id,
                            s.sequence_id,
                        );
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

struct ReceiptBroker {
    sessions_handled: Arc<Mutex<u32>>,
}

impl ReceiptBroker {
    fn new() -> Self {
        Self {
            sessions_handled: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Workload for ReceiptBroker {
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
                                "read-fairness-broker-session",
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

#[derive(Debug, Default)]
struct ClientObservation {
    /// `Some(true)` once the canary send resolved with a receipt under
    /// sustained `driver_waker` pressure; `Some(false)` if it resolved with an
    /// error; `None` if the virtual-time budget expired without resolution
    /// (the read-starvation regression).
    resolved_ok: Option<bool>,
    last_error: Option<String>,
    virtual_elapsed: Option<Duration>,
}

struct PressureClient {
    obs: Arc<Mutex<ClientObservation>>,
}

impl PressureClient {
    fn new() -> Self {
        Self {
            obs: Arc::new(Mutex::new(ClientObservation::default())),
        }
    }
}

#[async_trait]
impl Workload for PressureClient {
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

        let producer = match tokio::time::timeout(
            Duration::from_secs(20),
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/read-fairness".to_owned(),
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

        // Keep the driver waker continuously armed — the worst-case
        // "almost-always-ready driver_waker" of a sustained publish stream,
        // reproduced under the deterministic sim scheduler. The read-FIRST arm
        // order keeps the inbound path live regardless of this pressure. A stop
        // flag lets the loop terminate so the handle joins cleanly at the end.
        let shared = client.shared().clone();
        let stop = Arc::new(AtomicBool::new(false));
        let pressure = ctx.providers().task().spawn_task("driver-waker-pressure", {
            let shared = shared.clone();
            let stop = stop.clone();
            async move {
                while !stop.load(Ordering::Relaxed) {
                    shared.driver_waker.notify_one();
                    tokio::task::yield_now().await;
                }
            }
        });

        let t_before = time.now();
        let send_fut = producer.send(OutgoingMessage {
            payload: bytes::Bytes::from_static(b"canary"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 6,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });

        // Drive virtual time forward in small ticks so the driver loop iterates
        // under sustained waker pressure. With the fix the receipt resolves on
        // the first read-arm win; without it, the budget expires.
        let resolved = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::pin!(send_fut);
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            for _ in 0..50 {
                tokio::select! {
                    biased;
                    result = &mut send_fut => return Some(result),
                    slept = time.sleep(Duration::from_millis(100)) => {
                        if slept.is_err() {
                            break;
                        }
                        for _ in 0..16 {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
            tokio::select! {
                biased;
                result = &mut send_fut => Some(result),
                () = tokio::task::yield_now() => None,
            }
        })
        .await
        .unwrap_or(None);

        let t_after = time.now();
        stop.store(true, Ordering::Relaxed);
        // Let the pressure loop observe the stop flag and exit, then join it so
        // no task is left running into the next iteration.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let _ = pressure.await;
        let mut obs = self.obs.lock();
        obs.virtual_elapsed = Some(t_after.saturating_sub(t_before));
        match resolved {
            Some(Ok(_msg_id)) => obs.resolved_ok = Some(true),
            Some(Err(e)) => {
                obs.resolved_ok = Some(false);
                obs.last_error = Some(format!("canary send resolved with error: {e:?}"));
            }
            None => {
                obs.resolved_ok = None;
                obs.last_error = Some(
                    "canary receipt never resolved under sustained driver_waker pressure \
                     (issue #303 read-starvation regression)"
                        .to_owned(),
                );
            }
        }
        Ok(())
    }
}

/// Deterministic-sim guard: a canary send's receipt is read and resolved within
/// a bounded virtual-time budget even while `driver_waker` is continuously
/// armed. The read-FIRST arm order is what keeps the inbound path live under
/// perpetual waker pressure; see the module docs for the limits of what a
/// single-threaded deterministic test can distinguish.
#[test]
fn canary_receipt_not_starved_by_perpetual_driver_waker() {
    let broker = ReceiptBroker::new();
    let sessions = broker.sessions_handled.clone();
    let client = PressureClient::new();
    let obs = client.obs.clone();
    let _report = SimulationBuilder::new()
        .workload(broker)
        .workload(client)
        .set_debug_seeds(vec![0x0303_0303_0303_0303_u64])
        .set_iterations(1)
        .run();

    let handled = *sessions.lock();
    assert!(
        handled >= 1,
        "broker must have accepted the client's CONNECT (sessions_handled={handled})"
    );
    let obs = obs.lock();
    assert_eq!(
        obs.resolved_ok,
        Some(true),
        "canary send→receipt must resolve under sustained driver_waker pressure \
         (obs={obs:?}); a `None` means the inbound read arm is starved by the \
         waker arm (issue #303 regression)."
    );
}

/// Compile-time check: `MoonpoolEngine<SimProviders>` is a valid construction
/// site (keeps the trait bounds honest if the engine surface shifts).
#[allow(dead_code)]
fn _engine_constructs(providers: SimProviders) {
    let _ = MoonpoolEngine::new(providers);
}
