// SPDX-License-Identifier: Apache-2.0

//! `Producer::flush` must not return while a send is still unacknowledged because a reconnect
//! relocated it — moonpool engine under the deterministic simulation. 1:1 twin of
//! `crates/magnetar-runtime-tokio/tests/producer_flush_reconnect.rs` (ADR-0024 runtime-test
//! parity).
//!
//! `Connection::reset()` moves every replayable in-flight publish out of the producer's `pending`
//! queue into the reset snapshot bucket. The sends return to `pending` only when the broker
//! acknowledges the re-attachment (`CommandProducerSuccess`) on the new session. `flush` is
//! documented as "wait for every in-flight send to be acknowledged by the broker" and as the call
//! to make before `close()` for at-least-once semantics on the trailing sends, yet it loops on
//! `producer_pending_count`, which reads `pending.len()` alone: while the sends sit in the
//! snapshot bucket it reads `0` and `flush` resolves `Ok(())` with no send acknowledged.
//!
//! Scenario: a send reaches the broker and the socket drops before any receipt; the supervisor
//! redials, the in-sim broker answers the handshake and then WITHHOLDS the re-attachment's
//! `CommandProducerSuccess` until the client has probed `flush()`. A single poll of `flush()`
//! inside that window must be `Pending`; once the ack is released the replayed send is
//! acknowledged and `flush()` resolves.

use std::future::Future as _;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

const BROKER_PORT: u16 = 6650;

/// Virtual-time granularity at which both workloads poll the shared observation.
const POLL_STEP: Duration = Duration::from_millis(10);

/// Upper bound on [`POLL_STEP`] ticks either side waits for the other (60 s of virtual time), so
/// a wedged run ends instead of spinning.
const MAX_POLL_STEPS: u32 = 6_000;

/// What the broker and the client observed, shared between the two workloads.
#[derive(Debug, Default)]
struct Observation {
    /// The first session received the send and dropped without a receipt.
    first_session_send_seen: bool,
    /// The second session received the re-attachment's `CommandProducer`.
    reattach_seen: bool,
    /// The client probed `flush()` inside the withheld-ack window: the broker may now answer.
    flush_probed: bool,
    /// The second session wrote the receipt of a replayed `CommandSend`.
    receipts_written: u32,
    /// Setup failure (connect / open / wait bound), if any.
    setup_error: Option<String>,
    /// The single probe poll of `flush()` inside the window resolved: the defect.
    flush_resolved_in_window: Option<bool>,
    /// `receipts_written` at the instant `flush()` finally resolved.
    receipts_at_flush_resolution: Option<u32>,
    /// `flush()` resolved with `Ok`.
    flush_ok: Option<bool>,
    /// The replayed send resolved with `Ok`.
    send_ok: Option<bool>,
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "flush-reconnect-broker/0".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
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
            producer_name: "flush-reconnect-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(out: &mut BytesMut, producer_id: u64, sequence_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id: 7,
                entry_id: sequence_id,
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

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

/// One in-sim broker session. The first session to answer `CONNECT` is role 1 (acknowledge
/// everything except the send, on which it drops the socket); every later one is role 2 (hold the
/// re-attachment's `CommandProducerSuccess` until the client probed `flush()`, then receipt every
/// send). A session claims its role only when its handshake lands, so connect-faulted dials that
/// never handshake do not consume one.
async fn serve_session<S, T>(
    mut stream: S,
    time: T,
    obs: Arc<Mutex<Observation>>,
    session_role: Arc<Mutex<u32>>,
) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: TimeProvider,
{
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut my_role = 0u32;
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
                pb::base_command::Type::Connect => {
                    {
                        let mut role = session_role.lock();
                        *role += 1;
                        my_role = *role;
                    }
                    emit_connected(&mut out_buf);
                }
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        if my_role >= 2 {
                            obs.lock().reattach_seen = true;
                            // Withhold the ack until the client probed `flush()`.
                            for _ in 0..MAX_POLL_STEPS {
                                if obs.lock().flush_probed {
                                    break;
                                }
                                if time.sleep(POLL_STEP).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        emit_producer_success(&mut out_buf, p.request_id);
                    }
                }
                pb::base_command::Type::Send => {
                    if let Some(send) = &frame.command.send {
                        if my_role == 1 {
                            // The send is on the wire; drop without a receipt.
                            obs.lock().first_session_send_seen = true;
                            return Ok(());
                        }
                        obs.lock().receipts_written += 1;
                        emit_send_receipt(&mut out_buf, send.producer_id, send.sequence_id);
                    }
                }
                _ => {}
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
        }
        match read_into(&mut stream, &mut read_buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(_) => {}
        }
    }
}

struct FlushBroker {
    obs: Arc<Mutex<Observation>>,
}

#[async_trait]
impl Workload for FlushBroker {
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
        let session_role = Arc::new(Mutex::new(0u32));
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let obs = self.obs.clone();
                            let role = session_role.clone();
                            let time = time.clone();
                            let _handle = task.spawn_task("flush-reconnect-session", async move {
                                let _ = serve_session(stream, time, obs, role).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

struct FlushClient {
    obs: Arc<Mutex<Observation>>,
}

impl FlushClient {
    /// Record a setup failure; the test body asserts it is absent.
    fn fail(&self, message: String) {
        self.obs.lock().setup_error = Some(message);
    }
}

#[async_trait]
impl Workload for FlushClient {
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

        let config = ConnectionConfig {
            supervisor: Some(SupervisorConfig {
                initial_backoff: Duration::from_millis(20),
                max_backoff: Duration::from_millis(100),
                ..Default::default()
            }),
            ..Default::default()
        };
        let client =
            match Client::connect_plain_supervised(&engine, &addr, config, None, None).await {
                Ok(client) => client,
                Err(e) => {
                    self.fail(format!("connect: {e:?}"));
                    return Ok(());
                }
            };
        let producer = match client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/flush-reconnect".to_owned(),
                ..Default::default()
            })
            .await
        {
            Ok(producer) => producer,
            Err(e) => {
                self.fail(format!("producer open: {e:?}"));
                return Ok(());
            }
        };

        // One send reaches the broker; the socket drops before any receipt.
        let send = producer.send(outgoing(b"in-flight"));

        // The supervisor reset the connection, redialled and re-handshaked: the re-attachment's
        // `CommandProducer` is on the wire and its ack is withheld, so the send is parked in the
        // reset snapshot bucket, in no live queue.
        let mut waited = 0;
        while !self.obs.lock().reattach_seen {
            waited += 1;
            if waited > MAX_POLL_STEPS || time.sleep(POLL_STEP).await.is_err() {
                self.fail("the re-attachment never reached the second session".into());
                return Ok(());
            }
        }

        // Probe `flush()` once inside the window. It registers its wake-up on the first poll.
        let mut flush = Box::pin(producer.flush());
        let probe = std::future::poll_fn(|cx| Poll::Ready(flush.as_mut().poll(cx))).await;
        {
            let mut obs = self.obs.lock();
            obs.flush_resolved_in_window = Some(probe.is_ready());
            obs.flush_probed = true;
        }

        // Whether or not the probe resolved, let the run finish: the ack is released, the
        // snapshot is replayed and the receipt arrives.
        let flush_result = match probe {
            Poll::Ready(result) => result,
            Poll::Pending => flush.await,
        };
        {
            let mut obs = self.obs.lock();
            obs.receipts_at_flush_resolution = Some(obs.receipts_written);
            obs.flush_ok = Some(flush_result.is_ok());
        }
        let send_result = send.await;
        self.obs.lock().send_ok = Some(send_result.is_ok());

        client.close().await;
        Ok(())
    }
}

#[test]
fn flush_stays_pending_while_the_reset_snapshot_holds_an_unacknowledged_send() {
    let obs = Arc::new(Mutex::new(Observation::default()));
    let _report = SimulationBuilder::new()
        .workload(FlushBroker { obs: obs.clone() })
        .workload(FlushClient { obs: obs.clone() })
        .set_debug_seeds(vec![0x0451_f1a5_u64])
        .set_iterations(1)
        .run();

    let obs = obs.lock();
    assert_eq!(
        obs.setup_error, None,
        "setup (connect, open, redial) must succeed: {obs:?}"
    );
    assert!(
        obs.first_session_send_seen && obs.reattach_seen,
        "the scenario must reach the reset -> re-attach window: {obs:?}"
    );
    assert_eq!(
        obs.flush_resolved_in_window,
        Some(false),
        "flush() resolved while the replayed send was still unacknowledged: the send sits in the \
         reset snapshot bucket, out of `pending`, until the re-attachment is acked: {obs:?}"
    );
    assert_eq!(
        (obs.flush_ok, obs.send_ok),
        (Some(true), Some(true)),
        "after the ack is released both the flush and the replayed send must succeed: {obs:?}"
    );
    assert_eq!(
        obs.receipts_at_flush_resolution,
        Some(1),
        "flush resolved before the replayed send reached the broker and was acknowledged: {obs:?}"
    );
}
