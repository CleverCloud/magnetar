// SPDX-License-Identifier: Apache-2.0

//! Lookup-error propagation — moonpool engine, deterministic simulation.
//!
//! ## Coverage gap this pins
//!
//! The existing `lookup_redirect_chain.rs` pair covers a redirect chain that
//! *settles* (terminal `Connect`) and the redirect-cap diagnostic via
//! `open_producer`. What was *not* covered anywhere is the two ways a
//! `CommandLookupTopic` round-trip can terminate in a **bounded
//! `ClientError`** rather than a hang:
//!
//! 1. **Broker-originated `Failed`** — the broker answers the LOOKUP with `LookupType::Failed`
//!    carrying an explicit `ServerError` code + message. The proto state machine translates this to
//!    `LookupOutcome::Failed { code, message }`, and the moonpool engine must re-emit it verbatim
//!    as [`magnetar_runtime_moonpool::ClientError::Broker`] — observed directly on the public
//!    [`Client::lookup_topic`] surface (a `Failed` is terminal there).
//! 2. **Unbounded redirect loop** — the broker answers *every* LOOKUP with `LookupType::Redirect`
//!    advertising its own address. The redirect-dial loop on the public `open_producer` surface
//!    re-issues on the bootstrap (bootstrap-equality reuse) up to
//!    [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops, then surfaces a bounded
//!    `ClientError::Broker` carrying the cap diagnostic — the proof the redirect-loop `DoS` is
//!    bounded end-to-end on the public producer-open surface.
//!
//! The termination proof in both cases is that the in-sim future *resolves*
//! under the per-run time budget: a regression that dropped the `Failed`
//! translation or the redirect cap would leave the future parked, the
//! sweep-level capture would stay `false`, and the assertion would fire.
//!
//! Mirrors `crates/magnetar-runtime-tokio/tests/lookup_error_propagation.rs`
//! (real loopback) to keep the tokio ↔ moonpool 1:1 test count required by
//! ADR-0024. Both engines surface an identically-shaped bounded
//! `ClientError::Broker`.

#![allow(clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, OperationRetryConfig, SubscribeRequest,
    SupervisorConfig, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::network::sim::SimTcpListener;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

/// Port the in-sim broker binds to. The sim network hands every workload its
/// own IP, so a fixed port keeps the client → broker derivation trivial.
const BROKER_PORT: u16 = 6650;

/// Topic the client looks up. Value is irrelevant — the broker answers by
/// frame kind, not by topic — but a realistic name keeps logs readable.
const TOPIC: &str = "persistent://public/default/lookup-error-propagation";

/// Broker-side `ServerError` code echoed on the `Failed` lookup response.
/// `TopicNotFound` is the canonical "this lookup cannot resolve" answer.
const FAILED_CODE: i32 = pb::ServerError::TopicNotFound as i32;

/// Broker-side message echoed on the `Failed` lookup response — must
/// round-trip verbatim into the engine-surfaced `ClientError::Broker`.
const FAILED_MESSAGE: &str = "topic does not exist";

/// Per-run virtual-time budget. Comfortably above the legitimate lookup
/// ceiling (handshake + one LOOKUP round-trip for the `Failed` case, or up
/// to `MAX_LOOKUP_REDIRECTS` round-trips for the redirect-loop case) yet
/// tight enough that any runaway lookup-park trips the orchestrator's
/// no-progress detector instead of burning a wall-clock core. Pure function
/// of the simulated schedule → never perturbs replay determinism
/// (ADR-0011, ADR-0036).
const RUN_TIME_BUDGET: Duration = Duration::from_secs(30);

/// How the broker should answer `CommandLookupTopic` frames.
#[derive(Clone)]
enum LookupBehavior {
    /// Answer the first LOOKUP with `LookupType::Failed { error, message }`.
    Failed,
    /// Correlate a LOOKUP with `CommandSuccess`, exercising the runtime's
    /// wrong-outcome diagnostic rather than leaving the request pending.
    LookupUnexpectedSuccess,
    /// Reject the first LOOKUP with `ServiceNotReady`, then resolve it and
    /// acknowledge the producer open. The counter records wire attempts.
    RetryableThenConnect {
        attempts: Arc<AtomicUsize>,
        run_attempts: Arc<AtomicUsize>,
    },
    /// Reject the first partition-metadata request with `ServiceNotReady`,
    /// then return a partition count. The counters record all and per-run attempts.
    MetadataRetryableThenSuccess {
        attempts: Arc<AtomicUsize>,
        run_attempts: Arc<AtomicUsize>,
    },
    /// Reject every partition-metadata request with `ServiceNotReady`.
    AlwaysRetryableMetadata { attempts: Arc<AtomicUsize> },
    /// Reject partition metadata with a terminal broker response.
    MetadataNonRetryable,
    /// Reject partition metadata through a generic correlated `CommandError`.
    MetadataGenericError,
    /// Resolve the partition-metadata request with the wrong outcome kind.
    MetadataUnexpectedSuccess,
    /// Drop the connection while partition metadata is pending.
    MetadataDropConnection,
    /// Reject every lookup with `ServiceNotReady`.
    AlwaysRetryableLookup { attempts: Arc<AtomicUsize> },
    /// Resolve every lookup while recording whether a defensive deadline
    /// preflight accidentally allowed an attachment frame onto the wire.
    ControlledAttachmentDeadlines {
        producer_opens: Arc<AtomicUsize>,
        subscribes: Arc<AtomicUsize>,
    },
    /// Resolve lookup, then leave the producer-open pending until the shared
    /// operation deadline fires.
    StallProducerOpen { producer_opens: Arc<AtomicUsize> },
    /// Answer *every* LOOKUP with `LookupType::Redirect` advertising the
    /// carried URL (the broker's own address — so the engine's dial loop
    /// re-issues on the bootstrap via bootstrap-equality and the redirect cap
    /// trips after `MAX_LOOKUP_REDIRECTS` hops rather than looping forever).
    AlwaysRedirect { redirect_url: String },
}

/// Single-`poll_read` helper — appends what was read into `buf`, returns the
/// count (`0` on EOF).
async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

/// Drive one broker session: complete the handshake, then answer LOOKUPs per
/// `behavior`. Returns when the peer closes.
async fn handle_session<S>(mut stream: S, behavior: LookupBehavior) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let behavior = &behavior;
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
            if !handle_frame(&frame, &mut out_buf, behavior) {
                return Ok(());
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

fn handle_frame(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    behavior: &LookupBehavior,
) -> bool {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return true;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(lookup) = &frame.command.lookup_topic {
                if matches!(behavior, LookupBehavior::LookupUnexpectedSuccess) {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Success as i32,
                        success: Some(pb::CommandSuccess {
                            request_id: lookup.request_id,
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(out, &cmd);
                } else {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::LookupResponse as i32,
                        lookup_topic_response: Some(lookup_response(lookup.request_id, behavior)),
                        ..Default::default()
                    };
                    let _ = encode_command(out, &cmd);
                }
            }
        }
        pb::base_command::Type::PartitionedMetadata => match behavior {
            LookupBehavior::MetadataGenericError => emit_metadata_generic_error(frame, out),
            LookupBehavior::MetadataUnexpectedSuccess => {
                emit_metadata_unexpected_success(frame, out);
            }
            LookupBehavior::MetadataDropConnection => return false,
            _ => emit_partitioned_metadata(frame, out, behavior),
        },
        pb::base_command::Type::Producer => {
            match behavior {
                LookupBehavior::ControlledAttachmentDeadlines { producer_opens, .. } => {
                    producer_opens.fetch_add(1, Ordering::SeqCst);
                }
                LookupBehavior::StallProducerOpen { producer_opens } => {
                    producer_opens.fetch_add(1, Ordering::SeqCst);
                    return true;
                }
                _ => {}
            }
            emit_producer_success(frame, out);
        }
        pb::base_command::Type::Subscribe => {
            if let LookupBehavior::ControlledAttachmentDeadlines { subscribes, .. } = behavior {
                subscribes.fetch_add(1, Ordering::SeqCst);
            }
            emit_subscribe_success(frame, out);
        }
        _ => {}
    }
    true
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

fn lookup_response(request_id: u64, behavior: &LookupBehavior) -> pb::CommandLookupTopicResponse {
    let connect = || pb::CommandLookupTopicResponse {
        broker_service_url: None,
        broker_service_url_tls: None,
        response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
        request_id,
        authoritative: Some(true),
        error: None,
        message: None,
        proxy_through_service_url: Some(false),
    };
    match behavior {
        LookupBehavior::Failed => pb::CommandLookupTopicResponse {
            response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
            error: Some(FAILED_CODE),
            message: Some(FAILED_MESSAGE.to_owned()),
            ..connect()
        },
        LookupBehavior::RetryableThenConnect {
            attempts,
            run_attempts,
        } => {
            attempts.fetch_add(1, Ordering::SeqCst);
            if run_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                pb::CommandLookupTopicResponse {
                    response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
                    error: Some(pb::ServerError::ServiceNotReady as i32),
                    message: Some("bundle is loading".to_owned()),
                    ..connect()
                }
            } else {
                connect()
            }
        }
        LookupBehavior::LookupUnexpectedSuccess
        | LookupBehavior::MetadataRetryableThenSuccess { .. }
        | LookupBehavior::AlwaysRetryableMetadata { .. }
        | LookupBehavior::MetadataNonRetryable
        | LookupBehavior::MetadataGenericError
        | LookupBehavior::MetadataUnexpectedSuccess
        | LookupBehavior::MetadataDropConnection
        | LookupBehavior::ControlledAttachmentDeadlines { .. }
        | LookupBehavior::StallProducerOpen { .. } => connect(),
        LookupBehavior::AlwaysRetryableLookup { attempts } => {
            attempts.fetch_add(1, Ordering::SeqCst);
            pb::CommandLookupTopicResponse {
                response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
                error: Some(pb::ServerError::ServiceNotReady as i32),
                message: Some("bundle is still loading".to_owned()),
                ..connect()
            }
        }
        LookupBehavior::AlwaysRedirect { redirect_url } => pb::CommandLookupTopicResponse {
            broker_service_url: Some(redirect_url.clone()),
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            ..connect()
        },
    }
}

fn emit_partitioned_metadata(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    behavior: &LookupBehavior,
) {
    let Some(metadata) = &frame.command.partition_metadata else {
        return;
    };
    let (partitions, response, error, message) = match behavior {
        LookupBehavior::MetadataRetryableThenSuccess {
            attempts,
            run_attempts,
        } => {
            attempts.fetch_add(1, Ordering::SeqCst);
            if run_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                (
                    0,
                    pb::command_partitioned_topic_metadata_response::LookupType::Failed as i32,
                    Some(pb::ServerError::ServiceNotReady as i32),
                    Some("metadata store is loading".to_owned()),
                )
            } else {
                (
                    3,
                    pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
                    None,
                    None,
                )
            }
        }
        LookupBehavior::AlwaysRetryableMetadata { attempts } => {
            attempts.fetch_add(1, Ordering::SeqCst);
            (
                0,
                pb::command_partitioned_topic_metadata_response::LookupType::Failed as i32,
                Some(pb::ServerError::ServiceNotReady as i32),
                Some("metadata store is still loading".to_owned()),
            )
        }
        LookupBehavior::MetadataNonRetryable => (
            0,
            pb::command_partitioned_topic_metadata_response::LookupType::Failed as i32,
            Some(pb::ServerError::AuthorizationError as i32),
            Some("metadata lookup denied".to_owned()),
        ),
        _ => (
            0,
            pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
            None,
            None,
        ),
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
        partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
            partitions: Some(partitions),
            request_id: metadata.request_id,
            response: Some(response),
            error,
            message,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_metadata_generic_error(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Some(metadata) = &frame.command.partition_metadata else {
        return;
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id: metadata.request_id,
            error: pb::ServerError::ConsumerBusy as i32,
            message: "generic metadata command error".to_owned(),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_metadata_unexpected_success(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Some(metadata) = &frame.command.partition_metadata else {
        return;
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: metadata.request_id,
            ..Default::default()
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Some(producer) = &frame.command.producer else {
        return;
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id: producer.request_id,
            producer_name: "lookup-retry-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_subscribe_success(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Some(subscribe) = &frame.command.subscribe else {
        return;
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: subscribe.request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// In-sim broker that completes the handshake and answers LOOKUPs per
/// `behavior`. Accepts every inbound connection so the supervised /
/// non-supervised client gets a clean handshake before its lookup.
struct LookupBroker {
    behavior: LookupBehavior,
    listener: Option<SimTcpListener>,
}

impl LookupBroker {
    fn new(behavior: LookupBehavior) -> Self {
        Self {
            behavior,
            listener: None,
        }
    }
}

#[async_trait]
impl Workload for LookupBroker {
    fn name(&self) -> &str {
        "broker"
    }

    async fn setup(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let bind_addr = format!("{}:{BROKER_PORT}", ctx.my_ip());
        self.listener = Some(
            ctx.network()
                .bind(&bind_addr)
                .await
                .map_err(|e| SimulationError::InvalidState(format!("broker bind: {e}")))?,
        );
        Ok(())
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let listener = self.listener.take().ok_or_else(|| {
            SimulationError::InvalidState("broker listener missing after setup".to_owned())
        })?;

        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        // Fill `AlwaysRedirect` with the broker's OWN address so the engine's
        // dial loop re-issues on the bootstrap (bootstrap-equality reuse) and
        // the redirect cap trips after MAX_LOOKUP_REDIRECTS hops.
        let behavior = match &self.behavior {
            LookupBehavior::Failed => LookupBehavior::Failed,
            LookupBehavior::LookupUnexpectedSuccess => LookupBehavior::LookupUnexpectedSuccess,
            LookupBehavior::RetryableThenConnect { attempts, .. } => {
                LookupBehavior::RetryableThenConnect {
                    attempts: attempts.clone(),
                    run_attempts: Arc::new(AtomicUsize::new(0)),
                }
            }
            LookupBehavior::MetadataRetryableThenSuccess { attempts, .. } => {
                LookupBehavior::MetadataRetryableThenSuccess {
                    attempts: attempts.clone(),
                    run_attempts: Arc::new(AtomicUsize::new(0)),
                }
            }
            LookupBehavior::AlwaysRetryableMetadata { attempts } => {
                LookupBehavior::AlwaysRetryableMetadata {
                    attempts: attempts.clone(),
                }
            }
            LookupBehavior::MetadataNonRetryable => LookupBehavior::MetadataNonRetryable,
            LookupBehavior::MetadataGenericError => LookupBehavior::MetadataGenericError,
            LookupBehavior::MetadataUnexpectedSuccess => LookupBehavior::MetadataUnexpectedSuccess,
            LookupBehavior::MetadataDropConnection => LookupBehavior::MetadataDropConnection,
            LookupBehavior::AlwaysRetryableLookup { attempts } => {
                LookupBehavior::AlwaysRetryableLookup {
                    attempts: attempts.clone(),
                }
            }
            LookupBehavior::ControlledAttachmentDeadlines {
                producer_opens,
                subscribes,
            } => LookupBehavior::ControlledAttachmentDeadlines {
                producer_opens: producer_opens.clone(),
                subscribes: subscribes.clone(),
            },
            LookupBehavior::StallProducerOpen { producer_opens } => {
                LookupBehavior::StallProducerOpen {
                    producer_opens: producer_opens.clone(),
                }
            }
            LookupBehavior::AlwaysRedirect { .. } => LookupBehavior::AlwaysRedirect {
                redirect_url: format!("pulsar://{}:{BROKER_PORT}", ctx.my_ip()),
            },
        };
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let behavior = behavior.clone();
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = handle_session(stream, behavior).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Four-seed sweep: an already-ready deadline wins before the first wire
/// command is enqueued.
#[test]
fn expired_deadline_does_not_enqueue_lookup() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::AlwaysRetryableLookup {
            attempts: attempts.clone(),
        }))
        .workload(ExpiredDeadlineClient {
            captured_error: captured.clone(),
        })
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let errors = (*captured.lock()).unwrap_or_default();
    assert_eq!(
        errors, 4,
        "each run must surface the operation timeout (report={report:?})"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "deadline preflight must prevent every first lookup command"
    );

    let producer_opens = Arc::new(AtomicUsize::new(0));
    let subscribes = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let controlled_report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(
            LookupBehavior::ControlledAttachmentDeadlines {
                producer_opens: producer_opens.clone(),
                subscribes: subscribes.clone(),
            },
        ))
        .workload(ControlledAttachmentDeadlineClient {
            captured: captured.clone(),
        })
        .set_debug_seeds(vec![42])
        .set_iterations(1)
        .run();

    assert_eq!(
        *captured.lock(),
        vec![
            "producer target resolution exceeded operation_timeout",
            "producer open exceeded operation_timeout",
            "consumer target resolution exceeded operation_timeout",
            "consumer subscribe exceeded operation_timeout",
        ],
        "each controlled poll boundary must surface its exact operation error \
         (report={controlled_report:?})"
    );
    assert_eq!(
        producer_opens.load(Ordering::SeqCst),
        0,
        "producer deadline preflights must emit no CommandProducer"
    );
    assert_eq!(
        subscribes.load(Ordering::SeqCst),
        0,
        "consumer deadline preflights must emit no CommandSubscribe"
    );

    let stalled_opens = Arc::new(AtomicUsize::new(0));
    let stalled_error = Arc::new(Mutex::new(None));
    let stalled_report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::StallProducerOpen {
            producer_opens: stalled_opens.clone(),
        }))
        .workload(StalledProducerDeadlineClient {
            captured: stalled_error.clone(),
        })
        .set_debug_seeds(vec![42])
        .set_iterations(1)
        .run();
    assert_eq!(
        stalled_error.lock().as_deref(),
        Some("producer open exceeded operation_timeout"),
        "an enqueued producer-open must stop at the shared operation deadline \
         (report={stalled_report:?})"
    );
    assert_eq!(
        stalled_opens.load(Ordering::SeqCst),
        1,
        "the deadline must cancel the pending producer without re-enqueueing it"
    );
}

struct ReadyOnPoll {
    ready_on: usize,
    polls: Arc<AtomicUsize>,
}

impl Future for ReadyOnPoll {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let observed = self.polls.fetch_add(1, Ordering::SeqCst) + 1;
        if observed >= self.ready_on {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct ControlledAttachmentDeadlineClient {
    captured: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Workload for ControlledAttachmentDeadlineClient {
    fn name(&self) -> &str {
        "controlled-attachment-deadline-client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let client = Client::connect_plain(&engine, &addr, ConnectionConfig::default())
            .await
            .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?;

        for ready_on in [4, 5] {
            let polls = Arc::new(AtomicUsize::new(0));
            let mut deadline = Box::pin(ReadyOnPoll {
                ready_on,
                polls: polls.clone(),
            });
            let mut last_broker_error = None;
            let error = client
                .open_producer_with_operation_deadline(
                    CreateProducerRequest {
                        topic: format!("{TOPIC}-producer-{ready_on}"),
                        ..Default::default()
                    },
                    None,
                    deadline.as_mut(),
                    &mut last_broker_error,
                )
                .await
                .expect_err("controlled producer deadline must win before enqueue");
            self.capture_operation_error(error);
            assert_eq!(polls.load(Ordering::SeqCst), ready_on);
        }

        for ready_on in [4, 5] {
            let polls = Arc::new(AtomicUsize::new(0));
            let mut deadline = Box::pin(ReadyOnPoll {
                ready_on,
                polls: polls.clone(),
            });
            let mut last_broker_error = None;
            let error = client
                .subscribe_with_operation_deadline(
                    SubscribeRequest {
                        topic: format!("{TOPIC}-consumer-{ready_on}"),
                        subscription: format!("controlled-{ready_on}"),
                        ..Default::default()
                    },
                    None,
                    deadline.as_mut(),
                    &mut last_broker_error,
                )
                .await
                .expect_err("controlled consumer deadline must win before enqueue");
            self.capture_operation_error(error);
            assert_eq!(polls.load(Ordering::SeqCst), ready_on);
        }

        client.close().await;
        Ok(())
    }
}

impl ControlledAttachmentDeadlineClient {
    fn capture_operation_error(&self, error: magnetar_runtime_moonpool::ClientError) {
        let magnetar_runtime_moonpool::ClientError::Other(message) = error else {
            panic!("controlled deadline must surface ClientError::Other, got {error:?}");
        };
        let message = match message.as_str() {
            "producer target resolution exceeded operation_timeout" => {
                "producer target resolution exceeded operation_timeout"
            }
            "producer open exceeded operation_timeout" => {
                "producer open exceeded operation_timeout"
            }
            "consumer target resolution exceeded operation_timeout" => {
                "consumer target resolution exceeded operation_timeout"
            }
            "consumer subscribe exceeded operation_timeout" => {
                "consumer subscribe exceeded operation_timeout"
            }
            other => panic!("unexpected controlled deadline message: {other}"),
        };
        self.captured.lock().push(message);
    }
}

struct StalledProducerDeadlineClient {
    captured: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Workload for StalledProducerDeadlineClient {
    fn name(&self) -> &str {
        "stalled-producer-deadline-client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let client = Client::connect_plain(
            &engine,
            &addr,
            ConnectionConfig {
                operation_timeout: Duration::from_millis(5),
                ..ConnectionConfig::default()
            },
        )
        .await
        .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?;
        let error = client
            .open_producer(CreateProducerRequest {
                topic: format!("{TOPIC}-stalled-producer"),
                ..Default::default()
            })
            .await
            .expect_err("stalled producer-open must reach operation_timeout");
        *self.captured.lock() = Some(match error {
            magnetar_runtime_moonpool::ClientError::Other(message) => message,
            other => format!("unexpected error: {other:?}"),
        });
        client.close().await;
        Ok(())
    }
}

struct ExpiredDeadlineClient {
    captured_error: Arc<Mutex<Option<usize>>>,
}

#[async_trait]
impl Workload for ExpiredDeadlineClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let client = Client::connect_plain(&engine, &addr, ConnectionConfig::default())
            .await
            .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?;
        let mut deadline = Box::pin(async {});
        let mut last_broker_error = None;
        let result = client
            .lookup_topic_with_operation_deadline(
                TOPIC,
                false,
                deadline.as_mut(),
                &mut last_broker_error,
            )
            .await;
        if matches!(
            result,
            Err(magnetar_runtime_moonpool::ClientError::Other(ref message))
                if message.contains("exceeded operation_timeout")
        ) {
            let mut count = self.captured_error.lock();
            *count = Some(count.unwrap_or(0) + 1);
        }
        client.close().await;
        Ok(())
    }
}

/// Four-seed sweep: the total operation deadline wins during the retry
/// backoff, preserves the last broker error, and emits no second lookup.
#[test]
fn lookup_deadline_during_backoff_returns_last_error_without_reissue() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::AlwaysRetryableLookup {
            attempts: attempts.clone(),
        }))
        .workload(LookupDeadlineClient {
            captured_error: captured.clone(),
            operation_timeout: Duration::from_millis(5),
            initial_backoff: Duration::from_millis(50),
            max_retries: Some(1),
        })
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let errors = (*captured.lock()).unwrap_or_default();
    assert_eq!(
        errors, 4,
        "each run must surface the preserved ServiceNotReady error (report={report:?})"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        4,
        "deadline expiry during backoff must prevent every configured reissue"
    );

    let give_up_attempts = Arc::new(AtomicUsize::new(0));
    let give_up_captured = Arc::new(Mutex::new(None));
    let give_up_report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::AlwaysRetryableLookup {
            attempts: give_up_attempts.clone(),
        }))
        .workload(LookupDeadlineClient {
            captured_error: give_up_captured.clone(),
            operation_timeout: Duration::from_secs(1),
            initial_backoff: Duration::from_millis(50),
            max_retries: Some(0),
        })
        .set_debug_seeds(vec![7])
        .set_iterations(1)
        .run();
    assert_eq!(
        (*give_up_captured.lock()).unwrap_or_default(),
        1,
        "zero retry budget must surface the first retryable lookup error \
         (report={give_up_report:?})"
    );
    assert_eq!(
        give_up_attempts.load(Ordering::SeqCst),
        1,
        "zero retry budget must not reissue the lookup"
    );
}

struct LookupDeadlineClient {
    captured_error: Arc<Mutex<Option<usize>>>,
    operation_timeout: Duration,
    initial_backoff: Duration,
    max_retries: Option<u32>,
}

#[async_trait]
impl Workload for LookupDeadlineClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let config = ConnectionConfig {
            operation_timeout: self.operation_timeout,
            ..ConnectionConfig::default()
        };
        let client = Client::connect_plain(&engine, &addr, config)
            .await
            .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?
            .with_operation_retry(OperationRetryConfig {
                initial_backoff: self.initial_backoff,
                max_backoff: self.initial_backoff,
                max_retries: self.max_retries,
            });
        let result = client.lookup_topic(TOPIC, false).await;
        if matches!(
            result,
            Err(magnetar_runtime_moonpool::ClientError::Broker {
                code,
                ref message
            }) if code == pb::ServerError::ServiceNotReady as i32
                && message == "bundle is still loading"
        ) {
            let mut count = self.captured_error.lock();
            *count = Some(count.unwrap_or(0) + 1);
        }
        client.close().await;
        Ok(())
    }
}

/// Four-seed sweep: a retryable partition-metadata rejection is re-issued
/// and the eventual broker count is returned.
#[test]
fn partition_metadata_service_not_ready_retries_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(
            LookupBehavior::MetadataRetryableThenSuccess {
                attempts: attempts.clone(),
                run_attempts: Arc::new(AtomicUsize::new(0)),
            },
        ))
        .workload(MetadataRetryClient {
            captured_error: captured.clone(),
        })
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    assert!(
        captured.lock().is_none(),
        "retryable metadata request must eventually succeed (report={report:?})"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        8,
        "each of four runs must issue one initial metadata request plus one retry"
    );

    let deadline_attempts = Arc::new(AtomicUsize::new(0));
    let deadline_error = capture_metadata_outcome(
        LookupBehavior::AlwaysRetryableMetadata {
            attempts: deadline_attempts.clone(),
        },
        Duration::from_millis(5),
        OperationRetryConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(50),
            max_retries: Some(1),
        },
    );
    assert!(
        deadline_error.contains("metadata store is still loading"),
        "metadata deadline must preserve the last retryable broker error, got {deadline_error}"
    );
    assert_eq!(
        deadline_attempts.load(Ordering::SeqCst),
        1,
        "metadata deadline during backoff must prevent the retry"
    );

    let non_retryable = capture_metadata_outcome(
        LookupBehavior::MetadataNonRetryable,
        Duration::from_secs(1),
        OperationRetryConfig::default(),
    );
    assert!(
        non_retryable.contains("metadata lookup denied"),
        "non-retryable metadata response must surface as Broker, got {non_retryable}"
    );

    let generic_error = capture_metadata_outcome(
        LookupBehavior::MetadataGenericError,
        Duration::from_secs(1),
        OperationRetryConfig::default(),
    );
    assert!(
        generic_error.contains("generic metadata command error"),
        "generic correlated metadata error must surface as Broker, got {generic_error}"
    );

    let terminal = capture_metadata_outcome(
        LookupBehavior::MetadataDropConnection,
        Duration::from_secs(1),
        OperationRetryConfig::default(),
    );
    assert_eq!(
        terminal, "Err(PeerClosed)",
        "terminal drop must resolve pending metadata as PeerClosed"
    );

    let unexpected = capture_metadata_outcome(
        LookupBehavior::MetadataUnexpectedSuccess,
        Duration::from_secs(1),
        OperationRetryConfig::default(),
    );
    assert!(
        unexpected.contains("unexpected partitioned metadata outcome: Success"),
        "wrong correlated outcome must surface a diagnostic, got {unexpected}"
    );
}

fn capture_metadata_outcome(
    behavior: LookupBehavior,
    operation_timeout: Duration,
    retry: OperationRetryConfig,
) -> String {
    let captured = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(behavior))
        .workload(MetadataOutcomeClient {
            captured: captured.clone(),
            operation_timeout,
            retry,
        })
        .set_debug_seeds(vec![11])
        .set_iterations(1)
        .run();
    captured
        .lock()
        .clone()
        .unwrap_or_else(|| format!("no metadata outcome captured (report={report:?})"))
}

struct MetadataOutcomeClient {
    captured: Arc<Mutex<Option<String>>>,
    operation_timeout: Duration,
    retry: OperationRetryConfig,
}

#[async_trait]
impl Workload for MetadataOutcomeClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let client = Client::connect_plain(
            &engine,
            &addr,
            ConnectionConfig {
                operation_timeout: self.operation_timeout,
                ..ConnectionConfig::default()
            },
        )
        .await
        .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?
        .with_operation_retry(self.retry.clone());
        *self.captured.lock() = Some(format!(
            "{:?}",
            client.partitioned_topic_metadata(TOPIC).await
        ));
        client.close().await;
        Ok(())
    }
}

struct MetadataRetryClient {
    captured_error: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Workload for MetadataRetryClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let config = ConnectionConfig::default();
        let client = Client::connect_plain(&engine, &addr, config)
            .await
            .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?
            .with_operation_retry(OperationRetryConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                max_retries: Some(1),
            });
        match client.partitioned_topic_metadata(TOPIC).await {
            Ok(3) => {}
            Ok(other) => {
                *self.captured_error.lock() = Some(format!("unexpected partition count: {other}"));
            }
            Err(err) => *self.captured_error.lock() = Some(format!("{err:?}")),
        }
        client.close().await;
        Ok(())
    }
}

/// Client workload: dial the broker via `Client::connect_plain`, issue one
/// `lookup_topic`, and record the *bounded* `ClientError::Broker` it
/// surfaces. The workload itself never returns `Err` — the sweep-level
/// assertion in the `#[test]` is the authoritative gate (mirrors
/// `handshake_error_capture.rs`).
struct LookupErrorClient {
    /// Stringified `ClientError` from the lookup, captured cross-iteration so
    /// a regression surfaces the actual error shape rather than a generic
    /// "nothing was captured".
    captured_error: Arc<Mutex<Option<String>>>,
    /// How the client drives the lookup surface.
    drive: ClientDrive,
}

/// Which public surface the client exercises.
#[derive(Clone, Copy)]
enum ClientDrive {
    /// Raw `lookup_topic` on an unsupervised client — a `Failed` response is
    /// terminal here and surfaces directly.
    RawLookup,
    /// `open_producer` on a supervised client — this drives the redirect-dial
    /// loop, so an unbounded `Redirect` chain trips the cap end-to-end.
    OpenProducer,
}

impl LookupErrorClient {
    fn new(drive: ClientDrive) -> Self {
        Self {
            captured_error: Arc::new(Mutex::new(None)),
            drive,
        }
    }
}

/// Four-seed sweep: a retryable lookup rejection is re-issued under the
/// configured operation policy and the eventual success opens the producer.
#[test]
fn lookup_service_not_ready_retries_then_opens_producer() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::RetryableThenConnect {
            attempts: attempts.clone(),
            run_attempts: Arc::new(AtomicUsize::new(0)),
        }))
        .workload(LookupRetryClient {
            captured_error: captured.clone(),
        })
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    assert!(
        captured.lock().is_none(),
        "retryable lookup must eventually open the producer (report={report:?})"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        8,
        "each of four runs must issue one initial lookup plus one retry"
    );
}

/// Client workload dedicated to the retry-success case so it can install the
/// short retry policy without changing the existing terminal-error fixtures.
struct LookupRetryClient {
    captured_error: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Workload for LookupRetryClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());
        let config = ConnectionConfig::default();
        let client = Client::connect_plain(&engine, &addr, config)
            .await
            .map_err(|err| SimulationError::InvalidState(format!("connect: {err}")))?
            .with_operation_retry(OperationRetryConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                max_retries: Some(1),
            });
        if let Err(err) = client
            .open_producer(CreateProducerRequest {
                topic: TOPIC.to_owned(),
                ..Default::default()
            })
            .await
        {
            *self.captured_error.lock() = Some(format!("{err:?}"));
        }
        client.close().await;
        Ok(())
    }
}

#[async_trait]
impl Workload for LookupErrorClient {
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

        // A timeout here means the sim budget never delivered the resolution;
        // the sweep-level assertion is the authoritative gate. No
        // `tokio::time::timeout` wrapper on the operation itself — the whole
        // point is that the proto + engine layers bound it (Failed translation
        // / redirect cap). Wrapping it would mask a regression where the bound
        // stopped firing.
        match self.drive {
            ClientDrive::RawLookup => {
                let connect = time
                    .timeout(
                        Duration::from_secs(20),
                        Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
                    )
                    .await;
                let Ok(Ok(client)) = connect else {
                    return Ok(());
                };
                if let Err(ref err) = client.lookup_topic(TOPIC, false).await {
                    *self.captured_error.lock() = Some(format!("{err:?}"));
                }
                client.close().await;
            }
            ClientDrive::OpenProducer => {
                // The redirect-dial loop lives on the public `open_producer`
                // path and needs the proxy pool (supervised client).
                let cfg = ConnectionConfig {
                    supervisor: Some(SupervisorConfig::default()),
                    ..ConnectionConfig::default()
                };
                let connect = time
                    .timeout(
                        Duration::from_secs(20),
                        Client::connect_plain_supervised(&engine, &addr, cfg, None, None),
                    )
                    .await;
                let Ok(Ok(client)) = connect else {
                    return Ok(());
                };
                let outcome = client
                    .open_producer(CreateProducerRequest {
                        topic: TOPIC.to_owned(),
                        ..Default::default()
                    })
                    .await;
                if let Err(ref err) = outcome {
                    *self.captured_error.lock() = Some(format!("{err:?}"));
                }
                client.close().await;
            }
        }
        Ok(())
    }
}

/// 4-seed sweep: a broker-originated `LookupType::Failed` response must
/// surface as a bounded [`magnetar_runtime_moonpool::ClientError::Broker`]
/// carrying the broker's `ServerError` code AND verbatim message — the lookup
/// future resolves with an error instead of parking forever waiting for a
/// `Connect`.
#[test]
fn lookup_failed_response_surfaces_bounded_broker_error() {
    let client = LookupErrorClient::new(ClientDrive::RawLookup);
    let captured = client.captured_error.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::Failed))
        .workload(client)
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let err = captured.lock().clone();
    let err = err.expect(
        "lookup against a Failed response must resolve to a bounded ClientError — \
         the future parked instead of surfacing the broker error",
    );
    // The broker's `ServerError` code and verbatim message must both ride
    // the surfaced `ClientError::Broker { code, message }`. `Debug` for the
    // `Broker` variant renders both fields, so substring checks are stable.
    assert!(
        err.contains(&FAILED_CODE.to_string()),
        "ClientError must carry the broker ServerError code {FAILED_CODE} (got {err:?}, \
         report={report:?})",
    );
    assert!(
        err.contains(FAILED_MESSAGE),
        "ClientError must carry the verbatim broker message \"{FAILED_MESSAGE}\" \
         (got {err:?}, report={report:?})",
    );

    let unexpected_client = LookupErrorClient::new(ClientDrive::RawLookup);
    let unexpected_error = unexpected_client.captured_error.clone();
    let unexpected_report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::LookupUnexpectedSuccess))
        .workload(unexpected_client)
        .set_debug_seeds(vec![42])
        .set_iterations(1)
        .run();
    let unexpected_error = unexpected_error
        .lock()
        .clone()
        .expect("wrong-kind lookup response must surface a bounded diagnostic");
    assert!(
        unexpected_error.contains("unexpected lookup outcome: Success"),
        "wrong-kind lookup response must identify the correlated outcome \
         (got {unexpected_error:?}, report={unexpected_report:?})"
    );
}

/// 4-seed sweep: a broker that answers *every* LOOKUP with `Redirect` (to its
/// own address) must NOT hang `open_producer`. The engine's redirect-dial loop
/// re-issues on the bootstrap (bootstrap-equality reuse) up to
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops and then surfaces a
/// bounded [`magnetar_runtime_moonpool::ClientError::Broker`] carrying the
/// "redirect cap exceeded" diagnostic — proving the redirect-loop `DoS` is
/// bounded end-to-end on the public producer-open surface.
#[test]
fn lookup_redirect_loop_surfaces_bounded_cap_error() {
    let client = LookupErrorClient::new(ClientDrive::OpenProducer);
    let captured = client.captured_error.clone();
    let report = SimulationBuilder::new()
        .run_time_budget(RUN_TIME_BUDGET)
        .workload(LookupBroker::new(LookupBehavior::AlwaysRedirect {
            // Replaced with the broker's real address in `LookupBroker::run`.
            redirect_url: String::new(),
        }))
        .workload(client)
        .set_debug_seeds(vec![1, 2, 3, 42])
        .set_iterations(4)
        .run();

    let err = captured.lock().clone();
    let err = err.expect(
        "an unbounded redirect loop must resolve to a bounded ClientError — \
         the redirect cap did not fire and the lookup parked",
    );
    assert!(
        err.contains("redirect cap exceeded"),
        "ClientError must carry the redirect-cap diagnostic (got {err:?}, report={report:?})",
    );
}
