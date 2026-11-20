// SPDX-License-Identifier: Apache-2.0

//! The façade `TableView` must run on the provider bundle its client was built over (ADR-0078).
//!
//! `TableViewBuilder::create` is generic over `E: Engine`, so it is reachable from
//! `PulsarClient<MoonpoolEngine<SimProviders>>`, the entry the façade advertises for seeded
//! simulation. Under `SimProviders` tasks run on Moonpool's own executor with no Tokio runtime or
//! reactor, so the drain task has to be spawned through the client's `TaskProvider` (ADR-0139).
//! Before ADR-0139 `table_view::spawn_drain` called `tokio::spawn` directly: with no ambient
//! runtime that panicked (and with one it would have run on the host executor, outside the seed).
//!
//! The scenario is the smallest one that reaches the call: a broker that answers CONNECT, LOOKUP
//! and SUBSCRIBE, a runtime client over the simulated network, and one `table_view(..).create()`.
//! The call runs under `catch_unwind` so the panic surfaces as an assertion naming the payload
//! instead of tearing the simulator down. The drain scenario leaves
//! `auto_update_partitions_interval` unset so the panic site it reports is the drain's; the
//! ticker scenario sets it, and `create()` arms the shared `AutoUpdateTask` ticker before the
//! drain, so before ADR-0139 it reported the ticker's `tokio::spawn` / `tokio::time::interval`.
//!
//! This file has no tokio twin: under a Tokio runtime `tokio::spawn` is the provider's own spawn,
//! and only `SimProviders` exposes the bypass. The two-engine scenario, which also checks that the
//! tasks do their work and stop, is `background_tasks_equivalence.rs`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::FutureExt;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar::PulsarClient;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;

const BROKER_PORT: u16 = 6650;
const TOPIC: &str = "persistent://public/default/table-view-sim-providers";

async fn read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut tmp = vec![0u8; 64 * 1024];
    let n = stream.read(&mut tmp).await?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

fn emit(out: &mut BytesMut, cmd: &pb::BaseCommand) {
    let _ = encode_command(out, cmd);
}

/// Broker session: CONNECT, PING, LOOKUP and SUBSCRIBE are answered; everything else (the
/// consumer's FLOW included) is ignored.
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
                pb::base_command::Type::Connect => emit(
                    &mut out_buf,
                    &pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "magnetar-table-view-sim".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    },
                ),
                pb::base_command::Type::Ping => emit(
                    &mut out_buf,
                    &pb::BaseCommand {
                        r#type: pb::base_command::Type::Pong as i32,
                        pong: Some(pb::CommandPong {}),
                        ..Default::default()
                    },
                ),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit(
                            &mut out_buf,
                            &pb::BaseCommand {
                                r#type: pb::base_command::Type::LookupResponse as i32,
                                lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                                    broker_service_url: None,
                                    broker_service_url_tls: None,
                                    response: Some(
                                        pb::command_lookup_topic_response::LookupType::Connect
                                            as i32,
                                    ),
                                    request_id: l.request_id,
                                    authoritative: Some(true),
                                    error: None,
                                    message: None,
                                    proxy_through_service_url: Some(false),
                                }),
                                ..Default::default()
                            },
                        );
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        emit(
                            &mut out_buf,
                            &pb::BaseCommand {
                                r#type: pb::base_command::Type::Success as i32,
                                success: Some(pb::CommandSuccess {
                                    request_id: s.request_id,
                                    schema: None,
                                }),
                                ..Default::default()
                            },
                        );
                    }
                }
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() || stream.flush().await.is_err() {
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

struct Broker {
    sessions_handled: Arc<Mutex<u32>>,
}

#[async_trait]
impl Workload for Broker {
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
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            *handled.lock() += 1;
                            let _h = task.spawn_task(
                                "table-view-sim-broker-session",
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

/// What the client workload saw, read back by the `#[test]` body.
#[derive(Debug, Default)]
struct Observation {
    /// `Some("ok" | "err: ..")` once `create()` returned, `Some("panicked: ..")` if it unwound.
    create_outcome: Option<String>,
}

struct TableViewClient {
    obs: Arc<Mutex<Observation>>,
    /// `true` builds the `TableView`; `false` only subscribes a plain consumer, which is the
    /// control: it proves the broker, the client and the subscribe path are sound under the
    /// simulator, so a failure of the first mode is the drain's alone.
    table_view: bool,
    /// `Some` arms `auto_update_partitions_interval` on the `TableView`, which spawns the shared
    /// `AutoUpdateTask` ticker before the drain.
    auto_update: Option<Duration>,
    /// Close a clone of the created view, then the view itself. The clones share one drain and
    /// one ticker, so the second close finds both already shut down.
    close_every_clone: bool,
}

#[async_trait]
impl Workload for TableViewClient {
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

        let connected = time
            .timeout(
                Duration::from_secs(20),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await;
        let Ok(Ok(client)) = connected else {
            return Err(SimulationError::InvalidState(format!(
                "connect_plain failed: {connected:?}"
            )));
        };
        let pulsar: PulsarClient<magnetar::MoonpoolEngine<SimProviders>> =
            PulsarClient::from_runtime_client(client);

        let created = if self.table_view {
            std::panic::AssertUnwindSafe(async {
                let mut builder = pulsar.table_view(TOPIC);
                if let Some(interval) = self.auto_update {
                    builder = builder.auto_update_partitions_interval(interval);
                }
                let view = match builder.create().await {
                    Ok(view) => view,
                    Err(error) => return Err(error),
                };
                if self.close_every_clone {
                    view.clone().close().await;
                    view.close().await;
                }
                Ok(())
            })
            .catch_unwind()
            .await
        } else {
            std::panic::AssertUnwindSafe(async {
                pulsar
                    .consumer(TOPIC)
                    .subscription("table-view-sim-control")
                    .subscribe()
                    .await
                    .map(|_consumer| ())
            })
            .catch_unwind()
            .await
        };
        let outcome = match created {
            Ok(Ok(())) => "ok".to_owned(),
            Ok(Err(error)) => format!("err: {error}"),
            Err(payload) => format!(
                "panicked: {}",
                payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("<non-string panic payload>")
            ),
        };
        self.obs.lock().create_outcome = Some(outcome);
        Ok(())
    }
}

/// Run one seeded simulation of the broker and the client; return the outcome of the call the
/// client made.
fn run_sim(
    table_view: bool,
    auto_update: Option<Duration>,
    close_every_clone: bool,
) -> Option<String> {
    let sessions = Arc::new(Mutex::new(0));
    let obs = Arc::new(Mutex::new(Observation::default()));
    let _report = SimulationBuilder::new()
        .workload(Broker {
            sessions_handled: sessions.clone(),
        })
        .workload(TableViewClient {
            obs: obs.clone(),
            table_view,
            auto_update,
            close_every_clone,
        })
        .set_debug_seeds(vec![1_234_567_890_u64])
        .set_iterations(1)
        .run();

    assert!(
        *sessions.lock() >= 1,
        "the broker must have accepted the client's CONNECT"
    );
    obs.lock().create_outcome.clone()
}

/// `TableViewBuilder::create` over `PulsarClient<MoonpoolEngine<SimProviders>>` completes: its
/// drain task is spawned through the simulator's task provider, not through `tokio::spawn`.
#[test]
fn table_view_create_runs_on_sim_providers_without_a_tokio_runtime() {
    assert_eq!(
        run_sim(true, None, false).as_deref(),
        Some("ok"),
        "TableViewBuilder::create over MoonpoolEngine<SimProviders> must complete; a panic here \
         means the drain task bypasses the provider boundary of ADR-0078"
    );
}

/// The same `create()` with `auto_update_partitions_interval` armed completes: the shared
/// `AutoUpdateTask` ticker (also armed by `PartitionedProducer` and `MultiTopicsConsumer`) is
/// spawned and paced through the simulator's providers, not `tokio::spawn` / `tokio::time`.
#[test]
fn table_view_auto_update_runs_on_sim_providers_without_a_tokio_runtime() {
    assert_eq!(
        run_sim(true, Some(Duration::from_secs(1)), false).as_deref(),
        Some("ok"),
        "TableViewBuilder::create with auto_update_partitions_interval over \
         MoonpoolEngine<SimProviders> must complete; a panic here means the AutoUpdateTask ticker \
         bypasses the provider boundary of ADR-0078"
    );
}

/// Control for the scenario above: the same broker, client and simulator, with a plain consumer
/// subscribe in place of the `TableView`. It passes with or without ADR-0139's fix, so a failure of
/// the other two is the helper task's and not the harness's.
#[test]
fn plain_consumer_subscribe_runs_on_sim_providers() {
    assert_eq!(
        run_sim(false, None, false).as_deref(),
        Some("ok"),
        "a plain consumer subscribe over MoonpoolEngine<SimProviders> must complete"
    );
}

/// Closing every clone of a `TableView` is allowed: the clones share one drain and one ticker,
/// the first `close` stops both and waits for them, and a later `close` of another clone finds
/// them already shut down and returns. The test requires both closes to return; it does not
/// measure how long the second one takes.
#[test]
fn closing_every_clone_of_a_table_view_on_sim_providers_returns() {
    assert_eq!(
        run_sim(true, Some(Duration::from_secs(1)), true).as_deref(),
        Some("ok"),
        "closing a TableView and then a clone of it over MoonpoolEngine<SimProviders> must return"
    );
}
