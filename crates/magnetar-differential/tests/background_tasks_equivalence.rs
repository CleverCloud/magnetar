// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer 4 — the façade's helper tasks run on the client's own providers, on both engines.
//!
//! Three façade surfaces own a helper task: the `TableView` drain, and the auto-update partition
//! ticker of a `TableView`, a `MultiTopicsConsumer` and a `PartitionedProducer`. Each is spawned
//! and paced through the client's `BackgroundApi` (ADR-0078), so it runs on the executor and clock
//! of the provider bundle the client was built over. `table_view_sim_providers.rs` proves the spawn
//! does not reach for an ambient Tokio runtime; this file proves the tasks then *do their work* and
//! *stop*:
//!
//! * the drain applies a keyed message the broker pushes, so a `TableView` is populated;
//! * every ticker signals `partitions_changed_notify()` once its interval has elapsed;
//! * `TableView::close` waits for both tasks to end and no tick follows it; and
//! * dropping a surface stops its ticker.
//!
//! The one scenario ([`observe`]) is generic over the engine and runs twice: on
//! `PulsarClient<TokioEngine>` against a loopback broker (real clock), and on
//! `PulsarClient<MoonpoolEngine<SimProviders>>` inside a seeded simulation (virtual clock, no Tokio
//! runtime at all). Both must produce the same [`Trace`], which is also the expected value.
//!
//! The Tokio-only builder paths (`TableViewBuilder::create_with_decryption`,
//! `PartitionedProducerBuilder::create_with_encryption`) spawn the same tasks from the Tokio
//! client; they have no Moonpool twin, because the Moonpool engine has no such builder.
//! No live Pulsar is needed.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::future::Either;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar::{
    BrokerMetadataApi, CreateProducerApi, Engine, MoonpoolEngine, PulsarClient, SubscribeApi,
    TokioEngine,
};
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    ConnectionConfig, FrameError, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::Client as MoonpoolClient;
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const BROKER_PORT: u16 = 6650;
const VIEW_TOPIC: &str = "persistent://public/default/background-tasks-view";
const MULTI_TOPIC_A: &str = "persistent://public/default/background-tasks-a";
const MULTI_TOPIC_B: &str = "persistent://public/default/background-tasks-b";
const PRODUCER_TOPIC: &str = "persistent://public/default/background-tasks-producer";
const KEY: &str = "k";
const VALUE: &[u8] = b"v";

/// The interval every ticker is armed with, on the engine's own clock.
const TICK: Duration = Duration::from_millis(100);

/// An engine-specific sleep: tokio's on the Tokio client, the simulator's on the Moonpool one.
type Sleeper = Arc<dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What the surfaces did, as the user sees it. Identical on both engines.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Trace {
    /// The value the drain materialised for [`KEY`].
    drained_value: Option<Vec<u8>>,
    /// For each observation, in order, whether a ticker signalled within the wait.
    ticks: Vec<(&'static str, bool)>,
}

fn expected_trace() -> Trace {
    Trace {
        drained_value: Some(VALUE.to_vec()),
        ticks: vec![
            ("table view, open", true),
            ("table view, after close", false),
            ("multi topics consumer, open", true),
            ("multi topics consumer, after drop", false),
            ("partitioned producer, open", true),
            ("partitioned producer, after drop", false),
        ],
    }
}

fn command(ty: pb::base_command::Type) -> pb::BaseCommand {
    pb::BaseCommand {
        r#type: ty as i32,
        ..Default::default()
    }
}

/// The broker, as a pure function of the frames it reads: `CONNECT`, `PING`, `LOOKUP`,
/// `SUBSCRIBE`, `PARTITIONED_METADATA` (never partitioned) and `PRODUCER` are answered, and the
/// first `FLOW` of a consumer on [`VIEW_TOPIC`] is answered with one keyed message. Everything
/// else is ignored.
#[derive(Default)]
struct Script {
    /// Topic of every consumer id the client subscribed.
    consumers: HashMap<u64, String>,
    pushed: bool,
}

impl Script {
    /// Consume every complete frame in `read_buf`, appending the answers to `out`. `false` when the
    /// stream is corrupt and the session should end.
    fn feed(&mut self, read_buf: &mut BytesMut, out: &mut BytesMut) -> bool {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => return true,
                Err(_) => return false,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            self.answer(kind, &frame.command, out);
        }
    }

    fn answer(&mut self, kind: pb::base_command::Type, cmd: &pb::BaseCommand, out: &mut BytesMut) {
        use pb::base_command::Type;
        match kind {
            Type::Connect => connected(out),
            Type::Ping => emit(
                out,
                &pb::BaseCommand {
                    pong: Some(pb::CommandPong {}),
                    ..command(Type::Pong)
                },
            ),
            Type::Lookup => {
                if let Some(l) = &cmd.lookup_topic {
                    looked_up(l.request_id, out);
                }
            }
            Type::PartitionedMetadata => {
                if let Some(m) = &cmd.partition_metadata {
                    not_partitioned(m.request_id, out);
                }
            }
            Type::Producer => {
                if let Some(p) = &cmd.producer {
                    producer_opened(p.request_id, out);
                }
            }
            Type::Subscribe => {
                if let Some(s) = &cmd.subscribe {
                    self.consumers.insert(s.consumer_id, s.topic.clone());
                    subscribed(s.request_id, out);
                }
            }
            Type::Flow => {
                if let Some(f) = &cmd.flow
                    && !self.pushed
                    && self.consumers.get(&f.consumer_id).map(String::as_str) == Some(VIEW_TOPIC)
                {
                    self.pushed = true;
                    push_keyed_message(out, f.consumer_id);
                }
            }
            _ => {}
        }
    }
}

fn emit(out: &mut BytesMut, cmd: &pb::BaseCommand) {
    let _ = encode_command(out, cmd);
}

fn connected(out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-background-tasks".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..command(pb::base_command::Type::Connected)
        },
    );
}

fn looked_up(request_id: u64, out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
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
            ..command(pb::base_command::Type::LookupResponse)
        },
    );
}

fn not_partitioned(request_id: u64, out: &mut BytesMut) {
    use pb::command_partitioned_topic_metadata_response::LookupType;
    emit(
        out,
        &pb::BaseCommand {
            partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
                partitions: Some(0),
                request_id,
                response: Some(LookupType::Success as i32),
                error: None,
                message: None,
            }),
            ..command(pb::base_command::Type::PartitionedMetadataResponse)
        },
    );
}

fn producer_opened(request_id: u64, out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "background-tasks-producer".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..command(pb::base_command::Type::ProducerSuccess)
        },
    );
}

fn subscribed(request_id: u64, out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..command(pb::base_command::Type::Success)
        },
    );
}

fn push_keyed_message(out: &mut BytesMut, consumer_id: u64) {
    let msg = pb::BaseCommand {
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id: 0,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..command(pb::base_command::Type::Message)
    };
    let metadata = pb::MessageMetadata {
        producer_name: "background-tasks-prod".to_owned(),
        sequence_id: 0,
        publish_time: 0,
        partition_key: Some(KEY.to_owned()),
        ..Default::default()
    };
    let _ = encode_payload(out, &msg, &metadata, VALUE);
}

/// How long a ticker may take to fire the first time. Generous: a positive check returns at the
/// first tick, so this only bounds a failure.
const PATIENCE: Duration = Duration::from_secs(5);

/// How long to listen for a tick that must not come. Several intervals, so a ticker that was
/// still running would have fired.
const QUIET: Duration = Duration::from_millis(300);

/// Wait up to `within` for `notify` to fire, on the engine's own clock. `true` when it fired.
async fn ticked(notify: &Arc<Notify>, sleep: &Sleeper, within: Duration) -> bool {
    let notified = Box::pin(notify.notified());
    matches!(
        futures::future::select(notified, sleep(within)).await,
        Either::Left(_)
    )
}

/// Run every surface once on `client`, on the engine's own clock `sleep`.
async fn observe<E>(client: &PulsarClient<E>, sleep: Sleeper) -> Trace
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi + CreateProducerApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
    <E::ClientState as CreateProducerApi>::Producer: std::fmt::Debug,
{
    let mut ticks = Vec::new();

    // TableView: the drain materialises the pushed message, the ticker signals, and `close` ends
    // both tasks.
    let view = client
        .table_view(VIEW_TOPIC)
        .auto_update_partitions_interval(TICK)
        .create()
        .await
        .expect("table view create");
    let mut drained_value = None;
    for _ in 0..500 {
        if let Some(value) = view.get(KEY) {
            drained_value = Some(value.to_vec());
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let view_notify = view.partitions_changed_notify().expect("watcher armed");
    ticks.push((
        "table view, open",
        ticked(&view_notify, &sleep, PATIENCE).await,
    ));
    view.close().await;
    ticks.push((
        "table view, after close",
        ticked(&view_notify, &sleep, QUIET).await,
    ));

    // MultiTopicsConsumer: the ticker signals, and stops when the consumer is dropped.
    let multi = client
        .multi_topics_consumer()
        .topic(MULTI_TOPIC_A)
        .topic(MULTI_TOPIC_B)
        .subscription("background-tasks")
        .auto_update_partitions_interval(TICK)
        .subscribe()
        .await
        .expect("multi topics subscribe");
    let multi_notify = multi.partitions_changed_notify().expect("watcher armed");
    ticks.push((
        "multi topics consumer, open",
        ticked(&multi_notify, &sleep, PATIENCE).await,
    ));
    drop(multi);
    ticks.push((
        "multi topics consumer, after drop",
        ticked(&multi_notify, &sleep, QUIET).await,
    ));

    // PartitionedProducer: same.
    let producer = client
        .partitioned_producer(PRODUCER_TOPIC)
        .auto_update_partitions_interval(TICK)
        .create()
        .await
        .expect("partitioned producer create");
    let producer_notify = producer.partitions_changed_notify().expect("watcher armed");
    // The producer's `Debug` output shows its armed ticker (it formats the task too).
    assert!(
        format!("{producer:?}").contains("auto_update: Some("),
        "the producer's Debug output must show its auto-update task"
    );
    ticks.push((
        "partitioned producer, open",
        ticked(&producer_notify, &sleep, PATIENCE).await,
    ));
    drop(producer);
    ticks.push((
        "partitioned producer, after drop",
        ticked(&producer_notify, &sleep, QUIET).await,
    ));

    Trace {
        drained_value,
        ticks,
    }
}

// --- Tokio leg: loopback broker, real clock -------------------------------------------------

async fn serve_tokio(listener: TcpListener) {
    while let Ok((mut stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let mut script = Script::default();
            let mut read_buf = BytesMut::with_capacity(64 * 1024);
            let mut out = BytesMut::with_capacity(64 * 1024);
            loop {
                if !script.feed(&mut read_buf, &mut out) {
                    return;
                }
                if !out.is_empty() {
                    if stream.write_all(&out).await.is_err() {
                        return;
                    }
                    out.clear();
                }
                match stream.read_buf(&mut read_buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        });
    }
}

async fn tokio_client() -> PulsarClient<TokioEngine> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(serve_tokio(listener));
    tokio::time::timeout(
        HANG_GUARD,
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok")
}

fn tokio_sleeper() -> Sleeper {
    Arc::new(|duration| Box::pin(tokio::time::sleep(duration)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn helper_tasks_run_and_stop_on_the_tokio_engine() {
    let client = tokio_client().await;
    let trace = tokio::time::timeout(HANG_GUARD, observe(&client, tokio_sleeper()))
        .await
        .expect("the scenario did not hang");
    assert_eq!(
        trace,
        expected_trace(),
        "the Tokio engine's helper tasks must drain, tick, and stop"
    );
}

/// The Tokio-only builders spawn the same tasks from the Tokio client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_only_builders_spawn_their_helper_tasks() {
    let client = tokio_client().await;
    let sleep = tokio_sleeper();

    let view = client
        .table_view(VIEW_TOPIC)
        .auto_update_partitions_interval(TICK)
        .create_with_decryption()
        .await
        .expect("table view create_with_decryption");
    let view_notify = view.partitions_changed_notify().expect("watcher armed");
    assert!(ticked(&view_notify, &sleep, PATIENCE).await);
    let mut drained = None;
    for _ in 0..500 {
        if let Some(value) = view.get(KEY) {
            drained = Some(value.to_vec());
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(drained.as_deref(), Some(VALUE));
    view.close().await;
    assert!(!ticked(&view_notify, &sleep, QUIET).await);

    let producer = client
        .partitioned_producer(PRODUCER_TOPIC)
        .auto_update_partitions_interval(TICK)
        .create_with_encryption()
        .await
        .expect("partitioned producer create_with_encryption");
    let producer_notify = producer.partitions_changed_notify().expect("watcher armed");
    assert!(ticked(&producer_notify, &sleep, PATIENCE).await);
    drop(producer);
    assert!(!ticked(&producer_notify, &sleep, QUIET).await);
}

// --- Moonpool leg: seeded simulation, virtual clock, no Tokio runtime -------------------------

async fn serve_sim<S>(mut stream: S) -> SimulationResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut script = Script::default();
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out = BytesMut::with_capacity(64 * 1024);
    loop {
        if !script.feed(&mut read_buf, &mut out) {
            return Ok(());
        }
        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() || stream.flush().await.is_err() {
                return Ok(());
            }
            out.clear();
        }
        let mut tmp = vec![0u8; 64 * 1024];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => read_buf.extend_from_slice(&tmp[..n]),
        }
    }
}

struct Broker;

#[async_trait]
impl Workload for Broker {
    fn name(&self) -> &str {
        "broker"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let listener = network
            .bind(&format!("{}:{BROKER_PORT}", ctx.my_ip()))
            .await
            .map_err(|e| SimulationError::InvalidState(format!("broker bind: {e}")))?;
        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                inbound = listener.accept() => {
                    match inbound {
                        Ok((stream, _peer)) => {
                            let _h = task.spawn_task("background-tasks-broker-session", async move {
                                let _ = serve_sim(stream).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

struct SimClient {
    trace: Arc<Mutex<Option<Trace>>>,
}

#[async_trait]
impl Workload for SimClient {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{BROKER_PORT}");
        let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(ctx.providers().clone());
        let time = ctx.providers().time().clone();

        let connected = time
            .timeout(
                Duration::from_secs(20),
                MoonpoolClient::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await;
        let Ok(Ok(client)) = connected else {
            return Err(SimulationError::InvalidState(format!(
                "connect_plain failed: {connected:?}"
            )));
        };
        let pulsar: PulsarClient<MoonpoolEngine<SimProviders>> =
            PulsarClient::from_runtime_client(client);
        let sleep: Sleeper = Arc::new(move |duration| {
            let time = time.clone();
            Box::pin(async move {
                let _ = time.sleep(duration).await;
            })
        });
        *self.trace.lock() = Some(observe(&pulsar, sleep).await);
        Ok(())
    }
}

#[test]
fn helper_tasks_run_and_stop_on_the_simulated_moonpool_engine() {
    let trace = Arc::new(Mutex::new(None));
    let _report = SimulationBuilder::new()
        .workload(Broker)
        .workload(SimClient {
            trace: trace.clone(),
        })
        .set_debug_seeds(vec![1_234_567_890_u64])
        .set_iterations(1)
        .run();
    assert_eq!(
        trace.lock().clone(),
        Some(expected_trace()),
        "the simulated Moonpool engine's helper tasks must drain, tick on virtual time, and stop"
    );
}
