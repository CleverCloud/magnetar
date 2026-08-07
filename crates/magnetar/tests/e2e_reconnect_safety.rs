// SPDX-License-Identifier: Apache-2.0

//! Real Pulsar regressions for reconnect safety issues #395, #396, and #398.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use futures_util::future::join_all;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use magnetar_proto::{FrameError, decode_one, pb};
use magnetar_runtime_tokio::ClientError;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use uuid::Uuid;

const PULSAR_IMAGE: &str = "apachepulsar/pulsar";
const PULSAR_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Observations {
    subscribe_start_ids: Vec<Option<pb::MessageIdData>>,
    ack_sets: Vec<Vec<i64>>,
}

struct ReconnectGate {
    url: String,
    cut: Arc<Notify>,
    drop_next_receipt: Arc<AtomicBool>,
    sessions: Arc<AtomicUsize>,
    observations: Arc<Mutex<Observations>>,
    changed: Arc<Notify>,
}

impl ReconnectGate {
    async fn spawn(upstream: String) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let cut = Arc::new(Notify::new());
        let drop_next_receipt = Arc::new(AtomicBool::new(false));
        let sessions = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(Mutex::new(Observations::default()));
        let changed = Arc::new(Notify::new());

        let task_cut = cut.clone();
        let task_drop = drop_next_receipt.clone();
        let task_sessions = sessions.clone();
        let task_observations = observations.clone();
        let task_changed = changed.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(broker) = TcpStream::connect(&upstream).await else {
                    drop(client);
                    continue;
                };
                task_sessions.fetch_add(1, Ordering::SeqCst);
                task_changed.notify_waiters();
                let session_cut = task_cut.clone();
                let session_drop = task_drop.clone();
                let session_observations = task_observations.clone();
                let session_changed = task_changed.clone();
                tokio::spawn(async move {
                    let (client_read, client_write) = client.into_split();
                    let (broker_read, broker_write) = broker.into_split();
                    tokio::select! {
                        _ = relay_frames(
                            client_read,
                            broker_write,
                            Direction::ClientToBroker,
                            session_drop.clone(),
                            session_observations.clone(),
                            session_changed.clone(),
                        ) => {}
                        _ = relay_frames(
                            broker_read,
                            client_write,
                            Direction::BrokerToClient,
                            session_drop,
                            session_observations,
                            session_changed,
                        ) => {}
                        () = session_cut.notified() => {}
                    }
                });
            }
        });

        Ok(Self {
            url: format!("pulsar://{address}"),
            cut,
            drop_next_receipt,
            sessions,
            observations,
            changed,
        })
    }

    fn cut_current(&self) {
        self.cut.notify_waiters();
    }

    fn drop_next_receipt_and_cut(&self) {
        self.drop_next_receipt.store(true, Ordering::SeqCst);
    }

    async fn wait_for_sessions(&self, count: usize) {
        tokio::time::timeout(WAIT, async {
            while self.sessions.load(Ordering::SeqCst) < count {
                self.changed.notified().await;
            }
        })
        .await
        .expect("client did not establish the expected proxy session count");
    }

    async fn wait_for_subscribes(&self, count: usize) -> Vec<Option<pb::MessageIdData>> {
        tokio::time::timeout(WAIT, async {
            loop {
                let current = self
                    .observations
                    .lock()
                    .expect("observation lock")
                    .subscribe_start_ids
                    .clone();
                if current.len() >= count {
                    return current;
                }
                self.changed.notified().await;
            }
        })
        .await
        .expect("expected CommandSubscribe was not observed")
    }

    async fn wait_for_acks(&self, count: usize) -> Vec<Vec<i64>> {
        tokio::time::timeout(WAIT, async {
            loop {
                let current = self
                    .observations
                    .lock()
                    .expect("observation lock")
                    .ack_sets
                    .clone();
                if current.len() >= count {
                    return current;
                }
                self.changed.notified().await;
            }
        })
        .await
        .expect("expected CommandAck was not observed")
    }
}

#[derive(Clone, Copy)]
enum Direction {
    ClientToBroker,
    BrokerToClient,
}

async fn relay_frames<R, W>(
    mut reader: R,
    mut writer: W,
    direction: Direction,
    drop_next_receipt: Arc<AtomicBool>,
    observations: Arc<Mutex<Observations>>,
    changed: Arc<Notify>,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffered = BytesMut::with_capacity(64 * 1024);
    loop {
        let read = reader.read_buf(&mut buffered).await?;
        if read == 0 {
            return Ok(());
        }
        loop {
            let mut candidate = buffered.clone().freeze();
            let before = candidate.len();
            let frame = match decode_one(&mut candidate) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - candidate.len();
            let raw = buffered.split_to(consumed);
            match direction {
                Direction::ClientToBroker => {
                    let mut observed = false;
                    if let Some(subscribe) = frame.command.subscribe {
                        observations
                            .lock()
                            .expect("observation lock")
                            .subscribe_start_ids
                            .push(subscribe.start_message_id);
                        observed = true;
                    }
                    if let Some(ack) = frame.command.ack {
                        let mut state = observations.lock().expect("observation lock");
                        for message_id in ack.message_id {
                            state.ack_sets.push(message_id.ack_set);
                        }
                        observed = true;
                    }
                    if observed {
                        changed.notify_waiters();
                    }
                }
                Direction::BrokerToClient => {
                    if frame.command.send_receipt.is_some()
                        && drop_next_receipt.swap(false, Ordering::SeqCst)
                    {
                        return Ok(());
                    }
                }
            }
            writer.write_all(&raw).await?;
            writer.flush().await?;
        }
    }
}

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    let container = GenericImage::new(PULSAR_IMAGE, PULSAR_TAG)
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_mins(3))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
        .with_env_var(
            "PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled",
            "true",
        )
        .with_cmd(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ conf/standalone.conf && \
             bin/pulsar standalone"
                .to_owned(),
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    Ok((format!("{host}:{port}"), container))
}

fn supervisor() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
        mandatory_stop: Duration::from_secs(30),
        max_attempts: Some(100),
        ..SupervisorConfig::default()
    }
}

fn assert_batch_reset_errors(results: Vec<Result<magnetar::MessageId, ClientError>>) {
    for result in results {
        match result {
            Err(ClientError::SendRejected { code, message }) => {
                assert_eq!(code, -1);
                assert_eq!(
                    message,
                    "batched send cannot be replayed after connection reset"
                );
            }
            other => panic!("expected a bounded batch reset error, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_batched_send_futures_resolve_for_every_reset_phase()
-> Result<(), Box<dyn std::error::Error>> {
    let (upstream, _container) = start_pulsar().await?;
    let gate = ReconnectGate::spawn(upstream).await?;
    let client = PulsarClient::builder()
        .service_url(&gate.url)
        .enable_reconnect(supervisor())
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-batch-reset-{}",
        Uuid::new_v4()
    );
    let producer = client
        .producer(&topic)
        .batching(100, 1024 * 1024)
        .batching_max_publish_delay(Duration::from_secs(30))
        .create()
        .await?;
    // A successful non-batched receipt is the readiness barrier after each accepted proxy
    // session. `wait_for_sessions` alone observes TCP accept before the reconnect handshake and
    // producer rebuild have necessarily completed.
    let readiness_probe = client.producer(&topic).create().await?;

    // Before flush: both eager SendFuts are present in the batch container when the socket drops.
    let preflush: Vec<_> = [b"pre-a".as_slice(), b"pre-b".as_slice()]
        .into_iter()
        .map(|payload| producer.send(OutgoingMessage::with_payload(payload.to_vec()).into()))
        .collect();
    gate.cut_current();
    let preflush_results = tokio::time::timeout(WAIT, join_all(preflush)).await?;
    assert_batch_reset_errors(preflush_results);
    gate.wait_for_sessions(2).await;
    tokio::time::timeout(
        WAIT,
        readiness_probe
            .send(OutgoingMessage::with_payload(b"ready-after-preflush".to_vec()).into()),
    )
    .await??;

    // After flush, before receipt: suppress the ranged receipt and close that session.
    let flushed: Vec<_> = [b"flush-a".as_slice(), b"flush-b".as_slice()]
        .into_iter()
        .map(|payload| producer.send(OutgoingMessage::with_payload(payload.to_vec()).into()))
        .collect();
    gate.drop_next_receipt_and_cut();
    let (flush_result, flushed_results) = tokio::time::timeout(WAIT, async {
        tokio::join!(producer.flush(), join_all(flushed))
    })
    .await?;
    flush_result?;
    assert_batch_reset_errors(flushed_results);
    gate.wait_for_sessions(3).await;
    tokio::time::timeout(
        WAIT,
        readiness_probe.send(OutgoingMessage::with_payload(b"ready-after-flush".to_vec()).into()),
    )
    .await??;

    // After receipt: every SendFut succeeds before the cut and remains resolved across it.
    let control: Vec<_> = [b"ok-a".as_slice(), b"ok-b".as_slice()]
        .into_iter()
        .map(|payload| producer.send(OutgoingMessage::with_payload(payload.to_vec()).into()))
        .collect();
    let (flush_result, control_results) = tokio::time::timeout(WAIT, async {
        tokio::join!(producer.flush(), join_all(control))
    })
    .await?;
    flush_result?;
    for result in control_results {
        result?;
    }
    gate.cut_current();
    gate.wait_for_sessions(4).await;

    // A receipt on the fourth session proves the cut happened after the ranged receipt was
    // applied rather than merely leaving the previous successful futures undisturbed.
    tokio::time::timeout(
        WAIT,
        readiness_probe.send(OutgoingMessage::with_payload(b"ready-after-receipt".to_vec()).into()),
    )
    .await??;

    readiness_probe.close().await?;
    producer.close().await?;
    client.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// One linear proxy timeline makes the durable and non-durable reattach frames unambiguous.
#[allow(clippy::too_many_lines)]
async fn e2e_partial_batch_ack_and_durable_cursor_survive_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let (upstream, _container) = start_pulsar().await?;
    let gate = ReconnectGate::spawn(upstream).await?;
    let client = PulsarClient::builder()
        .service_url(&gate.url)
        .enable_reconnect(supervisor())
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-ack-reset-{}",
        Uuid::new_v4()
    );
    let subscription = format!("magnetar-e2e-ack-reset-{}", Uuid::new_v4());
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .start_message_id(magnetar::proto::MessageId::EARLIEST)
        .subscribe()
        .await?;
    let producer = client
        .producer(&topic)
        .batching(4, 1024 * 1024)
        .batching_max_publish_delay(Duration::from_secs(30))
        .create()
        .await?;

    let sends: Vec<_> = (0..4)
        .map(|index| {
            producer
                .send(OutgoingMessage::with_payload(format!("batch-{index}").into_bytes()).into())
        })
        .collect();
    for result in join_all(sends).await {
        result?;
    }
    let mut original = Vec::new();
    for _ in 0..4 {
        original.push(tokio::time::timeout(WAIT, consumer.receive()).await??);
    }

    gate.cut_current();
    gate.wait_for_sessions(2).await;
    let subscribes = gate.wait_for_subscribes(2).await;
    let initial = subscribes[0]
        .as_ref()
        .expect("the fresh durable subscribe must retain its explicit start_message_id");
    assert_eq!((initial.ledger_id, initial.entry_id), (u64::MAX, u64::MAX));
    assert!(
        subscribes[1].is_none(),
        "the durable reattach must omit the local start_message_id (#398)"
    );

    // Drain the broker's reconnect redelivery before exercising a stale pre-reset MessageId.
    for _ in 0..4 {
        let _ = tokio::time::timeout(WAIT, consumer.receive()).await??;
    }
    let ack_count = gate
        .observations
        .lock()
        .expect("observation lock")
        .ack_sets
        .len();
    consumer.ack(original[0].message_id).await?;
    let ack_sets = gate.wait_for_acks(ack_count + 1).await;
    let stale_id = original[0].message_id;
    let all_unacked = (1_u64 << stale_id.batch_size) - 1;
    let expected_ack_set = vec![(all_unacked & !(1_u64 << stale_id.batch_index)) as i64];
    assert_eq!(
        ack_sets[ack_count], expected_ack_set,
        "a stale individual batch ack must clear exactly its requested index (#396)"
    );

    consumer.negative_ack(original[1].message_id);
    let redelivered = tokio::time::timeout(WAIT, consumer.receive()).await??;
    assert_eq!(
        redelivered.message_id.ledger_id,
        original[1].message_id.ledger_id
    );
    assert_eq!(
        redelivered.message_id.entry_id,
        original[1].message_id.entry_id
    );

    consumer.close().await?;
    let non_durable_subscribe_index = gate
        .observations
        .lock()
        .expect("observation lock")
        .subscribe_start_ids
        .len();
    let non_durable = client
        .consumer(&topic)
        .subscription(format!("magnetar-e2e-non-durable-{}", Uuid::new_v4()))
        .subscription_type(SubType::Exclusive)
        .durable(false)
        .start_message_id(magnetar::proto::MessageId::EARLIEST)
        .subscribe()
        .await?;
    let subscribes = gate
        .wait_for_subscribes(non_durable_subscribe_index + 1)
        .await;
    let non_durable_initial = subscribes[non_durable_subscribe_index]
        .as_ref()
        .expect("the fresh non-durable subscribe must retain its explicit start_message_id");
    assert_eq!(
        (non_durable_initial.ledger_id, non_durable_initial.entry_id),
        (u64::MAX, u64::MAX)
    );

    let plain_producer = client.producer(&topic).create().await?;
    plain_producer
        .send(OutgoingMessage::with_payload(b"non-durable".to_vec()).into())
        .await?;
    let non_durable_message = tokio::time::timeout(WAIT, non_durable.receive()).await??;
    non_durable.ack(non_durable_message.message_id).await?;

    gate.cut_current();
    gate.wait_for_sessions(3).await;
    let subscribes = gate
        .wait_for_subscribes(non_durable_subscribe_index + 2)
        .await;
    let resumed = subscribes[non_durable_subscribe_index + 1]
        .as_ref()
        .expect("a non-durable reattach must retain its client-side resume watermark");
    assert_eq!(resumed.ledger_id, non_durable_message.message_id.ledger_id);
    assert_eq!(resumed.entry_id, non_durable_message.message_id.entry_id);

    non_durable.close().await?;
    plain_producer.close().await?;
    producer.close().await?;
    client.close().await;
    Ok(())
}
