// SPDX-License-Identifier: Apache-2.0

//! Isolated performance scenarios. Input is one immutable JSON request file.
//! Native run time excludes payload preparation, warmup and close. External
//! profiles cover the whole process and must retain the phase markers.

use std::error::Error;
use std::hint::black_box;
use std::io::Write;
use std::time::{Duration, Instant, SystemTime};

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{
    BrokerMetadataApi, ConsumerApi, CreateProducerApi, Engine, OutgoingMessage, ProducerApi,
    PulsarClient, SubscribeApi,
};
use serde::{Deserialize, Serialize};

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    scenario_id: String,
    scenario_revision: String,
    runtime: String,
    service_url: String,
    topic: String,
    messages: usize,
    payload_bytes: usize,
    seed: u64,
    batching: bool,
    control_cost_multiplier: usize,
}

#[derive(Debug, Serialize)]
struct Observation {
    schema_version: u32,
    scenario_id: String,
    scenario_revision: String,
    runtime: String,
    transport: &'static str,
    seed: u64,
    requested: usize,
    completed: usize,
    payload_bytes: usize,
    confirmed_payload_bytes: usize,
    denominator: &'static str,
    native_run_ns: u128,
    send_latency_ns: Vec<u128>,
    receive_ack_latency_ns: Vec<u128>,
    phases: Vec<(&'static str, u128)>,
    scope: &'static str,
    batching: bool,
    concurrency: usize,
    run_consumer_present: bool,
    drain_verified_messages: usize,
    terminal_marker_acknowledged: bool,
    terminal_marker_position: Option<String>,
    terminal_marker_receipt: Option<String>,
    last_broker_position: Option<String>,
    broker_message_after_marker: Option<bool>,
    consumer_queue_after_drain: Option<usize>,
    consumer_end_of_topic: Option<bool>,
    producer_pending_after_drain: Option<usize>,
    topic_partition_count: Option<u32>,
}

fn phase(name: &'static str, phases: &mut Vec<(&'static str, u128)>) -> BenchResult<()> {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    eprintln!("PHASE {name} {timestamp}");
    phases.push((name, timestamp));
    Ok(())
}

fn validate_request(request: &Request) -> BenchResult<()> {
    if !matches!(
        request.scenario_id.as_str(),
        "producer"
            | "consumer"
            | "roundtrip"
            | "idle"
            | "control-empty"
            | "control-allocation"
            | "control-syscall"
            | "control-copy"
            | "control-invalid"
    ) {
        return Err("unknown scenario; refusing fixture access".into());
    }
    if !request.scenario_id.starts_with("control-")
        && (!matches!(request.runtime.as_str(), "tokio" | "moonpool")
            || !request.service_url.starts_with("pulsar://"))
    {
        return Err(
            "client scenario requires a known runtime and plain TCP URL; TLS is not covered yet"
                .into(),
        );
    }
    if request.messages == 0
        || request.messages > 100_000
        || !(8..=1024 * 1024).contains(&request.payload_bytes)
    {
        return Err("bounded messages 1..100000 and payload bytes 8..1048576 required".into());
    }
    if request.scenario_revision.is_empty()
        || request.topic.is_empty()
        || request.control_cost_multiplier == 0
        || request.control_cost_multiplier > 10
    {
        return Err("scenario revision/topic and bounded control multiplier required".into());
    }
    if !request.scenario_id.starts_with("control-") && request.control_cost_multiplier != 1 {
        return Err("artificial cost is limited to calibration controls".into());
    }
    Ok(())
}

fn validate_payload(payload: &[u8], corpus: &[Vec<u8>], seen: &mut [bool]) -> BenchResult<()> {
    let encoded: [u8; 8] = payload
        .get(..8)
        .ok_or("truncated message identity")?
        .try_into()?;
    let identity = usize::try_from(u64::from_le_bytes(encoded))?;
    let expected = corpus.get(identity).ok_or("unexpected message identity")?;
    if seen[identity] || payload != expected {
        return Err("duplicate identity or corrupted payload".into());
    }
    seen[identity] = true;
    Ok(())
}

fn validate_drain_message(
    payload: &[u8],
    terminal: &[u8],
    corpus: &[Vec<u8>],
    seen: &mut [bool],
) -> BenchResult<bool> {
    if payload == terminal {
        if !seen.iter().all(|seen| *seen) {
            return Err("terminal marker arrived before complete payload validation".into());
        }
        return Ok(true);
    }
    validate_payload(payload, corpus, seen)?;
    Ok(false)
}

fn marker_position_matches(
    mut receipt: magnetar::proto::MessageId,
    delivered: magnetar::proto::MessageId,
) -> bool {
    // SendReceipt omits size (-1); ConsumerState normalizes non-batched deliveries to 0.
    // Preserve every known position field and only accept that demonstrated representation pair.
    if receipt.batch_index == -1
        && delivered.batch_index == -1
        && receipt.batch_size == -1
        && delivered.batch_size == 0
    {
        receipt.batch_size = 0;
    }
    receipt == delivered
}

fn empty_observation(request: &Request) -> Observation {
    let control = request.scenario_id.starts_with("control-");
    Observation {
        schema_version: 1,
        scenario_id: ToOwned::to_owned(&request.scenario_id),
        scenario_revision: ToOwned::to_owned(&request.scenario_revision),
        runtime: ToOwned::to_owned(&request.runtime),
        transport: if control { "not-applicable" } else { "tcp" },
        seed: request.seed,
        requested: request.messages,
        completed: 0,
        payload_bytes: request.payload_bytes,
        confirmed_payload_bytes: 0,
        denominator: "confirmed-or-received-and-acknowledged-message",
        native_run_ns: 0,
        send_latency_ns: Vec::with_capacity(request.messages),
        receive_ack_latency_ns: Vec::with_capacity(request.messages),
        phases: Vec::with_capacity(5),
        scope: match request.scenario_id.as_str() {
            "producer" => "producer-confirmed-send-and-flush",
            "consumer" => "consumer-subscribe-transfer-and-ack",
            "roundtrip" => "client-send-receive-and-ack",
            "idle" => "idle-client-open-producer-and-consumer",
            _ => "calibration-control-loop",
        },
        batching: request.batching,
        concurrency: 1,
        run_consumer_present: false,
        drain_verified_messages: 0,
        terminal_marker_acknowledged: false,
        terminal_marker_position: None,
        terminal_marker_receipt: None,
        last_broker_position: None,
        broker_message_after_marker: None,
        consumer_queue_after_drain: None,
        consumer_end_of_topic: None,
        producer_pending_after_drain: None,
        topic_partition_count: None,
    }
}

fn control(request: &Request) -> BenchResult<Observation> {
    let mut observation = empty_observation(request);
    observation.runtime = String::from("none");
    observation.denominator = "control-cycle";
    let mut source = vec![0x5a; request.payload_bytes];
    let mut destination = vec![0; request.payload_bytes];
    let mut sink = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
    phase("setup", &mut observation.phases)?;
    phase("warmup", &mut observation.phases)?;
    phase("run", &mut observation.phases)?;
    let start = Instant::now();
    for index in 0..request.messages {
        for _ in 0..request.control_cost_multiplier {
            match request.scenario_id.as_str() {
                "control-empty" => {
                    black_box(index);
                }
                "control-allocation" => {
                    let mut allocation = Vec::<u8>::with_capacity(request.payload_bytes);
                    allocation.resize(request.payload_bytes, 0x69);
                    allocation.reserve_exact(request.payload_bytes);
                    black_box(&allocation);
                }
                "control-syscall" => sink.write_all(black_box(&source[..1]))?,
                "control-copy" => {
                    source[0] = u8::try_from(index % 256)?;
                    destination.copy_from_slice(black_box(&source));
                    if destination != source {
                        return Err("copy control corruption".into());
                    }
                    black_box(&destination);
                }
                "control-invalid" => return Err("intentional invalid calibration outcome".into()),
                _ => return Err("unknown control scenario".into()),
            }
        }
        observation.completed += 1;
    }
    observation.native_run_ns = start.elapsed().as_nanos();
    phase("drain", &mut observation.phases)?;
    drop((source, destination, sink));
    phase("finished", &mut observation.phases)?;
    Ok(observation)
}

struct Workload<E: Engine>
where
    E::ClientState: CreateProducerApi + SubscribeApi,
{
    consumer: Option<<E::ClientState as SubscribeApi>::Consumer>,
    producer: <E::ClientState as CreateProducerApi>::Producer,
    corpus: Vec<Vec<u8>>,
    outgoing: Vec<OutgoingMessage>,
    seen: Vec<bool>,
    subscription: String,
}

async fn subscribe_workload<E>(
    client: &PulsarClient<E>,
    request: &Request,
    subscription: &str,
) -> BenchResult<<E::ClientState as SubscribeApi>::Consumer>
where
    E: Engine,
    E::ClientState: SubscribeApi,
{
    Ok(client
        .consumer(&request.topic)
        .subscription(ToOwned::to_owned(subscription))
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?)
}

async fn setup_workload<E>(
    client: &PulsarClient<E>,
    request: &Request,
    observation: &mut Observation,
) -> BenchResult<Workload<E>>
where
    E: Engine,
    E::ClientState: BrokerMetadataApi + CreateProducerApi + SubscribeApi,
{
    let partitions = client.partitions_for_topic(&request.topic).await?;
    if partitions != 0 {
        return Err("ordered terminal oracle requires one nonpartitioned topic".into());
    }
    observation.topic_partition_count = Some(partitions);
    let corpus: Vec<Vec<u8>> = (0..request.messages)
        .map(|index| {
            let mut payload = vec![
                u8::try_from((request.seed.wrapping_add(index as u64)) % 256)
                    .unwrap_or(0);
                request.payload_bytes
            ];
            payload[..8].copy_from_slice(&(index as u64).to_le_bytes());
            payload
        })
        .collect();
    let outgoing: Vec<_> = corpus
        .iter()
        .map(|payload| OutgoingMessage::with_payload(payload.to_owned()))
        .collect();
    let seen = vec![false; request.messages];
    let mut builder = client.producer(&request.topic);
    if request.batching {
        builder = builder
            .batching(32, 256 * 1024)
            .batching_max_publish_delay(Duration::from_millis(1));
    }
    let producer = builder.create().await?;
    let subscription = format!(
        "{}-subscription",
        request.topic.rsplit('/').next().ok_or("invalid topic")?
    );
    let warm_consumer = subscribe_workload(client, request, &subscription).await?;
    phase("warmup", &mut observation.phases)?;
    ProducerApi::send(
        &producer,
        OutgoingMessage::with_payload(vec![0xa5; request.payload_bytes]),
    )
    .await?;
    let warm = ConsumerApi::receive(&warm_consumer).await?;
    if warm.payload.as_ref() != vec![0xa5; request.payload_bytes] {
        return Err("warmup corruption".into());
    }
    ConsumerApi::ack(&warm_consumer, warm.message_id).await?;
    ConsumerApi::close_owned(warm_consumer).await?;
    if request.scenario_id == "consumer" {
        for message in &outgoing {
            ProducerApi::send(&producer, message.to_owned()).await?;
        }
        ProducerApi::flush(&producer).await?;
    }
    let consumer = if matches!(request.scenario_id.as_str(), "roundtrip" | "idle") {
        Some(subscribe_workload(client, request, &subscription).await?)
    } else {
        None
    };
    Ok(Workload {
        consumer,
        producer,
        corpus,
        outgoing,
        seen,
        subscription,
    })
}

async fn run_workload<E>(
    client: &PulsarClient<E>,
    request: &Request,
    state: &mut Workload<E>,
    observation: &mut Observation,
) -> BenchResult<()>
where
    E: Engine,
    E::ClientState: CreateProducerApi + SubscribeApi,
{
    if request.scenario_id == "consumer" {
        // No consumer exists while preloading; subscribe/dispatch/decode are in the window.
        state.consumer = Some(subscribe_workload(client, request, &state.subscription).await?);
    }
    observation.run_consumer_present = state.consumer.is_some();
    if matches!(request.scenario_id.as_str(), "producer" | "roundtrip") {
        for message in std::mem::take(&mut state.outgoing) {
            let sent = Instant::now();
            ProducerApi::send(&state.producer, message).await?;
            observation.send_latency_ns.push(sent.elapsed().as_nanos());
        }
        ProducerApi::flush(&state.producer).await?;
        if ProducerApi::pending_count(&state.producer) != 0 {
            return Err("producer pending work after flush".into());
        }
        if request.scenario_id == "producer" {
            observation.completed = request.messages;
        }
    }
    if matches!(request.scenario_id.as_str(), "consumer" | "roundtrip") {
        let consumer = state
            .consumer
            .as_ref()
            .ok_or("consumer scenario has no consumer")?;
        for _ in 0..request.messages {
            let received = Instant::now();
            let message = ConsumerApi::receive(consumer).await?;
            validate_payload(&message.payload, &state.corpus, &mut state.seen)?;
            ConsumerApi::ack(consumer, message.message_id).await?;
            observation
                .receive_ack_latency_ns
                .push(received.elapsed().as_nanos());
            observation.completed += 1;
            observation.drain_verified_messages += 1;
        }
    }
    if request.scenario_id == "idle" {
        tokio::time::sleep(Duration::from_millis(100)).await;
        observation.denominator = "idle-cycle";
        observation.requested = 1;
        observation.completed = 1;
    } else if !matches!(
        request.scenario_id.as_str(),
        "producer" | "consumer" | "roundtrip"
    ) {
        return Err("unknown client scenario".into());
    }
    Ok(())
}

async fn drain_workload<E>(
    client: &PulsarClient<E>,
    request: &Request,
    mut state: Workload<E>,
    observation: &mut Observation,
) -> BenchResult<()>
where
    E: Engine,
    E::ClientState: CreateProducerApi + SubscribeApi,
{
    if request.scenario_id != "idle" {
        // One producer/nonpartitioned topic: the full sentinel follows the complete corpus.
        // This bounded suffix proof does not rule out arbitrarily late redelivery.
        let mut terminal = vec![0xff; request.payload_bytes];
        terminal[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        let terminal_id = ProducerApi::send(
            &state.producer,
            OutgoingMessage::with_payload(ToOwned::to_owned(&terminal)),
        )
        .await?;
        ProducerApi::flush(&state.producer).await?;
        if state.consumer.is_none() {
            state.consumer = Some(subscribe_workload(client, request, &state.subscription).await?);
        }
        let consumer = state.consumer.as_ref().ok_or("drain has no consumer")?;
        loop {
            let message = ConsumerApi::receive(consumer).await?;
            let is_terminal = validate_drain_message(
                &message.payload,
                &terminal,
                &state.corpus,
                &mut state.seen,
            )?;
            if is_terminal && !marker_position_matches(terminal_id, message.message_id) {
                return Err(format!(
                    "terminal marker position differs from confirmed send: sent={terminal_id:?}, received={:?}",
                    message.message_id
                ).into());
            }
            ConsumerApi::ack(consumer, message.message_id).await?;
            if is_terminal {
                observation.terminal_marker_acknowledged = true;
                observation.terminal_marker_position = Some(format!("{:?}", message.message_id));
                observation.terminal_marker_receipt = Some(format!("{terminal_id:?}"));
                break;
            }
            observation.drain_verified_messages += 1;
        }
        observation.last_broker_position =
            Some(ConsumerApi::last_message_id(consumer).await?.to_string());
        let after = ConsumerApi::has_message_after(consumer, terminal_id).await?;
        observation.broker_message_after_marker = Some(after);
        if after {
            return Err("broker reports a message after terminal marker".into());
        }
    }
    if let Some(consumer) = state.consumer {
        let queued = ConsumerApi::available_in_queue(&consumer);
        observation.consumer_queue_after_drain = Some(queued);
        observation.consumer_end_of_topic = Some(ConsumerApi::has_reached_end_of_topic(&consumer));
        if queued != 0 {
            return Err("consumer queue nonempty after bounded drain".into());
        }
        ConsumerApi::close_owned(consumer).await?;
    }
    let pending = ProducerApi::pending_count(&state.producer);
    observation.producer_pending_after_drain = Some(pending);
    if pending != 0 {
        return Err("producer pending work after bounded drain".into());
    }
    ProducerApi::close_owned(state.producer).await?;
    Ok(())
}

async fn workload<E>(client: &PulsarClient<E>, request: &Request) -> BenchResult<Observation>
where
    E: Engine,
    E::ClientState: BrokerMetadataApi + CreateProducerApi + SubscribeApi,
{
    let mut observation = empty_observation(request);
    phase("setup", &mut observation.phases)?;
    let mut state = setup_workload(client, request, &mut observation).await?;
    phase("run", &mut observation.phases)?;
    let start = Instant::now();
    run_workload(client, request, &mut state, &mut observation).await?;
    observation.native_run_ns = start.elapsed().as_nanos();
    if request.scenario_id != "idle" {
        observation.confirmed_payload_bytes = observation
            .completed
            .checked_mul(request.payload_bytes)
            .ok_or("payload count overflow")?;
    }
    phase("drain", &mut observation.phases)?;
    drain_workload(client, request, state, &mut observation).await?;
    Ok(observation)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> BenchResult<()> {
    let request_path = std::env::args()
        .nth(1)
        .ok_or("one request JSON path required")?;
    let request: Request = serde_json::from_slice(&std::fs::read(request_path)?)?;
    validate_request(&request)?;
    let mut observation = if request.scenario_id.starts_with("control-") {
        control(&request)?
    } else if request.runtime == "tokio" {
        let client = PulsarClient::builder()
            .service_url(&request.service_url)
            .build()
            .await?;
        let result =
            tokio::time::timeout(Duration::from_secs(120), workload(&client, &request)).await;
        client.close().await;
        result??
    } else {
        #[cfg(feature = "moonpool")]
        {
            if request.runtime != "moonpool" {
                return Err("unknown runtime".into());
            }
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async {
                    let addr = request
                        .service_url
                        .strip_prefix("pulsar://")
                        .ok_or("Moonpool plain TCP URL required")?;
                    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(
                        moonpool_core::TokioProviders::new(),
                    );
                    let runtime_client =
                        magnetar_runtime_moonpool::Client::connect_plain_supervised(
                            &engine,
                            addr,
                            magnetar::proto::ConnectionConfig::default(),
                            None,
                            None,
                        )
                        .await?;
                    let client = PulsarClient::<
                        magnetar::MoonpoolEngine<moonpool_core::TokioProviders>,
                    >::from_runtime_client(runtime_client);
                    let result =
                        tokio::time::timeout(Duration::from_secs(120), workload(&client, &request))
                            .await;
                    client.close().await;
                    result?
                })
                .await?
        }
        #[cfg(not(feature = "moonpool"))]
        {
            return Err("Moonpool feature required for this runtime".into());
        }
    };
    if observation.completed != observation.requested {
        return Err("incomplete scenario work".into());
    }
    if !request.scenario_id.starts_with("control-") {
        phase("finished", &mut observation.phases)?;
    }
    println!("{}", serde_json::to_string(&observation)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_position_only_normalizes_absent_nonbatch_receipt_size() {
        let mut receipt = magnetar::proto::MessageId::EARLIEST;
        receipt.ledger_id = 7;
        receipt.entry_id = 19;
        receipt.batch_size = -1;
        let mut delivered = receipt;
        delivered.batch_size = 0;
        assert!(marker_position_matches(receipt, delivered));
        let mut different = delivered;
        different.ledger_id += 1;
        assert!(!marker_position_matches(receipt, different));
        different = delivered;
        different.entry_id += 1;
        assert!(!marker_position_matches(receipt, different));
        different = delivered;
        different.partition = 0;
        assert!(!marker_position_matches(receipt, different));
        different = delivered;
        different.batch_index = 0;
        assert!(!marker_position_matches(receipt, different));
        different = delivered;
        different.batch_size = 2;
        assert!(!marker_position_matches(receipt, different));
        receipt.batch_index = 0;
        delivered.batch_index = 0;
        assert!(!marker_position_matches(receipt, delivered));
        #[cfg(feature = "scalable-topics")]
        {
            receipt = delivered;
            delivered.segment_id = Some(magnetar::proto::types::SegmentId(3));
            assert!(!marker_position_matches(receipt, delivered));
        }
    }

    #[test]
    fn terminal_oracle_rejects_premature_marker_and_duplicate_suffix() {
        let corpus = vec![vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0]];
        let mut terminal = vec![0xff; 16];
        terminal[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        let mut seen = vec![false; 2];
        assert!(validate_drain_message(&terminal, &terminal, &corpus, &mut seen).is_err());
        assert!(!validate_drain_message(&corpus[0], &terminal, &corpus, &mut seen).expect("first"));
        assert!(
            !validate_drain_message(&corpus[1], &terminal, &corpus, &mut seen).expect("second")
        );
        assert!(validate_drain_message(&corpus[1], &terminal, &corpus, &mut seen).is_err());
        assert!(
            validate_drain_message(&terminal, &terminal, &corpus, &mut seen).expect("terminal")
        );
        let mut corrupt_terminal = ToOwned::to_owned(&terminal);
        corrupt_terminal[8] = 0;
        assert!(validate_drain_message(&corrupt_terminal, &terminal, &corpus, &mut seen).is_err());
    }

    #[test]
    fn calibration_scope_does_not_claim_tcp_client_traffic() {
        let request = Request {
            scenario_id: "control-empty".to_owned(),
            scenario_revision: "test".to_owned(),
            runtime: "none".to_owned(),
            service_url: String::new(),
            topic: "unused".to_owned(),
            messages: 1,
            payload_bytes: 8,
            seed: 1,
            batching: false,
            control_cost_multiplier: 1,
        };
        let observation = empty_observation(&request);
        assert_eq!(observation.transport, "not-applicable");
        assert_eq!(observation.scope, "calibration-control-loop");
    }

    #[test]
    fn oracle_rejects_duplicate_corrupt_and_unexpected_messages() {
        let corpus = vec![vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0]];
        let mut seen = vec![false; 2];
        validate_payload(&corpus[1], &corpus, &mut seen).expect("valid identity");
        assert!(validate_payload(&corpus[1], &corpus, &mut seen).is_err());
        assert!(validate_payload(&[0, 0, 0, 0, 0, 0, 0, 0, 1], &corpus, &mut seen).is_err());
        assert!(validate_payload(&[2, 0, 0, 0, 0, 0, 0, 0], &corpus, &mut seen).is_err());
        assert!(validate_payload(&[0], &corpus, &mut seen).is_err());
        validate_payload(&corpus[0], &corpus, &mut seen).expect("remaining identity");
        assert!(seen.into_iter().all(std::convert::identity));
    }
}
