// SPDX-License-Identifier: Apache-2.0

//! The configured per-consumer ceiling is observable as identical plaintext delivery by both
//! runtime engines, including a payload larger than the unchanged wire-frame limit.

use std::sync::Arc;
use std::time::Duration;

use magnetar::v5::PulsarClientV5;
use magnetar::{MoonpoolEngine, PulsarClient};
use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{BatchLayout, Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::compress::{
    CompressionError, compress, decompress_with_limit, decompress_within_limit,
};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{ConnectionConfig, pb};
use moonpool_core::TokioProviders;

const TOPIC: &str = "persistent://public/default/configured-decompression-limit-partition-0";
const SUBSCRIPTION: &str = "configured-decompression-limit";

fn trace(payload: &[u8]) -> Trace {
    Trace::new(
        TOPIC,
        SUBSCRIPTION,
        vec![
            Op::SendCompressedBatch {
                payloads: vec![payload.to_vec()],
                codec: CompressionKind::Zstd,
                layout: BatchLayout::Java,
            },
            Op::OpenSharedConsumer {
                name: "large-limit".to_owned(),
                receiver_queue_size: 1,
                max_redeliver_count: 0,
                max_uncompressed_message_size: Some(8 * 1024 * 1024),
            },
            Op::RecvShared {
                name: "large-limit".to_owned(),
                timeout: Duration::from_secs(5),
            },
            Op::AckLastReceivedShared {
                name: "large-limit".to_owned(),
            },
            Op::Close,
        ],
    )
}

#[tokio::test(flavor = "current_thread")]
async fn configured_large_payload_event_streams_match_byte_for_byte() {
    let payload = vec![0x6a; 6 * 1024 * 1024];
    let trace = trace(&payload);
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_events = runner_tokio::run(&broker.pulsar_url(), &trace)
        .await
        .expect("tokio trace");
    broker.clear_frame_log();
    let moonpool_events = runner_moonpool::run(&broker.host_port(), &trace)
        .await
        .expect("moonpool trace");

    assert_eq!(tokio_events, moonpool_events);
    assert!(tokio_events.events.iter().any(|event| matches!(
        event,
        Event::Received { payload: received, .. } if received == &payload
    )));
    assert!(tokio_events.events.contains(&Event::Acked));
}

type MoonpoolFacade = PulsarClient<MoonpoolEngine<TokioProviders>>;

async fn publish_compressed_fixture() -> (ScriptedBroker, MoonpoolFacade, Vec<u8>) {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    broker.set_topic_list_response(vec![TOPIC.to_owned()]);
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let runtime_client = magnetar_runtime_moonpool::Client::connect_plain(
        &engine,
        &broker.host_port(),
        ConnectionConfig::default(),
    )
    .await
    .expect("connect Moonpool runtime client");
    let client = MoonpoolFacade::from_runtime_client(runtime_client);
    let payload = vec![0x6a; 6 * 1024 * 1024];
    let compressed = compress(CompressionKind::Zstd, &payload).expect("compress fixture");
    assert!(compressed.len() < magnetar_proto::MAX_FRAME_SIZE);
    let producer = client
        .producer(TOPIC)
        .create()
        .await
        .expect("open compressed fixture producer");
    producer
        .send(OutgoingMessage {
            payload: compressed,
            metadata: pb::MessageMetadata {
                compression: Some(pb::CompressionType::Zstd as i32),
                uncompressed_size: Some(u32::try_from(payload.len()).expect("fixture size")),
                ..Default::default()
            },
            uncompressed_size: u32::try_from(payload.len()).expect("fixture size"),
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        })
        .await
        .expect("publish compressed fixture");
    producer.close().await.expect("close fixture producer");
    (broker, client, payload)
}

#[tokio::test(flavor = "current_thread")]
async fn public_consumer_builders_deliver_configured_compressed_payloads() {
    let (broker, client, payload) = publish_compressed_fixture().await;
    let earliest = pb::command_subscribe::InitialPosition::Earliest;

    let reader = client
        .reader(TOPIC)
        .start_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .create()
        .await
        .expect("create configured reader");
    let message = tokio::time::timeout(Duration::from_secs(5), reader.read_next())
        .await
        .expect("reader delivery did not time out")
        .expect("reader receives configured compressed payload");
    assert_eq!(message.payload.as_ref(), payload.as_slice());

    let typed = client
        .typed_consumer(TOPIC, Arc::new(magnetar_proto::schema::BytesSchema::new()))
        .subscription("configured-typed")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured typed consumer");
    let message = tokio::time::timeout(
        Duration::from_secs(5),
        magnetar::ConsumerApi::receive(typed.inner()),
    )
    .await
    .expect("typed delivery did not time out")
    .expect("typed consumer receives configured compressed payload");
    assert_eq!(message.payload.as_ref(), payload.as_slice());
    typed
        .ack(message.message_id)
        .await
        .expect("ack typed message");
    typed.close().await.expect("close typed consumer");

    let multi = client
        .multi_topics_consumer()
        .topics([TOPIC])
        .subscription("configured-multi")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured multi-topic consumer");
    let message = tokio::time::timeout(Duration::from_secs(5), multi.receive())
        .await
        .expect("multi-topic delivery did not time out")
        .expect("multi-topic consumer receives configured compressed payload");
    assert_eq!(message.topic, TOPIC);
    assert_eq!(message.message.payload.as_ref(), payload.as_slice());
    multi
        .ack(&message.topic, message.message.message_id)
        .await
        .expect("ack multi-topic message");
    multi.close().await.expect("close multi-topic consumer");

    let partitioned = client
        .partitioned_consumer(TOPIC)
        .subscription("configured-partitioned")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured partitioned consumer");
    let message = tokio::time::timeout(Duration::from_secs(5), partitioned.receive())
        .await
        .expect("partitioned delivery did not time out")
        .expect("partitioned consumer receives configured compressed payload");
    assert_eq!(message.message.payload.as_ref(), payload.as_slice());
    partitioned
        .ack(&message.topic, message.message.message_id)
        .await
        .expect("ack partitioned message");
    partitioned
        .close()
        .await
        .expect("close partitioned consumer");

    let pattern = client
        .pattern_consumer()
        .namespace("public/default")
        .pattern("configured-decompression-limit-partition-0$")
        .subscription("configured-pattern")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured pattern consumer");
    let message = tokio::time::timeout(Duration::from_secs(5), pattern.receive())
        .await
        .expect("pattern delivery did not time out")
        .expect("pattern consumer receives configured compressed payload");
    assert_eq!(message.topic, TOPIC);
    assert_eq!(message.message.payload.as_ref(), payload.as_slice());
    pattern
        .ack(&message.topic, message.message.message_id)
        .await
        .expect("ack pattern message");
    pattern.close().await.expect("close pattern consumer");

    client.close().await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_and_typed_builders_keep_the_default_decompression_ceiling() {
    let (broker, client, _payload) = publish_compressed_fixture().await;
    let earliest = pb::command_subscribe::InitialPosition::Earliest;

    let typed = client
        .typed_consumer(TOPIC, Arc::new(magnetar_proto::schema::BytesSchema::new()))
        .subscription("default-typed")
        .initial_position(earliest)
        .subscribe()
        .await
        .expect("subscribe default typed consumer");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        magnetar::ConsumerApi::receive(typed.inner()),
    )
    .await
    .expect("default typed delivery did not time out");
    let error = result.expect_err("typed consumer uses the default 5 MiB ceiling");
    assert!(
        error
            .to_string()
            .contains("configured decompression ceiling 5242880"),
        "unexpected typed decompression error: {error}"
    );
    typed.close().await.expect("close default typed consumer");

    let multi = client
        .multi_topics_consumer()
        .topics([TOPIC])
        .subscription("default-multi")
        .initial_position(earliest)
        .subscribe()
        .await
        .expect("subscribe default multi-topic consumer");
    let result = tokio::time::timeout(Duration::from_secs(5), multi.receive())
        .await
        .expect("default multi-topic delivery did not time out");
    let error = result.expect_err("multi-topic children use the default 5 MiB ceiling");
    assert!(
        error
            .to_string()
            .contains("configured decompression ceiling 5242880"),
        "unexpected multi-topic decompression error: {error}"
    );
    multi
        .close()
        .await
        .expect("close default multi-topic consumer");

    let partitioned = client
        .partitioned_consumer(TOPIC)
        .subscription("default-partitioned")
        .initial_position(earliest)
        .subscribe()
        .await
        .expect("subscribe default partitioned consumer");
    let result = tokio::time::timeout(Duration::from_secs(5), partitioned.receive())
        .await
        .expect("default partitioned delivery did not time out");
    let error = result.expect_err("partitioned child uses the default 5 MiB ceiling");
    assert!(
        error
            .to_string()
            .contains("configured decompression ceiling 5242880"),
        "unexpected partitioned decompression error: {error}"
    );
    partitioned
        .close()
        .await
        .expect("close default partitioned consumer");

    client.close().await;
    broker.shutdown().await;
}

#[test]
fn malformed_legacy_zstd_stream_returns_the_bounded_decode_error() {
    let error = decompress_within_limit(CompressionKind::Zstd, b"not-a-zstd-frame", 1024, 1024)
        .expect_err("malformed legacy zstd stream is rejected");
    assert!(matches!(error, CompressionError::Zstd(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn v5_consumer_builders_deliver_configured_compressed_payloads() {
    let (broker, client, payload) = publish_compressed_fixture().await;
    let client = PulsarClientV5::from_v4(client);
    let earliest = magnetar::v5::V5SubscriptionInitialPosition::Earliest;

    let queue = client
        .queue_consumer(TOPIC)
        .subscription("configured-v5-queue")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured V5 queue consumer");
    let message = tokio::time::timeout(Duration::from_secs(5), queue.receive())
        .await
        .expect("V5 queue delivery did not time out")
        .expect("V5 queue consumer receives configured compressed payload");
    assert_eq!(message.payload.as_ref(), payload.as_slice());
    queue.ack(message.id).await.expect("ack V5 queue message");
    queue
        .into_v4()
        .close()
        .await
        .expect("close V5 queue consumer");

    let stream = client
        .stream_consumer(TOPIC)
        .subscription("configured-v5-stream")
        .initial_position(earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await
        .expect("subscribe configured V5 stream consumer");
    let message = tokio::time::timeout(Duration::from_secs(5), stream.receive())
        .await
        .expect("V5 stream delivery did not time out")
        .expect("V5 stream consumer receives configured compressed payload");
    assert_eq!(message.payload.as_ref(), payload.as_slice());
    stream.ack(message.id).await.expect("ack V5 stream message");
    stream
        .into_v4()
        .close()
        .await
        .expect("close V5 stream consumer");

    client.into_v4().close().await;
    broker.shutdown().await;
}

#[test]
fn decompression_allocation_overflow_is_rejected_before_validation_allocation() {
    let error = decompress_with_limit(
        CompressionKind::None,
        b"unused",
        isize::MAX as usize,
        usize::MAX,
    )
    .expect_err("validation allocation length exceeds the Rust allocation bound");
    assert!(matches!(
        error,
        CompressionError::UncompressedSizeTooLarge { got, ceiling }
            if got == isize::MAX as usize && ceiling == (isize::MAX as usize) - 1
    ));
}
