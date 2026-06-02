// SPDX-License-Identifier: Apache-2.0

//! End-to-end schema round-trip tests against a real Apache Pulsar 4.x standalone broker.
//!
//! Mirrors Apache Pulsar's Java client coverage in
//! `pulsar-broker/src/test/java/org/apache/pulsar/client/api/SimpleSchemaTest.java` and
//! `JsonSchemaTest.java`: a typed producer publishes a few values through a schema, a
//! typed consumer reads them back, and we assert byte-for-byte (or value-for-value)
//! parity for each of the headline schemas.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_schemas -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_proto::schema::{BytesSchema, Int32Schema, JsonSchema, StringSchema};
use schemars::JsonSchema as SchemarsJsonSchema;
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

/// Untyped bytes producer/consumer pair. Mirrors Java `SimpleSchemaTest#testBytesSchema`,
/// which produces and consumes raw `byte[]` payloads via the default `Schema.BYTES`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_bytes_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-bytes";

    let producer = client.producer(topic).create().await?;
    let payloads: &[&[u8]] = &[b"alpha", b"beta", &[0x00, 0xFF, 0x7F, 0x80]];
    for p in payloads {
        producer
            .send(OutgoingMessage::with_payload(p.to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-bytes")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>()
    );
    // Sanity-check: `BytesSchema` advertises identity encoding, so a typed pair would
    // yield the same wire bytes. We assert that here without spinning another broker.
    let schema = BytesSchema::new();
    for p in payloads {
        let encoded =
            magnetar_proto::schema::Schema::encode(&schema, &bytes::Bytes::copy_from_slice(p))?;
        assert_eq!(encoded.as_ref(), *p);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_string_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-string";

    let producer = client
        .typed_producer(topic, Arc::new(StringSchema::new()))
        .create()
        .await?;
    let values: Vec<String> = vec![
        "hello".to_owned(),
        "héllo, wörld".to_owned(),
        "embedded\0nul".to_owned(),
    ];
    for v in &values {
        producer.send(v, None).await?;
    }
    producer.close().await?;

    let consumer = client
        .typed_consumer(topic, Arc::new(StringSchema::new()))
        .subscription("magnetar-e2e-string")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..values.len() {
        let msg = consumer.receive().await?;
        received.push(msg.value.clone());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, values);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemarsJsonSchema)]
struct Person {
    name: String,
    age: u32,
}

/// JSON schema parity with Java `JsonSchemaTest#testJsonSchemaCreate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-json";

    let producer = client
        .typed_producer(topic, Arc::new(JsonSchema::<Person>::new()))
        .create()
        .await?;
    let values = vec![
        Person {
            name: "Ada Lovelace".to_owned(),
            age: 36,
        },
        Person {
            name: "Élise".to_owned(),
            age: 42,
        },
    ];
    for v in &values {
        producer.send(v, None).await?;
    }
    producer.close().await?;

    let consumer = client
        .typed_consumer(topic, Arc::new(JsonSchema::<Person>::new()))
        .subscription("magnetar-e2e-json")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..values.len() {
        let msg = consumer.receive().await?;
        received.push(msg.value.clone());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, values);
    Ok(())
}

/// Int32 schema parity with Java `SimpleSchemaTest` Int schemas. Verifies the broker
/// preserves the big-endian 4-byte wire layout end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_int32_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-int32";

    let producer = client
        .typed_producer(topic, Arc::new(Int32Schema::new()))
        .create()
        .await?;
    let values: Vec<i32> = vec![0, 1, -1, i32::MAX, i32::MIN, 0x0A0B_0C0D];
    for v in &values {
        producer.send(v, None).await?;
    }
    producer.close().await?;

    let consumer = client
        .typed_consumer(topic, Arc::new(Int32Schema::new()))
        .subscription("magnetar-e2e-int32")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received_values = Vec::new();
    let mut received_payloads = Vec::new();
    for _ in 0..values.len() {
        let msg = consumer.receive().await?;
        received_values.push(msg.value);
        received_payloads.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received_values, values);
    // Big-endian 4-byte wire layout: each payload must equal `v.to_be_bytes()`.
    for (got, want) in received_payloads.iter().zip(values.iter()) {
        assert_eq!(got.as_slice(), &want.to_be_bytes());
    }
    Ok(())
}
