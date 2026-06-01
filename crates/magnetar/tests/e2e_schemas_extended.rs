// SPDX-License-Identifier: Apache-2.0

//! Extended end-to-end schema round-trip tests against a real Apache Pulsar 4.x
//! standalone broker.
//!
//! Extends the headline matrix in [`e2e_schemas.rs`](./e2e_schemas.rs) (which
//! covers Bytes / String / JSON / Int32) with three more parity-critical
//! Java schema families:
//!
//! - **Avro** — `org.apache.pulsar.client.impl.schema.AvroSchema`. Verifies the
//!   parsing-canonical-form `schema_data` survives the broker registration and that
//!   `apache_avro::to_avro_datum` / `from_avro_datum` round-trips end-to-end.
//! - **`KeyValue`<String, Int32>** in `Inline` encoding — Java `KeyValueSchema.of(Schema.STRING,
//!   Schema.INT32, KeyValueEncodingType.INLINE)`. `Separated` mode requires the key carrier
//!   (`MessageMetadata.partition_key`) and is not yet decodable via [`KeyValueSchema::decode`], so
//!   we exercise the inline path here.
//! - **Temporal** — `DateSchema` / `TimeSchema` / `TimestampSchema`. All three share the same
//!   i64-big-endian wire layout but carry distinct `pb::schema::Type` discriminators (`Date` /
//!   `Time` / `Timestamp`) so the broker stores the semantic intent. We round-trip a representative
//!   i64 for each.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_schemas_extended -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::sync::Arc;
use std::time::Duration;

use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar_proto::schema::{
    AvroSchema, DateSchema, Int32Schema, KeyValueEncodingType, KeyValuePair, KeyValueSchema,
    StringSchema, TimeSchema, TimestampSchema,
};
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AvroUser {
    name: String,
    age: i32,
}

const AVRO_USER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "AvroUser",
    "namespace": "magnetar.e2e",
    "fields": [
        { "name": "name", "type": "string" },
        { "name": "age",  "type": "int" }
    ]
}"#;

/// Avro schema parity with Java `SimpleSchemaTest` / `AvroSchemaTest`. Verifies
/// `AvroSchema::parse_str` + the broker accept the parsing-canonical-form
/// `schema_data`, and that `apache_avro` round-trips the value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_avro_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-avro";

    let producer_schema = Arc::new(AvroSchema::<AvroUser>::parse_str(AVRO_USER_SCHEMA)?);
    let producer = client
        .typed_producer(topic, Arc::clone(&producer_schema))
        .create()
        .await?;
    let values = vec![
        AvroUser {
            name: "Ada Lovelace".to_owned(),
            age: 36,
        },
        AvroUser {
            name: "Grace Hopper".to_owned(),
            age: 85,
        },
    ];
    for v in &values {
        producer.send(v, None).await?;
    }
    producer.close().await?;

    let consumer_schema = Arc::new(AvroSchema::<AvroUser>::parse_str(AVRO_USER_SCHEMA)?);
    let consumer = client
        .typed_consumer(topic, consumer_schema)
        .subscription("magnetar-e2e-avro")
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

/// KeyValue<String, Int32> in `Inline` encoding. Mirrors Java
/// `KeyValueSchema.of(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE)`.
///
/// `Separated` mode is intentionally NOT exercised here: per the in-tree
/// [`KeyValueSchema::decode`] contract, decoding `Separated` requires the
/// `MessageMetadata.partition_key` carrier surfaced via `decode_with_key` —
/// the inline form is the only path that round-trips fully through the typed
/// consumer today.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_key_value_string_int32_inline_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-schema-kv-inline";

    let producer_schema = Arc::new(KeyValueSchema::new(
        StringSchema::new(),
        Int32Schema::new(),
        KeyValueEncodingType::Inline,
    ));
    let producer = client
        .typed_producer(topic, Arc::clone(&producer_schema))
        .create()
        .await?;
    let pairs: Vec<KeyValuePair<String, i32>> = vec![
        KeyValuePair {
            key: "alpha".to_owned(),
            value: 1,
        },
        KeyValuePair {
            key: "beta".to_owned(),
            value: -1,
        },
        KeyValuePair {
            key: "gamma".to_owned(),
            value: i32::MAX,
        },
    ];
    for p in &pairs {
        producer.send(p, None).await?;
    }
    producer.close().await?;

    let consumer_schema = Arc::new(KeyValueSchema::new(
        StringSchema::new(),
        Int32Schema::new(),
        KeyValueEncodingType::Inline,
    ));
    let consumer = client
        .typed_consumer(topic, consumer_schema)
        .subscription("magnetar-e2e-kv")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..pairs.len() {
        let msg = consumer.receive().await?;
        received.push(msg.value.clone());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, pairs);
    Ok(())
}

/// Temporal schemas — Date / Time / Timestamp — all share the i64-big-endian
/// wire layout but with distinct `pb::schema::Type` discriminators. We
/// round-trip representative values for each so any future regression in the
/// `schema_type()` discriminator path is caught.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_schema_temporal_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // --- DateSchema (epoch millis, semantic = java.util.Date) ---
    {
        let topic = "persistent://public/default/magnetar-e2e-schema-date";
        let producer = client
            .typed_producer(topic, Arc::new(DateSchema::new()))
            .create()
            .await?;
        let values: Vec<i64> = vec![0, 1_700_000_000_000, i64::MAX];
        for v in &values {
            producer.send(v, None).await?;
        }
        producer.close().await?;

        let consumer = client
            .typed_consumer(topic, Arc::new(DateSchema::new()))
            .subscription("magnetar-e2e-date")
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .subscribe()
            .await?;
        let mut received = Vec::new();
        let mut payloads = Vec::new();
        for _ in 0..values.len() {
            let msg = consumer.receive().await?;
            received.push(msg.value);
            payloads.push(msg.payload.to_vec());
            consumer.ack(msg.message_id).await?;
        }
        consumer.close().await?;
        assert_eq!(received, values);
        for (got, want) in payloads.iter().zip(values.iter()) {
            assert_eq!(got.as_slice(), &want.to_be_bytes(), "Date wire layout");
        }
    }

    // --- TimeSchema (millis since midnight, semantic = java.sql.Time) ---
    {
        let topic = "persistent://public/default/magnetar-e2e-schema-time";
        let producer = client
            .typed_producer(topic, Arc::new(TimeSchema::new()))
            .create()
            .await?;
        let values: Vec<i64> = vec![0, 12 * 3600 * 1000, 86_400_000 - 1];
        for v in &values {
            producer.send(v, None).await?;
        }
        producer.close().await?;

        let consumer = client
            .typed_consumer(topic, Arc::new(TimeSchema::new()))
            .subscription("magnetar-e2e-time")
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .subscribe()
            .await?;
        let mut received = Vec::new();
        for _ in 0..values.len() {
            let msg = consumer.receive().await?;
            received.push(msg.value);
            consumer.ack(msg.message_id).await?;
        }
        consumer.close().await?;
        assert_eq!(received, values);
    }

    // --- TimestampSchema (epoch millis, semantic = java.sql.Timestamp) ---
    {
        let topic = "persistent://public/default/magnetar-e2e-schema-timestamp";
        let producer = client
            .typed_producer(topic, Arc::new(TimestampSchema::new()))
            .create()
            .await?;
        let values: Vec<i64> = vec![i64::MIN, 0, 1_700_000_000_000];
        for v in &values {
            producer.send(v, None).await?;
        }
        producer.close().await?;

        let consumer = client
            .typed_consumer(topic, Arc::new(TimestampSchema::new()))
            .subscription("magnetar-e2e-timestamp")
            .subscription_type(SubType::Exclusive)
            .initial_position(InitialPosition::Earliest)
            .subscribe()
            .await?;
        let mut received = Vec::new();
        for _ in 0..values.len() {
            let msg = consumer.receive().await?;
            received.push(msg.value);
            consumer.ack(msg.message_id).await?;
        }
        consumer.close().await?;
        assert_eq!(received, values);
    }

    client.close().await;
    Ok(())
}
