// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for ADR-0053 OpenTelemetry context propagation.
//!
//! Verifies that `traceparent` / `tracestate` properties injected by the
//! producer survive the Pulsar broker round-trip and are extractable on
//! the consumer side, and (ADR-0053 §D2) that the retry-letter
//! (`reconsume_later`) path on the façade `TypedConsumer` re-injects the
//! retrying consumer's current span, replacing the inbound trace.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_otel_context_propagation -- --nocapture
//! ```
//!
//! Requires Docker on the host.
//!
//! The companion layers are:
//! - `crates/magnetar/src/otel.rs` (façade unit tests)
//! - `crates/magnetar-runtime-tokio/tests/otel_context_propagation.rs`
//! - `crates/magnetar-runtime-moonpool/tests/otel_context_propagation.rs`
//! - `crates/magnetar-differential/tests/otel_context_propagation_equivalence.rs`

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
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

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// Install the W3C `TraceContext` propagator, produce a message under a
/// synthetic span, consume it and verify that `traceparent` survives
/// the broker round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_otel_traceparent_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };

    let (service_url, _container) = start_pulsar().await?;
    let topic = unique_topic("magnetar-e2e-otel");

    // Install W3C propagator for the duration of this test.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let client = PulsarClient::builder()
        .service_url(&service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;

    // Attach a synthetic span context.
    let trace_id = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap();
    let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
    let span_ctx = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    let otel_ctx = opentelemetry::Context::current().with_remote_span_context(span_ctx);
    let _attached = otel_ctx.attach();

    // Send — OTel context should be injected at the send boundary.
    let msg = OutgoingMessage::with_payload(b"otel-e2e".as_slice());
    msg.send(&producer).await?;
    producer.flush().await?;

    // Consume and verify traceparent round-tripped.
    let consumer = client
        .consumer(&topic)
        .subscription("otel-e2e-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await
        .expect("receive should not time out")?;

    let traceparent = msg
        .metadata
        .properties
        .iter()
        .find(|kv| kv.key == "traceparent");
    assert!(
        traceparent.is_some(),
        "expected traceparent in received message properties: {:?}",
        msg.metadata.properties
    );

    // Verify the trace_id portion of the traceparent value.
    let tp_value = &traceparent.unwrap().value;
    assert!(
        tp_value.contains("0af7651916cd43dd8448eb211c80319c"),
        "traceparent should contain the original trace_id: {tp_value}"
    );

    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    producer.close().await?;
    client.close().await;

    // Reset propagator.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry::trace::noop::NoopTextMapPropagator::new(),
    );

    Ok(())
}

/// ADR-0053 §D2 — produce a message under trace A, then `reconsume_later` it
/// under trace B on the façade `TypedConsumer`. The retry-letter copy must carry
/// trace B (the retrying consumer's current span), not the inbound trace A, and
/// must carry exactly one `traceparent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // single-shot e2e scenario; splitting scatters the produce → reconsume → assert narrative
async fn e2e_otel_reconsume_reinjects_traceparent() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use magnetar_proto::schema::StringSchema;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };

    const TRACE_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TRACE_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let (service_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-otel-retry-{id}");
    let retry_topic = format!("{topic}-RETRY");

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let span_ctx = |trace: &str, span: &str| {
        SpanContext::new(
            TraceId::from_hex(trace).unwrap(),
            SpanId::from_hex(span).unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        )
    };

    let client = PulsarClient::builder()
        .service_url(&service_url)
        .build()
        .await?;

    // Produce under trace A — TypedProducer::send auto-injects at the send boundary.
    let producer = client
        .typed_producer(&topic, Arc::new(StringSchema::new()))
        .create()
        .await?;
    {
        let cx_a = opentelemetry::Context::current()
            .with_remote_span_context(span_ctx(TRACE_A, "a1a1a1a1a1a1a1a1"));
        let _guard = cx_a.attach();
        producer.send(&"retry-me".to_owned(), None).await?;
    }
    producer.flush().await?;
    producer.close().await?;

    // Consume on a façade TypedConsumer and reconsume under trace B.
    let consumer = client
        .typed_consumer(&topic, Arc::new(StringSchema::new()))
        .subscription("otel-retry-sub")
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let received = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await
        .expect("receive should not time out")?;

    // Sanity: the original carries trace A.
    let inbound_tp = received
        .raw
        .metadata
        .properties
        .iter()
        .find(|kv| kv.key == "traceparent")
        .map(|kv| kv.value.clone());
    assert!(
        inbound_tp.as_deref().is_some_and(|v| v.contains(TRACE_A)),
        "original message should carry trace A: {inbound_tp:?}"
    );

    let retry_producer = client.producer(retry_topic.clone()).create().await?;
    {
        let cx_b = opentelemetry::Context::current()
            .with_remote_span_context(span_ctx(TRACE_B, "b2b2b2b2b2b2b2b2"));
        let _guard = cx_b.attach();
        consumer
            .reconsume_later(&retry_producer, received.raw, Duration::from_secs(1))
            .await?;
    }

    // Pull the retry-letter copy and assert the trace was re-injected (B), not A.
    let retry_consumer = client
        .consumer(retry_topic.clone())
        .subscription("otel-retry-tail")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let redelivered = tokio::time::timeout(Duration::from_secs(15), retry_consumer.receive())
        .await
        .expect("retry receive should not time out")?;

    let traceparents: Vec<&str> = redelivered
        .metadata
        .properties
        .iter()
        .filter(|kv| kv.key == "traceparent")
        .map(|kv| kv.value.as_str())
        .collect();
    assert_eq!(
        traceparents.len(),
        1,
        "exactly one traceparent on the retry copy: {:?}",
        redelivered.metadata.properties
    );
    assert!(
        traceparents[0].contains(TRACE_B),
        "retry copy must carry the reconsume span (trace B): {}",
        traceparents[0]
    );
    assert!(
        !traceparents[0].contains(TRACE_A),
        "inbound trace A must have been replaced: {}",
        traceparents[0]
    );

    retry_consumer.ack(redelivered.message_id).await?;
    retry_consumer.close().await?;
    retry_producer.close().await?;
    consumer.close().await?;
    client.close().await;

    opentelemetry::global::set_text_map_propagator(
        opentelemetry::trace::noop::NoopTextMapPropagator::new(),
    );

    Ok(())
}
