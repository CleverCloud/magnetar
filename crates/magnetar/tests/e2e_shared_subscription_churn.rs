// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for issue #414: a `Shared` subscription must keep
//! draining after a consumer leaves mid-drain, against a real Apache Pulsar 4.x
//! standalone broker spun up via `testcontainers-rs`.
//!
//! ## The production failure
//!
//! A Shared subscription wedged broker-side after a churn window (a cursor reset
//! with consumers attached, a 12 → 1 scale-down, and an instance recycle
//! mid-drain). The survivors received about twenty messages and then nothing,
//! forever: the broker's `availablePermits` for the subscription sat at
//! `-177300`, `acks_failed` was `0`, and the client reported no error at all.
//! Only a superuser `pulsar-admin topics unload` recovered it.
//!
//! The wire protocol carries only monotonic client → broker permit increments
//! (`CommandFlow`), so the client cannot itself drive the broker's counter
//! negative — the root cause is broker-side. What this test pins is the
//! client-side contract that has to hold for the *healthy* broker, because
//! without it the client-side mitigations of issue #414 would be measuring
//! nothing:
//!
//! 1. **Churn drains.** N consumers share one subscription; one closes mid-drain; the survivors
//!    must between them receive every published message exactly once, including whatever the
//!    departed consumer had un-acked.
//! 2. **`available_permits()` is a live signal.** Issue #414 re-pointed that accessor from the
//!    purely-additive grant mirror to the REAL decrementing balance, so it must be observed
//!    strictly below the configured receiver-queue size while the broker is dispatching. On the old
//!    semantics it was pinned at the queue size forever and a wedged consumer was indistinguishable
//!    from a healthy one.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_shared_subscription_churn -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::collections::HashSet;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container. See
/// `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

/// Receiver-queue size for every consumer here. Small enough that a modest
/// backlog forces several flow round-trips — which is what makes the permit
/// balance move, and therefore observable.
const RECEIVER_QUEUE_SIZE: usize = 4;

/// Consumers attached to the one subscription. Three is the smallest count that
/// still leaves TWO survivors after the mid-drain close, so the assertion is
/// about the subscription rather than about a single fallback consumer.
const CONSUMERS: usize = 3;

/// Messages published into the backlog.
const TOTAL: usize = 60;

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

/// Start a Pulsar 4.x standalone container and return (`service_url`,
/// `admin_url`, `container_handle`). The container is held by the returned
/// guard; dropping it stops the broker.
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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
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

/// Receive and ack up to `max` messages, stopping early once the consumer stays
/// idle for `idle_timeout`. Records the lowest `available_permits()` observed
/// while messages were actually flowing.
async fn drain_some(
    consumer: &magnetar::runtime_tokio::Consumer,
    max: usize,
    idle_timeout: Duration,
    lowest_permits: &mut u32,
) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    while payloads.len() < max {
        let Ok(Ok(message)) = tokio::time::timeout(idle_timeout, consumer.receive()).await else {
            break;
        };
        // Sampled BEFORE the ack, i.e. with the broker's grant partly spent —
        // this is the observation that only means something on the issue #414
        // semantics (the REAL decrementing balance).
        *lowest_permits = (*lowest_permits).min(consumer.available_permits());
        payloads.push(message.payload.to_vec());
        let _ = consumer.ack(message.message_id).await;
    }
    payloads
}

/// N consumers share one subscription; one is closed mid-drain while it still
/// holds un-acked work. The survivors must finish the backlog, every message
/// exactly once, and the permit balance must be seen moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_shared_subscription_survives_mid_drain_consumer_close()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-churn-{suffix}");
    let subscription = format!("magnetar-e2e-churn-{suffix}");

    // Attach every consumer BEFORE publishing so the broker dispatches across
    // the whole set instead of letting the first one drain the backlog alone.
    let mut consumers = Vec::with_capacity(CONSUMERS);
    for index in 0..CONSUMERS {
        consumers.push(
            client
                .consumer(&topic)
                .subscription(&subscription)
                .subscription_type(SubType::Shared)
                .name(format!("churn-consumer-{index}"))
                .receiver_queue_size(RECEIVER_QUEUE_SIZE)
                .initial_position(InitialPosition::Earliest)
                .subscribe()
                .await?,
        );
    }

    // Every consumer starts with its full grant un-spent.
    for (index, consumer) in consumers.iter().enumerate() {
        assert_eq!(
            consumer.available_permits(),
            RECEIVER_QUEUE_SIZE as u32,
            "consumer {index} must start with its full receiver-queue grant"
        );
    }

    let producer = client.producer(&topic).create().await?;
    let mut sent: Vec<Vec<u8>> = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        let payload = format!("churn-{i}").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
        sent.push(payload);
    }
    producer.close().await?;

    let mut received: Vec<Vec<u8>> = Vec::with_capacity(TOTAL);
    let mut lowest_permits = u32::MAX;

    // Partial drain across every consumer — a few each, so the one we are about
    // to close is genuinely mid-stream rather than idle.
    for consumer in &consumers {
        received
            .extend(drain_some(consumer, 3, Duration::from_secs(10), &mut lowest_permits).await);
    }

    // The churn: one consumer leaves mid-drain. Anything the broker had
    // dispatched to it and it never acked is now the subscription's problem to
    // redeliver to the survivors.
    let leaver = consumers.remove(0);
    leaver.close().await?;

    // The survivors must finish the backlog. Round-robin between them so
    // neither is starved by the other's idle timeout.
    loop {
        let before = received.len();
        for consumer in &consumers {
            received.extend(
                drain_some(
                    consumer,
                    TOTAL,
                    Duration::from_secs(10),
                    &mut lowest_permits,
                )
                .await,
            );
        }
        if received.len() == before || received.len() >= TOTAL {
            break;
        }
    }

    for consumer in consumers {
        consumer.close().await?;
    }
    client.close().await;

    // Every published message reached a live consumer, exactly once across the
    // churn — nothing stranded on the consumer that left.
    let unique: HashSet<Vec<u8>> = received.iter().cloned().collect();
    let expected: HashSet<Vec<u8>> = sent.into_iter().collect();
    assert_eq!(
        unique,
        expected,
        "every published message must reach a surviving consumer after the mid-drain close; \
         received {} message(s), {} distinct",
        received.len(),
        unique.len(),
    );

    // Issue #414's detection premise: `available_permits()` reports the REAL
    // decrementing balance. Under the pre-#414 additive mirror this stayed
    // pinned at `RECEIVER_QUEUE_SIZE` for the whole run, which is exactly why an
    // application polling it could not tell a draining consumer from a wedged
    // one.
    assert!(
        lowest_permits < RECEIVER_QUEUE_SIZE as u32,
        "available_permits() must fall below the receiver-queue size ({RECEIVER_QUEUE_SIZE}) \
         while the broker is dispatching — it reports the real balance now, not the \
         cumulative grant; observed minimum {lowest_permits}"
    );

    Ok(())
}
