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
//! 3. **The broker agrees with the client about the initial grant** (issue #426). Both engines used
//!    to follow the sans-io `Connection::initial_flow` with a raw `Connection::flow(handle,
//!    receiver_queue_size)`: a second, wire-only frame no client mirror accounted for.
//!    `available_permits()` said `receiver_queue_size` while the broker's own `availablePermits`
//!    said twice that, so the balance point 2 rests on was measured against the wrong number and
//!    the broker could hand a consumer twice the messages its queue was sized for. Read straight
//!    out of the broker's topic stats, which is the only oracle that can tell the two apart.
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
use magnetar_admin::{AdminClient, TopicStats};
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

/// How long the issue #426 check waits for every consumer's initial grant to
/// show up in the broker's topic stats.
const ADMIN_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const ADMIN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Settle window between "the broker reports a non-zero grant" and the reading
/// the assertion uses. A redundant second `CommandFlow` rides the same driver
/// flush as the first, so it is already on the wire — this only removes any
/// dependence on where the broker happened to be in processing that write.
const ADMIN_SETTLE: Duration = Duration::from_secs(1);

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

/// The broker's own `availablePermits` for every consumer registered on
/// `subscription`, in the order the broker lists them. Empty while the
/// subscription is not yet in the stats response.
fn broker_available_permits(stats: &TopicStats, subscription: &str) -> Vec<i64> {
    stats
        .subscriptions
        .get(subscription)
        .and_then(|sub| sub.get("consumers"))
        .and_then(serde_json::Value::as_array)
        .map(|consumers| {
            consumers
                .iter()
                .filter_map(|consumer| {
                    consumer
                        .get("availablePermits")
                        .and_then(serde_json::Value::as_i64)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Poll the broker until it reports a grant for all `expected` consumers, let
/// the reading settle, then return the settled per-consumer permits. Nothing
/// has been published at the call site, so the broker has spent none of what it
/// was granted and its counter is the grant itself.
async fn settled_broker_permits(
    admin: &AdminClient,
    topic: &str,
    subscription: &str,
    expected: usize,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + ADMIN_POLL_TIMEOUT;
    loop {
        let observation = match admin.topic_stats(topic).await {
            Ok(stats) => {
                let permits = broker_available_permits(&stats, subscription);
                if permits.len() == expected && permits.iter().all(|p| *p > 0) {
                    tokio::time::sleep(ADMIN_SETTLE).await;
                    return Ok(broker_available_permits(
                        &admin.topic_stats(topic).await?,
                        subscription,
                    ));
                }
                format!("{permits:?}")
            }
            // The topic is created by the subscribe itself, so a stats call
            // that races that creation 404s. Retry until the deadline.
            Err(error) => format!("admin error: {error}"),
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "broker did not report an initial grant for all {expected} consumer(s) of \
                 `{subscription}` within {ADMIN_POLL_TIMEOUT:?}; last observation: {observation}"
            )
            .into());
        }
        tokio::time::sleep(ADMIN_POLL_INTERVAL).await;
    }
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
    let (service_url, admin_url, _container) = start_pulsar().await?;

    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    // Both #414 knobs armed end-to-end against a real broker (ADR-0101, ADR-0103).
    //
    // **This is a wiring claim, not a behaviour claim**, and the window is deliberately
    // unreachable rather than the documented 30 s production value. The watchdog reports
    // SILENCE, not fault (ADR-0101), so a *healthy* consumer that has drained its share
    // while its siblings finish, or that is simply waiting out an admin poll, satisfies
    // the stall predicate exactly as a wedged one does. With a reachable window this test
    // could therefore fire automatic recovery and send a `CommandSubscribe` for a live,
    // still-registered consumer id to a real broker — a path nothing here pins, and one
    // that would make this test's outcome depend on CI runner speed.
    //
    // 300 s is above every wait this test can accumulate after the subscribe that arms
    // the window: `ADMIN_POLL_TIMEOUT` is 15 s, the drain idle timeouts are 10 s, and
    // `ADMIN_SETTLE` is 1 s, for a worst case well under a minute.
    //
    // What this does prove is that the façade accepts and plumbs both knobs through to a
    // real broker session, and that arming them perturbs nothing about the churn drain
    // asserted below. The mechanism's own behaviour — one report per episode, at most
    // `consumer_stall_auto_recovery` re-subscribes per stall streak, reset on a real
    // dispatch unit — is pinned deterministically at the three layers below this one
    // (`magnetar-proto`'s `consumer_stall_and_recovery_tests`, both engines'
    // `consumer_stall_recovery.rs`, and `magnetar-differential`'s
    // `wedged_shared_dispatcher_*` traces), where the clock is injected and the broker's
    // permit accounting can be corrupted on purpose.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .consumer_stall_timeout(Duration::from_secs(300))
        .consumer_stall_auto_recovery(2)
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

    // Issue #426: and the BROKER must agree. A subscribe that grants twice
    // leaves the client reporting `RECEIVER_QUEUE_SIZE` above while the broker
    // holds `2 ×` — visible only from here, and only before anything has been
    // published to spend it.
    let granted = settled_broker_permits(&admin, &topic, &subscription, CONSUMERS).await?;
    assert_eq!(
        granted,
        vec![i64::from(RECEIVER_QUEUE_SIZE as u32); CONSUMERS],
        "the broker must hold exactly one receiver-queue grant per consumer after subscribe \
         (issue #426): configured {RECEIVER_QUEUE_SIZE}, broker reports {granted:?}",
    );

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
