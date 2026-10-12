// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for `ConsumerEventListener` push delivery (issue #348,
//! ADR-0081) against a real Apache Pulsar 4.x standalone broker via
//! `testcontainers-rs`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker +
//! `apachepulsar/pulsar` reachable.
//!
//! ## What this proves end-to-end
//!
//! 1. Two `Failover` consumers on the same subscription: the broker elects one active, the other
//!    stand-by.
//! 2. Each consumer's `ConsumerEventListener` observes its own election outcome.
//! 3. Closing the active consumer promotes the stand-by — its listener fires
//!    `ConsumerEvent::BecameActive` and `Consumer::is_active()` flips to `Some(true)`, both within
//!    a generous bounded timeout (issue #307's flow re-arm plus this issue's event surface, driven
//!    by the SAME `CommandActiveConsumerChange` frame).

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{ConsumerEvent, ConsumerEventListener, PulsarClient};
use parking_lot::Mutex;
use uuid::Uuid;

mod common;

use common::pulsar::start_pulsar;

/// Poll `probe` every 200ms until it returns `Some`, or panic with `msg` once
/// `deadline` elapses. Bounded busy-wait — the broker-side election / promotion
/// settle time is not otherwise observable from the client.
async fn wait_for<T, F: Fn() -> Option<T>>(deadline: Duration, msg: &str, probe: F) -> T {
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(start.elapsed() < deadline, "{msg}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_consumer_event_listener_fires_on_failover_promotion()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-cel-{suffix}");
    let subscription = format!("magnetar-e2e-cel-{suffix}");

    let consumer_a = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("cel-consumer-a")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("cel-consumer-b")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Attach an event listener to each, recording every observed transition
    // into its own edge log.
    let edges_a: Arc<Mutex<Vec<ConsumerEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let edges_b: Arc<Mutex<Vec<ConsumerEvent>>> = Arc::new(Mutex::new(Vec::new()));

    let listener_a: ConsumerEventListener = {
        let edges = edges_a.clone();
        Arc::new(move |ev: ConsumerEvent| {
            edges.lock().push(ev);
        })
    };
    let listener_b: ConsumerEventListener = {
        let edges = edges_b.clone();
        Arc::new(move |ev: ConsumerEvent| {
            edges.lock().push(ev);
        })
    };

    let handle_a = magnetar::spawn_consumer_event_listener(consumer_a.clone(), listener_a);
    let handle_b = magnetar::spawn_consumer_event_listener(consumer_b.clone(), listener_b);

    // Broker takes a beat to elect the active consumer once both have
    // registered (`activeConsumerFailoverDelayTimeMillis`, default 1s).
    // `is_active()` reads the per-slot state directly — no probe message
    // needed to determine which side won the election.
    let active_is_a = wait_for(
        Duration::from_secs(15),
        "broker never elected an active Failover consumer (no CommandActiveConsumerChange \
         observed by either side)",
        || match (consumer_a.is_active(), consumer_b.is_active()) {
            (Some(true), _) => Some(true),
            (_, Some(true)) => Some(false),
            _ => None,
        },
    )
    .await;
    let active_name = if active_is_a {
        "cel-consumer-a"
    } else {
        "cel-consumer-b"
    };
    let standby_name = if active_is_a {
        "cel-consumer-b"
    } else {
        "cel-consumer-a"
    };
    eprintln!("phase-1: broker elected {active_name} as active");

    // The winner's own listener must have observed its promotion. `is_active()`
    // and the ring entry the listener pops are set atomically together, but the
    // listener poller task still needs its own scheduler turn to pop + fire the
    // callback — bound the wait instead of asserting synchronously.
    let winner_edges = if active_is_a {
        edges_a.clone()
    } else {
        edges_b.clone()
    };
    wait_for(
        Duration::from_secs(5),
        &format!("{active_name}'s listener never observed its own BecameActive promotion"),
        || {
            winner_edges
                .lock()
                .contains(&ConsumerEvent::BecameActive)
                .then_some(())
        },
    )
    .await;

    // Split into active vs stand-by so the close-self move is unambiguous.
    let (active_consumer, standby_consumer, standby_edges) = if active_is_a {
        (consumer_a, consumer_b, edges_b.clone())
    } else {
        (consumer_b, consumer_a, edges_a.clone())
    };

    // Close the active consumer → broker promotes the stand-by.
    eprintln!("phase-2: closing {active_name}, expecting {standby_name} to take over");
    active_consumer.close().await?;

    // The stand-by's `is_active()` must flip to `Some(true)`.
    wait_for(
        Duration::from_secs(20),
        &format!(
            "{standby_name} never observed promotion after {active_name} closed \
             (is_active() stayed != Some(true))"
        ),
        || (standby_consumer.is_active() == Some(true)).then_some(()),
    )
    .await;

    // And the stand-by's listener must have fired BecameActive for the
    // promotion (in addition to whatever it may have logged during the
    // initial election, which for the loser is nothing — Failover only
    // notifies the side whose state actually changed).
    wait_for(
        Duration::from_secs(5),
        &format!("{standby_name}'s listener never observed BecameActive after promotion"),
        || {
            standby_edges
                .lock()
                .contains(&ConsumerEvent::BecameActive)
                .then_some(())
        },
    )
    .await;

    handle_a.close().await;
    handle_b.close().await;
    standby_consumer.close().await?;
    client.close().await;

    Ok(())
}
