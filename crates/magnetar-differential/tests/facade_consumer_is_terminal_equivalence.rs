// SPDX-License-Identifier: Apache-2.0

//! `ConsumerApi::is_terminal` reports the same transitions on the tokio and the moonpool facade
//! consumer: `false` while the consumer is subscribed and its connection is live, `true` once the
//! client is closed and no driver is left to resolve a receive.
//!
//! The facade's push loops key their exit on this predicate (ADR-0130), so the two engines must
//! agree on it; running it from `magnetar-differential` also executes both engines' `ConsumerApi`
//! implementations inside the Moonpool coverage domain (ADR-0120, ADR-0121).

use magnetar::{ConsumerApi, MoonpoolEngine, PulsarClient};
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::ConnectionConfig;
use moonpool_core::TokioProviders;

const TOPIC: &str = "persistent://public/default/facade-consumer-is-terminal";
const SUBSCRIPTION: &str = "facade-consumer-is-terminal";

/// `(is_terminal while subscribed, is_terminal after the client closed)` on the tokio facade.
async fn tokio_transitions() -> (bool, bool) {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let client = PulsarClient::builder()
        .service_url(broker.pulsar_url())
        .build()
        .await
        .expect("connect tokio facade client");
    let consumer = client
        .consumer(TOPIC)
        .subscription(SUBSCRIPTION)
        .subscribe()
        .await
        .expect("subscribe tokio facade consumer");
    let open = ConsumerApi::is_terminal(&consumer);
    client.close().await;
    let closed = ConsumerApi::is_terminal(&consumer);
    broker.shutdown().await;
    (open, closed)
}

/// The same transitions on the moonpool facade.
async fn moonpool_transitions() -> (bool, bool) {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let runtime_client = magnetar_runtime_moonpool::Client::connect_plain(
        &engine,
        &broker.host_port(),
        ConnectionConfig::default(),
    )
    .await
    .expect("connect moonpool runtime client");
    let client =
        PulsarClient::<MoonpoolEngine<TokioProviders>>::from_runtime_client(runtime_client);
    let consumer = client
        .consumer(TOPIC)
        .subscription(SUBSCRIPTION)
        .subscribe()
        .await
        .expect("subscribe moonpool facade consumer");
    let open = ConsumerApi::is_terminal(&consumer);
    client.close().await;
    let closed = ConsumerApi::is_terminal(&consumer);
    broker.shutdown().await;
    (open, closed)
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_is_terminal_transitions_are_equivalent() {
    let tokio = tokio_transitions().await;
    let moonpool = moonpool_transitions().await;
    assert_eq!(
        tokio,
        (false, true),
        "a subscribed consumer is not terminal, and it is once its client has closed"
    );
    assert_eq!(
        tokio, moonpool,
        "both engines report the same is_terminal transitions"
    );
}
