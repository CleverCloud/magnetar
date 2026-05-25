// SPDX-License-Identifier: Apache-2.0

//! End-to-end transaction (PIP-31) tests against a real Apache Pulsar 4.x broker.
//!
//! Mirrors selected scenarios from Java
//! `org.apache.pulsar.client.api.TransactionEndToEndTest` and `TransactionTest`:
//! commit publishes are visible, abort drops them, and consumer acks issued inside
//! a transaction are rolled back on abort so a fresh subscription redelivers.
//!
//! Gated behind the `e2e` feature flag:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_transactions -- --nocapture --ignored
//! ```
//!
//! ## Why double-ignored
//!
//! The default `apachepulsar/pulsar:4.0.4` standalone image does NOT spin up the
//! transaction coordinator. Enabling it requires either a custom config that sets
//! `transactionCoordinatorEnabled=true` plus the system namespace bootstrap, or a
//! purpose-built image. Until we ship that harness these tests are landed as
//! parity scaffolding and are skipped in every default test run — both
//! `e2e: requires Docker` (shared with [`e2e_pulsar`]) and
//! `e2e: requires transaction-coordinator-enabled broker` keep them opt-in.

#![cfg(feature = "e2e")]

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const TXN_TIMEOUT: Duration = Duration::from_secs(30);
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);
const ABORT_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

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

/// Start a Pulsar 4.x standalone container with transactions enabled and return
/// (`service_url`, `admin_url`, `container_handle`).
///
/// Sets `PULSAR_PREFIX_transactionCoordinatorEnabled=true` so the broker spins up
/// the txn coordinator. The default standalone image does NOT enable this — if
/// the coordinator fails to bootstrap the new-txn RPC will hang or be rejected;
/// that's why these tests are marked
/// `e2e: requires transaction-coordinator-enabled broker` and stay opt-in.
async fn start_pulsar_with_txn() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Created namespace public/default"))
        .with_startup_timeout(Duration::from_secs(180))
        .with_env_var("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
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

fn unique_suffix() -> String {
    Uuid::new_v4().simple().to_string()
}

/// Commit path: 3 messages sent inside a transaction must all be visible to a
/// consumer after `commit_transaction`. Mirrors Java
/// `TransactionEndToEndTest#produceCommitTest`.
#[ignore = "e2e: requires Docker + transaction-coordinator-enabled broker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_txn_commit_produces_visible() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar_with_txn().await?;

    let suffix = unique_suffix();
    let topic = format!("persistent://public/default/magnetar-e2e-txn-commit-{suffix}");
    let subscription = format!("magnetar-e2e-txn-commit-{suffix}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Subscribe BEFORE commit so the consumer sees the post-commit flush. The
    // marker frames the broker emits at commit time push the visible boundary.
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let producer = client.producer(&topic).create().await?;

    let txn = client.new_transaction(TXN_TIMEOUT).await?;
    client
        .register_partition_to_transaction(txn, &topic)
        .await?;

    let payloads: Vec<Vec<u8>> = (0..3).map(|i| format!("commit-{i}").into_bytes()).collect();
    for payload in &payloads {
        producer
            .send(
                OutgoingMessage::with_payload(payload.clone())
                    .txn(txn.id())
                    .into(),
            )
            .await?;
    }

    let state = client.commit_transaction(txn).await?;
    tracing::info!(?state, "transaction committed");

    let mut received = Vec::with_capacity(payloads.len());
    for _ in 0..payloads.len() {
        let msg = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive()).await??;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }

    producer.close().await?;
    consumer.close().await?;
    client.close().await;

    assert_eq!(received, payloads);
    Ok(())
}

/// Abort path: messages produced inside a transaction must NOT be delivered to a
/// consumer once the transaction is aborted. Mirrors Java
/// `TransactionEndToEndTest#produceAbortTest`.
#[ignore = "e2e: requires Docker + transaction-coordinator-enabled broker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_txn_abort_drops_messages() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar_with_txn().await?;

    let suffix = unique_suffix();
    let topic = format!("persistent://public/default/magnetar-e2e-txn-abort-{suffix}");
    let subscription = format!("magnetar-e2e-txn-abort-{suffix}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let producer = client.producer(&topic).create().await?;

    let txn = client.new_transaction(TXN_TIMEOUT).await?;
    client
        .register_partition_to_transaction(txn, &topic)
        .await?;

    for i in 0..3 {
        producer
            .send(
                OutgoingMessage::with_payload(format!("abort-{i}").into_bytes())
                    .txn(txn.id())
                    .into(),
            )
            .await?;
    }

    let state = client.abort_transaction(txn).await?;
    tracing::info!(?state, "transaction aborted");

    // The broker must NOT deliver any aborted message. A short window of
    // silence is the only signal we have — committed traffic on this isolated
    // topic+subscription would arrive promptly, so a timeout here is the green
    // path.
    let timeout = tokio::time::timeout(ABORT_DRAIN_TIMEOUT, consumer.receive()).await;
    assert!(
        timeout.is_err(),
        "aborted transaction must not deliver messages, but consumer received one: {timeout:?}",
    );

    producer.close().await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// Ack rollback: messages produced outside a transaction are consumed and acked
/// INSIDE a transaction that then aborts. A fresh subscription on the same
/// topic+subscription must redeliver all messages because the txn-scoped acks
/// were rolled back. Mirrors Java
/// `TransactionEndToEndTest#txnAckTestNoBatchAndSharedSubAbort` /
/// `TransactionTest#testAckMessageRollback`.
#[ignore = "e2e: requires Docker + transaction-coordinator-enabled broker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_consumer_ack_with_txn_rolled_back_on_abort() -> Result<(), Box<dyn std::error::Error>>
{
    let (service_url, _admin_url, _container) = start_pulsar_with_txn().await?;

    let suffix = unique_suffix();
    let topic = format!("persistent://public/default/magnetar-e2e-txn-ackrb-{suffix}");
    let subscription = format!("magnetar-e2e-txn-ackrb-{suffix}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Publish 3 messages OUTSIDE any transaction. These are durable and visible
    // immediately.
    let producer = client.producer(&topic).create().await?;
    let payloads: Vec<Vec<u8>> = (0..3).map(|i| format!("ackrb-{i}").into_bytes()).collect();
    for payload in &payloads {
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
    }
    producer.close().await?;

    // Consume + ack INSIDE a transaction, then abort. Shared subscription so the
    // broker keeps per-message ack state (cumulative ack on Exclusive would also
    // work; Shared mirrors the Java fixture and the rollback path most users hit).
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let txn = client.new_transaction(TXN_TIMEOUT).await?;
    client
        .register_subscription_to_transaction(txn, &topic, &subscription)
        .await?;

    let mut acked = Vec::with_capacity(payloads.len());
    for _ in 0..payloads.len() {
        let msg = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive()).await??;
        acked.push(msg.payload.to_vec());
        consumer.ack_with_txn(msg.message_id, txn.id()).await?;
    }
    assert_eq!(acked, payloads, "first consumer should see all 3 messages");

    let state = client.abort_transaction(txn).await?;
    tracing::info!(?state, "ack transaction aborted");

    // Close the consumer so the broker can redeliver to the next subscriber on
    // the same subscription. Aborting the txn rolls back the acks, but the
    // already-delivered tracker only releases on session close.
    consumer.close().await?;

    // Fresh consumer on the SAME subscription. The aborted acks must be rolled
    // back so the broker redelivers all 3 messages.
    let replay_consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Shared)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut replayed = Vec::with_capacity(payloads.len());
    for _ in 0..payloads.len() {
        let msg = tokio::time::timeout(RECEIVE_TIMEOUT, replay_consumer.receive()).await??;
        replayed.push(msg.payload.to_vec());
        replay_consumer.ack(msg.message_id).await?;
    }
    replay_consumer.close().await?;
    client.close().await;

    // Order is not guaranteed under Shared after rollback; compare as sorted sets.
    let mut replayed_sorted = replayed.clone();
    replayed_sorted.sort();
    let mut expected_sorted = payloads.clone();
    expected_sorted.sort();
    assert_eq!(
        replayed_sorted, expected_sorted,
        "after txn abort, all acked messages must be redelivered"
    );
    Ok(())
}
