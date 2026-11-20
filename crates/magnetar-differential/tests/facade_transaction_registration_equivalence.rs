// SPDX-License-Identifier: Apache-2.0

//! The façade's PIP-31 registration calls behave the same on both engines.
//!
//! `PulsarClient::new_transaction`, `register_partition_to_transaction`,
//! `register_subscription_to_transaction` and `commit_transaction` reach the
//! coordinator through each engine's `TransactionApi` implementation, which
//! bounds every request by `operation_timeout` and fast-fails it on a terminal
//! connection
//! ([ADR-0129](../../../specs/adr/0129-bound-and-fast-fail-pip-31-transaction-requests.md)).
//! Driving the same transaction against the scripted broker on the tokio and on
//! the moonpool façade must yield the same outcomes.

use std::time::Duration;

use magnetar::{MoonpoolEngine, PulsarClient};
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::{ConnectionConfig, TxnState};
use moonpool_core::TokioProviders;

const TOPIC: &str = "persistent://public/default/facade-txn-registration";
const SUBSCRIPTION: &str = "facade-txn-registration";
const TXN_TIMEOUT: Duration = Duration::from_secs(30);

/// What one engine's façade reported for each step of the transaction.
#[derive(Debug, PartialEq, Eq)]
struct RegistrationTrace {
    opened: bool,
    partition_registered: Result<(), String>,
    subscription_registered: Result<(), String>,
    committed: Result<TxnState, String>,
}

async fn tokio_trace() -> RegistrationTrace {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let client = PulsarClient::builder()
        .service_url(broker.pulsar_url())
        .build()
        .await
        .expect("connect tokio facade client");
    let txn = client
        .new_transaction(TXN_TIMEOUT)
        .await
        .expect("open a transaction on the tokio facade");
    let trace = RegistrationTrace {
        opened: true,
        partition_registered: client
            .register_partition_to_transaction(txn, TOPIC)
            .await
            .map_err(|err| err.to_string()),
        subscription_registered: client
            .register_subscription_to_transaction(txn, TOPIC, SUBSCRIPTION)
            .await
            .map_err(|err| err.to_string()),
        committed: client
            .commit_transaction(txn)
            .await
            .map_err(|err| err.to_string()),
    };
    client.close().await;
    broker.shutdown().await;
    trace
}

async fn moonpool_trace() -> RegistrationTrace {
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
    let txn = client
        .new_transaction(TXN_TIMEOUT)
        .await
        .expect("open a transaction on the moonpool facade");
    let trace = RegistrationTrace {
        opened: true,
        partition_registered: client
            .register_partition_to_transaction(txn, TOPIC)
            .await
            .map_err(|err| err.to_string()),
        subscription_registered: client
            .register_subscription_to_transaction(txn, TOPIC, SUBSCRIPTION)
            .await
            .map_err(|err| err.to_string()),
        committed: client
            .commit_transaction(txn)
            .await
            .map_err(|err| err.to_string()),
    };
    client.close().await;
    broker.shutdown().await;
    trace
}

#[tokio::test(flavor = "current_thread")]
async fn transaction_registration_is_equivalent_on_both_facades() {
    let tokio = tokio_trace().await;
    let moonpool = moonpool_trace().await;
    assert_eq!(
        tokio,
        RegistrationTrace {
            opened: true,
            partition_registered: Ok(()),
            subscription_registered: Ok(()),
            committed: Ok(TxnState::Committed),
        },
        "the scripted coordinator accepts every registration and the commit"
    );
    assert_eq!(
        tokio, moonpool,
        "both facades report the same registration outcomes"
    );
}
