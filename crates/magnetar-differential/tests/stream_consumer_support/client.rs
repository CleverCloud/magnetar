// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use magnetar::{MoonpoolEngine, PulsarClient, TokioEngine};
use moonpool_core::TokioProviders;

use super::server::M1SocketCluster;

pub(crate) async fn connect_tokio(cluster: &M1SocketCluster) -> PulsarClient<TokioEngine> {
    PulsarClient::<TokioEngine>::builder()
        .service_url(cluster.controller_url())
        .operation_timeout(Duration::from_secs(2))
        .enable_reconnect(magnetar::SupervisorConfig::default())
        .build()
        .await
        .expect("connect Tokio facade")
}

#[allow(dead_code)]
fn terminal_supervisor() -> magnetar::SupervisorConfig {
    magnetar::SupervisorConfig {
        initial_backoff: Duration::ZERO,
        max_attempts: Some(0),
        ..Default::default()
    }
}

#[allow(dead_code)]
pub(crate) async fn connect_tokio_with_terminal_reconnect_budget(
    cluster: &M1SocketCluster,
) -> PulsarClient<TokioEngine> {
    PulsarClient::<TokioEngine>::builder()
        .service_url(cluster.controller_url())
        .operation_timeout(Duration::from_secs(2))
        .enable_reconnect(terminal_supervisor())
        .build()
        .await
        .expect("connect Tokio facade with terminal reconnect budget")
}

#[allow(dead_code)]
pub(crate) async fn connect_tokio_with_keepalive(
    cluster: &M1SocketCluster,
    keepalive: Duration,
) -> PulsarClient<TokioEngine> {
    PulsarClient::<TokioEngine>::builder()
        .service_url(cluster.controller_url())
        .keepalive(keepalive)
        .operation_timeout(Duration::from_secs(2))
        .enable_reconnect(magnetar::SupervisorConfig::default())
        .build()
        .await
        .expect("connect Tokio facade with custom keepalive")
}

pub(crate) async fn connect_moonpool(
    cluster: &M1SocketCluster,
) -> PulsarClient<MoonpoolEngine<TokioProviders>> {
    let providers = TokioProviders::new();
    let runtime_engine = magnetar::runtime_moonpool::MoonpoolEngine::new(providers);
    let config = magnetar::ConnectionConfig {
        operation_timeout: Duration::from_secs(2),
        supervisor: Some(magnetar::SupervisorConfig::default()),
        ..Default::default()
    };
    let address = cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller URL");
    let runtime_client = magnetar::runtime_moonpool::Client::connect_plain_supervised(
        &runtime_engine,
        address,
        config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool facade runtime");
    PulsarClient::<MoonpoolEngine<TokioProviders>>::from_runtime_client(runtime_client)
}

#[allow(dead_code)]
pub(crate) async fn connect_moonpool_with_terminal_reconnect_budget(
    cluster: &M1SocketCluster,
) -> PulsarClient<MoonpoolEngine<TokioProviders>> {
    let providers = TokioProviders::new();
    let runtime_engine = magnetar::runtime_moonpool::MoonpoolEngine::new(providers);
    let config = magnetar::ConnectionConfig {
        operation_timeout: Duration::from_secs(2),
        supervisor: Some(terminal_supervisor()),
        ..Default::default()
    };
    let address = cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller URL");
    let runtime_client = magnetar::runtime_moonpool::Client::connect_plain_supervised(
        &runtime_engine,
        address,
        config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool facade with terminal reconnect budget");
    PulsarClient::<MoonpoolEngine<TokioProviders>>::from_runtime_client(runtime_client)
}

#[allow(dead_code)]
pub(crate) async fn connect_moonpool_with_keepalive(
    cluster: &M1SocketCluster,
    keepalive: Duration,
) -> PulsarClient<MoonpoolEngine<TokioProviders>> {
    let providers = TokioProviders::new();
    let runtime_engine = magnetar::runtime_moonpool::MoonpoolEngine::new(providers);
    let config = magnetar::ConnectionConfig {
        keepalive_interval: keepalive,
        operation_timeout: Duration::from_secs(2),
        supervisor: Some(magnetar::SupervisorConfig::default()),
        ..Default::default()
    };
    let address = cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller URL");
    let runtime_client = magnetar::runtime_moonpool::Client::connect_plain_supervised(
        &runtime_engine,
        address,
        config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool facade runtime with custom keepalive");
    PulsarClient::<MoonpoolEngine<TokioProviders>>::from_runtime_client(runtime_client)
}
