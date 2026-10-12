// SPDX-License-Identifier: Apache-2.0

//! Differential equivalence of the per-client waiters' close contract.
//!
//! `Client::next_topic_list_change` (PIP-145) and
//! `Client::next_replicated_subscription_marker` (PIP-33) both document `None`
//! once the connection closes. A waiter that is parked on its buffer's `Notify`
//! only re-checks `is_closed()` when it is woken, and a dead connection pushes
//! nothing more, so each driver has to pulse that `Notify` itself when it marks
//! the connection disconnected. The runtime crates pin that per engine; this
//! test pins that the two engines' clients **observe the same thing**: parked
//! waiters, a broker hang-up, `None` from both.
//!
//! It is also the only place the sim-coverage gate (`xtask check-sim-coverage`,
//! which executes only the moonpool and differential test binaries) reaches the
//! tokio engine's `next_topic_list_change`.
//!
//! The first poll of each waiter is made by hand, so it returning `Pending`
//! proves the waiter is parked before the broker hangs up — no settle, no race.

#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Waker};

use magnetar_differential::HANG_GUARD;
use magnetar_proto::ConnectionConfig;
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

/// What a caller observes from the two waiters once the connection has died:
/// whether each resolved to `None`.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    topic_list_change_is_none: bool,
    replicated_subscription_marker_is_none: bool,
}

/// Bind a broker that completes one Pulsar handshake, stays silent, and drops
/// the socket when the returned `Notify` fires.
async fn spawn_broker_that_hangs_up_on_demand() -> (SocketAddr, Arc<Notify>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let hang_up = Arc::new(Notify::new());
    let broker_hang_up = Arc::clone(&hang_up);
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0_u8; 4096];
        let _ = sock.read(&mut buf).await;
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
            connected: Some(magnetar_proto::pb::CommandConnected {
                server_version: "hang-up-on-demand-broker".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(magnetar_proto::pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut out, &cmd).expect("encode Connected");
        let _ = sock.write_all(&out).await;
        let _ = sock.flush().await;
        broker_hang_up.notified().await;
        // Drop closes the socket.
    });
    (addr, hang_up)
}

/// Poll `fut` once by hand and require `Pending`: the waiter is now parked.
fn assert_parked<F: Future>(fut: &mut std::pin::Pin<&mut F>, what: &str) {
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "{what}: an open connection with an empty buffer parks the waiter"
    );
}

async fn observe_tokio(addr: SocketAddr, hang_up: &Notify) -> Observed {
    let client = tokio::time::timeout(
        HANG_GUARD,
        TokioClient::connect(&format!("pulsar://{addr}"), ConnectionConfig::default()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect");

    let mut topic_list = pin!(client.next_topic_list_change());
    let mut marker = pin!(client.next_replicated_subscription_marker());
    assert_parked(&mut topic_list, "tokio topic-list");
    assert_parked(&mut marker, "tokio marker");

    hang_up.notify_one();

    let topic_list_change = tokio::time::timeout(HANG_GUARD, topic_list)
        .await
        .expect("tokio topic-list waiter woke when the connection died");
    let marker = tokio::time::timeout(HANG_GUARD, marker)
        .await
        .expect("tokio marker waiter woke when the connection died");
    Observed {
        topic_list_change_is_none: topic_list_change.is_none(),
        replicated_subscription_marker_is_none: marker.is_none(),
    }
}

async fn observe_moonpool(addr: SocketAddr, hang_up: &Notify) -> Observed {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        MoonpoolClient::connect_plain(&engine, &addr.to_string(), ConnectionConfig::default()),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect");

    let mut topic_list = pin!(client.next_topic_list_change());
    let mut marker = pin!(client.next_replicated_subscription_marker());
    assert_parked(&mut topic_list, "moonpool topic-list");
    assert_parked(&mut marker, "moonpool marker");

    hang_up.notify_one();

    let topic_list_change = tokio::time::timeout(HANG_GUARD, topic_list)
        .await
        .expect("moonpool topic-list waiter woke when the connection died");
    let marker = tokio::time::timeout(HANG_GUARD, marker)
        .await
        .expect("moonpool marker waiter woke when the connection died");
    Observed {
        topic_list_change_is_none: topic_list_change.is_none(),
        replicated_subscription_marker_is_none: marker.is_none(),
    }
}

/// (d) — a topic-list waiter and a marker waiter parked on a live connection
/// both resolve to `None` when the broker hangs up, identically on both engines.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_waiters_return_none_when_the_connection_dies_parity() {
    let (tokio_addr, tokio_hang_up) = spawn_broker_that_hangs_up_on_demand().await;
    let tokio_observed = observe_tokio(tokio_addr, &tokio_hang_up).await;

    let (moonpool_addr, moonpool_hang_up) = spawn_broker_that_hangs_up_on_demand().await;
    let moonpool_observed = observe_moonpool(moonpool_addr, &moonpool_hang_up).await;

    assert_eq!(
        tokio_observed, moonpool_observed,
        "engine close observations diverged"
    );
    // Pin the outcome itself, so both engines agreeing on a hang or a value
    // cannot pass.
    assert_eq!(
        tokio_observed,
        Observed {
            topic_list_change_is_none: true,
            replicated_subscription_marker_is_none: true,
        },
        "a dead connection ends both waits with None"
    );
}
