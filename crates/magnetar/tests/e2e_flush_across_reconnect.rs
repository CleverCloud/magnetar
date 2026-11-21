// SPDX-License-Identifier: Apache-2.0

//! End-to-end: `Producer::flush` stays pending while a reconnect holds a send in the reset
//! snapshots.
//!
//! `Connection::reset()` moves an unacknowledged publish out of the producer's live queue into the
//! reset snapshots, and it goes back only when the broker acknowledges the producer's
//! re-attachment. `flush` used to read the live queue alone, so in that window it returned `Ok`
//! with the publish still owed a receipt (ADR-0137). A proxy between a supervised client and a
//! real broker drops the publish so it stays unacknowledged, cuts the session, and holds the
//! broker's `CommandProducerSuccess` on the redial while `flush` is observed.
//!
//! Runs as a regular test under `cargo test` (ADR-0046); requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::base_command::Type;
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use uuid::Uuid;

mod common;

use common::frame_proxy::{FrameProxy, authority};
use common::pulsar::start_pulsar;

/// Far beyond the test: the send must resolve through its replay, not through `send_timeout`.
const SEND_TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_flush_waits_for_the_send_a_reconnect_relocated()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let proxy = FrameProxy::start(authority(&service_url)).await?;
    let client = PulsarClient::builder()
        .service_url(proxy.url())
        .enable_reconnect(SupervisorConfig {
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(1),
            max_attempts: None,
            ..SupervisorConfig::default()
        })
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-flush-relocated-{}",
        Uuid::new_v4().simple()
    );
    let producer = client
        .producer(&topic)
        .batching(0, 0)
        .send_timeout(SEND_TIMEOUT)
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(b"before-the-cut".to_vec()).into())
        .await?;

    // The broker never sees this publish, so it is still owed a receipt when the session drops.
    proxy.drop_next_client_frame(Type::Send);
    let send =
        tokio::spawn(producer.send(OutgoingMessage::with_payload(b"relocated".to_vec()).into()));
    tokio::time::timeout(
        Duration::from_secs(30),
        proxy.wait_until(|p| p.dropped() == 1),
    )
    .await
    .map_err(|_| "the publish never reached the proxy")?;

    // Cut the session; on the redial the broker's answer to the producer's re-attachment is held,
    // so the publish sits in the reset snapshots, out of the live queue.
    proxy.hold_broker_frames_from(Type::ProducerSuccess);
    assert!(
        proxy.cut() > 0,
        "the client must have a connection through the proxy"
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        proxy.wait_until(|p| p.holding() > 0),
    )
    .await
    .map_err(|_| "the producer was never re-attached")?;

    let in_window = tokio::time::timeout(Duration::from_secs(3), producer.flush()).await;
    assert!(
        in_window.is_err(),
        "flush() returned {in_window:?} while the send the reconnect relocated was still owed a \
         receipt"
    );
    assert_eq!(
        producer.pending_count(),
        1,
        "the relocated send is still unacknowledged"
    );

    // Once the re-attachment is acknowledged the publish is replayed and acknowledged.
    proxy.release();
    let replayed = tokio::time::timeout(Duration::from_secs(30), send)
        .await
        .map_err(|_| "the relocated send was never acknowledged after the re-attachment")??;
    replayed?;
    tokio::time::timeout(Duration::from_secs(30), producer.flush())
        .await
        .map_err(|_| "flush() never returned once the replayed send was acknowledged")??;
    producer.close().await?;
    client.close().await;
    Ok(())
}
