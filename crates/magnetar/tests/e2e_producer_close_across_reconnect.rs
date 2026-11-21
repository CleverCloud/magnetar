// SPDX-License-Identifier: Apache-2.0

//! End-to-end: a send a reconnect relocated resolves when its producer is closed before the
//! rebuild.
//!
//! `Connection::reset()` moves an unacknowledged publish into the reset snapshots, where it waits
//! for the producer's re-attachment. A producer closed after the reset and before the rebuild is
//! not re-attached, and the rebuild used to discard its snapshots: the close acknowledgement,
//! which arrives after the rebuild, then found nothing to fail, and the send's future stayed
//! pending until the client shut down (ADR-0135). A proxy between a supervised client and a real
//! broker drops the publish so it stays unacknowledged, cuts the session, and holds the broker's
//! `CommandConnected` on the redial while the producer is closed.
//!
//! Runs as a regular test under `cargo test` (ADR-0046); requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::base_command::Type;
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use uuid::Uuid;

mod common;

use common::frame_proxy::{FrameProxy, authority};
use common::pulsar::start_pulsar;

/// Far beyond the test: the send must resolve through the close, not through `send_timeout`.
const SEND_TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_relocated_by_a_reconnect_resolves_when_its_producer_is_closed_before_the_rebuild()
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
        "persistent://public/default/magnetar-e2e-close-relocated-{}",
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

    // Cut every connection; on each redial the broker's CONNECTED is held, and a held CONNECTED
    // proves that its connection has reset. The client has one connection for its lookups and one
    // for the producer, which redial a few milliseconds apart, so wait for all of them: only then
    // has the producer's own connection reset (the publish sits in its reset snapshots) without
    // rebuilding its producers yet. Waiting for the first alone could close the producer before
    // its connection's reset, which then takes the close and resolves the send without the rebuild.
    proxy.hold_broker_frames_from(Type::Connected);
    let cut = proxy.cut();
    assert!(
        cut > 0,
        "the client must have a connection through the proxy"
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        proxy.wait_until(|p| p.holding() >= cut),
    )
    .await
    .map_err(|_| "the client never redialled")?;

    // Close the producer in that window: the last-clone drop guard marks it closed and sends a
    // fire-and-forget close the broker answers only after the rebuild.
    drop(producer);
    proxy.release();

    let resolved = tokio::time::timeout(Duration::from_secs(30), send).await;
    let outcome = resolved.map_err(|_| {
        "the send the reconnect relocated never resolved after its producer was closed before \
         the rebuild"
    })??;
    assert!(
        outcome.is_err(),
        "the producer was closed before its publish reached the broker, so the send must fail, \
         got {outcome:?}"
    );
    client.close().await;
    Ok(())
}
