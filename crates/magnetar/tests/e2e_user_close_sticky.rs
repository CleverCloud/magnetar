// SPDX-License-Identifier: Apache-2.0

//! End-to-end: a `close()` that lands while a write is stalled stays a user close.
//!
//! The driver keeps writing while bytes are queued, so a close issued while a publish is stuck
//! mid-write waits for that write, and the write deadline (`operation_timeout`, ADR-0083) then
//! goes through `mark_disconnected()`. That used to turn `Closing` into the redialable `Failed`:
//! the supervisor dialled again, re-attached the producer and `close()` never returned
//! (ADR-0134). Here a proxy between a supervised client and a real broker stops reading what the
//! client writes, the producer queues more than the kernel buffers between the two hold, and the
//! client is closed.
//!
//! Runs as a regular test under `cargo test` (ADR-0046); requires Docker on the host.

use std::time::Duration;

use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use uuid::Uuid;

mod common;

use common::frame_proxy::{FrameProxy, authority};
use common::pulsar::start_pulsar;

/// The write deadline: a stalled write fails after this long.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

/// One publish, below the broker's default 5 MiB `max_message_size`.
const MESSAGE_BYTES: usize = 1024 * 1024;

/// Far above what the loopback kernel buffers between the client and the proxy can hold.
const QUEUED_MESSAGES: usize = 48;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_close_during_a_stalled_write_ends_the_client() -> Result<(), Box<dyn std::error::Error>>
{
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
        .operation_timeout(OPERATION_TIMEOUT)
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-close-stall-{}",
        Uuid::new_v4().simple()
    );
    let producer = client.producer(&topic).batching(0, 0).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"before-the-stall".to_vec()).into())
        .await?;

    // From here on nothing the client writes is read: its writes pile up until the kernel
    // buffers are full and the driver's write is parked.
    proxy.pause_client_writes();
    let payload = vec![0x5a_u8; MESSAGE_BYTES];
    let sends: Vec<_> = (0..QUEUED_MESSAGES)
        .map(|_| tokio::spawn(producer.send(OutgoingMessage::with_payload(payload.clone()).into())))
        .collect();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let accepted_at_close = proxy.accepted();
    let closed = tokio::time::timeout(Duration::from_secs(60), client.close()).await;
    let accepted_after_close = proxy.accepted() - accepted_at_close;
    for send in &sends {
        send.abort();
    }
    assert!(
        closed.is_ok(),
        "close() must return once the stalled write's deadline fires; it did not within 60 s, and \
         the supervisor dialled the broker {accepted_after_close} more time(s) after the close"
    );
    assert_eq!(
        accepted_after_close, 0,
        "a closed client must not dial the broker again"
    );
    Ok(())
}
