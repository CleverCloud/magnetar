// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for a batch the broker rejects with one `CommandSendError`.
//!
//! A batching producer sends a whole batch as one `CommandSend`, and a broker that refuses the
//! frame answers with a single `CommandSendError` naming the batch's highest sequence id. Every
//! member of the batch must fail at once with the broker's error. Before the fix only the named
//! member did: the others kept their future and their memory reservation until `send_timeout`.
//!
//! The refusal is the broker's own: a frame proxy flips the last byte of the batch's frame, so
//! its CRC32C no longer matches and the broker answers `ChecksumError` without persisting it.
//! The next batch on the same producer, forwarded untouched, must resolve on its own receipts.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb;
use magnetar::proto::pb::base_command::Type;
use magnetar::runtime_tokio::ClientError;
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::frame_proxy::{FrameProxy, authority};
use common::pulsar::start_pulsar;

/// Far beyond the test's own bounds, so a member that only fails at `send_timeout` shows up as a
/// member that never resolved.
const SEND_TIMEOUT: Duration = Duration::from_mins(5);

/// How long each member of a batch may take to resolve.
const RESOLVE_BOUND: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_every_member_of_a_rejected_batch_fails_with_the_broker_error()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let proxy = FrameProxy::start(authority(&service_url)).await?;
    let client = PulsarClient::builder()
        .service_url(proxy.url())
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-batch-rejected-{}",
        uuid::Uuid::new_v4().simple()
    );
    // Three members fill a batch; the publish delay is long enough that only a full batch goes
    // out, so the three sends below share one frame.
    let producer = client
        .producer(&topic)
        .batching(3, 1024 * 1024)
        .batching_max_publish_delay(Duration::from_mins(1))
        .send_timeout(SEND_TIMEOUT)
        .create()
        .await?;

    proxy.corrupt_next_client_frame(Type::Send);
    let members: Vec<_> = (0..3)
        .map(|i| {
            tokio::spawn(
                producer
                    .send(OutgoingMessage::with_payload(format!("member-{i}").into_bytes()).into()),
            )
        })
        .collect();
    for (i, member) in members.into_iter().enumerate() {
        let outcome = tokio::time::timeout(RESOLVE_BOUND, member)
            .await
            .map_err(|_| {
                format!(
                    "member {i} of the rejected batch did not resolve within {RESOLVE_BOUND:?}: only \
                     the member the broker's CommandSendError names was failed"
                )
            })??;
        match outcome {
            Err(ClientError::SendRejected { code, .. }) => assert_eq!(
                code,
                pb::ServerError::ChecksumError as i32,
                "member {i} must fail with the broker's own error"
            ),
            other => panic!("member {i} must fail with the broker's SendError, got {other:?}"),
        }
    }
    assert_eq!(
        proxy.corrupted(),
        1,
        "exactly the one batch frame was corrupted"
    );

    // Recovery: the same producer's next batch goes through untouched and resolves on its own
    // receipts.
    let next: Vec<_> = (0..3)
        .map(|i| {
            tokio::spawn(
                producer
                    .send(OutgoingMessage::with_payload(format!("next-{i}").into_bytes()).into()),
            )
        })
        .collect();
    for (i, member) in next.into_iter().enumerate() {
        tokio::time::timeout(RESOLVE_BOUND, member)
            .await
            .map_err(|_| format!("member {i} of the next batch did not resolve"))???;
    }

    producer.close().await?;
    client.close().await;
    Ok(())
}
