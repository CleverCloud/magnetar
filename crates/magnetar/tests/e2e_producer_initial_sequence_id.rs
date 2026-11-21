// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for the resume point of a producer opened with `initial_sequence_id`.
//!
//! A producer opened with `initial_sequence_id(n)` reports `n - 1` as its last published sequence
//! id until the first receipt, so a caller resuming from a checkpoint can read where it stands.
//! A broker that keeps no deduplication state answers `CommandProducerSuccess` with
//! `last_sequence_id = -1`, and the client used to store that answer unconditionally: the resume
//! point was gone before `create()` returned. After the fix the broker's value can only raise the
//! counter, so the resume point survives the attach and the first receipt advances it.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker on the host.

use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

/// The sequence id the producer resumes from.
const INITIAL_SEQUENCE_ID: u64 = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_a_producer_keeps_its_initial_sequence_id_resume_point_across_the_attach()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = format!(
        "persistent://public/default/magnetar-e2e-initial-sequence-id-{}",
        uuid::Uuid::new_v4().simple()
    );
    let producer = client
        .producer(&topic)
        .initial_sequence_id(INITIAL_SEQUENCE_ID)
        .create()
        .await?;

    assert_eq!(
        producer.last_sequence_id_published(),
        i64::try_from(INITIAL_SEQUENCE_ID)? - 1,
        "the attach must not erase the resume point the producer was opened with"
    );

    producer
        .send(OutgoingMessage::with_payload(b"first".to_vec()).into())
        .await?;
    assert_eq!(
        producer.last_sequence_id_published(),
        i64::try_from(INITIAL_SEQUENCE_ID)?,
        "the receipt of the first publish advances the counter to its sequence id"
    );

    producer.close().await?;
    client.close().await;
    Ok(())
}
