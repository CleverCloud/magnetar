// SPDX-License-Identifier: Apache-2.0

//! PIP-180 / ADR-0033 — end-to-end shadow topic tests against a real
//! Apache Pulsar 4.x standalone broker (the shared fixture's default image).
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires
//! Docker on the host. PIP-180 is available against the baseline broker
//! (Pulsar 4.0+, ADR-0009). No new container, no docker-compose helper
//! — uses the same single-broker fixture as `e2e_pulsar.rs`.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_admin::AdminClient;

mod common;

use common::pulsar::start_pulsar;

/// PIP-180 e2e — admin REST + produce / consume cycle:
/// 1. Bootstrap the source topic by producing once.
/// 2. Create a shadow topic via the admin REST.
/// 3. List the shadow topics on the source — must include the created shadow.
/// 4. Resolve the shadow's source via `get_shadow_source`.
/// 5. Produce on the source, consume on the shadow — assert the payload crosses the source ⇄ shadow
///    boundary.
/// 6. Delete the shadow topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_shadow_topic_full_cycle() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;

    let admin = AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;
    let source = "persistent://public/default/magnetar-e2e-source";
    let shadow = "persistent://public/default/magnetar-e2e-shadow";

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // 1. Bootstrap the source topic by producing once. Pulsar auto-creates persistent topics on
    //    first producer attach.
    {
        let producer = client.producer(source).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"warmup".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // 2. Create the shadow topic via the admin REST.
    admin.create_shadow_topic(source, shadow).await?;

    // 3. List the shadow topics on the source — must include the created shadow. Topic-policy
    //    propagation is asynchronous, so poll briefly instead of asserting on the first read.
    let mut shadows = Vec::new();
    for _ in 0..20 {
        shadows = admin.get_shadow_topics(source).await?;
        if shadows.iter().any(|s| s == shadow) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        shadows.iter().any(|s| s == shadow),
        "expected {shadow:?} in shadow list, got {shadows:?}",
    );

    // 4. Resolve the shadow's source via `get_shadow_source`.
    let resolved = admin.get_shadow_source(shadow).await?;
    assert_eq!(
        resolved.as_deref(),
        Some(source),
        "shadow.shadowSource must resolve to source"
    );

    // 5. Produce on source → consume on shadow — assert the payload crosses the source ⇄ shadow
    //    boundary. The broker presents the same entries on the shadow side.
    let consumer = client
        .consumer(shadow)
        .subscription("magnetar-e2e-shadow-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let payload = b"payload-on-source".to_vec();
    {
        let producer = client.producer(source).create().await?;
        producer
            .send(OutgoingMessage::with_payload(payload.clone()).into())
            .await?;
        producer.close().await?;
    }
    let mut saw_payload = false;
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await {
            Ok(Ok(msg)) => {
                if msg.payload.as_ref() == payload.as_slice() {
                    saw_payload = true;
                }
                consumer.ack(msg.message_id).await?;
                if saw_payload {
                    break;
                }
            }
            _ => break,
        }
    }
    consumer.close().await?;
    assert!(
        saw_payload,
        "expected payload {payload:?} to surface on the shadow consumer"
    );

    // 6. Delete the shadow topic.
    admin.delete_shadow_topic(shadow, true).await?;

    client.close().await;
    Ok(())
}
