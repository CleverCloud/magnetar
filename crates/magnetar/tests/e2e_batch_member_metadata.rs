// SPDX-License-Identifier: Apache-2.0

//! End-to-end: a batch member surfaces its OWN metadata, not the batch envelope's.
//!
//! The envelope of a batch carries the FIRST member's key and ordering key
//! (`Commands.initBatchMessageMetadata`); the member's own values live in its
//! `SingleMessageMetadata`. A keyless member must therefore report no key (Java
//! `MessageImpl.init`), and a `TableView` must skip it instead of overwriting the first member's
//! entry. Requires Docker.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_batch_member_metadata_is_the_members_own() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-batch-member-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    // A long publish delay so the three concurrent sends are packed into ONE broker entry,
    // whose envelope carries the first (keyed) member's key and ordering key.
    let producer = client
        .producer(&topic)
        .batching(10, 128 * 1024)
        .batching_max_publish_delay(Duration::from_secs(2))
        .create()
        .await?;
    let keyed = |payload: &str, key: &str, time: u64| {
        producer.send(
            OutgoingMessage::with_payload(payload.as_bytes().to_vec())
                .key(key)
                .ordering_key(format!("order-{key}").into_bytes())
                .event_time_ms(time)
                .property("custom", format!("value-{key}"))
                .into(),
        )
    };
    let keyless = producer.send(OutgoingMessage::with_payload(b"keyless".to_vec()).into());
    let (first, second, third) = tokio::try_join!(
        keyed("first", "key-0", 1_700_000_001_000),
        keyless,
        keyed("third", "key-1", 1_700_000_001_001),
    )?;
    assert_eq!(
        (first.ledger_id, first.entry_id),
        (second.ledger_id, second.entry_id)
    );
    assert_eq!(
        (first.ledger_id, first.entry_id),
        (third.ledger_id, third.entry_id),
        "all three messages must share one batched broker entry"
    );
    producer.close().await?;

    // Consumer-side accessors.
    let consumer = client
        .consumer(&topic)
        .subscription(format!("magnetar-batch-member-{id}"))
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut seen = std::collections::BTreeMap::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(15), consumer.receive()).await??;
        let msg = magnetar::IncomingMessage::from(msg);
        seen.insert(String::from_utf8(msg.payload.to_vec())?, msg);
    }
    let first = &seen["first"];
    assert_eq!(first.key(), Some("key-0"));
    assert_eq!(
        first.ordering_key().map(AsRef::as_ref),
        Some(&b"order-key-0"[..])
    );
    assert_eq!(first.event_time_ms(), 1_700_000_001_000);
    assert_eq!(first.property("custom"), Some("value-key-0"));
    let third = &seen["third"];
    assert_eq!(third.key(), Some("key-1"));
    assert_eq!(third.property("custom"), Some("value-key-1"));
    let keyless = &seen["keyless"];
    assert_eq!(
        keyless.key(),
        None,
        "the envelope key is the first member's"
    );
    assert_eq!(keyless.ordering_key(), None);
    assert!(!keyless.has_properties());
    consumer.close().await?;

    // Table view: the keyless member is skipped, it does not overwrite the first member's key.
    let table_view = client
        .table_view(&topic)
        .subscription_name(format!("magnetar-batch-member-tv-{id}"))
        .create()
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while table_view.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "table view did not converge"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let snapshot = table_view.snapshot();
    assert_eq!(
        snapshot.len(),
        2,
        "keyless member must be skipped: {snapshot:?}"
    );
    assert_eq!(
        snapshot.get("key-0").map(AsRef::as_ref),
        Some(&b"first"[..])
    );
    assert_eq!(
        snapshot.get("key-1").map(AsRef::as_ref),
        Some(&b"third"[..])
    );
    table_view.close().await;
    client.close().await;
    Ok(())
}
