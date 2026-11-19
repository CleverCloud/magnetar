// SPDX-License-Identifier: Apache-2.0

//! End-to-end: a HAND-BUILT Java-style batch, read back through every façade surface that has to
//! honour a batch member's own metadata.
//!
//! A Java producer's batch envelope copies the FIRST member's partition key, key b64 flag and
//! ordering key (`Commands.initBatchMessageMetadata`); each member's own values live in its
//! `SingleMessageMetadata`, and Java's consumer clears the key and ordering key on a member that
//! has none (`MessageImpl.init`). magnetar's own batching producer does not stamp the envelope
//! that way, so this test packs the batch by hand and publishes it as one raw entry:
//!
//! - member 0: keyed (`key-0`, ordering key, event time, properties);
//! - member 1: keyless, with its own properties.
//!
//! The façade accessors, the `TableView`, the DLQ republish and `reconsume_later` must all report
//! member 1 with NO key and NO ordering key and its own properties, and member 0 with its own.
//! Requires Docker.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use magnetar::proto::pb;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::producer::OutgoingMessage as RawMessage;
use magnetar::{IncomingMessage, OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

const KEYED: &str = "keyed";
const KEYLESS: &str = "keyless";
const WAIT: Duration = Duration::from_secs(30);

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).expect("7 bits") | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).expect("7 bits"));
}

fn length_delimited(out: &mut Vec<u8>, field: u8, bytes: &[u8]) {
    out.push(field << 3 | 2);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A hand-encoded `SingleMessageMetadata` (fields: 1 `properties`, 2 `partition_key`, 3
/// `payload_size`, 5 `event_time`, 7 `ordering_key`).
fn single_metadata(
    key: Option<&str>,
    ordering_key: Option<&[u8]>,
    event_time: Option<u64>,
    custom: &str,
    payload_size: usize,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut kv = Vec::new();
    length_delimited(&mut kv, 1, b"custom");
    length_delimited(&mut kv, 2, custom.as_bytes());
    length_delimited(&mut out, 1, &kv);
    if let Some(key) = key {
        length_delimited(&mut out, 2, key.as_bytes());
    }
    out.push(3 << 3);
    varint(&mut out, payload_size as u64);
    if let Some(time) = event_time {
        out.push(5 << 3);
        varint(&mut out, time);
    }
    if let Some(ordering_key) = ordering_key {
        length_delimited(&mut out, 7, ordering_key);
    }
    out
}

/// `[u32 BE size][SingleMessageMetadata][payload]` per member, as `BatchMessageContainerImpl`
/// writes them.
fn java_style_batch() -> RawMessage {
    let members = [
        (
            KEYED,
            single_metadata(
                Some("key-0"),
                Some(b"order-0"),
                Some(1_700_000_001_000),
                "value-keyed",
                KEYED.len(),
            ),
        ),
        (
            KEYLESS,
            single_metadata(None, None, None, "value-keyless", KEYLESS.len()),
        ),
    ];
    let mut body = BytesMut::new();
    for (payload, single) in &members {
        body.put_u32(u32::try_from(single.len()).expect("fits"));
        body.extend_from_slice(single);
        body.extend_from_slice(payload.as_bytes());
    }
    // `Commands.initBatchMessageMetadata`: the envelope carries the FIRST member's key, key b64
    // flag and ordering key.
    let metadata = pb::MessageMetadata {
        partition_key: Some("key-0".to_owned()),
        partition_key_b64_encoded: Some(false),
        ordering_key: Some(Bytes::from_static(b"order-0")),
        num_messages_in_batch: Some(2),
        ..Default::default()
    };
    RawMessage {
        uncompressed_size: u32::try_from(body.len()).expect("fits"),
        payload: body.freeze(),
        metadata,
        num_messages: 2,
        txn_id: None,
        source_message_id: None,
    }
}

/// What the member `payload` must look like on any façade surface.
fn assert_member(
    payload: &str,
    key: Option<&str>,
    ordering_key: Option<&[u8]>,
    custom: Option<&str>,
    surface: &str,
) {
    let (want_key, want_order, want_custom) = if payload == KEYED {
        (Some("key-0"), Some(&b"order-0"[..]), Some("value-keyed"))
    } else {
        (None, None, Some("value-keyless"))
    };
    assert_eq!(key, want_key, "{surface}: {payload} key");
    assert_eq!(
        ordering_key, want_order,
        "{surface}: {payload} ordering key"
    );
    assert_eq!(custom, want_custom, "{surface}: {payload} custom property");
}

fn assert_facade(msg: &IncomingMessage, surface: &str) {
    let payload = std::str::from_utf8(&msg.payload).expect("utf8");
    assert_member(
        payload,
        msg.key(),
        msg.ordering_key().map(AsRef::as_ref),
        msg.property("custom"),
        surface,
    );
}

async fn receive_members(
    consumer: &magnetar_runtime_tokio::Consumer,
) -> Result<BTreeMap<String, magnetar_proto::IncomingMessage>, Box<dyn std::error::Error>> {
    let mut members = BTreeMap::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(WAIT, consumer.receive()).await??;
        members.insert(String::from_utf8(msg.payload.to_vec())?, msg);
    }
    Ok(members)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_java_style_batch_member_metadata_on_every_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-java-batch-{id}");
    let dlq_topic = format!("{topic}-DLQ");
    let retry_topic = format!("{topic}-RETRY");
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Publish the hand-built batch as ONE entry, then a keyed sentinel behind it.
    let producer = client.producer(&topic).create().await?;
    producer.send(java_style_batch()).await?;
    producer
        .send(
            OutgoingMessage::with_payload(b"sentinel".to_vec())
                .key("sentinel-key")
                .into(),
        )
        .await?;
    producer.close().await?;

    let subscribe = |name: &str| {
        client
            .consumer(&topic)
            .subscription(format!("{name}-{id}"))
            .subscription_type(SubType::Shared)
            .initial_position(InitialPosition::Earliest)
    };

    // 1. Façade accessors.
    let consumer = subscribe("accessors").subscribe().await?;
    for (_, msg) in receive_members(&consumer).await? {
        assert_facade(&IncomingMessage::from(msg), "facade accessors");
    }
    consumer.close().await?;

    // 2. TableView: the keyless member is skipped (it must not overwrite `key-0`). The sentinel is
    //    behind the batch, so once it is visible both members have been processed.
    let table_view = client
        .table_view(&topic)
        .subscription_name(format!("table-view-{id}"))
        .create()
        .await?;
    tokio::time::timeout(WAIT, async {
        while !table_view.contains_key("sentinel-key") {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let snapshot = table_view.snapshot();
    assert_eq!(
        snapshot.len(),
        2,
        "keyless member must be skipped: {snapshot:?}"
    );
    assert_eq!(
        snapshot.get("key-0").map(AsRef::as_ref),
        Some(&b"keyed"[..])
    );
    table_view.close().await;

    // 3. DLQ republish.
    let consumer = subscribe("dlq")
        .dead_letter_policy(1, Some(dlq_topic.clone()))
        .subscribe()
        .await?;
    for _round in 0..2 {
        receive_members(&consumer).await?;
        consumer.redeliver_unacked();
    }
    let dlq_producer = client.producer(&dlq_topic).create().await?;
    let mut republished = 0usize;
    tokio::time::timeout(WAIT, async {
        while republished < 2 {
            republished += consumer.republish_dead_letters(&dlq_producer).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await
    .map_err(|_| "timed out republishing the batch members")??;
    let dlq_consumer = client
        .consumer(&dlq_topic)
        .subscription(format!("dlq-tail-{id}"))
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    for (_, msg) in receive_members(&dlq_consumer).await? {
        assert_facade(&IncomingMessage::from(msg), "DLQ republish");
    }
    dlq_consumer.close().await?;
    dlq_producer.close().await?;
    consumer.close().await?;

    // 4. reconsume_later.
    let consumer = subscribe("retry").subscribe().await?;
    let retry_producer = client.producer(&retry_topic).create().await?;
    for (_, msg) in receive_members(&consumer).await? {
        consumer
            .reconsume_later(&retry_producer, msg, Duration::from_millis(500))
            .await?;
    }
    let retry_consumer = client
        .consumer(&retry_topic)
        .subscription(format!("retry-tail-{id}"))
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    for (_, msg) in receive_members(&retry_consumer).await? {
        assert_facade(&IncomingMessage::from(msg), "reconsume_later");
    }
    retry_consumer.close().await?;
    retry_producer.close().await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}
