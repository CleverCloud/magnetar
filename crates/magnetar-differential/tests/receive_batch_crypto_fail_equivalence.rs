// SPDX-License-Identifier: Apache-2.0

//! `receive_batch` under `CryptoFailureAction::Fail` — differential equivalence (ADR-0024 layer d).
//!
//! Companion to `crypto_failure_action_equivalence.rs`, which proves the three PIP-4 arms for a
//! SINGLE undecryptable message surfaced through `receive()`. This file proves the batched
//! receive path, where the failing member is NOT the first one: the call has already popped good
//! messages into its accumulator when a later member fails post-processing.
//!
//! ## The scenario
//!
//! Three messages are published with a [`StampOnlyEncryptor`] (the PIP-4 encryption metadata is
//! stamped, the payload goes through unchanged), so the consumer enters its decrypt path for
//! each of them. The consumer's [`KeepPrefixDecryptor`] accepts the payloads that start with
//! `keep` and refuses the one that starts with `poison`:
//!
//! | entry | payload    | decryptor |
//! | ----- | ---------- | --------- |
//! | 0     | `keep-0`   | accepts   |
//! | 1     | `keep-1`   | accepts   |
//! | 2     | `poison-2` | refuses   |
//!
//! The consumer waits until all three sit in its receiver queue, then calls `receive_batch` a
//! bounded number of times. The failure is a `Fail`-arm decrypt error, so it must reach the
//! caller, and the two good messages must reach the caller too: popping them from the receiver
//! queue transferred their ownership to the call, and the broker will not redeliver them until a
//! reconnect or an ack timeout.
//!
//! The assertion is deliberately not tied to one way of surfacing the failure (the good prefix
//! returned with the error deferred to the next call, or the good messages left in the queue):
//! whichever calls deliver them, `keep-0` and `keep-1` must be among the delivered payloads, in
//! order, and the error must be surfaced by some call.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, CryptoFailureAction, SubscribeRequest, pb,
};

/// Producer-side stub: stamps the PIP-4 encryption metadata so the message is flagged as
/// encrypted on the wire, and returns the payload unchanged.
#[derive(Debug, Default)]
struct StampOnlyEncryptor;

/// Consumer-side stub: accepts a payload that starts with `keep`, refuses every other one.
#[derive(Debug, Default)]
struct KeepPrefixDecryptor;

impl magnetar_proto::MessageEncryptor for StampOnlyEncryptor {
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_proto::EncryptError> {
        metadata.encryption_keys.push(pb::EncryptionKeys {
            key: "rbcf-test".to_owned(),
            value: Bytes::from_static(b"k"),
            metadata: Vec::new(),
        });
        metadata.encryption_algo = Some("RBCF-TEST".to_owned());
        metadata.encryption_param = Some(Bytes::from_static(b"iv"));
        Ok(Bytes::copy_from_slice(plaintext))
    }
}

impl magnetar_proto::MessageDecryptor for KeepPrefixDecryptor {
    fn decrypt(
        &self,
        ciphertext: &[u8],
        _metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_proto::EncryptError> {
        if ciphertext.starts_with(b"keep") {
            Ok(Bytes::copy_from_slice(ciphertext))
        } else {
            Err(magnetar_proto::EncryptError::new(
                "decryptor refuses a non-keep payload",
            ))
        }
    }
}

const TOPIC: &str = "persistent://public/default/rbcf-later-member";
const SUBSCRIPTION: &str = "rbcf-sub";
const PAYLOADS: [&[u8]; 3] = [b"keep-0", b"keep-1", b"poison-2"];

/// Every `receive_batch` call the scenario makes: the first one has the whole queue to drain, the
/// follow-ups drain whatever the first one left and surface a deferred error.
const CALL_WAITS_MS: [u64; 4] = [2_000, 200, 200, 200];

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        topic: TOPIC.to_owned(),
        subscription: SUBSCRIPTION.to_owned(),
        receiver_queue_size: 16,
        durable: true,
        crypto_failure_action: CryptoFailureAction::Fail,
        ..Default::default()
    }
}

/// What the caller of `receive_batch` observed across the bounded call sequence.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Payloads handed to the caller, in delivery order.
    delivered: Vec<String>,
    /// Whether any call returned the `Fail`-arm decrypt error.
    error_surfaced: bool,
}

impl Observed {
    fn record(
        &mut self,
        result: Result<Vec<magnetar_proto::IncomingMessage>, impl std::fmt::Debug>,
    ) {
        match result {
            Ok(batch) => self.delivered.extend(
                batch
                    .iter()
                    .map(|m| String::from_utf8_lossy(&m.payload).into_owned()),
            ),
            Err(_) => self.error_surfaced = true,
        }
    }
}

/// Bounded wait for the scripted broker's three pushes to land in the consumer's queue, so the
/// first `receive_batch` call sees the whole queue regardless of how the frames were segmented.
async fn wait_until_queued(available_in_queue: impl Fn() -> usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while available_in_queue() < PAYLOADS.len() {
        assert!(
            Instant::now() < deadline,
            "the broker's {} messages never reached the consumer's receiver queue",
            PAYLOADS.len(),
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn run_tokio(pulsar_url: &str) -> Observed {
    use magnetar_runtime_tokio::{Client, MessageDecryptor, MessageEncryptor};

    let client = Client::connect(pulsar_url, ConnectionConfig::default())
        .await
        .expect("tokio connect");
    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: TOPIC.to_owned(),
                ..Default::default()
            },
            Some(Arc::new(StampOnlyEncryptor) as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("tokio open_producer_with");
    for payload in PAYLOADS {
        producer.send(outgoing(payload)).await.expect("tokio send");
    }
    let consumer = client
        .subscribe_with(
            subscribe_request(),
            Some(Arc::new(KeepPrefixDecryptor) as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("tokio subscribe_with");
    wait_until_queued(|| consumer.available_in_queue()).await;

    let mut observed = Observed {
        delivered: Vec::new(),
        error_surfaced: false,
    };
    for wait_ms in CALL_WAITS_MS {
        observed.record(
            consumer
                .receive_batch(10, Duration::from_millis(wait_ms))
                .await,
        );
    }
    observed
}

async fn run_moonpool(host_port: &str) -> Observed {
    use magnetar_runtime_moonpool::{Client, MessageDecryptor, MessageEncryptor, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("moonpool connect_plain");
    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: TOPIC.to_owned(),
                ..Default::default()
            },
            Some(Arc::new(StampOnlyEncryptor) as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("moonpool open_producer_with");
    for payload in PAYLOADS {
        producer
            .send(outgoing(payload))
            .await
            .expect("moonpool send");
    }
    let consumer = client
        .subscribe_with(
            subscribe_request(),
            Some(Arc::new(KeepPrefixDecryptor) as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("moonpool subscribe_with");
    wait_until_queued(|| consumer.available_in_queue()).await;

    let mut observed = Observed {
        delivered: Vec::new(),
        error_surfaced: false,
    };
    for wait_ms in CALL_WAITS_MS {
        observed.record(
            consumer
                .receive_batch(10, Duration::from_millis(wait_ms))
                .await,
        );
    }
    client.close().await;
    observed
}

/// A `Fail`-arm decrypt error on a LATER batch member neither loses the good messages the same
/// call already popped nor goes unreported, and both engines agree on what the caller saw.
#[tokio::test(flavor = "current_thread")]
async fn receive_batch_fail_on_later_member_event_stream_parity() {
    let tokio_broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_observed = run_tokio(&tokio_broker.pulsar_url()).await;
    tokio_broker.shutdown().await;

    let moonpool_broker = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_observed = run_moonpool(&moonpool_broker.host_port()).await;
    moonpool_broker.shutdown().await;

    assert_eq!(
        tokio_observed, moonpool_observed,
        "engine outcomes diverged for receive_batch with a failing later member",
    );
    assert!(
        tokio_observed.error_surfaced,
        "the Fail-arm decrypt error must reach the caller: {tokio_observed:?}",
    );
    assert_eq!(
        tokio_observed.delivered,
        vec!["keep-0".to_owned(), "keep-1".to_owned()],
        "the good messages popped before the failing member were lost: {tokio_observed:?}",
    );
}
