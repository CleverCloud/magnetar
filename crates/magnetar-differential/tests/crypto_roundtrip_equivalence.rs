// SPDX-License-Identifier: Apache-2.0

//! PIP-4 message-crypto differential equivalence (ADR-0024 layer d).
//!
//! The tokio and moonpool engines now both ship the PIP-4 encrypt-on-send /
//! decrypt-on-receive bridge (`Producer::send` encrypts after compression;
//! `Consumer::receive` decrypts before delivery, honoring
//! `CryptoFailureAction`). This test drives an ENCRYPTED round-trip
//! (`open_producer_with(encryptor)` → send → `subscribe_with(decryptor)` →
//! receive → ack) through BOTH engines against the same scripted broker and
//! asserts their user-visible [`EventStream`]s agree.
//!
//! It is the programmatic differential test, NOT the golden-trace JSON (the
//! corrupt-ciphertext `cryptoFailureAction` golden matrix is a separate
//! follow-up phase). The scripted broker round-trips the producer's PIP-4
//! `encryption_keys` / `encryption_algo` / `encryption_param` metadata
//! verbatim (a real broker is opaque to PIP-4 — it is a client-side concern),
//! so the consumer-side decrypt path is reachable end-to-end.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, EventStream};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SubscribeRequest, pb};

const XOR_KEY: u8 = 0x5A;
const PLAINTEXT: &[u8] = b"differential-pip4-secret";

/// Deterministic, dependency-free PIP-4 crypto stub used on both engines.
/// XORs the payload with [`XOR_KEY`] and stamps the canonical encryption
/// metadata. Implements both runtimes' `MessageEncryptor` / `MessageDecryptor`
/// trait pairs so one `Arc<XorCrypto>` drives the tokio AND moonpool engines.
#[derive(Debug, Default)]
struct XorCrypto;

fn stamp(metadata: &mut pb::MessageMetadata) {
    metadata.encryption_keys.push(pb::EncryptionKeys {
        key: "xor-test".to_owned(),
        value: Bytes::from_static(b"k"),
        metadata: Vec::new(),
    });
    metadata.encryption_algo = Some("XOR-TEST".to_owned());
    metadata.encryption_param = Some(Bytes::from_static(b"iv"));
}

fn xor(bytes: &[u8]) -> Bytes {
    Bytes::from(bytes.iter().map(|b| b ^ XOR_KEY).collect::<Vec<u8>>())
}

// Both runtime crates re-export the same canonical `magnetar_proto::crypto` traits, so a single
// impl per (type, trait) satisfies tokio and moonpool simultaneously.
impl magnetar_proto::MessageEncryptor for XorCrypto {
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_proto::EncryptError> {
        stamp(metadata);
        Ok(xor(plaintext))
    }
}

impl magnetar_proto::MessageDecryptor for XorCrypto {
    fn decrypt(
        &self,
        ciphertext: &[u8],
        _metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_proto::EncryptError> {
        Ok(xor(ciphertext))
    }
}

fn topic() -> String {
    "persistent://public/default/crypto-equiv".to_owned()
}

fn outgoing() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(PLAINTEXT),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(PLAINTEXT.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Run the encrypted round-trip against the tokio engine.
async fn run_tokio(pulsar_url: &str) -> EventStream {
    use magnetar_runtime_tokio::{Client, MessageDecryptor, MessageEncryptor};

    let mut stream = EventStream::empty();
    let crypto = Arc::new(XorCrypto);
    let client = Client::connect(pulsar_url, ConnectionConfig::default())
        .await
        .expect("tokio connect");

    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: topic(),
                ..Default::default()
            },
            Some(crypto.clone() as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("tokio open_producer_with");

    match producer.send(outgoing()).await {
        Ok(message_id) => stream.push(Event::Sent { message_id }),
        Err(e) => stream.push(Event::SendError {
            kind: format!("{e}"),
        }),
    }

    let consumer = client
        .subscribe_with(
            SubscribeRequest {
                topic: topic(),
                subscription: "crypto-equiv-sub".to_owned(),
                receiver_queue_size: 16,
                durable: true,
                ..Default::default()
            },
            Some(crypto.clone() as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("tokio subscribe_with");

    match tokio::time::timeout(Duration::from_secs(5), consumer.receive()).await {
        Ok(Ok(msg)) => {
            let mid = msg.message_id;
            stream.push(Event::Received {
                payload: msg.payload.to_vec(),
                message_id: mid,
            });
            match consumer.ack(mid).await {
                Ok(()) => stream.push(Event::Acked),
                Err(e) => stream.push(Event::AckError {
                    kind: format!("{e}"),
                }),
            }
        }
        Ok(Err(_)) | Err(_) => stream.push(Event::RecvTimeout),
    }
    stream
}

/// Run the encrypted round-trip against the moonpool engine.
///
/// No [`tokio::task::LocalSet`] / `Kicker` pump is needed: moonpool main
/// (rev `3863d1d`) ships a `Send`-bound `TaskProvider` whose `spawn_task`
/// goes through `tokio::task::Builder::new().spawn(...)`, so the driver
/// task spawned inside `connect_plain` runs on the ambient tokio runtime
/// and a parked `consumer.receive()` is woken normally via the sans-io
/// waker slab.
async fn run_moonpool(host_port: &str) -> EventStream {
    use magnetar_runtime_moonpool::{Client, MessageDecryptor, MessageEncryptor, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let mut stream = EventStream::empty();
    let crypto = Arc::new(XorCrypto);
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("moonpool connect_plain");

    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: topic(),
                ..Default::default()
            },
            Some(crypto.clone() as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("moonpool open_producer_with");

    match producer.send(outgoing()).await {
        Ok(message_id) => stream.push(Event::Sent { message_id }),
        Err(e) => stream.push(Event::SendError {
            kind: format!("{e}"),
        }),
    }

    let consumer = client
        .subscribe_with(
            SubscribeRequest {
                topic: topic(),
                subscription: "crypto-equiv-sub".to_owned(),
                receiver_queue_size: 16,
                durable: true,
                ..Default::default()
            },
            Some(crypto.clone() as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("moonpool subscribe_with");

    match tokio::time::timeout(Duration::from_secs(5), consumer.receive()).await {
        Ok(Ok(msg)) => {
            let mid = msg.message_id;
            stream.push(Event::Received {
                payload: msg.payload.to_vec(),
                message_id: mid,
            });
            match consumer.ack(mid).await {
                Ok(()) => stream.push(Event::Acked),
                Err(e) => stream.push(Event::AckError {
                    kind: format!("{e}"),
                }),
            }
        }
        Ok(Err(_)) | Err(_) => stream.push(Event::RecvTimeout),
    }

    stream
}

/// The two engines produce byte-identical event streams for an encrypted
/// send → receive → ack round-trip. The decrypted payload must equal the
/// original plaintext on both engines, and the broker-assigned message ids
/// must match. A divergence here means one engine's PIP-4 bridge handled the
/// encrypt/decrypt ordering or metadata stamping differently.
#[tokio::test(flavor = "current_thread")]
async fn encrypted_roundtrip_event_stream_parity() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = run_tokio(&pulsar_url).await;
    let moonpool_stream = run_moonpool(&host_port).await;

    // Sanity: the decrypted payload round-trips to the original plaintext on
    // the tokio side (the equality assert below extends this to moonpool).
    assert!(
        matches!(tokio_stream.events.first(), Some(Event::Sent { .. })),
        "tokio send must succeed, got {:?}",
        tokio_stream.events.first()
    );
    if let Some(Event::Received { payload, .. }) = tokio_stream.events.get(1) {
        assert_eq!(
            payload.as_slice(),
            PLAINTEXT,
            "tokio consumer must decrypt back to the original plaintext"
        );
    } else {
        panic!(
            "tokio stream missing a decrypted Received event: {:?}",
            tokio_stream.events
        );
    }

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the encrypted round-trip",
    );

    broker.shutdown().await;
}
