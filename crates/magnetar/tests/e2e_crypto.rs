// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for PIP-4 end-to-end encryption against a real Apache
//! Pulsar 4.x standalone broker spun up via `testcontainers-rs`.
//!
//! Gated behind both `e2e` and `encryption` features. Run with:
//!
//! ```sh
//! cargo test --features e2e,encryption -p magnetar --test e2e_crypto -- --nocapture
//! ```
//!
//! Requires Docker on the host. The broker is opaque to encryption — PIP-4 is
//! a client-side concern — so we reuse the standard `apachepulsar/pulsar:4.0.4`
//! fixture from `e2e_pulsar.rs` and exercise:
//!
//! 1. Happy-path encrypt/decrypt round trip with a shared `MessageCrypto`.
//! 2. `CryptoFailureAction::Fail` — consumer surfaces a decrypt error.
//! 3. `CryptoFailureAction::Discard` — undecryptable messages are silently acked + skipped; a
//!    follow-up plaintext message still flows.
//! 4. `CryptoFailureAction::Consume` — ciphertext is handed back as-is and bytes don't match the
//!    plaintext.
//! 5. PIP-4 + PIP-37 cross-feature: a > `max_message_size` payload that is both chunked AND
//!    encrypted reassembles + decrypts on the consumer.

#![cfg(all(feature = "e2e", feature = "encryption"))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeySize, PrivateDecryptingKey};
use bytes::Bytes;
use magnetar::proto::conn::CryptoFailureAction;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::runtime_tokio::{EncryptError, MessageDecryptor, MessageEncryptor};
use magnetar::{MessageCryptoBridge, OutgoingMessage, PulsarClient};
use magnetar_messagecrypto::{CryptoError, CryptoKeyReader, KeyInfo, MessageCrypto};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Start a Pulsar 4.x standalone container. Mirror of the helper in
/// `e2e_pulsar.rs` / `e2e_batch_chunk.rs`; duplicated rather than re-exported
/// because integration-test crates do not share modules.
async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout("messaging service is ready"))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

fn fresh_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// In-memory `CryptoKeyReader` for the e2e suite. Mirrors the `pub(crate)`
/// `test_support::InMemoryKeyReader` inside `magnetar-messagecrypto`, which is
/// not reachable from integration tests.
#[derive(Debug)]
struct InMemoryKeyReader {
    // name -> (public_der_spki, private_pkcs8)
    keys: Mutex<HashMap<String, (Bytes, Bytes)>>,
}

impl InMemoryKeyReader {
    fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }

    fn add_named(&self, name: &str) {
        let priv_key = PrivateDecryptingKey::generate(KeySize::Rsa2048).expect("rsa keygen");
        let pub_key = priv_key.public_key();
        let pub_der = pub_key.as_der().expect("der public");
        let priv_der = priv_key.as_der().expect("der private");
        self.keys.lock().unwrap().insert(
            name.to_owned(),
            (
                Bytes::copy_from_slice(pub_der.as_ref()),
                Bytes::copy_from_slice(priv_der.as_ref()),
            ),
        );
    }
}

impl CryptoKeyReader for InMemoryKeyReader {
    fn public_key(&self, key_name: &str) -> Result<KeyInfo, CryptoError> {
        let map = self.keys.lock().unwrap();
        let (pub_bytes, _) = map
            .get(key_name)
            .ok_or_else(|| CryptoError::PublicKeyLookup(key_name.to_owned(), "not found".into()))?;
        Ok(KeyInfo {
            key_name: key_name.to_owned(),
            key_value: pub_bytes.clone(),
            metadata: vec![],
        })
    }

    fn private_key(&self, key_name: &str) -> Result<KeyInfo, CryptoError> {
        let map = self.keys.lock().unwrap();
        let (_, priv_bytes) = map.get(key_name).ok_or_else(|| {
            CryptoError::PrivateKeyLookup(key_name.to_owned(), "not found".into())
        })?;
        Ok(KeyInfo {
            key_name: key_name.to_owned(),
            key_value: priv_bytes.clone(),
            metadata: vec![],
        })
    }
}

/// Decryptor stub that always fails — used to exercise the three
/// `CryptoFailureAction` policies independently of whatever the real backend
/// would do. Mirrors the `MessageDecryptor` trait so the consumer treats it
/// just like a misconfigured key reader.
#[derive(Debug, Default)]
struct AlwaysFailDecryptor;

impl MessageDecryptor for AlwaysFailDecryptor {
    fn decrypt(
        &self,
        _ciphertext: &[u8],
        _metadata: &magnetar::proto::pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError> {
        Err(EncryptError::new("forced decrypt failure (test)"))
    }
}

fn make_crypto(key_name: &str) -> (Arc<MessageCrypto>, Arc<InMemoryKeyReader>) {
    let reader = Arc::new(InMemoryKeyReader::new());
    reader.add_named(key_name);
    let crypto = Arc::new(
        MessageCrypto::new(reader.clone(), vec![key_name.to_owned()]).expect("crypto init"),
    );
    (crypto, reader)
}

fn bridge_for(crypto: Arc<MessageCrypto>) -> Arc<MessageCryptoBridge> {
    Arc::new(MessageCryptoBridge::new(crypto))
}

/// (1) Happy path — same `MessageCrypto` on both sides round-trips plaintext.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_crypto_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let (crypto, _reader) = make_crypto("alice");
    let bridge = bridge_for(crypto);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("magnetar-e2e-crypto-rt");

    let producer = client
        .producer(&topic)
        .encryption(bridge.clone() as Arc<dyn MessageEncryptor>)
        .create()
        .await?;
    let payload = b"hello PIP-4 world".to_vec();
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-crypto-rt")
        .subscription_type(SubType::Exclusive)
        .encryption(bridge.clone() as Arc<dyn MessageDecryptor>)
        .subscribe()
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive()).await??;
    assert_eq!(msg.payload.as_ref(), payload.as_slice());
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// (2) `CryptoFailureAction::Fail` — consumer surfaces a decrypt error when
/// the configured decryptor refuses every message.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_crypto_failure_action_fail() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    // Producer encrypts with a real bridge; consumer plugs in `AlwaysFailDecryptor`.
    let (crypto, _reader) = make_crypto("alice");
    let prod_bridge = bridge_for(crypto);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("magnetar-e2e-crypto-fail");

    let producer = client
        .producer(&topic)
        .encryption(prod_bridge as Arc<dyn MessageEncryptor>)
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(b"opaque-payload".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-crypto-fail")
        .subscription_type(SubType::Exclusive)
        .encryption(Arc::new(AlwaysFailDecryptor) as Arc<dyn MessageDecryptor>)
        .crypto_failure_action(CryptoFailureAction::Fail)
        .subscribe()
        .await?;

    let result = tokio::time::timeout(Duration::from_secs(30), consumer.receive()).await?;
    assert!(
        result.is_err(),
        "expected receive() to surface a decrypt error under CryptoFailureAction::Fail; got {result:?}"
    );
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// (3) `CryptoFailureAction::Discard` — undecryptable messages are silently
/// acked + skipped. Then a non-encrypted message still flows through.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_crypto_failure_action_discard() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let (crypto, _reader) = make_crypto("alice");
    let prod_bridge = bridge_for(crypto);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("magnetar-e2e-crypto-discard");

    // Subscribe first so the broker tracks the cursor from message 0.
    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-crypto-discard")
        .subscription_type(SubType::Exclusive)
        .encryption(Arc::new(AlwaysFailDecryptor) as Arc<dyn MessageDecryptor>)
        .crypto_failure_action(CryptoFailureAction::Discard)
        .subscribe()
        .await?;

    // First: send an encrypted message that the consumer will fail to decrypt
    // and (per Discard policy) silently drop.
    let enc_producer = client
        .producer(&topic)
        .encryption(prod_bridge as Arc<dyn MessageEncryptor>)
        .create()
        .await?;
    enc_producer
        .send(OutgoingMessage::with_payload(b"undecryptable".to_vec()).into())
        .await?;
    enc_producer.close().await?;

    // `receive_with_timeout` returns `Ok(None)` because the encrypted message
    // was acked and discarded internally.
    let discarded = consumer
        .receive_with_timeout(Duration::from_secs(5))
        .await?;
    assert!(
        discarded.is_none(),
        "expected encrypted message to be discarded silently, got {discarded:?}"
    );

    // Then: send a plaintext message and confirm it still flows.
    let plain_producer = client.producer(&topic).create().await?;
    plain_producer
        .send(OutgoingMessage::with_payload(b"after-discard".to_vec()).into())
        .await?;
    plain_producer.close().await?;

    let next = tokio::time::timeout(Duration::from_secs(30), consumer.receive()).await??;
    assert_eq!(next.payload.as_ref(), b"after-discard");
    consumer.ack(next.message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// (4) `CryptoFailureAction::Consume` — ciphertext is handed back to the
/// caller untouched. We assert the bytes don't match the plaintext and the
/// `encryption_keys` metadata is preserved.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_crypto_failure_action_consume() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let (crypto, _reader) = make_crypto("alice");
    let prod_bridge = bridge_for(crypto);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("magnetar-e2e-crypto-consume");

    let producer = client
        .producer(&topic)
        .encryption(prod_bridge as Arc<dyn MessageEncryptor>)
        .create()
        .await?;
    let plaintext = b"distinctive-plaintext-payload-XYZ".to_vec();
    producer
        .send(OutgoingMessage::with_payload(plaintext.clone()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-crypto-consume")
        .subscription_type(SubType::Exclusive)
        .encryption(Arc::new(AlwaysFailDecryptor) as Arc<dyn MessageDecryptor>)
        .crypto_failure_action(CryptoFailureAction::Consume)
        .subscribe()
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive()).await??;
    assert_ne!(
        msg.payload.as_ref(),
        plaintext.as_slice(),
        "Consume policy must hand back the still-encrypted ciphertext, not the plaintext"
    );
    assert!(
        !msg.metadata.encryption_keys.is_empty(),
        "Consume policy must preserve `encryption_keys` so the caller can decrypt out-of-band"
    );
    assert_eq!(
        msg.metadata.encryption_algo.as_deref(),
        Some("AES_GCM_256"),
        "encryption metadata should round-trip alongside the ciphertext"
    );
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}

/// (5) PIP-4 + PIP-37 cross-feature: a payload larger than the broker's
/// default 5 MiB `max_message_size` is chunked AND encrypted. The consumer
/// must reassemble *and* decrypt it back to the original bytes.
///
/// Chunks-never-batched per PIP-37 — we explicitly disable batching even
/// though it's already off by default.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_crypto_with_chunking() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let (crypto, _reader) = make_crypto("alice");
    let bridge = bridge_for(crypto);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = fresh_topic("magnetar-e2e-crypto-chunk");

    let producer = client
        .producer(&topic)
        .chunking(true)
        .batching(0, 0)
        .encryption(bridge.clone() as Arc<dyn MessageEncryptor>)
        .create()
        .await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-crypto-chunk")
        .subscription_type(SubType::Exclusive)
        .encryption(bridge.clone() as Arc<dyn MessageDecryptor>)
        .subscribe()
        .await?;

    // ~6 MiB so the producer is forced to emit at least two chunks against
    // the broker's default 5 MiB max message size.
    let payload_size: usize = 6 * 1024 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let msg = tokio::time::timeout(Duration::from_secs(120), consumer.receive()).await??;
    assert_eq!(
        msg.payload.len(),
        payload_size,
        "reassembled + decrypted payload length mismatch"
    );
    assert_eq!(
        msg.payload.as_ref(),
        payload.as_slice(),
        "reassembled + decrypted payload bytes mismatch"
    );
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}
