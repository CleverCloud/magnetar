// SPDX-License-Identifier: Apache-2.0

//! PIP-4 `cryptoFailureAction` matrix — differential equivalence (ADR-0024 layer d).
//!
//! Companion to `crypto_roundtrip_equivalence.rs` (which proves the *happy*
//! encrypt → decrypt round-trip). This file proves the *failure* matrix: when
//! the broker delivers a message the consumer's decryptor REJECTS, each
//! [`magnetar_proto::CryptoFailureAction`] arm behaves identically across the
//! tokio and moonpool engines at the consumer surface.
//!
//! ## How the corrupt ciphertext is injected — deterministically
//!
//! The producer is opened with a [`StampOnlyEncryptor`]: it stamps the PIP-4
//! `encryption_keys` / `encryption_algo` / `encryption_param` metadata (so the
//! scripted broker round-trips it verbatim and the consumer enters its decrypt
//! path) but returns the payload bytes UNCHANGED. Those plaintext bytes are
//! therefore "intentionally-corrupt ciphertext" from the decryptor's point of
//! view. The consumer's [`AlwaysFailDecryptor`] rejects them unconditionally —
//! so the arm is exercised regardless of byte content, with zero crypto
//! dependency and full determinism. (This mirrors the `AlwaysFailDecryptor`
//! used by the per-runtime unit tests B1 landed.)
//!
//! ## What each arm surfaces (verified identical on both engines —
//! tokio `consumer.rs` `ReceiveFut::poll` and moonpool `consumer.rs`
//! `ReceiveFut::poll`):
//!
//! | Arm       | `receive()` outcome                          | normalized tag                |
//! | --------- | -------------------------------------------- | ----------------------------- |
//! | `Fail`    | `Err(ClientError::Other("decrypt: ..."))`    | `"Fail:RecvDecryptError"`     |
//! | `Discard` | message acked + dropped → next recv times out| `"Discard:RecvTimeout"`       |
//! | `Consume` | `Ok(msg)` carrying the CORRUPT ciphertext    | `"Consume:Received(corrupt)"` |
//!
//! A golden trace lives at `tests/golden/crypto_failure_action.json` —
//! human-reviewable, regenerated via `MAGNETAR_REGENERATE_GOLDEN=1`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, CryptoFailureAction, SubscribeRequest, pb,
};

/// The bytes the producer publishes. With [`StampOnlyEncryptor`] they reach the
/// consumer UNCHANGED but are tagged as encrypted, so the decryptor treats them
/// as ciphertext and (via [`AlwaysFailDecryptor`]) rejects them.
const CORRUPT_CIPHERTEXT: &[u8] = b"not-real-ciphertext";

/// Producer-side stub: stamps the PIP-4 encryption metadata so the message is
/// flagged as encrypted on the wire, but returns the payload unchanged. The
/// "ciphertext" is therefore deterministic and dependency-free.
#[derive(Debug, Default)]
struct StampOnlyEncryptor;

/// Consumer-side stub: rejects every payload, exercising the
/// `CryptoFailureAction` arm whatever the bytes are.
#[derive(Debug, Default)]
struct AlwaysFailDecryptor;

fn stamp(metadata: &mut pb::MessageMetadata) {
    metadata.encryption_keys.push(pb::EncryptionKeys {
        key: "cfa-test".to_owned(),
        value: Bytes::from_static(b"k"),
        metadata: Vec::new(),
    });
    metadata.encryption_algo = Some("CFA-TEST".to_owned());
    metadata.encryption_param = Some(Bytes::from_static(b"iv"));
}

impl magnetar_runtime_tokio::MessageEncryptor for StampOnlyEncryptor {
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_runtime_tokio::EncryptError> {
        stamp(metadata);
        Ok(Bytes::copy_from_slice(plaintext))
    }
}

impl magnetar_runtime_moonpool::MessageEncryptor for StampOnlyEncryptor {
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_runtime_moonpool::EncryptError> {
        stamp(metadata);
        Ok(Bytes::copy_from_slice(plaintext))
    }
}

impl magnetar_runtime_tokio::MessageDecryptor for AlwaysFailDecryptor {
    fn decrypt(
        &self,
        _ciphertext: &[u8],
        _metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_runtime_tokio::EncryptError> {
        Err(magnetar_runtime_tokio::EncryptError::new(
            "decryptor rejects corrupt ciphertext",
        ))
    }
}

impl magnetar_runtime_moonpool::MessageDecryptor for AlwaysFailDecryptor {
    fn decrypt(
        &self,
        _ciphertext: &[u8],
        _metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, magnetar_runtime_moonpool::EncryptError> {
        Err(magnetar_runtime_moonpool::EncryptError::new(
            "decryptor rejects corrupt ciphertext",
        ))
    }
}

/// The three matrix arms, in a fixed order so the event stream is stable.
const ARMS: [(CryptoFailureAction, &str); 3] = [
    (CryptoFailureAction::Fail, "fail"),
    (CryptoFailureAction::Discard, "discard"),
    (CryptoFailureAction::Consume, "consume"),
];

fn topic(arm: &str) -> String {
    format!("persistent://public/default/cfa-{arm}")
}

fn outgoing() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(CORRUPT_CIPHERTEXT),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(CORRUPT_CIPHERTEXT.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Normalize a single arm's consumer-surface outcome into a stable tag.
/// `recv` is the result of `receive()` (already timeout-wrapped) and `arm` the
/// human-readable arm name used in the tag prefix.
fn classify_outcome(
    arm: &str,
    payload_on_consume: Option<Vec<u8>>,
    decrypt_failed: bool,
) -> String {
    match (payload_on_consume, decrypt_failed) {
        // Consume: a message was delivered carrying the corrupt ciphertext.
        (Some(payload), _) => {
            let verbatim = payload.as_slice() == CORRUPT_CIPHERTEXT;
            format!("{arm}:Received(corrupt={verbatim})")
        }
        // Fail: receive surfaced a decrypt error.
        (None, true) => format!("{arm}:RecvDecryptError"),
        // Discard: the message was acked + dropped, so receive timed out with
        // nothing delivered (and no decrypt error bubbled to the surface).
        (None, false) => format!("{arm}:RecvTimeout"),
    }
}

/// Drive `send → receive` for one arm against the tokio engine, returning the
/// normalized outcome tag.
async fn run_tokio_arm(pulsar_url: &str, action: CryptoFailureAction, arm: &str) -> String {
    use magnetar_runtime_tokio::{Client, MessageDecryptor, MessageEncryptor};

    let encryptor = Arc::new(StampOnlyEncryptor);
    let decryptor = Arc::new(AlwaysFailDecryptor);
    let client = Client::connect(pulsar_url, ConnectionConfig::default())
        .await
        .expect("tokio connect");

    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: topic(arm),
                ..Default::default()
            },
            Some(encryptor as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("tokio open_producer_with");
    producer.send(outgoing()).await.expect("tokio send");

    let consumer = client
        .subscribe_with(
            SubscribeRequest {
                topic: topic(arm),
                subscription: format!("cfa-sub-{arm}"),
                receiver_queue_size: 16,
                durable: true,
                crypto_failure_action: action,
                ..Default::default()
            },
            Some(decryptor as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("tokio subscribe_with");

    // A short timeout: the `Discard` arm legitimately delivers nothing (the
    // message is acked + dropped), so we must not wait the full happy-path
    // window. `Fail` and `Consume` resolve immediately on first delivery.
    match tokio::time::timeout(Duration::from_secs(2), consumer.receive()).await {
        Ok(Ok(msg)) => classify_outcome(arm, Some(msg.payload.to_vec()), false),
        Ok(Err(_)) => classify_outcome(arm, None, true),
        Err(_) => classify_outcome(arm, None, false),
    }
}

/// Drive `send → receive` for one arm against the moonpool engine.
///
/// No [`tokio::task::LocalSet`] / `Kicker` pump: moonpool main (rev `3863d1d`)
/// spawns the driver via a `Send`-bound `TaskProvider`, so a parked
/// `consumer.receive()` is woken normally on the ambient runtime.
async fn run_moonpool_arm(host_port: &str, action: CryptoFailureAction, arm: &str) -> String {
    use magnetar_runtime_moonpool::{Client, MessageDecryptor, MessageEncryptor, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let encryptor = Arc::new(StampOnlyEncryptor);
    let decryptor = Arc::new(AlwaysFailDecryptor);
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("moonpool connect_plain");

    let producer = client
        .open_producer_with(
            CreateProducerRequest {
                topic: topic(arm),
                ..Default::default()
            },
            Some(encryptor as Arc<dyn MessageEncryptor>),
        )
        .await
        .expect("moonpool open_producer_with");
    producer.send(outgoing()).await.expect("moonpool send");

    let consumer = client
        .subscribe_with(
            SubscribeRequest {
                topic: topic(arm),
                subscription: format!("cfa-sub-{arm}"),
                receiver_queue_size: 16,
                durable: true,
                crypto_failure_action: action,
                ..Default::default()
            },
            Some(decryptor as Arc<dyn MessageDecryptor>),
        )
        .await
        .expect("moonpool subscribe_with");

    let tag = match tokio::time::timeout(Duration::from_secs(2), consumer.receive()).await {
        Ok(Ok(msg)) => classify_outcome(arm, Some(msg.payload.to_vec()), false),
        Ok(Err(_)) => classify_outcome(arm, None, true),
        Err(_) => classify_outcome(arm, None, false),
    };
    client.close().await;
    tag
}

/// Run the full 3-arm matrix against one engine, one fresh broker per arm so
/// cursor / subscription state never leaks across arms.
async fn run_matrix_tokio() -> Vec<String> {
    let mut tags = Vec::new();
    for (action, arm) in ARMS {
        let broker = ScriptedBroker::bind().await.expect("broker bind");
        tags.push(run_tokio_arm(&broker.pulsar_url(), action, arm).await);
        broker.shutdown().await;
    }
    tags
}

async fn run_matrix_moonpool() -> Vec<String> {
    let mut tags = Vec::new();
    for (action, arm) in ARMS {
        let broker = ScriptedBroker::bind().await.expect("broker bind");
        tags.push(run_moonpool_arm(&broker.host_port(), action, arm).await);
        broker.shutdown().await;
    }
    tags
}

/// The `cryptoFailureAction` matrix is differential-equivalent: for an
/// undecryptable (corrupt-ciphertext) delivery, the tokio and moonpool engines
/// surface identical consumer-side behavior on every arm. Also validates the
/// human-reviewable golden trace.
#[tokio::test(flavor = "current_thread")]
async fn crypto_failure_action_matrix_event_stream_parity() {
    let tokio_tags = run_matrix_tokio().await;
    let moonpool_tags = run_matrix_moonpool().await;

    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the cryptoFailureAction matrix",
    );

    // Sanity: each arm produced its expected shape (proves the matrix actually
    // exercised the three distinct branches, not three identical timeouts).
    assert_eq!(
        tokio_tags,
        vec![
            "fail:RecvDecryptError".to_owned(),
            "discard:RecvTimeout".to_owned(),
            "consume:Received(corrupt=true)".to_owned(),
        ],
        "matrix arms did not produce the expected per-arm outcomes",
    );

    // Golden trace — human-reviewable, regenerated via MAGNETAR_REGENERATE_GOLDEN=1.
    let golden_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/crypto_failure_action.json");
    let expected = "[\
\n  \"fail:RecvDecryptError\",\
\n  \"discard:RecvTimeout\",\
\n  \"consume:Received(corrupt=true)\"\
\n]\n";
    if std::env::var_os("MAGNETAR_REGENERATE_GOLDEN").is_some() {
        std::fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
        std::fs::write(&golden_path, expected).unwrap();
    }
    let actual = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|_| panic!("golden file missing at {golden_path:?}"));
    assert_eq!(
        actual.trim(),
        expected.trim(),
        "PIP-4 cryptoFailureAction golden trace drift — regenerate via MAGNETAR_REGENERATE_GOLDEN=1"
    );
}
