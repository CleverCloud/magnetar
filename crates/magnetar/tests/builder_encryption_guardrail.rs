// SPDX-License-Identifier: Apache-2.0

//! BREAKING-CHANGE guardrail: `ProducerBuilder::create()` /
//! `ConsumerBuilder::subscribe()` must refuse a configured PIP-4 encryptor /
//! decryptor instead of silently opening a plaintext producer / consumer.
//!
//! Before this guardrail, calling `.encryption(enc).create()` on the
//! engine-generic dispatch path would silently drop the encryptor (the
//! engine-typed slot didn't thread through `CreateProducerApi`), so callers
//! got a plaintext producer where they expected ciphertext. This file pins
//! the new behaviour: the engine-generic terminal calls now return
//! [`magnetar::PulsarError::Other`] when the encryptor / decryptor slot is
//! populated, with a message pointing at the per-engine
//! `*_with_encryption()` / `*_with_decryption()` specialisations.
//!
//! Two engines, two tests apiece for the engine-generic
//! `ProducerBuilder` / `ConsumerBuilder` (4 tests), plus four
//! companion guardrails on the remaining engine-generic builders that
//! still expose a per-engine `encryption()` setter:
//! `PartitionedProducerBuilder::create`, `TableViewBuilder::create`,
//! `TypedTableViewBuilder::create`, and the tokio-only
//! `TypedProducerBuilder::create`. The latter four only have a tokio
//! `encryption()` setter today, so only the tokio guardrail path is
//! exercised — the moonpool side of those builders can't even
//! populate the encryptor/decryptor slot via the public API.
//!
//! ADR-0024 four-layer policy: this file lives at the façade tier; the
//! runtime tiers stay unchanged (the guardrail is a façade-only
//! short-circuit), the proto tier is unaffected (no wire change), and the
//! differential tier is N/A (no tokio ↔ moonpool divergence — both engines
//! short-circuit identically).
//!
//! Both engines spin up a minimal TCP fake-broker that answers CONNECT and
//! then sits idle; the guardrail fires before any `CommandProducer` /
//! `CommandSubscribe` would reach the wire, so the broker never has to
//! respond beyond the handshake.

#![cfg(all(feature = "tokio", feature = "encryption"))]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{MessageCryptoBridge, PulsarClient, PulsarError};
use magnetar_messagecrypto::{CryptoKeyReader, KeyInfo, MessageCrypto};
#[cfg(feature = "moonpool")]
use magnetar_proto::ConnectionConfig;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Build a real `MessageCrypto` from a freshly-generated RSA-2048 keypair.
/// The keypair stays inside this test binary — the guardrail under test
/// fires before any `encrypt()` / `decrypt()` is ever called, but
/// `MessageCrypto::new` wraps the data key against each named recipient at
/// construction time, so we hand it a real DER-encoded RSA key anyway.
fn build_bridge() -> Arc<MessageCryptoBridge> {
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeySize, PrivateDecryptingKey};

    let priv_key =
        PrivateDecryptingKey::generate(KeySize::Rsa2048).expect("generate RSA-2048 priv key");
    let pub_key = priv_key.public_key();
    let priv_der = priv_key.as_der().expect("priv-key DER").as_ref().to_vec();
    let pub_der = pub_key.as_der().expect("pub-key DER").as_ref().to_vec();

    let reader = Arc::new(RealKeyReader {
        priv_der: Bytes::from(priv_der),
        pub_der: Bytes::from(pub_der),
    });
    let crypto = Arc::new(
        MessageCrypto::new(reader, vec!["test-key".to_owned()]).expect("MessageCrypto init"),
    );
    Arc::new(MessageCryptoBridge::new(crypto))
}

#[derive(Debug)]
struct RealKeyReader {
    priv_der: Bytes,
    pub_der: Bytes,
}

impl CryptoKeyReader for RealKeyReader {
    fn public_key(&self, key_name: &str) -> Result<KeyInfo, magnetar_messagecrypto::CryptoError> {
        Ok(KeyInfo {
            key_name: key_name.to_owned(),
            key_value: self.pub_der.clone(),
            metadata: vec![],
        })
    }

    fn private_key(&self, key_name: &str) -> Result<KeyInfo, magnetar_messagecrypto::CryptoError> {
        Ok(KeyInfo {
            key_name: key_name.to_owned(),
            key_value: self.priv_der.clone(),
            metadata: vec![],
        })
    }
}

/// Spawn a TCP fake-broker that answers a single `CommandConnect` with
/// `CommandConnected` and then idles. Returns the broker's host:port so the
/// façade can dial it. The broker never has to respond to a
/// `CommandProducer` / `CommandSubscribe` because the guardrail short-
/// circuits before either reaches the wire.
async fn spawn_fake_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut read_buf = BytesMut::with_capacity(8 * 1024);
                let mut out_buf = BytesMut::with_capacity(8 * 1024);
                loop {
                    loop {
                        let mut framed = Bytes::copy_from_slice(&read_buf);
                        let before = framed.len();
                        let frame = match decode_one(&mut framed) {
                            Ok(f) => f,
                            Err(FrameError::Incomplete { .. }) => break,
                            Err(_) => return,
                        };
                        let consumed = before - framed.len();
                        let _ = read_buf.split_to(consumed);
                        if frame.command.r#type == pb::base_command::Type::Connect as i32 {
                            let connected = pb::BaseCommand {
                                r#type: pb::base_command::Type::Connected as i32,
                                connected: Some(pb::CommandConnected {
                                    server_version: "magnetar-guardrail-fake".to_owned(),
                                    protocol_version: Some(21),
                                    max_message_size: Some(5 * 1024 * 1024),
                                    feature_flags: Some(pb::FeatureFlags::default()),
                                }),
                                ..Default::default()
                            };
                            encode_command(&mut out_buf, &connected)
                                .expect("encode CommandConnected");
                        }
                    }
                    if !out_buf.is_empty() {
                        if stream.write_all(&out_buf).await.is_err() {
                            return;
                        }
                        if stream.flush().await.is_err() {
                            return;
                        }
                        out_buf.clear();
                    }
                    match stream.read_buf(&mut read_buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    addr
}

// ---------------------------------------------------------------------------
// Tokio engine — 2 tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_producer_create_refuses_configured_encryptor() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .producer("persistent://public/default/guardrail-tokio-prod")
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageEncryptor>)
        .create()
        .await
        .expect_err("create() must refuse a configured encryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("create_with_encryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_consumer_subscribe_refuses_configured_decryptor() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .consumer("persistent://public/default/guardrail-tokio-cons")
        .subscription("guardrail")
        .subscription_type(SubType::Exclusive)
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageDecryptor>)
        .subscribe()
        .await
        .expect_err("subscribe() must refuse a configured decryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("subscribe_with_decryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}

// ---------------------------------------------------------------------------
// Moonpool engine — 2 tests (gated on the `moonpool` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moonpool_producer_create_refuses_configured_encryptor() {
    // Façade engine marker (zero-sized PhantomData) — distinct from the
    // runtime's `MoonpoolEngine` which holds the actual `Providers` bundle.
    use magnetar::MoonpoolEngine as FacadeMoonpoolEngine;
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine as RuntimeMoonpoolEngine};
    use moonpool_core::TokioProviders;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr = spawn_fake_broker().await;
            let engine = RuntimeMoonpoolEngine::new(TokioProviders::new());
            let runtime_client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");
            let client = PulsarClient::<FacadeMoonpoolEngine<TokioProviders>>::from_runtime_client(
                runtime_client,
            );

            let bridge = build_bridge();
            // The moonpool engine ships its own `MessageEncryptor` trait. The
            // `MessageCryptoBridge` impls both (tokio + moonpool) under the
            // `encryption` feature so the same handle plugs into either side.
            let err = client
                .producer("persistent://public/default/guardrail-moonpool-prod")
                .encryption(bridge.clone() as Arc<dyn magnetar::runtime_moonpool::MessageEncryptor>)
                .create()
                .await
                .expect_err("create() must refuse a configured encryptor");
            let msg = match err {
                PulsarError::Other(s) => s,
                other => panic!("expected PulsarError::Other, got {other:?}"),
            };
            assert!(
                msg.contains("create_with_encryption"),
                "guardrail error must point at the per-engine specialisation, got {msg:?}"
            );
            client.close().await;
        })
        .await;
}

#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moonpool_consumer_subscribe_refuses_configured_decryptor() {
    // Façade engine marker (zero-sized PhantomData) — distinct from the
    // runtime's `MoonpoolEngine` which holds the actual `Providers` bundle.
    use magnetar::MoonpoolEngine as FacadeMoonpoolEngine;
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine as RuntimeMoonpoolEngine};
    use moonpool_core::TokioProviders;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr = spawn_fake_broker().await;
            let engine = RuntimeMoonpoolEngine::new(TokioProviders::new());
            let runtime_client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");
            let client = PulsarClient::<FacadeMoonpoolEngine<TokioProviders>>::from_runtime_client(
                runtime_client,
            );

            let bridge = build_bridge();
            let err = client
                .consumer("persistent://public/default/guardrail-moonpool-cons")
                .subscription("guardrail")
                .subscription_type(SubType::Exclusive)
                .encryption(bridge.clone() as Arc<dyn magnetar::runtime_moonpool::MessageDecryptor>)
                .subscribe()
                .await
                .expect_err("subscribe() must refuse a configured decryptor");
            let msg = match err {
                PulsarError::Other(s) => s,
                other => panic!("expected PulsarError::Other, got {other:?}"),
            };
            assert!(
                msg.contains("subscribe_with_decryption"),
                "guardrail error must point at the per-engine specialisation, got {msg:?}"
            );
            client.close().await;
        })
        .await;
}

// ---------------------------------------------------------------------------
// Companion guardrails on the remaining engine-generic builders.
// These four builders only expose a tokio `encryption()` setter today;
// the moonpool side cannot populate the encryptor/decryptor slot via the
// public API, so the moonpool guardrail path is unreachable and not
// exercised here (the check itself is still correct on both engines).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_partitioned_producer_create_refuses_configured_encryptor() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .partitioned_producer("persistent://public/default/guardrail-tokio-pprod")
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageEncryptor>)
        .create()
        .await
        .expect_err("create() must refuse a configured encryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("create_with_encryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_table_view_create_refuses_configured_decryptor() {
    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .table_view("persistent://public/default/guardrail-tokio-tv")
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageDecryptor>)
        .create()
        .await
        .expect_err("create() must refuse a configured decryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("create_with_decryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_typed_table_view_create_refuses_configured_decryptor() {
    use magnetar_proto::schema::StringSchema;

    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .typed_table_view(
            "persistent://public/default/guardrail-tokio-ttv",
            Arc::new(StringSchema::new()),
        )
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageDecryptor>)
        .create()
        .await
        .expect_err("create() must refuse a configured decryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("create_with_decryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_typed_producer_create_refuses_configured_encryptor() {
    use magnetar_proto::schema::StringSchema;

    let addr = spawn_fake_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        PulsarClient::builder()
            .service_url(format!("pulsar://{addr}"))
            .build(),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let bridge = build_bridge();
    let err = client
        .typed_producer(
            "persistent://public/default/guardrail-tokio-tprod",
            Arc::new(StringSchema::new()),
        )
        .encryption(bridge.clone() as Arc<dyn magnetar::runtime_tokio::MessageEncryptor>)
        .create()
        .await
        .expect_err("create() must refuse a configured encryptor");
    let msg = match err {
        PulsarError::Other(s) => s,
        other => panic!("expected PulsarError::Other, got {other:?}"),
    };
    assert!(
        msg.contains("create_with_encryption"),
        "guardrail error must point at the per-engine specialisation, got {msg:?}"
    );
}
