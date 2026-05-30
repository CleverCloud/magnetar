// SPDX-License-Identifier: Apache-2.0

//! Bridge `magnetar-messagecrypto::MessageCrypto` into the runtime's
//! `MessageEncryptor` / `MessageDecryptor` trait hooks.
//!
//! Only compiled when the `encryption` feature is enabled. The runtime crate stays
//! crypto-agnostic; the bridge lives here in the façade where both `magnetar-messagecrypto`
//! and the runtime engines are visible. Uses a newtype to satisfy the orphan rule.
//!
//! Since both runtime crates re-export the same canonical [`magnetar_proto::crypto`] traits,
//! the single impl below satisfies the tokio and moonpool engines simultaneously — no
//! duplicate per-runtime impls.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use magnetar::{MessageCryptoBridge, PulsarClient};
//! # use magnetar_messagecrypto::MessageCrypto;
//! # async fn run(crypto: Arc<MessageCrypto>) -> Result<(), Box<dyn std::error::Error>> {
//! let bridge: Arc<MessageCryptoBridge> = Arc::new(MessageCryptoBridge::new(crypto));
//! let client = PulsarClient::builder().service_url("pulsar://localhost:6650").build().await?;
//! // Use `bridge.clone() as Arc<dyn MessageEncryptor>` when constructing a producer with
//! // encryption, etc.
//! # Ok(()) }
//! ```

use std::sync::Arc;

use bytes::Bytes;
use magnetar_messagecrypto::MessageCrypto;
use magnetar_proto::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
use magnetar_proto::pb;

/// Newtype wrapper that exposes a [`magnetar_messagecrypto::MessageCrypto`] as both
/// [`MessageEncryptor`] and [`MessageDecryptor`] for any runtime engine.
///
/// Satisfies the Rust orphan rule (the runtime traits and `MessageCrypto` both live in
/// different crates; magnetar — being downstream of both — is the only place we can implement
/// them on a wrapper type). Both `magnetar-runtime-tokio` and `magnetar-runtime-moonpool`
/// re-export the same canonical [`magnetar_proto::crypto`] traits, so this single pair of
/// impls is consumed by every engine.
#[derive(Debug)]
pub struct MessageCryptoBridge {
    inner: Arc<MessageCrypto>,
}

impl MessageCryptoBridge {
    /// Wrap a shared `MessageCrypto`.
    #[must_use]
    pub fn new(inner: Arc<MessageCrypto>) -> Self {
        Self { inner }
    }

    /// Borrow the underlying `MessageCrypto` (for testing / direct use).
    #[must_use]
    pub fn inner(&self) -> &Arc<MessageCrypto> {
        &self.inner
    }
}

impl MessageEncryptor for MessageCryptoBridge {
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError> {
        self.inner
            .encrypt(plaintext, metadata)
            .map_err(EncryptError::new)
    }
}

impl MessageDecryptor for MessageCryptoBridge {
    fn decrypt(
        &self,
        ciphertext: &[u8],
        metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError> {
        self.inner
            .decrypt(ciphertext, metadata)
            .map_err(EncryptError::new)
    }
}
