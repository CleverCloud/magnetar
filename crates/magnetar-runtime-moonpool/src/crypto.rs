// SPDX-License-Identifier: Apache-2.0

//! Runtime-level encryption hook traits.
//!
//! Mirrors `magnetar_runtime_tokio::crypto` exactly. `magnetar-runtime-moonpool` does NOT
//! depend on `magnetar-messagecrypto` directly — that crate pulls in `aws-lc-rs`, which is
//! heavy. Instead the producer / consumer hold an `Arc<dyn MessageEncryptor>` /
//! `Arc<dyn MessageDecryptor>` populated from the magnetar façade (which CAN depend on
//! magnetar-messagecrypto behind the `encryption` feature).
//!
//! `magnetar_messagecrypto::MessageCrypto` implements both traits via the `Arc`-based
//! blanket impls in the façade's `crypto` module.

use bytes::Bytes;
use magnetar_proto::pb;

/// Producer-side hook: encrypt a plaintext payload and stamp the matching
/// [`pb::MessageMetadata`] fields (encryption_keys, encryption_algo, encryption_param).
pub trait MessageEncryptor: Send + Sync + std::fmt::Debug {
    /// Encrypt the plaintext payload, returning the ciphertext + populating metadata.
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError>;
}

/// Consumer-side hook: decrypt a ciphertext payload using the metadata stamped on it.
pub trait MessageDecryptor: Send + Sync + std::fmt::Debug {
    /// Decrypt the ciphertext payload.
    fn decrypt(
        &self,
        ciphertext: &[u8],
        metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError>;
}

/// Error type bridging the runtime to whatever underlying encryption backend the user
/// plugged in. Implementations stringify their concrete error type into the message field.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct EncryptError(pub String);

impl EncryptError {
    /// Build an `EncryptError` from any displayable backend error.
    pub fn new(err: impl std::fmt::Display) -> Self {
        Self(err.to_string())
    }
}
