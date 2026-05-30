// SPDX-License-Identifier: Apache-2.0

//! Runtime-level encryption hook traits (PIP-4).
//!
//! The sans-io [`Connection`](crate::Connection) does not itself encrypt or decrypt — payload
//! crypto belongs to the runtime engines so the cipher pulls into the heavy-dependency
//! `magnetar-messagecrypto` crate only when the façade's `encryption` feature is on. Producers and
//! consumers carry an `Arc<dyn MessageEncryptor>` / `Arc<dyn MessageDecryptor>` populated from the
//! façade.
//!
//! The trait surface lives here in `magnetar-proto` rather than being duplicated per runtime
//! crate: both [`MessageEncryptor::encrypt`] and [`MessageDecryptor::decrypt`] take raw
//! [`bytes::Bytes`] plus a [`crate::pb::MessageMetadata`] — no I/O, no async, no runtime-specific
//! types — so the canonical sans-io home keeps the contract single-sourced. The runtime crates
//! re-export the symbols (`pub use magnetar_proto::crypto::{…}`) so existing import paths keep
//! working.
//!
//! `magnetar_messagecrypto::MessageCrypto` implements both traits via the `Arc`-based blanket
//! impls in the façade's `crypto` module.

use bytes::Bytes;

use crate::pb;

/// Producer-side hook: encrypt a plaintext payload and stamp the matching
/// [`pb::MessageMetadata`] fields (encryption_keys, encryption_algo, encryption_param).
pub trait MessageEncryptor: Send + Sync + std::fmt::Debug {
    /// Encrypt the plaintext payload, returning the ciphertext + populating metadata.
    ///
    /// # Errors
    /// Surfaces any backend failure (key lookup, cipher init, etc.) as [`EncryptError`].
    fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, EncryptError>;
}

/// Consumer-side hook: decrypt a ciphertext payload using the metadata stamped on it.
pub trait MessageDecryptor: Send + Sync + std::fmt::Debug {
    /// Decrypt the ciphertext payload.
    ///
    /// # Errors
    /// Surfaces any backend failure (key lookup, tag mismatch, etc.) as [`EncryptError`].
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
