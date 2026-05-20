// SPDX-License-Identifier: Apache-2.0

//! PIP-4 end-to-end message encryption for magnetar.
//!
//! Mirrors the Java `MessageCryptoBc` design (`pulsar-client-messagecrypto-bc/`):
//! AES-GCM data-key wrapping. The data key (`encryptionKey`) rotates per call;
//! the encrypted key list rides on `MessageMetadata.encryption_keys`. Crypto
//! provider here is `aws-lc-rs` (audited, FIPS-friendly).
//!
//! Real implementation lands in M8.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder crypto handle.
#[derive(Debug, Default)]
pub struct MessageCrypto {
    _private: (),
}

#[cfg(test)]
mod tests {
    use super::MessageCrypto;

    #[test]
    fn crypto_compiles() {
        let _ = MessageCrypto::default();
    }
}
