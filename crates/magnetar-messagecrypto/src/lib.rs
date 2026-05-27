// SPDX-License-Identifier: Apache-2.0

//! PIP-4 end-to-end message encryption for magnetar.
//!
//! Mirrors `pulsar-client-messagecrypto-bc/.../MessageCryptoBc.java`. A
//! 256-bit AES-GCM data key is generated per [`MessageCrypto`] instance and
//! rotated periodically. For each recipient (named in `encryption_keys`),
//! the data key is wrapped under the recipient's RSA-OAEP-SHA-256 public
//! key. The wrapped keys ride on
//! `pb::MessageMetadata::encryption_keys`;
//! the random 12-byte AES-GCM nonce rides on
//! `pb::MessageMetadata::encryption_param`;
//! the algo name rides on
//! `pb::MessageMetadata::encryption_algo`.
//!
//! Decryption walks the encrypted-key list, tries each recipient's private
//! key, and the first that succeeds becomes the data key for AES-GCM.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use aws_lc_rs::rsa::{
    OAEP_SHA256_MGF1SHA256, OaepPrivateDecryptingKey, OaepPublicEncryptingKey,
    PrivateDecryptingKey, PublicEncryptingKey,
};
use bytes::Bytes;
use magnetar_proto::pb;
use parking_lot::Mutex;

const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;
const ALGO_NAME: &str = "AES_GCM_256";

/// Default data-key TTL (mirrors `MessageCryptoBc.java:90`'s 4-hour rotation).
pub const DEFAULT_DATA_KEY_TTL: Duration = Duration::from_secs(4 * 60 * 60);

/// Look up RSA public/private keys by logical name.
pub trait CryptoKeyReader: Send + Sync + std::fmt::Debug {
    /// Resolve the DER-encoded `SubjectPublicKeyInfo` bytes for a key name.
    fn public_key(&self, key_name: &str) -> Result<KeyInfo, CryptoError>;
    /// Resolve the PKCS#8 (v1) private key bytes for a key name.
    fn private_key(&self, key_name: &str) -> Result<KeyInfo, CryptoError>;
}

/// One key returned by a [`CryptoKeyReader`].
#[derive(Debug, Clone)]
pub struct KeyInfo {
    /// Echoed key name (consumers ride this on every encrypted-key entry).
    pub key_name: String,
    /// Key bytes — DER-encoded `SubjectPublicKeyInfo` (public) or PKCS#8 (private).
    pub key_value: Bytes,
    /// Arbitrary metadata (key version, KMS resource id, etc.).
    pub metadata: Vec<(String, String)>,
}

/// Errors that the encryption layer can surface.
#[allow(clippy::doc_markdown)]
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// No recipients were configured.
    #[error("no encryption keys configured")]
    NoKeys,
    /// Public key lookup failed.
    #[error("public key `{0}` lookup failed: {1}")]
    PublicKeyLookup(String, String),
    /// Private key lookup failed.
    #[error("private key `{0}` lookup failed: {1}")]
    PrivateKeyLookup(String, String),
    /// No private key in the reader could unwrap the data key.
    #[error("no private key could decrypt the data key")]
    NoMatchingKey,
    /// AES-GCM tag check failed (ciphertext tampered or wrong key).
    #[error("AES-GCM tag verification failed")]
    TagInvalid,
    /// `MessageMetadata.encryption_algo` is not the one we ship.
    #[error("unsupported encryption algorithm: {0}")]
    UnsupportedAlgo(String),
    /// `MessageMetadata.encryption_param` (nonce) is missing or has wrong length.
    #[error("malformed encryption parameters")]
    MalformedNonce,
    /// Underlying aws-lc-rs failure.
    #[error("crypto backend error: {0}")]
    Backend(String),
}

#[derive(Debug)]
struct DataKeyState {
    data_key: [u8; 32],
    wrapped: Vec<(String, Vec<u8>)>,
    last_refresh: Instant,
}

/// PIP-4 encryption handle. One per producer; reuse across sends.
pub struct MessageCrypto {
    reader: Arc<dyn CryptoKeyReader>,
    encryption_keys: Vec<String>,
    state: Mutex<DataKeyState>,
    data_key_ttl: Duration,
    rng: SystemRandom,
}

impl std::fmt::Debug for MessageCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageCrypto")
            .field("encryption_keys", &self.encryption_keys)
            .field("data_key_ttl", &self.data_key_ttl)
            .finish_non_exhaustive()
    }
}

impl MessageCrypto {
    /// Construct a new encryption handle.
    pub fn new(
        reader: Arc<dyn CryptoKeyReader>,
        encryption_keys: Vec<String>,
    ) -> Result<Self, CryptoError> {
        Self::with_ttl(reader, encryption_keys, DEFAULT_DATA_KEY_TTL)
    }

    /// Construct with a custom TTL (useful in tests).
    pub fn with_ttl(
        reader: Arc<dyn CryptoKeyReader>,
        encryption_keys: Vec<String>,
        data_key_ttl: Duration,
    ) -> Result<Self, CryptoError> {
        if encryption_keys.is_empty() {
            return Err(CryptoError::NoKeys);
        }
        let rng = SystemRandom::new();
        let state = generate_state(&rng, reader.as_ref(), &encryption_keys)?;
        Ok(Self {
            reader,
            encryption_keys,
            state: Mutex::new(state),
            data_key_ttl,
            rng,
        })
    }

    /// Force a fresh data key.
    pub fn rotate_data_key(&self) -> Result<(), CryptoError> {
        let new_state = generate_state(&self.rng, self.reader.as_ref(), &self.encryption_keys)?;
        *self.state.lock() = new_state;
        Ok(())
    }

    /// Encrypt `plaintext` and populate `metadata`.
    pub fn encrypt(
        &self,
        plaintext: &[u8],
        metadata: &mut pb::MessageMetadata,
    ) -> Result<Bytes, CryptoError> {
        self.maybe_rotate()?;

        let mut nonce_bytes = [0u8; AES_GCM_NONCE_LEN];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|e| CryptoError::Backend(format!("rng fill: {e:?}")))?;

        let (data_key, wrapped) = {
            let s = self.state.lock();
            (s.data_key, s.wrapped.clone())
        };

        let mut in_out = Vec::with_capacity(plaintext.len() + AES_GCM_TAG_LEN);
        in_out.extend_from_slice(plaintext);

        let unbound = UnboundKey::new(&AES_256_GCM, &data_key)
            .map_err(|e| CryptoError::Backend(format!("unbound key: {e:?}")))?;
        let key = LessSafeKey::new(unbound);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
            .map_err(|e| CryptoError::Backend(format!("seal: {e:?}")))?;

        metadata.encryption_algo = Some(ALGO_NAME.to_owned());
        metadata.encryption_param = Some(Bytes::copy_from_slice(&nonce_bytes));
        metadata.encryption_keys = wrapped
            .into_iter()
            .map(|(name, value)| pb::EncryptionKeys {
                key: name,
                value: Bytes::from(value),
                metadata: Vec::new(),
            })
            .collect();
        Ok(Bytes::from(in_out))
    }

    /// Decrypt a PIP-4 ciphertext.
    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        metadata: &pb::MessageMetadata,
    ) -> Result<Bytes, CryptoError> {
        match metadata.encryption_algo.as_deref() {
            Some(name) if name == ALGO_NAME => {}
            Some(other) => return Err(CryptoError::UnsupportedAlgo(other.to_owned())),
            None => return Err(CryptoError::UnsupportedAlgo("(missing)".to_owned())),
        }
        let nonce_bytes = metadata
            .encryption_param
            .as_deref()
            .ok_or(CryptoError::MalformedNonce)?;
        if nonce_bytes.len() != AES_GCM_NONCE_LEN {
            return Err(CryptoError::MalformedNonce);
        }

        let mut data_key: Option<[u8; 32]> = None;
        for entry in &metadata.encryption_keys {
            let Ok(priv_info) = self.reader.private_key(&entry.key) else {
                continue;
            };
            let Ok(priv_key) = PrivateDecryptingKey::from_pkcs8(&priv_info.key_value) else {
                continue;
            };
            let Ok(oaep_priv) = OaepPrivateDecryptingKey::new(priv_key) else {
                continue;
            };
            let mut out = vec![0u8; oaep_priv.min_output_size()];
            let Ok(unwrapped) =
                oaep_priv.decrypt(&OAEP_SHA256_MGF1SHA256, &entry.value, &mut out, None)
            else {
                continue;
            };
            if unwrapped.len() != 32 {
                continue;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(unwrapped);
            data_key = Some(key);
            break;
        }
        let data_key = data_key.ok_or(CryptoError::NoMatchingKey)?;

        let mut in_out = ciphertext.to_vec();
        let unbound = UnboundKey::new(&AES_256_GCM, &data_key)
            .map_err(|e| CryptoError::Backend(format!("unbound key: {e:?}")))?;
        let key = LessSafeKey::new(unbound);
        let mut nonce = [0u8; AES_GCM_NONCE_LEN];
        nonce.copy_from_slice(nonce_bytes);
        let nonce = Nonce::assume_unique_for_key(nonce);
        let plaintext = key
            .open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|_| CryptoError::TagInvalid)?;
        Ok(Bytes::copy_from_slice(plaintext))
    }

    fn maybe_rotate(&self) -> Result<(), CryptoError> {
        let needs = {
            let s = self.state.lock();
            s.last_refresh.elapsed() >= self.data_key_ttl
        };
        if needs {
            self.rotate_data_key()?;
        }
        Ok(())
    }
}

fn generate_state(
    rng: &SystemRandom,
    reader: &dyn CryptoKeyReader,
    encryption_keys: &[String],
) -> Result<DataKeyState, CryptoError> {
    let mut data_key = [0u8; 32];
    rng.fill(&mut data_key)
        .map_err(|e| CryptoError::Backend(format!("rng fill: {e:?}")))?;
    let mut wrapped = Vec::with_capacity(encryption_keys.len());
    for name in encryption_keys {
        let info = reader
            .public_key(name)
            .map_err(|e| CryptoError::PublicKeyLookup(name.clone(), e.to_string()))?;
        let pubkey = PublicEncryptingKey::from_der(&info.key_value)
            .map_err(|e| CryptoError::PublicKeyLookup(name.clone(), format!("{e:?}")))?;
        let oaep = OaepPublicEncryptingKey::new(pubkey)
            .map_err(|e| CryptoError::PublicKeyLookup(name.clone(), format!("{e:?}")))?;
        let mut ciphertext = vec![0u8; oaep.ciphertext_size()];
        let written = oaep
            .encrypt(&OAEP_SHA256_MGF1SHA256, &data_key, &mut ciphertext, None)
            .map_err(|e| CryptoError::Backend(format!("wrap: {e:?}")))?;
        let written_len = written.len();
        ciphertext.truncate(written_len);
        wrapped.push((info.key_name, ciphertext));
    }
    Ok(DataKeyState {
        data_key,
        wrapped,
        last_refresh: Instant::now(),
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Tiny in-memory `CryptoKeyReader` used by the test suite.

    use std::collections::HashMap;
    use std::sync::Mutex;

    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeySize, PrivateDecryptingKey};
    use bytes::Bytes;

    use super::{CryptoError, CryptoKeyReader, KeyInfo};

    #[derive(Debug)]
    pub(crate) struct InMemoryKeyReader {
        // name -> (public_der, private_pkcs8)
        keys: Mutex<HashMap<String, (Bytes, Bytes)>>,
    }

    impl InMemoryKeyReader {
        pub(crate) fn new() -> Self {
            Self {
                keys: Mutex::new(HashMap::new()),
            }
        }

        pub(crate) fn add_named(&self, name: &str) {
            let priv_key = PrivateDecryptingKey::generate(KeySize::Rsa2048).expect("rsa keygen");
            let pub_key = priv_key.public_key();
            // aws-lc-rs's `AsDer` trait returns owned `*Der` newtypes wrapping
            // a Vec<u8>; we just clone the bytes out for transport.
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
            let (pub_bytes, _) = map.get(key_name).ok_or_else(|| {
                CryptoError::PublicKeyLookup(key_name.to_owned(), "not found".into())
            })?;
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use magnetar_proto::pb;

    use super::test_support::InMemoryKeyReader;
    use super::{CryptoError, MessageCrypto};

    fn make_crypto(reader: Arc<InMemoryKeyReader>) -> MessageCrypto {
        MessageCrypto::new(reader, vec!["alice".into()]).expect("crypto")
    }

    fn make_reader_with_alice() -> Arc<InMemoryKeyReader> {
        let r = Arc::new(InMemoryKeyReader::new());
        r.add_named("alice");
        r
    }

    #[test]
    fn roundtrip_single_recipient() {
        let r = make_reader_with_alice();
        let crypto = make_crypto(r);
        let mut md = pb::MessageMetadata::default();
        let ct = crypto.encrypt(b"hello world", &mut md).expect("encrypt");
        assert_eq!(md.encryption_algo.as_deref(), Some("AES_GCM_256"));
        assert_eq!(md.encryption_keys.len(), 1);
        let pt = crypto.decrypt(&ct, &md).expect("decrypt");
        assert_eq!(&pt[..], b"hello world");
    }

    #[test]
    fn wrong_recipient_rejected() {
        // Sender encrypts to *its* alice key.
        let r_send = make_reader_with_alice();
        let crypto = make_crypto(r_send);
        let mut md = pb::MessageMetadata::default();
        let ct = crypto.encrypt(b"top secret", &mut md).expect("encrypt");

        // Receiver has a different alice key (so it accepts the public-key
        // lookup at construction time but cannot unwrap the data key).
        let r_recv = Arc::new(InMemoryKeyReader::new());
        r_recv.add_named("alice");
        let recv_crypto = MessageCrypto::new(r_recv, vec!["alice".into()]).expect("crypto");
        let err = recv_crypto
            .decrypt(&ct, &md)
            .expect_err("decrypt should fail");
        assert!(matches!(err, CryptoError::NoMatchingKey));
    }

    #[test]
    fn tampered_ciphertext_rejected() {
        let r = make_reader_with_alice();
        let crypto = make_crypto(r);
        let mut md = pb::MessageMetadata::default();
        let mut ct = crypto
            .encrypt(b"hello world", &mut md)
            .expect("encrypt")
            .to_vec();
        ct[0] ^= 0xff;
        let err = crypto.decrypt(&ct, &md).expect_err("tag should fail");
        assert!(matches!(err, CryptoError::TagInvalid));
    }

    #[test]
    fn empty_payload_roundtrip() {
        let r = make_reader_with_alice();
        let crypto = make_crypto(r);
        let mut md = pb::MessageMetadata::default();
        let ct = crypto.encrypt(b"", &mut md).expect("encrypt");
        let pt = crypto.decrypt(&ct, &md).expect("decrypt");
        assert!(pt.is_empty());
    }

    #[test]
    fn missing_algo_rejected() {
        let r = make_reader_with_alice();
        let crypto = make_crypto(r);
        let mut md = pb::MessageMetadata::default();
        let ct = crypto.encrypt(b"x", &mut md).expect("encrypt");
        md.encryption_algo = None;
        let err = crypto
            .decrypt(&ct, &md)
            .expect_err("missing algo should fail");
        assert!(matches!(err, CryptoError::UnsupportedAlgo(_)));
    }

    #[test]
    fn malformed_nonce_rejected() {
        let r = make_reader_with_alice();
        let crypto = make_crypto(r);
        let mut md = pb::MessageMetadata::default();
        let ct = crypto.encrypt(b"x", &mut md).expect("encrypt");
        md.encryption_param = Some(vec![1, 2, 3].into());
        let err = crypto.decrypt(&ct, &md).expect_err("nonce length");
        assert!(matches!(err, CryptoError::MalformedNonce));
    }
}
