// SPDX-License-Identifier: Apache-2.0

//! ring RS256 backend for the Athenz [`crate::zts::JwtSigner`].
//!
//! Gated behind `feature = "crypto-ring"`. Pairs with the rustls
//! `ring` crypto provider so a single workspace feature flip switches
//! every consumer (rustls + Athenz signer) at once (issue #9,
//! ADR-0035). Produces byte-identical signatures with the aws-lc-rs
//! sibling for the same key + claims (RSASSA-PKCS1-v1_5 with SHA-256
//! is deterministic per RFC 8017 §8.2).

use std::sync::Arc;

use ring::rand::SystemRandom;
use ring::signature::RsaKeyPair;
use rustls_pki_types::PrivatePkcs8KeyDer;
use rustls_pki_types::pem::PemObject;
use zeroize::Zeroizing;

use super::jws;
use crate::zts::{JwtSigner, ZtsClaims};
use crate::{AthenzConfig, AthenzError};

/// ring RSA backend. Mirrors `super::aws_lc_rs::AwsLcRsSigner`
/// (only resolves when `crypto-aws-lc-rs` is also enabled) — holds
/// the parsed PKCS#8 DER bytes under [`Zeroizing`] and reconstructs
/// the keypair on every sign. See the parent module docstring for
/// the ADR-0030 close-out rationale.
pub struct RingSigner {
    /// PKCS#8-encoded private key DER. Wiped on drop.
    key_der: Zeroizing<Vec<u8>>,
    /// Reused PRNG. ring ignores the rng for PKCS#1 v1.5 sign (the
    /// encoding is deterministic per RFC 8017 §8.2) but the
    /// `RsaKeyPair::sign` signature still requires one.
    rng: SystemRandom,
}

impl std::fmt::Debug for RingSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingSigner")
            .field("key_der", &"<redacted>")
            .finish()
    }
}

impl RingSigner {
    /// Construct from a tenant [`AthenzConfig`] — parses
    /// [`AthenzConfig::private_key_pem`] once and validates the PKCS#8
    /// blob before stashing the DER bytes for reuse.
    ///
    /// # Errors
    /// - [`AthenzError::Config`] when the PEM body is malformed or not PKCS#8 (an `-----BEGIN
    ///   PRIVATE KEY-----` block).
    /// - [`AthenzError::SignerFailure`] when ring rejects the PKCS#8 blob (wrong algorithm OID,
    ///   modulus out of policy, …).
    pub fn from_athenz_config(config: &AthenzConfig) -> Result<Self, AthenzError> {
        Self::from_pem_str(&config.private_key_pem)
    }

    /// Construct from a raw PEM string. Exposed mainly to make the
    /// per-backend tests independent of the surrounding [`AthenzConfig`]
    /// struct.
    ///
    /// # Errors
    /// See [`Self::from_athenz_config`].
    pub fn from_pem_str(pem: &str) -> Result<Self, AthenzError> {
        let der: PrivatePkcs8KeyDer<'static> = PrivatePkcs8KeyDer::from_pem_slice(pem.as_bytes())
            .map_err(|e| {
            AthenzError::Config(format!("athenz private key PEM parse failed: {e}"))
        })?;
        let key_der: Zeroizing<Vec<u8>> = Zeroizing::new(der.secret_pkcs8_der().to_vec());

        // Validate eagerly so misconfiguration is surfaced at
        // construction, not at the first sign.
        let _ = RsaKeyPair::from_pkcs8(&key_der).map_err(|e| {
            AthenzError::SignerFailure(format!("ring rejected the athenz tenant PKCS#8 key: {e}"))
        })?;

        Ok(Self {
            key_der,
            rng: SystemRandom::new(),
        })
    }

    /// Wrap the signer in [`Arc`] for handing to
    /// [`crate::zts::HttpZtsClient::new`] or [`crate::AthenzProvider::builder`].
    #[must_use]
    pub fn into_arc(self) -> Arc<dyn JwtSigner> {
        Arc::new(self)
    }

    /// Sign the supplied raw payload bytes (already JSON-encoded JWS
    /// claims, base64url-prefixed by the JOSE header) with the held
    /// RSA private key. Exposed for the cross-backend byte-identity
    /// test in `super::aws_lc_rs::tests` — production code should call
    /// [`JwtSigner::sign`].
    #[doc(hidden)]
    pub fn sign_raw(&self, signing_input: &[u8]) -> Result<Vec<u8>, AthenzError> {
        let key_pair = RsaKeyPair::from_pkcs8(&self.key_der).map_err(|e| {
            AthenzError::SignerFailure(format!(
                "ring key-pair rebuild from zeroized DER failed: {e}"
            ))
        })?;
        let mut signature = vec![0u8; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &self.rng,
                signing_input,
                &mut signature,
            )
            .map_err(|e| AthenzError::SignerFailure(format!("ring RSA-SHA256 sign failed: {e}")))?;
        Ok(signature)
    }
}

impl JwtSigner for RingSigner {
    fn sign(&self, claims: &ZtsClaims) -> Result<String, AthenzError> {
        let header = jws::header_json(&claims.kid);
        let payload = jws::claims_json(claims);
        let signing_input = jws::signing_input(&header, &payload);
        let signature = self.sign_raw(signing_input.as_bytes())?;
        Ok(jws::assemble(&signing_input, &signature))
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use ring::signature::{KeyPair, RSA_PKCS1_2048_8192_SHA256, RsaKeyPair, UnparsedPublicKey};

    use super::*;
    use crate::zts::ZtsClaims;

    /// Same PKCS#8 v1 RSA-2048 test key as
    /// `super::super::aws_lc_rs::tests::TEST_PKCS8_PEM`. Duplicated as
    /// a constant rather than re-imported so each backend's test
    /// module is independently compilable (no `cfg(all(feature = "…",
    /// feature = "…"))` cross-module wiring).
    pub(crate) const TEST_PKCS8_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxJud1eHdqMtxK
hTb7LKgcRuw1k/3/e2aOIPzgPOc3nGTgh+AgsSz5VCPVoqsub/ipbWU/3u5rN6pa
aSXxCRSdKF1LCTD+Qrp4T86W9vgBQeCiw61YyjTQQ55naN9Sngy6V+JzQOBOYqrY
i67ppWIebI5ThK/a0KbqFI+btDwt0W285c9h+/HIPrGWU1JokWuBzJW7DHgv7rLc
euEXPQqaHoMLZFgDsD7zvyOsVod+gbbMIhRJ72G0R18XBchwOfnbRZkDjiIVR7bI
uad+zPoWZLnxXZvvIOm0twkRtCoM0qcVzuAsDxuVfG9OGarQvk1p4lnALEb8Zl1J
+n7qKoCRAgMBAAECggEATIxyEcmnWCV4GV9s/aYzUly3LwOvCtmo3BuXCdJnWxli
Yb908st8kpRwE52B+MP7oEKcMLhFL+FS5FRxR7FTzgEmJwlmuUfeSaS6sXMwgWKV
DeAeJLLjlWbSqP6hGZMgDtlxCbpr8pMiHgZl46JKPrlL2v0H/DaTGa0ezPpZ0rXl
MWkHieSaGaC5oxoB/khxk22tZYn7XR0E78/w1k3JZr6tiHHPRZGCU8dpl3xRowfp
76JEEkf7ZosLtw+rigU5D44vIcUVJUbweNy/Ad2CzL7hGvdeXOjCRLjhOdbVwwzw
yNcsCK1qNq5YlieSuFVBT89OuAeYuzqDhc47serLnQKBgQD5Oa1n9JuKTc1nkiFz
p7x4n7503p5fpwDPtIrUBjEX+xkFAV+1ujBtiLbMgkga4dMz3UBbRCx/ip7THhVt
8THYMILZ5jzw6AeO5jQYsb92jRf7VNLa9/F2jSzQnUdNHwJFh7rx1Zeg1SoCpt34
wk0fNfufvTWCJ1lDI1kjn+aOlwKBgQC1963OI5CWNlsYBwlaQyfYeo4yn1ghPUoK
Dlshpe16HWzaBxhaOhaanaYuqFXGW082plgQ+bg8w7rXU+mhpt0S6n1VDNg2WnVN
rq0Uz73yq44Dhhd7w0ugH5oBLbSwOAQSkQ1nxYtng3g1akaiiSrmErOHcIPmxPww
2NzOqiD3FwKBgG7IoLhxFyLnasL7Rjtu+Gx2Neclfijux4GMs5mEFxad24VKEw1o
8lX+S6Ok1gB9GbEYTJ9FMrKPIAKggM4aRRnglonduoEr4xA2bDn96Sn9lgd2sTP8
uy0DnEQvZZ52hj/6EbOmSnyHxODg5BLL7BRPnsZnCP4OF7OsZtdbINWJAoGAVvCP
Sf4UBrDRtRknjsinMPbdGbKoGLl/tm5FfD4ayE1mxIS/TdyTECxiSciDstHNdv7i
9LlbHS0nB9o/tcxTs8X1O713UADIKuVaLKdUyazNnUFj1u3oJAj1O7rqqYcZ6wUC
sqHfiQV3WY39UYrXxDULMZrAanGTTINQfC0ssuECgYEAzXpBkVWCu5VTcTNxrCOl
+btMpklzgovlpZWNxo8gDW6iNV2q3FcUjwxM4KRaxjKEKtmpi7HBDgLbDUxL3GffI
6Rc4ifbJEa41FakC7MGusbsyqeS2e0nF8WUn1fRgoBxARezLU9gVv/JpGDBSyt8
VKO8LwAfoAvnoIH0CDFftdg=
-----END PRIVATE KEY-----";

    fn fixed_claims() -> ZtsClaims {
        ZtsClaims {
            iss: "mydomain.myservice".into(),
            sub: "mydomain.myservice".into(),
            aud: "https://zts.example.invalid:4443/zts/v1/".into(),
            kid: "key0".into(),
            iat: 1_735_689_600,
            exp: 1_735_689_660,
        }
    }

    #[test]
    fn from_pem_str_parses_pkcs8_rsa_key() {
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse PKCS#8 key");
        let jwt = signer.sign(&fixed_claims()).expect("sign smoke");
        assert_eq!(jwt.matches('.').count(), 2, "jwt = {jwt}");
    }

    #[test]
    fn from_pem_str_rejects_garbage_pem() {
        let err = RingSigner::from_pem_str(
            "-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n",
        )
        .unwrap_err();
        assert!(
            matches!(err, AthenzError::Config(_) | AthenzError::SignerFailure(_)),
            "{err}",
        );
    }

    #[test]
    fn sign_round_trips_through_ring_verify() {
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse PKCS#8 key");
        let claims = fixed_claims();
        let jwt = signer.sign(&claims).expect("sign");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "compact JWS has three segments: {jwt}");
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let signature = URL_SAFE_NO_PAD
            .decode(parts[2].as_bytes())
            .expect("base64url-decode signature");

        let der_bytes = pem_pkcs8_to_der(TEST_PKCS8_PEM);
        let key_pair = RsaKeyPair::from_pkcs8(&der_bytes).expect("rebuild key");
        let pub_der = key_pair.public_key().as_ref().to_vec();
        let verifier = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, pub_der);
        verifier
            .verify(signing_input.as_bytes(), &signature)
            .expect("ring verify must accept the freshly-minted signature");
    }

    #[test]
    fn jwt_payload_carries_iss_sub_aud_exp() {
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse PKCS#8 key");
        let claims = fixed_claims();
        let jwt = signer.sign(&claims).expect("sign");
        let payload_b64 = jwt.split('.').nth(1).expect("payload segment");
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_b64.as_bytes())
            .expect("base64url payload");
        let payload =
            std::str::from_utf8(&payload_bytes).expect("payload is valid UTF-8 JSON bytes");
        assert!(
            payload.contains("\"iss\":\"mydomain.myservice\""),
            "{payload}"
        );
        assert!(
            payload.contains("\"sub\":\"mydomain.myservice\""),
            "{payload}"
        );
        assert!(
            payload.contains("\"aud\":\"https://zts.example.invalid:4443/zts/v1/\""),
            "{payload}"
        );
        assert!(payload.contains("\"exp\":1735689660"), "{payload}");
    }

    #[test]
    fn sign_is_deterministic_for_fixed_claims() {
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse");
        let claims = fixed_claims();
        let a = signer.sign(&claims).expect("sign a");
        let b = signer.sign(&claims).expect("sign b");
        assert_eq!(a, b, "RS256 signatures must be deterministic");
    }

    #[test]
    fn debug_does_not_leak_key_material() {
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse");
        let rendered = format!("{signer:?}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(!rendered.contains("BEGIN PRIVATE KEY"), "{rendered}");
    }

    #[test]
    fn zeroizing_wrap_pins_drop_semantics() {
        fn assert_zeroize_pinned<T: zeroize::Zeroize>(_: &T) {}
        let signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("parse");
        assert_zeroize_pinned(&*signer.key_der);
    }

    /// Cross-backend byte-identity. Only built when BOTH crypto
    /// backends are compiled in. RSASSA-PKCS1-v1_5 with SHA-256 is
    /// deterministic per RFC 8017 §8.2 — aws-lc-rs and ring must
    /// produce the same signature bytes for the same key + payload.
    /// If this assertion ever fails, that's a bug in one of the
    /// libraries (we have produced a reproducer).
    #[cfg(feature = "crypto-aws-lc-rs")]
    #[test]
    fn cross_backend_signature_byte_identity() {
        use super::super::aws_lc_rs::AwsLcRsSigner;
        let ring_signer = RingSigner::from_pem_str(TEST_PKCS8_PEM).expect("ring parse");
        let aws_signer = AwsLcRsSigner::from_pem_str(TEST_PKCS8_PEM).expect("aws-lc-rs parse");
        let claims = fixed_claims();
        let ring_jwt = ring_signer.sign(&claims).expect("ring sign");
        let aws_jwt = aws_signer.sign(&claims).expect("aws sign");
        assert_eq!(
            ring_jwt, aws_jwt,
            "aws-lc-rs and ring must produce byte-identical RS256 JWTs (RFC 8017 §8.2)"
        );
    }

    fn pem_pkcs8_to_der(pem: &str) -> Vec<u8> {
        let der = PrivatePkcs8KeyDer::from_pem_slice(pem.as_bytes()).expect("PEM decode");
        der.secret_pkcs8_der().to_vec()
    }
}
