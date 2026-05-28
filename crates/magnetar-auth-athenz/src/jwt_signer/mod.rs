// SPDX-License-Identifier: Apache-2.0

//! Concrete [`crate::zts::JwtSigner`] backends (ADR-0030 + ADR-0035).
//!
//! Two mutually-pluggable implementations of the
//! `crate::zts::JwtSigner` trait, selected by the workspace crypto-
//! provider feature matrix:
//!
//! | Feature on `magnetar-auth-athenz` | Backend type      | RSA library |
//! | --------------------------------- | ----------------- | ----------- |
//! | `crypto-aws-lc-rs`                | `AwsLcRsSigner`   | aws-lc-rs   |
//! | `crypto-ring`                     | `RingSigner`      | ring        |
//!
//! (the type links resolve only when the matching feature is enabled;
//! the table renders the names verbatim so docs build under both
//! single-feature cells of the `cargo xtask check-crypto-matrix`
//! cartesian product).
//!
//! Both backends produce **byte-identical** signatures for the same
//! key + payload (RSASSA-PKCS1-v1_5 with SHA-256 is deterministic per
//! RFC 8017 §8.2). The matrix mirrors the rustls provider selection in
//! [`magnetar-runtime-tokio/src/tls_crypto.rs`](https://github.com/CleverCloud/magnetar/blob/main/crates/magnetar-runtime-tokio/src/tls_crypto.rs)
//! so the workspace stays internally consistent: enabling
//! `crypto-aws-lc-rs` lights up aws-lc-rs everywhere (rustls + Athenz
//! signer + PIP-4 message encryption), and enabling `crypto-ring`
//! lights up ring (rustls + Athenz signer; PIP-4 stays on aws-lc-rs by
//! workspace policy).
//!
//! When both features are enabled (e.g. `--all-features`) the cfg
//! cascade picks aws-lc-rs first, matching the ADR-0035 priority
//! `aws-lc-rs > fips > openssl > ring`. The ring path stays compiled
//! (it is still publicly callable via `RingSigner` in case a downstream
//! caller wants to instantiate it explicitly) but the
//! [`default_signer_for`] helper returns the aws-lc-rs backend.
//!
//! The default (`crypto-aws-lc-rs` and `crypto-ring` both off) keeps
//! the historical "ship the trait, downstream picks the signer" stance —
//! the module is empty, [`crate::AthenzProvider::with_default_signer`]
//! is not compiled, and callers wire their own [`crate::zts::JwtSigner`]
//! impl (jsonwebtoken, an HSM bridge, …) via
//! [`crate::AthenzProvider::with_zts_client`].
//!
//! # Zeroization (ADR-0030 close-out)
//!
//! Both backends wrap the parsed PKCS#8 DER bytes in
//! [`zeroize::Zeroizing`] so the secret material is wiped from memory
//! when the signer drops. aws-lc-rs / ring `RsaKeyPair` types
//! themselves are opaque wrappers around C-allocated `EVP_PKEY` /
//! `BIGNUM` structures and cannot be made `Zeroize`-friendly from
//! Rust; the design therefore stores the **DER bytes** under
//! `Zeroizing<Vec<u8>>`
//! and reconstructs the keypair on each sign. The trade-off is roughly
//! one PKCS#8 ASN.1 parse + RSA structure rebuild per sign call —
//! noise in the budget of an N-token mint that already does a
//! 2048-bit modular exponentiation — in exchange for a hard guarantee
//! that the secret never lingers after the signer is dropped.

#[cfg(feature = "crypto-aws-lc-rs")]
pub mod aws_lc_rs;
#[cfg(feature = "crypto-ring")]
pub mod ring;

#[cfg(feature = "crypto-aws-lc-rs")]
pub use aws_lc_rs::AwsLcRsSigner;
#[cfg(feature = "crypto-ring")]
pub use ring::RingSigner;

use crate::AthenzError;

/// Construct the cfg-active concrete [`crate::zts::JwtSigner`] for the
/// supplied [`crate::AthenzConfig`]. Picks aws-lc-rs over ring under
/// `--all-features` (mirroring ADR-0035's
/// `aws-lc-rs > fips > openssl > ring` priority).
///
/// Only compiled when at least one of `crypto-aws-lc-rs` /
/// `crypto-ring` is enabled. Callers without either feature wire their
/// own [`crate::zts::JwtSigner`] (jsonwebtoken / HSM bridge / etc.) via
/// [`crate::AthenzProvider::with_zts_client`].
///
/// # Errors
/// Surfaces [`AthenzError::Config`] or [`AthenzError::SignerFailure`]
/// from the per-backend constructor (PEM parse failure, ASN.1 reject,
/// modulus-size policy violation, …).
#[cfg(any(feature = "crypto-aws-lc-rs", feature = "crypto-ring"))]
pub fn default_signer_for(
    config: &crate::AthenzConfig,
) -> Result<std::sync::Arc<dyn crate::zts::JwtSigner>, AthenzError> {
    #[cfg(feature = "crypto-aws-lc-rs")]
    {
        let signer = AwsLcRsSigner::from_athenz_config(config)?;
        Ok(std::sync::Arc::new(signer) as std::sync::Arc<dyn crate::zts::JwtSigner>)
    }
    #[cfg(all(not(feature = "crypto-aws-lc-rs"), feature = "crypto-ring"))]
    {
        let signer = RingSigner::from_athenz_config(config)?;
        Ok(std::sync::Arc::new(signer) as std::sync::Arc<dyn crate::zts::JwtSigner>)
    }
}

/// Shared helpers used by both backend modules. Folded into a private
/// inline module rather than a sibling file because the helpers are
/// tiny (a JOSE header, a base64url emitter, a JSON-claims emitter) and
/// only meaningful in the context of an RS256 signer.
#[cfg(any(feature = "crypto-aws-lc-rs", feature = "crypto-ring"))]
pub(crate) mod jws {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use crate::zts::ZtsClaims;

    /// RS256 JOSE header bytes for the supplied key id, ready to be
    /// base64url-encoded. Constant shape: `{"alg":"RS256","kid":"…","typ":"JWT"}`.
    /// The field order matches `jsonwebtoken` / Athenz Java client output
    /// so any third-party verifier handed our JWT sees the same
    /// signing input bytes.
    pub(crate) fn header_json(kid: &str) -> String {
        let escaped = escape_json_string(kid);
        format!("{{\"alg\":\"RS256\",\"kid\":\"{escaped}\",\"typ\":\"JWT\"}}")
    }

    /// Render [`ZtsClaims`] as the JSON payload we sign. Matches the
    /// Athenz N-token claim set (`iss` / `sub` / `aud` / `kid` /
    /// `iat` / `exp`). Field order is fixed so the output is
    /// byte-stable across calls — load-bearing for the cross-backend
    /// signature-identity test (the signature is over the JSON bytes
    /// verbatim, not a re-serialised normalised form).
    pub(crate) fn claims_json(claims: &ZtsClaims) -> String {
        let iss = escape_json_string(&claims.iss);
        let sub = escape_json_string(&claims.sub);
        let aud = escape_json_string(&claims.aud);
        let kid = escape_json_string(&claims.kid);
        format!(
            "{{\"iss\":\"{iss}\",\"sub\":\"{sub}\",\"aud\":\"{aud}\",\"kid\":\"{kid}\",\"iat\":{iat},\"exp\":{exp}}}",
            iat = claims.iat,
            exp = claims.exp,
        )
    }

    /// Compose the signing input — `base64url(header) || "." || base64url(payload)` —
    /// per RFC 7515 §3.1.
    pub(crate) fn signing_input(header_json: &str, claims_json: &str) -> String {
        let header_b64 = URL_SAFE_NO_PAD.encode(header_json.as_bytes());
        let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());
        format!("{header_b64}.{claims_b64}")
    }

    /// Append the RS256 signature segment to a previously-prepared
    /// signing input. Returns the full compact serialisation
    /// `header.payload.signature`.
    pub(crate) fn assemble(signing_input: &str, signature_bytes: &[u8]) -> String {
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature_bytes);
        format!("{signing_input}.{sig_b64}")
    }

    /// Minimal JSON-string escaper for the fields we emit: tenant
    /// service names, ZTS URLs, key ids. None of those should ever
    /// contain control characters in practice, but escaping the small
    /// reserved set (`"`, `\`, `\n`, `\r`, `\t`) keeps the output
    /// well-formed even on misconfigured Athenz tenants and stays
    /// dep-free (no `serde_json` round-trip).
    fn escape_json_string(s: &str) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => {
                    // Writing into `String` via `fmt::Write` is infallible.
                    let _ = write!(out, "\\u{:04x}", c as u32);
                }
                c => out.push(c),
            }
        }
        out
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::zts::ZtsClaims;

        #[test]
        fn header_json_field_order_is_stable() {
            assert_eq!(
                header_json("key0"),
                r#"{"alg":"RS256","kid":"key0","typ":"JWT"}"#
            );
        }

        #[test]
        fn claims_json_field_order_is_stable() {
            let claims = ZtsClaims {
                iss: "mydomain.myservice".into(),
                sub: "mydomain.myservice".into(),
                aud: "https://zts.example.invalid:4443/zts/v1/".into(),
                kid: "key0".into(),
                iat: 1_700_000_000,
                exp: 1_700_000_060,
            };
            assert_eq!(
                claims_json(&claims),
                r#"{"iss":"mydomain.myservice","sub":"mydomain.myservice","aud":"https://zts.example.invalid:4443/zts/v1/","kid":"key0","iat":1700000000,"exp":1700000060}"#
            );
        }

        #[test]
        fn signing_input_is_dot_separated_base64url() {
            let header = r#"{"alg":"RS256","kid":"k","typ":"JWT"}"#;
            let claims = r#"{"x":1}"#;
            let input = signing_input(header, claims);
            // RFC 7515 §3.1 — the two segments are URL-safe-no-pad
            // base64, joined by a single "." with no padding.
            assert!(!input.contains('='));
            assert_eq!(input.matches('.').count(), 1);
        }

        #[test]
        fn escape_json_string_handles_reserved_chars() {
            assert_eq!(escape_json_string("a\"b\\c"), r#"a\"b\\c"#);
            assert_eq!(escape_json_string("a\nb"), "a\\nb");
        }
    }
}
