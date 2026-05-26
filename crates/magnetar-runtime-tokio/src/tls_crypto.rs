// SPDX-License-Identifier: Apache-2.0

//! Pluggable rustls crypto provider shim (issue #9, ADR-0035).
//!
//! Magnetar picks the rustls [`CryptoProvider`] at compile time via four
//! mutually-pluggable Cargo features:
//!
//! - `crypto-aws-lc-rs` (default): `aws-lc-rs`, brings rustls 0.23's default-on
//!   `prefer-post-quantum` hybrid X25519MLKEM768 key exchange.
//! - `crypto-ring`: `ring`.
//! - `crypto-openssl`: `rustls-openssl` (wraps system OpenSSL via the `deny.toml` `wrappers =
//!   ["rustls-openssl"]` carve-out).
//! - `crypto-fips`: aws-lc-rs FIPS-validated module (pulls `aws-lc-fips-sys`, requires cmake + a C
//!   toolchain at build time).
//!
//! Under `--all-features` the cfg cascade resolves to aws-lc-rs.
//! [`install_default_provider`] is idempotent (uses [`std::sync::Once`])
//! and may be called from any callsite that needs a default provider.
//! [`active_provider`] is the recommended entry — it installs first,
//! then returns the active provider clone, eliminating the historical
//! `ring`-hard-coded `unwrap_or_else` fallback.
//!
//! [`CryptoProvider`]: rustls::crypto::CryptoProvider

use std::sync::{Arc, Once};

use rustls::crypto::CryptoProvider;

#[cfg(not(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
)))]
compile_error!(
    "magnetar: enable at least one of crypto-{aws-lc-rs,ring,openssl,fips} \
     on the magnetar / magnetar-runtime-tokio crate"
);

/// Install the configured rustls crypto provider as the process-global
/// default. Idempotent — safe to call from any number of callsites,
/// including tests.
///
/// Under `--all-features` the cfg cascade picks aws-lc-rs first, then
/// fips, then openssl, then ring. Single-provider builds (the per-cell
/// matrix exercised by `cargo xtask check-crypto-matrix`) only have one
/// candidate compiled in.
#[cfg(feature = "crypto-aws-lc-rs")]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// FIPS-validated aws-lc-rs provider; selected when `crypto-fips` is on
/// and `crypto-aws-lc-rs` is off.
#[cfg(all(not(feature = "crypto-aws-lc-rs"), feature = "crypto-fips"))]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::default_fips_provider().install_default();
    });
}

/// `rustls-openssl` provider; selected when neither aws-lc-rs nor FIPS
/// is on and `crypto-openssl` is.
#[cfg(all(
    not(any(feature = "crypto-aws-lc-rs", feature = "crypto-fips")),
    feature = "crypto-openssl"
))]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls_openssl::default_provider().install_default();
    });
}

/// `ring` provider; only selected when none of the higher-priority
/// providers are on.
#[cfg(all(
    not(any(
        feature = "crypto-aws-lc-rs",
        feature = "crypto-fips",
        feature = "crypto-openssl"
    )),
    feature = "crypto-ring"
))]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Return the active rustls [`CryptoProvider`] as an `Arc`, installing
/// it first if no default is set. Replaces the historical
/// `CryptoProvider::get_default().cloned().unwrap_or_else(|| Arc::new(
/// ring::default_provider()))` pattern at the four production callsites.
///
/// # Panics
///
/// Only panics if [`install_default_provider`] failed to populate the
/// process-global default, which would indicate a rustls bug.
#[must_use]
pub fn active_provider() -> Arc<CryptoProvider> {
    install_default_provider();
    CryptoProvider::get_default()
        .cloned()
        .expect("install_default_provider() must populate the global rustls CryptoProvider")
}

#[cfg(test)]
mod tests {
    use super::{active_provider, install_default_provider};

    #[test]
    fn install_default_provider_is_idempotent() {
        install_default_provider();
        install_default_provider();
        install_default_provider();
        // No panic = idempotent. `Once::call_once` guarantees a single
        // initialisation; repeated calls are cheap no-ops.
    }

    #[test]
    fn active_provider_returns_a_valid_provider() {
        let provider = active_provider();
        assert!(
            !provider.cipher_suites.is_empty(),
            "active rustls provider must expose at least one cipher suite"
        );
    }
}
