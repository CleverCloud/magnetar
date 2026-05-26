// SPDX-License-Identifier: Apache-2.0

//! Pluggable rustls crypto provider shim — moonpool engine variant
//! (issue #9, ADR-0035).
//!
//! Mirrors `magnetar-runtime-tokio::tls_crypto` exactly. The moonpool
//! engine drives [`rustls::ClientConnection`] over a byte pipe directly
//! (ADR-0006); it pulls in `rustls` (and optionally `rustls-openssl`)
//! but never `tokio-rustls`.
//!
//! See the tokio sibling for the rationale around the cfg cascade.

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
     on the magnetar / magnetar-runtime-moonpool crate"
);

/// Install the configured rustls crypto provider as the process-global
/// default. Idempotent.
#[cfg(feature = "crypto-aws-lc-rs")]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[cfg(all(not(feature = "crypto-aws-lc-rs"), feature = "crypto-fips"))]
pub fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::default_fips_provider().install_default();
    });
}

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
/// it first if no default is set.
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
