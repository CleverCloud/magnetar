// SPDX-License-Identifier: Apache-2.0

//! Pluggable rustls crypto provider shim used by `HttpZtsClient`.
//! Mirror of `magnetar-runtime-tokio::tls_crypto`,
//! `magnetar-admin::tls_crypto`, and `magnetar-auth-oauth2::tls_crypto`.
//! Gated on `feature = "zts"` because that is the only path in this crate
//! that builds a `reqwest::Client` and therefore needs a default provider
//! installed.

#[cfg(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
))]
use std::sync::Once;

#[cfg(feature = "crypto-aws-lc-rs")]
pub(crate) fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[cfg(all(not(feature = "crypto-aws-lc-rs"), feature = "crypto-fips"))]
pub(crate) fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::default_fips_provider().install_default();
    });
}

#[cfg(all(
    not(any(feature = "crypto-aws-lc-rs", feature = "crypto-fips")),
    feature = "crypto-openssl"
))]
pub(crate) fn install_default_provider() {
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
pub(crate) fn install_default_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// No-crypto-feature build with `zts` on: reqwest is pulled in but no
/// rustls provider sub-feature is enabled — the resulting `HttpZtsClient`
/// will panic at runtime if it actually tries to speak HTTPS. Matches the
/// pre-existing behavior on `main`; preserved here as a stub so the call
/// site stays uniform across feature shapes.
#[cfg(not(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
)))]
pub(crate) fn install_default_provider() {}
