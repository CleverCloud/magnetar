// SPDX-License-Identifier: Apache-2.0

//! Pluggable rustls crypto provider shim (mirror of
//! `magnetar-runtime-tokio::tls_crypto` and `magnetar-admin::tls_crypto`).
//! Duplicated to keep the `OAuth2` provider standalone without depending
//! on the tokio runtime crate. Both shims pick the same provider for any
//! given feature set, so a process that builds an `OauthClientCredentials`
//! provider alongside a `PulsarClient` ends up with one consistent default.

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

/// No-crypto-feature build: reqwest compiles without TLS, so no provider
/// install is required. Stub keeps the call-site API uniform.
#[cfg(not(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
)))]
pub(crate) fn install_default_provider() {}
