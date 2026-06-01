// SPDX-License-Identifier: Apache-2.0

//! Pluggable rustls crypto provider shim for the admin REST client.
//!
//! Mirror of `magnetar-runtime-tokio::tls_crypto` — duplicated rather than
//! shared so the admin crate stays standalone (no dep on the tokio engine).
//! Both shims are tiny, cfg-cascaded, and idempotent; they pick the same
//! provider for any given feature set, so a process that builds both an
//! `AdminClient` and a `PulsarClient` ends up with one consistent default.
//!
//! When admin is built with a single provider feature (the `check-crypto-matrix`
//! per-cell case), only one cfg arm compiles in. When several are unioned
//! (e.g. workspace `cargo test` with both `crypto-aws-lc-rs` default and
//! `crypto-ring`), the cascade picks the highest-priority provider — same
//! ordering as the runtime crate.

#[cfg(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
))]
use std::sync::Once;

/// Install the configured rustls crypto provider as the process-global
/// default. Idempotent — safe to call from any number of callsites,
/// including parallel tests.
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

/// No-crypto-feature build: admin compiles without TLS (reqwest gets
/// neither the `rustls` nor `rustls-no-provider` sub-feature), so a
/// provider install would be pointless. Stub preserves the call-site
/// API surface in `AdminClientBuilder::build`.
#[cfg(not(any(
    feature = "crypto-aws-lc-rs",
    feature = "crypto-ring",
    feature = "crypto-openssl",
    feature = "crypto-fips",
)))]
pub(crate) fn install_default_provider() {}
