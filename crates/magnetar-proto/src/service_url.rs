// SPDX-License-Identifier: Apache-2.0

//! Pluggable broker service URL provider — Java parity (PIP-121 skeleton).
//!
//! Mirrors `org.apache.pulsar.client.api.ServiceUrlProvider`: the runtime polls the provider
//! for the active broker URL on every connection attempt instead of caching a single URL once
//! at builder time. Implementors decide when the URL has changed (typically a background task
//! feeding the provider externally) and return the current value via
//! [`ServiceUrlProvider::get_service_url`].
//!
//! # Sans-io discipline
//!
//! The trait is **deliberately sync** — `get_service_url` must be cheap and must never block
//! the driver loop. Providers that need I/O (e.g. polling a control plane) do so on an
//! external task and stamp the result into shared state (e.g. an
//! `Arc<parking_lot::Mutex<String>>`) that the impl reads. This keeps `magnetar-proto`
//! I/O-free as required by [`GUIDELINES.md`].
//!
//! # Default
//!
//! [`StaticServiceUrlProvider`] is the stock impl wrapping a single fixed URL. The existing
//! `ClientBuilder::service_url(...)` shortcut wires one of these internally so callers that
//! pin a single URL keep working without change.
//!
//! # PIP-121 follow-up
//!
//! This module ships only the trait + the static impl. `AutoClusterFailover` (latency-based)
//! and `ControlledClusterFailover` (external signal) policies are scoped for follow-up.
//!
//! [`GUIDELINES.md`]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

use core::fmt::Debug;
use std::sync::Arc;

/// Java parity: `org.apache.pulsar.client.api.ServiceUrlProvider`.
///
/// The runtime polls the provider for the active broker URL on every connection attempt.
/// Implementors decide when the URL has changed (typically a background task feeding the
/// provider externally).
///
/// Implementations MUST be `Send + Sync + Debug` because they live behind an [`Arc`] shared
/// between the runtime driver task and any caller that built the
/// [`crate::conn::ConnectionConfig`].
pub trait ServiceUrlProvider: Send + Sync + Debug {
    /// Return the current service URL (`pulsar://...` or `pulsar+ssl://...`).
    ///
    /// Called by the runtime on every (re)connect attempt. Must be cheap — never block,
    /// never do I/O. Push asynchronous URL discovery into a background task and read its
    /// result from shared state.
    fn get_service_url(&self) -> String;
}

/// Stock single-URL provider — the default the existing `ClientBuilder::service_url(...)`
/// shortcut wires up. Mirrors Java's behaviour when `serviceUrlProvider` is not configured:
/// every reconnect dials the same URL.
#[derive(Debug, Clone)]
pub struct StaticServiceUrlProvider {
    url: String,
}

impl StaticServiceUrlProvider {
    /// Construct a static provider from any string-like input.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    /// Borrow the wrapped URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl ServiceUrlProvider for StaticServiceUrlProvider {
    fn get_service_url(&self) -> String {
        self.url.clone()
    }
}

/// Convenience constructor: wrap a single URL in an [`Arc<dyn ServiceUrlProvider>`] so callers
/// can hand the result directly to [`crate::conn::ConnectionConfig::service_url_provider`].
#[must_use]
pub fn static_service_url_provider(url: impl Into<String>) -> Arc<dyn ServiceUrlProvider> {
    Arc::new(StaticServiceUrlProvider::new(url))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_provider_returns_wrapped_url() {
        let provider = StaticServiceUrlProvider::new("pulsar://x:6650");
        assert_eq!(provider.get_service_url(), "pulsar://x:6650");
    }

    #[test]
    fn static_provider_is_stable_across_calls() {
        let provider = StaticServiceUrlProvider::new("pulsar+ssl://broker.example:6651");
        for _ in 0..5 {
            assert_eq!(
                provider.get_service_url(),
                "pulsar+ssl://broker.example:6651",
            );
        }
    }

    #[test]
    fn static_provider_borrows_url() {
        let provider = StaticServiceUrlProvider::new("pulsar://x:6650");
        assert_eq!(provider.url(), "pulsar://x:6650");
    }

    /// Mock provider that returns URL A on first call and URL B on subsequent calls — used to
    /// verify the runtime calls `get_service_url` on every (re)connect rather than caching.
    #[derive(Debug)]
    struct FlippingProvider {
        first: String,
        rest: String,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl ServiceUrlProvider for FlippingProvider {
        fn get_service_url(&self) -> String {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                self.first.clone()
            } else {
                self.rest.clone()
            }
        }
    }

    #[test]
    fn provider_is_polled_each_call() {
        let provider = FlippingProvider {
            first: "pulsar://a:6650".to_owned(),
            rest: "pulsar://b:6650".to_owned(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let provider: Arc<dyn ServiceUrlProvider> = Arc::new(provider);
        assert_eq!(provider.get_service_url(), "pulsar://a:6650");
        assert_eq!(provider.get_service_url(), "pulsar://b:6650");
        assert_eq!(provider.get_service_url(), "pulsar://b:6650");
    }

    #[test]
    fn helper_wraps_static_provider_in_arc() {
        let provider = static_service_url_provider("pulsar://broker:6650");
        assert_eq!(provider.get_service_url(), "pulsar://broker:6650");
    }
}
