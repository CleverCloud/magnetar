// SPDX-License-Identifier: Apache-2.0

//! PIP-121 cluster-failover service URL providers.
//!
//! [`ControlledClusterFailover`] — externally-driven URL swap. The user
//! (e.g. a control-plane sidecar) calls [`ControlledClusterFailover::set_url`]
//! when the active cluster changes; every subsequent
//! [`crate::ServiceUrlProvider::get_service_url`] call returns the new
//! URL. The supervised reconnect path picks it up on the next attempt
//! (see `magnetar-runtime-tokio::driver::supervised_driver_loop`).
//!
//! [`AutoClusterFailover`] (in `magnetar-runtime-tokio`) builds on this
//! shape with a background tokio task that runs user-supplied health
//! probes and flips the active URL automatically.
//!
//! Mirrors Java `org.apache.pulsar.client.api.ControlledClusterFailover`.
//!
//! See [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)
//! — the URL slot uses `parking_lot::Mutex<String>`, not a channel.

use std::sync::{Arc, Mutex};

use crate::ServiceUrlProvider;

/// Externally-driven PIP-121 cluster failover. The user mutates the
/// active URL via [`Self::set_url`]; subsequent
/// [`ServiceUrlProvider::get_service_url`] calls return the new value.
///
/// Cheap to clone — internally an [`Arc<Mutex<String>>`].
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider};
///
/// let failover = ControlledClusterFailover::new("pulsar://cluster-a:6650");
/// let provider: Arc<dyn ServiceUrlProvider> = Arc::new(failover.clone());
/// assert_eq!(provider.get_service_url(), "pulsar://cluster-a:6650");
///
/// failover.set_url("pulsar://cluster-b:6650");
/// assert_eq!(provider.get_service_url(), "pulsar://cluster-b:6650");
/// ```
#[derive(Debug, Clone)]
pub struct ControlledClusterFailover {
    url: Arc<Mutex<String>>,
}

impl ControlledClusterFailover {
    /// Construct a failover provider seeded with the given initial URL.
    #[must_use]
    pub fn new(initial_url: impl Into<String>) -> Self {
        Self {
            url: Arc::new(Mutex::new(initial_url.into())),
        }
    }

    /// Atomically swap the active URL. The change takes effect on the
    /// next [`ServiceUrlProvider::get_service_url`] call — which the
    /// supervised reconnect path consults on every reconnect attempt.
    pub fn set_url(&self, url: impl Into<String>) {
        if let Ok(mut guard) = self.url.lock() {
            *guard = url.into();
        }
    }

    /// Snapshot the current URL. Mostly useful for tests and logging.
    #[must_use]
    pub fn current_url(&self) -> String {
        self.url
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|poison| poison.into_inner().clone())
    }
}

impl ServiceUrlProvider for ControlledClusterFailover {
    fn get_service_url(&self) -> String {
        self.url
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|poison| poison.into_inner().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_url_returned_until_swap() {
        let f = ControlledClusterFailover::new("pulsar://a:6650");
        assert_eq!(f.get_service_url(), "pulsar://a:6650");
        assert_eq!(f.current_url(), "pulsar://a:6650");
    }

    #[test]
    fn set_url_changes_subsequent_reads() {
        let f = ControlledClusterFailover::new("pulsar://a:6650");
        f.set_url("pulsar://b:6651");
        assert_eq!(f.get_service_url(), "pulsar://b:6651");
        f.set_url("pulsar://c:6652");
        assert_eq!(f.get_service_url(), "pulsar://c:6652");
    }

    #[test]
    fn clone_shares_url_slot() {
        let f1 = ControlledClusterFailover::new("pulsar://a:6650");
        let f2 = f1.clone();
        f2.set_url("pulsar://b:6650");
        // Both handles see the new URL — the slot is shared via Arc<Mutex<...>>.
        assert_eq!(f1.get_service_url(), "pulsar://b:6650");
        assert_eq!(f2.get_service_url(), "pulsar://b:6650");
    }

    #[test]
    fn implements_service_url_provider_via_arc() {
        let f = ControlledClusterFailover::new("pulsar://x:6650");
        let provider: Arc<dyn ServiceUrlProvider> = Arc::new(f);
        assert_eq!(provider.get_service_url(), "pulsar://x:6650");
    }
}
