// SPDX-License-Identifier: Apache-2.0

//! `PulsarClient<MoonpoolEngine<P>>` — the moonpool branch of the generic
//! façade.
//!
//! `MoonpoolEngine<P>::ClientState` is
//! [`magnetar_runtime_moonpool::Client<P>`] directly — same shape as the
//! tokio branch where `TokioEngine::ClientState = magnetar_runtime_tokio::Client`.
//! The runtime `Client<P>` already implements the surface extension
//! traits (`SubscribeApi`, `CreateProducerApi`, `TransactionApi`, …),
//! so the full façade — `producer()` / `consumer()` / `reader()` builders
//! and everything downstream — is available on the moonpool engine
//! without a parallel state struct.
//!
//! Refs: [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md)
//! gate (e) and [ADR-0026](../../specs/adr/0026-engine-trait-extension-strategy.md) §D1.

use std::sync::Arc;

use moonpool_core::Providers;

use crate::{MoonpoolEngine, PulsarClient};

impl<P: Providers> PulsarClient<MoonpoolEngine<P>> {
    /// Construct a moonpool-engine-backed [`PulsarClient`] from the
    /// `(shared, driver)` pair that
    /// [`magnetar_runtime_moonpool::MoonpoolEngine::connect_plain`] and its
    /// supervised / TLS variants already return.
    ///
    /// Owns the [`magnetar_runtime_moonpool::DriverHandle`] until
    /// [`Self::close`] is called — dropping the client without calling
    /// `close` leaves the driver task running until the runtime shuts it
    /// down (matches the tokio-engine behaviour).
    ///
    /// Internally wraps the pair in a
    /// [`magnetar_runtime_moonpool::Client<P>`] (the engine's
    /// `ClientState`).
    #[must_use]
    pub fn from_moonpool(
        shared: Arc<magnetar_runtime_moonpool::ConnectionShared>,
        driver: magnetar_runtime_moonpool::DriverHandle,
    ) -> Self {
        Self {
            inner: magnetar_runtime_moonpool::Client::from_parts(shared, driver),
            memory_limit: None,
        }
    }

    /// Construct directly from an already-built moonpool runtime client.
    /// Useful when callers want to drive
    /// [`magnetar_runtime_moonpool::Client::connect_plain`] themselves and
    /// only later wrap the result in the façade.
    #[must_use]
    pub fn from_runtime_client(client: magnetar_runtime_moonpool::Client<P>) -> Self {
        Self {
            inner: client,
            memory_limit: None,
        }
    }

    /// Borrow the inner runtime client. Useful when callers need to reach
    /// runtime-only APIs without losing the façade handle.
    #[must_use]
    pub fn runtime_client(&self) -> &magnetar_runtime_moonpool::Client<P> {
        &self.inner
    }

    /// Borrow the shared connection state. Useful for tests that drive
    /// producers / consumers via the moonpool runtime's APIs directly
    /// while keeping the `PulsarClient<MoonpoolEngine<P>>` type-erased
    /// handle around for ownership.
    #[must_use]
    pub fn shared(&self) -> &Arc<magnetar_runtime_moonpool::ConnectionShared> {
        self.inner.shared()
    }

    /// Take the driver handle out of the client, returning it to the
    /// caller for explicit `.join().await`. After this call the client
    /// will not abort the driver on `close()`.
    #[must_use]
    pub fn take_driver(&self) -> Option<magnetar_runtime_moonpool::DriverHandle> {
        self.inner.take_driver()
    }

    /// Returns `true` while the underlying broker connection is in
    /// [`magnetar_proto::HandshakeState::Connected`]. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::is_connected`] for the
    /// moonpool engine.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// `true` once the underlying broker connection has entered a terminal
    /// state. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::is_closed`] for the
    /// moonpool engine.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Close the connection. Notifies the driver to drain and joins the
    /// driver task. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::close`].
    pub async fn close(self) {
        self.inner.close().await;
    }
}

#[cfg(test)]
mod tests {
    use moonpool_core::TokioProviders;

    use super::*;
    use crate::Engine;

    fn names_client<P: Providers>() -> &'static str {
        std::any::type_name::<PulsarClient<MoonpoolEngine<P>>>()
    }

    /// `PulsarClient<MoonpoolEngine<P>>` names cleanly against the same
    /// `PulsarClient` type as the default tokio engine, and the engine's
    /// marker carries the right name.
    #[test]
    fn pulsar_client_moonpool_engine_can_be_named() {
        assert_eq!(MoonpoolEngine::<TokioProviders>::name(), "moonpool");
        let n = names_client::<TokioProviders>();
        assert!(n.contains("PulsarClient"));
        assert!(n.contains("MoonpoolEngine"));
    }

    /// `MoonpoolEngine` is `Send + Sync + 'static` regardless of `P` so a
    /// `PulsarClient<MoonpoolEngine<P>>` can be moved across spawn
    /// boundaries.
    #[test]
    fn pulsar_client_moonpool_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PulsarClient<MoonpoolEngine<TokioProviders>>>();
    }

    /// The runtime `Client<P>` now serves as `MoonpoolEngine::ClientState`,
    /// so the façade builder traits dispatch through the existing
    /// runtime impls without a separate state struct.
    #[test]
    fn moonpool_engine_client_state_is_runtime_client() {
        fn assert_same<T, U>()
        where
            T: 'static,
            U: 'static,
        {
            assert_eq!(
                std::any::TypeId::of::<T>(),
                std::any::TypeId::of::<U>(),
                "moonpool ClientState should be the runtime Client<P>",
            );
        }
        assert_same::<
            <MoonpoolEngine<TokioProviders> as Engine>::ClientState,
            magnetar_runtime_moonpool::Client<TokioProviders>,
        >();
    }

    /// Compile-time witness: the engine-generic builder entry points on
    /// `PulsarClient<E>` (`producer` / `consumer` / `reader`) typecheck
    /// for `MoonpoolEngine<P>`, AND calling `.create()` / `.subscribe()`
    /// satisfies the `CreateProducerApi` / `SubscribeApi` bounds via the
    /// runtime `Client<P>` trait impls. The function body is never run —
    /// we only need it to typecheck. `P` is constrained the same way the
    /// trait impls in `engine.rs` constrain it.
    #[allow(dead_code)]
    fn moonpool_builder_dispatch_compiles<P: Providers + Send + Sync + 'static>(
        client: &PulsarClient<MoonpoolEngine<P>>,
    ) {
        // Future is never polled — the `async` block exists purely so the
        // generic builder calls are typechecked against `MoonpoolEngine<P>`.
        drop(async {
            let produced = client.producer("t").create().await;
            let subscribed = client.consumer("t").subscription("s").subscribe().await;
            let reader = client.reader("t").create().await;
            (produced, subscribed, reader)
        });
    }
}
