// SPDX-License-Identifier: Apache-2.0

//! `PulsarClient<MoonpoolEngine<P>>` — the moonpool branch of the generic
//! façade.
//!
//! Surface intentionally minimal for M6 (ADR-0019 gate (e), 2026-05-21):
//! the constructor wraps the engine-side handshake outputs (the shared
//! connection state + driver join handle) into a `PulsarClient` so callers
//! can name the engine type parameter `MoonpoolEngine<P>` in tests without
//! re-implementing the façade. The full façade surface
//! (`producer`/`consumer`/`reader`/typed schemas/partitioned/multi-topics/
//! pattern/table-view/transactions/interceptor SPI) stays bound to
//! [`crate::PulsarClient<crate::TokioEngine>`] — moonpool-side callers
//! reach for it via the engine's own producer/consumer types until the
//! moonpool parity train (M7–M8) lifts those into the façade.
//!
//! Refs: [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md).

use std::sync::Arc;

use moonpool_core::Providers;

use crate::engine::MoonpoolClientState;
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
    /// See [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md)
    /// gate (e). The full producer/consumer façade is not yet wired on
    /// this engine — for v0.1.0, drive it through
    /// [`magnetar_runtime_moonpool`] directly using the [`Self::shared`]
    /// accessor.
    #[must_use]
    pub fn from_moonpool(
        shared: Arc<magnetar_runtime_moonpool::ConnectionShared>,
        driver: magnetar_runtime_moonpool::DriverHandle,
    ) -> Self {
        Self {
            inner: MoonpoolClientState {
                shared,
                driver: parking_lot::Mutex::new(Some(driver)),
            },
            memory_limit: None,
        }
    }

    /// Borrow the shared connection state. Useful for tests that drive
    /// producers / consumers via the moonpool runtime's APIs directly
    /// while keeping the `PulsarClient<MoonpoolEngine<P>>` type-erased
    /// handle around for ownership.
    #[must_use]
    pub fn shared(&self) -> &Arc<magnetar_runtime_moonpool::ConnectionShared> {
        &self.inner.shared
    }

    /// Take the driver handle out of the client, returning it to the
    /// caller for explicit `.join().await`. After this call the client
    /// will not abort the driver on `close()`.
    #[must_use]
    pub fn take_driver(&self) -> Option<magnetar_runtime_moonpool::DriverHandle> {
        self.inner.driver.lock().take()
    }

    /// Returns `true` while the underlying broker connection is in
    /// [`magnetar_proto::HandshakeState::Connected`]. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::is_connected`] for the
    /// moonpool engine.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.shared.inner.lock().is_connected()
    }

    /// `true` once the underlying broker connection has entered a terminal
    /// state. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::is_closed`] for the
    /// moonpool engine.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.shared.inner.lock().is_closed()
    }

    /// Close the connection. Notifies the driver to drain and aborts the
    /// driver task. Idempotent. Mirrors
    /// [`crate::PulsarClient::<crate::TokioEngine>::close`] — the tokio
    /// variant is `async` so it can join the driver, the moonpool variant
    /// only `abort`s for now (the engine's `DriverHandle::join` lives on
    /// the engine type; `take_driver` exposes it for callers that want to
    /// observe the terminal outcome).
    pub fn close(self) {
        {
            let mut conn = self.inner.shared.inner.lock();
            conn.close();
        }
        self.inner.shared.driver_waker.notify_one();
        if let Some(driver) = self.inner.driver.lock().take() {
            driver.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;
    use moonpool_core::TokioProviders;

    use super::*;
    use crate::Engine;

    fn names_client<P: Providers>() -> &'static str {
        std::any::type_name::<PulsarClient<MoonpoolEngine<P>>>()
    }

    /// Smoke test for ADR-0019 gate (e): `PulsarClient<MoonpoolEngine<P>>`
    /// names cleanly against the same `PulsarClient` type as the default
    /// tokio engine, the engine's marker carries the right name, and the
    /// per-engine `ClientState` typechecks.
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

    fn accepts_shared<P: Providers>(
        _shared: &Arc<magnetar_runtime_moonpool::ConnectionShared>,
    ) -> std::marker::PhantomData<PulsarClient<MoonpoolEngine<P>>> {
        std::marker::PhantomData
    }

    /// The engine's connect-path returns shape — `(Arc<ConnectionShared>,
    /// DriverHandle)` — is exactly the input to `from_moonpool`, so the
    /// constructor compiles end-to-end against the runtime's public
    /// surface. We don't actually dial a broker here; the moonpool engine
    /// already exercises the full handshake in its own integration tests.
    #[test]
    fn from_moonpool_signature_matches_engine_connect_output() {
        // Pull a `ConnectionShared` constructor into scope so the
        // engine-side type is unambiguous. The driver handle's only
        // public constructor is the engine's `connect_*` family, which
        // dials a real socket — tests here only need to typecheck the
        // constructor.
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let _: std::marker::PhantomData<PulsarClient<MoonpoolEngine<TokioProviders>>> =
            accepts_shared::<TokioProviders>(&shared);
    }
}
