// SPDX-License-Identifier: Apache-2.0

//! `Engine` trait — the abstraction the public [`crate::PulsarClient`] is
//! generic over.
//!
//! `Engine` is a marker trait with a single associated type
//! ([`Engine::ClientState`]) that selects the per-engine storage backing
//! [`crate::PulsarClient<E>`]. Today the two implementations are
//! [`TokioEngine`] (production, default) and [`MoonpoolEngine<P>`]
//! (deterministic simulation; `P` is the
//! [`moonpool_core::Providers`](moonpool_core::Providers) bundle).
//!
//! Engine-specific methods (`producer`, `consumer`, partitioned, …) live in
//! dedicated `impl PulsarClient<ConcreteEngine>` blocks rather than on the
//! trait — production engines have wildly different connect signatures
//! (tokio takes a URL, moonpool takes `host:port` + a `Providers` bundle)
//! and trying to surface those through a single trait would either lose
//! typing or reintroduce the per-engine façade duplication
//! [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md)
//! rejected as Option B.
//!
//! Instead, moonpool callers that reach for a tokio-only method get a
//! clean trait-bound error rather than a silent fallback — exactly the
//! ADR-0019 §Decision contract for v0.1.0.
//!
//! See ADR-0019 gate (e) — "Option A: generic `PulsarClient<E: Engine>`
//! with default `E = TokioEngine`" — for the rationale.

use std::fmt::Debug;
#[cfg(feature = "moonpool")]
use std::marker::PhantomData;

/// Marker trait labelling a runtime engine. Implementations select the
/// concrete storage type ([`Self::ClientState`]) that backs the engine's
/// branch of [`crate::PulsarClient<E>`].
///
/// `'static + Send + Sync` mirrors what we already require of producers and
/// consumers; downstream users that hand `PulsarClient<E>` to a tokio
/// `spawn` (or moonpool `spawn`) need at least that.
pub trait Engine: 'static + Send + Sync + Debug {
    /// Per-engine state stored inside [`crate::PulsarClient<E>`]. The tokio
    /// engine plugs in [`magnetar_runtime_tokio::Client`]; the moonpool
    /// engine plugs in `(Arc<moonpool::ConnectionShared>,
    /// moonpool::DriverHandle)`. Both bundles are `'static + Send + Sync`
    /// so the façade can be moved across spawn boundaries unchanged.
    type ClientState: 'static + Send + Sync;

    /// Human-readable engine name, surfaced in logs / panics / errors.
    /// Default returns the Rust type name — engines override to e.g.
    /// `"tokio"` / `"moonpool"`.
    fn name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

/// Zero-sized marker for the tokio production engine. Default `E` on
/// [`crate::PulsarClient<E>`].
///
/// Available behind the `tokio` feature (default-on).
#[cfg(feature = "tokio")]
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEngine;

#[cfg(feature = "tokio")]
impl Engine for TokioEngine {
    type ClientState = magnetar_runtime_tokio::Client;

    fn name() -> &'static str {
        "tokio"
    }
}

/// Zero-sized marker for the moonpool deterministic-simulation engine,
/// parametrised by the [`moonpool_core::Providers`] bundle the underlying
/// driver runs on.
///
/// Available behind the `moonpool` feature. `P` is the providers bundle —
/// `TokioProviders` for production-ish runs and a `moonpool-sim`
/// `SimProviders` for chaos-tested reproducible test suites.
#[cfg(feature = "moonpool")]
pub struct MoonpoolEngine<P: moonpool_core::Providers> {
    _marker: PhantomData<fn() -> P>,
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Default for MoonpoolEngine<P> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// Hand-rolled `Clone` so the bound `P: Providers` doesn't propagate through
// `derive(Clone)` — the marker holds no value, so cloning is just
// reconstructing the phantom.
#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Clone for MoonpoolEngine<P> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Debug for MoonpoolEngine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoonpoolEngine").finish_non_exhaustive()
    }
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Engine for MoonpoolEngine<P> {
    type ClientState = MoonpoolClientState;

    fn name() -> &'static str {
        "moonpool"
    }
}

/// Per-engine storage for [`crate::PulsarClient<MoonpoolEngine<P>>`] — the
/// shared connection state plus the driver join handle, in line with the
/// pair the engine's `connect_*` calls return.
///
/// Lives at the façade boundary (not inside `magnetar-runtime-moonpool`) so
/// the moonpool crate's public surface stays oriented around the engine's
/// own `(Arc<ConnectionShared>, DriverHandle)` return shape rather than a
/// façade-coupled bundle.
#[cfg(feature = "moonpool")]
#[derive(Debug)]
pub struct MoonpoolClientState {
    /// Shared connection state — the sans-io [`magnetar_proto::Connection`]
    /// behind a non-async mutex plus the driver wakeup.
    pub shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
    /// Driver-task handle returned by
    /// [`magnetar_runtime_moonpool::MoonpoolEngine::connect_plain`]. The
    /// façade keeps it alive for the lifetime of the
    /// [`crate::PulsarClient`].
    pub driver: parking_lot::Mutex<Option<magnetar_runtime_moonpool::DriverHandle>>,
}

// `PhantomData<fn() -> P>` keeps the engine `Send + Sync` regardless of
// `P`'s thread-safety story. The marker is a witness type, not a value
// holder — engine state actually lives on `PulsarClient<E>`.

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_engine_implements_engine() {
        fn takes_engine<E: Engine>() -> &'static str {
            E::name()
        }
        assert_eq!(takes_engine::<TokioEngine>(), "tokio");
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TokioEngine>();
    }

    #[cfg(feature = "moonpool")]
    #[test]
    fn moonpool_engine_implements_engine() {
        use moonpool_core::TokioProviders;
        fn takes_engine<E: Engine>() -> &'static str {
            E::name()
        }
        assert_eq!(takes_engine::<MoonpoolEngine<TokioProviders>>(), "moonpool");
    }

    #[cfg(feature = "moonpool")]
    #[test]
    fn moonpool_engine_is_send_sync() {
        use moonpool_core::TokioProviders;
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MoonpoolEngine<TokioProviders>>();
    }
}
