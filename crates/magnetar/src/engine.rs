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
use std::future::Future;
#[cfg(feature = "moonpool")]
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

/// Marker trait labelling a runtime engine. Implementations select the
/// concrete storage type ([`Self::ClientState`]) that backs the engine's
/// branch of [`crate::PulsarClient<E>`].
///
/// `'static + Send + Sync` mirrors what we already require of producers and
/// consumers; downstream users that hand `PulsarClient<E>` to a tokio
/// `spawn` (or moonpool `spawn`) need at least that.
///
/// # Task and timer primitives (ADR-0025 phase 1)
///
/// The associated [`Self::TaskHandle`] and [`Self::Interval`] types plus the
/// [`Self::spawn`] / [`Self::abort_task`] / [`Self::new_interval`] /
/// [`Self::interval_tick`] methods give the façade an engine-agnostic way to
/// spawn background tasks and drive periodic timers. They are the
/// prerequisite for moving `PartitionedProducer::health_loop`,
/// `TableView::drain_task`, `MultiTopicsConsumer::auto_update`, and the
/// other surface lifts off `impl PulsarClient<TokioEngine>`. See
/// [ADR-0025](../../specs/adr/0025-engine-trait-task-and-timer-primitives.md).
pub trait Engine: 'static + Send + Sync + Debug {
    /// Per-engine state stored inside [`crate::PulsarClient<E>`]. The tokio
    /// engine plugs in [`magnetar_runtime_tokio::Client`]; the moonpool
    /// engine plugs in `(Arc<moonpool::ConnectionShared>,
    /// moonpool::DriverHandle)`. Both bundles are `'static + Send + Sync`
    /// so the façade can be moved across spawn boundaries unchanged.
    type ClientState: 'static + Send + Sync;

    /// Opaque, cancel-safe handle to a background task spawned via
    /// [`Self::spawn`]. Dropping the handle aborts the task on the tokio
    /// engine; explicit [`Self::abort_task`] is the happens-before-Drop
    /// path the façade uses on shutdown.
    type TaskHandle: 'static + Send;

    /// Opaque periodic timer created via [`Self::new_interval`]. The
    /// façade drives ticks via [`Self::interval_tick`].
    type Interval: 'static + Send;

    /// Human-readable engine name, surfaced in logs / panics / errors.
    /// Default returns the Rust type name — engines override to e.g.
    /// `"tokio"` / `"moonpool"`.
    fn name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }

    /// Spawn an async future on the engine's executor. Returns a cancel-
    /// safe [`Self::TaskHandle`]. Tokio wraps [`tokio::spawn`]; moonpool
    /// delegates through its `Providers::TaskProvider` (`moonpool_core`).
    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static;

    /// Abort a spawned task. Idempotent: calling on an already-completed
    /// or already-aborted handle is a no-op.
    fn abort_task(handle: &mut Self::TaskHandle);

    /// Create a periodic timer with `period` between ticks. The first
    /// tick fires immediately (matches `tokio::time::interval`).
    fn new_interval(period: Duration) -> Self::Interval;

    /// Await the next tick. The returned future is `Send` and boxed so
    /// the caller can `.await` from a generic context without exposing
    /// the engine-specific timer shape.
    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
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
    type TaskHandle = tokio::task::JoinHandle<()>;
    type Interval = tokio::time::Interval;

    fn name() -> &'static str {
        "tokio"
    }

    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn abort_task(handle: &mut Self::TaskHandle) {
        handle.abort();
    }

    fn new_interval(period: Duration) -> Self::Interval {
        // tokio's `interval` fires immediately on the first tick; the
        // ADR contract preserves that behaviour.
        tokio::time::interval(period)
    }

    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            interval.tick().await;
        })
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
    // Under both TokioProviders and moonpool-sim's SimProviders the
    // moonpool engine ultimately schedules onto tokio (determinism comes
    // from substituting the providers, not from replacing tokio). The
    // task handle and interval types are therefore the same tokio shapes
    // as the TokioEngine — see ADR-0025 §Decision.
    type TaskHandle = tokio::task::JoinHandle<()>;
    type Interval = tokio::time::Interval;

    fn name() -> &'static str {
        "moonpool"
    }

    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn abort_task(handle: &mut Self::TaskHandle) {
        handle.abort();
    }

    fn new_interval(period: Duration) -> Self::Interval {
        tokio::time::interval(period)
    }

    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            interval.tick().await;
        })
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

    // -------------------------------------------------------------
    // ADR-0025 phase 1: task + timer primitive smoke tests. One pair
    // per engine — keeps the per-engine test count balanced even
    // though the new primitives don't yet have façade callers.

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_spawn_and_abort_round_trip() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let handle = <TokioEngine as Engine>::spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
        // Drive the spawned task once.
        tokio::task::yield_now().await;
        // Awaiting the JoinHandle works on a non-aborted task.
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Spawn a second task that we abort before it can increment.
        let c2 = counter.clone();
        let mut handle2 = <TokioEngine as Engine>::spawn(async move {
            // Sleep forever — abort wins.
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            c2.fetch_add(1, Ordering::SeqCst);
        });
        <TokioEngine as Engine>::abort_task(&mut handle2);
        // Second abort is a no-op.
        <TokioEngine as Engine>::abort_task(&mut handle2);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "aborted task must not run its body",
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        let mut interval = <TokioEngine as Engine>::new_interval(Duration::from_secs(10));
        let start = tokio::time::Instant::now();
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        // First tick fires immediately per the tokio interval contract.
        assert_eq!(
            tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on tokio",
        );
        // Second tick waits for the period.
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        assert!(
            tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }

    #[cfg(feature = "moonpool")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn moonpool_engine_spawn_and_abort_round_trip() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use moonpool_core::TokioProviders;

        type E = MoonpoolEngine<TokioProviders>;

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let handle = <E as Engine>::spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let c2 = counter.clone();
        let mut handle2 = <E as Engine>::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            c2.fetch_add(1, Ordering::SeqCst);
        });
        <E as Engine>::abort_task(&mut handle2);
        <E as Engine>::abort_task(&mut handle2);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "aborted task must not run its body",
        );
    }

    #[cfg(feature = "moonpool")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn moonpool_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        use moonpool_core::TokioProviders;

        type E = MoonpoolEngine<TokioProviders>;

        let mut interval = <E as Engine>::new_interval(Duration::from_secs(10));
        let start = tokio::time::Instant::now();
        <E as Engine>::interval_tick(&mut interval).await;
        assert_eq!(
            tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on moonpool",
        );
        <E as Engine>::interval_tick(&mut interval).await;
        assert!(
            tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }
}
