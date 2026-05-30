// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 client surface (ADR-0032).
//!
//! `PulsarClientV5` is a thin wrapper holding the same engine state as
//! [`crate::PulsarClient`]. The `v4()` escape hatch returns a
//! [`crate::PulsarClient`] borrowing the SAME state — no double-init,
//! no second handshake. Callers can mix V5 and v4 surfaces on the same
//! connection while the V5 surface is still iterating upstream.

use crate::{Engine, PulsarClient, TokioEngine};

/// **Experimental** — PIP-466 V5 client surface (ADR-0032). Behaviour
/// and signatures may change before V5 is promoted to default.
///
/// Holds the same engine state as [`crate::PulsarClient`]. Use the
/// [`Self::v4`] escape hatch to fall back to the v4 surface on the
/// same connection without re-handshaking.
///
/// Engine-generic: `E: Engine` defaults to [`crate::TokioEngine`] so
/// call sites that write `PulsarClientV5` keep resolving to the tokio
/// specialisation; moonpool callers name
/// `PulsarClientV5<MoonpoolEngine<P>>` directly.
pub struct PulsarClientV5<E: Engine = TokioEngine> {
    inner: PulsarClient<E>,
}

impl<E: Engine> std::fmt::Debug for PulsarClientV5<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarClientV5")
            .field("engine", &E::name())
            .finish_non_exhaustive()
    }
}

impl<E: Engine> PulsarClientV5<E> {
    /// Wrap an already-built v4 [`PulsarClient`] in the V5 surface.
    /// The V5 wrapper holds no state of its own — every call delegates
    /// to the wrapped v4 client.
    #[must_use]
    pub fn from_v4(inner: PulsarClient<E>) -> Self {
        Self { inner }
    }

    /// Escape hatch back to the v4 surface. Borrows the same engine
    /// state — useful when the caller needs a v4-only feature (e.g.
    /// `Reader`, `TableView`, transactions) that V5 has not yet lifted.
    #[must_use]
    pub fn v4(&self) -> &PulsarClient<E> {
        &self.inner
    }

    /// Consume the V5 wrapper and return the inner v4 client. Useful
    /// when migrating call sites off the experimental surface.
    #[must_use]
    pub fn into_v4(self) -> PulsarClient<E> {
        self.inner
    }

    /// Start building a V5 [`super::producer::ProducerBuilder`] for the given
    /// topic. The V5 builder accepts `Duration`-typed timeouts and
    /// `Option<usize>` max-pending; the v4 wire equivalents are
    /// computed via [`super::mapping`] at `.create()` time.
    #[must_use]
    pub fn producer(&self, topic: impl Into<String>) -> super::producer::ProducerBuilder<'_, E> {
        super::producer::ProducerBuilder::new(self.inner.producer(topic))
    }

    /// Start building a V5 [`super::stream_consumer::StreamConsumerBuilder`]
    /// (Exclusive / Failover subscriptions; ordered delivery on a
    /// single active consumer per partition). Pre-pins the v4
    /// `subscription_type` to`Exclusive` — callers can flip to
    /// `Failover` via the builder's `failover()` selector.
    #[must_use]
    pub fn stream_consumer(
        &self,
        topic: impl Into<String>,
    ) -> super::stream_consumer::StreamConsumerBuilder<'_, E> {
        super::stream_consumer::StreamConsumerBuilder::new(self.inner.consumer(topic))
    }

    /// Start building a V5 [`super::queue_consumer::QueueConsumerBuilder`]
    /// (Shared / `KeyShared` subscriptions; work-distribution across
    /// multiple active consumers per partition). Pre-pins the v4
    /// `subscription_type` to`Shared` — callers flip to `KeyShared`
    /// via the builder's `key_shared()` selector.
    #[must_use]
    pub fn queue_consumer(
        &self,
        topic: impl Into<String>,
    ) -> super::queue_consumer::QueueConsumerBuilder<'_, E> {
        super::queue_consumer::QueueConsumerBuilder::new(self.inner.consumer(topic))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure type-level assertions: the V5 wrapper accepts the v4
    // client, escape-hatch borrow returns the v4 client unchanged,
    // and `into_v4` consumes back to the v4 client. We don't try to
    // construct a real `PulsarClient` here (that needs a live broker
    // / a `magnetar-fakes` fixture); the type-level surface is what
    // PIP-466 ADR-0032 actually pins.
    #[test]
    fn type_surface_compiles() {
        fn _round_trip(c: PulsarClient) -> PulsarClient {
            PulsarClientV5::from_v4(c).into_v4()
        }
        fn _borrow_v4(v5: &PulsarClientV5) -> &PulsarClient {
            v5.v4()
        }
    }

    /// Compile-time witness that the V5 wrapper is parametric over
    /// `E: Engine` (WAVE 3 lift). Mirrors the tokio assertion above
    /// against the moonpool engine.
    #[cfg(feature = "moonpool")]
    #[test]
    fn moonpool_type_surface_compiles() {
        use moonpool_core::TokioProviders;

        use crate::MoonpoolEngine;
        fn _round_trip(
            c: PulsarClient<MoonpoolEngine<TokioProviders>>,
        ) -> PulsarClient<MoonpoolEngine<TokioProviders>> {
            PulsarClientV5::<MoonpoolEngine<TokioProviders>>::from_v4(c).into_v4()
        }
        fn _borrow_v4(
            v5: &PulsarClientV5<MoonpoolEngine<TokioProviders>>,
        ) -> &PulsarClient<MoonpoolEngine<TokioProviders>> {
            v5.v4()
        }
    }
}
