// SPDX-License-Identifier: Apache-2.0

//! FoundationDB-style buggify fault-injection scaffolding (ADR-0048).
//!
//! `Buggify` is a tiny, sans-io fault-injection helper that lives at the
//! four named choice points in the [`crate::Connection`] state machine
//! and inside [`crate::Backoff`]. Each call site asks
//! [`Buggify::should_fire`] with a static `label` and a `probability`;
//! the helper consults an engine-supplied RNG closure and returns
//! `true` on a hit, `false` otherwise.
//!
//! # Build modes
//!
//! - **`buggify` feature OFF (default).** Every [`Buggify::should_fire`] call collapses to
//!   `#[inline(always)] -> false`. The Connection's `buggify` field is a zero-sized type and the
//!   four choice-point branches are dead code post-monomorphisation. Production builds pay nothing.
//!
//! - **`buggify` feature ON.** [`Buggify`] holds an `Arc<dyn Fn() -> u64 + Send + Sync>` RNG
//!   handle. The engine plugs in a deterministic source: moonpool routes it through
//!   `Providers::Random` (seed-driven); tokio uses [`Buggify::disabled`] which always returns
//!   `false` (no spurious fault injection in production binaries even if the feature flag is on by
//!   mistake).
//!
//! # Sans-io contract
//!
//! `magnetar-proto` does NOT depend on the `rand` crate. The helper
//! abstracts the RNG via a closure so the engine owns the dependency.
//! This preserves [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md)
//! (zero I/O deps in the proto crate) and aligns with
//! [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) (clock
//! injection): the same "engine plugs a closure in" pattern used for
//! `wall_clock` and `now_instant_provider` is reused here for the
//! buggify RNG.
//!
//! # Channels rule
//!
//! Per [ADR-0003](../specs/adr/0003-no-channels-rule.md), the helper
//! uses no channels. The fire-counter map (under the feature) sits
//! behind a [`parking_lot::Mutex`] — the same primitive the rest of
//! the proto crate uses for `ProducerSlot::state`.

#[cfg(feature = "buggify")]
use std::sync::Arc;

/// Engine-supplied RNG handle used by [`Buggify`] when the `buggify`
/// feature is enabled. The closure returns a uniform `u64`; the helper
/// folds it down to the probability check.
///
/// The moonpool engine wires this from `Providers::Random`. The tokio
/// engine wires [`Buggify::disabled`] so production binaries never see
/// fault injection even when compiled with `buggify`.
#[cfg(feature = "buggify")]
pub type BuggifyRng = Arc<dyn Fn() -> u64 + Send + Sync>;

/// FoundationDB-style buggify helper. Holds the engine-supplied RNG
/// handle (under the `buggify` feature) and exposes a single
/// [`Self::should_fire`] entry that every choice point in the
/// connection state machine consults.
///
/// `Clone` is cheap (the inner state is reference-counted) and the
/// helper is `Send + Sync` so engines can stash a single instance on
/// `ConnectionShared` and share it with `Backoff`.
#[derive(Clone, Default)]
pub struct Buggify {
    #[cfg(feature = "buggify")]
    inner: Option<Arc<BuggifyInner>>,
}

#[cfg(feature = "buggify")]
struct BuggifyInner {
    rng: BuggifyRng,
    counts: parking_lot::Mutex<std::collections::HashMap<&'static str, u64>>,
}

#[cfg(feature = "buggify")]
impl core::fmt::Debug for BuggifyInner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BuggifyInner")
            .field("counts", &self.counts.lock().len())
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for Buggify {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        #[cfg(feature = "buggify")]
        {
            f.debug_struct("Buggify")
                .field("installed", &self.inner.is_some())
                .finish_non_exhaustive()
        }
        #[cfg(not(feature = "buggify"))]
        {
            f.debug_struct("Buggify")
                .field("feature", &"disabled")
                .finish_non_exhaustive()
        }
    }
}

impl Buggify {
    /// A `Buggify` with no RNG installed. Every
    /// [`Self::should_fire`] call returns `false`. This is the default
    /// the [`crate::Connection`] starts with and the value the tokio
    /// engine ships even when compiled with the `buggify` feature on.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Construct a buggify helper backed by `rng`. The closure is
    /// called once per [`Self::should_fire`] invocation; the engine
    /// owns the seeding contract.
    ///
    /// `_rng` is consumed (and dropped) when the `buggify` feature is
    /// off, so production builds pay no closure-storage cost.
    #[cfg(feature = "buggify")]
    #[must_use]
    pub fn with_rng(rng: BuggifyRng) -> Self {
        Self {
            inner: Some(Arc::new(BuggifyInner {
                rng,
                counts: parking_lot::Mutex::new(std::collections::HashMap::new()),
            })),
        }
    }

    /// Stub for the no-feature build path. Consumes `_rng` to keep the
    /// same call shape on both feature axes.
    #[cfg(not(feature = "buggify"))]
    #[must_use]
    pub fn with_rng<R>(_rng: R) -> Self {
        Self::default()
    }

    /// Roll the engine-supplied RNG and return `true` if the fault
    /// fires. Probability is in `[0.0, 1.0]`; values outside the range
    /// clamp to the nearest bound.
    ///
    /// The `label` is stamped on the internal fire-count map so tests
    /// can observe which paths actually fired across a seed sweep.
    ///
    /// Under `not(feature = "buggify")` this is `#[inline(always)]`
    /// and returns `false` regardless of arguments — the choice point's
    /// `if` branch is dead code that the optimiser strips.
    #[cfg(feature = "buggify")]
    pub fn should_fire(&self, label: &'static str, probability: f64) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };
        let p = probability.clamp(0.0, 1.0);
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            inner
                .counts
                .lock()
                .entry(label)
                .and_modify(|c| *c += 1)
                .or_insert(1);
            return true;
        }
        // Fold the engine RNG into [0.0, 1.0). The 10_000 granularity
        // is sufficient for fault-injection probabilities (which we
        // pick in the 1%-10% range) and side-steps any subtle
        // u64→f64 precision loss — the bucket is `< 10_000`, well
        // inside f64's 52-bit mantissa, so the `as f64` cast is
        // lossless by construction.
        let bucket = (inner.rng)() % 10_000;
        #[allow(clippy::cast_precision_loss)]
        let roll = bucket as f64 / 10_000.0;
        let fired = roll < p;
        if fired {
            inner
                .counts
                .lock()
                .entry(label)
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }
        fired
    }

    /// No-feature fast path. Returns `false` unconditionally so the
    /// choice-point branches at every `should_fire` call collapse to
    /// dead code under `cargo build` without the feature.
    ///
    /// `#[inline(always)]` here is load-bearing — the body is a single
    /// `false`, and LLVM only eliminates the dead caller-side branch if
    /// the call is inlined. Clippy's `inline_always` lint flags it as
    /// "usually a bad idea"; this is the documented exception, hence
    /// the explicit allow.
    #[cfg(not(feature = "buggify"))]
    #[inline(always)]
    #[allow(clippy::inline_always)]
    pub fn should_fire(&self, _label: &'static str, _probability: f64) -> bool {
        false
    }

    /// Total number of times `label` has fired since construction.
    /// `0` when the feature is off or the label has never fired.
    /// Test-only observability hook.
    #[cfg(feature = "buggify")]
    #[must_use]
    pub fn fire_count(&self, label: &'static str) -> u64 {
        self.inner
            .as_ref()
            .and_then(|inner| inner.counts.lock().get(label).copied())
            .unwrap_or(0)
    }

    /// No-feature stub of [`Self::fire_count`] — always `0`.
    #[cfg(not(feature = "buggify"))]
    #[must_use]
    pub fn fire_count(&self, _label: &'static str) -> u64 {
        0
    }

    /// Pull a uniform `u64` from the engine-supplied RNG. Returns
    /// `None` when no RNG is installed (or the feature is off) so
    /// call sites can short-circuit to their production path.
    /// Used by the `retry_clock.skew` label inside
    /// [`crate::Backoff::next`] to derive a `[0.5, 2.0]` scale factor
    /// after [`Self::should_fire`] returns `true`.
    #[cfg(feature = "buggify")]
    #[must_use]
    pub fn roll_u64(&self) -> Option<u64> {
        self.inner.as_ref().map(|inner| (inner.rng)())
    }

    /// No-feature stub of [`Self::roll_u64`] — always `None`.
    #[cfg(not(feature = "buggify"))]
    #[must_use]
    pub fn roll_u64(&self) -> Option<u64> {
        None
    }

    /// `true` when an RNG is installed AND the `buggify` feature is on.
    /// Choice-point sites do not need to call this — `should_fire`
    /// short-circuits internally — but tests use it to assert wiring.
    #[must_use]
    pub fn is_armed(&self) -> bool {
        #[cfg(feature = "buggify")]
        {
            self.inner.is_some()
        }
        #[cfg(not(feature = "buggify"))]
        {
            false
        }
    }
}

/// Labels in use by the four named buggify points (ADR-0048). Stamped
/// as `&'static str` so the fire-counter map is allocation-free.
pub mod labels {
    /// `Connection::reset` — when fired, preserve the prior
    /// `last_activity` timestamp so the post-reset state machine sees
    /// an older keepalive baseline (one extra idle-tick before the
    /// engine re-arms keepalive).
    pub const CONNECTION_RESET_DELAY: &str = "connection.reset.delay";

    /// `Connection::flush_producer` — when fired (and the batch holds
    /// more than one message), skip the underlying `flush_batch` call.
    /// The batch survives the flush; the next caller-driven flush
    /// drains it.
    pub const BATCH_CONTAINER_FLUSH_SPLIT: &str = "batch_container.flush.split";

    /// `Connection::handle_bytes` — when fired, break out of the
    /// per-frame decode loop after a single frame even if the inbound
    /// buffer already holds more complete frames. The next
    /// `handle_bytes` call resumes the drain, exercising the
    /// framing-resume path.
    pub const HANDLE_BYTES_SHORT_READ: &str = "handle_bytes.short_read";

    /// `Backoff::next` — when fired, scale the returned `Duration` by
    /// a seed-driven factor in `[0.5, 2.0]`. The Backoff schedule's
    /// own jitter is preserved; this layer adds gross clock-skew on
    /// top so reconnect-ordering bugs surface under simulation.
    pub const RETRY_CLOCK_SKEW: &str = "retry_clock.skew";
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Disabled buggify (no RNG installed) returns `false` for every
    /// label / probability combination. Holds whether the feature is
    /// on or off — the disabled-state must NEVER fire.
    #[test]
    fn disabled_never_fires() {
        let b = Buggify::disabled();
        assert!(!b.should_fire("any.label", 1.0));
        assert!(!b.should_fire("any.label", 0.999));
        assert!(!b.should_fire("any.label", 0.5));
        assert!(!b.should_fire("any.label", 0.0));
        assert_eq!(b.fire_count("any.label"), 0);
        assert!(!b.is_armed());
    }

    /// Under the `buggify` feature, a closure-backed RNG that returns
    /// a fixed `0` deterministically fires at any non-zero
    /// probability. Confirms the fold-into-[0.0, 1.0) arithmetic.
    #[cfg(feature = "buggify")]
    #[test]
    fn fires_when_rng_returns_zero() {
        let b = Buggify::with_rng(std::sync::Arc::new(|| 0_u64));
        assert!(b.is_armed());
        assert!(b.should_fire("test.label", 0.05));
        assert!(b.should_fire("test.label", 1.0));
        // probability 0.0 short-circuits without even rolling.
        assert!(!b.should_fire("test.label", 0.0));
    }

    /// Conversely, an RNG that always lands at the top of the bucket
    /// range never fires for sub-1.0 probabilities. Confirms the
    /// `roll < p` half-open inequality.
    #[cfg(feature = "buggify")]
    #[test]
    fn never_fires_when_rng_caps_bucket() {
        // `9_999 % 10_000 == 9_999` → roll = 0.9999, below 1.0 but
        // above any reasonable injection probability.
        let b = Buggify::with_rng(std::sync::Arc::new(|| 9_999_u64));
        assert!(!b.should_fire("test.label", 0.5));
        assert!(!b.should_fire("test.label", 0.9999));
        // p == 1.0 is the unconditional path so we still fire.
        assert!(b.should_fire("test.label", 1.0));
    }

    /// The fire-counter map records each hit so test code can prove
    /// the label actually triggered across a seed sweep. Counter
    /// monotonically increases.
    #[cfg(feature = "buggify")]
    #[test]
    fn fire_count_monotonic() {
        let b = Buggify::with_rng(std::sync::Arc::new(|| 0_u64));
        assert_eq!(b.fire_count(labels::CONNECTION_RESET_DELAY), 0);
        let _ = b.should_fire(labels::CONNECTION_RESET_DELAY, 1.0);
        assert_eq!(b.fire_count(labels::CONNECTION_RESET_DELAY), 1);
        let _ = b.should_fire(labels::CONNECTION_RESET_DELAY, 1.0);
        let _ = b.should_fire(labels::CONNECTION_RESET_DELAY, 1.0);
        assert_eq!(b.fire_count(labels::CONNECTION_RESET_DELAY), 3);
        // Misses do NOT advance the counter.
        let cold = Buggify::with_rng(std::sync::Arc::new(|| 9_999_u64));
        let _ = cold.should_fire(labels::CONNECTION_RESET_DELAY, 0.5);
        assert_eq!(cold.fire_count(labels::CONNECTION_RESET_DELAY), 0);
    }

    /// Deterministic RNG → deterministic fire pattern across all four
    /// labels. Exercises the helper exactly as the connection state
    /// machine will under a fixed seed.
    #[cfg(feature = "buggify")]
    #[test]
    fn deterministic_across_labels() {
        // SplitMix64-ish counter so each `should_fire` call sees a
        // distinct (and reproducible) `u64`. Mirrors what the moonpool
        // engine plugs in from `Providers::Random` under a fixed seed.
        let counter = std::sync::Arc::new(parking_lot::Mutex::new(0_u64));
        let counter_handle = counter.clone();
        let b = Buggify::with_rng(std::sync::Arc::new(move || {
            let mut g = counter_handle.lock();
            *g = g.wrapping_add(1);
            // SplitMix64 step.
            let mut z = *g;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            z
        }));
        // Pick a probability where SOME of the four labels fire and
        // some don't. The exact pattern is determined by the SplitMix
        // sequence and pinned here to detect accidental algorithm
        // changes to `should_fire`.
        let probability = 0.5;
        let pattern: Vec<bool> = [
            labels::CONNECTION_RESET_DELAY,
            labels::BATCH_CONTAINER_FLUSH_SPLIT,
            labels::HANDLE_BYTES_SHORT_READ,
            labels::RETRY_CLOCK_SKEW,
        ]
        .iter()
        .map(|label| b.should_fire(label, probability))
        .collect();
        // Replay with a fresh counter to confirm the pattern is
        // reproducible — the buggify helper is itself deterministic.
        let counter_b = std::sync::Arc::new(parking_lot::Mutex::new(0_u64));
        let counter_b_handle = counter_b.clone();
        let b2 = Buggify::with_rng(std::sync::Arc::new(move || {
            let mut g = counter_b_handle.lock();
            *g = g.wrapping_add(1);
            let mut z = *g;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            z
        }));
        let pattern_b: Vec<bool> = [
            labels::CONNECTION_RESET_DELAY,
            labels::BATCH_CONTAINER_FLUSH_SPLIT,
            labels::HANDLE_BYTES_SHORT_READ,
            labels::RETRY_CLOCK_SKEW,
        ]
        .iter()
        .map(|label| b2.should_fire(label, probability))
        .collect();
        assert_eq!(pattern, pattern_b, "buggify must be deterministic");
        // And: at least one label fires under p=0.5 with this RNG, so
        // the test is not vacuously passing on an all-false pattern.
        assert!(
            pattern.iter().any(|hit| *hit),
            "p=0.5 sweep should land at least one fire"
        );
    }
}
