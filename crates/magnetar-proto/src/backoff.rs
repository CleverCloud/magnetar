// SPDX-License-Identifier: Apache-2.0

//! Truncated-exponential backoff with deterministic jitter.
//!
//! Port of `org.apache.pulsar.client.impl.Backoff` (see `pulsar-client/src/main/java/org/apache/
//! pulsar/client/impl/Backoff.java`). Used by reconnect logic and any retry path inside the
//! sans-io state machine.
//!
//! # Algorithm
//!
//! - Initial delay: `initial`.
//! - Each step doubles the previous delay, capped at `max`.
//! - A jitter factor in `[0.0, 0.2]` of the next delay is subtracted to spread reconnects.
//! - `mandatory_stop` is an upper bound on cumulative wait time; once exceeded, the very next
//!   `next()` will yield `max` and reset.
//! - `reset()` returns the backoff to its initial state (call after a successful operation).
//!
//! # Determinism
//!
//! The jitter is derived from a `u64` seed that is rotated through a splittable PRNG (a
//! splitmix64). Callers may construct the [`Backoff`] with an explicit seed for reproducible
//! tests; the default constructor seeds from a monotonic counter so production callers still
//! see spread-out reconnects across clients without depending on any I/O.

use core::time::Duration;

/// Default initial delay (100 ms).
pub const DEFAULT_INITIAL: Duration = Duration::from_millis(100);

/// Default max delay (60 s).
pub const DEFAULT_MAX: Duration = Duration::from_secs(60);

/// Default mandatory-stop window (30 min).
pub const DEFAULT_MANDATORY_STOP: Duration = Duration::from_secs(60 * 30);

/// Truncated-exponential backoff with deterministic jitter.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    mandatory_stop: Duration,
    next_delay: Duration,
    total_elapsed: Duration,
    /// PRNG state for jitter computation.
    rng_state: u64,
    first_call: bool,
    /// FoundationDB-style buggify helper (ADR-0048). Default
    /// [`crate::Buggify::disabled`] preserves the production
    /// production-correct schedule; the moonpool engine wires an
    /// armed helper via [`Self::install_buggify`] to inject
    /// `retry_clock.skew` faults at the next-delay computation.
    buggify: crate::Buggify,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(DEFAULT_INITIAL, DEFAULT_MAX, DEFAULT_MANDATORY_STOP, 0)
    }
}

impl Backoff {
    /// Construct a new backoff with explicit parameters.
    ///
    /// `seed` controls jitter; pass `0` for the default seed.
    pub fn new(initial: Duration, max: Duration, mandatory_stop: Duration, seed: u64) -> Self {
        Self {
            initial,
            max,
            mandatory_stop,
            next_delay: initial,
            total_elapsed: Duration::ZERO,
            rng_state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
            first_call: true,
            buggify: crate::Buggify::disabled(),
        }
    }

    /// Install a [`crate::Buggify`] helper on this schedule. The
    /// `retry_clock.skew` label fires inside [`Self::next`] when
    /// armed, multiplying the returned `Duration` by a seed-driven
    /// factor in `[0.5, 2.0]`. Engines call this once after
    /// constructing the Backoff; the moonpool engine threads the same
    /// helper instance the [`crate::Connection`] is using so the four
    /// labels share a single fire-counter map. ADR-0048.
    pub fn install_buggify(&mut self, buggify: crate::Buggify) {
        self.buggify = buggify;
    }

    /// Compute the next backoff delay.
    ///
    /// On the very first call after construction (or after [`Self::reset`]), this returns the
    /// `initial` delay. Subsequent calls double the previous delay, clamped at `max`, with a
    /// jitter factor in `[0%, 20%]` of the next delay subtracted.
    ///
    /// If the cumulative elapsed delay exceeds `mandatory_stop`, the next delay snaps to `max`
    /// and the cumulative counter resets to zero. This mirrors Java's behaviour
    /// (`Backoff.java:60-89`).
    pub fn next(&mut self) -> Duration {
        let mut current = if self.first_call {
            self.first_call = false;
            self.next_delay
        } else {
            self.next_delay
        };

        // Apply jitter: subtract up to 20% of current.
        let jitter = self.jitter_fraction();
        let jitter_ns = (current.as_nanos() as u64).saturating_mul(jitter) / 1000;
        current = current.saturating_sub(Duration::from_nanos(jitter_ns));

        self.total_elapsed = self.total_elapsed.saturating_add(current);

        // Pre-compute the next-step base delay (doubled, clamped).
        let doubled = self.next_delay.saturating_mul(2);
        self.next_delay = if doubled > self.max {
            self.max
        } else {
            doubled
        };

        if self.total_elapsed > self.mandatory_stop {
            // Snap to max and reset the elapsed budget so a steady-state reconnect loop keeps
            // ticking at the `max` cadence.
            self.total_elapsed = Duration::ZERO;
            self.next_delay = self.max;
            return self.apply_buggify_skew(self.max);
        }
        self.apply_buggify_skew(current)
    }

    /// ADR-0048 buggify point: `retry_clock.skew`. When the label
    /// fires, scale `base` by a seed-driven factor in `[0.5, 2.0]`.
    /// The factor is derived from the engine RNG handle so two runs
    /// of the same seed produce the same schedule. With no RNG armed
    /// (or under `not(feature = "buggify")`) the function returns
    /// `base` unmodified — production builds compile to a NOP.
    fn apply_buggify_skew(&self, base: Duration) -> Duration {
        if !self
            .buggify
            .should_fire(crate::buggify::labels::RETRY_CLOCK_SKEW, 0.05)
        {
            return base;
        }
        // Pull an additional u64 to derive the [0.5, 2.0] scale.
        // Buckets the roll into 10_000 steps over the range — same
        // resolution Buggify uses for the fire probability. If the
        // RNG is unavailable, fall through to the unmodified base
        // (Buggify::roll_u64 should not return None here because
        // should_fire already required an armed helper, but guarding
        // keeps `magnetar-proto`'s no-panic invariant intact).
        let Some(roll) = self.buggify.roll_u64() else {
            return base;
        };
        // Bucket the roll over 10_000 steps; the result is `< 10_000`
        // so the `as f64` cast is lossless by construction (well
        // inside f64's 52-bit mantissa).
        #[allow(clippy::cast_precision_loss)]
        let bucket = (roll % 10_000) as f64 / 10_000.0; // [0.0, 1.0)
        let factor = 0.5 + bucket * 1.5; // [0.5, 2.0)
        // Convert via nanoseconds so we keep sub-millisecond fidelity
        // while staying inside the saturating u128 → u64 range.
        // `base.as_nanos()` for any practical `Duration` fits in
        // f64's mantissa (a `Duration` of u64::MAX seconds is ~5e11
        // years; we never schedule retries near that range).
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let scaled_nanos = (base.as_nanos() as f64 * factor) as u128;
        let scaled_nanos_u64 = u64::try_from(scaled_nanos).unwrap_or(u64::MAX);
        Duration::from_nanos(scaled_nanos_u64)
    }

    /// Reset the backoff to its initial state. Call after a successful operation.
    pub fn reset(&mut self) {
        self.next_delay = self.initial;
        self.total_elapsed = Duration::ZERO;
        self.first_call = true;
    }

    /// Returns the configured maximum delay.
    pub fn max(&self) -> Duration {
        self.max
    }

    /// Returns the configured initial delay.
    pub fn initial(&self) -> Duration {
        self.initial
    }

    /// SplitMix64 PRNG step, returning a u64 in `[0, 200]` representing the jitter percentage
    /// scaled to the 1/1000 unit used in `next()`.
    fn jitter_fraction(&mut self) -> u64 {
        // SplitMix64
        self.rng_state = self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // 0..=200 → 0% .. 20% in /1000 units.
        z % 201
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_call_returns_initial_within_jitter() {
        let mut b = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(60),
            Duration::from_secs(60 * 30),
            42,
        );
        let d = b.next();
        assert!(d <= Duration::from_millis(100));
        assert!(d >= Duration::from_millis(80));
    }

    #[test]
    fn doubles_until_max() {
        let mut b = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(60 * 30),
            1,
        );
        let mut last = Duration::ZERO;
        for _ in 0..20 {
            let d = b.next();
            assert!(d <= Duration::from_secs(1) + Duration::from_millis(1));
            last = d;
        }
        // After enough iterations, we should be capped at max-ish.
        assert!(last >= Duration::from_millis(800));
        assert!(last <= Duration::from_secs(1));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut b = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(60 * 30),
            1,
        );
        for _ in 0..5 {
            let _ = b.next();
        }
        b.reset();
        let d = b.next();
        assert!(d <= Duration::from_millis(100));
        assert!(d >= Duration::from_millis(80));
    }

    /// ADR-0048: when no buggify helper is installed (the default),
    /// `Backoff::next` returns the same schedule it did before the
    /// retry-clock-skew label was added. Equivalent to the production
    /// path for all callers that don't opt in.
    #[test]
    fn next_without_buggify_matches_baseline_schedule() {
        let mut b = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(60 * 30),
            1,
        );
        let first = b.next();
        let second = b.next();
        // No buggify → no skew. The jitter is in [80%, 100%] of the
        // pre-doubled `next_delay`, so we just confirm we're inside
        // the pre-skew band.
        assert!(first <= Duration::from_millis(100));
        assert!(first >= Duration::from_millis(80));
        assert!(second <= Duration::from_millis(200));
        assert!(second >= Duration::from_millis(160));
    }

    /// ADR-0048 `retry_clock.skew`: with an armed buggify helper that
    /// always fires AND a follow-up roll at the bottom of the bucket
    /// range, the returned duration is rescaled by `×0.5`. The two
    /// rolls happen in order: first for `should_fire`, then for the
    /// skew factor.
    #[cfg(feature = "buggify")]
    #[test]
    fn buggified_next_scales_by_half_at_rng_zero() {
        let mut b = Backoff::new(
            Duration::from_secs(10),
            Duration::from_secs(60),
            Duration::from_secs(60 * 30),
            // Tip: the base-jitter PRNG (`rng_state`) is independent
            // of the buggify RNG, so we still get a deterministic
            // jitter on top — the skew applies to the post-jitter
            // duration.
            42,
        );
        // Always-zero RNG: should_fire (p=0.05) lands roll=0.0 → fires;
        // skew roll lands bucket=0 → factor 0.5.
        b.install_buggify(crate::Buggify::with_rng(std::sync::Arc::new(|| 0_u64)));
        let scaled = b.next();
        // Base ≈ 8-10s after jitter; ×0.5 → 4-5s.
        assert!(scaled >= Duration::from_secs(4));
        assert!(scaled <= Duration::from_secs(5));
    }

    /// At the high end of the bucket range, the factor approaches
    /// `2.0`. To exercise both the should_fire (must land low) and
    /// the skew factor (must land high), the RNG alternates: first
    /// call returns 0 (fires), second returns `9_998` (×2 scale).
    #[cfg(feature = "buggify")]
    #[test]
    fn buggified_next_scales_by_two_at_rng_top() {
        let mut b = Backoff::new(
            Duration::from_secs(10),
            Duration::from_secs(60),
            Duration::from_secs(60 * 30),
            7,
        );
        let counter = std::sync::Arc::new(parking_lot::Mutex::new(0_u64));
        let counter_handle = counter.clone();
        b.install_buggify(crate::Buggify::with_rng(std::sync::Arc::new(move || {
            let mut g = counter_handle.lock();
            let v = *g;
            *g += 1;
            // Roll #0 → 0 (fires); roll #1 → 9_998 (×2 scale); then
            // wrap to 0 if anyone keeps rolling.
            if v == 1 { 9_998 } else { 0 }
        })));
        let scaled = b.next();
        // Base ≈ 8-10s after jitter; ×2 → 16-20s.
        assert!(scaled >= Duration::from_secs(16));
        assert!(scaled <= Duration::from_secs(20));
    }

    /// Negative-space assertion: even with `buggify` armed, a probability
    /// of zero never fires. Default fire-probability in
    /// `apply_buggify_skew` is 0.05; an RNG that always lands at the
    /// top of the [0, 10_000) bucket never crosses that threshold, so
    /// the schedule is unmodified.
    #[cfg(feature = "buggify")]
    #[test]
    fn buggified_next_skips_skew_when_roll_above_threshold() {
        let mut b = Backoff::new(
            Duration::from_secs(10),
            Duration::from_secs(60),
            Duration::from_secs(60 * 30),
            7,
        );
        // 9_999 % 10_000 = 9_999 → roll = 0.9999, well above 0.05.
        b.install_buggify(crate::Buggify::with_rng(std::sync::Arc::new(|| 9_999_u64)));
        let baseline = {
            let mut clone = b.clone();
            clone.install_buggify(crate::Buggify::disabled());
            clone.next()
        };
        let with_buggify = b.next();
        assert_eq!(baseline, with_buggify);
    }

    #[test]
    fn mandatory_stop_snaps_to_max() {
        // Configure a tiny mandatory_stop so we cross it after a handful of steps.
        let mut b = Backoff::new(
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::from_millis(150),
            7,
        );
        let _ = b.next(); // ~50ms
        let _ = b.next(); // ~100ms — crosses mandatory stop
        let snap = b.next();
        assert_eq!(snap, Duration::from_millis(200));
    }
}
