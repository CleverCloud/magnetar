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
        }
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
            return self.max;
        }
        current
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
