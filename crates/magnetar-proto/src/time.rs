// SPDX-License-Identifier: Apache-2.0

//! Panic-free time arithmetic helpers.
//!
//! `Instant + Duration` panics on overflow — and `Duration::MAX` is a
//! perfectly valid `Duration` a user can construct via
//! `Duration::from_secs(u64::MAX)`. Any deadline computation that uses
//! `+` directly will panic if a caller ever sets `send_timeout`,
//! `ack_timeout`, `redelivery_delay`, etc. to a near-`MAX` value.
//!
//! Invariant #6 forbids panics in `magnetar-proto` outside `#[cfg(test)]`,
//! so every `Instant + Duration` site in the proto crate routes through
//! [`deadline_with_clamp`] instead. The clamp picks a 1-hour fallback
//! deadline, which is "longer than any sane caller cares about" while
//! staying nowhere near the `Instant` overflow boundary.

use std::time::{Duration, Instant};

/// Fallback deadline horizon when `base + delta` would overflow
/// `Instant`. One hour is well past any reasonable Pulsar-side timeout
/// (default send-timeout is 30 s; default ack-timeout is 0 s "disabled"
/// or O(seconds) when enabled).
pub const OVERFLOW_FALLBACK: Duration = Duration::from_secs(3600);

/// `base + delta`, clamped to `base + OVERFLOW_FALLBACK` when the
/// addition would overflow `Instant`. Never panics, mirroring the
/// "long timeout means long" semantic the caller probably intended.
///
/// Mirrors the Java client, which silently caps near-`Long.MAX` timeouts
/// at the `Instant` representation boundary instead of overflowing.
#[must_use]
pub fn deadline_with_clamp(base: Instant, delta: Duration) -> Instant {
    base.checked_add(delta)
        .unwrap_or_else(|| base + OVERFLOW_FALLBACK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_delta_is_unchanged() {
        let now = Instant::now();
        let small = Duration::from_millis(500);
        assert_eq!(deadline_with_clamp(now, small), now + small);
    }

    #[test]
    fn zero_delta_returns_base() {
        let now = Instant::now();
        assert_eq!(deadline_with_clamp(now, Duration::ZERO), now);
    }

    /// `Duration::MAX` is `u64::MAX` seconds — adding it to any
    /// representable `Instant` overflows. The clamp must turn the
    /// overflow into the 1-hour fallback instead of panicking.
    #[test]
    fn max_duration_is_clamped_to_fallback() {
        let now = Instant::now();
        let clamped = deadline_with_clamp(now, Duration::MAX);
        assert_eq!(
            clamped,
            now + OVERFLOW_FALLBACK,
            "Duration::MAX must clamp to the OVERFLOW_FALLBACK horizon, not panic"
        );
    }

    /// A `Duration` just one second past whatever `Instant::checked_add`
    /// considers the boundary also clamps cleanly. We can't construct
    /// that exact `Duration` portably, so the `Duration::MAX` case acts
    /// as the canonical overflow witness — verified above.
    #[test]
    fn clamp_horizon_is_one_hour() {
        assert_eq!(OVERFLOW_FALLBACK, Duration::from_secs(3600));
    }
}
