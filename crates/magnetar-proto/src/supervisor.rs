// SPDX-License-Identifier: Apache-2.0

//! Supervisor configuration for auto-reconnect.
//!
//! Mirrors Java's `PulsarClientImpl` reconnect loop: when the underlying connection
//! drops, the supervisor pauses for an exponential-backoff interval (computed by
//! [`Backoff`]), then re-runs the connect path. Runtime engines (tokio /
//! moonpool) read this config from
//! [`crate::conn::ConnectionConfig::supervisor`].
//!
//! # Semantics
//!
//! - When `supervisor` is `None` (the default), the driver exits on the first I/O failure — matches
//!   pre-supervisor behavior.
//! - When `supervisor` is `Some`, the runtime engine wraps the driver loop in a reconnect loop.
//!   Reconnect performs TCP + (optional) TLS + Pulsar handshake; the broker may assign new
//!   producer/consumer ids on the new connection, so pending in-flight requests fail with a
//!   "session lost" outcome. Re-subscribing consumers and re-creating producers transparently
//!   across a reconnect is a future enhancement.
//!
//! # References
//!
//! - `PulsarClientImpl.java` (`reconnectLater`)
//! - `Backoff.java` (`Backoff.next`)

use core::time::Duration;

use crate::backoff::Backoff;

/// Configuration for the auto-reconnect supervisor.
///
/// The supervisor uses an exponential-backoff schedule between reconnect attempts.
/// All durations are wall-clock; jitter is applied deterministically (seeded from
/// `Backoff::new`).
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// Initial backoff delay applied to the first reconnect attempt after a drop.
    pub initial_backoff: Duration,
    /// Maximum backoff delay; subsequent doubled delays clamp here.
    pub max_backoff: Duration,
    /// Cumulative-elapsed cap before the schedule snaps to `max_backoff`. Mirrors
    /// Java's `Backoff.mandatoryStop`.
    pub mandatory_stop: Duration,
    /// Maximum total reconnect attempts. `None` means infinite — keep reconnecting
    /// forever (matching Java's default). `Some(N)` gives up after `N` consecutive
    /// failures and surfaces the last error to the caller.
    pub max_attempts: Option<u32>,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            mandatory_stop: Duration::from_secs(60 * 60),
            max_attempts: None,
        }
    }
}

impl SupervisorConfig {
    /// Build a [`Backoff`] from this config. `seed` controls jitter; pass `0` for
    /// the deterministic default seed.
    #[must_use]
    pub fn build_backoff(&self, seed: u64) -> Backoff {
        Backoff::new(
            self.initial_backoff,
            self.max_backoff,
            self.mandatory_stop,
            seed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_infinite_with_sensible_caps() {
        let cfg = SupervisorConfig::default();
        assert_eq!(cfg.initial_backoff, Duration::from_millis(100));
        assert_eq!(cfg.max_backoff, Duration::from_secs(60));
        assert!(
            cfg.max_attempts.is_none(),
            "default reconnect must be infinite"
        );
    }

    #[test]
    fn custom_config_round_trips() {
        let cfg = SupervisorConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(30),
            mandatory_stop: Duration::from_secs(120),
            max_attempts: Some(5),
        };
        assert_eq!(cfg.initial_backoff, Duration::from_millis(50));
        assert_eq!(cfg.max_attempts, Some(5));
    }

    #[test]
    fn build_backoff_produces_first_delay_at_initial() {
        let cfg = SupervisorConfig {
            initial_backoff: Duration::from_millis(123),
            max_backoff: Duration::from_secs(10),
            mandatory_stop: Duration::from_secs(60),
            max_attempts: None,
        };
        let mut backoff = cfg.build_backoff(0);
        // First next() returns the initial delay with up to 20% jitter subtracted.
        let first = backoff.next();
        assert!(
            first <= Duration::from_millis(123),
            "first backoff must not exceed initial, got {first:?}",
        );
        assert!(
            first >= Duration::from_millis(80),
            "first backoff after jitter must remain near initial, got {first:?}",
        );
    }
}
