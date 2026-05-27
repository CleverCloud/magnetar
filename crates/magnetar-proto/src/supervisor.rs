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

use crate::anti_thrash::AntiThrashThreshold;
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
    /// Anti-thrash detector threshold (ADR-0028). `None` (the default)
    /// disables the detector and preserves current behaviour — the
    /// supervisor uses only per-handle backoff and the in-band transient
    /// retry path.
    ///
    /// When `Some(threshold)`, the supervisor escalates to a
    /// connection-level cooldown once
    /// `threshold.successful_attaches` consecutive re-attaches succeed and
    /// each is followed by a TCP-level drop within `threshold.drop_within`
    /// (all inside `threshold.window`). The cooldown floor is
    /// [`Self::max_backoff_after_thrash`].
    ///
    /// Recommended starting values when opting in (see [ADR-0028 §"Defaults
    /// and migration"](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0028-supervised-reconnect-anti-thrash-policy.md)):
    /// `AntiThrashThreshold { successful_attaches: 5, window:
    /// Duration::from_secs(2), drop_within: Duration::from_millis(50) }` with
    /// `max_backoff_after_thrash = Duration::from_secs(30)`.
    pub anti_thrash_threshold: Option<AntiThrashThreshold>,
    /// Driver-side grace window for attributing a transport close to a
    /// recent successful re-attach. When a `TransportClosed` arrives within
    /// `drop_grace` of the most recent
    /// [`ConnectionEvent::ProducerReady`](crate::ConnectionEvent::ProducerReady)
    /// or [`ConnectionEvent::SubscribeAcked`](crate::ConnectionEvent::SubscribeAcked),
    /// the engine driver feeds it into the anti-thrash detector as a
    /// [`ReAttachOutcomeKind::TcpDropAfterReAttach`](crate::ReAttachOutcomeKind::TcpDropAfterReAttach).
    /// Defaults to `Duration::from_millis(500)`.
    ///
    /// The stricter per-pair `drop_within` knob on
    /// [`AntiThrashThreshold`] decides whether the paired entry actually
    /// counts toward the threshold — `drop_grace` is the engine-side
    /// attribution window only.
    pub drop_grace: Duration,
    /// Cooldown floor applied once
    /// [`Self::anti_thrash_threshold`] trips. Stacks above the per-handle
    /// backoff; the supervisor sleeps until at least
    /// `now + max_backoff_after_thrash` before its next `Transport::connect`
    /// once the cooldown engages. Default `Duration::from_secs(30)`.
    pub max_backoff_after_thrash: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            mandatory_stop: Duration::from_secs(60 * 60),
            max_attempts: None,
            anti_thrash_threshold: None,
            drop_grace: Duration::from_millis(500),
            max_backoff_after_thrash: Duration::from_secs(30),
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

    /// Policy gate for the engine drivers' persisted [`Backoff`] schedule:
    /// returns `true` when the previous socket survived past [`Self::drop_grace`]
    /// — i.e. when the previous reconnect counts as stable and the engine
    /// should call [`Backoff::reset`] at the top of the next reconnect cycle.
    ///
    /// Sockets that died inside `drop_grace` of the most recent successful
    /// attach are treated as thrashes: the schedule keeps growing, so
    /// successive ProducerReady-then-drop cycles slow down geometrically up
    /// to `max_backoff`. This is the per-handle defence in depth that pairs
    /// with the connection-level anti-thrash cooldown (ADR-0028) — both must
    /// be wired for the supervisor to bound CPU under the storm pattern hit
    /// by clients sitting behind the Apache Pulsar Proxy.
    ///
    /// Engines call this from the top of the supervisor outer loop with the
    /// wall-clock-elapsed time between the previous socket coming up and
    /// `driver_loop_inner` returning (i.e. the socket's lifetime).
    #[must_use]
    pub fn should_reset_backoff(&self, socket_alive: Duration) -> bool {
        socket_alive > self.drop_grace
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
            ..SupervisorConfig::default()
        };
        assert_eq!(cfg.initial_backoff, Duration::from_millis(50));
        assert_eq!(cfg.max_attempts, Some(5));
        assert!(cfg.anti_thrash_threshold.is_none());
        assert_eq!(cfg.drop_grace, Duration::from_millis(500));
        assert_eq!(cfg.max_backoff_after_thrash, Duration::from_secs(30));
    }

    #[test]
    fn anti_thrash_defaults_are_off_with_documented_recommendations() {
        let cfg = SupervisorConfig::default();
        assert!(
            cfg.anti_thrash_threshold.is_none(),
            "anti-thrash default must be OFF (ADR-0028 §Defaults)"
        );
        assert_eq!(cfg.drop_grace, Duration::from_millis(500));
        assert_eq!(cfg.max_backoff_after_thrash, Duration::from_secs(30));
        let recommended = AntiThrashThreshold::recommended();
        assert_eq!(recommended.successful_attaches, 5);
        assert_eq!(recommended.window, Duration::from_secs(2));
        assert_eq!(recommended.drop_within, Duration::from_millis(50));
    }

    #[test]
    fn should_reset_backoff_gates_on_drop_grace() {
        let cfg = SupervisorConfig::default();
        assert_eq!(cfg.drop_grace, Duration::from_millis(500));
        assert!(!cfg.should_reset_backoff(Duration::ZERO));
        assert!(!cfg.should_reset_backoff(Duration::from_millis(50)));
        assert!(!cfg.should_reset_backoff(Duration::from_millis(499)));
        assert!(
            !cfg.should_reset_backoff(Duration::from_millis(500)),
            "drop_grace itself is the upper bound of the thrash window — strict >"
        );
        assert!(cfg.should_reset_backoff(Duration::from_millis(501)));
        assert!(cfg.should_reset_backoff(Duration::from_secs(1)));
        assert!(cfg.should_reset_backoff(Duration::from_secs(3600)));
    }

    #[test]
    fn should_reset_backoff_respects_custom_drop_grace() {
        let cfg_tight = SupervisorConfig {
            drop_grace: Duration::from_millis(50),
            ..SupervisorConfig::default()
        };
        assert!(!cfg_tight.should_reset_backoff(Duration::from_millis(40)));
        assert!(cfg_tight.should_reset_backoff(Duration::from_millis(60)));

        let cfg_lax = SupervisorConfig {
            drop_grace: Duration::from_secs(5),
            ..SupervisorConfig::default()
        };
        assert!(!cfg_lax.should_reset_backoff(Duration::from_secs(4)));
        assert!(cfg_lax.should_reset_backoff(Duration::from_secs(6)));
    }

    #[test]
    fn supervisor_storm_schedule_grows_geometrically_without_reset() {
        let cfg = SupervisorConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            mandatory_stop: Duration::from_secs(60 * 60),
            drop_grace: Duration::from_millis(500),
            ..SupervisorConfig::default()
        };
        let mut backoff = cfg.build_backoff(1);
        let mut delays = Vec::new();
        for _ in 0..10 {
            let socket_alive = Duration::from_millis(5);
            if cfg.should_reset_backoff(socket_alive) {
                backoff.reset();
            }
            delays.push(backoff.next());
        }
        assert!(
            delays[0] <= Duration::from_millis(100),
            "first delay starts at initial (with jitter), got {:?}",
            delays[0]
        );
        assert!(
            delays[2] >= Duration::from_millis(320),
            "third delay must reflect at least 4x growth (got {:?})",
            delays[2]
        );
        // 8th call: base 12.8 s (= initial × 2^7), with up to 20 % jitter
        // → 10.24 – 12.8 s. The lower bound proves the schedule is no
        // longer near `initial`; the higher you go, the more obvious the
        // storm is bounded.
        assert!(
            delays[7] >= Duration::from_secs(10) || delays[7] == cfg.max_backoff,
            "schedule must approach max_backoff under sustained thrash, got {:?}",
            delays[7]
        );

        if cfg.should_reset_backoff(Duration::from_secs(2)) {
            backoff.reset();
        }
        let after_reset = backoff.next();
        assert!(
            after_reset <= Duration::from_millis(100),
            "schedule resets to initial after a stable socket, got {:?}",
            after_reset
        );
    }

    #[test]
    fn build_backoff_produces_first_delay_at_initial() {
        let cfg = SupervisorConfig {
            initial_backoff: Duration::from_millis(123),
            max_backoff: Duration::from_secs(10),
            mandatory_stop: Duration::from_secs(60),
            max_attempts: None,
            ..SupervisorConfig::default()
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
