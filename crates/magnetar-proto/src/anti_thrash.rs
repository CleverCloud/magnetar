// SPDX-License-Identifier: Apache-2.0

//! Anti-thrash detector — per-`Connection` ring of re-attach outcomes.
//!
//! Per [ADR-0028](../../specs/adr/0028-supervised-reconnect-anti-thrash-policy.md),
//! when the broker exhibits the create-then-drop cascade (each `CommandProducer`
//! / `CommandSubscribe` is acked, then the TCP socket is closed within a few
//! milliseconds), the supervisor escalates from per-handle retry to a
//! connection-level cooldown. The escalation is driven by this small ring
//! detector, not by a separate task — the engine driver records outcomes
//! into [`AntiThrashState`] and the driver-loop polls
//! [`AntiThrashState::tick`] to learn the current cooldown disposition.
//!
//! No channels, no I/O — the detector lives inside the sans-io
//! `magnetar-proto::Connection` and shares its [`parking_lot::Mutex`].

use core::time::Duration;
use std::collections::VecDeque;
use std::time::Instant;

use crate::types::{ConsumerHandle, ProducerHandle};

/// Kind of re-attach outcome the driver feeds into the anti-thrash detector.
///
/// The driver calls
/// [`Connection::record_reattach_outcome`](crate::Connection::record_reattach_outcome)
/// twice per attach attempt: once on success
/// ([`ConnectionEvent::ProducerReady`](crate::ConnectionEvent::ProducerReady) /
/// [`ConnectionEvent::SubscribeAcked`](crate::ConnectionEvent::SubscribeAcked))
/// with [`ReAttachOutcomeKind::ReAttachOk`], and again with
/// [`ReAttachOutcomeKind::TcpDropAfterReAttach`] if the TCP socket is observed
/// to close within
/// [`SupervisorConfig::drop_grace`](crate::supervisor::SupervisorConfig::drop_grace).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReAttachOutcomeKind {
    /// Broker acked the `CommandProducer` / `CommandSubscribe`. Recorded on
    /// the corresponding [`ConnectionEvent::ProducerReady`](crate::ConnectionEvent::ProducerReady)
    /// or [`ConnectionEvent::SubscribeAcked`](crate::ConnectionEvent::SubscribeAcked).
    ReAttachOk,
    /// The TCP socket closed shortly after a [`Self::ReAttachOk`] for the same
    /// connection — the canonical create-then-drop signal documented in
    /// ADR-0028. Recorded by the driver when a socket EOF / RST arrives within
    /// the configured `drop_grace` of the most recent successful attach.
    TcpDropAfterReAttach,
}

/// Handle kind associated with a recorded re-attach outcome.
///
/// Mostly diagnostic — the cooldown decision is purely time-based — but useful
/// for tracing log lines (operators want to know "which handle got cleared
/// after the drop").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReAttachHandle {
    /// `CommandProducerSuccess` arrived for this producer.
    Producer(ProducerHandle),
    /// `CommandSubscribe` ack-`Success` arrived for this consumer.
    Consumer(ConsumerHandle),
}

/// A single entry in the [`AntiThrashState`] ring — a successful re-attach
/// optionally paired with the moment the TCP socket dropped after it.
#[derive(Debug, Clone, Copy)]
pub struct AttachOutcome {
    /// Wall-clock `Instant` the attach succeeded.
    pub attached_at: Instant,
    /// Wall-clock `Instant` the TCP socket dropped, if a drop arrived within
    /// the supervisor's `drop_grace` of the attach. `None` means the attach
    /// has not (yet) been followed by a drop — the broker is behaving.
    pub dropped_at: Option<Instant>,
    /// The handle the broker acked. Diagnostic only.
    pub handle: ReAttachHandle,
}

impl AttachOutcome {
    /// Compute the delta between attach and drop, if any.
    #[must_use]
    pub fn drop_delta(&self) -> Option<Duration> {
        self.dropped_at
            .map(|d| d.saturating_duration_since(self.attached_at))
    }
}

/// Threshold parameters for the anti-thrash detector.
///
/// See [ADR-0028](../../specs/adr/0028-supervised-reconnect-anti-thrash-policy.md)
/// for the rationale. The detector trips into
/// [`AntiThrashDisposition::Cooldown`] when `successful_attaches` consecutive
/// attach-then-drop pairs all fit within `window` and each individual drop is
/// within `drop_within` of its paired attach.
#[derive(Debug, Clone, Copy)]
pub struct AntiThrashThreshold {
    /// Number of consecutive successful-attach-then-drop pairs that must
    /// happen inside `window` to trip the cooldown. Recommended starting
    /// value: `5`.
    pub successful_attaches: u32,
    /// Wall-clock window inside which the `successful_attaches` pairs must
    /// happen. Recommended starting value: `Duration::from_secs(2)`.
    pub window: Duration,
    /// Per-pair maximum delta between the attach and its TCP drop. Pairs with
    /// a larger delta do not count toward the threshold. Recommended starting
    /// value: `Duration::from_millis(50)`.
    pub drop_within: Duration,
}

impl AntiThrashThreshold {
    /// Recommended starting threshold per ADR-0028 §"Defaults and migration".
    /// `successful_attaches = 5`, `window = 2 s`, `drop_within = 50 ms`.
    #[must_use]
    pub fn recommended() -> Self {
        Self {
            successful_attaches: 5,
            window: Duration::from_secs(2),
            drop_within: Duration::from_millis(50),
        }
    }
}

/// Disposition the supervisor consults each time it considers redialling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AntiThrashDisposition {
    /// No cooldown active — the supervisor's normal per-handle backoff
    /// schedule applies.
    Normal,
    /// Cooldown engaged — the supervisor must sleep until `until` before
    /// attempting another `Transport::connect`.
    Cooldown {
        /// Absolute wake instant. The supervisor sleeps until this point even
        /// if its per-handle backoff would have retried sooner.
        until: Instant,
    },
}

/// Internal ring + disposition state. Lives inside `magnetar-proto::Connection`
/// and is mutated by the driver via
/// [`Connection::record_reattach_outcome`](crate::Connection::record_reattach_outcome).
///
/// The state is intentionally tiny — a bounded `VecDeque` of recent outcomes
/// plus a single optional cooldown deadline — so no allocation happens on the
/// steady-state hot path.
#[derive(Debug, Clone)]
pub struct AntiThrashState {
    /// Configured threshold. `None` means the detector is disabled — recording
    /// outcomes is a no-op and [`Self::tick`] always returns
    /// [`AntiThrashDisposition::Normal`].
    threshold: Option<AntiThrashThreshold>,
    /// Configured cooldown floor applied once the threshold trips.
    cooldown: Duration,
    /// Recent attach outcomes. Capped at `threshold.successful_attaches × 2`
    /// to avoid unbounded growth under pathological broker behaviour.
    ring: VecDeque<AttachOutcome>,
    /// Currently-active cooldown deadline, if any.
    cooldown_until: Option<Instant>,
    /// `true` after a successful attach has been observed and no drop has yet
    /// followed within `drop_grace`. Cleared by the driver when it records a
    /// successful first-op (e.g. a `SendReceipt` or `Message` arriving after
    /// the attach) — that is the signal the broker has stabilised.
    pending_first_op: bool,
}

impl AntiThrashState {
    /// Construct a fresh, disabled detector. Engines call
    /// [`Self::set_threshold`] (typically from
    /// [`Connection::set_anti_thrash`](crate::Connection::set_anti_thrash))
    /// to opt in.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            threshold: None,
            cooldown: Duration::from_secs(30),
            ring: VecDeque::new(),
            cooldown_until: None,
            pending_first_op: false,
        }
    }

    /// Enable (or update) the threshold + cooldown. Passing `threshold = None`
    /// disables the detector and clears any active cooldown.
    pub fn set_threshold(&mut self, threshold: Option<AntiThrashThreshold>, cooldown: Duration) {
        self.threshold = threshold;
        self.cooldown = cooldown;
        if threshold.is_none() {
            self.ring.clear();
            self.cooldown_until = None;
            self.pending_first_op = false;
        } else if let Some(th) = threshold {
            let cap = (th.successful_attaches as usize).saturating_mul(2).max(2);
            // Trim the ring if the new capacity is smaller.
            while self.ring.len() > cap {
                self.ring.pop_front();
            }
        }
    }

    /// Currently-configured threshold, if any.
    #[must_use]
    pub fn threshold(&self) -> Option<AntiThrashThreshold> {
        self.threshold
    }

    /// Currently-configured cooldown floor.
    #[must_use]
    pub fn cooldown(&self) -> Duration {
        self.cooldown
    }

    /// Snapshot of the ring — exposed for tests + diagnostics. The slice is
    /// in chronological order; oldest entry first.
    #[must_use]
    pub fn ring(&self) -> Vec<AttachOutcome> {
        self.ring.iter().copied().collect()
    }

    /// `Instant` of the most recent successful re-attach (whether or not it
    /// has been paired with a drop). Engines compare this to the engine's
    /// `Instant::now()` to decide whether a fresh transport close happened
    /// inside the supervisor's `drop_grace` and therefore counts as a
    /// post-attach drop.
    #[must_use]
    pub fn last_reattach_at(&self) -> Option<Instant> {
        self.ring.back().map(|e| e.attached_at)
    }

    /// Record a re-attach outcome.
    ///
    /// `ReAttachOk` appends a fresh `AttachOutcome` with `dropped_at = None`.
    /// `TcpDropAfterReAttach` stamps `dropped_at` on the most recent entry
    /// whose `dropped_at` is still `None` — the canonical "drop after attach"
    /// pairing — *if* that pairing is within `drop_within`. If the most-recent
    /// open attach is older than `drop_within`, the drop is treated as
    /// unrelated and ignored for thresholding purposes (it neither trips nor
    /// resets the detector).
    ///
    /// After recording, the detector evaluates the ring; if the trip
    /// conditions are met it arms a cooldown until `now + cooldown`.
    pub fn record(&mut self, now: Instant, kind: ReAttachOutcomeKind, handle: ReAttachHandle) {
        let Some(th) = self.threshold else {
            // Detector disabled — nothing to do.
            return;
        };

        match kind {
            ReAttachOutcomeKind::ReAttachOk => {
                // Push a new open outcome; trim to capacity.
                self.ring.push_back(AttachOutcome {
                    attached_at: now,
                    dropped_at: None,
                    handle,
                });
                let cap = (th.successful_attaches as usize).saturating_mul(2).max(2);
                while self.ring.len() > cap {
                    self.ring.pop_front();
                }
                self.pending_first_op = true;
            }
            ReAttachOutcomeKind::TcpDropAfterReAttach => {
                // Find the most recent open attach.
                let mut paired = false;
                for entry in self.ring.iter_mut().rev() {
                    if entry.dropped_at.is_none() {
                        let delta = now.saturating_duration_since(entry.attached_at);
                        if delta <= th.drop_within {
                            entry.dropped_at = Some(now);
                            paired = true;
                        }
                        break;
                    }
                }
                if !paired {
                    // No recent attach to pair with — ignore.
                    return;
                }
                self.pending_first_op = false;
                self.evaluate(now);
            }
        }
    }

    /// Record a healthy first-op outcome after an attach (e.g. a
    /// `SendReceipt` or a delivered `Message`). Per ADR-0028, "the detector
    /// resets on any successful attach + first-op-success pair (proves the
    /// broker has stabilised)" — so this clears any active cooldown and the
    /// ring, letting the supervisor fall back to its normal backoff.
    pub fn record_first_op_success(&mut self) {
        if self.threshold.is_none() {
            return;
        }
        if !self.pending_first_op && self.cooldown_until.is_none() && self.ring.is_empty() {
            return;
        }
        self.ring.clear();
        self.cooldown_until = None;
        self.pending_first_op = false;
    }

    /// Drop any active cooldown — used when the supervisor has slept past
    /// `until` and is ready to dial again.
    pub fn clear_cooldown(&mut self) {
        self.cooldown_until = None;
    }

    /// Inspect the current disposition. `now` is the engine's `Instant::now()`
    /// snapshot — passed in to honour the sans-io clock-injection invariant
    /// from ADR-0011.
    #[must_use]
    pub fn tick(&self, now: Instant) -> AntiThrashDisposition {
        match self.cooldown_until {
            Some(until) if until > now => AntiThrashDisposition::Cooldown { until },
            _ => AntiThrashDisposition::Normal,
        }
    }

    /// Evaluate the ring against the configured threshold and arm a cooldown
    /// if the trip conditions are met.
    fn evaluate(&mut self, now: Instant) {
        let Some(th) = self.threshold else {
            return;
        };

        // Cooldown already active — don't re-arm; the supervisor will see the
        // existing deadline.
        if self.cooldown_until.is_some_and(|u| u > now) {
            return;
        }

        // Count consecutive (in ring order) trailing entries that have both an
        // attach and a drop within `drop_within`, and fit inside `window`.
        let needed = th.successful_attaches as usize;
        if self.ring.len() < needed {
            return;
        }
        let tail = self.ring.iter().rev().take(needed);
        let mut earliest: Option<Instant> = None;
        let mut latest: Option<Instant> = None;
        let mut count = 0;
        for entry in tail {
            let Some(dropped) = entry.dropped_at else {
                return;
            };
            if dropped.saturating_duration_since(entry.attached_at) > th.drop_within {
                return;
            }
            earliest = Some(match earliest {
                Some(e) if e < entry.attached_at => e,
                _ => entry.attached_at,
            });
            latest = Some(match latest {
                Some(l) if l > dropped => l,
                _ => dropped,
            });
            count += 1;
        }
        if count < needed {
            return;
        }
        let (Some(e), Some(l)) = (earliest, latest) else {
            return;
        };
        if l.saturating_duration_since(e) > th.window {
            return;
        }

        self.cooldown_until = Some(now + self.cooldown);
    }
}

impl Default for AntiThrashState {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProducerHandle;

    fn handle(id: u64) -> ReAttachHandle {
        ReAttachHandle::Producer(ProducerHandle(id))
    }

    #[test]
    fn disabled_detector_is_noop() {
        let mut s = AntiThrashState::disabled();
        let now = Instant::now();
        s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
        s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
        assert!(matches!(s.tick(now), AntiThrashDisposition::Normal));
    }

    #[test]
    fn trips_after_n_pairs_inside_window() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 3,
                window: Duration::from_millis(500),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_secs(30),
        );
        let mut now = Instant::now();
        for _ in 0..3 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(10);
            s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
            now += Duration::from_millis(20);
        }
        let disp = s.tick(now);
        match disp {
            AntiThrashDisposition::Cooldown { until } => {
                assert!(until > now, "cooldown must lie in the future");
                let delta = until.saturating_duration_since(now);
                assert!(
                    delta >= Duration::from_secs(29),
                    "cooldown ≈30 s, got {delta:?}",
                );
            }
            AntiThrashDisposition::Normal => {
                panic!("threshold met but tick() returned Normal");
            }
        }
    }

    #[test]
    fn does_not_trip_when_drops_exceed_drop_within() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 3,
                window: Duration::from_secs(10),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_secs(30),
        );
        let mut now = Instant::now();
        for _ in 0..3 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(200); // way past drop_within
            s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
            now += Duration::from_millis(20);
        }
        // Drops that arrive past `drop_within` are ignored entirely — the
        // detector never sees a paired entry, so it cannot trip.
        assert!(matches!(s.tick(now), AntiThrashDisposition::Normal));
    }

    #[test]
    fn does_not_trip_when_window_exceeded() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 3,
                window: Duration::from_millis(100),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_secs(30),
        );
        let mut now = Instant::now();
        // Three pairs, but each spaced 1 s apart — total span 2 s ≫ 100 ms.
        for _ in 0..3 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(5);
            s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
            now += Duration::from_secs(1);
        }
        assert!(matches!(s.tick(now), AntiThrashDisposition::Normal));
    }

    #[test]
    fn first_op_success_clears_cooldown() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 2,
                window: Duration::from_secs(5),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_secs(30),
        );
        let mut now = Instant::now();
        for _ in 0..2 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(10);
            s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
            now += Duration::from_millis(20);
        }
        assert!(matches!(
            s.tick(now),
            AntiThrashDisposition::Cooldown { .. }
        ));
        // Broker stabilised — first-op succeeded.
        s.record_first_op_success();
        assert!(matches!(s.tick(now), AntiThrashDisposition::Normal));
    }

    #[test]
    fn cooldown_expires_naturally() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 2,
                window: Duration::from_secs(5),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_millis(100),
        );
        let mut now = Instant::now();
        for _ in 0..2 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(10);
            s.record(now, ReAttachOutcomeKind::TcpDropAfterReAttach, handle(1));
            now += Duration::from_millis(20);
        }
        match s.tick(now) {
            AntiThrashDisposition::Cooldown { until } => {
                assert!(
                    s.tick(now).ne(&AntiThrashDisposition::Normal),
                    "still under cooldown at now"
                );
                let later = until + Duration::from_millis(1);
                assert!(matches!(s.tick(later), AntiThrashDisposition::Normal));
            }
            AntiThrashDisposition::Normal => panic!("expected cooldown"),
        }
    }

    #[test]
    fn ring_caps_at_threshold_times_two() {
        let mut s = AntiThrashState::disabled();
        s.set_threshold(
            Some(AntiThrashThreshold {
                successful_attaches: 3,
                window: Duration::from_secs(5),
                drop_within: Duration::from_millis(50),
            }),
            Duration::from_secs(30),
        );
        let mut now = Instant::now();
        for _ in 0..20 {
            s.record(now, ReAttachOutcomeKind::ReAttachOk, handle(1));
            now += Duration::from_millis(1);
        }
        assert!(
            s.ring().len() <= 6,
            "ring grew past cap, got {}",
            s.ring().len()
        );
    }
}
