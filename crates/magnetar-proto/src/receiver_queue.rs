// SPDX-License-Identifier: Apache-2.0

//! Pluggable receiver-queue-size policy (issue #301, PIP-74 parity).
//!
//! The consumer's receiver queue size — the number of permits handed to the
//! broker so it may push messages without further consent — is no longer a raw
//! `usize`. It is now derived from a [`ReceiverQueuePolicy`] held on each
//! [`crate::consumer::ConsumerState`].
//!
//! Two built-in policies ship:
//!
//! - [`Fixed`] — the historical behaviour. `initial()` and every `adjust()` return the same
//!   constant, so the consumer asks for exactly `receiver_queue_size` permits forever. This is the
//!   **default**, so an un-opted-in consumer behaves bit-for-bit identically to the pre-#301 code.
//! - [`Auto`] — PIP-74 `autoScaledReceiverQueueSizeEnabled` parity. The target grows while the
//!   broker keeps draining our permits to zero (a starvation signal) and is capped so the
//!   prefetched payload bytes never exceed a byte budget (an OOM guard). Growth and decay are
//!   bounded per tick so the target converges without thrashing.
//!
//! # Sans-io + determinism (ADR-0004, ADR-0011)
//!
//! [`ReceiverQueuePolicy::adjust`] is a **pure function** of the observed
//! [`FlowStats`]. It MUST NOT read a clock, draw randomness, or perform I/O —
//! the connection drives it from
//! [`crate::Connection::handle_timeout`]'s injected `now`, and the moonpool
//! simulation engine relies on bit-for-bit reproducibility. The two built-in
//! policies are pure; custom user policies are contractually required to be
//! pure as well (documented on the trait). A policy that read the wall clock or
//! an RNG inside `adjust` would diverge the tokio and moonpool engines and
//! break the [ADR-0024](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
//! differential parity guarantee.
//!
//! # `Arc<dyn>` + `Clone`
//!
//! [`crate::conn_types::SubscribeRequest`] derives `Clone`; a policy is carried
//! as `Arc<dyn ReceiverQueuePolicy>`, and `Arc` is `Clone`, so replaying a
//! subscribe on reconnect clones the *handle* to the same policy object — never
//! the policy itself. There is no per-clone ordering or determinism hazard
//! because the policy holds only immutable configuration; all mutable target
//! state lives on `ConsumerState` (`receiver_queue_size`), which is not
//! `Clone`.

use std::fmt;
use std::sync::Arc;

/// Default receiver queue size when neither `receiver_queue_size` nor a policy
/// is set. Mirrors Java `ConsumerConfigurationData#receiverQueueSize = 1000`.
pub const DEFAULT_RECEIVER_QUEUE_SIZE: usize = 1000;

/// Observed flow-control signals handed to [`ReceiverQueuePolicy::adjust`] on
/// every adjust tick. `#[non_exhaustive]` so future signals can be added
/// without breaking downstream policies.
///
/// All fields are snapshots taken under the per-slot lock at the tick instant
/// — they are pure data, carry no clock, and are identical across the tokio and
/// moonpool engines for the same logical history.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowStats {
    /// Current receiver-queue target (the value the policy last returned, or
    /// the initial value before the first adjust). The policy uses this as the
    /// base it grows or shrinks from.
    pub current_queue_size: usize,
    /// Number of messages currently buffered in the consumer's user-facing
    /// queue, waiting to be popped. `0` means the user is keeping up (or
    /// nothing has arrived); a value near `current_queue_size` means the user
    /// is falling behind.
    pub queued_messages: usize,
    /// Permits the broker still holds for us (grants minus dispatched); `0`
    /// under load is the starvation signal: the broker has exhausted its
    /// grant and will push nothing until we flow more. Issue #349: fed from
    /// [`crate::consumer::ConsumerState::permit_balance`] — a REAL,
    /// decrementing balance, not the purely-additive grant mirror
    /// ([`crate::consumer::ConsumerState::granted_permits`]) that never
    /// registered a genuine dispatch-driven starvation before this split.
    pub available_permits: u32,
    /// Rolling per-second message-receive rate (Java
    /// `ConsumerStats#getRateMsgsReceived`). `0.0` before the second rate
    /// snapshot lands.
    pub consume_rate_msgs_per_s: f64,
    /// Mean delivered-message payload size in bytes, derived from the
    /// cumulative `total_bytes_received / total_msgs_received`. `0` before the
    /// first message lands.
    pub avg_message_bytes: u64,
    /// Bytes currently buffered in the consumer's user-facing queue — the
    /// running total of `payload.len()` across every queued [`crate::event::IncomingMessage`]
    /// not yet popped. This is the figure the OOM guard bounds.
    pub in_flight_bytes: u64,
    /// Number of partitions the owning façade consumer spans. `1` for a
    /// non-partitioned consumer. [`Auto`] divides its byte budget by this so the
    /// aggregate buffered bytes across every partition stay within `max_bytes`.
    pub partitions: usize,
}

/// A pluggable strategy for sizing a consumer's receiver queue.
///
/// Implementors decide how many permits the consumer asks the broker for: a
/// large queue trades memory for throughput (the broker can stream ahead), a
/// small queue trades throughput for a tight memory bound.
///
/// # Purity contract (REQUIRED)
///
/// [`Self::adjust`] and [`Self::initial`] MUST be pure functions of their
/// inputs: **no clock reads, no randomness, no I/O, no interior mutability that
/// depends on call timing.** The connection calls `adjust` from its sans-io
/// timeout tick (ADR-0004, ADR-0011); a non-deterministic policy would diverge
/// the production tokio engine from the deterministic moonpool simulation
/// engine and break differential parity (ADR-0024). The two built-in policies
/// ([`Fixed`], [`Auto`]) satisfy this contract.
pub trait ReceiverQueuePolicy: Send + Sync + fmt::Debug {
    /// The receiver queue target at subscribe time — the number of permits the
    /// consumer asks the broker for in its initial `CommandFlow`.
    fn initial(&self) -> usize;

    /// Recompute the receiver queue target from the observed [`FlowStats`].
    ///
    /// Returns the new target. The connection grows the broker's grant when the
    /// target rises (emitting an incremental `CommandFlow`) and stops
    /// replenishing — letting permits drain naturally — when it falls (permits
    /// already granted cannot be un-granted).
    ///
    /// MUST be pure (see the trait-level purity contract).
    fn adjust(&self, flow: &FlowStats) -> usize;
}

/// The historical fixed-size receiver queue. `initial()` and every `adjust()`
/// return the wrapped size unchanged, so the consumer asks the broker for that
/// many permits forever. This is the **default** policy, so a consumer that does
/// not opt into [`Auto`] behaves identically to the pre-#301 client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fixed(pub usize);

impl ReceiverQueuePolicy for Fixed {
    fn initial(&self) -> usize {
        self.0
    }

    fn adjust(&self, _flow: &FlowStats) -> usize {
        // Fixed never reacts to load — the target is constant. Returning
        // `self.0` (not `flow.current_queue_size`) keeps the consumer pinned
        // even if some other path ever perturbed the running target.
        self.0
    }
}

/// PIP-74 `autoScaledReceiverQueueSizeEnabled` parity: a self-tuning receiver
/// queue that grows under starvation and is bounded by a byte budget.
///
/// # Invariants
///
/// 1. **Never starve.** While the broker keeps draining our permits to zero (`available_permits ==
///    0`) and the byte budget still has room, the target doubles (bounded by [`Self::max_bytes`])
///    so the next flow grant is larger and steady-state `available_permits` stays above zero.
/// 2. **Never OOM.** The target is capped so the projected buffered bytes (`target *
///    avg_message_bytes`, summed across partitions) never exceed [`Self::max_bytes`]. When
///    `in_flight_bytes` approaches the budget the target shrinks.
/// 3. **Converge, don't thrash.** Growth is multiplicative-but-capped (at most a doubling per tick)
///    and decay is gentle (at most a halving toward [`Self::min`]), and the target only moves when
///    a clear signal is present — so it settles instead of oscillating.
///
/// All decisions are a pure function of [`FlowStats`]; no clock or RNG is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Auto {
    /// Floor for the target. The queue never shrinks below this many permits,
    /// guaranteeing forward progress even under the OOM guard. Mirrors the
    /// implicit `1` floor Java keeps on its auto-scaled queue; callers pick a
    /// larger floor (e.g. `1_000`) to keep a healthy prefetch.
    pub min: usize,
    /// Aggregate byte budget across every partition of the owning consumer. The
    /// target is capped so `target * avg_message_bytes * partitions` stays at or
    /// below this. Mirrors the memory-limit dimension Java exposes via
    /// `ClientBuilder#memoryLimit`, scoped to one consumer here.
    pub max_bytes: usize,
}

impl Auto {
    /// Hard ceiling on the per-consumer target, independent of `max_bytes`.
    /// Mirrors Java's `maxReceiverQueueSize` auto-scale cap and prevents a
    /// degenerate `avg_message_bytes == 0` (no message seen yet) from letting
    /// the byte-budget cap go unbounded. Pure safety floor, not user-tunable.
    const ABSOLUTE_MAX: usize = 1_000_000;

    /// Construct an [`Auto`] policy with the given floor and byte budget. The
    /// floor is clamped to at least `1` so the queue can always make progress.
    #[must_use]
    pub fn new(min: usize, max_bytes: usize) -> Self {
        Self {
            min: min.max(1),
            max_bytes,
        }
    }

    /// The largest target the byte budget admits given the observed average
    /// message size and partition count. With no average yet (`avg == 0`) the
    /// byte budget cannot be projected, so we fall back to the absolute cap; the
    /// growth path is still bounded by the doubling rule, so this never runs
    /// away on the first few ticks.
    fn byte_budget_cap(&self, flow: &FlowStats) -> usize {
        let partitions = flow.partitions.max(1);
        // Per-partition byte budget — the aggregate ceiling divided across the
        // façade's partitions so the SUM of buffered bytes stays within
        // `max_bytes` (the plan's "max_bytes / partitions" approach).
        let per_partition = (self.max_bytes / partitions).max(1);
        if flow.avg_message_bytes == 0 {
            return Self::ABSOLUTE_MAX;
        }
        // target * avg <= per_partition  =>  target <= per_partition / avg
        let cap = (per_partition as u64 / flow.avg_message_bytes) as usize;
        cap.clamp(self.min, Self::ABSOLUTE_MAX)
    }
}

impl ReceiverQueuePolicy for Auto {
    fn initial(&self) -> usize {
        // Start at the floor and let `adjust` grow it under observed
        // starvation. Mirrors Java's auto-scaled queue, which starts small and
        // ramps up rather than pre-committing a large grant.
        self.min.max(1)
    }

    fn adjust(&self, flow: &FlowStats) -> usize {
        let current = flow.current_queue_size.max(self.min).max(1);
        let cap = self.byte_budget_cap(flow);

        // OOM guard (invariant 2): if the buffered bytes have reached the
        // per-partition budget, shrink toward the floor. Gentle halving so a
        // transient spike does not collapse the queue.
        let partitions = flow.partitions.max(1);
        let per_partition_budget = (self.max_bytes / partitions).max(1) as u64;
        if flow.in_flight_bytes >= per_partition_budget {
            let shrunk = (current / 2).max(self.min);
            return shrunk.min(cap).max(self.min);
        }

        // Starvation signal (invariant 1): the broker drained every permit. If
        // the byte budget still has headroom, double the target (bounded) so the
        // next grant is larger. Bounded doubling keeps growth from thrashing
        // (invariant 3).
        if flow.available_permits == 0 {
            let grown = current.saturating_mul(2);
            return grown.clamp(self.min, cap);
        }

        // Steady state: permits remain and bytes are within budget. Hold the
        // current target (clamped to the live byte-budget cap, which may have
        // tightened as the average message size grew). No oscillation because we
        // only move on a clear starve/OOM signal.
        current.clamp(self.min, cap)
    }
}

/// The default policy: a [`Fixed`] queue of [`DEFAULT_RECEIVER_QUEUE_SIZE`].
/// Returned as `Arc<dyn ReceiverQueuePolicy>` so it slots directly into
/// [`crate::consumer::ConsumerState`].
#[must_use]
pub fn default_policy() -> Arc<dyn ReceiverQueuePolicy> {
    Arc::new(Fixed(DEFAULT_RECEIVER_QUEUE_SIZE))
}

/// A [`Fixed`] policy of the given size as `Arc<dyn ReceiverQueuePolicy>`.
/// `receiver_queue_size(n)` builder sugar resolves to this.
#[must_use]
pub fn fixed(size: usize) -> Arc<dyn ReceiverQueuePolicy> {
    Arc::new(Fixed(size))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(
        current: usize,
        available: u32,
        avg: u64,
        in_flight: u64,
        partitions: usize,
    ) -> FlowStats {
        FlowStats {
            current_queue_size: current,
            queued_messages: 0,
            available_permits: available,
            consume_rate_msgs_per_s: 0.0,
            avg_message_bytes: avg,
            in_flight_bytes: in_flight,
            partitions,
        }
    }

    #[test]
    fn fixed_initial_and_adjust_are_constant() {
        let p = Fixed(1000);
        assert_eq!(p.initial(), 1000);
        // No matter the observed load, Fixed never moves.
        assert_eq!(p.adjust(&stats(1000, 0, 512, 9_000_000, 4)), 1000);
        assert_eq!(p.adjust(&stats(1000, 50, 1, 0, 1)), 1000);
    }

    #[test]
    fn auto_starts_at_floor() {
        let p = Auto::new(1000, 128 * 1024 * 1024);
        assert_eq!(p.initial(), 1000);
    }

    #[test]
    fn auto_floor_clamped_to_one() {
        let p = Auto::new(0, 1024);
        assert_eq!(p.min, 1);
        assert_eq!(p.initial(), 1);
    }

    #[test]
    fn auto_grows_under_starvation() {
        let p = Auto::new(100, 128 * 1024 * 1024);
        // available_permits == 0, plenty of byte headroom (avg 100 B) -> double.
        let next = p.adjust(&stats(100, 0, 100, 0, 1));
        assert_eq!(next, 200);
        // Repeated starvation keeps growing (bounded doubling).
        let next2 = p.adjust(&stats(200, 0, 100, 0, 1));
        assert_eq!(next2, 400);
    }

    #[test]
    fn auto_growth_is_bounded_per_tick() {
        let p = Auto::new(100, 128 * 1024 * 1024);
        // One tick can at most double — never jump arbitrarily.
        let next = p.adjust(&stats(1000, 0, 10, 0, 1));
        assert_eq!(next, 2000);
        assert!(next <= 1000 * 2, "growth exceeded a doubling: {next}");
    }

    #[test]
    fn auto_caps_at_byte_budget() {
        // 1 MiB budget, 1 KiB messages -> at most ~1024 buffered messages.
        let budget = 1024 * 1024;
        let avg = 1024;
        let p = Auto::new(1, budget);
        // Already at a large target under starvation: growth is capped by the
        // byte budget, not allowed to double past it.
        let next = p.adjust(&stats(1000, 0, avg, 0, 1));
        assert!(
            (next as u64) * avg <= budget as u64,
            "target {next} * avg {avg} exceeds budget {budget}"
        );
    }

    #[test]
    fn auto_shrinks_under_oom_pressure() {
        let budget = 1024 * 1024;
        let p = Auto::new(10, budget);
        // in_flight_bytes at/above the budget -> shrink toward floor (halve).
        let next = p.adjust(&stats(1000, 5, 1024, budget as u64, 1));
        assert_eq!(next, 500);
        // Never below the floor.
        let floored = p.adjust(&stats(11, 0, 1024, budget as u64, 1));
        assert_eq!(floored, 10);
    }

    #[test]
    fn auto_holds_steady_when_healthy() {
        let p = Auto::new(100, 128 * 1024 * 1024);
        // Permits remain and bytes within budget -> hold the current target.
        let next = p.adjust(&stats(500, 50, 1024, 1024, 1));
        assert_eq!(next, 500);
    }

    #[test]
    fn auto_partitions_divide_the_byte_budget() {
        // 4 MiB budget across 4 partitions -> 1 MiB per partition. With 1 KiB
        // messages that is ~1024 messages per partition.
        let budget = 4 * 1024 * 1024;
        let avg = 1024;
        let p = Auto::new(1, budget);
        let next = p.adjust(&stats(2000, 0, avg, 0, 4));
        let per_partition = budget / 4;
        assert!(
            (next as u64) * avg <= per_partition as u64,
            "per-partition target {next} * avg {avg} exceeds per-partition budget {per_partition}"
        );
    }

    #[test]
    fn auto_no_avg_yet_falls_back_to_absolute_cap() {
        // avg == 0 (no message seen) under starvation: still bounded by the
        // doubling rule and the absolute cap, never runs away.
        let p = Auto::new(100, 1024);
        let next = p.adjust(&stats(100, 0, 0, 0, 1));
        assert_eq!(next, 200);
        assert!(next <= Auto::ABSOLUTE_MAX);
    }
}
