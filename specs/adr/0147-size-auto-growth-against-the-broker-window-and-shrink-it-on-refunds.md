# ADR-0147 — Size `Auto` growth against the broker window, and shrink it on refunds

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, receiver-queue, auto-scaling, state-machine, tokio, moonpool, sans-io

## Context

The `Auto` receiver-queue policy (ADR-0071) moves a target, `ConsumerState::receiver_queue_size`, and the connection reconciles the window the broker holds with it.
That window is `permit_balance + consumed_since_flow + queue.len()`: a refund hands back exactly what a pop consumed (`maybe_flow`), so the window does not move under pops, refills or dispatch.
Two reconciliation rules did not hold that invariant, and a third counter drifted beside them.

**Growth was sized against the wrong register.**
`adjust_receiver_queue` granted `new_target - granted_permits` (ADR-0082 §Consequences records the choice).
`granted_permits` is the cumulative grant record: every `maybe_flow` refill adds to it, so after a few windows of pops it is larger than the window by the messages consumed since the last zeroing.
A growth tick then saturated to a zero delta.
The target and the refill threshold (`target / 2`) rose, the window did not, and the next doubling raised the threshold above the window: `maybe_flow` could never fire again, the balance sat at zero and neither side could move.
Once the byte-budget cap (or `Auto::ABSOLUTE_MAX`) was at or below `granted_permits` the wedge was permanent, and nothing rescues it: the starvation gate is read only at the two `initial_flow` sites and the stall watchdog needs a positive balance.

**A shrink never shrank the window.**
Under memory pressure `Auto::adjust` lowers the target, and `adjust_receiver_queue` stored it and emitted nothing, on the stated expectation that the smaller threshold would make `maybe_flow` "ask for less".
It asks for less often, not less in total: every popped unit is still refunded one for one, so the window stays at its peak until a zeroing site re-seeds it.
The out-of-memory guard lowered the number it reads and left the buffered bytes it exists to bound unbounded.

**A reset left the byte counter behind.**
`Connection::reset` cleared every consumer's queue and zeroed the permit mirrors but not `queued_bytes`, which `flow_stats` feeds to the policy as `in_flight_bytes`.
Each reset made the dropped messages count toward the budget for good, and the redelivered copies were added on top, so the guard pinned the target at `min`.
`begin_seek` already zeroes it with the queue.

## Decision

**Growth is sized against the window the broker holds.**
The window equals the previous target at every adjust tick (each grant site tops it up to the target and a refund preserves it), so a growth tick grants the `new - current` the target gained and not the difference against `granted_permits`.
The window is not recomputed from `permit_balance + consumed_since_flow + queue.len()`: a broker that over-dispatches saturates `permit_balance` at zero, and the recomputed figure would then exceed the real window and withhold a grant.
`granted_permits` keeps its additive meaning and its other readers (the failover re-arm gate, the churn-window guard, `Connection::initial_flow`'s once-per-attach guard).

**A shrink records what the broker cannot take back, and the refunds retire it.**
`ConsumerState` gains `window_surplus: u32`, the permits the broker holds beyond the target.
A shrink adds `current - new` to it and emits nothing, as before.
`maybe_flow` settles the owed `consumed_since_flow` against the surplus first (those units are retired: they are neither granted nor deferred) and grants only the rest, so the window falls to the lowered target as the application pops, in steps of the refill threshold.
A refund that settles entirely against the surplus emits no `CommandFlow` but still clears the owed count.
A growth tick absorbs the surplus before it grants anything, so growing back to a size the broker still holds grants nothing and the window ends at the target rather than above it.
`initial_flow`, the one site that seeds the window afresh at the target, zeroes the surplus; every site that zeroes the permit mirrors of a consumer that re-attaches (reset, broker close, in-place recovery, seek) is followed by it, and a rejected seek leaves the broker's window, surplus included, as it was, so `SeekRollback` carries nothing for it.
A terminal subscribe failure and an in-place re-attach the consumer is no longer eligible for zero the mirrors without an `initial_flow` and leave the surplus stale until the next one, which is harmless: the churn-window guard skips every adjust tick while `granted_permits == 0`, and a refund retired meanwhile is one the broker would have dropped against a consumer id it no longer holds.

A stateless cap in `maybe_flow` (never refund past `receiver_queue_size - permit_balance - queue.len()`) was rejected: it cannot tell a deliberate shrink from a broker that dispatched past its window.
In that case the broker's own permit count is negative and the full refund is what restores it, which ADR-0133's rollback relies on when it charges the overflow on both sides of the ledger.

**A reset drops the byte counter with the queue.**
`Connection::reset` zeroes `queued_bytes` next to `queue.clear()`.
The same pass already drops `seek_rollback`, so no earlier figure is restored later, and the raw-entry queue of a scalable aggregate is cleared by the same call and was counted in the same field.

- No wire change, no new dependency, no signature change: `window_surplus` is `pub(crate)`.
- A consumer whose target never moves (`Fixed`) never has a surplus, so its refunds are unchanged.
- Both engines run the same `Connection` code; neither runtime file changes.

## Consequences

- An `Auto` consumer that processed more than its window and then starves is topped up to the doubled target instead of wedging, and the staged ramp still emits 100, 200, 400, 800.
- After a shrink the window the broker holds converges to the new target as the application pops, so the byte budget bounds what is buffered; the cost is that a refill shortly after a shrink grants less than it is owed.
- A growth tick right after a shrink may emit no `CommandFlow` at all.
- A reset no longer pins the out-of-memory guard open on messages that no longer exist.
- Changes the ADR-0082 §Consequences statement that the growth delta is computed against `granted_permits`, and the ADR-0071 statement that a shrink "lets permits drain"; the rest of both stands.
- Not changed here: a same-broker `CommandCloseConsumer` keeps the receiver queue while zeroing the mirrors, so the window the next `initial_flow` seeds sits above the target by the queue's length until it drains. That is a separate drift and not a policy decision.

### ADR-0024 test layers

(a) `magnetar-proto/src/conn.rs` `conn_state_tests::auto_growth_tops_up_the_broker_window_to_the_new_target_after_refunds`, `auto_consumer_is_not_left_unable_to_refill_after_growth_over_refunded_window`, `auto_shrink_converges_the_broker_window_to_the_smaller_target`, `reset_zeroes_the_buffered_byte_counter_with_the_queue` and `auto_growth_after_a_shrink_is_covered_by_the_surplus_the_window_still_holds`; (b) and (c) the same scenarios in `magnetar-runtime-tokio/tests/receiver_queue_auto_growth.rs` and `magnetar-runtime-moonpool/tests/receiver_queue_auto_growth.rs`, mirrored one to one; (d) `magnetar-differential/tests/receiver_queue_policy_equivalence.rs`.
The end-to-end layer is not added in this change.
A real broker does reach all three paths: an `Auto` consumer that consumes more than its window and then slows (growth), one that buffers `max_bytes` (shrink), and one whose connection drops with messages queued (reset).
`crates/magnetar/tests/e2e_receiver_queue_policy.rs` covers the backlog drain and the arming schedule and none of them; the runtime and differential tests drive the same `Connection` with scripted broker dispatches that never exceed the granted permits.

## References

- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::adjust_receiver_queue`, `maybe_flow`, `initial_flow`, `window_surplus`.
- `crates/magnetar-proto/src/conn.rs` — `Connection::reset`.
- [ADR-0071](0071-pluggable-receiver-queue-policy.md) — the policy and its adjust tick.
- [ADR-0082](0082-consumer-permit-balance-split.md) — `granted_permits` and `permit_balance`, and the growth delta this changes.
- [ADR-0133](0133-connection-owned-seek-reply-handling.md) — the seek rollback and the overflow that is repaid in full.
