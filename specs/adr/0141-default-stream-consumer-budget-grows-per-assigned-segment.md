# ADR-0141 — Grow the default `StreamConsumer` receive budget by one FLOW permit per assigned segment

- **Status**: Proposed (amends [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) §Aggregate receive budget and concurrency for the default budget only; a proposal for the maintainers to arbitrate)
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: scalable-topics, stream-consumer, receive-budget, flow-control, liveness

## Context

The assignment-driven `StreamConsumer` ([ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md)) grants every child consumer manual FLOW, and charges each granted permit to one aggregate budget as a `MAX_FRAME_SIZE` (5,242,880 bytes) reservation, because the size of the message a permit will admit is unknown.
The default budget is `DEFAULT_RECEIVER_BUDGET_BYTES`, 16 MiB including the 64 KiB cleanup reserve, so the data limit is 16,711,680 bytes.
Of that, `RECEIVER_BUDGET_AUTHORITY_HEADROOM` (8,388,744 bytes) stays free for delivery authority, which leaves 8,322,936 bytes for everything else: **exactly one FLOW permit** (a second one would need 10,485,760) and 3,080,056 bytes of slack for retained messages.

Two consequences follow.

1. `StreamConsumerModel::arbitrate_flow` grants eligible children in cursor order and stops at the first reservation that fails.
   A `Flowing` child gives its permit back only when a frame arrives, at a terminal, a seek or a close; `StopFlow` is emitted for a seek or a drain only, and Pulsar cannot retract a granted permit.
   Nothing times an unused permit out.
2. After an arrival the cursor starts after the last grantee, so a busy child hands the single permit to the next eligible child even when that child never receives anything.

With two assigned segments, one idle and one busy, the busy segment therefore delivers one message per message that reaches its idle sibling, which is none, and the aggregate reports neither an event nor an error.
Reproduced by `default_budget_does_not_strand_a_busy_segment_behind_an_idle_sibling` (`crates/magnetar-differential/tests/stream_consumer_equivalence.rs`): depending on which child the first permit goes to, either the busy segment's first message or its second is not delivered within the 60 s hang guard, and in both cases the fake broker shows `permits` 0 on the busy segment and 1 on the idle one, with `receiver_budget_used` at 5,242,880 (the parked permit alone) or 5,243,752.

[ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) accepts "lower concurrency when the configured budget admits few maximum-frame reservations", and the proposal calls fair selection "an implementation liveness property".
A permit parked on a segment with no traffic is neither lower concurrency nor fairness: it is zero progress for every other segment, for as long as the idle segment stays idle.
The cases ADR-0120 lists as accepted forever-waits (cross-member ancestry in Strict mode, a drain on an unresolved delivery, M1 close) do not include it.

The defect has no one-line fix because a granted permit cannot be revoked.
The remedies considered, and why each was or was not taken:

- **Refuse, with a typed budget error, a budget that cannot hold one permit per assigned segment.** Not taken.
  `freed_capacity_rotates_fairly_across_eligible_children` (`stream_consumer.rs`) and the differential scenario `observe_single_permit_budget` pin one permit rotating over two segments on an **explicit** budget as supported behaviour, which is what ADR-0120 accepts, and a refusal turns both red.
  Applied to the default alone it would make every default `subscribe()` on a topic with two or more segments fail, although it works today whenever every segment carries traffic: 29 of the 87 `scalable_stream_consumer(…)` call sites in the repository's Rust sources leave the budget at its default.
- **Raise the floor of `ReceiverBudget::bytes`.** Not taken.
  It invalidates budgets that are valid today, the minimum-budget scenario among them, and so breaks a public constructor.
- **Raise the constant `DEFAULT_RECEIVER_BUDGET_BYTES`.** Not taken.
  A fixed 32 MiB moves the threshold from two segments to three, which is a mitigation and not a cure, and it raises the memory ceiling of every single-segment consumer by 16 MiB.
- **Change the arbiter (keep the permit on the busy child, skip a child that has not yet received anything).** Not taken.
  The arbiter cannot know which child is idle: the first grant may go to the idle child, and what it grants cannot be taken back.
  A rule that keeps the permit on the child that last received fails whenever the idle segment is the first one granted, which the proving scenario itself shows in some runs: the busy segment's first message is then never delivered.
- **Reclaim an unused permit on a timer by closing and reopening its child.** Not taken for this change.
  It needs a clock in the sans-io model, a new action, and a close and reopen of every quiet segment on each period, with the ordering, drain and acknowledgement state that this resets.
  It is the only remedy that also repairs an explicit budget below one permit per segment, and it stays open as a later decision.

## Decision

The default budget grows with the assignment; an explicit budget never does.

- **`ReceiverBudget::growing_bytes(bytes)`** validates `bytes` like `ReceiverBudget::bytes` and marks the budget as growing.
  A growing budget is the size for one assigned segment: the model raises its limit by one `MAX_FRAME_SIZE` for each assigned segment beyond the first, so that every assigned segment can hold a FLOW permit at once.
  The retained-message slack does not grow: the aggregate keeps the one 3,080,056-byte slack that the default holds beside one permit, shared by every segment (for two segments, 22,020,096 − 65,536 − 8,388,744 − 2 × 5,242,880 = 3,080,056), so busy segments with large messages still take turns through it; that is backpressure that clears as messages are acknowledged, not a stall.
  With _n_ assigned segments the default limit is `16 MiB + (n − 1) × 5,242,880`: 16,777,216 for one segment, 22,020,096 for two.
- **`StreamConsumerBuilder` starts from `ReceiverBudget::growing_bytes(DEFAULT_RECEIVER_BUDGET_BYTES)`**, and `receiver_budget(…)` replaces it with the caller's fixed budget.
  `ReceiverBudget::bytes` is unchanged, so every explicit budget, including the exact minimum, behaves as before.
  No public signature changes: `ReceiverBudget` gains one private field, `StreamConsumerOptions` and `StreamConsumerModel::new` keep their shapes, and `DEFAULT_RECEIVER_BUDGET_BYTES` keeps its value.
- **The limit follows every assignment change and never shrinks.** `reconcile_assignment`, which every assignment and control-plane transition goes through, counts the children it holds afterwards, draining children included because they still hold permits, and raises the staged budget to cover that count.
  A limit that fell below bytes already reserved would underflow `data_available`, and a segment that returns would find its room gone, so the high-water mark stays for the life of the aggregate.
- **A chunk chain is still judged against the configured size.** The check that a chain "cannot fit even alone" ([ADR-0131](0131-several-chunk-chains-per-stream-consumer-child.md)) subtracts the grown bytes from the limit it compares with.
  Against the grown limit a chain that fits only because of the room reserved for sibling permits would pass that check, then meet those parked permits and fail with `Exhausted`, a resynchronization loop where the typed, terminal `MessageTooLargeForBudget` is the contract.
  The documented chunk-chain bound, 8,322,936 bytes at the default budget, therefore holds for any number of segments.
  Decompression and batch-assembly reservations are still judged against the grown limit, so one that the configured size refuses as `MessageTooLargeForBudget` can pass that check and then meet the sibling permits as `Exhausted`; both runtimes answer either error from that work with a resynchronization, as before, so the outcome on that path is the same, or better when the sibling permits leave room and the reservation now succeeds.

## Consequences

- A busy segment is no longer stranded behind an idle one at the default budget, and both engines behave identically because the sizing is in the shared model.
  The first permit still goes to whichever child is granted first, and a second message is delivered only after the first permit's capacity was returned, as before.
- **The default memory ceiling scales with the segment count**: _n_ segments admit up to `16 MiB + (n − 1) × 5 MiB` of controlled receive memory, which contradicts the statement that adding segments "never multiplies the budget" for the default; the statement now holds for an explicit budget only.
  The figure is an accounting limit, reached only when every assigned segment delivers a maximum-size frame at once.
  A caller that needs a hard memory bound sets `receiver_budget(…)` explicitly and accepts the residual below.
  Capping the growth would bound the memory and bring the stall back above the cap, so no cap is proposed.
- `StreamConsumerStatus::receiver_budget_limit` is no longer the constant 16 MiB for a default consumer: it reports the grown limit.
  A growing budget also compares unequal to a fixed budget of the same size, because `ReceiverBudget` derives `PartialEq` over its new private field.
- One existing test pinned the old default: `live_aggregates_surface_identical_facade_failures_after_client_close` (`scalable_client_equivalence.rs`) waited, on both engines, for two attached children holding exactly one permit.
  It is a readiness wait before the test closes the client, not an assertion of the stall, and it now waits for two permits; the other four stream-consumer differential targets, the proto and facade tests pass unchanged, and a search for `permits == 1` found the remaining pins only in tests that set an explicit budget or drive the fake broker without an aggregate.
  If the maintainers prefer a remedy that keeps one permit at the default, that wait goes back with it.
- **Residual, not fixed here.** An explicit budget that holds fewer maximum-frame permits than there are assigned segments can still strand a busy segment behind an idle one, and a Draining child that waits for a flow drain (`wait_for_flow_drain`) still pins its permit until it receives a frame.
  Sizing the budget is the caller's decision, as [ADR-0131](0131-several-chunk-chains-per-stream-consumer-child.md) already says for chunk chains.
  The timer-driven reclaim above is the remedy for that case if the maintainers want one.
- The change is additive and reversible: dropping `growing_bytes` from the builder restores the previous default and nothing else depends on it.

## References

- `crates/magnetar-proto/src/stream_consumer.rs` — `ReceiverBudget::growing_bytes`, `ReceiverBudget::fit_segments`, `ReceiverBudgetState::non_authority_limit`, `StreamConsumerModel::reconcile_assignment`, `arbitrate_flow`.
- `crates/magnetar/src/scalable.rs` — `DEFAULT_RECEIVER_BUDGET_BYTES`, `StreamConsumerBuilder`.
- `crates/magnetar-differential/tests/stream_consumer_equivalence.rs` — `default_budget_does_not_strand_a_busy_segment_behind_an_idle_sibling`, `observe_single_permit_budget`.
- [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md), [ADR-0131](0131-several-chunk-chains-per-stream-consumer-child.md), [`docs/pip-features.md`](../../docs/pip-features.md) §Aggregate receive budget.
