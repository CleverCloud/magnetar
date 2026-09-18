# ADR-0108 — Close the consumer before re-subscribing it in place

- **Status**: Accepted
- **Date**: 2026-09-18
- **Decider**: Florentin Dubois
- **Tags**: consumer, recovery, permits, sans-io, issue-414

## Context

[ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) §3 shipped `Connection::resubscribe_consumer_in_place`, and [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md) wired the stall watchdog to drive it. Both rest on one sentence, written into the method's own documentation and repeated in four other places in the tree:

> the broker recreates its dispatcher slot at `availablePermits = 0`

The implementation followed from it: zero `granted_permits`, `permit_balance` and `consumed_since_flow`, fail the in-flight acks, re-emit `CommandSubscribe` for the SAME, still-live consumer id, and let the re-subscribe `Success` re-grant a full `receiver_queue_size` window.

**The sentence is false.** Apache Pulsar's `ServerCnx.handleSubscribe` looks the consumer id up in its own per-connection `consumers` map before it does anything else. When `consumers.putIfAbsent` finds an entry whose subscribe has already fully completed, the broker logs a warning, calls `sendSuccessResponse(requestId)`, and returns. No new `Consumer` is constructed, no dispatcher and no subscription is touched, and nothing on that branch reads `MESSAGE_PERMITS`, `availablePermits` or `flowPermits`. Verified identical at three points on the release line:

- v4.0.4 — `ServerCnx.java:1320-1326`
- v4.2.4 — `ServerCnx.java:1408-1414`
- master — `ServerCnx.java:2037-2044`

The sibling branches of that same lookup are _not_ no-ops, which is what makes the completed-consumer branch a deliberate idempotency answer rather than an oversight: a still-pending creation is rejected with `ServiceNotReady`, and an exceptionally-completed one returns the mapped error. Neither `SubType` nor durability is read on this path at all.

Two consequences follow, and they are the client-side residue of [issue #414](https://github.com/CleverCloud/magnetar/issues/414):

1. **The client over-commits permits it was never granted.** The broker keeps the consumer's existing `availablePermits`, and the client then grants a second full `receiver_queue_size` window on top of it, once per recovery attempt. Measured on the `magnetar-differential` scripted broker with a faithful live-id model, one recovery moved the broker-observed balance for an 8-message receiver queue from `8` to `16` (`crates/magnetar-differential/tests/live_id_resubscribe_permit_overcommit.rs`). Issue #414's production signature is a subscription permit counter at `-177300`; a recovery that adds permits the broker never agreed to is moving the same counter, in the opposite direction, on the same client's initiative.
2. **The recovery cannot recover anything.** The broker-side slot the method exists to reset is not reset. The client zeroes its own mirrors, the broker changes nothing, and the two are now further apart than before the call.

The rest of the mechanism was built on the same premise and was therefore also wrong in the same direction: zeroing the permit mirrors described a broker state that never happened, and `fail_acks_orphaned_by_consumer_reattach` failed acks against a consumer generation the broker had never retired.

### Alternatives considered

- **Subscribe under a FRESH consumer id.** This is rung 2 of the recovery ladder and it works, but it is not an in-place repair: the handle the application holds is bound to its consumer id across the whole runtime surface (`ConsumerHandle` keys `consumers`, `consumer_subscribe_requests`, every `PendingRequestKind`, the receiver queue, the ack tracker). Re-keying them is a different, larger change, and the caller can already do it by closing and re-subscribing.
- **Send a corrective `CommandFlow` instead of a re-subscribe.** The wire protocol carries only monotonic permit increments; there is no decrement. A client cannot lower a broker-side counter, which is also why issue #414's negative aggregate cannot have a client-side cause.
- **Keep the live-id re-subscribe and stop re-granting on its `Success`.** That removes the over-commit but leaves the method a no-op with a `tracing::info!` that says it recovered something. A recovery entry point that provably does nothing is worse than no entry point.
- **Delete the recovery and leave only detection.** ADR-0101 considered and rejected event-only; ADR-0103 then added automatic recovery. Reverting both to repair a premise is a larger retreat than the evidence asks for — the entry point is sound once the close makes it real.

## Decision

`Connection::resubscribe_consumer_in_place` **closes the consumer and re-subscribes it on the close's `Success`**, in two phases on the same live socket, with no transport reconnect.

- **Phase 1** — emit `CommandCloseConsumer` for this consumer id, registered as the new `PendingRequestKind::ConsumerCloseForReattach`. The client slot is deliberately NOT marked closed and its ack-grouping tracker is not flushed: this consumer is being repaired, not retired. Return that close's `RequestId`; the re-subscribe's does not exist yet.
- **Phase 2** — on the close `Success`, and only then, `Connection::complete_consumer_close_for_reattach` zeroes the permit mirrors, drops any open stall window, fails every in-flight ack (issue #346), and re-emits `CommandSubscribe` with the initial `CommandFlow` deferred to ITS `Success`.

The mirror zeroing and the deferred initial flow are unchanged code. What changes is what they are anchored to: the broker has now really dropped the slot, so the slot really is recreated at `availablePermits = 0` and the client's mirrors really do describe it.

**Every mutation moves to phase 2.** A rejected close leaves the consumer byte-for-byte as the caller found it — still wedged, but still holding the positive `permit_balance` that `is_stall_candidate` requires, so it stays detectable and another attempt stays possible. Zeroing up front and then failing to close would have produced a consumer that is wedged AND invisible to the watchdog, which is strictly worse than the stall it was called to repair.

**Two subscription shapes are refused outright**, returning `None` and mutating nothing, exactly as the existing eligibility gate refuses a closed, unsubscribing, terminal, mid-seek or already-re-attaching consumer:

- **`Failover`** — the close really detaches the consumer, so the broker runs an election. An active consumer would hand its partition to a standby and return at the end of the priority order; a standby's re-attach re-enters that order. That is a subscription-wide reshuffle to repair one slot.
- **non-durable** — a non-durable subscription's cursor lives only as long as the consumer. Closing it discards the cursor, and the re-attach restarts from the subscribe request's `start_message_id` ([ADR-0099](0099-nondurable-reattach-cursor-safety.md)), silently skipping or replaying the backlog. There is no in-place repair for a cursor the close destroys.

Both refusals live in `resubscribe_consumer_in_place` and **not** in the shared `consumer_reattach_in_place_is_eligible`, which issue #307's broker-initiated `CommandCloseConsumer` arm and phase 2's own re-attach both call: there the broker has already closed the consumer, so refusing a Failover standby or a non-durable consumer would only leave it detached forever. ADR-0103's Failover-standby pre-check in the stall watchdog is retained and is now redundant for `Failover` specifically; it still documents why a standby's silence is correct.

**Races are resolved by existing state, not by new bookkeeping:**

- **A second `resubscribe_consumer_in_place` while a close is pending** is refused. The marker is read out of `pending_requests` rather than out of a `ConsumerState` flag, so it clears itself exactly when the session that carried the close dies.
- **The connection dropping mid-sequence** is won by the reconnect path. `Connection::reset` and `Connection::fail_all_pending` take `pending_requests` wholesale and now consume a `ConsumerCloseForReattach` entry without materialising an undrainable `OpOutcome` (the issue #241 leak shape the two `*CloseForgotten` kinds already avoid). Phase 2 therefore never runs for a dead session — correctly, because the broker reaped that consumer with the connection — and `rebuild_consumers` owns the re-attach.
- **A broker-initiated `CommandCloseConsumer` arriving inside the window** re-attaches through issue #307's arm, which sets `flow_on_subscribe_ack`; phase 2 then finds the consumer ineligible and emits nothing. One `CommandSubscribe`, not two.

## Consequences

**The permit accounting is now honest.** One recovery attempt is exactly permit-neutral on the subscription's aggregate under correct broker accounting: the close returns this consumer's remaining permits and the re-subscribe's `CommandFlow` grants them back. The broker-observed per-consumer balance after one recovery is `receiver_queue_size`, not `2 x receiver_queue_size`.

**Automatic recovery no longer pays down a dispatcher-wide leak, and ADR-0103's budget arithmetic is superseded.** ADR-0103 stated that "one attempt credits the aggregate back exactly `receiver_queue_size`". That was true only of the live-id re-subscribe, which credited a window without the broker ever having debited one — i.e. it was true only because of the over-commit this ADR removes. The recovery's own `CommandCloseConsumer` is itself a consumer-churn event, so under the hypothesised churn-accounting fault that issue #414's harness models, attempts no longer climb out of the hole. `consumer_stall_auto_recovery` therefore repairs **this client's own slot** and nothing wider — which is what ADR-0101 always claimed for it — and `pulsar-admin topics unload` remains the escalation for a subscription-wide corruption. The bound stays small for the same reason it always was: to stop and escalate rather than act forever against a fault this client cannot repair.

**A recovery now costs redelivery.** The broker genuinely drops the consumer, so everything it held un-acked returns to the subscription's redelivery pool: to this consumer after its re-attach, or to a `Shared` sibling meanwhile. Acks the grouping tracker emits inside the close → re-subscribe window are dropped by the broker ("Cannot find consumer") and their messages redelivered. At-least-once is preserved; duplicates are not. In-flight acks are genuinely orphaned, which is why phase 2 fails them and why `resubscribe_in_place_fails_in_flight_acks` is the correct contract.

**A recovery now costs one extra round trip.** `Consumer::resubscribe()` still returns as soon as phase 1 is staged, but the grant re-arms two broker replies later instead of one.

**Two subscription shapes lose the entry point.** A `Failover` or non-durable consumer that wedges has rungs 2 and 3 of the ladder and no rung 1. That is the honest position: for both, the close this recovery depends on is not a repair.

**The four sites asserting the false premise are corrected in the same changeset**, and the two differential scenarios whose expected values were derived from it are re-measured rather than re-tuned.

## References

- `crates/magnetar-proto/src/conn.rs` — `resubscribe_consumer_in_place`, `emit_consumer_close_for_reattach`, `complete_consumer_close_for_reattach`, `consumer_close_for_reattach_is_pending`, `PendingRequestKind::ConsumerCloseForReattach` and its `Success` / `Error` / `reset` / `fail_all_pending` handling.
- `crates/magnetar-differential/tests/live_id_resubscribe_permit_overcommit.rs` — the over-commit regression, red at `([8,16],[8,16])` before this change and green at `([8,8],[8,8])` after.
- `crates/magnetar-differential/src/broker.rs` — `ScriptedBroker::subscribe_on_live_consumer_id_is_a_success_noop`, the opt-in faithful live-id model that makes the defect observable.
- `crates/magnetar-runtime-tokio/tests/consumer_stall_recovery.rs`, `crates/magnetar-runtime-moonpool/tests/consumer_stall_recovery.rs` — the two-phase watchdog coverage, 1:1 across engines (ADR-0024).
- `crates/magnetar-differential/tests/shared_subscription_churn_equivalence.rs` — the two leak scenarios, re-measured under the new arithmetic.
- `docs/consumer-stall-recovery.md` — the operator ladder.
- `ARCHITECTURE.md` § the consumer stall watchdog — the in-tree summary of both phases.
- [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) — amended: §3's "zero the mirrors, re-emit `CommandSubscribe` for the same consumer id" becomes close-then-re-subscribe, and its "the broker recreates its dispatcher slot at `availablePermits = 0`" premise is corrected.
- [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md) — amended: the in-place-recovery clause now runs through the close, and the "one attempt credits the aggregate back exactly `receiver_queue_size`" arithmetic is superseded.
- [ADR-0099](0099-nondurable-reattach-cursor-safety.md) — the non-durable cursor rule the non-durable refusal follows from.
- [ADR-0102](0102-grant-the-initial-consumer-flow-once-per-attach.md) — corrected: one parenthetical in its rejected "zero the mirrors at every emission" alternative restates the same false premise. Its rejection rests on blast radius and is unaffected.
- [ADR-0102](0102-grant-the-initial-consumer-flow-once-per-attach.md) — `initial_grant_due`, which is what makes the deferred flow grant exactly once per attach.
