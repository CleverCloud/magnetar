# ADR-0102 — Grant the initial consumer flow once per attach

- **Status**: Accepted (amends [ADR-0082](0082-consumer-permit-balance-split.md), the "two callers" clause)
- **Date**: 2026-08-22
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, failover, sans-io, issue-427

## Context

[Issue #427](https://github.com/CleverCloud/magnetar/issues/427), found while testing [issue #426](https://github.com/CleverCloud/magnetar/issues/426): a fresh `Exclusive` or `Failover` subscribe grants the broker `2 × receiver_queue_size` permits.
Measured 32 against a configured 16 with the issue #426 mock broker at `announce_active: true`.
`Shared` subscriptions are correct at `1 ×` — the broker never sends `CommandActiveConsumerChange` for them, which is exactly why ADR-0082's sibling fix for #426 left the count right there and this case open.

Two independent sites can decide a consumer needs its initial grant, and on this path a real broker makes both fire.

- **The runtime's post-ack call.** `Connection::subscribe` emits `CommandSubscribe` with `SubscribeAckAction::NotifyWaiter`, which deliberately does NOT set `flow_on_subscribe_ack`: the `Success` arm only wakes the waiter, and the ENGINE issues `Connection::initial_flow` once the subscribe future resolves (`Client::subscribe_with_operation_deadline` in `crates/magnetar-runtime-tokio/src/client.rs`, mirrored in `crates/magnetar-runtime-moonpool/src/consumer.rs`).
- **The issue #307 promotion re-arm.** The `CommandActiveConsumerChange` arm of `Connection::handle_command` re-issues `Connection::initial_flow` for a promoted consumer holding `granted_permits == 0`, so a standby promoted against a non-empty backlog is not starved forever.

A real broker answers the subscribe with `CommandSuccess` and then `CommandActiveConsumerChange { is_active: true }` in the same write.
Both frames therefore reach the client in one read and are decoded inside one `handle_bytes` call on the driver task — while the subscribing task is still parked on the resolving subscribe future.
At that instant `granted_permits` is legitimately `0`, because the grant is owed and has not been issued, so the re-arm's gate passes and it grants.
The engine's own call then lands on top and grants again.

The guard is a proxy that answers the wrong question.
`granted_permits == 0` asks "does the client hold an outstanding grant?" as a stand-in for "has the broker been told it may dispatch?".
Those coincide everywhere except in the window between a `CommandSubscribe` going out and its grant being issued — and that window is precisely where the broker's active announcement lands.

The cost is the same one issue #426 carried: the broker may hand a consumer twice the messages its receiver queue was sized for, the configured memory ceiling is doubled, and every client-side reading of the broker's balance — including the ADR-0101 stall signature, which is measured against exactly that balance — is wrong from the first frame.

### Alternatives considered

- **Gate the #307 re-arm on "the runtime still owes this attach's grant".** The symmetric twin of the existing `!flow_on_subscribe_ack` clause: a new flag set on `NotifyWaiter` subscribes and cleared at the grant, blocking the re-arm until the engine has had its turn. Correct, but it makes the re-arm the loser of the race by construction rather than making the outcome order-independent, and it does not help if a broker ever sends the announcement BEFORE the `Success`. It also invalidates the way all three existing #307 tests model a starved standby (subscribe, ack, no `initial_flow`), which would have had to be rewritten to keep asserting behaviour the fix leaves untouched.
- **Zero the permit mirrors at every `CommandSubscribe` emission**, making `granted_permits != 0` a sufficient guard on its own with no new state. Semantically right — the broker recreates its dispatcher slot at zero permits — but it changes the post-seek resubscribe's live permit accounting mid-flight, where `flow_stats` and `adjust_receiver_queue` read those mirrors. A wider blast radius than the decision needs.
- **Drop the #307 re-arm and let the engines own every grant.** Refused: the re-arm is the only path that restores flow to a consumer whose mirrors a churn boundary zeroed without a re-subscribe following, and `maybe_flow` cannot substitute for it (it only fires once messages have been consumed, and none can arrive at zero permits).

## Decision

`Connection::initial_flow` grants **at most once per attach**.

It emits a `CommandFlow` (and arms the ADR-0084 auto-adjust schedule and the ADR-0101 stall window) when either holds:

- **`ConsumerState::initial_grant_due`** — a `CommandSubscribe` has gone out since the last grant, so the broker's freshly (re-)created dispatcher slot starts at zero permits.
  The flag is set by `Connection::emit_command_subscribe` for every emission — fresh subscribe, reconnect rebuild, transient-subscribe retry, post-seek resubscribe, in-place re-attach — and cleared by `ConsumerState::initial_flow`, the single funnel every initial grant already routes through (ADR-0071, ADR-0084).
- **`granted_permits == 0`** — the client holds no outstanding grant at all.
  This is the churn boundary the issue #307 re-arm exists for: a promoted consumer whose mirrors a `reset`, a terminal subscribe failure, or a same-broker `CommandCloseConsumer` zeroed still gets its flow back.

Neither being true means this attach's grant already reached the broker, and a second one would be the double grant above — so the call is a no-op that logs at `DEBUG` and returns `None`, which is what it returned on every path already.
No signature, return type, or public API changes; `initial_grant_due` is `pub(crate)` on `ConsumerState`, exactly like `flow_on_subscribe_ack_request`.

Both conditions are needed and neither is redundant.
The post-seek resubscribe (`Connection::resubscribe_consumer_after_seek`) re-attaches without zeroing the additive `granted_permits` mirror, so only the flag can tell that attach apart from a consumer that is already fed — dropping it would reintroduce issue #67, where the broker confirms a backlog after the cursor reset and dispatches nothing.
A genuine later promotion carries no fresh `CommandSubscribe`, so only the mirror can tell it apart from a fed consumer — dropping it would silently retire the issue #307 re-arm.

### What this deliberately does not change

- **The #307 re-arm's own gate is untouched.** It still fires only for an `active == true` transition on a dispatch-eligible consumer at `granted_permits == 0`. On a fresh subscribe it now legitimately wins the race and issues the grant, and the engine's post-ack call is the no-op; that is fine, because the grant is owed either way and idempotence makes the ORDER of the two callers unobservable on the wire.
- **`maybe_flow` and `adjust_receiver_queue` are untouched.** Replenishment and growth are incremental top-ups against a live grant; they do not route through `initial_flow` and are not idempotent per attach, nor should they be.
- **The engines are untouched.** The fix is entirely inside `magnetar-proto`, so both runtimes inherit it and the ADR-0024 differential claim is that the two engines react identically — which is what `initial_flow_grant_equivalence.rs` asserts.

## Consequences

- A fresh `Exclusive` / `Failover` subscribe grants exactly `receiver_queue_size`, matching what `available_permits()` and `FlowStats` have always reported and what the user configured. With issue #426 this closes the initial-grant count on every subscription type.
- `Connection::initial_flow` is idempotent per attach, so a future third caller cannot reopen this class of bug by construction. The cost is that a caller which genuinely wanted a redundant re-grant would now be silently ignored; there is no such caller, and the `DEBUG` line names the skip.
- The issue #307 re-arm keeps its real case — a promoted consumer at a zeroed grant mirror — and is now also the site that legitimately issues the FIRST grant on a fresh Failover subscribe, which it did not before.
- ADR-0024's five layers ship in the same changeset, each seen red at 32-against-16 with the guard disabled: two `magnetar-proto` unit tests (one pinning the single grant across `Success` + `ActiveConsumerChange` + the engine's call, one pinning that an established consumer zeroed at a churn boundary still re-arms), twin tokio / moonpool wire-level assertions on the mock broker's tallied grant with `announce_active: true`, a differential trace over a `ScriptedBroker` that now announces the active consumer behind every subscribe `Success`, and a broker-side `availablePermits` assertion in `e2e_failover_subscription_active_only`.
- The `magnetar-differential` scripted broker gains one knob, `announce_active_consumer_on_subscribe`, off by default so every other trace keeps its existing shape.

### Amends ADR-0082

[ADR-0082](0082-consumer-permit-balance-split.md) § Decision states:

> **`ConsumerState::granted_permits: u32`** — the existing field, renamed, semantics UNCHANGED: a purely additive record of every permit granted to the broker since the last zeroing (subscribe, reconnect reset, terminal subscribe failure, same-broker `CloseConsumer`). It answers "how much have we told the broker it may use" — the #307 failover-reflow gate (`conn.rs`'s `ActiveConsumerChange` arm) and the `adjust_receiver_queue` want-have delta both need exactly that question answered, and keep reading this field.

and its 2026-08-21 amendment restates "`granted_permits` keeps its additive semantics and its two callers".

The field's semantics are unchanged and remain binding.
What this ADR amends is the callers clause: `granted_permits` now has a **third** reader, `Connection::initial_flow`'s once-per-attach guard, and the #307 failover-reflow gate is no longer the sole arbiter of whether that promotion actually grants — its `granted_permits == 0` predicate answers "does the client hold an outstanding grant", which on a fresh subscribe is true while the grant is merely owed.
`ConsumerState::initial_grant_due` supplies the part the additive mirror cannot answer.

ADR-0082's churn-window guard (`adjust_receiver_queue` returns `None` at `granted_permits == 0`) is untouched, as is every other ADR-0082 decision and ADR-0101's amendment of the `consumer_available_permits` accessor.

## References

- [`crates/magnetar-proto/src/conn.rs`](../../crates/magnetar-proto/src/conn.rs) — `Connection::initial_flow`'s once-per-attach guard, `emit_command_subscribe`'s flag set, and the `ActiveConsumerChange` arm's re-arm gate.
- [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs) — `ConsumerState::initial_grant_due` and `ConsumerState::initial_flow`.
- [ADR-0082](0082-consumer-permit-balance-split.md) — the additive grant mirror this guard reads, amended above.
- [ADR-0084](0084-arm-auto-adjust-schedule-at-initial-flow.md) — `initial_flow` as the single funnel every first grant routes through; the arming rides the same branch.
- [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) — the stall window `initial_flow` seeds, and the balance the doubled grant used to falsify.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the five test layers this ships with.
