# ADR-0082 — Split the consumer permit mirror into a grant register and a real balance

- **Status**: Accepted
- **Date**: 2026-07-17
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, pip-74, receiver-queue, java-parity, issue-349

## Context

[Issue #349](https://github.com/CleverCloud/magnetar/issues/349) reports that the `Auto` receiver-queue policy (ADR-0071, PIP-74 `autoScaledReceiverQueueSizeEnabled` parity) never scales up under real load.

`ConsumerState::available_permits` (`crates/magnetar-proto/src/consumer.rs`) was a single `u32` bumped at every grant site — `initial_flow`, `maybe_flow`, and `adjust_receiver_queue`'s growth branch — and never decremented as messages actually arrived.
It is purely additive: once a consumer's first flow lands, the field only grows (or gets forced to `0` at a session reset, a terminal subscribe failure, or a same-broker `CommandCloseConsumer`) for as long as the consumer runs.

`ConsumerState::flow_stats` fed this same field into `FlowStats::available_permits`, the signal [`Auto::adjust`](../../crates/magnetar-proto/src/receiver_queue.rs) uses to detect starvation (`available_permits == 0`).
Because the field never decremented under real dispatch, `available_permits == 0` was reachable only via the churn-reset paths, never via a broker legitimately exhausting its grant.
`Auto` therefore never observed a genuine starvation signal and never grew past its floor — the headline symptom in #349, even though the policy's own doubling/OOM-guard/hysteresis logic (verified independently in `crates/magnetar-proto/src/receiver_queue.rs`'s unit tests) was correct.

A second, related bug: the #307 failover-reflow gate and the same reset/close-consumer paths that zero the permit mirror are legitimate "no outstanding grant" states, not starvation — but with a single field, any future fix that made the field track real dispatch would make these churn windows indistinguishable from starvation, risking a growth (and a wasted `CommandFlow`) on every reset or same-broker bundle reassignment.

## Decision

Split the one field into two, each with a single, unambiguous meaning.

- **`ConsumerState::granted_permits: u32`** — the existing field, renamed, semantics UNCHANGED: a purely additive record of every permit granted to the broker since the last zeroing (subscribe, reconnect reset, terminal subscribe failure, same-broker `CloseConsumer`). It answers "how much have we told the broker it may use" — the #307 failover-reflow gate (`conn.rs`'s `ActiveConsumerChange` arm) and the `adjust_receiver_queue` want-have delta both need exactly that question answered, and keep reading this field.
- **`ConsumerState::permit_balance: u32`** — new field, the REAL broker-side balance: `granted_permits` minus one unit per broker dispatch unit that has actually arrived. Incremented at the same three grant sites as `granted_permits`, by the identical delta. Decremented by exactly one (`saturating_sub`) per dispatch unit as it arrives:
  - once per delivered logical message in `classify_and_queue` — covers a plain message, each batch member, and the chunk-completing logical message, unconditionally across both the queued and dead-lettered branches (the broker already spent the permit dispatching the entry regardless of where the client routes it);
  - once per incomplete chunk buffered in `deliver` (the chunk never reaches `classify_and_queue` while reassembly is pending, but the broker already dispatched it);
  - once per PIP-33 marker in `record_marker_consumed`.

  Force-zeroed everywhere `granted_permits` is zeroed, so the two mirrors never drift apart at a churn boundary.

- **`flow_stats` feeds `permit_balance`, not `granted_permits`, into `FlowStats::available_permits`.** The public contract documented on `FlowStats::available_permits` ("`0` is the starvation signal") is now literally true.
- **Churn-window guard**: `adjust_receiver_queue` returns `None` immediately when `granted_permits == 0`. A zero grant mirror only occurs right after a reset / terminal-failure / same-broker `CloseConsumer` zeroing — there is no outstanding grant for the broker to have dispatched against, so a zero `permit_balance` in that window reflects the churn, not load starvation. Without the guard, a tick landing in that window would misread it and grow (or emit a `CommandFlow` the broker would drop against a torn-down consumer id).
- **`Auto::adjust` itself is unchanged.** The policy's doubling/OOM-guard/hysteresis math was already correct; only the signal it is fed was wrong.
- **`Connection::consumer_available_permits()`** (and the façade's `Consumer::available_permits()` chain on both engines) is intentionally left reading `granted_permits` — unchanged behaviour. Extending Java-parity semantics (a genuinely decrementing counter matching `ConsumerBase#getAvailablePermits`) to this public accessor is a separate, unscoped change; this ADR's fix is confined to the `FlowStats::available_permits` signal `Auto::adjust` consumes.

The dispatch-unit accounting mirrors ADR-0076's `consumed_since_flow` conservation rule exactly (K for a K-message batch, one per PIP-37 chunk, one per PIP-33 marker) but is tracked as an independent counter: `permit_balance`'s decrement sites are deliberately NOT routed through `record_broker_permit_consumed` (which only tracks the pop-driven `consumed_since_flow` counter — the wrong site for a live balance that must reflect arrival, not user-side consumption).

## Consequences

- `Auto` now genuinely ramps under real dispatch-driven starvation — the issue #349 fix. Proto unit tests (`auto_policy_grows_under_real_dispatch_starvation`, the four `permit_balance_decrements_per_dispatch_unit_*` shape tests, and `permit_balance_decrements_for_dlq_routed_message_too`), tokio/moonpool integration tests (`receiver_queue_auto_growth.rs`, 1:1), a differential rewrite (`receiver_queue_policy_equivalence.rs`, real deliveries replacing the old synthetic-zero hack), and an e2e growth assertion (`e2e_receiver_queue_policy.rs`) all exercise the real signal.
- `adjust_skips_growth_during_churn_window` (proto, tokio, moonpool) pins the new churn-window guard: a same-broker `CommandCloseConsumer` no longer risks a spurious growth/flow during re-attach.
- The #307 failover reflow gate, `flow_refills_on_half_drain`, `fixed_policy_default_never_adjusts`, and the ADR-0076 chunk-conservation tests are unaffected — they read `granted_permits`/`consumed_since_flow`, neither of which changed meaning.
- One observable consequence of finally exercising real growth: the `CommandFlow` delta a growth tick emits is computed against `granted_permits` — a value real dispatch never perturbs — so successive growth ticks top up by the PREVIOUS target rather than re-granting the full new target (e.g. ramping 100→200→400→800→1600 emits flows of 100, 200, 400, 800, not 200, 400, 800, 1600). This is `adjust_receiver_queue`'s pre-existing want-have arithmetic (design item 1, unchanged); before this fix, growth essentially never fired under real dispatch at all (the #349 bug), so there is no real prior production behaviour this differs from — only the tests' synthetic `available_permits = 0` hack, which forced `have` to `0` on every tick and is exactly what this ADR's test rewrites remove.
- `Connection::consumer_available_permits()` keeps its pre-existing (additive, non-decrementing) semantics; a reader expecting Java-parity decrementing behaviour from that specific accessor will not get it from this change.

## References

- [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs) — `ConsumerState::granted_permits` / `permit_balance`, `flow_stats`, `adjust_receiver_queue`, `classify_and_queue`, `record_marker_consumed`.
- [`crates/magnetar-proto/src/conn.rs`](../../crates/magnetar-proto/src/conn.rs) — the three zeroing sites (`reset`, `fail_consumer_subscribe`, the `CloseConsumer` same-broker arm) and the #307 reflow gate.
- [`crates/magnetar-proto/src/receiver_queue.rs`](../../crates/magnetar-proto/src/receiver_queue.rs) — `FlowStats::available_permits` doc, `Auto::adjust` (unchanged).
- [ADR-0071](0071-pluggable-receiver-queue-policy.md) — the pluggable receiver-queue policy this ADR fixes the starvation signal for.
- [ADR-0076](0076-conserve-flow-permits-across-chunk-reassembly.md) — the dispatch-unit conservation rule `permit_balance`'s decrement sites mirror.
- [ADR-0038](0038-split-connection-mutex.md) — lock ordering (`adjust_receiver_queue` runs entirely under the per-slot lock).
