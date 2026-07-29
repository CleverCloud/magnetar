# ADR-0084 — Arm the `Auto` receiver-queue adjust schedule at initial-flow time

- **Status**: Accepted
- **Date**: 2026-07-29
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, sans-io, determinism, parity

## Context

[ADR-0071](0071-pluggable-receiver-queue-policy.md) put the `Auto` receiver-queue adjust tick on `Connection::handle_timeout(now)` and surfaced its deadline through `Connection::poll_timeout`, so the driver wakes deterministically for it.
What that decision left implicit is **how the schedule starts**.

`ConsumerState::next_adjust_deadline()` derives the deadline from `last_adjust_at + adjust_interval` and returns `None` while `last_adjust_at` is `None`.
The only writer that could set it out of nothing was `ConsumerState::arm_adjust_clock`, and it had exactly one caller: the `None =>` fallback arm inside `handle_timeout`'s consumer-slot loop.
So the first arm required `handle_timeout` to run, `handle_timeout` runs only when a `poll_timeout()` deadline actually elapses, and — for a fresh `Auto` consumer whose adjust deadline is still `None` — the only deadline `poll_timeout()` can return is the keepalive one, `last_activity + keepalive_interval`.

That is a cycle with a real escape hatch problem: every decoded inbound frame refreshes `last_activity` ([ADR-0058](0058-keepalive-watchdog-progress-based.md)'s single refresh site).
A connection with continuous inbound traffic — message deliveries, or the `CommandAckResponse` stream produced by a consumer that awaits each individual ack — therefore pushes the keepalive deadline forward faster than it can elapse.
`handle_timeout` never runs, `arm_adjust_clock` is never called, `next_adjust_deadline()` stays `None`, and `Auto` never scales at all, regardless of the configured `keepalive_interval`.
The schedule was parasitic on whichever unrelated deadline happened to fire first, and a busy connection has none.

This was hit twice while writing the issue #349 e2e coverage.
The test there worked around it by avoiding per-message ack awaits and dropping the keepalive to 100 ms so natural gaps in dispatch would arm the schedule.
The production impact was recorded as uncertain at the time; it is not.
Reverting the fix below and re-running `e2e_auto_adjust_arms_under_continuous_ack_response_traffic` against a real Pulsar 4.0.4 broker leaves the auto-tuned target pinned at its floor of 20 for the whole 300-message drain — the exact usage pattern (individually-awaited acks in a receive loop) disables PIP-74 auto-scaling outright.

Alternatives considered:

- **Make `next_adjust_deadline()` report "due now" while unarmed.** Rejected: `poll_timeout` is `&self` and takes no `now`, so there is no instant to synthesise a deadline from without reading a clock inside `magnetar-proto` — a direct [ADR-0011](0011-clock-injection-sans-io.md) violation and non-deterministic under moonpool.
- **Arm from `Connection::subscribe`.** Rejected: `subscribe` only emits `CommandSubscribe`; the consumer is not yet attached and may never be (a rejected subscribe, a cancelled waiter). Arming there would run a schedule for a consumer the broker never acknowledged, and `subscribe` has no `now` parameter either.
- **Have the engines call `handle_timeout` on a fixed internal cadence.** Rejected: that reintroduces a poll loop the sans-io design exists to avoid, burns wakeups on idle connections, and makes the tick cadence an engine property rather than sans-io state — moonpool and tokio would have to be kept in lockstep by convention instead of by construction.

## Decision

`Connection::initial_flow` becomes the adjust schedule's dedicated bootstrap, and takes the injected `now` needed to do it:

```rust
pub fn initial_flow(&mut self, handle: ConsumerHandle, now: Instant) -> Option<RequestId>
```

Inside, the flow command and the arming happen under a **single** per-slot lock acquisition, dropped before the connection-wide encode ([ADR-0038](0038-split-connection-mutex.md) ordering unchanged):

```rust
let mut consumer = self.consumers.get(&handle)?.state.lock();
let flow_cmd = consumer.initial_flow();
consumer.arm_adjust_clock(now);
```

- **`initial_flow` is the right funnel.** Every path that grants a consumer its first permits already routes through it: the engines' subscribe-ack path, the post-seek resubscribe, the `SubscribeSuccess` re-attach arm, and the #307 Failover-promotion re-arm. Arming there makes the first tick's deadline a function of the subscribe-ack instant alone.
- **Idempotent by construction.** `arm_adjust_clock` only fires while `last_adjust_at` is `None`, and is a no-op when `adjust_interval` is `None` (the default `Fixed` policy). Re-attach and promotion re-flows therefore neither restart nor skew a running schedule, and the `Fixed` path is byte-identical to before.
- **`abandon_consumer_subscribe_waiter` also takes `now`**, forwarding it to its immediate-release `initial_flow` call. It is the only other public proto function in the call graph.
- **The `handle_timeout` arm stays as a backstop**, re-commented as such. It costs nothing (the idempotency guard makes it a no-op for any armed consumer) and still covers a consumer that somehow reaches a tick without ever having been flowed.
- **Each engine supplies its own clock at the call site**, per ADR-0011: `magnetar-runtime-tokio` snapshots `std::time::Instant::now()`; `magnetar-runtime-moonpool` uses `ConnectionShared::now_instant()`, which routes through the `providers.time()`-bound closure, so a seed replay arms the schedule at the same simulated instant.

**Breaking change**: both `Connection::initial_flow` and `Connection::abandon_consumer_subscribe_waiter` gain a `now: Instant` parameter.

## Consequences

- **Fixed**: an `Auto` consumer scales under the load PIP-74 auto-scaling exists for, including the individually-awaited-ack receive loop that previously disabled it. No user-visible configuration changes; no wire-format change.
- **Timing is unchanged for already-passing scenarios.** Callers that armed via a `handle_timeout(t0)` tick now arm at the `initial_flow(…, t0)` that precedes it, so the first _adjust_ still lands on the following interval boundary. The existing proto / runtime / differential trajectories (`200, 400, 800, 1600`) are untouched.
- **Determinism**: the armed deadline is now derived from an injected instant on both engines instead of from whichever unrelated deadline happened to fire, removing a seed-dependent input to when the first adjust lands. `receiver_queue_adjust_arming_agrees_under_continuous_ack_traffic` pins tokio ↔ moonpool agreement on the whole `poll_timeout()` sequence under continuous ack traffic.
- **Harder**: two public `Connection` methods gained a parameter, so every caller — including tests — must supply a clock. That is the intended pressure: a caller that cannot name the instant it is operating at is a caller that should not be granting flow.
- **Amends [ADR-0071](0071-pluggable-receiver-queue-policy.md)**, whose "the adjust tick rides `handle_timeout(now)`" clause implied `handle_timeout` also owned the arming. The tick itself still rides `handle_timeout`; only the bootstrap moves. ADR-0071 is otherwise unchanged and remains binding.

## References

- `crates/magnetar-proto/src/conn.rs` — `initial_flow` (the bootstrap), `abandon_consumer_subscribe_waiter`, the `SubscribeSuccess` and `ActiveConsumerChange` re-flow arms, and the `handle_timeout` backstop.
- `crates/magnetar-proto/src/consumer.rs` — `arm_adjust_clock`, `next_adjust_deadline`.
- `crates/magnetar-runtime-tokio/src/client.rs`, `crates/magnetar-runtime-tokio/src/consumer.rs` — host-clock snapshots at the call sites.
- `crates/magnetar-runtime-moonpool/src/consumer.rs` — `ConnectionShared::now_instant()` snapshots at the matching call sites.
- ADR-0024 five-layer coverage: `crates/magnetar-proto/src/conn.rs` (`auto_policy_arms_adjust_schedule_at_initial_flow`, `auto_adjust_schedule_arms_under_continuous_ack_response_traffic`), `crates/magnetar-runtime-tokio/tests/receiver_queue_auto_growth.rs` + `crates/magnetar-runtime-moonpool/tests/receiver_queue_auto_growth.rs` (`auto_adjust_schedule_armed_by_initial_flow`, `auto_adjust_schedule_survives_continuous_ack_response_traffic`, 1:1), `crates/magnetar-differential/tests/receiver_queue_policy_equivalence.rs` (`receiver_queue_adjust_arming_agrees_under_continuous_ack_traffic`), `crates/magnetar/tests/e2e_receiver_queue_policy.rs` (`e2e_auto_adjust_arms_under_continuous_ack_response_traffic`).
- [ADR-0011](0011-clock-injection-sans-io.md) — the injected-clock rule the `now` parameter satisfies.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the cross-runtime test + parity policy this change satisfies.
- [ADR-0038](0038-split-connection-mutex.md) — the per-slot lock the arming runs under.
- [ADR-0058](0058-keepalive-watchdog-progress-based.md) — the `last_activity` refresh that made the old arming path starve.
- [ADR-0071](0071-pluggable-receiver-queue-policy.md) — the policy this schedule belongs to.
