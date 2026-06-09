# ADR-0061 — Supervisor give-up budget counts post-dial handshake failures

- **Status**: Accepted
- **Date**: 2026-06-09
- **Decider**: Florentin Dubois
- **Tags**: reconnect, resilience, runtime, supervisor, sans-io

## Context

The supervised driver loop (`magnetar-runtime-{tokio,moonpool}/src/driver.rs`) bounds its reconnect storm two ways: an exponential `Backoff` schedule sets the cadence, and the `SupervisorConfig::max_attempts` budget decides when to STOP and surface the last error to the caller (`None` — the default — means infinite, matching Java's bounded-but-unlimited reconnect).

Before this ADR the give-up budget was wired wrong.
The `attempt` counter was declared at the TOP of each OUTER supervisor iteration, so it reset to `0` on every cycle, and `max_attempts` was checked ONLY inside the INNER TCP-dial loop.
`begin_handshake` (the post-dial CONNECT) and the post-handshake `driver_loop_inner` ran OUTSIDE that loop, after `break t`.
So when a reconnect dialed successfully but the Pulsar handshake then failed, `driver_loop_inner` returned up to the outer supervisor loop, which re-entered and reset `attempt` back to `0`.

Behind a docker-proxy / Apache Pulsar Proxy / load balancer that accepts the TCP connection while its backend is down — the EXACT storm class the anti-thrash supervision ([ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md)) was built for — the dial always succeeds and the handshake never completes.
The budget therefore NEVER fired: a caller who set a finite `max_attempts` expecting the supervisor to give up was instead retried forever.

The backoff-reset side was already correct: `SupervisorConfig::should_reset_backoff(socket_alive)` ([ADR-0011](0011-clock-injection-sans-io.md)-clean — the duration is the injected-clock-measured socket lifetime, `socket_alive > drop_grace`) gates `Backoff::reset` on the previous socket surviving past `drop_grace`.
The give-up budget was simply not aligned with that same stability definition.

## Decision

Hoist the give-up counter so it spans the FULL dial+handshake cycle, and reset it on the same stability gate the backoff schedule already uses.

### 1. A sans-io give-up policy helper

`SupervisorConfig::should_give_up(attempts: u32) -> bool` (`magnetar-proto/src/supervisor.rs`) is the single shared definition both engines call:

```rust
self.max_attempts.is_some_and(|max| attempts > max)
```

It is a pure policy gate — no state, no I/O — so `magnetar-proto` stays zero-I/O ([ADR-0004](0004-sans-io-protocol-core.md), `check-no-io-deps`).
`max_attempts == None` (the default) never gives up.
The strict `>` mirrors the drivers' `if attempt > max` guard: `attempt == max` keeps trying, `attempt > max` gives up.

### 2. Hoist the counter in both engine drivers

`give_up_attempts: u32` is declared OUTSIDE the outer supervisor loop (next to the persisted `Backoff`), so a post-dial handshake failure (the `driver_loop_inner` return path after `begin_handshake`) counts against the SAME budget as a TCP-dial failure instead of letting the outer loop reset it.
Each pass through the inner dial loop increments the counter and checks `should_give_up`; a pass that dials successfully but whose post-handshake `driver_loop_inner` later returns re-enters the outer loop WITHOUT resetting the counter, so the next dial increments from where the previous left off.

### 3. Reset on the shared stability gate

The counter resets to `0` ONLY when `should_reset_backoff(socket_alive)` is true — the SAME predicate, computed at the same place, that already gates `Backoff::reset`.
Backoff-reset and give-up-reset now share ONE definition of "the last reconnect counted as stable": a connection that survived `drop_grace`.
A socket that merely accepted TCP and handshaked but died inside `drop_grace`, or never handshaked at all behind a TCP-accepting proxy, resets NEITHER.

### 4. Count every post-dial failure uniformly

The rule counts EVERY post-dial failure the same way — a broker that actively rejected the handshake and a connection that was dropped after TCP-accept both cost one cycle.
This is the simplest rule, it matches Java's bounded reconnect, and it does not require the driver to introspect WHY the handshake failed.

### 5. Relabel the reconnect logs

The pre-handshake `info!` that fired on a successful `Transport::connect` was labelled `"supervisor: reconnected to broker"` — but a TCP accept behind a down backend is NOT a reconnect.
It is relabelled `"supervisor: TCP connected; handshaking"` (keeping its `attempt` / `host` / `port` structured fields, [ADR-0054](0054-logging-policy.md) `check-log-fields`).
The TRUE reconnect-success `info!` now fires AFTER the handshake actually completes — at the once-per-reconnect `pending_rebuild` compare-exchange where `is_connected()` is confirmed — so operators keep a reconnect-confirmed signal and a TCP accept is never mislabelled as a reconnect.
That site fires even with zero handles to replay (`producers = 0, consumers = 0`), so a handshake that never completes never reaches it.

## Consequences

- **Breaking change for callers who set a finite `max_attempts`.** Behind a TCP-accepting proxy whose backend is down, the supervisor now gives up at the budget instead of retrying forever. Callers relying on the old (accidental) infinite-retry-behind-a-proxy behaviour must raise `max_attempts` or leave it `None`. The default is `None` (infinite), so most callers are unaffected.
- A finite-budget supervised connection behind a TCP-accept storm now terminates: it exhausts `max_attempts`, latches `no_driver` ([ADR-0059](0056-terminal-fast-fail-new-ops.md)), runs `fail_all_pending`, and new ops fast-fail with `ClientError::PeerClosed` instead of hanging.
- Backoff-reset and give-up-reset share one stability predicate, so a connection that genuinely stabilizes (survives `drop_grace`) clears BOTH the backoff schedule and the give-up budget — a later storm starts the budget from scratch.
- `magnetar-proto` stays zero-I/O: the only proto-side addition is the stateless `should_give_up` policy method.
- This **amends** the supervision model of [ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) (anti-thrash) and [ADR-0052](0052-initial-connect-timeout-retry.md) (connect-timeout-retry): the per-attempt `connect_timeout` of ADR-0052 still bounds each dial, and the anti-thrash cooldown of ADR-0028 still bounds cadence; ADR-0061 only changes WHICH failures the give-up budget counts. Neither prior ADR is superseded.
- ADR-0024 layers ship in the same commit: a proto unit (`should_give_up` fires at the budget behind a TCP-accept; a stable socket resets the counter; default `None` never gives up; strict-`>` boundary), tokio + moonpool integration twins driving the hoisted-counter decision sequence (kept tokio↔moonpool 1:1), a `magnetar-differential` give-up-sequence-equivalence test, and an e2e (`e2e_reconnect.rs` extended with a localhost handshake-failing stub-acceptor — a real broker always completes the handshake, so the stub is the only way to exercise the budget end-to-end; no `#[ignore]` / no feature gate per [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)).

## References

- [ADR-0004](0004-sans-io-protocol-core.md) — `magnetar-proto` zero-I/O; `should_give_up` is a stateless policy method.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection; the `socket_alive` duration feeding both reset gates is the injected-clock-measured socket lifetime, not a host read.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime four-layer test + 1:1 parity policy.
- [ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) — the anti-thrash supervision this aligns the give-up budget with (amended, not superseded).
- [ADR-0052](0052-initial-connect-timeout-retry.md) — the per-attempt `connect_timeout` that still bounds each dial (amended, not superseded).
- [ADR-0054](0054-logging-policy.md) — the structured-field policy the relabelled + new reconnect logs satisfy.
- [ADR-0059](0056-terminal-fast-fail-new-ops.md) — the `no_driver` latch + `fail_all_pending` the give-up path triggers, so exhausted-budget ops fast-fail instead of hanging.
