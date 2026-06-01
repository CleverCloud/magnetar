# ADR-0050 — Add a swizzle-clog broker workload to the moonpool chaos pack

- **Status**: Accepted
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: testing, moonpool, chaos, fdb

## Context

The moonpool chaos pack at [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) exercises mid-handshake partitions, OAuth refresh edge cases, PIP-121 oscillation, PIP-188 migrate-then-migrate, anti-thrash cooldown, and a stateful producer/consumer loop.
What it does **not** exercise yet is the FoundationDB simulator's **swizzle-clogging** fault — pick a random subset of N peers, stop their traffic, then restart them **in a different random order** to expose reconnection-ordering and back-pressure-resume bugs that pure crash/restart misses.

The pattern is described in:

- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md#foundationdb-simulator-the-reference-implementation) — "FoundationDB simulator → Fault injection → Swizzle-clogging" — the research note this plan operationalises.
- Simulation-deepening rollout (removed after landing) §P3 — the sequenced implementation plan that put this ADR in flight; consolidated into ADRs 0047–0050.

The chaos broker can drop one connection today (see `DropsTcpAfterCreate` at [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs:1385`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)), but stopping N random _consumers'_ permit issuance and then restoring them in a different order is not yet expressible.
That's exactly the bug shape FDB calls out: a consumer that lost permits during a partition and gets resumed last (after every other peer has already drained their queue) is a different code path from a consumer that resumed first — and both are different from the no-partition path.

## Decision

Add a new `SwizzleClogBrokerWorkload` to [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) alongside the existing `StatefulBrokerWorkload` / `DropsTcpAfterCreate` / `ProxyThroughBroker` workloads.
Specifically:

- Workload owns a `SwizzleSpec { n_clogged: usize, clog_duration_ms: u64, restore_order: Vec<u64> }` plus a `SwizzleClogPhase { Clogging, Restoring }` cursor.
- A controller task spawned from `Workload::run()` consults `ctx.providers().random()` (the seed-driven `RandomProvider`, **not** `rand::thread_rng()`) to pick the clogged consumer ids and derive `restore_order` as a different permutation of the same set.
- The clog is enforced inside `push_pending_messages_excluding_clogged` — the broker still accepts SEND from producers and updates its ledger during the clog window, but skips pushing to consumers whose ids live in the `clogged_set`.
  So producers can keep producing; consumers in the affected set are queued, not dropped.
- After `clog_duration_ms` virtual ms, the controller drains the clogged set in `restore_order`, releasing one id at a time.
- A companion `SwizzleClogClientWorkload` opens N consumers + a producer, drives M sends, and asserts:
  - No duplicate messages on consumers that were never in the clogged set.
  - Every clogged consumer eventually receives a message or surfaces `SessionLost` by the end of the sim budget.
  - Monotonic message-id holds across the swizzle window (reuses `MonotonicMsgIdInvariant`).
- A new 16-seed sweep test `sim_chaos_swizzle_clog_sweep_16_seeds` ensures the workload runs cleanly across a representative seed range.

The chaos-pack exemption in [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) applies: this is a pure simulation extension with no wire-format change, no tokio-side counterpart, and no `magnetar-proto` modification.
The exemption is justified inline in the landing commit per ADR-0024's "Exemptions" clause.

## Consequences

**Positive**

- The chaos pack now exercises FDB's swizzle pattern, closing one of the five gaps documented in [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md#status-pattern-adoption-in-magnetar).
- The new test feeds the 16-random-seed daily sweep ([ADR-0036](0036-moonpool-seed-sweep-daily-random.md)) and benefits from any future bump in seed coverage (per the simulation-deepening plan §P4, the daily sweep is queued to move from 16 → 128 seeds).
- The shared-state design (the broker's `clogged_set` is an `Arc<Mutex<HashSet<u64>>>` consulted by per-session push code) makes future variants — randomised clog/restore delays, partial-clog where permits drip rather than stop, per-broker swizzles — drop-in extensions without re-architecting.

**Negative**

- One more workload struct + spawned controller task in the chaos binary; the file gains ~250 lines.
- A swizzle iteration takes 2-3× the wall time of a vanilla produce / consume iteration because the controller burns virtual time to hold the clog.
  The 16-seed sweep is still well under the per-test budget documented in [ADR-0036](0036-moonpool-seed-sweep-daily-random.md), but the daily CI runner sees a small, measurable bump.

**Neutral**

- No change to public API surface.
  No change to `magnetar-proto`.
  No new feature flag.

## References

- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md#foundationdb-simulator-the-reference-implementation) — FDB simulator's swizzle-clogging definition.
- Simulation-deepening rollout §P3 — the sequenced implementation plan, consolidated into ADRs 0047–0050.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the chaos-pack carve-out from cross-runtime parity.
- [ADR-0026 §D2](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md#d2--wire-moonpool-sim-option-1-pure-sim-chaos-suite--converged) — pure-sim chaos suite is moonpool-specific by design.
- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — daily seed sweep cadence.
- [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) — landing site for the new workload + test.
