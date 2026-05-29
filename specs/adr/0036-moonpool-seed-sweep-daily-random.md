# ADR-0036 — Moonpool seed sweep: daily random, not fixed per-PR

- **Status**: Accepted (amended by [ADR-0043](0043-temporary-floating-moonpool-git-dep.md), exact-pin discipline scoped exception)
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: testing, moonpool, ci, process

> **Amendment (2026-05-29, [ADR-0043](0043-temporary-floating-moonpool-git-dep.md)).**
> The exact-pin reproducibility discipline this ADR relies on is
> temporarily relaxed for **two named crates only** — `moonpool-core` and
> `moonpool-sim` now track git `branch = "main"` to consume the futures-io
> `TcpStream` + segment-granular `write_vectored` change ([ADR-0040](0040-vectored-io-transmit-enum.md)
> wave 2) ahead of a crates.io release. `Cargo.lock` still records a
> concrete rev, the daily seed sweep below is one of the mitigations, and
> the float is re-pinned to an exact `=x.y.z` once
> [PR #113](https://github.com/PierreZ/moonpool/pull/113) ships. Nothing
> else in this ADR changes.

## Context

[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) §"Decision"
#3 specifies a deterministic seed sweep over `seed ∈ 1..32` on every
validation pass, mirrored by the `moonpool-sim` matrix job in
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). That job
fans out 32 parallel runners on every push to `main` and every PR
synchronisation, each running the full `magnetar-runtime-moonpool` test
suite under one fixed `MOONPOOL_SEED`.

Two problems surfaced once the sweep had been live for a while:

1. **Fixed seeds are useless after the first green run.** A given
   `(commit, seed)` pair is bit-for-bit reproducible by ADR-0024's own
   guarantee. Once seed 7 passes for HEAD, re-running seed 7 on the
   next merge-base rebase covers the exact same scheduling. The matrix
   is a deterministic regression check on 32 specific scheduling
   trajectories — not an exploration of the seed space.
2. **PR latency cost.** 32 parallel runners × ~5–10 min of moonpool
   tests is ~150–300 runner-minutes per PR sync. With concurrency
   cancellation on rapid pushes this is largely wasted compute, and
   the merge gate waits on the slowest seed even when 31 of them
   trivially pass the way they did yesterday.

The deterministic-simulation suite's value is **exploring scheduling
trajectories** that real I/O never reaches. Fresh random seeds, rolled
each run, do that strictly better than a fixed list: over a week of
daily runs the suite covers ~112 unique seeds; the fixed-32 sweep
covers exactly the same 32 forever.

[`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) §"What is
*not* yet exercised under simulation" already notes that property-style
seed sweeps were a known gap. This ADR closes that gap by moving the
sweep out of the per-PR gate (where deterministic re-execution buys
nothing) and into a daily cron job that rolls fresh seeds each run.

## Decision

The moonpool seed sweep moves from per-PR / per-push to a dedicated
**daily** workflow with **16 random seeds in parallel**.

Concretely:

1. **Drop `moonpool-sim` from
   [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)**. The
   regular `test` job (`cargo test --workspace --all-features --locked`)
   still exercises `magnetar-runtime-moonpool` on the moonpool default
   seed on every PR / push — that remains the per-commit smoke test.

2. **Add
   [`.github/workflows/moonpool-seed-sweep.yml`](../../.github/workflows/moonpool-seed-sweep.yml)**
   running on `schedule: '17 3 * * *'` (03:17 UTC daily) and
   `workflow_dispatch`. The workflow has two jobs:
   - `generate-seeds` — rolls **16 hex-encoded random `u64` seeds**
     via Python's `secrets.randbits(64)`, emits them as a JSON array in
     `$GITHUB_OUTPUT`.
   - `moonpool-sim` — matrix of 16 parallel runners, each setting
     `MOONPOOL_SEED=<seed>` and running the
     `magnetar-runtime-moonpool` test suite. `fail-fast: false` so the
     full set of failing seeds is visible in one run summary.

3. **Failure handling.** Any seed failure leaves the matrix entry red
   and surfaces in the daily run. Diagnosis: copy the seed from the
   run summary, reproduce locally with
   `MOONPOOL_SEED=<seed> cargo test -p magnetar-runtime-moonpool …`,
   fix, and land via the normal PR flow. The fix's commit needs only
   the standard ADR-0024 four-layer test set; no special "daily-sweep
   regression" gate.

4. **Local validation chain (CLAUDE.md, GUIDELINES.md, docs/testing.md,
   docs/moonpool-engine.md)** keeps the fixed `1..32` sweep snippet —
   it is still the recommended local pre-flight check before pushing
   a moonpool-touching change. Local runs are not blocked on CI
   runner availability and benefit from the deterministic reproduce-
   bit-for-bit guarantee. Only the CI cadence changes.

5. **Amends ADR-0024 §3.** This ADR overrides the "`seed ∈ 1..32` on
   every pass" CI requirement for the CI cadence specifically. The
   four-layer test policy, 100% diff sim coverage, and 1:1 runtime
   test count from ADR-0024 §§1–2,4–8 are unchanged.

## Consequences

**Positive**

- Per-PR runner-minutes drop by ~150–300 minutes; the merge gate
  finishes faster.
- Coverage of the moonpool seed space grows over time (~5,840 distinct
  seeds/year vs. the fixed 32 forever).
- Seed-dependent regressions surface within 24 hours of landing, with
  a reproducible `MOONPOOL_SEED=<hex>` value attached.
- The daily cadence makes it visible *when* a regression landed
  (yesterday's sweep was green, today's is red).

**Negative**

- A seed-dependent regression can land on `main` and stay there for up
  to 24 hours before the next nightly run flags it. This is a
  deliberate trade against the wasted-compute cost of the per-PR fixed
  sweep — random per-PR sweeps could re-introduce the latency without
  the determinism, and "all seeds 1..32 plus 16 random" is the worst
  of both.
- Diagnosing a regression requires copying a hex seed from the run
  summary rather than picking from a known short list. Mitigated by
  the workflow's "Rolled seeds:" echo step.

**Neutral**

- The deterministic-simulation suite itself is unchanged; only the
  cadence and seed source change.
- Local validation chain still runs `seq 1 32` — developers who want
  the per-PR fixed sweep behaviour can `act`-run the old job or call
  the shell snippet directly.

## Alternatives considered

- **Keep per-PR fixed sweep, add daily random sweep on top.**
  Rejected: doubles compute, doesn't fix the "fixed seeds are useless
  after the first green run" problem.
- **Per-PR random sweep (16 random seeds rolled per PR).** Rejected:
  loses determinism — a flake under one PR's roll can't be reproduced
  on a rebase. The seed-sweep value is in the *reproducible failure*,
  which only random-but-recorded provides.
- **Weekly cadence instead of daily.** Rejected: a regression can sit
  for a week before being noticed; that's too long given how often
  the moonpool surface changes.
- **More seeds per day (32 or 64).** Rejected for now: 16 is enough
  for one day's exploration and keeps runner cost bounded. Easy to
  bump later if the failure rate suggests we're undersampling.
- **Use `RUSTFLAGS=-C panic=abort` to short-circuit failing seeds and
  roll more.** Rejected: solves a problem we don't have (runner cost
  is fine at 16); adds complexity.

## References

- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the
  amended decision; §3 (seed sweep CI cadence) is overridden here.
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) — the
  `moonpool-sim` matrix job is removed here.
- [`.github/workflows/moonpool-seed-sweep.yml`](../../.github/workflows/moonpool-seed-sweep.yml)
  — the new daily workflow.
- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) §"What is
  *not* yet exercised under simulation" — closes the property-seed-
  sweep gap noted there.
- [`docs/testing.md`](../../docs/testing.md) — local validation chain
  (unchanged; still runs `seq 1 32` locally).
- [`CLAUDE.md`](../../CLAUDE.md) §"Validation chain" — unchanged for
  local; CI mirror updated.
- [`GUIDELINES.md`](../../GUIDELINES.md) §"Cross-runtime test +
  coverage policy" / §"Seed sweep" — text updated to describe the
  new CI cadence.
