# ADR-0024 — Cross-runtime test + coverage policy

- **Status**: Accepted — amended by [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) for the CI seed-sweep cadence (§"Decision" #3 only; the four-layer test policy, sim coverage, and 1:1 parity rules in §§1–2,4–8 stand)
- **Date**: 2026-05-22
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, moonpool, tokio, differential, ci, process

## Context

The workspace ships **two** I/O engines on top of one sans-io core:

- `magnetar-runtime-tokio` — production engine.
- `magnetar-runtime-moonpool` — deterministic-simulation engine over
  `moonpool_core::Providers`.

Both engines target the same Java-client parity matrix (see
[`README.md`](../../README.md#java-client-parity-matrix)) and are
expected to be **observationally equivalent** at the user-visible
`EventStream` boundary. That equivalence is the load-bearing
hypothesis behind every moonpool-driven test:

- The chaos pack ([`docs/moonpool-engine.md`](../../docs/moonpool-engine.md))
  exercises virtual-clock-only scenarios that cannot be reproduced
  against a real broker.
- The differential harness
  (`crates/magnetar-differential/`) replays the same `Trace` against
  both engines and asserts equivalence. It is the workshop's
  bug-finding oracle.

The hypothesis stops being load-bearing the moment moonpool's coverage
of the production surface drifts. Observed risks:

1. **Coverage drift.** A `feat` lands in `magnetar-runtime-tokio` with
   a unit test; the equivalent code path in
   `magnetar-runtime-moonpool` exists but is never exercised by a
   moonpool test. The differential harness still passes because the
   `Trace` doesn't go near the new code. The simulation oracle has
   gone blind on that surface and nobody notices until a production
   incident.
2. **Test-count drift.** `magnetar-runtime-tokio` grows organically
   (54+ test files); `magnetar-runtime-moonpool` lags (smaller set).
   Reviewers can't tell at a glance whether a missing moonpool test
   is "doesn't apply" or "the contributor skipped it".
3. **Single-layer fixes.** A `magnetar-proto` bug fix lands with a
   proto unit test only. The engines never re-validate that the fix
   composes with their I/O — and the e2e suite, gated behind Docker,
   skips it too.
4. **Seed-dependent flakiness.** Moonpool's deterministic scheduler
   is seed-driven; a test that passes on `MOONPOOL_SEED=1` can fail
   on `seed=17`. Today the seed sweep is *documented* in
   `docs/testing.md` but not *enforced*, so seed-dependent regressions
   slip through.

The parity finish-line wave produced three near-misses fitting
patterns (1)-(3), each caught manually during ADR review rather than
by CI.

ADR-0019 already records that moonpool parity is a follow-up train;
ADR-0021 already records that tests are fixed, not silently ignored.
This ADR closes the gap between those two by
specifying **what coverage and what test layers a "fix" or "feature"
must touch**, and by adding two hard-failing xtask checks to enforce
the rules in the local + CI validation chain.

## Decision

Every behavioral change (runtime behavior, public API, wire format)
and every change inside `magnetar-proto` ships with **all four** test
layers in the same commit, plus a Docker e2e test:

1. **`magnetar-proto` unit test** — sans-io state-machine behavior,
   driven from `#[cfg(test)] mod tests` blocks in the proto crate.
2. **`magnetar-runtime-tokio` integration test** under
   `crates/magnetar-runtime-tokio/tests/`.
3. **`magnetar-runtime-moonpool` integration test** under
   `crates/magnetar-runtime-moonpool/tests/`.
4. **`magnetar-differential` equivalence test** asserting tokio ↔
   moonpool user-visible `EventStream` parity, under
   `crates/magnetar-differential/tests/`.
5. **Docker end-to-end test** under
   `crates/magnetar/tests/e2e_*.rs`, gated by
   `#[cfg(feature = "e2e")]` + `#[ignore = "e2e: requires Docker"]`.

Concretely, the binding rules are:

1. **Patch coverage = 100% on the moonpool runner.** Every line added
   or modified relative to `git merge-base origin/main HEAD` must be
   executed by at least one test running under
   `magnetar-runtime-moonpool` (chaos pack, integration test, or
   differential test). Enforced by `cargo xtask check-sim-coverage`,
   which wraps `cargo-llvm-cov --json -p magnetar-runtime-moonpool`,
   parses the LCOV-equivalent JSON, intersects with `git diff
   merge-base...HEAD` line ranges, and fails on any uncovered added
   line.

2. **Strict 1:1 runtime test count.** The number of `#[test]` +
   `#[tokio::test]` + `#[moonpool::test]` attributes under
   `crates/magnetar-runtime-tokio/{src,tests}` must equal the
   equivalent count under `crates/magnetar-runtime-moonpool/{src,tests}`.
   Enforced by `cargo xtask check-runtime-test-parity`.

3. **Seed sweep.** The local validation chain runs
   `MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool` for
   `seed ∈ 1..32` on every pass. Any seed failure fails the chain. In
   CI, the cadence is **daily, 128 random seeds in parallel**, per
   [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — fixed seeds
   in the per-PR gate were wasted compute because each `(commit, seed)`
   pair is bit-for-bit reproducible.

4. **Coverage tool.** `cargo-llvm-cov` (LLVM source-based coverage),
   not `tarpaulin`. Reason: tracks features cleanly, works with
   stable toolchain, has stable JSON output for diff intersection.
   The xtask helper installs/uses `cargo-llvm-cov`; if missing, it
   exits with the install command.

5. **Scope.** All four rules apply to:
   - Behavioral changes (alter runtime behavior, public API, wire
     format).
   - Any change in `magnetar-proto/`.

   They do NOT apply to:
   - Docs-only / comment-only / formatter-only changes.
   - Dependency bumps with no functional impact (author asserts +
     reviewer confirms).
   - Tooling changes that don't affect product surface
     (`xtask/`, `.github/`, `docs/`).

6. **Enforcement surface.** Both new xtask checks are enabled and
   hard-failing **from the moment this ADR lands**. Closing existing
   coverage gaps and bringing tokio↔moonpool test counts into 1:1
   alignment is tracked in
   [`docs/follow-ups.md`](../../docs/follow-ups.md); the executable plan lives in
   the local (gitignored) `tasks/coverage-closure-prompt.md`. Until
   those gaps close, merges to `main` will fail the validation chain
   by design — no exceptions.

7. **CI mirror.** GitHub Actions runs the same xtask commands on
   every PR. The coverage report uploads (HTML + JSON) to the CI
   artifact store for inspection. Patch-coverage failure blocks the
   merge. The seed sweep is the exception: per
   [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) it runs daily
   with 16 freshly-rolled random seeds in
   [`.github/workflows/moonpool-seed-sweep.yml`](../../.github/workflows/moonpool-seed-sweep.yml),
   not on every PR.

8. **ADR-0021 still applies.** A failing test under the new rules is
   *not* a license to `#[ignore]` — the escape hatch is "stop and
   surface to the user", same as before.

## Consequences

**Positive**

- Moonpool stays a credible equivalence oracle for tokio: every
  production-surface line under tokio has at least one moonpool test
  exercising it.
- The differential harness gains teeth: it can't pass while the
  underlying surface is silently uncovered.
- Seed-dependent flakiness surfaces at commit time, not in
  production.
- Reviewers have one mechanical check (`xtask check-runtime-test-parity`)
  for "did you remember the moonpool side?"
- Patch-coverage style avoids the "boil the ocean" trap of demanding
  a full-workspace baseline before the rule takes effect.

**Negative**

- Coverage gaps in the current `magnetar-runtime-moonpool` surface
  block merges until closed. The
  [`tasks/coverage-closure-prompt.md`](../../tasks/coverage-closure-prompt.md)
  prompt is sized for one focused session.
- Every behavioral PR now adds 4-5 test files instead of 1-2.
  Throughput drops in exchange for invariant guarantees.
- `cargo-llvm-cov` adds ~30-60s to the local validation chain. The
  seed sweep adds ~5-10 min depending on hardware. CI absorbs both.
- Cross-engine bug fixes get more verbose: a one-line proto patch
  pulls in proto-unit + tokio-integration + moonpool-integration +
  differential + e2e additions.

**Neutral**

- The existing five test categories described in
  [`docs/testing.md`](../../docs/testing.md) map 1:1 onto the four
  layers + e2e structure here. No category is added or removed; the
  ADR formalises which layers a *change* must touch.
- ADR-0019 (engine scope + moonpool parity follow-up) remains in
  force; ADR-0024 specifies the test policy that ratchets parity
  forward.

## Alternatives considered

- **Differential test only.** Cheaper per-commit but doesn't protect
  against the "Trace doesn't exercise the new code" failure mode.
  Rejected: differential equivalence is necessary but not sufficient.
- **Engine-parity tests without the differential layer.** Cheaper per
  commit but loses the cross-engine equivalence assertion. Rejected:
  parallel tests with no equivalence assertion can drift in lockstep
  and miss real divergences.
- **Coverage % threshold (e.g. 90% on diff) instead of strict 100%.**
  Rejected: leaves room for "the happy path is covered, error paths
  aren't" — and error paths are exactly where moonpool's
  fault-injection adds value.
- **Test-count parity within a tolerance (±N).** Rejected:
  tolerance lets organic drift accumulate; strict equality forces a
  conversation every time the runtimes diverge.
- **Forward-only patch coverage, grandfather existing gaps.**
  Rejected by Florentin in favour of strict retroactive — gap closure
  to be tackled in a focused follow-up session before more behavioral
  work lands. Tracked in
  [`docs/follow-ups.md`](../../docs/follow-ups.md).
- **Tarpaulin instead of `cargo-llvm-cov`.** Rejected: tarpaulin is
  Linux-only and its JSON output is less stable across versions.
- **CI-only enforcement (no local xtask).** Rejected: pre-commit
  hooks already enforce other invariants locally; consistency wins.

## References

- [ADR-0004 — sans-io `magnetar-proto` + swappable I/O engines](0004-sans-io-protocol-core.md)
- [ADR-0006 — moonpool engine drives `rustls::ClientConnection`](0006-moonpool-tls-byte-pipe.md)
- [ADR-0010 — full Java parity](0010-v0-1-full-java-parity.md)
- [ADR-0019 — engine scope and moonpool parity follow-up](0019-engine-scope-and-moonpool-parity.md)
- [ADR-0021 — tests are fixed, not silently ignored or removed](0021-no-silent-test-ignore-or-remove.md)
- [`docs/testing.md`](../../docs/testing.md) — five test categories.
- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) — chaos
  pack + differential harness.
- [`docs/parity-status.md`](../../docs/parity-status.md) — per-engine
  parity snapshot.
- [`xtask/src/main.rs`](../../xtask/src/main.rs) — `check-sim-coverage`
  and `check-runtime-test-parity` subcommands.
- [`docs/follow-ups.md`](../../docs/follow-ups.md) — the tracked
  follow-up; the executable prompt lives locally in
  `tasks/coverage-closure-prompt.md` (gitignored).
- [`CLAUDE.md`](../../CLAUDE.md) §"Non-negotiable invariants" #9 —
  references this ADR.
- [`GUIDELINES.md`](../../GUIDELINES.md) §"Cross-runtime test +
  coverage policy" — binding spec.
