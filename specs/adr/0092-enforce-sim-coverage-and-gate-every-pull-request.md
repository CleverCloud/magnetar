# ADR-0092 — Enforce uncovered added lines in `check-sim-coverage`, and gate every pull request on it

- **Status**: Accepted
- **Date**: 2026-08-01
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, xtask, moonpool, ci, adr-0024

## Context

[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) requires 100% moonpool patch coverage on every diff.
[ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) made the gate stop passing silently on what it could not see, and [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) widened its report from 16 `SF:` records to 63 so that `magnetar-proto` and `magnetar-runtime-tokio` are measured at all.

Neither made the requirement enforceable, and ADR-0090 said so in as many words.
Two independent things were missing, and each made the other pointless.

**The verdict was advisory.**
`const SIM_COVERAGE_ENFORCES_UNCOVERED: bool = false` in `xtask/src/main.rs` printed uncovered added lines with a count and exited 0.
A green `check-sim-coverage` proved the gate had run and printed its findings, not that the patch was covered — the same shape of over-claim ADR-0088 was written to stop, accepted by ADR-0090 temporarily and with eyes open.

**The gate had never measured a pull request.**
This is the load-bearing half, and it was measured on 2026-07-31.
`.github/workflows/xtask-gates.yml` ran `check-sim-coverage` on a daily cron and on `workflow_dispatch` only; `.github/workflows/ci.yml` carried no sim-coverage job at all.
It is a _patch_ gate against `git merge-base origin/main HEAD`, and both scheduled runs target `main`, where `merge-base(origin/main, HEAD) == HEAD` makes the diff empty and the check short-circuits with "nothing to verify" before it builds anything.
The proof is `magnetar-runtime-moonpool` itself: gated since ADR-0024, inside the report the whole time, and still carrying 43 uncovered added lines over `HEAD~25`.

So flipping the constant alone would have changed nothing in practice, and a per-PR job alone would have gated nothing.
That is why this ADR does both in one changeset.

### What the backlog actually costs

Replaying real history through the widened gate on 2026-07-31:

| Diff base                                   | Uncovered added lines | Files                                                                                                                                                                                    |
| ------------------------------------------- | --------------------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HEAD~10` — roughly the last ten merged PRs |                 **6** | `magnetar-proto/src/conn.rs` (284, 1528), `magnetar-proto/src/health_probe.rs` (204), `magnetar-runtime-tokio/src/client.rs` (2036), `magnetar-runtime-tokio/src/consumer.rs` (228, 231) |
| `HEAD~25` — back to 2026-07-01              |                   450 | dominated by `magnetar-runtime-tokio/src/client.rs` (191), `.../consumer.rs` (61), `magnetar-proto/src/conn.rs` (142)                                                                    |

**6 is the number that governs, and 450 is not a debt anyone inherits.**
Each PR is measured only on its own added lines against its own merge-base, so the 450 is an artifact of diffing against a 25-commit-old base that no ordinary workflow produces.
Nothing has to be burned down before this flip, which is what makes the flip affordable now rather than after a coverage sprint.

Note what "cover it" means here, because it is narrower than it looks: execution scope is still `-p magnetar-runtime-moonpool -p magnetar-differential`, so a `magnetar-proto` or `magnetar-runtime-tokio` line counts only when a moonpool or differential test reaches it.
Those crates' own unit tests never run under this gate and can never satisfy it.

### Cost of the per-PR job

Measured 2026-08-01 on a `workflow_dispatch` run against a scratch branch whose diff adds one uncovered line to `crates/magnetar-proto/src/frame.rs` — the case the scheduled run can never exercise ([run 30708811273](https://github.com/CleverCloud/magnetar/actions/runs/30708811273)):

| Case                                                           | Wall clock                                                |
| -------------------------------------------------------------- | --------------------------------------------------------- |
| Diff touching a gated crate, `Swatinem/rust-cache` **hit**     | **11.3 min** (16:44:50Z → 16:56:09Z), concluded `failure` |
| This changeset's own PR run — short-circuit, no build          | **2m1s**                                                  |
| Local warm run on a 16-core workstation, NFS-backed target dir | ~5 min                                                    |

**Cold cache is unmeasured.** The dispatched run logged `Cache hit for: v0-rust-sim-coverage-Linux-x64-…` / `Cache restored successfully`, so 11.3 minutes is a warm number and must not be quoted as a cold one. `timeout-minutes: 180` leaves room for a large multiple of it, which is the point of choosing 180 over the scheduled copy's 90 rather than a number derived from this measurement.

That run is also the CI-side half of the red-then-green proof, and it exercises the path the `ci.yml` job does not: `xtask-gates.yml` passes **no** `--enforce`, so its `failure` conclusion on `uncovered (moonpool runner): crates/magnetar-proto/src/frame.rs: 1 line(s)` is the flipped **constant** enforcing in CI, not the flag.

Two properties keep this affordable:

- The check is a diff gate and bails with "nothing to verify" before compiling anything when **every** added production `.rs` line is excluded: `xtask/`, `.github/`, `docs/`, `specs/`, `tasks/`, `.claude/`, `crates/magnetar-proto/src/pb/`, every `/tests/`, `/benches/`, `/examples/` path, and everything below a file's first `#[cfg(test)]` (`SIM_COVERAGE_EXCLUDE_PREFIXES`, `SIM_COVERAGE_EXCLUDE_FRAGMENTS`).
  This very changeset is one of them, and its own `check-sim-coverage` run finished in **2m1s**.
- Widening the report cost no extra compilation (ADR-0090), so the job's cost is one instrumented `--all-features` build of the sim closure and the sim run — the same order as the existing `test` and `crypto-matrix` jobs, which both budget 180 minutes.

**Be precise about what the bail is _not_ keyed on**, because the obvious reading is wrong: it tests the exclusion lists, not `SIM_COVERAGE_GATED_CRATE_PREFIXES`.
A PR touching only a crate the sim run never compiles — the `magnetar` façade, `magnetar-admin`, `magnetarctl`, `magnetar-auth-oauth2`, `magnetar-messagecrypto`, `magnetar-fakes` — does **not** short-circuit.
It pays the full instrumented build and then prints those files as advisory `not gated`, a verdict derivable from the path alone.

Measured over the last 40 merged pull requests: **30%** short-circuit, **60%** build and can produce a real verdict, **10%** build for a verdict that is impossible (#374, #306, #293, #292 — façade, `magnetar-admin` and `magnetarctl` only).

That 10% is a deliberate accepted cost, not an oversight.
Bailing early whenever nothing gated was touched would be provably verdict-preserving — with no gated file in the diff, `report_missing_gated` cannot fire and `intersect_diff_with_coverage` cannot produce an uncovered line — but it would also suppress the `not gated` report, and printing that report instead of passing silently is the entire subject of [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md).
Trading ADR-0088's guarantee for one run in ten is a bad trade; revisit only if that ratio moves.

## Decision

Enforce the uncovered-line verdict, and give the gate a per-PR home. Both in this changeset.

- **`SIM_COVERAGE_ENFORCES_UNCOVERED = true`** in `xtask/src/main.rs`.
  An added line inside the reported scope that the sim run never executed fails `check-sim-coverage`.
- **`.github/workflows/ci.yml` gains a `check-sim-coverage` job on `pull_request`**, copied step for step from the `sim-coverage` job in `xtask-gates.yml`: `fetch-depth: 0` plus the explicit `git fetch origin main` so the merge base resolves, the free-disk-space step, `clang llvm libclang-dev libkrb5-dev`, `llvm-tools-preview`, and `cargo-llvm-cov`.
  `timeout-minutes: 180`, matching the comparable `--all-features` jobs rather than the 90 the scheduled copy budgets — that 90 only ever bound a dispatched run, whereas a cold-cache cancel here is a false red on a contributor's PR.
  It runs on every pull request; on what makes it actually _block_ one, see § Required check below.
- **The job passes `--enforce` explicitly.**
  It is redundant against the flipped constant — `enforcing = enforce || SIM_COVERAGE_ENFORCES_UNCOVERED` — and is passed so the workflow states its own intent and keeps gating if the constant is ever flipped back.
- **`--enforce` is retained rather than removed.**
  Existing invocations keep working, and it stays the one explicit way to ask for the verdict should the constant be reverted.
- **Two tripwires, guarding two different regressions.** Both are needed, and it is worth being exact about which catches what, because the obvious assumption is wrong.

  1. _Flipping the constant back._ `sim_coverage_enforces_uncovered_by_default` wraps `assert!(SIM_COVERAGE_ENFORCES_UNCOVERED)` in a `const` block, so a revert fails to **compile** rather than waiting for the test to be run. Verified by doing it: `error[E0080]: evaluation panicked: ADR-0092 enforces uncovered added lines; …`. The `const` form is also what `clippy::assertions_on_constants` demands, and the lint is right that it is the stronger one.
  2. _Cutting the call site._ Rewriting `check_sim_coverage` to `let enforcing = enforce;` leaves the constant untouched, so tripwire 1 stays green — **and so does the whole test**, verified 2026-08-01. What catches it is `dead_code` under `-D warnings`: both `SIM_COVERAGE_ENFORCES_UNCOVERED` and `sim_coverage_enforcing` become unreachable from production code, and `cargo clippy -p xtask -- -D warnings` fails with `constant … is never used` / `function … is never used`. Hence the extraction of `sim_coverage_enforcing` as a named `const fn` with exactly one production call site: it gives the concept a name, gives the test something to assert about its semantics, and keeps the `dead_code` tripwire armed.

  Neither is ceremony. Because the `ci.yml` job passes `--enforce`, CI would stay green straight through a silent revert of this ADR while every _other_ caller — the local validation chain, the scheduled `xtask-gates.yml` job — quietly stopped failing. That is precisely the fail-open shape ADR-0088 exists to prevent.

- **The scheduled copy in `xtask-gates.yml` stays**, for dispatching against a branch with no PR open and as a daily proof that the gate still builds.

### Required check

**A workflow job going red does not block a merge on its own.** That takes a branch-protection rule naming it a required status check, and branch protection lives in repository settings, not in this tree — `.github/` holds only `dependabot.yml` and the four workflows.

Measured 2026-08-01: `GET /repos/CleverCloud/magnetar/branches/main/protection` returns `404 Branch not protected`. So `main` today has no required checks at all, and neither this job nor any existing one gates a merge.

This is the same failure this ADR exists to fix, one level up: ADR-0090 flipped nothing because the gate had no per-PR home, and a per-PR home blocks nothing without a required-check rule. Naming it here so the gap is recorded rather than assumed away.

**Manual step, outside this changeset**, to be applied by someone with repository admin: add `check-sim-coverage (patch coverage, sim-instrumented crates)` to `main`'s required status checks. Until that is done, the job is an enforcing _signal_ on every pull request — its verdict is real and its exit code is real — and merging past a red one takes only a human choosing to.

What does **not** change:

- Execution scope is still `-p magnetar-runtime-moonpool -p magnetar-differential`. This ADR changes the exit code, not what runs.
- Report scope is ADR-0090's six crates, unchanged.
- The `magnetar` façade stays outside both sets; façade additions still print `not gated` and still cannot fail the check, `--enforce` included. The ungated report is a scope limit, not a verdict.
- The record-less-gated-crate case still hard-fails, as it did while the verdict was advisory: a gate that cannot measure must never report success.

## Consequences

**Easier.** A green `check-sim-coverage` finally means what ADR-0024 always said it should, for lines inside the reported scope. The gate now measures real pull requests instead of diffing `main` against itself, which is the only way it can catch anything at all.

**Harder.** Every PR that adds a line under one of the six gated crates must have a moonpool or differential test reaching it, in that same PR. A `magnetar-proto` unit test does not discharge this. That is the intended cost, and the per-PR shape of it is 6 lines' worth over the last ten merged PRs — not the 450 the `HEAD~25` replay shows.

**Cost.** One instrumented `--all-features` build (`aws-lc-fips-sys` included) on any PR that touches a gated crate. PRs that do not touch one short-circuit before building.

**Incompatible with macOS.** `check-sim-coverage` is Linux-only in practice: the gate pins `CC`/`CXX`/`ASM` to clang plus `AR`/`RANLIB` to the LLVM binutils (`force_clang_toolchain`), because `aws-lc-fips-sys`'s `delocate` pass rejects the `.data.rel.ro.local` sections gcc emits — at any gcc version, reproduced on gcc 14.4.0, not the "gcc 16+" threshold once assumed. This mattered less while the gate was a local courtesy; it is worth stating plainly now that it sits on every pull request.

**Reversal.** Flipping the constant back is one line, and the advisory arm of `report_uncovered` is kept working and tested so that it stays one line. It also stops the `xtask` **test** build compiling until `sim_coverage_enforces_uncovered_by_default`'s `const` assertion is deleted — the assertion sits in a `#[cfg(test)]` module, so `cargo build --workspace` still succeeds while `cargo test --workspace --all-features` and `cargo clippy --workspace --all-targets` (both in `ci.yml`) fail — and that assertion's message names the ADR that would have to supersede this one — the compile error is the tripwire that forces the second step to be deliberate.

## References

- `xtask/src/main.rs` — `SIM_COVERAGE_ENFORCES_UNCOVERED`, `check_sim_coverage` (`enforcing = enforce || …`), `report_uncovered`, `force_clang_toolchain`, `SIM_COVERAGE_EXCLUDE_PREFIXES`, `SIM_COVERAGE_GATED_CRATE_PREFIXES`; `sim_coverage_enforces_uncovered_by_default` pins the flip.
- `.github/workflows/ci.yml` — the per-PR `check-sim-coverage` job this ADR adds.
- `.github/workflows/xtask-gates.yml` — the scheduled / dispatchable copy, and its header note on why the `main` run short-circuits.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the policy this finally enforces.
- [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) — supersedes its advisory landing only; its report widening, gated-crate list and clang pinning remain binding.
- [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) — the fail-open analysis and the `not gated` reporting, both still binding.
- `CLAUDE.md` invariant #9, `GUIDELINES.md` § Sim coverage, `CONTRIBUTING.md` — updated in this changeset.
- GitHub issue [#386](https://github.com/CleverCloud/magnetar/issues/386) — the residual this closes.
