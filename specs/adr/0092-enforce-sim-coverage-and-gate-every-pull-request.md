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

**The per-PR number governs, and 450 is not a debt anyone inherits.**
Each PR is measured only on its own added lines against its own merge-base, so the 450 is an artifact of diffing against a 25-commit-old base that no ordinary workflow produces.
Nothing has to be burned down before this flip, which is what makes the flip affordable now rather than after a coverage sprint.

**Both rows above were measured through a filter that was hiding most of the diff**, so treat them as historical. See § The `#[cfg(test)]` hole — the same window remeasured after closing it gives **3 uncovered added lines across 2 files**.

Note what "cover it" means here, because it is narrower than it looks: execution scope is still `-p magnetar-runtime-moonpool -p magnetar-differential`, so a `magnetar-proto` or `magnetar-runtime-tokio` line counts only when a moonpool or differential test reaches it.
Those crates' own unit tests never run under this gate and can never satisfy it.

### The `#[cfg(test)]` hole

Review of this changeset surfaced a defect that would have made the flip largely theatre, so it is fixed here rather than deferred.

`check_sim_coverage` stripped inline unit tests by finding the file's **first** `#[cfg(test)]` line and dropping everything from there to EOF. The doc comment justified it: every file "puts its unit tests inside `#[cfg(test)] mod tests { … }` at the bottom of the file, so the first occurrence is a reliable upper bound on the production region."

Measured 2026-08-01, that premise is false. The first `#[cfg(test)]` is very often a gated `use` or helper near the top:

| File                                         | Cut at | On                                    | Production lines exempted |
| -------------------------------------------- | -----: | ------------------------------------- | ------------------------: |
| `magnetar-runtime-tokio/src/driver.rs`       |     48 | `#[cfg(test)] use std::io::IoSlice;`  |          **2781 of 2828** |
| `magnetar-runtime-moonpool/src/consumer.rs`  |   1997 | `#[cfg(test)]` helper                 |              2813 of 4809 |
| `magnetar-proto/src/consumer.rs`             |   1240 | `#[cfg(test)] fn pending_chunk_count` |              2408 of 3647 |
| `magnetar-runtime-moonpool/src/transport.rs` |     55 | `#[cfg(test)] use std::io::IoSlice;`  |              1317 of 1371 |

Across all gated crates that exempted **48%** of lines (37,317 of 77,329), and **71%** of the gated lines actually added over the preceding ten merged pull requests (1466 of 2056). A gate silently exempting most of its own surface is precisely the fail-open shape [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) exists to prevent, and enforcing it in that state would have claimed a guarantee it did not deliver.

**Fix: use the scanner that already existed.** `cfg_test_line_flags` — the shared, brace-depth-tracking span scanner `check-log-fields` and `check-no-internal-clock` already use — replaces the ad-hoc first-line cut, wrapped as `sim_coverage_cfg_test_lines` so the gate's stripping is a named, directly testable step. This gate had reinvented a worse scanner alongside a correct one in the same file; the fix is a convergence, not a new mechanism.

**Cost of closing it: zero, measured.** Over `origin/main~10..HEAD`, against the same freshly-generated LCOV, the old first-line cut and the new span-aware filter both report the **same 3 uncovered added lines across 2 files** (`magnetar-proto/src/conn.rs:284`, `magnetar-runtime-tokio/src/consumer.rs:228,231`). The 1466 lines the old filter had been hiding in that window were already covered by the sim or carried no executable `DA:` record. So the guarantee is restored without creating a backlog — a property of this window, not a promise about every future one, which is the point: from here on such a line is caught instead of exempted.

### Cost of the per-PR job

Measured 2026-08-01 on a `workflow_dispatch` run against a scratch branch whose diff adds one uncovered line to `crates/magnetar-proto/src/frame.rs` — the case the scheduled run can never exercise ([run 30708811273](https://github.com/CleverCloud/magnetar/actions/runs/30708811273)):

| Case                                                           | Wall clock                                                |
| -------------------------------------------------------------- | --------------------------------------------------------- |
| Diff touching a gated crate, `Swatinem/rust-cache` **hit**     | **11.3 min** (16:44:50Z → 16:56:09Z), concluded `failure` |
| Short-circuit, no build — this changeset's own PR runs         | **2m0s – 7m21s** across three runs                        |
| Local warm run on a 16-core workstation, NFS-backed target dir | ~5 min                                                    |

The short-circuit row is a range, not a number, and the spread is worth understanding before treating this job as cheap: it is **fixed setup overhead**, not the check.
Every run pays `actions/checkout` at `fetch-depth: 0`, the free-disk-space reclaim, an apt install of four packages, a `cargo-llvm-cov` download and a cache restore before `check-sim-coverage` is invoked at all — after which it prints "nothing to verify" in seconds.
So the floor this job puts on **every** pull request, including one that touches nothing it measures, is a few minutes of runner time rather than the near-zero the short-circuit might suggest.

**Cold cache is unmeasured.** The dispatched run logged `Cache hit for: v0-rust-sim-coverage-Linux-x64-…` / `Cache restored successfully`, so 11.3 minutes is a warm number and must not be quoted as a cold one. `timeout-minutes: 180` leaves room for a large multiple of it, which is the point of choosing 180 over the scheduled copy's 90 rather than a number derived from this measurement.

That run is also the CI-side half of the red-then-green proof, and it exercises the path the `ci.yml` job does not: `xtask-gates.yml` passes **no** `--enforce`, so its `failure` conclusion on `uncovered (moonpool runner): crates/magnetar-proto/src/frame.rs: 1 line(s)` is the flipped **constant** enforcing in CI, not the flag.

Two properties keep this affordable:

- The check is a diff gate and bails with "nothing to verify" before compiling anything when **every** added production `.rs` line is excluded: `xtask/`, `.github/`, `docs/`, `specs/`, `tasks/`, `.claude/`, `crates/magnetar-proto/src/pb/`, every `/tests/`, `/benches/`, `/examples/` path, and everything inside a `#[cfg(test)]` span (`SIM_COVERAGE_EXCLUDE_PREFIXES`, `SIM_COVERAGE_EXCLUDE_FRAGMENTS`, `sim_coverage_cfg_test_lines`).
  This very changeset is one of them; its own `check-sim-coverage` runs finished in 2m0s–7m21s, all of it setup rather than measurement.
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
- **Inline-test stripping moves to the shared span scanner.** `sim_coverage_cfg_test_lines`, over `cfg_test_line_flags`, replaces the first-`#[cfg(test)]`-line cut that exempted 48% of gated lines. Enforcing without this would have been enforcing a third of the surface while claiming all of it. See § The `#[cfg(test)]` hole.

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

**Harder.** Every PR that adds a line under one of the six gated crates must have a moonpool or differential test reaching it, in that same PR. A `magnetar-proto` unit test does not discharge this. That is the intended cost, and the per-PR shape of it is 3 lines' worth over the last ten merged PRs — not the 450 the `HEAD~25` replay shows. It is also now the _real_ shape: before the `#[cfg(test)]` fix above, 71% of the gated lines in that window were never examined at all.

**Cost.** One instrumented `--all-features` build (`aws-lc-fips-sys` included) on any PR that touches a gated crate. PRs that do not touch one short-circuit before building.

**Known limits, stated rather than discovered later.** Three of these came out of review of this changeset and are recorded because a gate that blocks work should not surprise the person it blocks.

- **Some enforced code is structurally uncoverable by a deterministic sim.** `crates/magnetar-auth-sasl/src/gssapi.rs` has no `#[cfg(test)]` at all, is compiled under `--all-features`, and is therefore fully enforced — yet every sim test drives `ScriptedGssapiClient`, and covering `LibGssapiClient` needs a live KDC the sim cannot have. Same shape: `magnetar-auth-athenz/src/jwt_signer/ring.rs` (both backends compile, `jwt_signer/mod.rs` selects aws-lc-rs), `magnetar-auth-athenz/src/zts.rs`'s `HttpZtsClient` (its own module doc says the sim cannot speak HTTPS), and `magnetar-runtime-tokio/src/dns.rs`. A hand-written `impl Display`/`From`/`Default`, a `panic!`, and a `#[cfg(unix)]` block all emit unhit `DA:` records too. **There is no skip mechanism**: `CLAUDE.md`'s exemptions are docs/comment/formatter/dependency-bump, "justified in the commit message", and CI cannot read a commit message. A PR that must touch one of these is stuck until someone adds an escape hatch or a test seam.
- **Code motion counts as addition.** `git diff --unified=0` sees moved lines as added, so splitting a large module — `magnetar-proto/src/conn.rs` is 15,476 lines — makes every production line of every new file require a sim test. Plan refactors accordingly.
- **The repository Actions cache is already full.** Measured 2026-08-01: `GET /actions/cache/usage` reports 10.61 GB against GitHub's 10 GB per-repository limit, with 41 entries, so LRU eviction is already running. `Swatinem/rust-cache` keys on `github.job`, so this job is a new claimant; its entry is 0.15 GB today only because this changeset short-circuits, and an instrumented `--all-features` closure should land near `crypto-matrix`'s 0.58 GB or `test`'s 0.92 GB. The real cost of this job is therefore not its own wall clock but the eviction pressure it adds to `test` (180 min), `build` ×2 and `msrv` (90 each) — jobs whose timeouts were already raised once because cold-cache runs were being cancelled.

**Concurrency.** `ci.yml` sets `cancel-in-progress: true`, while `xtask-gates.yml` deliberately sets `false` for the same job. Measured: 15 of the last 100 `pull_request` runs concluded `cancelled`. That is harmless while `main` is unprotected, because a cancelled check blocks nothing. If the required-check step above is taken, a push cadence faster than the job's runtime can hold a PR at "Expected — waiting for status" indefinitely, and the fix is to give this job its own workflow with `cancel-in-progress: false` rather than to relax the gate.

**Why not a `paths:` filter.** Deliberate. `paths:`/`paths-ignore:` exist only at the workflow `on:` level, so they would apply to all seventeen `ci.yml` jobs; worse, a `paths`-filtered workflow never creates the check run at all, which leaves a required check pending forever. The runtime short-circuit is the correct instrument for a check that is meant to be required.

**Incompatible with macOS.** `check-sim-coverage` is Linux-only in practice: the gate pins `CC`/`CXX`/`ASM` to clang plus `AR`/`RANLIB` to the LLVM binutils (`force_clang_toolchain`), because `aws-lc-fips-sys`'s `delocate` pass rejects the `.data.rel.ro.local` sections gcc emits — at any gcc version, reproduced on gcc 14.4.0, not the "gcc 16+" threshold once assumed. This mattered less while the gate was a local courtesy; it is worth stating plainly now that it sits on every pull request.

**Reversal.** Flipping the constant back is one line, and the advisory arm of `report_uncovered` is kept working and tested so that it stays one line. It also stops the `xtask` **test** build compiling until `sim_coverage_enforces_uncovered_by_default`'s `const` assertion is deleted — the assertion sits in a `#[cfg(test)]` module, so `cargo build --workspace` still succeeds while `cargo test --workspace --all-features` and `cargo clippy --workspace --all-targets` (both in `ci.yml`) fail — and that assertion's message names the ADR that would have to supersede this one — the compile error is the tripwire that forces the second step to be deliberate.

## References

- `xtask/src/main.rs` — `SIM_COVERAGE_ENFORCES_UNCOVERED`, `sim_coverage_enforcing`, `check_sim_coverage`, `report_uncovered`, `force_clang_toolchain`, `SIM_COVERAGE_EXCLUDE_PREFIXES`, `SIM_COVERAGE_GATED_CRATE_PREFIXES`, and `sim_coverage_cfg_test_lines` over the shared `cfg_test_line_flags` (replacing the removed `first_cfg_test_line`). Pinned by `sim_coverage_enforces_uncovered_by_default` and `sim_coverage_cfg_test_import_does_not_exempt_the_rest_of_the_file`.
- `.github/workflows/ci.yml` — the per-PR `check-sim-coverage` job this ADR adds.
- `.github/workflows/xtask-gates.yml` — the scheduled / dispatchable copy, and its header note on why the `main` run short-circuits.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the policy this finally enforces.
- [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) — supersedes its advisory landing only; its report widening, gated-crate list and clang pinning remain binding.
- [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) — the fail-open analysis and the `not gated` reporting, both still binding.
- `CLAUDE.md` invariant #9, `GUIDELINES.md` § Sim coverage, `CONTRIBUTING.md` — updated in this changeset.
- GitHub issue [#386](https://github.com/CleverCloud/magnetar/issues/386) — the residual this closes.
