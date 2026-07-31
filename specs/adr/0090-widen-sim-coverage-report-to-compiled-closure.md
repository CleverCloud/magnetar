# ADR-0090 — Widen the sim-coverage report to every crate the sim run compiles, and land it advisory

- **Status**: Accepted (amends [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) — the report scope it measured and recorded as narrow is widened; its `not gated` reporting and its fail-open analysis remain binding)
- **Date**: 2026-07-31
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, xtask, moonpool, adr-0024, adr-0088

## Context

[ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) recorded what ADR-0024's patch-coverage gate mechanically achieved rather than what its doc comment claimed: `cargo run -p xtask -- check-sim-coverage` enforced 100% patch coverage over `crates/magnetar-runtime-moonpool/src/**` and `crates/magnetar-differential/src/**` and nothing else, so `magnetar-proto` — the crate `CLAUDE.md` invariant #9 singles out — had never been gated at all.
It made the shortfall audible (`not gated (outside the moonpool coverage run): …`) instead of silent, and deferred the fix to `docs/follow-ups.md` §10.

The deferral rested on a cost estimate.
§10 called fixing the scope "a rework of the gate's mechanics, not a flag change", and priced the `--no-report` + `llvm-cov report` stitch at "a multi-step invocation and a longer run".

That estimate is wrong, and the measurement below is what disproves it.

### The measurement

Measured 2026-07-31 on branch `fix/sim-coverage-scope`, re-exporting the profile data of an unchanged execution step over the packages the run actually compiles:

```
rg -o '^SF:.*' target/sim-coverage.lcov | sed 's|^SF:.*/crates/||' | cut -d/ -f1 | sort | uniq -c
```

| Crate                       | `SF:` records, 2026-07-30 (ADR-0088 baseline) | `SF:` records, 2026-07-31 (widened) |
| --------------------------- | --------------------------------------------- | ----------------------------------- |
| `magnetar-proto`            | 0                                             | 28                                  |
| `magnetar-runtime-tokio`    | 0                                             | 12                                  |
| `magnetar-runtime-moonpool` | 12                                            | 12                                  |
| `magnetar-auth-athenz`      | 0                                             | 5                                   |
| `magnetar-differential`     | 4                                             | 4                                   |
| `magnetar-auth-sasl`        | 0                                             | 2                                   |
| **Total**                   | **16**                                        | **63**                              |

The baseline column reproduces ADR-0088's 2026-07-30 measurement exactly, so the two are comparable rather than merely adjacent.

**The widening required zero recompilation.**
`cargo-llvm-cov`'s `RUSTC_WRAPPER` builds its instrumentation list from `cx.ws.metadata.workspace_members` — every workspace member, with `-p` never consulted (`cargo-llvm-cov-0.8.7`, `src/wrapper.rs:63-83`).
`-p` selects which test binaries run, which packages get cleaned, and the `-ignore-filename-regex` handed to `llvm-cov export` (`src/report.rs:869-986`, where an excluded member's manifest directory is OR-joined into one exclusion string).
So `magnetar-proto`'s counters were in the profile data all along; only the report filter hid them.
The second step is a second `llvm-cov export` over object files and profdata already on disk — no build, no test run, no measurable added time.

`docs/follow-ups.md` §10's claim that closing this is a rework of the gate's mechanics rather than a flag change, at the cost of a longer run, is therefore disproven and must not be repeated.

### The backlog the widened report exposes

Replaying real history through the widened gate on 2026-07-31:

| Window                                        | Uncovered added lines                                                                                                                                                                                    |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--base HEAD~10` (merged since 2026-07-28)    | 6 lines across 4 files: `magnetar-proto/src/conn.rs` 284 + 1528, `magnetar-proto/src/health_probe.rs` 204, `magnetar-runtime-tokio/src/client.rs` 2036, `.../src/consumer.rs` 228 + 231                  |
| `--base HEAD~25` (merged since release 1.2.0) | 450 lines across 15 files, plus 20 files on the advisory `not gated` path — dominated by `magnetar-runtime-tokio/src/client.rs` (191), `.../src/consumer.rs` (61) and `magnetar-proto/src/conn.rs` (142) |

**Why a backlog exists at all.**
The gate has effectively never run per-PR.
[`.github/workflows/xtask-gates.yml`](../../.github/workflows/xtask-gates.yml) runs it only on a schedule against `main`, where `merge-base(origin/main, HEAD) == HEAD` makes the diff empty and the check short-circuits with "nothing to verify".
The proof that this is the cause, and not the widening, is `magnetar-runtime-moonpool` itself: gated since ADR-0024, inside the old 16-record report the whole time, and still carrying 43 uncovered added lines over `HEAD~25`.

**Why the 450 is never charged to a future changeset.**
`check-sim-coverage` is a patch gate against `merge-base(origin/main, HEAD)`, so each branch is measured on its own added lines only.
The 450 is an artifact of diffing against a 25-commit-old base — something no ordinary branch does — not a debt that a normal changeset inherits.
The `HEAD~10` figure, 6 lines, is the honest estimate of what a working branch meets.

## Decision

`run_sim_lcov` (formerly `run_moonpool_lcov`) becomes two steps, because execution scope and report scope are different questions.

1. **Execute** — unchanged: `cargo llvm-cov -p magnetar-runtime-moonpool -p magnetar-differential --all-features --locked`, writing `target/sim-coverage-exec.lcov`.
   Only those two crates' test binaries run.
2. **Re-export** — new: `cargo llvm-cov report` over the same profdata and object files, with one `-p` per entry of `SIM_COVERAGE_REPORT_PACKAGES` plus `--ignore-filename-regex 'crates/magnetar-proto/src/pb/'`, writing `target/sim-coverage.lcov`.
   No clean, no build, no test run.

Seven decisions ride on that split.

- **Report scope is the six crates the sim run actually compiles**: `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-auth-athenz`, `magnetar-auth-sasl`.
  Derived from the measurement above and corroborated independently by reading the `--extern` flags on the rustc invocations for the moonpool and differential test targets.
  `magnetar-admin`, `magnetar-auth-oauth2`, `magnetar-fakes` and `magnetar-messagecrypto` were tried in the `-p` list and emit **zero** records — they are not linked into the sim binaries — so they are deliberately absent, since `report_ungated` prints this list as "the reported closure" and naming a crate there that can never contribute a record would tell a record-less file it sits outside a closure carrying its own crate name.
- **Execution scope is unchanged.**
  `magnetar-proto`'s and `magnetar-runtime-tokio`'s own unit tests still never run under this gate and can never satisfy it: a proto line counts as covered only when a moonpool or differential test reaches it transitively.
- **`magnetar-runtime-tokio` is in report scope deliberately.**
  It is a regular dependency of `magnetar-differential` (`crates/magnetar-differential/Cargo.toml`), so the differential equivalence suite drives it, and ADR-0024 already requires a differential test for every behavioural change — a tokio line no differential test executes is exactly the gap the policy says should not exist.
  This also resolves the complaint recorded in the module doc of `crates/magnetar-differential/tests/corrupted_broker_scheme_equivalence.rs`, which had to warn readers that the one test executing the fixed tokio helper bought no gate coverage.
- **The landing is advisory**: `const SIM_COVERAGE_ENFORCES_UNCOVERED: bool = false`.
  Uncovered added lines are printed in full, with a count, and the check exits 0.
  A new `--enforce` flag restores the failing exit code for a single invocation, which is how the fail path stays exercised while the default is `false`.
  Flipping the constant to `true` is the tracked follow-up, and it should be flipped together with wiring the gate into per-PR CI — without that wiring the flip changes nothing in practice, since the scheduled `main` run short-circuits before it can fail.
- **A gated crate that emits no records at all is a hard failure**, `--enforce` or not.
  `SIM_COVERAGE_GATED_CRATE_PREFIXES` names the six crates; if one contributes not a single `SF:` record, `report_missing_gated` bails.
  That signals a broken or misconfigured gate rather than a missing test, and a gate that cannot measure must never report success — it is precisely the fail-open shape ADR-0088 documented, so it cannot be left on the advisory path.
  A record-less **file** inside a crate that did emit records stays advisory: LLVM derives its mapping from per-function records, so a `pub mod` / `pub use` / `pub const`-only file legitimately has no `SF:` entry and no test could ever cover its added lines.
- **The `magnetar-driver` façade stays out of the report.**
  Nothing in the executed closure depends on it, so step 1 never compiles it and no `-p` can conjure records for it.
  Pulling it in would drag the façade's 58 Docker-bound `crates/magnetar/tests/e2e_*.rs` files — which per [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) carry no feature gate and no `#[ignore]` — into every coverage run.
  Façade additions keep printing `not gated` and exiting 0.
- **The gate pins its own C/asm toolchain to clang on Linux** (`force_clang_toolchain`, split out of `apply_fips_toolchain`).
  `--all-features` reaches `crypto-fips` and therefore `aws-lc-fips-sys`, whose `delocate` pass rejects gcc's `.data.rel.ro.local` sections with `.data section found in module`.
  Before this the gate was **unrunnable on a bare Linux checkout**.
  The failure was previously attributed to "gcc 16+ (Fedora 44 default)" in both `CLAUDE.md` and the helper's doc comment; that attribution is wrong — it reproduced on gcc 14.4.0 on 2026-07-31.
  Which aws-lc sources land in `bcm.c` depends on cargo's feature unification, so the host gcc version is the wrong axis: no version is safe, and the toolchain is pinned unconditionally instead.
  `apply_fips_toolchain`'s feature-name match could not serve here, because the callers that need it most pass `--all-features`, a shape no feature-name match recognises.

### Rejected alternatives

| Option                                                                                    | Why not                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Per-package `--no-report` runs stitched with `llvm-cov report` (`docs/follow-ups.md` §10) | `--no-report` implies `--no-clean` (`cargo-llvm-cov-0.8.7`, `src/cli.rs:1590`: `clean.no_clean \|= report.no_report \| no_run;`), so stale `*.profraw` from an earlier run merges into the export and an uncovered added line can report as **covered**. A coverage gate that passes on stale data is worse than the narrow one it replaces.                                 |
| `--workspace` with `--exclude-from-test` (`docs/follow-ups.md` §10)                       | Two defects. `clean_partial` cleans `cx.workspace_members.included` (`src/clean.rs:52-61`), so `--workspace` wipes and rebuilds every member on each run. And `--exclude-from-test` is an inverse allowlist: a crate added to the workspace later joins the **execution** set by default, silently admitting a tokio-only or Docker-bound suite. The `-p` form fails closed. |
| Inverting `--ignore-filename-regex` into a keep-only pattern                              | `ignore_filename_regex` OR-joins the user's pattern with the excluded members' manifest directories and the default vendor/registry patterns into one exclusion string (`src/report.rs:869-986`), so the flag can only ever subtract files from the report, never add them.                                                                                                  |
| Adding `-p magnetar-driver` to reach the façade                                           | This is the option that drags the Docker-bound e2e suite in (ADR-0046: no feature gate, no `#[ignore]`), making a live `apachepulsar/pulsar` container a precondition of every coverage run. Already flagged in `docs/follow-ups.md` §10, and it stays rejected.                                                                                                             |

## Consequences

**Easier.**
A `magnetar-proto` or `magnetar-runtime-tokio` addition is now measured against the sim run instead of skipped as "not executable", which is what ADR-0024 asked for and what ADR-0088 recorded as absent.
The remaining `not gated` lines mean what they say — the façade and the crates the sim run never compiles — instead of covering three quarters of the workspace.

**Harder.**
Nothing is newly blocked, because `SIM_COVERAGE_ENFORCES_UNCOVERED` is `false`.
What is newly visible is the work: a branch touching engine-visible proto or tokio code now prints uncovered lines it never printed before, and closing them means a moonpool or differential test that reaches them — a tokio unit test cannot.

**Cost.**
A second `cargo llvm-cov report` invocation, two constants (`SIM_COVERAGE_REPORT_PACKAGES`, `SIM_COVERAGE_GATED_CRATE_PREFIXES`), the `classify_uninstrumented` / `silent_gated_prefixes` / `report_missing_gated` split, the `--enforce` flag, and `force_clang_toolchain`.
A `--reuse-lcov` flag exists for sizing and debugging only; every line it prints carries `[REUSED LCOV — NOT A FRESH MEASUREMENT]`, because a report older than the diff maps stale covered line numbers onto new code and is otherwise textually identical to a real run.

**Incompatible with — say this plainly.**
While `SIM_COVERAGE_ENFORCES_UNCOVERED` is `false`, a green `check-sim-coverage` is **not** evidence of ADR-0024 patch coverage.
It is evidence that the gate ran, measured the six crates, and printed what it found.
That is the same shape of over-claim ADR-0088 was written to stop, and it is being accepted temporarily with eyes open rather than overlooked: the advisory output is loud, carries a count, names the constant, and tells the reader to re-run with `--enforce`.

**Residual risk.**
Three, in descending order.
An advisory gate is a gate people learn to scroll past, so the longer the constant stays `false` the more the backlog grows and the harder the flip becomes — that is why the flip is tied to per-PR CI wiring rather than left open-ended.
Execution scope is still the moonpool + differential binaries, so a proto or tokio line only the tokio unit tests reach is reported uncovered and can only be answered by a new sim or differential test, never by pointing at an existing tokio test.
And the façade remains entirely unmeasured, which no report-side change can fix while its e2e suite requires Docker.

## References

- `xtask/src/main.rs` — `run_sim_lcov` (the two-step execute-then-re-export), `SIM_COVERAGE_REPORT_PACKAGES`, `SIM_COVERAGE_GATED_CRATE_PREFIXES`, `SIM_COVERAGE_ENFORCES_UNCOVERED`, `classify_uninstrumented`, `silent_gated_prefixes`, `report_missing_gated`, `report_ungated`, `report_uncovered`, `force_clang_toolchain`; the `sim_coverage_*` unit tests pin the constants against the executed closure.
- `cargo-llvm-cov` 0.8.7 — `src/wrapper.rs:63-83` (instrumentation is workspace-wide, `-p` is not consulted), `src/report.rs:869-986` (the OR-joined exclusion regex), `src/cli.rs:1590` (`--no-report` implies `--no-clean`), `src/clean.rs:52-61` (`clean_partial` cleans every included member).
- `crates/magnetar-differential/Cargo.toml` — the dependency set that makes `magnetar-runtime-tokio` part of the compiled closure.
- `crates/magnetar-differential/tests/corrupted_broker_scheme_equivalence.rs` — the module doc whose "no coverage at all in the gate's sense" complaint this closes.
- `.github/workflows/xtask-gates.yml` — the scheduled home where the gate short-circuits on `main`, which is why the backlog accumulated.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the patch-coverage policy this gate serves.
- [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md) — the narrow scope this amends, and the `not gated` reporting it introduced, which is unchanged.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — why the façade cannot enter the coverage run.
- [ADR-0035](0035-pluggable-crypto-provider.md) — the `crypto-fips` feature that makes the clang pin necessary.
- `docs/follow-ups.md` — §10, whose cost estimate this disproves, and the tracked flip of `SIM_COVERAGE_ENFORCES_UNCOVERED`.
- `CLAUDE.md` § "Non-negotiable invariants" #9 and § "Validation chain" — where the gate's scope is stated for everyday work.
