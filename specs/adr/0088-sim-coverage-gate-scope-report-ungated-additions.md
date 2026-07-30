# ADR-0088 — Report additions outside the moonpool coverage run instead of passing them silently

- **Status**: Accepted
- **Date**: 2026-07-30
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, xtask, moonpool, adr-0024

## Context

[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) requires 100% moonpool coverage on the diff, enforced by `cargo run -p xtask -- check-sim-coverage`.
The gate's own doc comment promised that "every line added relative to the merge base is executed by at least one moonpool test".

It did not deliver that over the whole workspace, and the shortfall was invisible.

`run_moonpool_lcov` (`xtask/src/main.rs`) filters the coverage run with `-p magnetar-runtime-moonpool -p magnetar-differential`, and its own doc comment claimed "the whole workspace is instrumented (so coverage attributes to the originating crate, e.g. `magnetar-proto`)".

Measured on 2026-07-30, that claim is false.
The emitted `target/sim-coverage.lcov` carries **16 `SF:` records: 12 under `crates/magnetar-runtime-moonpool/src/` and 4 under `crates/magnetar-differential/src/`, and nothing else.**
The report covers only the two selected packages' own sources, not their dependencies.
`magnetar-proto`, `magnetar-runtime-tokio` and the `magnetar` façade emit no records at all.
Reproduce with `rg -o '^SF:.*' target/sim-coverage.lcov | sed 's|^SF:.*/crates/||'`.

`intersect_diff_with_coverage` reports an added line only when LCOV considers it executable **and** unhit:

```rust
let is_executable = entry.is_some_and(|(exec, _)| exec.contains(&line));
let is_hit = entry.is_some_and(|(_, hit)| hit.contains(&line));
if is_executable && !is_hit { … }
```

For a file with no LCOV entry, `entry` is `None`, so `is_executable` is `false` and every added line is skipped as though it were a blank line or a `use` statement.
The run then printed "all added lines across N file(s) are covered by the moonpool runner", counting files it had never measured.

Any changeset outside those two crates therefore got a **vacuous pass** on ADR-0024's patch-coverage requirement while reporting full success.
The consequence is larger than a façade blind spot: **`magnetar-proto` has never been gated either**, and it is the crate `CLAUDE.md` invariant #9 singles out ("every change inside `magnetar-proto` ships with all four test layers").

Found while adding `PatternConsumer::aggregate_stats` (`crates/magnetar/src/pattern_consumer.rs`), whose new lines the gate silently ignored; the proto-wide extent surfaced on the first run of the reporting this ADR introduces, which flagged doc-comment additions in `magnetar-proto/src/{conn,consumer,producer}.rs` as ungated.

Two alternatives were considered and rejected **for this changeset**:

- **Broaden the coverage run** so it reports on dependencies too (a `--workspace` run whose _execution_ is still restricted to the moonpool + differential test binaries, or per-package `--no-report` runs stitched with `llvm-cov report`).
  This is the right end state, but it is a rework of the gate's mechanics with its own runtime and correctness questions — including whether adding `-p magnetar-driver` drags the façade's `tests/e2e_*.rs` suite in, which per [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) carries no feature gate and no `#[ignore]` and so always needs Docker and a live `apachepulsar/pulsar` container.
  Tracked as `docs/follow-ups.md` §10 rather than bundled here.
- **Fail on any addition the run cannot measure.**
  Correct in the abstract, but with the scope as measured it would fail on virtually every changeset, including `magnetar-proto` ones for which the required coverage is unobtainable until the gate itself is reworked.

## Decision

`check-sim-coverage` distinguishes the two cases that were conflated, and says out loud what it could not measure.

- An added line that LCOV marks executable and unhit is a **failure**, unchanged: `report_uncovered` still bails, and ADR-0024's 100% requirement is untouched for every package inside the coverage run.
- An added line in a file with **no LCOV entry at all**, and not already matched by `SIM_COVERAGE_EXCLUDE_PREFIXES` / `SIM_COVERAGE_EXCLUDE_FRAGMENTS`, is reported as `not gated (outside the moonpool coverage run): <path>: N added line(s)` and the check **exits 0**.
- The ungated report is printed **before** the failure path, so it survives a run that also has genuine uncovered lines.
- The success line no longer claims coverage over files that were never instrumented; it reports the gated file count and the out-of-scope count separately.

The scope of ADR-0024's patch-coverage gate is hereby stated explicitly and narrowly: as implemented today it is enforced over `crates/magnetar-runtime-moonpool/src/**` and `crates/magnetar-differential/src/**` only — **not** over their dependencies, and therefore not over `magnetar-proto`, `magnetar-runtime-tokio`, or the `magnetar` façade.
ADR-0024's _policy_ (100% moonpool coverage on the diff) is unchanged and remains the target; this ADR records what the _gate_ mechanically achieves so the shortfall is documented and visible rather than silent, and `docs/follow-ups.md` §10 tracks closing it.

## Consequences

**Easier.** A changeset outside the two instrumented crates now says plainly that patch coverage was not enforced on it, instead of printing a success line that overstates what ran.
The reviewer sees the exact files and line counts, and can decide whether the added code is engine-visible enough to deserve a moonpool or differential test that reaches it.
The reporting paid for itself immediately: it is what revealed that `magnetar-proto` was never gated.

**Harder.** Nothing is newly blocked — the gate's failure conditions are strictly unchanged.
The cost is a new advisory line on façade changes, which is the intended signal.

**Cost.** Two helpers (`uninstrumented_files`, `report_ungated`) plus three unit tests in `xtask/src/main.rs`.

**Incompatible with.** Reading a green `check-sim-coverage` as "the whole diff is covered".
It means "the diff is covered wherever the gate can see", and the run now prints the difference.

**Residual — the important one.** Everything outside the two instrumented crates remains unmeasured, `magnetar-proto` included.
Until `docs/follow-ups.md` §10 lands, ADR-0024's patch-coverage requirement on proto changes is carried by review and by the four-layer test policy, not by this gate.
Do not read a green `check-sim-coverage` on a proto change as coverage evidence.

## References

- `xtask/src/main.rs` — `run_moonpool_lcov`, `intersect_diff_with_coverage`, `uninstrumented_files`, `report_ungated`, `check_sim_coverage`; the three `sim_coverage_*` unit tests pin the behaviour.
- `crates/magnetar-differential/Cargo.toml` — the dependency set that determines what the coverage run can instrument.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the patch-coverage policy this scopes.
- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — why broadening the run to the façade is not free.
- `CLAUDE.md` § "Non-negotiable invariants" #9 and § "Validation chain" — where the gate's scope is stated for everyday work.
