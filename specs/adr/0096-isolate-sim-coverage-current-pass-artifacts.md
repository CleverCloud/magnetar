# ADR-0096 — Isolate sim-coverage artifacts to the current pass

- **Status**: Accepted
- **Date**: 2026-08-05
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, llvm, ci, artifact-provenance
- **Adds to**: [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md), [ADR-0092](0092-enforce-sim-coverage-and-gate-every-pull-request.md), and [ADR-0094](0094-measure-sim-coverage-unoptimized.md)

## Context

ADR-0090 deliberately split `check-sim-coverage` into two cargo-llvm-cov phases with different package scopes.
The execution phase runs the Moonpool and differential test binaries, while the report phase re-exports the six packages their instrumented closure compiles.

With cargo-llvm-cov 0.8.7, `--no-report` also means `--no-clean`, and package selection controls cleaning as well as execution.
The wider report could therefore discover object files and profiles that no command in the current pass established as its inputs.
Warm local targets and restored CI targets made that a latent fail-open path: a stale object could credit a newly added line even though the current simulation run never executed it.

This was not the cause of PR #391's false red.
A workspace clean reproduced that failure, and ADR-0094 fixed its actual cause by disabling MIR inlining for the measurement.
Artifact provenance remains independently load-bearing because a green enforcing gate must describe the current pass, not whatever compatible object files happen to be present.

A missing per-file `SF:` record is not a valid substitute.
LLVM legitimately emits no record for functionless module, export, and constant-only files, so making every missing file fatal would reject source that has no executable coverage mapping.

## Decision

Every non-reused `check-sim-coverage` invocation owns one fresh coverage/build directory for both cargo-llvm-cov phases.

1. Run locked Cargo metadata before coverage to resolve the effective target and build-storage paths without creating or rewriting `Cargo.lock`.
2. Resolve final-component symlinks and missing suffixes before selecting storage.
3. Create a unique scratch sibling outside both cached target/build trees and on the configured build filesystem.
   If the configured storage is itself a mount root and no outside-cache sibling exists on that filesystem, fail with a diagnostic instead of silently building on another disk.
4. Set `CARGO_TARGET_DIR`, `CARGO_LLVM_COV_TARGET_DIR`, `CARGO_LLVM_COV_BUILD_DIR`, and, when Cargo reports support, `CARGO_BUILD_BUILD_DIR` to that same absolute directory for execution and report.
5. Reject non-empty `LLVM_COV_FLAGS`, `LLVM_PROFDATA_FLAGS`, `CARGO_LLVM_COV_FLAGS`, and `CARGO_LLVM_PROFDATA_FLAGS` without printing their values. cargo-llvm-cov appends those values to tool commands, where they can name arbitrary external objects or profiles and bypass directory isolation.
6. Keep the scratch directory alive through LCOV generation and reading, then remove it after every post-creation success or failure.
   Cleanup failure is reported without masking the primary coverage error.
7. Keep the final report at `target/sim-coverage.lcov`.
   It is an output only; no profile or object input comes from the cached workspace target.

Execution scope, report scope, `opt-level = 0`, merge-base intersection, generated-code exclusion, functionless-file treatment, record-less-crate failure, and the enforcing verdict remain unchanged.
The `--reuse-lcov` diagnostic path remains explicitly non-authoritative and skips this build contract by design.

## Consequences

**Easier.** A fresh, warm, locally poisoned, or CI-restored workspace now produces the verdict from the same current-pass input set.
Cache implementation details are no longer part of the gate's integrity argument.

**Harder.** A real coverage pass recompiles its instrumented closure in an invocation-owned directory.
Dependency downloads and ordinary non-coverage Cargo targets remain reusable, but first-party coverage objects do not.

**Fail closed.** Locked metadata drift, injected LLVM artifact flags, an unusable storage topology, child-command failure, and cleanup failure are all explicit errors.

**Disk behavior.** The scratch directory follows the configured Cargo build filesystem rather than the checkout filesystem.
An uncatchable process death can leave one uniquely named scratch directory, but no later invocation reuses it.

**Test evidence.** The xtask suite poisons the default primary, `ui_test`, trybuild, and fallback trybuild targets with profile-only, object-only, and combined cases; covers missing and stale lockfiles, final-component symlinks, missing path suffixes, mount-root rejection, child failures, and cleanup error precedence; and includes an explicitly run cargo-llvm-cov 0.8.7 fixture for cold and back-to-back warm passes.

## References

- `xtask/src/main.rs` — `run_sim_lcov`, locked storage discovery, scratch lifecycle, and the provenance regression suite.
- [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) — the two-phase execution/report split retained here.
- [ADR-0092](0092-enforce-sim-coverage-and-gate-every-pull-request.md) — the enforcing per-pull-request verdict.
- [ADR-0094](0094-measure-sim-coverage-unoptimized.md) — the independent optimization-level integrity fix.
