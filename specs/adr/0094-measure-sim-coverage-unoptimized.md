# ADR-0094 — Measure sim coverage unoptimized, because the inliner silences counters

- **Status**: Accepted
- **Date**: 2026-08-04
- **Decider**: Florentin Dubois
- **Tags**: sim-coverage, xtask, coverage, llvm-cov, opt-level, mir-inlining, fail-open, ci

## Context

[ADR-0092](0092-enforce-sim-coverage-and-gate-every-pull-request.md) made `check-sim-coverage` enforcing and gave it a per-PR home in `.github/workflows/ci.yml`.
[ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md) had widened its report to the six packages the sim run compiles.
Neither decided what **optimization level** the measurement runs at, and the workspace's `Cargo.toml:223` sets `[profile.test] opt-level = 1` "so tests stay quick under the sans-io harness".

That unstated default makes the gate's verdict wrong, and wrong in both directions.

At `opt-level >= 1` rustc enables MIR inlining.
An inlined callee's coverage counter is never incremented — the call site is attributed and the callee reads zero.
`llvm-cov` then emits a `DA:` record with a zero hit count for a line that a passing test provably executed, and the gate correctly classifies that record as executable-but-uncovered.
The gate is not misreading the report; the report is wrong.

### The measurement

On PR #391, on an unchanged tree, with `rustc 1.97.1 (8bab26f4f 2026-07-14)` and `cargo-llvm-cov 0.8.7` on both sides.

`crates/magnetar-proto/src/scalable_consumer.rs:271` is `consumer_type()`, a `#[must_use]` getter returning a `Copy` field.
It is called twice — `crates/magnetar-differential/tests/scalable_topic_equivalence.rs:893` and `:924` — from `scalable_consumer_session_and_watch_accessors`, a plain `#[test]` with no async, no sockets and no timing, inside a run that reported **127/127 test binaries `ok`, zero failures**.

| `[profile.test] opt-level` | `DA:271` | verdict         |
| -------------------------- | -------- | --------------- |
| `1` (workspace default)    | `0`      | gate **fails**  |
| `0` (this ADR)             | `2`      | gate **passes** |

`2` is exactly the number of call sites.

### It is also non-deterministic

Whether a given function is inlined follows codegen-unit partitioning, so the same commit measured under different build states produces different reports.
Three measurements of the same tree:

| run                        | `SF:` records | uncovered set                          |
| -------------------------- | ------------- | -------------------------------------- |
| ADR-0090, 2026-07-31       | 63            | —                                      |
| local, warm target         | 70            | none — gate passed                     |
| local cold / hermetic / CI | 81            | `scalable_consumer.rs:271-273` (local) |

CI, on the same commit, instead blamed five lines of `magnetar-runtime-tokio/src/client.rs` — `1725, 1728, 1729, 1730, 1731`, the signature of the `async fn` `scalable_topic_subscribe`.
That is the same mechanism seen from the other side: rustc lowers an `async fn` into an outer future constructor, source-mapped to the signature, plus a coroutine body.
The outer constructor is trivial and gets inlined; the coroutine cannot be, and reported hits throughout `1735-1853`.
A function whose body is covered while its signature reads zero is the signature of this defect, not of a missing test.

Two hypotheses were tested and refuted before this one:

- **Toolchain skew** — CI resolves `dtolnay/rust-toolchain@stable` to `1.97.1 (8bab26f4f 2026-07-14)`, byte-identical to local.
- **Stale objects.** `cargo llvm-cov`'s `-p` also selects which packages get _cleaned_, so the four report packages outside the execution set are never cleaned, and `Swatinem/rust-cache@v2` caches `target/` wholesale.
  This looked decisive and is not: `cargo llvm-cov clean --workspace` followed by a cold `CARGO_INCREMENTAL=0` run reproduced the failing report **exactly** — 81 `SF:`, `DA:271,0`.
  Contamination is a real integrity weakness of this gate but it is not what produced these verdicts.

### Why this matters more than a false red

The false red is the visible half.
The dangerous half is that the same mechanism credits a line that never ran, when the neighbour it was folded into did.
A gate whose job is to prove patch coverage must not be able to certify coverage that did not happen, and until this ADR it could.

## Decision

Measure at `opt-level = 0`.

- `SIM_COVERAGE_OPT_LEVEL` in `xtask/src/main.rs` is `"0"`, and `run_sim_lcov` exports `CARGO_PROFILE_TEST_OPT_LEVEL` with it on **both** the execution and the re-export commands, so the re-export resolves the artifacts the execution just built.
- It is a `[profile.test]` override rather than a `RUSTFLAGS` entry because `cargo-llvm-cov` owns `RUSTFLAGS` — it appends `-C instrument-coverage` there — and a second writer would clobber it.
- The workspace's `[profile.test] opt-level = 1` is untouched. It governs every ordinary `cargo test`; only the coverage measurement overrides it.

## Consequences

**Easier.** The gate's verdict is a property of the source and the tests, not of the optimizer. The same commit measures the same way warm, cold, and on CI, so a red gate is now evidence about the diff rather than about the build state — which is what ADR-0092 assumed when it made the check enforcing.

**Harder.** The coverage run is slower: its test binaries are unoptimized, and changing `CARGO_PROFILE_TEST_OPT_LEVEL` changes the fingerprint, so the first run after this lands rebuilds the instrumented closure. The job already budgets `timeout-minutes: 180`.

**Cost.** One environment variable on two commands.

**Not addressed.** The clean-scope asymmetry ADR-0090 introduced — execution cleans two packages, the report covers six — remains. It was refuted as the cause here but is still capable of reporting over artifacts the current pass did not build. `docs/follow-ups.md` carries it.

**Incompatible with.** Any future decision to speed the gate up by optimizing its build. That trade is not available: the measurement is only meaningful against the source structure it reports on.

## References

- `xtask/src/main.rs` — `SIM_COVERAGE_OPT_LEVEL` and `run_sim_lcov`.
- `Cargo.toml:223` — `[profile.test] opt-level = 1`, the default this overrides.
- `crates/magnetar-proto/src/scalable_consumer.rs:271` and `crates/magnetar-differential/tests/scalable_topic_equivalence.rs:893` — the measured case.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the 100% patch-coverage requirement this gate enforces.
- [ADR-0088](0088-sim-coverage-gate-scope-report-ungated-additions.md), [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md), [ADR-0092](0092-enforce-sim-coverage-and-gate-every-pull-request.md) — the gate's scope, report closure and enforcement. None decided the optimization level; this ADR adds that and supersedes none of them.
