# ADR-0098 — Parallelize per-PR test execution

- **Status**: Accepted
- **Date**: 2026-08-10
- **Decider**: Florentin Dubois
- **Tags**: testing, ci, e2e, github-actions

## Context

ADR-0046 folded every test into one per-PR `test` job to avoid a duplicate Cargo build.
The test surface has since grown enough that the aggregate job reached GitHub Actions' 180-minute ceiling twice on pull request #402.
The latest run spent about 23 minutes reaching the Rust tests, then stalled in the non-e2e `scalable_pushed_layout_reaches_the_client` test until the job was cancelled at three hours.
An aggregate job hides whether the constrained resource is compilation, Docker startup, an e2e target, or a non-e2e test, and one stalled target prevents every later target from reporting.

## Decision

Keep ADR-0046's test semantics and split only the CI execution topology.

1. A `non-e2e` matrix cell runs every workspace package except `magnetar-driver`, then the façade library, doctests, and every façade integration target whose filename does not start with `e2e_`.
2. Four `e2e` cells enumerate the sorted `crates/magnetar/tests/e2e_*.rs` paths at runtime and assign every target to one shard by index modulo four.
3. `e2e_replicated_subscriptions` is always assigned to shard 3, and only that cell starts, configures, logs, and tears down the PIP-33 two-cluster fixture.
4. Every cell retains `--all-features`, `--locked`, normal libtest execution, failure output, and the 180-minute hard timeout.
5. The local validation contract remains `cargo test --workspace --all-features --locked`; no Cargo feature, `#[ignore]`, retry, or widened test timeout is introduced.
6. The six previously unbounded connect, lookup, and subscribe waits in `scalable_pushed_layout_reaches_the_client` use the existing one-minute `HANG_GUARD`, so a recurrence names the blocked engine and operation instead of consuming the whole CI budget.

The shard inventory is fail-closed without a second maintained list: the shell glob is the inventory, every matching path receives exactly one numeric assignment, and a shard that selects no targets fails.
A newly added `e2e_*.rs` target therefore runs automatically.

## Consequences

**Positive**

- Non-e2e and Docker-bound failures report independently and execute concurrently.
- A stalled operation fails within one minute, and one failed cell no longer suppresses the results of every e2e target.
- Each e2e cell stays far below the aggregate 180-minute budget under the current suite size.
- PIP-33's five-container fixture consumes resources on one runner instead of every e2e runner.

**Negative**

- Four e2e runners compile the all-features façade independently, increasing total billed runner minutes in exchange for lower wall-clock latency and isolation.
- A failure in shared setup can appear in more than one cell.

**Neutral**

- The finite guard exposes a recurrence of the PR #402 race but does not weaken, skip, or retry the test.
- ADR-0046's no-feature-gate and no-`#[ignore]` decisions remain binding; its single-job CI topology is superseded by this ADR.

## References

- [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — test semantics retained; single-job topology superseded.
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) — parallel test matrix.
- [`docs/testing.md`](../../docs/testing.md) — local and CI execution contracts.
- GitHub pull request [#402](https://github.com/CleverCloud/magnetar/pull/402) — aggregate-job timeout evidence.
