# ADR-0113 — Run the performance measurement nightly instead of on every pull request

- **Status**: Accepted
- **Date**: 2026-10-09
- **Decider**: Florentin Dubois
- **Tags**: ci, performance, moonpool, process

## Context

`performance.yml` (added by PR #729, described in `docs/performance.md` § "Nightly and on-demand delivery") ran on every `pull_request`.
Each run builds `main` and the PR head in release mode with thin LTO and all features, once per worker: one prepare job (90 minutes), thirteen workers (180 minutes each, at most four at a time) and a reconciliation job (30 minutes).
The measured costs are informative — no threshold fails a PR — so the only verdicts it adds to a PR are functional failures and invalid collection, which the rest of `ci.yml` already reports for the same tests.

In practice the workflow was the slowest and least reliable check on every PR:

- `concurrency` keyed on `github.ref` with `cancel-in-progress: true` cancelled the whole run on every push, so an iterated PR rarely produced a report.
- The reconciliation job fails whenever one worker artifact is missing, so a single cancelled or timed-out shard turned the run red.
- `docs/performance.md` itself records that the eight-shard partition's duration and disk fit were never qualified on Actions; the first four-shard run hit the 180-minute cap.

Alternatives considered for a scheduled run:

- **`main` against itself.** A bare `schedule:` checks out `main` and resolves `main` as the base, which the harness labels calibration with no product effect — the same trap `CLAUDE.md` records for the scheduled sim-coverage run, which "diffs `main` against itself".
- **`main` against the latest release tag.** `v1.7.2` predates the `apachepulsar/pulsar:4.2.4` pin of ADR-0109, so its e2e families would boot Pulsar 5.0.0 and fail on the base side until the next release.
- **`main` against `main` as of 24 hours earlier.** Scheduled runs start with a variable delay, so consecutive windows can leave a gap a merge falls into.

## Decision

`performance.yml` runs nightly at 01:17 UTC on `main` and on `workflow_dispatch`; the `pull_request` trigger is removed.

- A nightly compares `main`'s head against the head the last successful nightly measured (`gh run list --event schedule --status success`), so consecutive nights cover `main` without a gap and a failed or skipped night widens the next window.
  With no previous successful nightly, or one whose head is no longer an ancestor of `main`, the base falls back to `main` as of 24 hours earlier.
- When the base equals the head, the nightly records "nothing to measure" in its summary and skips the workers and reconciliation.
- A manual dispatch keeps the previous contract: the dispatched ref against `main`'s head, including a branch that targets something other than `main`.
  A dispatch on `main` is a `main`/`main` calibration.
- Nightly and manual runs use separate concurrency groups; a manual dispatch never cancels a running nightly, and a nightly still running when the next fires makes it wait.
- Only the prepare job gains `actions: read`, to look up the previous nightly; every other job keeps `contents: read`.

## Consequences

- PRs no longer wait on, or turn red from, thirteen release builds; a PR that needs a measurement before merge dispatches the workflow on its branch.
- A cost change is attributed to the set of commits merged since the last successful nightly, not to one PR.
- Fixed Moonpool seeds 1–32 without buggify, which the 2026-10-03 amendment of ADR-0036 moved into this workflow's per-PR run, now run nightly on `main`.
  Open failing-seed anchors are still replayed on every PR by `ci.yml`'s `seed-replay` job (ADR-0047), and the daily 128-random-seed sweep of ADR-0036 is unchanged.
- The report text still says "main" and "PR" for the base and candidate columns; in a nightly both are `main` revisions, identified by their SHAs.

## References

- `.github/workflows/performance.yml` — the triggers, base resolution and skip gate.
- `docs/performance.md` § "Nightly and on-demand delivery".
- ADR-0036 — moonpool seed sweep cadence, amended by this ADR.
- ADR-0047 — the per-PR `seed-replay` job that keeps open registry seeds on every PR.
- ADR-0109 — the Pulsar image pin that rules out the release-tag base.
