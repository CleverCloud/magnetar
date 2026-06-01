# ADR-0047 — Failing-seed registry: per-PR replay + daily discovery + mandatory triage

- **Status**: Accepted
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: testing, moonpool, ci, process, regression-guard

## Context

[ADR-0036](0036-moonpool-seed-sweep-daily-random.md) shifted the moonpool deterministic-simulation seed sweep from a fixed per-PR matrix to a **daily random** sweep (`.github/workflows/moonpool-seed-sweep.yml`).
The motivation was sound: a fixed per-PR matrix always exercises the same scheduling ordering, so the same seeds passing on every PR adds zero coverage once a commit-pair is in the cache.
A 16-random-seed daily sweep over the same wall-time budget explores the seed space materially better.

The trade-off ADR-0036 explicitly accepted: _failures the daily sweep discovers do not gate the PR that introduced them_. If a regression lands today, the daily sweep flags it tomorrow, by which point the authoring PR has merged, the contributor has context-switched, and the regression is co-located with whatever the next PR happened to touch.
The first observation is also the last one the sweep makes against that seed — the next day's 16 seeds are fresh randoms, so the regression-causing seed is never re-exercised unless somebody manually notes it down.

Concretely, the gap looked like this:

1. Daily sweep finds seed `S` fails on `main@<sha-N>`.
2. The workflow reports a failure with the seed printed in the log.
3. The contributor that introduced the regression on `<sha-N>` (often no longer in the room) doesn't see the seed in CI of their PR — the sweep ran the next morning.
4. No replay path exists.
   The seed is in the log of one failed workflow run, nowhere else.
5. By the time triage happens, `main` has moved several commits past `<sha-N>`, the seed may now pass against `main@<sha-M>` (because later code changes accidentally fixed it, or because the failure was rare even at that seed), and the original regression slips back into the simulation surface invisibly.

The discipline this ADR fixes is **persistence**: once a seed has been observed failing, it must be replayed on every subsequent CI run until the underlying bug is fixed.

## Decision

Adopt a persistent **failing-seed registry** in the repository.

Concretely:

1.  **Registry file**: `crates/magnetar-runtime-moonpool/seeds/known-failing.toml`.
    TOML so each entry carries metadata (date discovered, the workflow run URL, status, narrative note) without needing a parallel markdown file that drifts.
    The file is the source of truth.

2.  **Per-PR replay** in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml): a `seed-replay` job reads every `[[seed]]` whose `status = "open"` and runs `MOONPOOL_SEED=<value> cargo test -p magnetar-runtime-moonpool --features crypto-aws-lc-rs --locked` against it.
    Any failure gates the PR.
    The job is a no-op (passes trivially) when the registry has no `open` entries.
    Runs on `push` to `main` AND on every `pull_request` targeting `main`, matching the rest of [`ci.yml`](../../.github/workflows/ci.yml).

3.  **Daily discovery** stays where ADR-0036 put it ([`moonpool-seed-sweep.yml`](../../.github/workflows/moonpool-seed-sweep.yml)), with one change: when the daily sweep finds a new failing seed, it **MUST open a GitHub issue** with the seed value, the commit SHA, the workflow run URL, and the failing test name.
    The issue is the trigger for human triage.
    The author of the triage PR appends the seed to `known-failing.toml` with `status = "open"` in the **same commit that adds the regression test** (or fixes the bug, see §4).

4.  **Mandatory triage**. A `known-failing.toml` entry is a binding open work item.
    The reviewing maintainer (or the agent picking up the triage `/goal`) MUST:
    1.  Reproduce locally: `MOONPOOL_SEED=<seed> cargo test -p magnetar-runtime-moonpool \ --features crypto-aws-lc-rs --locked`
    2.  **If it reproduces locally** → fix the bug.
        The fix PR removes the entry from `known-failing.toml` in the same commit.
        CI replay then asserts the seed passes against the post-fix code.
    3.  **If it does NOT reproduce locally** → this is the trickier case and is **still mandatory work**. Common shapes: - **Genuine flake** (data-race tolerance, host-clock leak the moonpool `Providers` aren't fully isolating).
        Update the entry with `status = "investigating"` + a narrative note documenting what's been ruled out.
        The entry stays in the registry; CI keeps replaying.
        Triage continues until either a reproducer materialises (back to case 2) or the entry gets marked `status = "wontfix"` with a binding ADR explaining why (extremely rare; the bar is "this seed exposed a known cross-host scheduler artefact that moonpool-sim cannot model and we have a separate test for the property"). - **Environment-specific failure** (CI runner image, Rust toolchain, transitive dep version).
        Pin or work around in a follow-up commit, document under the entry.
        Don't remove the entry until the workaround is in.

              "I can't reproduce, so I'm closing this" is **not** an

        acceptable resolution.
        Per the user-authoritative policy driving this ADR: "if it does not work locally, dig in anyway."

5.  **Local validation chain** picks up the replay too.
    `cargo xtask check-known-failing-seeds` (lands alongside this ADR) parses `known-failing.toml`, replays each `open` seed, and exits non-zero on any failure.
    Mirrors the [`ci.yml`](../../.github/workflows/ci.yml) `seed-replay` job one-to-one — the local invariant is "if CI's replay job would fail, this xtask fails too."
    Added to the workspace validation chain in `CLAUDE.md`.

6.  **Registry hygiene**: removing an entry happens in the **same PR** that lands the bug fix; the PR description names the seed so the audit trail survives `git log`.
    The PR's CI replay run is the binding evidence that the fix actually works (the seed no longer fails).
    A seed that has been silently dropped without a fix PR is a process violation surfaced by `git blame` on the `known-failing.toml` removal commit.

## Consequences

**Positive**

- A regression discovered by the daily sweep is replayed on every subsequent PR until fixed.
  No more "the sweep ran once two weeks ago and the seed has been buried since."
- The registry is the single source of truth.
  There is no log-trawl required to find the open seeds.
- Local repro is a one-liner from the registry value.
  The PR author of the fix doesn't need to context-switch from CI logs to figure out which command to run.
- The same xtask command is enforceable locally and in CI, matching the existing pattern for `check-no-channels`, `check-no-io-deps`, etc.
- "Mandatory dig-in even when local repro fails" is an explicit, binding policy rather than a soft-handshake norm.
  Saves the reviewer the conversation every time a triage PR shows up with "couldn't repro, closing."

**Negative**

- Per-PR CI gets a new `seed-replay` step.
  Cost is `O(number of open entries × ~test wall time)`.
  With the discipline that an open entry must be cleared by a fix PR, the steady-state count is expected to be 0–3; replay cost is bounded.
- The daily sweep workflow grows the "open an issue on failure" responsibility.
  Issue creation is bounded too (only on new failures) but adds a `gh issue create` step and the corresponding `permissions: issues: write` block.
- A flake that genuinely doesn't reproduce locally now sits in the registry indefinitely, polluting per-PR CI with a permanent red until somebody invests the deep-dive time.
  The `status = "investigating"` shape is the pressure relief valve — but per the binding policy, it's pressure, not exemption: triage continues until resolved.
- The registry file is hand-curated.
  There is no auto-append by the daily sweep; the human/agent picking up the discovery issue adds it.
  This is intentional — auto-append would create a registry that grows without bounded review.

**Neutral**

- ADR-0036's "daily random discovery" precedent is **preserved exactly**. This ADR adds a persistence + replay layer on top; it does not change which seeds the daily sweep picks (still 16 fresh randoms per run).
- Registry TOML schema (one `[[seed]]` array entry per failing seed) is intentionally minimal — `value` + `discovered` + `workflow_run`
  - `status` + optional `note` + optional `test_name`.
    Anything
    richer goes in the linked issue / PR.

## Alternatives considered

- **Don't persist anything; rely on the daily sweep alone**. Status quo prior to this ADR.
  Rejected on the regression-slippage grounds laid out in §Context.
- **Persist seeds in a plain `.txt` file, one per line**. Simpler parser, but no place for the discovery date, status, or narrative.
  Rejected: when a seed sits in the registry for weeks, the reviewer needs to know whether it's `open` (waiting for triage) or `investigating` (deep-dive in flight).
  A flat list collapses those into "still there."
- **Auto-append failing seeds via the daily workflow opening a PR**. Considered but rejected: an auto-PR with `permissions: contents: write + pull-requests: write` widens the attack surface for limited gain.
  The human/agent who triages the discovery issue already touches the registry; let them.
- **Move the per-PR replay into the existing `test` job**. Rejected: a separate `seed-replay` job runs in parallel with `test` (no serial dependency), and its failure surface is more clearly attributable in the GH Actions UI ("seed-replay failed against seed X" vs. "test job failed at some point inside a 20-minute e2e + sim run").
- **Cap the registry size**. Rejected: the natural ceiling is the fix-rate of the triage discipline.
  A registry that grows past 5–10 entries is a process-failure signal, not a state to engineer around.

## References

- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — Moonpool seed sweep: daily random, not fixed per-PR.
  **Amended by this ADR** (the discovery cadence is unchanged; the persistence + replay layer is added on top).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — Cross- runtime test + coverage policy.
  The 1:1 tokio↔moonpool test count and the differential equivalence harness are the upstream invariants this ADR's registry protects.
- `crates/magnetar-runtime-moonpool/seeds/known-failing.toml` — the registry file landed by this ADR.
- `.github/workflows/ci.yml` — `seed-replay` job lands here.
- `.github/workflows/moonpool-seed-sweep.yml` — daily discovery workflow gains the issue-open responsibility.
- `xtask/src/main.rs` — `check-known-failing-seeds` subcommand.
- `CLAUDE.md` — validation chain entry for the local replay command.
