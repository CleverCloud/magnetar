# ADR-0075 — Retain the weekly two-cluster PIP-33 e2e gate (amends ADR-0046 §6)

- **Status**: Accepted
- **Date**: 2026-07-07
- **Decider**: Florentin Dubois
- **Tags**: testing, ci, process, pip-33

## Context

[ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) folded the end-to-end suite into the per-PR `test` job of `ci.yml`, including the PIP-33 two-cluster `e2e_replicated_subscriptions` test — `ci.yml` now brings up the `docker-compose.replicated-subs.yml` fixture and runs it on every PR and push.
As part of that consolidation, ADR-0046 §6 (and its file-changes list) called for **deleting** the standalone weekly workflow `.github/workflows/e2e-replicated-subs.yml`, arguing that per-PR coverage makes a separate weekly run redundant.

That deletion was never carried out, and the file kept running weekly against stale configuration, so every weekly run failed:

- the broker admin health-check polled `:8080` / `:8081`, but the fixture moved its host ports to `:18080` / `:18081` in `b699181`;
- it built and ran with the `e2e` / `e2e-multi-cluster` Cargo features and `--include-ignored`, all removed by ADR-0046;
- it used the package name `-p magnetar`, renamed to `magnetar-driver` by [ADR-0067](0067-publish-facade-as-magnetar-driver-cli-as-magnetarctl.md).

When the fix was proposed as _deleting_ the workflow (completing ADR-0046 §6), the maintainer rejected it: the dedicated weekly two-cluster run is wanted as a **standalone** gate, not only as one test buried inside the large per-PR e2e job.

## Decision

**Retain `.github/workflows/e2e-replicated-subs.yml`** as a dedicated weekly (plus `workflow_dispatch`) two-cluster PIP-33 gate, and **repair** its stale references so it runs green:

- health-check the brokers' admin endpoints on the fixture's actual host ports `:18080` / `:18081`;
- build and run with `-p magnetar-driver --test e2e_replicated_subscriptions` and no Cargo features or `--include-ignored`.

This **reverses ADR-0046 §6 only** — the deletion of this one file. ADR-0046's core decision stands unchanged: e2e tests are casual (no feature flag, no `#[ignore]`) and PIP-33 runs per-PR in `ci.yml`. The weekly job is now an _additional_ gate on top of per-PR coverage, not a replacement for it.

## Consequences

- PIP-33 two-cluster coverage runs **both** per-PR (in `ci.yml`, ADR-0046) **and** weekly (this workflow). The weekly run isolates PIP-33 broker-orchestration flakiness from the large per-PR suite and gives the release manager a standalone green signal to gate a tag on.
- The redundancy is intentional and cheap (~2 min fixture bring-up + ~15 s test) at a weekly cadence.
- A future contributor reading ADR-0046 §6 must not re-delete the workflow: this ADR, the note on ADR-0046's status line, and a comment in the workflow header all record that the deletion was reversed.
- Verified against the live two-cluster fixture while repairing: both PIP-33 tests pass (2 passed, 0 failed) with the corrected ports and package name.
