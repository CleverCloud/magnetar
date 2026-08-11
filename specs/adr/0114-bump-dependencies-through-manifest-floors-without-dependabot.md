# ADR-0114 — Bump dependencies through manifest floors, without Dependabot version updates

- **Status**: Accepted
- **Date**: 2026-10-09
- **Decider**: Florentin Dubois
- **Tags**: dependencies, ci, process

## Context

`.github/dependabot.yml` asked Dependabot for weekly version updates of both Cargo workspaces (`/` and `crates/magnetar-proto/fuzz`) and of the GitHub Actions under `.github/workflows/`.

The workspace does not take dependency bumps that way.
A bump raises the caret floor in `Cargo.toml` (`tokio = { version = "^1.53.2", … }`) and refreshes `Cargo.lock` in the same changeset, as every "bumped workspace manifest floors" entry in `CHANGELOG.md` records — most recently PR #470.
Dependabot instead opened lockfile-only PRs whenever an existing requirement already admitted a newer release: #862 (`tokio`), #863 (`uuid`), #864 (`rustls-openssl`), #865 (`tokio-rustls`) and #467 (`thiserror`) each changed `Cargo.lock` alone.
A lockfile-only bump leaves the manifest promising an older version than the one every build resolves, so it is not accepted here.

Those PRs also outlived their purpose.
PR #470 raised every one of those floors, and both `opentelemetry` floors from #465 and #466, on 2026-10-09; Dependabot closed the two GitHub Actions PRs it superseded within minutes, but left the seven Cargo PRs open until they were closed by hand.

## Decision

`.github/dependabot.yml` is removed; Dependabot no longer opens version-update PRs for Cargo or GitHub Actions.

- Dependency and action bumps land as consolidated changesets that raise the manifest floors, refresh `Cargo.lock` (and the fuzz workspace's own lockfile) in the same commit, and record the floors in `CHANGELOG.md`.
- A PR that changes `Cargo.lock` without the matching manifest floor is not merged.
- Dependabot alerts and Dependabot security updates are repository settings, not part of this file, and this ADR does not change them.
  Security updates were enabled on 2026-10-09 (`GET /repos/CleverCloud/magnetar/automated-security-fixes` → `enabled: true`); while they stay enabled, a security advisory can still open a lockfile-only PR, which this ADR's rule then applies to.

## Consequences

- No weekly queue of lockfile-only PRs to triage or close.
- Upstream releases are no longer surfaced automatically; staying current depends on the periodic manual dependency and CI refresh, which is how floors moved in practice.
- Security advisories still reach the repository through Dependabot alerts.

## References

- `CHANGELOG.md` — "bumped workspace manifest floors" entries.
- PR #470 — the dependency and CI refresh that superseded #465–#467 and #862–#865.
- `docs/testing.md` § fuzzing — the independent fuzz workspace and its lockfile.
