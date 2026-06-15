# ADR-0066 — Release 1.0.0 as the first stable version

- **Status**: Accepted
- **Date**: 2026-06-15
- **Decider**: Florentin Dubois
- **Tags**: release-policy, versioning, semver

## Context

Magnetar has been developed under the pre-release version `0.1.0-dev.0` since genesis (75f7c16, 2026-05-20).
Nothing has been tagged or published to crates.io; this is the first release.

[ADR-0010](0010-v0-1-full-java-parity.md) set the _scope_ bar — full Apache Pulsar Java-client parity, no deferrals — and defined the release-cut gate: "the parity matrix is all ✅ or documented 🟡 with a clear remaining-scope statement."
Its filename (`0010-v0-1-...`) reflected an early assumption that the first parity release would carry a `0.x` version.

As of cecf8c4 (2026-06-15) the [parity matrix](../../README.md#java-client-parity-matrix) meets that gate: every Magnetar-column row is ✅ except PIP-460 (scalable topics), a documented 🟡.
The two ❌ in the matrix are the legend row and a _Java_-column gap (OpenTelemetry context propagation) that Magnetar in fact exceeds.
The open question is the version to assign: continue the `0.x` line per ADR-0010's filename, or commit to a stable `1.0.0`.

## Decision

- The first published release is **1.0.0**, a stable release under Semantic Versioning.
  The public API is stable; post-1.0.0 breaking changes require a major-version bump.
  Internal crates version in lockstep with the workspace.
- Project status moves from **pre-alpha** to **stable** across all user-facing docs (`README.md` badge + blurb, `crates/magnetar-cli/README.md`, `crates/magnetar-admin/README.md`, `docs/cli.md`).
  The "API is unstable / do not depend on this in production" caveat is removed.
- This supersedes only the `v0-1` version framing implied by ADR-0010's filename.
  ADR-0010's scope decision and release-cut gate remain in force, unchanged.
- Surfaces not yet stable stay honestly labeled and are **excluded from the 1.0 stability promise** until they graduate:
  - **PIP-460 scalable topics** — experimental scaffold behind `feature = "scalable-topics"` (default off); may change without a major bump while gated ([ADR-0031](0031-pip-460-scalable-subscription-scope.md)).
  - **CLI `produce` / `consume`** — documented stubs; `magnetar-cli` ships at 1.0.0 with these subcommands marked not-yet-implemented.
  - **moonpool engine** — carries client/producer/consumer; documented as a subset of the tokio engine surface.
- The workspace version and all internal `[workspace.dependencies]` requirements move `0.1.0-dev.0` / `^0.1.0-dev.0` → `1.0.0` / `^1.0.0` in lockstep.
- Tag scheme: annotated, GPG-signed `vMAJOR.MINOR.PATCH` (first tag `v1.0.0`).
- A `CHANGELOG.md` (Keep a Changelog) is introduced with this release.

## Consequences

- Magnetar commits to semver from 1.0.0 onward; the stability promise covers every ✅ parity-matrix surface, not the gated/experimental ones above.
- The e2e replicated-subs CI gate (`.github/workflows/e2e-replicated-subs.yml`) remains the pre-tag gate: a green run is required before `v1.0.0` is pushed.
- Internal-only crates (`magnetar-differential`, `xtask`) stay `publish = false`.
  `magnetar-fakes` is a dev-dependency of `magnetar` carrying a version requirement, so it must be published before `magnetar`, or its dev-dependency entry made path-only.

## References

- [ADR-0010](0010-v0-1-full-java-parity.md) — full Java parity; release-cut gate
- [ADR-0031](0031-pip-460-scalable-subscription-scope.md) — PIP-460 experimental scope
- [ADR-0002](0002-license-apache-2-0.md) — Apache-2.0 license
- [README §"Java client parity matrix"](../../README.md#java-client-parity-matrix)
