# ADR-0067 — Publish the façade as `magnetar-driver` and the CLI as `magnetarctl`

- **Status**: Accepted
- **Date**: 2026-06-15
- **Decider**: Florentin Dubois
- **Tags**: release-policy, naming, crates-io

## Context

The 1.0.0 release ([ADR-0066](0066-release-1-0-0-first-stable.md)) published nine supporting crates to crates.io but could **not** publish the top-level façade: the crates.io name `magnetar` is held by an unrelated, abandoned crate ("An exploratory ActivityPub project", owner `AMNatty`, a single `0.1.0` from 2023).
`magnetarctl` was blocked transitively, since it depends on the façade.

Two paths were considered: pursue a transfer of the `magnetar` name (slow, uncertain), or rename the published packages so the full stack can ship now.
We chose to rename.

## Decision

- The façade crate is published on crates.io as **`magnetar-driver`**.
  Its **library name stays `magnetar`** (`[lib] name = "magnetar"`), so consumer code is unchanged — `use magnetar::*` still works; only the dependency line differs (`magnetar-driver = "1.0.1"`).
- The CLI crate is published as **`magnetarctl`**, and its **binary / command is `magnetarctl`** (kubectl-style).
  All CLI docs and examples use `magnetarctl`.
- The façade directory `crates/magnetar` is kept as-is (cosmetic — the library is still `magnetar`); the CLI directory is `crates/magnetarctl`, matching the package and binary name.
- The supporting crates (`magnetar-proto`, `magnetar-admin`, `magnetar-auth-*`, `magnetar-messagecrypto`, `magnetar-runtime-*`, `magnetar-differential`) keep their names; the publishable ones already shipped at 1.0.0 and are unaffected.
- Shipped as a **patch release 1.0.1**: the whole workspace bumps `1.0.0` → `1.0.1` (shared version) and every publishable crate is (re)published at 1.0.1, so crates.io and the `v1.0.1` tag are coherent.
  The `v1.0.0` tag / GitHub Release is left in place as historical.
- This does **not** supersede [ADR-0001](0001-project-name-magnetar.md) or [ADR-0066](0066-release-1-0-0-first-stable.md): the project is still "Magnetar" and the library / import path is still `magnetar`; only the crates.io _package_ names carry suffixes.

## Consequences

- Consumers add `magnetar-driver = "1.0.1"` / install `magnetarctl`, but still write `use magnetar::*`; the project remains "Magnetar".
- If the `magnetar` crates.io name is ever obtained, the façade could re-publish under it via a coordinated change; not planned.
- The shared workspace version means the supporting crates carry a 1.0.1 with no functional change versus 1.0.0 (accepted cost of a single coherent release).

## References

- [ADR-0066](0066-release-1-0-0-first-stable.md) — 1.0.0 first stable release
- [ADR-0001](0001-project-name-magnetar.md) — project name `magnetar`
