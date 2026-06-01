# ADR-0043 — Temporarily float `moonpool-core` / `moonpool-sim` on git `branch = "main"`

- **Status**: Accepted
- **Date**: 2026-05-29
- **Decider**: Florentin Dubois
- **Tags**: dependencies, moonpool, supply-chain, process, network

## Context

[ADR-0040](0040-vectored-io-transmit-enum.md) wave 2 needs the moonpool simulator to model vectored writes at segment granularity so the chaos pack can drop / re-order individual `IoSlice`s.
The matching follow-up ([ADR-0040](0040-vectored-io-transmit-enum.md), [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111)) was blocked until upstream grew that surface.

Upstream has now merged the work on `main` ([PR #113](https://github.com/PierreZ/moonpool/pull/113)).
Two changes ship together and both are **breaking**:

1. **`NetworkProvider::TcpStream` migrated from `tokio::io` to `futures::io`.** The associated stream type now bounds on `futures::io::{AsyncRead, AsyncWrite}` instead of `tokio::io::{AsyncRead, AsyncWrite}`; `TokioNetworkProvider` wraps its `tokio::net::TcpStream` in `tokio_util::compat::Compat` to bridge the two ecosystems.
2. **`SimTcpStream` gained `poll_write_vectored(&[IoSlice])` + `is_write_vectored() -> true`.** Each `IoSlice` is recorded as its own ordered delivery event (segment-granular chaos), with `writev`-style partial-accept semantics.

This is exactly the substrate ADR-0040 wave 2 specified.
The wrinkle: **neither change is on crates.io.** The last published release is `moonpool-core` / `moonpool-sim` `0.6.0`, which still exposes the `tokio::io` stream and has no vectored entry.
The only way to consume the merged work today is a **git dependency** tracking the branch that carries it.

That collides with [ADR-0036](0036-moonpool-seed-sweep-daily-random.md)'s neighbour discipline — the workspace pins exact, reproducible dependency versions so that `(commit, seed)` pairs stay bit-for-bit reproducible (the whole premise of the deterministic-simulation suite).
A `branch = "main"` git dependency is, by construction, a moving target: `cargo update` can pull an arbitrary later `main` commit.

## Decision

Track **`branch = "main"`** for both `moonpool-core` and `moonpool-sim` in `[workspace.dependencies]`, as a **documented, temporary** exception to the exact-pin discipline:

```toml
moonpool-core = { version = "0.6.0", git = "https://github.com/PierreZ/moonpool", branch = "main" }
moonpool-sim  = { version = "0.6.0", git = "https://github.com/PierreZ/moonpool", branch = "main" }
```

- The explicit **`version = "0.6.0"`** alongside the git source is load-bearing, not redundant with `branch`: (1) it keeps the dep off `cargo-deny`'s `wildcards = "deny"` ban (a git dep with no version requirement resolves to `*`), and (2) it acts as a guard rail — when moonpool `main` crosses to `0.7`, `cargo` fails resolution against the `^0.6.0` requirement, which is exactly the re-pin trigger below surfacing automatically instead of a silent major-version drift.
  `deny.toml` separately allow-lists the moonpool git source under `[sources].allow-git`.
- Both crates track the **same git ref** so they stay version-compatible (they are published in lockstep upstream).
- `Cargo.lock` continues to record a **concrete resolved rev** — the float is in the _manifest constraint_, not the _locked artefact_. CI and local builds run `--locked`, so a given commit of magnetar resolves to one fixed moonpool rev until someone deliberately runs `cargo update -p moonpool-core`.
- The moonpool engine's transport is ported from the `tokio::io` ext traits to the `futures::io` ext traits accordingly, and dispatches `TransmitOwned::Vectored` through real `write_vectored` on the plaintext arm (ADR-0040 wave 2 — see Consequences).

**Re-pin trigger.** The **first moonpool crates.io release that contains [PR #113](https://github.com/PierreZ/moonpool/pull/113)** flips this back to an exact released version:

```toml
moonpool-core = "=x.y.z"   # the release carrying PR #113
moonpool-sim  = "=x.y.z"
```

At that point this ADR's status changes to `Superseded by ADR-NNNN` (the re-pin ADR), restoring ADR-0036's exact-pin discipline in full.

## Consequences

**Easier**

- Unblocks [ADR-0040](0040-vectored-io-transmit-enum.md) wave 2: the moonpool engine can dispatch real segment-granular vectored writes under `SimProviders` instead of the placeholder coalesce.
- Closes [ADR-0040](0040-vectored-io-transmit-enum.md) / [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111) — the chaos pack now sees per-`IoSlice` delivery events.

**Harder / cost**

- **Non-reproducible across `cargo update`.** Until the re-pin, a `cargo update` can advance the moonpool rev to an arbitrary later `main` commit.
  The `Cargo.lock` rev is the only thing keeping a given magnetar commit reproducible — it must be treated as load-bearing and reviewed on every bump.
- **CI may pick up unrelated moonpool `main` changes** the moment the lock is refreshed, mixing upstream churn into an otherwise unrelated magnetar PR.
  Lock bumps should be isolated, deliberate commits.
- A `branch`-tracked git dep is **not auditable by `cargo deny`'s version/advisory gates** the way a crates.io release is.

**Mitigations**

- `Cargo.lock` records a concrete rev (`--locked` everywhere in the validation chain — CLAUDE.md / docs/testing.md / parity-status.md).
- The daily 128-random-seed moonpool sweep ([ADR-0036](0036-moonpool-seed-sweep-daily-random.md), [`.github/workflows/moonpool-seed-sweep.yml`](../../.github/workflows/moonpool-seed-sweep.yml)) guards against a silent moonpool-side scheduling regression sneaking in on a lock bump.
- The four-layer cross-runtime + 1:1 parity gates ([ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)) catch any tokio ↔ moonpool divergence introduced by the futures-io transport port.

**Incompatible with**

- The exact-pin steady state assumed by [ADR-0036](0036-moonpool-seed-sweep-daily-random.md).
  This ADR scopes a **single, named** exception (the two moonpool crates) for a **bounded** window (until PR #113 is released); it does not relax the exact-pin rule for any other dependency.

## References

- [ADR-0040](0040-vectored-io-transmit-enum.md) — vectored `Transmit` descriptor; wave 2 is what this git dep unblocks.
- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — the exact-pin reproducibility discipline this ADR carves a temporary exception out of; the daily seed sweep is one of the mitigations.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer + 1:1 parity gates that guard the futures-io transport port.
- `Cargo.toml` — `[workspace.dependencies]` `moonpool-core` / `moonpool-sim` git entries (the floating constraint this ADR records).
- `Cargo.lock` — the concrete resolved moonpool rev (the reproducibility anchor while the constraint floats).
- [ADR-0040](0040-vectored-io-transmit-enum.md) / [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111), [PR #113](https://github.com/PierreZ/moonpool/pull/113) — the upstream work and the re-pin trigger.
- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) §"Transport
  - vectored writes" — the engine-side description of the futures-io port
    and segment-granular dispatch.
