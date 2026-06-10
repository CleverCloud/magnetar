# ADR-0056 — Re-pin moonpool to crates.io `0.7.0`

- **Status**: Accepted
- **Date**: 2026-06-10
- **Decider**: Florentin Dubois
- **Tags**: dependencies, moonpool, supply-chain, process

## Context

[ADR-0043](0043-temporary-floating-moonpool-git-dep.md) temporarily consumed `moonpool-core` and `moonpool-sim` from git `branch = "main"`.
That exception existed because magnetar needed moonpool's futures-io `TcpStream` migration and segment-granular `write_vectored` support before those changes were published on crates.io.

[ADR-0052](0052-initial-connect-timeout-retry.md) then added a temporary `[patch]` to FlorentinDUBOIS/moonpool branch `fix/no-progress-detector-busy-peer` so the simulation orchestrator could terminate a busy-peer no-progress storm deterministically.

Upstream has now published `moonpool-core` `0.7.0`, `moonpool-sim` `0.7.0`, and `moonpool-explorer` `0.7.0` on crates.io.
I checked with `cargo search moonpool-core --limit 5` and `cargo search moonpool-sim --limit 5`; both commands report `0.7.0` as the current published version.

## Decision

Replace the temporary git dependencies with normal crates.io requirements:

```toml
moonpool-core = "^0.7.0"
moonpool-sim  = "^0.7.0"
```

Remove the temporary `[patch."https://github.com/PierreZ/moonpool"]` entries for `moonpool-core`, `moonpool-sim`, and `moonpool-explorer`.
Remove the matching `[sources].allow-git` entries in `deny.toml`.

This restores cargo-deny's registry, advisory, and source checks for moonpool.
It does **not** use exact `=0.7.0` manifest pins: magnetar's Rust dependency convention is caret requirements in `Cargo.toml`, while `Cargo.lock` plus the `--locked` validation chain remains the reproducibility anchor for deterministic `(commit, seed)` replay.

## Consequences

**Easier**

- `cargo update` no longer advances a moving moonpool git branch.
- `cargo deny check` can inspect moonpool through the normal crates.io source path.
- The temporary fork patch for the no-progress detector is gone; the lockfile now resolves `moonpool-explorer` from crates.io through `moonpool-sim`.

**Cost**

- Future compatible `0.7.x` moonpool releases can be selected by an explicit `cargo update`.
  That is consistent with the rest of the workspace, and the concrete selected version remains reviewed in `Cargo.lock`.

**Supersedes**

- [ADR-0043](0043-temporary-floating-moonpool-git-dep.md) — the temporary git float is closed.

**Amends**

- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — the moonpool git-source exception is removed; reproducibility is now provided by the lockfile and the `--locked` validation chain, matching the wider workspace dependency policy.

## References

- `Cargo.toml` — `[workspace.dependencies]` `moonpool-core` / `moonpool-sim` crates.io requirements.
- `Cargo.lock` — concrete selected `moonpool-*` `0.7.0` crates.
- `deny.toml` — no moonpool git source allowlist remains.
- [ADR-0040](0040-vectored-io-transmit-enum.md) — the vectored-write substrate that required the temporary git float.
- [ADR-0052](0052-initial-connect-timeout-retry.md) — the no-progress detector patch that is now consumed through the published moonpool release.
