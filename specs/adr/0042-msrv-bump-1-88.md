# ADR-0042 — Bump MSRV to Rust 1.88 (`let_chains`)

- **Status**: Accepted
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: toolchain, msrv

## Context

ADR-0007 pinned the workspace MSRV at Rust 1.85 (edition 2024 anchor) and
incorrectly attributed the `let_chains` stabilisation to that release. In
fact, the `let_chains` feature — `if let Some(x) = … && let Some(y) = …` —
was stabilised in **Rust 1.88.0** (issue
[`rust-lang/rust#53667`](https://github.com/rust-lang/rust/issues/53667)).

The runtime fixes for in-flight publish snapshots across reset cycles
introduced let-chain patterns in `crates/magnetar-proto/src/conn.rs` (the
chained `if let Some(snapshots) = … && let Some(producer) = …` block),
which causes the workspace to fail compilation on 1.85:

```
error[E0658]: `let` expressions in this position are unstable
   --> crates/magnetar-proto/src/conn.rs:998:16
    |
998 |             if let Some(snapshots) = self.in_flight_publish_snapshots.remove(&handle)
    |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

The MSRV detected by `cargo msrv find` against the post-`chore(deps)` workspace
is **1.88.0**.

## Decision

- `rust-version = "1.88"` (declared in `[workspace.package]` and inherited by
  every member crate via `rust-version.workspace = true`).
- The CI `msrv` job pins `dtolnay/rust-toolchain@1.88.0`.
- `rust-toolchain.toml` continues to use the rolling `stable` channel; the
  pin is a *minimum*, not the dev default.
- `clippy.toml`'s `msrv = "1.88"` matches.
- ADR-0007 is **superseded by this ADR**; its rationale stands except for
  the let-chains attribution.

## Consequences

- Contributors and CI runners must have Rust ≥ 1.88 (released
  2025-06-26). Both already match.
- Future MSRV bumps follow the same shape: declared in `Cargo.toml`,
  mirrored into CI, captured in a new ADR superseding this one.
- We now have a single canonical path for chained `if let` patterns; the
  code base can rely on the feature without `cfg`-gates.

## References

- `Cargo.toml` — `[workspace.package] rust-version = "1.88"`
- `rust-toolchain.toml` — rolling `stable`
- `.github/workflows/ci.yml` — `msrv` job pins `1.88.0`
- `clippy.toml` — `msrv = "1.88"`
- `crates/magnetar-proto/src/conn.rs` — let-chain usage that forces 1.88
- Supersedes [ADR-0007](0007-edition-2024-msrv-1-85.md)
- Detected via `cargo msrv find` (cargo-msrv 0.19.3)
