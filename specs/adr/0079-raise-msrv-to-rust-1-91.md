# ADR-0079 — Raise the MSRV to Rust 1.91

- **Status**: Accepted
- **Date**: 2026-07-16
- **Decider**: Florentin Dubois
- **Tags**: toolchain, msrv, dependencies, moonpool

## Context

[ADR-0042](0042-msrv-bump-1-88.md) set the workspace MSRV to Rust 1.88 for stable `let_chains`.
The Moonpool 0.8 source selected by this upgrade uses `std::time::Duration::from_hours` and `Duration::from_mins`.
Compiling `moonpool-sim` 0.8 with Rust 1.88 fails with `E0658` because those duration constructors are not stable on that toolchain.
The same dependency compiles with Rust 1.91.

Magnetar does not carry or patch third-party dependency source to preserve an older compiler floor.
The workspace MSRV must therefore describe the minimum compiler that builds the resolved dependency graph.

## Decision

Raise the workspace MSRV from Rust 1.88 to Rust 1.91.

- Set `[workspace.package] rust-version = "1.91"` in `Cargo.toml`.
- Set `msrv = "1.91"` in `clippy.toml`.
- Pin the CI MSRV job to `dtolnay/rust-toolchain@1.91.0`.
- Keep `rust-toolchain.toml` on the rolling `stable` channel for development; the explicit CI job enforces the minimum.
- Update public and contributor-facing documentation to state Rust 1.91.

## Consequences

- Contributors and downstream source builds require Rust 1.91 or newer.
- Moonpool 0.8 builds without a local fork, a dependency downgrade, or a patch that rewrites upstream duration APIs.
- CI continues to detect accidental use of language or standard-library features newer than the declared minimum.
- [ADR-0042](0042-msrv-bump-1-88.md) is superseded; its `let_chains` rationale remains historical context.

## References

- `Cargo.toml` — workspace `rust-version = "1.91"`.
- `clippy.toml` — Clippy MSRV policy.
- `.github/workflows/ci.yml` — exact Rust 1.91 MSRV build.
- `moonpool-sim` 0.8 `runner/orchestrator.rs` and `runner/fault_injector.rs` — stable duration constructors that establish the new floor.
- Supersedes [ADR-0042](0042-msrv-bump-1-88.md).
