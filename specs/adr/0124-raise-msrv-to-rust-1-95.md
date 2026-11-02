# ADR-0124 — Raise the MSRV to Rust 1.95

- **Status**: Accepted
- **Date**: 2026-10-29
- **Decider**: Florentin Dubois
- **Tags**: toolchain, msrv, dependencies, parking-lot
- **Supersedes**: [ADR-0079](0079-raise-msrv-to-rust-1-91.md)

## Context

[ADR-0079](0079-raise-msrv-to-rust-1-91.md) set the workspace minimum supported Rust version (MSRV) to Rust 1.91 because Moonpool 0.8 could not compile on the previous Rust 1.88 floor.

The workspace dependency refresh upgrades its direct `parking_lot` requirement from 0.12.5 to 0.13.0. The published `parking_lot` 0.13.0 manifest declares `rust-version = "1.95"`, so retaining Rust 1.91 would require keeping the workspace on the older direct dependency. `moonpool-sim` 0.8.0 independently requires `parking_lot` 0.12, and Cargo can resolve that transitive version beside the workspace's direct 0.13 version.

The alternatives are therefore to retain the Rust 1.91 floor and defer the direct dependency upgrade, or raise the compiler floor and keep the workspace on the current dependency family. The project chooses the latter.

## Decision

Raise the workspace MSRV from Rust 1.91 to Rust 1.95.

- Set `[workspace.package] rust-version = "1.95"` in `Cargo.toml`.
- Set `msrv = "1.95"` in `clippy.toml`.
- Pin the CI MSRV job to `dtolnay/rust-toolchain@1.95.0` and continue building the whole workspace with all features there.
- Require `parking_lot = "^0.13.0"` for direct workspace consumers.
- Keep `rust-toolchain.toml` on the rolling `stable` channel for development; the exact CI job remains the minimum-version proof.
- Update public, architecture, CLI, and contributor documentation to state Rust 1.95.

## Consequences

- Contributors and downstream source builds require Rust 1.95 or newer.
- Direct workspace users compile against `parking_lot` 0.13.0 or a compatible release.
- The resolved graph temporarily contains both `parking_lot` 0.13 for workspace code and 0.12 for `moonpool-sim`; this is a normal Cargo resolution, not a claim that Moonpool has migrated.
- CI continues to detect accidental use of language or standard-library features newer than the declared minimum.
- [ADR-0079](0079-raise-msrv-to-rust-1-91.md) is superseded; its Moonpool and Rust 1.91 rationale remains historical context.

## References

- `Cargo.toml` — workspace compiler and direct dependency floors.
- `Cargo.lock` — resolved `parking_lot` 0.13.0 and Moonpool's transitive 0.12.5.
- `clippy.toml` — Clippy MSRV policy.
- `.github/workflows/ci.yml` — exact Rust 1.95 MSRV build.
- [`parking_lot` 0.13.0](https://crates.io/crates/parking_lot/0.13.0) — published crate whose manifest declares Rust 1.95.
- Supersedes [ADR-0079](0079-raise-msrv-to-rust-1-91.md).
