# ADR-0002 — License everything Apache-2.0

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: license, legal

## Context

Apache Pulsar (the upstream) is Apache-2.0. moonpool (the simulation engine
we adopt) is Apache-2.0. The natural choice is to mirror that — both because
it removes any sub-licensing concerns (we vendor `PulsarApi.proto` verbatim
under Apache-2.0) and because every crate the project will ever depend on is
either Apache-2.0 or compatible.

The MIT/Apache-2.0 dual-license pattern is common in the Rust ecosystem, but
it adds NOTICE management complexity without buying us anything — Pulsar's
upstream doesn't dual-license, and our consumers are already comfortable with
Apache-2.0.

## Decision

- **Single license**: Apache-2.0, root `LICENSE` + `NOTICE` files committed.
- Every `.rs` file carries an `// SPDX-License-Identifier: Apache-2.0` header.
- The `cargo deny` license allow-list (in `deny.toml`) is exactly:
  `["Apache-2.0", "MIT", "BSD-3-Clause", "ISC", "Unicode-DFS-2016"]`.
  Other licenses (GPL, AGPL, MPL) trigger a CI failure.

## Consequences

- We can vendor the Pulsar `.proto` definitions verbatim, no re-licensing.
- Contributors implicitly license their work under Apache-2.0 by the standard
  inbound = outbound rule (called out in `CONTRIBUTING.md`).
- The CI gate (`cargo deny check`) makes it impossible for a transitive dep
  to silently introduce an incompatible license.

## References

- [`docs/decisions-log.md` §"License"](../../docs/decisions-log.md)
- `deny.toml` (allow-list enforced in CI)
- `LICENSE`, `NOTICE` (committed at the repo root)
