# ADR-0009 — Minimum supported broker: Pulsar 4.0+

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: compatibility, broker-version

## Context

The audit asked: do we target Pulsar 3.0 LTS, 3.x, or 4.0+? Each has trade-offs:

- Pulsar 3.0 LTS — widest fleet coverage, but predates several PIPs we want to land (notably PIP-188 `TOPIC_MIGRATED`, PIP-313 force unsubscribe).
- Pulsar 3.x — splits the test matrix without much fleet benefit.
- Pulsar 4.0+ — newest LTS line, covers the entire PIP scope of [ADR-0010](0010-v0-1-full-java-parity.md) without needing version-conditional wire code.

Targeting **4.0+** is also forward-looking: by the time magnetar 0.1 ships, Pulsar 4 will be the deploying-Pulsar default.

## Decision

- **Minimum broker: Apache Pulsar 4.0** (LTS line).
- The `CONNECT` frame advertises `ProtocolVersion::V21`; the connection falls back to whatever lower version the broker reports on `CONNECTED`.
  But the wire-level features magnetar uses (broker-entry metadata, PIP-188, PIP-87, PIP-145, PIP-37 chunking + redelivery backoff, …) require 4.0.
- The end-to-end suite runs against the official `apachepulsar/pulsar:4.0.4` image and the CI matrix pins that tag.
- We do **not** maintain a 3.x compatibility branch.

## Consequences

- The wire code has no version-gated branches — every PIP-named feature is unconditional.
- Users on 3.x are out of scope.
  The README and the parity matrix call this out front and centre.
- When Pulsar 5.0 ships, we extend the supported list rather than replacing it — old features stay supported as long as 4.x is.

## References

- [`README.md` §"Supported broker versions"](../../README.md)
- [`docs/parity-status.md`](../../docs/parity-status.md)
- `crates/magnetar/tests/e2e_pulsar.rs` — image pin (`apachepulsar/pulsar:4.0.4`)
