# ADR-0010 — Ship full Java-client parity (no deferrals)

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: scope, release-policy

## Context

The audit's audit proposed an "MVP-then-iterate" model: ship a small core first and defer PIPs (notably transactions, encryption, scalable topics) to follow-up waves.
The argument is "smaller initial surface → earlier release → real-world feedback".

The argument against is that magnetar's value proposition is _parity_. If users have to pick between "Java with everything" and "Rust with subset", they pick Java.
There's no point shipping a half-built Rust client — that ground is already covered by `pulsar-rs`.

Florentin's signoff (2026-05-20) chose **full Java parity, no deferrals**.

## Decision

- **Magnetar ships feature-complete vs the Apache Pulsar Java client** against a Pulsar 4.0+ broker.
- **PIPs in scope**: PIP-4 (encryption), PIP-22, PIP-26, PIP-30, PIP-31 (transactions), PIP-33 (replicated subs), PIP-34, PIP-37 (chunking + redelivery backoff), PIP-54 (partial-batch ACK), PIP-58, PIP-68 (access modes), PIP-87 (AutoConsumeSchema lookup), PIP-90 (broker entry metadata), PIP-107, PIP-119, PIP-121 (cluster failover), PIP-124, PIP-131, PIP-145 (topic-list watcher), PIP-180 (shadow topic), PIP-188 (`TOPIC_MIGRATED`), PIP-282, PIP-292, PIP-296, PIP-313 (force unsubscribe), PIP-344, PIP-379, PIP-391, PIP-409, PIP-415 (`getMessageIdByIndex`), PIP-460 / PIP-466 (scalable topics — experimental tag).
- Admin REST client + `magnetar` CLI both ship.
- Auth providers in scope: Token, mTLS, OAuth2 (`ClientCredentialsFlow`), SASL `PLAIN` (RFC 4616), SASL Kerberos / GSSAPI via `libgssapi` (under the `auth-sasl-kerberos` façade feature per [ADR-0029](0029-sasl-kerberos-gssapi-scope.md)), Athenz with a pre-fetched role token (`AthenzProvider::with_role_token`) plus the ZTS round-trip per [ADR-0041](0041-athenz-provider-testability-seams.md) (which supersedes the earlier deferral recorded in [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D3 and [ADR-0030](0030-athenz-zts-round-trip-scope.md)).
- No PIP is silently dropped; any future deferral lands as its own ADR.

## Consequences

- The implementation timeline is multi-month, tracked via the parity matrix in [`README.md`](../../README.md) and the per-feature status in [`docs/parity-status.md`](../../docs/parity-status.md).
- The parity matrix in `README.md` is the merge-gate document — a row going from ❌ → ✅ requires an accompanying test (unit + ideally e2e).
- A release-cut decision becomes "the parity matrix is all ✅ or documented 🟡 with a clear remaining-scope statement".
- Any features added beyond Java parity are tracked separately as follow-up work.

## References

- [`README.md` §"Java client parity matrix"](../../README.md)
- [`README.md` §"Supported PIPs"](../../README.md)
- [`docs/parity-status.md`](../../docs/parity-status.md) — per-feature status snapshot
- [ADR-0009 Pulsar 4 minimum](0009-pulsar-4-minimum.md)
