# ADR-0010 — v0.1.0 ships full Java-client parity (no deferrals)

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: scope, release-policy

## Context

The audit's audit proposed an "MVP-then-iterate" model: ship a core in v0.1
and defer PIPs (notably transactions, encryption, scalable topics) to v0.2 /
v0.3. The argument is "smaller v0.1 → earlier release → real-world feedback".

The argument against is that magnetar's value proposition is *parity*. If
users have to pick between "Java with everything" and "Rust with subset",
they pick Java. There's no point shipping a half-built Rust client — that
ground is already covered by `pulsar-rs`.

Florentin's signoff (2026-05-20) chose **full parity at v0.1**.

## Decision

- **v0.1.0 ships feature-complete vs the Apache Pulsar Java client** against
  a Pulsar 4.0+ broker.
- **PIPs in scope for v0.1.0**: PIP-4 (encryption), PIP-22, PIP-26, PIP-30,
  PIP-31 (transactions), PIP-33 (replicated subs), PIP-34, PIP-37
  (chunking + redelivery backoff), PIP-54 (partial-batch ACK), PIP-58,
  PIP-68 (access modes), PIP-87 (AutoConsumeSchema lookup), PIP-90 (broker
  entry metadata), PIP-107, PIP-119, PIP-121 (cluster failover), PIP-124,
  PIP-131, PIP-145 (topic-list watcher), PIP-180 (shadow topic)[^pip-180], PIP-188
  (`TOPIC_MIGRATED`), PIP-282, PIP-292, PIP-296, PIP-313 (force unsubscribe),
  PIP-344, PIP-379, PIP-391, PIP-409, PIP-415 (`getMessageIdByIndex`),
  PIP-460 / PIP-466 (scalable topics — experimental tag).
- Admin REST client + `magnetar` CLI both ship in v0.1.0.
- Auth providers in scope: Token, mTLS, OAuth2 (`ClientCredentialsFlow`),
  SASL `PLAIN` (RFC 4616), SASL Kerberos / GSSAPI via `libgssapi`
  (under the `auth-sasl-kerberos` façade feature, landed ahead of
  v0.2.0 per [ADR-0029](0029-sasl-kerberos-gssapi-scope.md)),
  Athenz with a pre-fetched role token
  (`AthenzProvider::with_role_token`). The Athenz ZTS round-trip
  remains **deferred to v0.2.0** per
  [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  §D3 — a full ZTS/ZMS client is a large, multi-stakeholder
  dependency whose scope is not proportional to the demand from
  Clever Cloud's v0.1.0 use cases. The Athenz ZTS stub surfaces
  `AuthError::Unsupported` so callers see the gap at the auth-method
  boundary rather than at the wire.
- No PIP is deferred to a v0.2 / v0.3.

[^pip-180]: Re-scoped to v0.2.0 by
    [ADR-0033](0033-pip-180-shadow-topic-scope.md) (2026-05-26):
    the shadow-topic surface turned out to span producer wire,
    admin REST (three new endpoints), consumer event classification,
    and a CLI surface — too large for the v0.1.0 finishing wave
    without slipping the rest of the parity matrix. Implemented in
    v0.2.0 commit `82ef01b`.

## Consequences

- The implementation timeline is multi-month (10 milestones M0–M9, see
  [`docs/implementation-plan.md` §0](../../docs/implementation-plan.md)).
- The parity matrix in `README.md` is the merge-gate document — a row going
  from ❌ → ✅ requires an accompanying test (unit + ideally e2e).
- A v0.1.0 release-cut decision becomes "the parity matrix is all ✅ or
  documented 🟡 with a clear remaining-scope statement".
- Any features added beyond Java parity are post-1.0 and tracked separately.

## References

- [`docs/decisions-log.md` §"v0.1.0 scope"](../../docs/decisions-log.md)
- [`docs/research.md`](../../docs/research.md) (Java client surface enumeration)
- [`docs/audit.md`](../../docs/audit.md) §D (the questions that prompted this decision)
- [`README.md` §"Java client parity matrix"](../../README.md)
- [`README.md` §"Supported PIPs"](../../README.md)
- [ADR-0009 Pulsar 4 minimum](0009-pulsar-4-minimum.md)
