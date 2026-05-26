# Magnetar — Proposals

Detailed implementation plans for upcoming features. Where ADRs lock the
**binding decision** ("we will ship X in v0.2.0, behind feature Y"),
proposals lock the **implementation map** that turns the decision into a
landed PR: wire-protocol delta, sans-io state-machine additions,
per-runtime surface ports, four-layer test plan per
[ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md), and
e2e plan.

Each proposal cites the ADR(s) that authorise it. Proposals are **living
documents** — they evolve as implementation lands. Once a proposal's
work ships, the document either freezes with a `Status: Implemented`
header or is removed and the ADR carries the post-implementation
references.

## Status legend

- `Draft` — Scope sketched; awaiting Florentin sign-off on the ADR.
- `Accepted` — ADR signed; implementation may start.
- `In-flight` — Branch open / PRs landing.
- `Implemented` — All four test layers + e2e green on main.
- `Superseded by …` — Replaced by a later proposal.

## Index — v0.2.0 wave

| # | Title | ADR | Proposal status | **Upstream readiness** |
| --- | --- | --- | --- | --- |
| [PIP-460](pip-460-scalable-topics.md) | Scalable topics / DAG-watch consumer (experimental) | [ADR-0031](../adr/0031-pip-460-scalable-subscription-scope.md) | Draft | 🔴 **NOT LIVE** — PIP is `Draft` upstream; targets Pulsar 5.0 LTS (Oct 2026) with phased rollout via 4.3.0 / 4.4.0. No release ships it yet. |
| [PIP-466](pip-466-v5-client-surface.md) | V5 client surface (experimental) | [ADR-0032](../adr/0032-pip-466-v5-client-surface-scope.md) | Draft | 🟠 **DESIGN-PHASE** — V5 Java client API still iterating upstream; no stable Pulsar release exposes the V5 modules as default. magnetar's V5 surface is a thin skin over v4 wire (which **is** live), so it works against Pulsar 4.x today. |
| [PIP-180](pip-180-shadow-topic.md) | Shadow topic — producer-side source-id + admin REST | [ADR-0033](../adr/0033-pip-180-shadow-topic-scope.md) | Draft | 🟢 **LIVE** — merged upstream in Pulsar 2.11; available on the v0.1.0 baseline broker (`apachepulsar/pulsar:4.0.4`). |
| [PIP-33](pip-33-replicated-subscriptions.md) | Replicated subscriptions — subscribe flag + marker filter | [ADR-0034](../adr/0034-pip-33-replicated-subscriptions-scope.md) | Draft | 🟢 **LIVE** — merged upstream in Pulsar 2.4 (2019); available on the v0.1.0 baseline broker. |

### Upstream-readiness legend

- 🟢 **LIVE** — Merged upstream and available in a released Pulsar
  broker we can target (4.0+). e2e is unblocked.
- 🟠 **DESIGN-PHASE** — Upstream PIP iterating; no stable broker
  release exposes the feature, but magnetar's implementation rides on
  wire bits that **are** live (so the magnetar surface is usable
  against current brokers).
- 🔴 **NOT LIVE** — Upstream PIP is still draft and no Pulsar release
  ships the feature. magnetar work that depends on the wire surface
  must wait for an upstream RC; tests are gated.

### v0.2.0 e2e implications

| PIP | e2e against `apachepulsar/pulsar:4.0.4` | Notes |
| --- | --- | --- |
| PIP-33 | ✅ Possible today | Requires the two-cluster fixture (peer brokers + geo-replication); same upstream image. |
| PIP-180 | ✅ Possible today | Single-broker, no extra fixture. |
| PIP-466 | ✅ Possible today | Mirror tests against existing v4 e2e suite. |
| PIP-460 | ⏸ Blocked | Needs `apachepulsar/pulsar:5.0.0-rc-*` with `scalableTopicsEnabled=true`. 4-layer tests against in-process fakes can land; e2e cannot. |

## How to add a proposal

1. Pick a stable slug — `pip-NNN-kebab-name.md`, `feat-<scope>.md`, or
   similar. Keep it stable: links from PRs and ADRs reference this name.
2. Cite the authorising ADR(s) at the top.
3. Use the section template below.
4. Append a row to the [Index](#index--v020-wave) in the same commit.
5. Update the status header as work progresses.

## Section template

```markdown
# <Title>

- **Status**: Draft / Accepted / In-flight / Implemented
- **ADR**: [ADR-NNNN](../adr/NNNN-...md)
- **Target**: v0.X.0
- **Date**: YYYY-MM-DD
- **Owner**: <name>

## 1. Wire-protocol delta vs. vendored `PulsarApi.proto`
Concrete line references; new commands, new fields, what is already
present vs. what needs a proto bump.

## 2. `magnetar-proto` state-machine additions
New entry points on `Conn` / driver surfaces; new `Event` variants;
new types and their invariants; out-of-scope items.

## 3. Runtime surface ports
### 3.1 `magnetar-runtime-tokio`
Public types, file paths, builder methods, feature flags.

### 3.2 `magnetar-runtime-moonpool`
Scripted broker additions, `Providers` plumbing, any new fakes.

## 4. Four-layer test plan ([ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md))
| Layer | Crate | Test file(s) | Coverage |
| --- | --- | --- | --- |
| (a) proto unit | `magnetar-proto` | … | … |
| (b) tokio integration | `magnetar-runtime-tokio` | … | … |
| (c) moonpool integration | `magnetar-runtime-moonpool` | … | 100% diff |
| (d) differential | `magnetar-differential` | … | EventStream parity |

## 5. E2E plan (`apachepulsar/pulsar:4.x` / `5.x`)
Container/image needs, test file, what it asserts, gating.

## 6. LOC + risk
Aggregate LOC estimate; risks; rollback story.
```

## Relationship to ADRs

ADRs are immutable once accepted; proposals iterate. Concretely:

- If a proposal needs a decision changed (e.g. a new feature flag name,
  a different sans-io entry shape, a new ban) — **write a superseding
  ADR**, not an edit to the proposal.
- If a proposal needs the implementation detail tweaked (a different
  test path, an extra moonpool scenario, a renamed file) — edit the
  proposal in place.
