# Open Follow-Ups

Consolidated tracker for known open work.
Each entry lists the gap, the reason it stays open, and (where actionable) a `/goal …` block ready to be copy-pasted verbatim into a fresh session for an agent team to pick up.

For the public-facing parity status, see the [parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally deferred or blocked.
Anything not listed below is either already shipped (check `git log` for the implementation reference) or explicitly out of scope ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D-series, [ADR-0098](../specs/adr/0098-assignment-driven-m1-hardened-stream-consumer.md), [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR / docs file carry the post-implementation reference); partially-closed items are trimmed to their remaining open residual.

**API stability stance.** The crates are published (`magnetar-driver`, `magnetar-proto`, and the rest of the workspace).
Breaking API changes are still acceptable when they improve correctness, ergonomics, or layering, but each one must carry a `BREAKING CHANGE:` footer in the commit body, a `CHANGELOG.md` entry, and an explicit statement of whether the ergonomic façade surface is affected or only the low-level `magnetar-proto` API (re-exported as `magnetar::proto`).
See [ADR-0086](../specs/adr/0086-inject-now-into-proto-latency-recording.md) for a worked example.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep · ⏳ blocked on upstream PIP release · 🧠 needs design decision · 🟡 deferred (not load-bearing).

| #   | Item                                                                                                                                                    | Status                 |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| 16  | [PIP-460 upstream assignment, lifecycle, DAG-ordering, and proxy contracts](#16-pip-460-upstream-assignment-lifecycle-dag-ordering-and-proxy-contracts) | 🔗 blocked on upstream |

---

## 16. PIP-460 upstream assignment, lifecycle, DAG-ordering, and proxy contracts

**Gap.** Four contracts needed for full PIP-460 parity are absent, incomplete, or undocumented in both Pulsar 5.0.0-M1 and the inspected current `master` implementation:

1. Different consumer assignments may carry the same `layout_epoch`, but the wire has no assignment generation, controller term, or cross-connection fencing rule ([apache/pulsar#26273](https://github.com/apache/pulsar/issues/26273)).
2. A pooled Java V5 consumer has no wire unregister command; logical close removes only the local callback and can leave durable broker membership indefinitely while another pool user keeps the connection alive ([apache/pulsar#26272](https://github.com/apache/pulsar/issues/26272)).
3. The broker's assignment barrier gates active direct children but does not establish the complete PIP-460 ordering contract for sealed intermediate segments, deep DAGs, merge, or cursor rewind ([apache/pulsar#26274](https://github.com/apache/pulsar/issues/26274)).
4. Proxy-any-broker controller registration appears to select an arbitrary broker even though scalable consumer registration is leader-only, and the upstream proxy e2e exercises `QueueConsumer`, which never sends that command ([apache/pulsar#26275](https://github.com/apache/pulsar/issues/26275)).

**Interim Magnetar contract.** [ADR-0098](../specs/adr/0098-assignment-driven-m1-hardened-stream-consumer.md) fences callbacks with a local connection incarnation, defaults to a strict barrier where local ownership history can prove every ancestor complete, reports cross-member ancestry as unprovable, documents pooled close as local-only, and fails closed when controller authority cannot be routed directly.
An explicit broker-managed compatibility mode may rely on M1 for ancestry owned by another member, but it does not claim the stronger local ordering guarantee.
It does not invent protocol fields or claim those local mechanisms settle the distributed contract.

**Why it stays open.** Closing these gaps requires an authoritative upstream answer, tests, and potentially a released Pulsar wire change.
If a response requires new fields or commands, Magnetar updates its vendored proto only from a tagged upstream revision under ADR-0026 §D4; no hand-maintained projection is acceptable after the failure recorded by ADR-0093.

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the wire surface Apache Pulsar actually ships (vendored from 5.0.0-M1) and fleshed out the e2e against a real broker; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md), §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md), §11 and §12 with [ADR-0098](../specs/adr/0098-assignment-driven-m1-hardened-stream-consumer.md), §14 with [ADR-0096](../specs/adr/0096-isolate-sim-coverage-current-pass-artifacts.md), and §15 with [ADR-0097](../specs/adr/0097-use-tokio-time-for-driver-write-deadlines.md). §13 closed with `e93deee`, which woke the scalable waiters on disconnect in both engines.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
