# Open Follow-Ups

Consolidated tracker for known open work.
Each entry lists the gap, the reason it stays open, and (where actionable) a `/goal …` block ready to be copy-pasted verbatim into a fresh session for an agent team to pick up.

For the public-facing parity status, see the [parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally deferred or blocked.
Anything not listed below is either already shipped (check `git log` for the implementation reference) or explicitly out of scope ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md), [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR / docs file carry the post-implementation reference); partially-closed items are trimmed to their remaining open residual.

**API stability stance.** The crate is not yet published.
Breaking API changes are acceptable when they improve correctness, ergonomics, or layering; flag them with `BREAKING CHANGE:` in the commit body so the eventual changelog picks them up.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep · ⏳ blocked on upstream PIP release · 🧠 needs design decision · 🟡 deferred (not load-bearing).

| #   | Item                                                                             | Status                                                                                           |
| --- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                    | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Log rate-limiting / sampling guidance](#2-log-rate-limiting--sampling-guidance) | 🧠 needs design decision                                                                         |

---

## 1. PIP-460 scalable-topics e2e

**Gap.** The PIP-460 scalable-topics surface scaffold is in place across proto / façade / both engines / CLI with the binding 4-layer in-process tests (proto unit + tokio + moonpool 1:1 + differential + golden trace), behind `feature = "scalable-topics"` (default off, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)).
The **e2e** tests in `crates/magnetar/tests/e2e_scalable_topic.rs` have stub bodies that touch a constant and return — per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) they run on every `cargo test --features scalable-topics` and trivially pass.
Three named tests are wired but un-fleshed; no released broker speaks PIP-460.

**Why it stays open.** Upstream PIP-460 is `Draft`, targeting Pulsar 5.0 LTS with phased rollout.
The wire surface is hand-encoded in `crates/magnetar-proto/src/pb/scalable_topics.rs` until a real RC ships.

**`/goal` (once a Pulsar 5.0 RC carries PIP-460).**

```text
/goal flesh out the PIP-460 e2e per docs/follow-ups.md §1 once upstream cuts a Pulsar 5.0 RC carrying PIP-460. First, as a dedicated commit per ADR-0026 §D4, run `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` to replace the hand-encoded crates/magnetar-proto/src/pb/scalable_topics.rs module and reconcile field numbers against the vendored proto. Then implement the bodies of the three stub tests in crates/magnetar/tests/e2e_scalable_topic.rs against a real broker spawned via testcontainers-rs (file is gated `feature = "scalable-topics"` per ADR-0046; no `#[ignore]`, no `feature = "e2e"`). Validation chain per CLAUDE.md.
```

---

## 2. Log rate-limiting / sampling guidance

**Gap.** [ADR-0054](../specs/adr/0054-logging-policy.md) §7 bounds log volume structurally — per-message records are confined to `trace!`/`debug!`, and `warn!` and above are bounded by churn, never by send throughput — but defines no rate-limiting or sampling story for when the churn itself storms (e.g. a broker-restart cascade emitting one `warn!` per reconnect attempt across many connections). sozu solves this with render-time sanitization in its own logger; `tracing` has no built-in per-callsite rate limit, so the options are subscriber-side sampling (application-owned, zero library change), a documented filtering recipe, or library-side per-callsite rate limiting (which carries state per call site — exactly the "state not worth carrying for a log line" trade-off ADR-0054 leans against).

**Why it stays open.** Needs a design decision on where the mechanism lives (subscriber vs library) before any guidance is written; picking the library side adds per-callsite state and an API surface that the subscriber side gets for free.

**`/goal` (once the design question is settled).**

```text
/goal design and document rate-limiting / sampling guidance for magnetar log output per docs/follow-ups.md §2. Decide subscriber-side (document a tracing-subscriber filtering/sampling recipe in docs/logging.md, zero library change) vs library-side (per-callsite rate limiting — justify the added state against ADR-0054 §7). Land the guidance in docs/logging.md and, if the decision is binding, a short ADR-0054 amendment per specs/README.md procedure. Validation chain per CLAUDE.md (docs-only exemption applies if no code changes).
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

One item is a fully external blocker: the PIP-460 e2e flesh-out ([§1](#1-pip-460-scalable-topics-e2e)) waits on a Pulsar 5.0 RC carrying PIP-460.
The logging rate-limit guidance ([§2](#2-log-rate-limiting--sampling-guidance)) waits on an internal design decision, not an external dependency.
