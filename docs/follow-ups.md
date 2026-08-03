# Open Follow-Ups

Consolidated tracker for known open work.
Each entry lists the gap, the reason it stays open, and (where actionable) a `/goal …` block ready to be copy-pasted verbatim into a fresh session for an agent team to pick up.

For the public-facing parity status, see the [parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally deferred or blocked.
Anything not listed below is either already shipped (check `git log` for the implementation reference) or explicitly out of scope ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md), [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR / docs file carry the post-implementation reference); partially-closed items are trimmed to their remaining open residual.

**API stability stance.** The crates are published (`magnetar-driver`, `magnetar-proto`, and the rest of the workspace).
Breaking API changes are still acceptable when they improve correctness, ergonomics, or layering, but each one must carry a `BREAKING CHANGE:` footer in the commit body, a `CHANGELOG.md` entry, and an explicit statement of whether the ergonomic façade surface is affected or only the low-level `magnetar-proto` API (re-exported as `magnetar::proto`).
See [ADR-0086](../specs/adr/0086-inject-now-into-proto-latency-recording.md) for a worked example.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep · ⏳ blocked on upstream PIP release · 🧠 needs design decision · 🟡 deferred (not load-bearing).

| #   | Item                                                                                                                           | Status                   |
| --- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------ |
| 11  | [`scalable_stream_consumer` is uncallable on the tokio engine](#11-scalable_stream_consumer-is-uncallable-on-the-tokio-engine) | ⚡ ready to dispatch     |
| 12  | [PIP-460 per-segment consumer fan-out](#12-pip-460-per-segment-consumer-fan-out)                                               | 🧠 needs design decision |

---

## 11. `scalable_stream_consumer` is uncallable on the tokio engine

**Gap.** `PulsarClient::scalable_stream_consumer` is bound `where E::ClientState: Clone`, and **neither** engine's client implements `Clone` — not `magnetar_runtime_tokio::Client`, nor `magnetar_runtime_moonpool::Client<P>`.
The method therefore resolves on no engine at all, and no caller has ever constructed a `StreamConsumer`.
It went unnoticed because the four in-process test layers drive `magnetar_proto::Connection` directly and the e2e bodies were stubs until [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md); the e2e written against a real broker is what surfaced it.

**Why it stays open.** The fix is a small API decision rather than a bug fix: either make both clients cheap-clone (each is already `Arc`-backed internally, so this is close to a `derive`), or drop the `Clone` bound and have `StreamConsumer` hold a borrow or an `Arc` of the client. Both change a published signature, so it wants a deliberate choice rather than the first thing that compiles.

**Workaround in the meantime.** The layout session is reachable directly — `lookup_scalable_topic` + `next_scalable_event` + `close_scalable_topic_session` — which is the same wire path `StreamConsumer` wraps. `crates/magnetar/tests/e2e_scalable_topic.rs` uses exactly that.

## 13. An unsupervised moonpool connection does not surface a peer hang-up as closed

**Gap.** Every `Client` wait loop on both engines guards on `is_closed()` so a caller does not wait for a reply that can never come. On tokio that guard fires when the peer hangs up. On an **unsupervised** moonpool connection (`Client::connect_plain`) it does not: the driver never marks the connection closed, so the caller waits indefinitely.

Measured 2026-08-03 with a socket that completes the Pulsar handshake and then closes: the tokio leg of `scalable_topic_subscribe` returned an error immediately; the moonpool leg timed out on the full 60 s `HANG_GUARD`.

**Why it stays open.** It is pre-existing and not specific to the scalable surface — `next_scalable_event`, the topic-list waiters and every other `next_*` loop on that engine share the shape. Fixing it means making the moonpool driver mark the connection closed on EOF, which changes behaviour for every waiter at once and wants its own differential transcript per surface rather than a spot fix.

**Consequence today.** `crates/magnetar-differential/tests/scalable_client_equivalence.rs::scalable_subscribe_errors_when_the_connection_closes` covers the tokio leg only, and the moonpool `is_closed()` guard is consequently unreachable from a test — it is the one place `check-sim-coverage` cannot reach on this branch. The guard is kept because it is correct; it fires on an explicit `close()`.

## 12. PIP-460 per-segment consumer fan-out

**Gap.** A registered scalable consumer receives its [`ConsumerAssignment`](../specs/adr/0093-pip-460-upstream-wire-surface.md) — the `segment://` topics it owns — and the client surfaces every rebalance, but nothing attaches an ordinary consumer to those segment topics and merges their streams.
`StreamConsumer` observes the layout; it does not yet deliver messages.

**Why it stays open.** Needs a design decision on ordering across segments, on how per-segment cursors interact with the single subscription name, and on what happens to in-flight messages at a rebalance. `QueueConsumer` and `CheckpointConsumer` sit behind the same decision, and ADR-0093 deliberately left all three out of scope.

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 closed with [ADR-0093](../specs/adr/0093-pip-460-upstream-wire-surface.md), which migrated PIP-460 onto the wire surface Apache Pulsar actually ships (vendored from 5.0.0-M1) and fleshed out the e2e against a real broker; §8 closed with [ADR-0091](../specs/adr/0091-broker-authority-default-port-unification.md) and §10 with [ADR-0092](../specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md). §11 and §12 were both surfaced by that work: the first is dispatch-ready, the second needs a design decision.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
