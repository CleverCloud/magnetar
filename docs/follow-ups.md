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

| #   | Item                                                                                             | Status                                                                                           |
| --- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                    | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2   | [Wrapper consumers/producers cannot drive `record_rate_window`](#2-wrapper-rate-window-fan-out)  | 🧠 needs design decision on the fan-out surface                                                  |
| 3   | [`Instant::elapsed()` clock leak in proto latency histograms](#3-latency-histogram-clock-leak)   | ⚡ ready to dispatch                                                                             |
| 4   | [`Auto` adjust-schedule arming is parasitic on other deadlines](#4-auto-adjust-arming-bootstrap) | ⚡ ready to dispatch                                                                             |
| 5   | [No gate keeps new e2e files on the container memory budget](#5-e2e-container-memory-gate)       | ⚡ ready to dispatch                                                                             |

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

## 2. Wrapper rate-window fan-out

**Gap.** `PartitionedProducer` / `MultiTopicsConsumer` (and the `PartitionedConsumer` alias) keep their children private and expose no way to drive `record_rate_window` on them, so the `msgs_per_sec` / `bytes_per_sec` fields of `aggregate_stats()` — structurally correct since the #347 fold fix — can never become nonzero for wrapper types.
Discovered while writing `crates/magnetar/tests/e2e_aggregate_stats.rs`; the single-consumer path is proven end-to-end there, the wrapper path only sums zeros today.

**Why it stays open.** Closing it needs new public API surface (a fan-out tick method on the wrappers, or an auto-update-ticker hookup like `auto_update_partitions_interval`) — a design decision outside #347's aggregation charter.

---

## 3. Latency-histogram clock leak

**Gap.** `ConsumerState::pop_message` and `ProducerState::apply_receipt` record latency via `Instant::elapsed()` — a host-clock read inside `magnetar-proto`, violating the ADR-0011 injected-clock rule.
`cargo run -p xtask -- check-no-internal-clock` greps only literal `Instant::now()` / `SystemTime::now()`, so the leak is invisible to the gate, and moonpool's receive/send-latency histograms are not actually deterministic under simulation.
Surfaced during the #347 work; the differential test `aggregate_stats_equivalence.rs` works around it by overwriting the histograms with a synthetic distribution before folding.

**Why it stays open.** The fix is a clock-injection refactor (thread `now: Instant` into the two record sites and derive latency as `now - arrived_at` / `now - enqueued_at`) plus extending the xtask gate to catch `.elapsed()`; both are out of #347's scope and deserve their own four-layer test set.

**`/goal`.**

```text
/goal remove the Instant::elapsed() host-clock reads from magnetar-proto per docs/follow-ups.md §3: thread the injected `now: Instant` into ConsumerState::pop_message and ProducerState::apply_receipt, compute latencies against it, extend `cargo run -p xtask -- check-no-internal-clock` to also reject `.elapsed()` in magnetar-proto src, and ship the ADR-0024 four-layer test set proving moonpool latency histograms are deterministic per seed. Validation chain per CLAUDE.md.
```

---

## 4. Auto adjust arming bootstrap

**Gap.** The `Auto` receiver-queue adjust schedule's first arm (`arm_adjust_clock`) runs only inside `Connection::handle_timeout`, which the drivers invoke only when a `poll_timeout()` deadline actually elapses — the schedule has no dedicated bootstrap trigger and is parasitic on whichever other deadline fires first (typically keepalive).
Every inbound frame refreshes `last_activity` (ADR-0058's single refresh site), so a connection with continuous inbound traffic — message deliveries, or the `CommandAckResponse` stream produced by a consumer that awaits each individual ack — defers the keepalive deadline indefinitely and the adjust schedule never arms, regardless of the configured `keepalive_interval`.
Reproduced during the #349 e2e work (two failing runs, root-caused; the test now avoids per-message ack awaits and uses a 100 ms keepalive so natural gaps arm the schedule).
Production impact is uncertain but not ruled out: individually-awaited acks in a receive loop are a common usage pattern.

**Why it stays open.** The clean fix — arming the adjust clock explicitly at subscribe-ack / initial-flow time — threads `now: Instant` through `Connection::initial_flow`'s call graph and touches `client.rs` plus both engines' `consumer.rs`, a materially larger change than #349's locked permit-split scope.

**`/goal`.**

```text
/goal arm the Auto receiver-queue adjust schedule deterministically per docs/follow-ups.md §4: give arm_adjust_clock a dedicated bootstrap at subscribe-ack/initial-flow time (threading the injected now: Instant through Connection::initial_flow's call graph in proto and both engines) so a continuously-busy connection cannot defer the first adjust tick, and ship the ADR-0024 four-layer test set including a regression test that drives continuous ack-response traffic and asserts the schedule still arms. Validation chain per CLAUDE.md.
```

---

## 5. e2e container memory gate

**Gap.** Every `pulsar standalone` container in `crates/magnetar/tests/e2e_*.rs` now passes `PULSAR_MEM = PULSAR_MEM_LIMIT` (see [`docs/testing.md` § "e2e container memory budget"](testing.md#e2e-container-memory-budget)), but nothing enforces it.
The e2e helpers are copy-paste duplicated per file — a new `e2e_*.rs` cloned from a pre-cap template, or a chain that drops the `.with_env_var` call, silently reintroduces a ~2.3 GiB stock-heap container and the CI runner memory pressure that makes brokers stall past `operation_timeout`.
The failure mode is a flaky timeout in whichever unrelated test happens to be running, so a regression is expensive to diagnose and cheap to misread as "just a flake".

**Why it stays open.** The gate itself is small — grep each `GenericImage::new(image_repo(), image_tag())` chain in `crates/magnetar/tests/` for a `.with_env_var("PULSAR_MEM", …)` before its `.start()` — but landing it means a new `xtask` subcommand plus wiring into [CLAUDE.md § Validation chain](../CLAUDE.md#validation-chain) and the CI workflow, which is wider than the container-budget fix that surfaced it.

**`/goal`.**

```text
/goal add a `cargo run -p xtask -- check-e2e-container-memory` gate per docs/follow-ups.md §5: fail when any `GenericImage::new(image_repo(), image_tag())` chain under crates/magnetar/tests/ reaches `.start()` without a `.with_env_var("PULSAR_MEM", …)` call, model it on the existing check-no-channels/check-log-fields greps, and wire it into the CLAUDE.md validation chain plus .github/workflows/ci.yml alongside the other cheap per-PR gates. Validation chain per CLAUDE.md.
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 is a fully external blocker (the PIP-460 e2e flesh-out waits on a Pulsar 5.0 RC carrying PIP-460); §2 waits on a fan-out API design call; §3, §4 and §5 are dispatch-ready.
